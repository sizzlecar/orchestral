pub mod app;
pub mod bottom_pane;
pub mod event_loop;
pub mod protocol;
pub mod ui;
pub mod update;
pub mod widgets;

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use tokio::sync::mpsc;

use crate::runtime::{PlannerOverrides, RuntimeClient, RuntimeMsg, TransientSlot};

use app::App;

pub struct SessionRunOptions {
    pub config: Option<PathBuf>,
    pub planner_overrides: PlannerOverrides,
    pub thread_id: Option<String>,
    pub no_mcp: bool,
    pub no_skills: bool,
    pub mcp_paths: Vec<PathBuf>,
    pub skill_dirs: Vec<PathBuf>,
    pub initial_input: Option<String>,
    pub script_path: Option<PathBuf>,
    pub once: bool,
    pub verbose: bool,
}

pub async fn run_session(options: SessionRunOptions) -> anyhow::Result<()> {
    let SessionRunOptions {
        config,
        planner_overrides,
        thread_id,
        no_mcp,
        no_skills,
        mcp_paths,
        skill_dirs,
        initial_input,
        script_path,
        once,
        verbose,
    } = options;
    if script_path.is_some() && initial_input.is_some() {
        bail!("cannot combine positional INPUT with --script");
    }

    let scripted_inputs = match script_path {
        Some(path) => load_script_inputs(&path)?,
        None => Vec::new(),
    };
    let use_tui = std::io::stdout().is_terminal() && scripted_inputs.is_empty();
    if use_tui {
        std::env::set_var("ORCHESTRAL_TUI_SILENT_LOGS", "1");
    }
    if no_mcp {
        std::env::set_var("ORCHESTRAL_DISABLE_MCP", "1");
    }
    if no_skills {
        std::env::set_var("ORCHESTRAL_DISABLE_SKILLS", "1");
    }
    if !mcp_paths.is_empty() {
        let joined = mcp_paths
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(":");
        std::env::set_var("ORCHESTRAL_MCP_EXTRA_PATHS", joined);
    }
    if !skill_dirs.is_empty() {
        let joined = skill_dirs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(":");
        std::env::set_var("ORCHESTRAL_SKILL_EXTRA_DIRS", joined);
    }

    let runtime_client = RuntimeClient::from_config(config, thread_id, planner_overrides)
        .await
        .context("initialize runtime client")?;

    if !scripted_inputs.is_empty() {
        return run_script(runtime_client, scripted_inputs).await;
    }

    if !use_tui {
        return run_plain(runtime_client, initial_input).await;
    }

    let mut app = App::new(0, 0, once);

    // Welcome banner
    {
        let info = runtime_client.boot_info();
        let version = env!("CARGO_PKG_VERSION");
        app.history.push(format!("  Orchestral CLI v{}", version));
        app.history.push(format!(
            "  Model: {} ({})",
            info.planner_model, info.planner_backend
        ));
        app.history.push(format!(
            "  Actions: {} builtin | Thread: {}",
            info.action_count,
            runtime_client.thread_id()
        ));
        app.history.push(String::new());
    }

    if verbose {
        app.history.push(format!(
            "  config={}",
            runtime_client.boot_info().config_source
        ));
        app.history.push(String::new());
    }

    event_loop::run_tui(app, runtime_client, initial_input).await
}

fn load_script_inputs(path: &Path) -> anyhow::Result<Vec<String>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read script file {}", path.display()))?;
    let inputs = parse_script_inputs(&raw);
    if inputs.is_empty() {
        bail!("script file {} has no runnable input lines", path.display());
    }
    Ok(inputs)
}

fn parse_script_inputs(raw: &str) -> Vec<String> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToString::to_string)
        .collect()
}

async fn run_script(runtime_client: RuntimeClient, inputs: Vec<String>) -> anyhow::Result<()> {
    for (idx, input) in inputs.into_iter().enumerate() {
        println!("--- turn {} ---", idx + 1);
        println!("> {}", input);
        run_plain_turn(runtime_client.clone(), input).await?;
    }
    Ok(())
}

async fn run_plain(
    runtime_client: RuntimeClient,
    initial_input: Option<String>,
) -> anyhow::Result<()> {
    let input = initial_input.ok_or_else(|| {
        anyhow::anyhow!("interactive terminal unavailable; provide input via `run <text>`")
    })?;
    run_plain_turn(runtime_client, input).await
}

async fn run_plain_turn(runtime_client: RuntimeClient, input: String) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel::<RuntimeMsg>(256);
    let client = runtime_client.clone();
    let submit = tokio::spawn(async move { client.submit_input(input, tx).await });
    let mut last_persist_line: Option<String> = None;

    while let Some(msg) = rx.recv().await {
        match msg {
            RuntimeMsg::PlanningStart => println!("Planning..."),
            RuntimeMsg::PlanningEnd => println!("Executing..."),
            RuntimeMsg::ExecutionStart {
                total,
                execution_mode,
            } => {
                if let Some(mode) = execution_mode {
                    println!("Execution started (total={}, mode={})", total, mode);
                } else {
                    println!("Execution started (total={})", total);
                }
            }
            RuntimeMsg::ExecutionProgress { step } => println!("Progress step={}", step),
            RuntimeMsg::ExecutionEnd => println!("Execution finished"),
            RuntimeMsg::ActivityStart { .. }
            | RuntimeMsg::ActivityItem { .. }
            | RuntimeMsg::ActivityEnd { .. } => {}
            RuntimeMsg::OutputPersist(line) => {
                let trimmed = line.trim().to_string();
                if last_persist_line.as_ref() == Some(&trimmed) {
                    continue;
                }
                println!("{}", line);
                last_persist_line = Some(trimmed);
            }
            RuntimeMsg::AssistantOutput(_) => {}
            RuntimeMsg::AssistantDelta { .. } => {}
            RuntimeMsg::OutputTransient { slot, text } => {
                if matches!(slot, TransientSlot::Status) {
                    println!("{}", text);
                }
            }
            RuntimeMsg::ApprovalRequested { reason, command } => {
                if let Some(command) = command {
                    println!(
                        "Approval required for `{}`: {} (reply /approve or /deny)",
                        command, reason
                    );
                } else {
                    println!("Approval required: {} (reply /approve or /deny)", reason);
                }
            }
            RuntimeMsg::Error(err) => eprintln!("Error: {}", err),
        }
    }

    submit.await.context("submit task join failed")??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_script_inputs;

    #[test]
    fn test_parse_script_inputs_skips_comments_and_blanks() {
        let raw = "\n# comment\nturn-1\n  turn-2  \n\n# another\n";
        let parsed = parse_script_inputs(raw);
        assert_eq!(parsed, vec!["turn-1".to_string(), "turn-2".to_string()]);
    }
}
