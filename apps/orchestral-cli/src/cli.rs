use std::env;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::envfile::load_env_file;
use crate::runtime::PlannerOverrides;

#[derive(Debug, Parser)]
#[command(name = "orchestral", about = "Orchestral CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run with optional initial input; enters chat by default
    Run(RunArgs),
    /// Run a repeatable scenario with env loading, structured report, and assertions
    Scenario(ScenarioArgs),
}

#[derive(Debug, Args, Clone)]
struct RunArgs {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    env_file: Option<PathBuf>,
    #[arg(long)]
    planner_backend: Option<String>,
    #[arg(long)]
    planner_model_profile: Option<String>,
    #[arg(long)]
    planner_model: Option<String>,
    #[arg(long)]
    planner_temperature: Option<f32>,
    #[arg(long)]
    thread_id: Option<String>,
    /// Disable MCP action auto-discovery and registration
    #[arg(long)]
    no_mcp: bool,
    /// Disable Skill action auto-discovery and registration
    #[arg(long)]
    no_skills: bool,
    /// Extra MCP discovery file paths (colon-separated or repeated)
    #[arg(long, value_delimiter = ':')]
    mcp_path: Vec<PathBuf>,
    /// Extra skill directories (colon-separated or repeated)
    #[arg(long, value_delimiter = ':')]
    skill_dir: Vec<PathBuf>,
    /// Read multi-turn inputs from file (one turn per line; '#' comments supported)
    #[arg(long)]
    script: Option<PathBuf>,
    #[arg(long)]
    verbose: bool,
    #[arg(long)]
    once: bool,
    #[arg(value_name = "INPUT")]
    input: Vec<String>,
}

#[derive(Debug, Args, Clone)]
struct ScenarioArgs {
    /// YAML scenario spec. If omitted, runs a single ad-hoc scenario from INPUT.
    #[arg(long)]
    spec: Option<PathBuf>,
    /// Load process env vars from KEY=VALUE lines before runtime boot.
    #[arg(long)]
    env_file: Option<PathBuf>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    planner_backend: Option<String>,
    #[arg(long)]
    planner_model_profile: Option<String>,
    #[arg(long)]
    planner_model: Option<String>,
    #[arg(long)]
    planner_temperature: Option<f32>,
    #[arg(long)]
    report: Option<PathBuf>,
    #[arg(long)]
    thread_id: Option<String>,
    #[arg(long)]
    no_mcp: bool,
    #[arg(long)]
    no_skills: bool,
    #[arg(long, value_delimiter = ':')]
    mcp_path: Vec<PathBuf>,
    #[arg(long, value_delimiter = ':')]
    skill_dir: Vec<PathBuf>,
    #[arg(long, default_value_t = 300)]
    timeout_secs: u64,
    #[arg(long)]
    verbose: bool,
    #[arg(long = "assert-persist-contains")]
    persist_contains: Vec<String>,
    #[arg(long = "assert-persist-not-contains")]
    persist_not_contains: Vec<String>,
    #[arg(long = "assert-transient-contains")]
    transient_contains: Vec<String>,
    #[arg(long = "assert-transient-not-contains")]
    transient_not_contains: Vec<String>,
    #[arg(long)]
    max_approvals: Option<usize>,
    #[arg(long)]
    max_errors: Option<usize>,
    #[arg(long)]
    allow_missing_execution_end: bool,
    #[arg(value_name = "INPUT")]
    input: Vec<String>,
}

impl RunArgs {
    fn planner_overrides(&self) -> PlannerOverrides {
        PlannerOverrides {
            backend: self.planner_backend.clone(),
            model_profile: self.planner_model_profile.clone(),
            model: self.planner_model.clone(),
            temperature: self.planner_temperature,
        }
    }
}

impl ScenarioArgs {
    fn planner_overrides(&self) -> PlannerOverrides {
        PlannerOverrides {
            backend: self.planner_backend.clone(),
            model_profile: self.planner_model_profile.clone(),
            model: self.planner_model.clone(),
            temperature: self.planner_temperature,
        }
    }
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.command {
            Some(Command::Run(args)) => {
                if let Some(env_file) = &args.env_file {
                    load_env_file(env_file)?;
                }
                ensure_log_filter(args.verbose);
                let planner_overrides = args.planner_overrides();
                let initial_input = if args.input.is_empty() {
                    None
                } else {
                    Some(args.input.join(" "))
                };
                crate::tui::run_session(crate::tui::SessionRunOptions {
                    config: args.config,
                    planner_overrides,
                    thread_id: args.thread_id,
                    no_mcp: args.no_mcp,
                    no_skills: args.no_skills,
                    mcp_paths: args.mcp_path,
                    skill_dirs: args.skill_dir,
                    initial_input,
                    script_path: args.script,
                    once: args.once,
                    verbose: args.verbose,
                })
                .await
            }
            Some(Command::Scenario(args)) => {
                let planner_overrides = args.planner_overrides();
                crate::scenario::run(crate::scenario::ScenarioRunOptions {
                    spec: args.spec,
                    env_file: args.env_file,
                    config: args.config,
                    planner_overrides,
                    report: args.report,
                    thread_id: args.thread_id,
                    no_mcp: args.no_mcp,
                    no_skills: args.no_skills,
                    mcp_paths: args.mcp_path,
                    skill_dirs: args.skill_dir,
                    timeout_secs: args.timeout_secs,
                    verbose: args.verbose,
                    input: if args.input.is_empty() {
                        None
                    } else {
                        Some(args.input.join(" "))
                    },
                    persist_contains: args.persist_contains,
                    persist_not_contains: args.persist_not_contains,
                    transient_contains: args.transient_contains,
                    transient_not_contains: args.transient_not_contains,
                    max_approvals: args.max_approvals,
                    max_errors: args.max_errors,
                    allow_missing_execution_end: args.allow_missing_execution_end,
                })
                .await
            }
            None => {
                ensure_log_filter(false);
                crate::tui::run_session(crate::tui::SessionRunOptions {
                    config: None,
                    planner_overrides: PlannerOverrides::default(),
                    thread_id: None,
                    no_mcp: false,
                    no_skills: false,
                    mcp_paths: Vec::new(),
                    skill_dirs: Vec::new(),
                    initial_input: None,
                    script_path: None,
                    once: false,
                    verbose: false,
                })
                .await
            }
        }
    }
}

fn ensure_log_filter(verbose: bool) {
    if verbose {
        return;
    }
    if env::var("RUST_LOG").is_ok() {
        return;
    }
    env::set_var("RUST_LOG", "info");
}
