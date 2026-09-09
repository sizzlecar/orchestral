//! Opt-in evaluation of an actual coding-agent process against pinned source tasks.
mod process;
mod report;
mod task;
mod verify;
mod workspace;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;

use report::{Attempt, Report};
use task::{Suite, Task};
use workspace::{digest, Repository};

#[derive(Parser)]
#[command(about = "Evaluate repository coding tasks without changing the source checkout")]
struct Cli {
    #[command(subcommand)]
    command: Operation,
}

#[derive(Subcommand)]
enum Operation {
    /// List task IDs, descriptions and categories. No model calls.
    List,
    /// Prove the reference passes and each seeded regression fails a real assertion.
    Validate(Options),
    /// Run actual agent processes and independently verify their changes.
    Run {
        #[command(flatten)]
        options: Options,
        /// Explicit agent invocation JSON; arguments are executed without a shell.
        #[arg(long)]
        agent_config: PathBuf,
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
        repetitions: u32,
    },
}

#[derive(clap::Args)]
struct Options {
    #[arg(long, default_value = ".")]
    repo: PathBuf,
    /// Fresh output directory. Defaults to a unique OS temporary directory.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Repeat to select several tasks; omission selects all 20 tasks.
    #[arg(long)]
    task: Vec<String>,
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
    #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..))]
    verify_timeout_secs: u64,
    /// Keep candidate workspaces in addition to patches, logs and reports.
    #[arg(long)]
    keep_workspaces: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Agent {
    name: String,
    model: String,
    argv: Vec<String>,
}

fn main() -> Result<()> {
    let suite = Suite::load()?;
    match Cli::parse().command {
        Operation::List => {
            for task in suite.tasks {
                println!("{}\t{}\t{}", task.id, task.category, task.prompt);
            }
        }
        Operation::Validate(options) => evaluate(&suite, options, None, 0)?,
        Operation::Run {
            options,
            agent_config,
            repetitions,
        } => {
            let mut agent: Agent = serde_json::from_slice(&fs::read(agent_config)?)?;
            ensure!(
                !agent.name.trim().is_empty() && !agent.model.trim().is_empty(),
                "agent and model identities are required"
            );
            ensure!(
                agent.argv.iter().any(|arg| arg.contains("{prompt}")),
                "agent argv must contain {{prompt}}"
            );
            ensure!(!agent.argv.is_empty(), "empty agent argv");
            agent.argv[0] = resolve_executable(&agent.argv[0])?
                .to_string_lossy()
                .into_owned();
            evaluate(&suite, options, Some(agent), repetitions)?;
        }
    }
    Ok(())
}

fn resolve_executable(program: &str) -> Result<PathBuf> {
    let path = Path::new(program);
    let resolved = if path.is_absolute() {
        Some(path.to_owned())
    } else if path.components().count() > 1 {
        Some(std::env::current_dir()?.join(path))
    } else {
        std::env::var_os("PATH").and_then(|value| {
            std::env::split_paths(&value)
                .map(|directory| directory.join(program))
                .find(|path| path.is_file())
        })
    }
    .with_context(|| format!("agent executable not found: {program}"))?;
    ensure!(
        resolved.is_file(),
        "agent executable is not a file: {}",
        resolved.display()
    );
    // Preserve the launch name of symlinked dispatchers, but resolve before
    // changing cwd so relative executable paths cannot refer to candidate files.
    Ok(resolved)
}

fn evaluate(suite: &Suite, options: Options, agent: Option<Agent>, repetitions: u32) -> Result<()> {
    let tasks = suite.select(&options.task)?;
    if tasks.iter().any(|task| task.inspect_first) && agent.is_some() {
        ensure!(
            agent
                .as_ref()
                .unwrap()
                .argv
                .iter()
                .any(|arg| arg.contains("{session_id}")),
            "continuation tasks require {{session_id}} in agent argv"
        );
    }
    let root = fs::canonicalize(&options.repo)?;
    let output = options.output.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("orchestral-coding-eval-{}", uuid::Uuid::new_v4()))
    });
    fs::create_dir(&output).context("output must be a fresh directory")?;
    let output = fs::canonicalize(output)?;
    eprintln!("Evaluation artifacts: {}", output.display());
    let rustc = Command::new("rustc").arg("--version").output()?;
    ensure!(rustc.status.success(), "rustc unavailable");
    let mut report = Report {
        schema_version: 1,
        mode: if agent.is_some() {
            "agent"
        } else {
            "validation"
        }
        .into(),
        base_commit: suite.base_commit.clone(),
        suite_digest: digest(format!("{}{}", task::CATALOG, workspace::EXTRA_TESTS).as_bytes()),
        agent: agent.as_ref().map(|agent| {
            json!({
                "name": agent.name, "model": agent.model,
                "invocation_digest": digest(&serde_json::to_vec(agent).unwrap()),
                "binary_digest": fs::read(&agent.argv[0]).ok().map(|bytes| digest(&bytes)),
            })
        }),
        rustc: String::from_utf8(rustc.stdout)?.trim().to_owned(),
        execution: json!({
            "agent_timeout_secs_per_turn": options.timeout_secs,
            "verification_timeout_secs_per_command": options.verify_timeout_secs,
            "runner_binary_digest": digest(&fs::read(std::env::current_exe()?)?),
        }),
        planned_attempts: tasks.len() * repetitions as usize,
        attempts: Vec::new(),
        validations: Vec::new(),
    };
    report.save(&output)?;
    let repository = Repository::prepare(&root, &output, &suite.base_commit, &tasks)?;
    let config = output.join("agent-config.yaml");
    workspace::default_config(&repository.verifier, &config)?;
    report.execution["orchestral_config_digest"] = digest(&fs::read(&config)?).into();
    let mut invalid = false;
    for task in tasks {
        let task_output = output.join(&task.id);
        fs::create_dir(&task_output)?;
        eprintln!("{}: validating reference and regression", task.id);
        let baseline = validate_task(&repository, task, &task_output, options.verify_timeout_secs);
        let valid = baseline.as_ref().is_ok_and(|value| value["valid"] == true);
        report.validations.push(match baseline {
            Ok(value) => value,
            Err(error) => json!({"task": task.id, "valid": false, "error": format!("{error:#}")}),
        });
        report.save(&output)?;
        if !valid {
            eprintln!("{}: INVALID TASK (see validation logs)", task.id);
            invalid = true;
            continue;
        }
        let Some(agent) = &agent else {
            continue;
        };
        for repetition in 1..=repetitions {
            let directory = task_output.join(format!("attempt-{repetition}"));
            fs::create_dir(&directory)?;
            let mut attempt = Attempt {
                task: task.id.clone(),
                category: task.category.clone(),
                repetition,
                status: "infrastructure_error".into(),
                turns: Vec::new(),
                violations: Vec::new(),
                verification: None,
                error: None,
                model_usage: None,
                cost: None,
                human_corrections: None,
            };
            if let Err(error) = run_attempt(
                &repository,
                task,
                agent,
                &config,
                &directory,
                &options,
                &mut attempt,
            ) {
                attempt.status = "infrastructure_error".into();
                attempt.error = Some(format!("{error:#}"));
            }
            eprintln!("{} attempt {}: {}", task.id, repetition, attempt.status);
            report::write_json(&directory.join("result.json"), &attempt)?;
            report.attempts.push(attempt);
            report.save(&output)?;
            if !options.keep_workspaces && directory.join("workspace").exists() {
                fs::remove_dir_all(directory.join("workspace"))?;
            }
        }
    }
    report.save(&output)?;
    // Build products and pinned source are reproducible; keep the much smaller
    // patches, exact checks and process logs as the default evidence.
    fs::remove_dir_all(&repository.verifier)?;
    fs::remove_file(&repository.archive)?;
    ensure!(
        !invalid,
        "one or more tasks failed reference/regression validation: {}",
        output.display()
    );
    ensure!(
        report
            .attempts
            .iter()
            .all(|attempt| attempt.status == "passed"),
        "one or more agent attempts failed: {}",
        output.display()
    );
    println!("Results: {}", output.join("summary.json").display());
    Ok(())
}

fn validate_task(
    repository: &Repository,
    task: &Task,
    output: &Path,
    seconds: u64,
) -> Result<serde_json::Value> {
    repository.reset_verifier()?;
    let reference = verify::verify(
        task,
        &repository.verifier,
        &output.join("reference"),
        seconds,
    )?;
    repository.seed(&repository.verifier, task)?;
    let regression = verify::verify(
        task,
        &repository.verifier,
        &output.join("regression"),
        seconds,
    )?;
    repository.reset_verifier()?;
    Ok(
        json!({"task": task.id, "valid": reference.passed && regression.assertion_failure,
        "reference": reference, "regression": regression}),
    )
}

#[allow(clippy::too_many_arguments)]
fn run_attempt(
    repository: &Repository,
    task: &Task,
    agent: &Agent,
    config: &Path,
    directory: &Path,
    options: &Options,
    attempt: &mut Attempt,
) -> Result<()> {
    let candidate = repository.candidate(&directory.join("workspace"), task)?;
    let session = uuid::Uuid::new_v4().to_string();
    fs::create_dir(directory.join("home"))?;
    let prompt = task.user_prompt();
    let prompts = if task.inspect_first {
        vec![format!("{prompt}\n\nFor this first turn, inspect and explain the cause only. Do not edit or stage any file yet; wait for my next message."),
             "Proceed with the repair and relevant verification, keeping all constraints from my previous message.".into()]
    } else {
        vec![prompt]
    };
    for (index, prompt) in prompts.iter().enumerate() {
        let args = render_args(&agent.argv, &candidate.root, config, &session, prompt);
        let mut command = Command::new(&args[0]);
        command
            .args(&args[1..])
            .current_dir(&candidate.root)
            .env("ORCHESTRAL_HOME", directory.join("home"))
            .env("CARGO_INCREMENTAL", "0");
        let logs = directory.join(format!("turn-{}", index + 1));
        fs::create_dir(&logs)?;
        fs::write(logs.join("prompt.txt"), prompt)?;
        let result = process::run(&mut command, &logs, options.timeout_secs)?;
        let success = result.success();
        attempt.status = if result.timed_out {
            "timeout"
        } else if !success {
            "agent_error"
        } else {
            "verification_failed"
        }
        .into();
        attempt.turns.push(result);
        let allowed = if task.inspect_first && index == 0 {
            BTreeSet::new()
        } else {
            task.paths()
        };
        attempt.violations.extend(candidate.violations(&allowed)?);
        if !attempt.violations.is_empty() || !success {
            break;
        }
    }
    candidate.save_patch(&directory.join("changes.patch"), &task.paths())?;
    let journal = candidate.root.join(".orchestral/agent-journal");
    attempt.model_usage = report::usage(&journal);
    if journal.is_dir() {
        fs::create_dir(directory.join("session-journals"))?;
        for entry in fs::read_dir(&journal)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry.file_name().to_string_lossy().starts_with("session-")
            {
                fs::copy(
                    entry.path(),
                    directory.join("session-journals").join(entry.file_name()),
                )?;
            }
        }
    }
    if !attempt.violations.is_empty() {
        attempt.status = "constraint_failed".into();
        return Ok(());
    }
    // Grade the repair even if the agent exhausted its budget or failed to
    // finish. A correct patch must not turn an incomplete Run into a success.
    let agent_completed = attempt.turns.iter().all(process::ProcessResult::success);
    if let Err(error) = repository.overlay(&candidate, task) {
        attempt.status = "constraint_failed".into();
        attempt.violations.push(format!("{error:#}"));
        return Ok(());
    }
    let verification = verify::verify(
        task,
        &repository.verifier,
        &directory.join("verification"),
        options.verify_timeout_secs,
    )?;
    if agent_completed {
        attempt.status = if verification.passed {
            "passed"
        } else {
            "verification_failed"
        }
        .into();
    }
    attempt.verification = Some(verification);
    Ok(())
}

fn render_args(
    argv: &[String],
    root: &Path,
    config: &Path,
    session: &str,
    prompt: &str,
) -> Vec<String> {
    argv.iter()
        .map(|arg| {
            arg.replace("{workspace}", &root.to_string_lossy())
                .replace("{config}", &config.to_string_lossy())
                .replace("{session_id}", session)
                .replace("{prompt}", prompt)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompts_are_one_literal_argument_not_shell_code() {
        let prompt = "Fix `code`; $(touch forbidden)\nretain Chinese 中文";
        let args = render_args(
            &["agent".into(), "{prompt}".into()],
            Path::new("/workspace"),
            Path::new("/config"),
            "session",
            prompt,
        );
        assert_eq!(args, ["agent", prompt]);
    }

    #[test]
    #[cfg(unix)]
    fn independent_grading_rejects_a_false_success_and_accepts_a_real_repair() {
        let root =
            std::env::temp_dir().join(format!("coding-eval-grading-{}", uuid::Uuid::new_v4()));
        let source = root.join("source");
        fs::create_dir_all(source.join("src")).unwrap();
        fs::write(
            source.join("Cargo.toml"),
            "[package]\nname = \"grading-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::write(
            source.join("Cargo.lock"),
            "version = 4\n\n[[package]]\nname = \"grading-fixture\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        let original = "pub fn total() -> u32 { 7 }\n#[cfg(test)]\nmod tests {\n    #[test] fn independent_result() { assert_eq!(super::total(), 7); }\n}\n";
        fs::write(source.join("src/lib.rs"), original).unwrap();
        let archive = root.join("source.tar");
        assert!(Command::new("tar")
            .arg("-cf")
            .arg(&archive)
            .arg("-C")
            .arg(&source)
            .arg(".")
            .status()
            .unwrap()
            .success());
        let mut task = Task {
            id: "grading-fixture".into(),
            category: "harness-self-test".into(),
            prompt: "Repair the code".into(),
            inspect_first: false,
            mutations: vec![task::Mutation {
                path: "src/lib.rs".into(),
                before: "{ 7 }".into(),
                after: "{ 0 }".into(),
            }],
            checks: vec![task::Check {
                package: "grading-fixture".into(),
                target: "lib".into(),
                filter: "tests::independent_result".into(),
                expected_tests: vec!["tests::independent_result".into()],
            }],
        };
        let repository = Repository {
            archive,
            verifier: source,
            original: std::collections::BTreeMap::from([("src/lib.rs".into(), original.into())]),
        };
        let options = Options {
            repo: root.clone(),
            output: None,
            task: Vec::new(),
            timeout_secs: 10,
            verify_timeout_secs: 60,
            keep_workspaces: false,
        };
        for (name, script, expected, verified, inspect_first) in [
            (
                "false-success",
                "printf 'Everything is fixed and all tests passed'".to_owned(),
                "verification_failed",
                Some(false),
                false,
            ),
            (
                "repair",
                format!("cat > src/lib.rs <<'SOURCE'\n{original}SOURCE\n"),
                "passed",
                Some(true),
                false,
            ),
            (
                "repair-without-completion",
                format!("cat > src/lib.rs <<'SOURCE'\n{original}SOURCE\nexit 1\n"),
                "agent_error",
                Some(true),
                false,
            ),
            (
                "test-removal",
                "printf 'pub fn total() -> u32 { 7 }\\n' > src/lib.rs".to_owned(),
                "constraint_failed",
                None,
                false,
            ),
            (
                "continuation",
                format!(
                    "if [ -f \"$ORCHESTRAL_HOME/session\" ]; then\n\
                     test \"$(cat \"$ORCHESTRAL_HOME/session\")\" = \"$1\" || exit 2\n\
                     case \"$2\" in 'Proceed with the repair'*) ;; *) exit 3 ;; esac\n\
                     cat > src/lib.rs <<'SOURCE'\n{original}SOURCE\n\
                     else\n\
                     printf '%s' \"$1\" > \"$ORCHESTRAL_HOME/session\"\n\
                     fi\n"
                ),
                "passed",
                Some(true),
                true,
            ),
            (
                "premature-repair",
                format!("cat > src/lib.rs <<'SOURCE'\n{original}SOURCE\n"),
                "constraint_failed",
                None,
                true,
            ),
        ] {
            task.inspect_first = inspect_first;
            let directory = root.join(name);
            fs::create_dir(&directory).unwrap();
            let agent = Agent {
                name: name.into(),
                model: "self-test-only".into(),
                argv: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    script,
                    "grading-fixture".into(),
                    "{session_id}".into(),
                    "{prompt}".into(),
                ],
            };
            let mut attempt = Attempt {
                task: task.id.clone(),
                category: task.category.clone(),
                repetition: 1,
                status: "infrastructure_error".into(),
                turns: Vec::new(),
                violations: Vec::new(),
                verification: None,
                error: None,
                model_usage: None,
                cost: None,
                human_corrections: None,
            };
            run_attempt(
                &repository,
                &task,
                &agent,
                &root.join("unused-config"),
                &directory,
                &options,
                &mut attempt,
            )
            .unwrap();
            assert_eq!(attempt.status, expected, "{attempt:?}");
            assert_eq!(attempt.verification.as_ref().map(|v| v.passed), verified);
            assert_eq!(
                attempt.turns.len(),
                if name == "continuation" { 2 } else { 1 }
            );
        }
        fs::remove_dir_all(root).unwrap();
    }
}
