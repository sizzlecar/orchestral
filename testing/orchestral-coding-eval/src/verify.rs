use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use serde::Serialize;

use crate::process::{run, ProcessResult};
use crate::task::Task;

#[derive(Debug, Serialize)]
pub struct Verification {
    pub passed: bool,
    pub assertion_failure: bool,
    pub commands: Vec<CheckResult>,
}

#[derive(Debug, Serialize)]
pub struct CheckResult {
    pub process: ProcessResult,
    pub expected_tests: Vec<String>,
    pub missing_tests: Vec<String>,
    pub failed_tests: Vec<String>,
}

pub fn verify(task: &Task, workspace: &Path, output: &Path, seconds: u64) -> Result<Verification> {
    let mut commands = Vec::new();
    for (index, check) in task.checks.iter().enumerate() {
        let logs = output.join(index.to_string());
        let mut command = Command::new("cargo");
        command.args(["test", "--offline", "--locked", "-p", &check.package]);
        if check.target == "lib" {
            command.arg("--lib");
        } else {
            command.args(["--test", check.target.strip_prefix("test:").unwrap()]);
        }
        command
            .arg(&check.filter)
            .args(["--", "--test-threads=1"])
            .current_dir(workspace)
            .env("CARGO_INCREMENTAL", "0")
            .env("CARGO_TERM_COLOR", "never")
            .env_remove("RUSTFLAGS")
            .env_remove("RUSTDOCFLAGS")
            .env_remove("RUSTC_WRAPPER")
            .env_remove("RUSTC_WORKSPACE_WRAPPER")
            .env_remove("CARGO_TARGET_DIR");
        let process = run(&mut command, &logs, seconds)?;
        let stdout = fs::read_to_string(logs.join("stdout.txt"))?;
        commands.push(classify(process, &check.expected_tests, &stdout));
    }
    Ok(Verification {
        passed: commands
            .iter()
            .all(|result| result.process.success() && result.missing_tests.is_empty()),
        assertion_failure: commands
            .iter()
            .any(|result| result.process.exit_code == Some(101) && !result.failed_tests.is_empty()),
        commands,
    })
}

fn classify(process: ProcessResult, expected: &[String], stdout: &str) -> CheckResult {
    let missing_tests = expected
        .iter()
        .filter(|name| {
            !stdout
                .lines()
                .any(|line| line == format!("test {name} ... ok"))
        })
        .cloned()
        .collect();
    let failed_tests = expected
        .iter()
        .filter(|name| {
            stdout
                .lines()
                .any(|line| line == format!("test {name} ... FAILED"))
        })
        .cloned()
        .collect();
    CheckResult {
        process,
        expected_tests: expected.to_vec(),
        missing_tests,
        failed_tests,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process(code: i32) -> ProcessResult {
        ProcessResult {
            exit_code: Some(code),
            elapsed_ms: 0,
            timed_out: false,
            output_limit: false,
        }
    }

    #[test]
    fn zero_selected_tests_and_compile_errors_are_not_successful_checks() {
        let names = vec!["independent_check".into()];
        assert_eq!(
            classify(process(0), &names, "running 0 tests\ntest result: ok.")
                .missing_tests
                .len(),
            1
        );
        assert!(classify(process(101), &names, "error: could not compile")
            .failed_tests
            .is_empty());
        assert!(
            classify(process(0), &names, "test independent_check ... ok\n")
                .missing_tests
                .is_empty()
        );
        assert_eq!(
            classify(process(101), &names, "test independent_check ... FAILED\n").failed_tests,
            names
        );
    }
}
