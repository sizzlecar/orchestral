use std::fs::{self, File};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;

const MAX_OUTPUT_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub struct ProcessResult {
    pub exit_code: Option<i32>,
    pub elapsed_ms: u128,
    pub timed_out: bool,
    pub output_limit: bool,
}

impl ProcessResult {
    pub fn success(&self) -> bool {
        self.exit_code == Some(0) && !self.timed_out && !self.output_limit
    }
}

/// Capture to files so a full pipe or an inherited pipe cannot deadlock grading.
/// Every command has a deadline and its own process group on Unix.
pub fn run(command: &mut Command, directory: &Path, seconds: u64) -> Result<ProcessResult> {
    fs::create_dir_all(directory)?;
    let stdout = directory.join("stdout.txt");
    let stderr = directory.join("stderr.txt");
    command
        .stdin(Stdio::null())
        .stdout(File::create(&stdout)?)
        .stderr(File::create(&stderr)?);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let started = Instant::now();
    let mut child = command.spawn().context("start evaluation command")?;
    let mut timed_out = false;
    let mut output_limit = false;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        timed_out = started.elapsed() >= Duration::from_secs(seconds);
        output_limit = [&stdout, &stderr]
            .iter()
            .any(|path| fs::metadata(path).is_ok_and(|metadata| metadata.len() > MAX_OUTPUT_BYTES));
        if timed_out || output_limit {
            terminate_group(child.id());
            let _ = child.kill();
            break child.wait()?;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    // Reap descendants even when a launcher exits before its subprocesses.
    terminate_group(child.id());
    // A short-lived command can exit between polls with oversized output.
    // Retain bounded evidence and reject that result as well.
    for path in [&stdout, &stderr] {
        if fs::metadata(path)?.len() > MAX_OUTPUT_BYTES {
            output_limit = true;
            File::options()
                .write(true)
                .open(path)?
                .set_len(MAX_OUTPUT_BYTES)?;
        }
    }
    Ok(ProcessResult {
        exit_code: status.code(),
        elapsed_ms: started.elapsed().as_millis(),
        timed_out,
        output_limit,
    })
}

#[cfg(unix)]
fn terminate_group(id: u32) {
    let _ = Command::new("/bin/kill")
        .args(["-KILL", "--", &format!("-{id}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(not(unix))]
fn terminate_group(_id: u32) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn a_deadline_stops_the_child_and_preserves_its_output() {
        let root =
            std::env::temp_dir().join(format!("coding-eval-process-{}", uuid::Uuid::new_v4()));
        let result = run(
            Command::new("/bin/sh").args(["-c", "printf started; sleep 30"]),
            &root,
            1,
        )
        .unwrap();
        assert!(result.timed_out);
        assert!(!result.success());
        assert!(result.elapsed_ms < 5000);
        assert_eq!(
            fs::read_to_string(root.join("stdout.txt")).unwrap(),
            "started"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn oversized_output_is_rejected_and_retained_logs_are_bounded() {
        let root =
            std::env::temp_dir().join(format!("coding-eval-output-{}", uuid::Uuid::new_v4()));
        let result = run(
            Command::new("/bin/sh").args(["-c", "dd if=/dev/zero bs=1048576 count=33"]),
            &root,
            10,
        )
        .unwrap();
        assert!(result.output_limit);
        assert!(!result.success());
        assert_eq!(
            fs::metadata(root.join("stdout.txt")).unwrap().len(),
            MAX_OUTPUT_BYTES
        );
        fs::remove_dir_all(root).unwrap();
    }
}
