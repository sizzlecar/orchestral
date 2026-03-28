use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;
use tracing::info;

use super::support::{params_get_string, truncate_utf8_lossy};
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

const DEFAULT_TIMEOUT_MS: u64 = 300_000; // 5 minutes
const MAX_TIMEOUT_MS: u64 = 600_000; // 10 minutes
const MAX_OUTPUT_BYTES: usize = 512 * 1024; // 512 KB

pub(super) struct SubprocessAction {
    name: String,
    description: String,
}

impl SubprocessAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Runs a long-lived subprocess (e.g. codex, claude, build tools)"),
        }
    }
}

#[async_trait]
impl Action for SubprocessAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("direct")
            .with_capabilities(["shell", "side_effect", "long_running"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Shell command to execute (runs via sh -c)."
                    },
                    "cwd": {
                        "type": "string",
                        "description": "Working directory. Defaults to CWD."
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Timeout in seconds. Default 300 (5 min), max 600 (10 min)."
                    }
                },
                "required": ["command"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "stdout": { "type": "string", "description": "Complete stdout output." },
                    "stderr": { "type": "string", "description": "Complete stderr output." },
                    "status": { "type": "integer", "description": "Exit code." },
                    "timed_out": { "type": "boolean" },
                    "line_count": { "type": "integer", "description": "Number of stdout lines." }
                },
                "required": ["stdout", "stderr", "status", "timed_out"]
            }))
    }

    async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let command = match params_get_string(params, "command") {
            Some(cmd) => cmd,
            None => return ActionResult::error("Missing command for subprocess action"),
        };

        let cwd = params_get_string(params, "cwd")
            .map(std::path::PathBuf::from)
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| std::path::PathBuf::from("."));

        let timeout_secs = params
            .get("timeout_secs")
            .and_then(|v| v.as_u64())
            .unwrap_or(DEFAULT_TIMEOUT_MS / 1000);
        let timeout_ms = (timeout_secs * 1000).min(MAX_TIMEOUT_MS);

        info!(
            action = %self.name,
            command = %command,
            cwd = %cwd.display(),
            timeout_secs = timeout_ms / 1000,
            "subprocess action starting"
        );

        let mut cmd = Command::new("sh");
        cmd.args(["-c", &command]);
        cmd.current_dir(&cwd);
        cmd.stdin(Stdio::null());
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());
        cmd.kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => return ActionResult::error(format!("Subprocess spawn failed: {}", e)),
        };

        let stdout_pipe = child.stdout.take().unwrap();
        let stderr_pipe = child.stderr.take().unwrap();

        // Read stdout line by line
        let cancel = ctx.cancellation_token.clone();
        let stdout_task = tokio::spawn(async move {
            let mut lines = Vec::new();
            let mut total_bytes = 0usize;
            let reader = BufReader::new(stdout_pipe);
            let mut line_stream = reader.lines();
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    line = line_stream.next_line() => {
                        match line {
                            Ok(Some(line)) => {
                                total_bytes += line.len() + 1;
                                if total_bytes <= MAX_OUTPUT_BYTES {
                                    lines.push(line);
                                }
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                }
            }
            (lines, total_bytes)
        });

        // Read stderr fully
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            let mut reader = BufReader::new(stderr_pipe);
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf).await;
            buf
        });

        // Wait with timeout
        let (exit_status, timed_out) =
            match timeout(Duration::from_millis(timeout_ms), child.wait()).await {
                Ok(Ok(status)) => (Some(status), false),
                Ok(Err(e)) => {
                    return ActionResult::error(format!("Subprocess wait failed: {}", e))
                }
                Err(_) => {
                    info!(
                        action = %self.name,
                        timeout_ms = timeout_ms,
                        "subprocess timed out, killing"
                    );
                    let _ = child.kill().await;
                    let status = child.wait().await.ok();
                    (status, true)
                }
            };

        let (stdout_lines, _stdout_bytes) = match stdout_task.await {
            Ok(v) => v,
            Err(e) => return ActionResult::error(format!("Stdout read failed: {}", e)),
        };
        let stderr_raw = match stderr_task.await {
            Ok(v) => v,
            Err(e) => return ActionResult::error(format!("Stderr read failed: {}", e)),
        };

        let stdout = stdout_lines.join("\n");
        let (stderr, _, _) = truncate_utf8_lossy(&stderr_raw, MAX_OUTPUT_BYTES);
        let status = exit_status.and_then(|s| s.code()).unwrap_or(-1);
        let line_count = stdout_lines.len();

        info!(
            action = %self.name,
            status = status,
            timed_out = timed_out,
            stdout_lines = line_count,
            stderr_len = stderr.len(),
            "subprocess action finished"
        );

        if timed_out {
            return ActionResult::error(format!(
                "Subprocess timed out after {} seconds",
                timeout_ms / 1000
            ));
        }
        if status != 0 {
            let stderr_preview = if stderr.len() > 300 {
                format!("{}...", &stderr[..300])
            } else {
                stderr.clone()
            };
            return ActionResult::error(format!(
                "Subprocess exited with status {}. stderr={}",
                status, stderr_preview
            ));
        }

        let mut exports = Map::new();
        exports.insert("stdout".to_string(), Value::String(stdout));
        exports.insert("stderr".to_string(), Value::String(stderr));
        exports.insert("status".to_string(), Value::Number(status.into()));
        exports.insert("timed_out".to_string(), Value::Bool(timed_out));
        exports.insert("line_count".to_string(), Value::Number(line_count.into()));
        ActionResult::success_with(exports.into_iter().collect())
    }
}
