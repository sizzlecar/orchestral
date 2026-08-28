//! Unified model-facing command execution Tools.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId,
    ToolIdempotency, ToolInvocation, ToolOutcome, ToolRestriction,
};
use serde_json::{json, Map, Value};

use crate::exec_process::{
    ExecPollResult, ExecProcessError, ExecSessionId, ExecSessionManager, ExecSpawnSpec,
};
use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};
use crate::tools::shell_sandbox::{sandbox_command, ShellSandboxPolicy};

use super::support::{build_allowlisted_env, canonical_roots, truncate_utf8_lossy};

pub const GUARDED_EXEC_SANDBOX_PROFILE: &str = "orchestral.exec_command.v1";

#[derive(Clone)]
pub struct GuardedExecCommandExecutor {
    manager: Arc<ExecSessionManager>,
    shell: PathBuf,
    runtime_readable_roots: Vec<PathBuf>,
}

#[derive(Clone)]
pub struct GuardedWriteStdinExecutor {
    manager: Arc<ExecSessionManager>,
}

impl GuardedExecCommandExecutor {
    pub fn new(
        manager: Arc<ExecSessionManager>,
        shell: impl Into<PathBuf>,
        runtime_readable_roots: impl IntoIterator<Item = PathBuf>,
    ) -> Result<Self, String> {
        let shell = std::fs::canonicalize(shell.into())
            .map_err(|error| format!("canonicalize command shell failed: {error}"))?;
        if !shell.is_file() {
            return Err(format!("command shell is not a file: {}", shell.display()));
        }
        let mut roots = runtime_readable_roots
            .into_iter()
            .filter_map(|root| std::fs::canonicalize(root).ok())
            .filter(|root| root.is_dir())
            .collect::<BTreeSet<_>>();
        if let Some(parent) = shell.parent() {
            roots.insert(parent.to_path_buf());
        }
        Ok(Self {
            manager,
            shell,
            runtime_readable_roots: roots.into_iter().collect(),
        })
    }
}

impl GuardedWriteStdinExecutor {
    pub fn new(manager: Arc<ExecSessionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedExecCommandExecutor {
    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let cmd = invocation
            .arguments
            .get("cmd")
            .and_then(Value::as_str)
            .unwrap_or("<invalid-command>");
        format!(
            "Execute in workspace sandbox: {}",
            cmd.chars().take(240).collect::<String>()
        )
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if let Err(outcome) = require_exact_approval(&execution) {
            return outcome;
        }
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let bounds = execution.effective_policy.bounds();
        if !bounds.sandbox.required
            || !bounds
                .sandbox
                .allowed_profiles
                .contains(GUARDED_EXEC_SANDBOX_PROFILE)
        {
            return rejected(
                "exec_sandbox_denied",
                "effective policy does not authorize the exec_command sandbox profile",
            );
        }
        if !bounds.process.allow_shell_expression {
            return rejected(
                "exec_shell_expression_denied",
                "effective policy does not authorize shell command semantics",
            );
        }
        let shell_identity = self.shell.to_string_lossy().into_owned();
        if !bounds.process.allowed_programs.contains(&shell_identity) {
            return rejected(
                "exec_shell_denied",
                "Host command shell is outside the effective process policy",
            );
        }
        let Some(cmd) = execution
            .invocation
            .arguments
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|cmd| !cmd.trim().is_empty())
        else {
            return rejected("exec_command_missing", "cmd must be a non-empty string");
        };
        let readable_roots = match canonical_roots(&bounds.filesystem.readable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => return rejected("exec_read_root_denied", "no readable workspace root"),
            Err(message) => return rejected("exec_read_root_invalid", message),
        };
        let writable_roots = match canonical_roots(&bounds.filesystem.writable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => return rejected("exec_write_root_denied", "no writable workspace root"),
            Err(message) => return rejected("exec_write_root_invalid", message),
        };
        let cwd = match resolve_workdir(
            execution.invocation.arguments.get("workdir"),
            &readable_roots,
            &writable_roots,
        ) {
            Ok(cwd) => cwd,
            Err(message) => return rejected("exec_workdir_invalid", message),
        };
        let mut sandbox_reads = readable_roots;
        sandbox_reads.extend(self.runtime_readable_roots.iter().cloned());
        sandbox_reads.sort();
        sandbox_reads.dedup();
        let sandboxed = match sandbox_command(
            shell_identity,
            shell_arguments(cmd),
            &cwd,
            &ShellSandboxPolicy {
                readable_roots: sandbox_reads,
                writable_roots,
                allow_child_processes: true,
                allowed_programs: vec![self.shell.clone()],
                linux_bwrap_path: None,
            },
        ) {
            Ok(command) => command,
            Err(message) => return failed("exec_sandbox_setup", message, false),
        };
        let allowed_environment = bounds
            .environment
            .allowed_variables
            .iter()
            .map(|name| name.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let environment = build_allowlisted_env(&allowed_environment, &sandboxed.env)
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let tty = execution
            .invocation
            .arguments
            .get("tty")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let wait = bounded_wait(
            &execution.invocation.arguments,
            bounds.max_timeout_ms,
            10_000,
        );
        let max_output_bytes =
            output_byte_limit(&execution.invocation.arguments, bounds.max_output_bytes);
        let session_id = match self
            .manager
            .spawn(ExecSpawnSpec {
                run_id: execution.invocation.run_id.clone(),
                program: sandboxed.program,
                args: sandboxed.args,
                cwd,
                environment,
                tty,
                backend_starts_new_session: sandboxed.backend_starts_new_session,
            })
            .await
        {
            Ok(session_id) => session_id,
            Err(error) => return exec_error(error),
        };

        let manager = self.manager.clone();
        let run_id = execution.invocation.run_id.clone();
        let cancellation = execution.cancellation.clone();
        tokio::spawn(async move {
            cancellation.cancelled().await;
            let _ = manager.close(&run_id, session_id).await;
        });

        let result = match self
            .manager
            .write_and_poll(
                &execution.invocation.run_id,
                session_id,
                None,
                wait,
                &execution.cancellation,
            )
            .await
        {
            Ok(result) => result,
            Err(ExecProcessError::Cancelled) => {
                let _ = self
                    .manager
                    .close(&execution.invocation.run_id, session_id)
                    .await;
                return ToolOutcome::Cancelled;
            }
            Err(error) => return exec_error(error),
        };
        ToolOutcome::Completed {
            output: render_result(result, session_id, max_output_bytes, sandboxed.backend).into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedWriteStdinExecutor {
    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let session_id = invocation
            .arguments
            .get("session_id")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        format!("Write to or poll exec session {session_id}")
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let bounds = execution.effective_policy.bounds();
        let Some(raw_session_id) = execution
            .invocation
            .arguments
            .get("session_id")
            .and_then(Value::as_u64)
        else {
            return rejected(
                "exec_session_missing",
                "session_id must be a positive integer",
            );
        };
        let session_id = match ExecSessionId::new(raw_session_id) {
            Ok(session_id) => session_id,
            Err(error) => return rejected("exec_session_invalid", error.to_string()),
        };
        let input = match execution.invocation.arguments.get("chars") {
            Some(Value::String(input)) => Some(input.as_str()),
            Some(_) => return rejected("exec_input_invalid", "chars must be a string"),
            None => None,
        };
        let wait = bounded_wait(
            &execution.invocation.arguments,
            bounds.max_timeout_ms,
            5_000,
        );
        let max_output_bytes =
            output_byte_limit(&execution.invocation.arguments, bounds.max_output_bytes);
        match self
            .manager
            .write_and_poll(
                &execution.invocation.run_id,
                session_id,
                input,
                wait,
                &execution.cancellation,
            )
            .await
        {
            Ok(result) => ToolOutcome::Completed {
                output: render_result(result, session_id, max_output_bytes, "existing_session")
                    .into(),
            },
            Err(error) => exec_error(error),
        }
    }
}

pub fn guarded_exec_command_descriptor(mut restriction: ToolRestriction) -> ToolDescriptor {
    restriction.bounds.approval = match restriction.bounds.approval {
        ApprovalPolicy::Deny => ApprovalPolicy::Deny,
        ApprovalPolicy::NotRequired | ApprovalPolicy::Required => ApprovalPolicy::Required,
        _ => ApprovalPolicy::Deny,
    };
    apply_exec_restriction(&mut restriction);
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/exec_command/v1"),
        model_schema: ModelToolSchema {
            name: "exec_command".to_owned(),
            description: "Run a shell command in the workspace. Short commands return directly; interactive or still-running commands return a session_id for write_stdin.".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["cmd"],
                "properties": {
                    "cmd": { "type": "string", "minLength": 1 },
                    "workdir": { "type": "string", "minLength": 1 },
                    "tty": { "type": "boolean" },
                    "yield_time_ms": { "type": "integer", "minimum": 1 },
                    "max_output_tokens": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }),
        },
        output_schema: exec_output_schema(),
        effect_scopes: exec_effects(),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

pub fn guarded_write_stdin_descriptor(mut restriction: ToolRestriction) -> ToolDescriptor {
    apply_exec_restriction(&mut restriction);
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/write_stdin/v1"),
        model_schema: ModelToolSchema {
            name: "write_stdin".to_owned(),
            description:
                "Send characters to a running exec session, or omit chars to poll for new output."
                    .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["session_id"],
                "properties": {
                    "session_id": { "type": "integer", "minimum": 1 },
                    "chars": { "type": "string" },
                    "yield_time_ms": { "type": "integer", "minimum": 1 },
                    "max_output_tokens": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }),
        },
        output_schema: exec_output_schema(),
        effect_scopes: exec_effects(),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

fn apply_exec_restriction(restriction: &mut ToolRestriction) {
    restriction.bounds.sandbox.required = true;
    restriction.bounds.sandbox.allowed_profiles =
        BTreeSet::from([GUARDED_EXEC_SANDBOX_PROFILE.to_owned()]);
    restriction.bounds.process.allow_shell_expression = true;
    restriction.bounds.environment.inherit_host_environment = false;
}

fn exec_effects() -> BTreeSet<EffectScope> {
    BTreeSet::from([
        EffectScope::Process,
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::EnvironmentRead,
        EffectScope::ExternalSideEffect,
    ])
}

fn exec_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["output", "stdout", "stderr", "alive", "wall_time_seconds", "truncated", "dropped_bytes", "sandbox_backend"],
        "properties": {
            "output": { "type": "string" },
            "stdout": { "type": "string" },
            "stderr": { "type": "string" },
            "alive": { "type": "boolean" },
            "exit_code": { "type": "integer" },
            "session_id": { "type": "integer" },
            "wall_time_seconds": { "type": "number" },
            "truncated": { "type": "boolean" },
            "dropped_bytes": { "type": "integer" },
            "sandbox_backend": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn require_exact_approval(execution: &GuardedToolExecution) -> Result<(), ToolOutcome> {
    let Some(approval) = execution.approval.as_ref() else {
        return Err(rejected(
            "approval_proof_missing",
            "exec_command requires a verified Host approval capability",
        ));
    };
    if approval.binding().run_id != execution.invocation.run_id
        || approval.binding().call_id != execution.invocation.call_id
        || approval.binding().tool_id != execution.invocation.tool_id
    {
        return Err(rejected(
            "approval_binding_mismatch",
            "verified approval does not belong to this exec_command invocation",
        ));
    }
    Ok(())
}

fn resolve_workdir(
    value: Option<&Value>,
    readable_roots: &[PathBuf],
    writable_roots: &[PathBuf],
) -> Result<PathBuf, String> {
    let base = writable_roots
        .first()
        .or_else(|| readable_roots.first())
        .ok_or_else(|| "no workspace root is available".to_owned())?;
    let requested = match value {
        Some(Value::String(path)) if !path.trim().is_empty() => {
            let path = Path::new(path);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                base.join(path)
            }
        }
        Some(_) => return Err("workdir must be a non-empty string".to_owned()),
        None => base.clone(),
    };
    let cwd = std::fs::canonicalize(&requested)
        .map_err(|error| format!("resolve workdir '{}': {error}", requested.display()))?;
    if !cwd.is_dir() {
        return Err(format!("workdir is not a directory: {}", cwd.display()));
    }
    if !writable_roots.iter().any(|root| cwd.starts_with(root)) {
        return Err(format!(
            "workdir is outside the Host-approved workspace: {}",
            cwd.display()
        ));
    }
    Ok(cwd)
}

#[cfg(unix)]
fn shell_arguments(cmd: &str) -> Vec<String> {
    vec!["-c".to_owned(), cmd.to_owned()]
}

#[cfg(windows)]
fn shell_arguments(cmd: &str) -> Vec<String> {
    vec!["/C".to_owned(), cmd.to_owned()]
}

fn bounded_wait(arguments: &Value, maximum_ms: Option<u64>, default_ms: u64) -> Duration {
    let requested = arguments
        .get("yield_time_ms")
        .and_then(Value::as_u64)
        .unwrap_or(default_ms)
        .max(1);
    Duration::from_millis(requested.min(maximum_ms.unwrap_or(requested)))
}

fn output_byte_limit(arguments: &Value, maximum_bytes: Option<u64>) -> usize {
    let requested = arguments
        .get("max_output_tokens")
        .and_then(Value::as_u64)
        .map(|tokens| tokens.saturating_mul(4));
    let bytes = match (requested, maximum_bytes) {
        (Some(requested), Some(maximum)) => requested.min(maximum),
        (Some(requested), None) => requested,
        (None, Some(maximum)) => maximum,
        (None, None) => 64 * 1024,
    };
    usize::try_from(bytes.max(1)).unwrap_or(usize::MAX)
}

fn render_result(
    result: ExecPollResult,
    session_id: ExecSessionId,
    max_output_bytes: usize,
    sandbox_backend: &str,
) -> Value {
    let (stdout, stdout_truncated, _) =
        truncate_utf8_lossy(result.stdout.as_bytes(), max_output_bytes);
    let remaining = max_output_bytes.saturating_sub(stdout.len());
    let (stderr, stderr_truncated, _) = truncate_utf8_lossy(result.stderr.as_bytes(), remaining);
    let output = if stderr.is_empty() {
        stdout.clone()
    } else if stdout.is_empty() {
        stderr.clone()
    } else {
        format!("{stdout}\n{stderr}")
    };
    let mut value = Map::from_iter([
        ("output".to_owned(), json!(output)),
        ("stdout".to_owned(), json!(stdout)),
        ("stderr".to_owned(), json!(stderr)),
        ("alive".to_owned(), json!(result.alive)),
        (
            "wall_time_seconds".to_owned(),
            json!(result.wall_time_seconds),
        ),
        (
            "truncated".to_owned(),
            json!(stdout_truncated || stderr_truncated || result.dropped_bytes > 0),
        ),
        ("dropped_bytes".to_owned(), json!(result.dropped_bytes)),
        ("sandbox_backend".to_owned(), json!(sandbox_backend)),
    ]);
    if result.alive {
        value.insert("session_id".to_owned(), json!(session_id.get()));
    }
    if let Some(exit_code) = result.exit_code {
        value.insert("exit_code".to_owned(), json!(exit_code));
    }
    Value::Object(value)
}

fn rejected(code: impl Into<String>, message: impl Into<String>) -> ToolOutcome {
    ToolOutcome::Rejected {
        code: code.into(),
        message: message.into(),
    }
}

fn failed(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> ToolOutcome {
    ToolOutcome::Failed {
        code: code.into(),
        message: message.into(),
        retryable,
    }
}

fn exec_error(error: ExecProcessError) -> ToolOutcome {
    match error {
        ExecProcessError::Cancelled => ToolOutcome::Cancelled,
        ExecProcessError::NotFound(_) => rejected("exec_session_not_found", error.to_string()),
        ExecProcessError::Invalid(_) => rejected("exec_invalid", error.to_string()),
        ExecProcessError::Unavailable | ExecProcessError::Io(_) => {
            failed("exec_process_failed", error.to_string(), true)
        }
    }
}
