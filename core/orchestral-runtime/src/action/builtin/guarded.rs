//! Native guarded file and process Tools.
//!
//! These executors consume only Host-derived effective policy. They do not
//! adapt or call the removed legacy `Action` stack.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use async_trait::async_trait;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId,
    ToolIdempotency, ToolOutcome, ToolRestriction,
};
use serde_json::{json, Value};
use tokio::process::{Child, Command};
use tokio::time::timeout;

use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};
use crate::tools::shell_sandbox::{sandbox_command, ShellSandboxPolicy};

use super::support::{
    build_allowlisted_env, canonical_roots, read_stream_limited, stderr_preview,
    truncate_utf8_lossy,
};

pub const GUARDED_SHELL_SANDBOX_PROFILE: &str = "orchestral.shell.exec.v1";

/// Host-owned executable aliases. Aliases are resolved once during composition
/// and never consult `PATH` during a Tool call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GuardedProgramAliases {
    by_alias: BTreeMap<String, GuardedProgramBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GuardedProgramBinding {
    launch_path: String,
    canonical_identity: String,
}

impl GuardedProgramAliases {
    pub fn new(aliases: impl IntoIterator<Item = (String, String)>) -> Result<Self, String> {
        let mut by_alias = BTreeMap::new();
        for (alias, executable) in aliases {
            validate_program_alias(&alias)?;
            let path = Path::new(&executable);
            if !path.is_absolute() {
                return Err(format!(
                    "Host executable for alias '{alias}' must be an absolute path"
                ));
            }
            let canonical = std::fs::canonicalize(path).map_err(|error| {
                format!("canonicalize executable for alias '{alias}' failed: {error}")
            })?;
            if !canonical.is_file() {
                return Err(format!(
                    "Host executable for alias '{alias}' is not a file: {}",
                    canonical.display()
                ));
            }
            let canonical_identity = canonical.to_string_lossy().to_string();
            if by_alias.contains_key(&alias) {
                return Err(format!(
                    "Host executable alias '{alias}' is configured more than once"
                ));
            }
            by_alias.insert(
                alias,
                GuardedProgramBinding {
                    launch_path: executable,
                    canonical_identity,
                },
            );
        }
        Ok(Self { by_alias })
    }

    pub fn canonical_programs(&self) -> BTreeSet<String> {
        self.by_alias
            .values()
            .map(|binding| binding.canonical_identity.clone())
            .collect()
    }

    pub(super) fn resolve(
        &self,
        command: &str,
        allowed_programs: &BTreeSet<String>,
    ) -> Result<String, String> {
        let path = Path::new(command);
        if path.is_absolute() {
            return canonical_absolute_program(command, allowed_programs);
        }
        validate_program_alias(command)?;
        let binding = self.by_alias.get(command).ok_or_else(|| {
            format!("executable alias is absent from the Host allowlist: {command}")
        })?;
        let current_identity = std::fs::canonicalize(&binding.launch_path)
            .map_err(|error| format!("revalidate executable alias '{command}' failed: {error}"))?
            .to_string_lossy()
            .into_owned();
        if current_identity != binding.canonical_identity {
            return Err(format!(
                "Host executable identity changed after composition: {command}"
            ));
        }
        if !allowed_programs.contains(&binding.canonical_identity) {
            return Err(format!(
                "executable alias is absent from the effective Run allowlist: {command}"
            ));
        }
        Ok(binding.launch_path.clone())
    }

    pub(super) fn accepted_commands(&self, allowed_programs: &BTreeSet<String>) -> Vec<String> {
        let mut commands = allowed_programs.iter().cloned().collect::<BTreeSet<_>>();
        commands.extend(
            self.by_alias
                .iter()
                .filter(|(_, binding)| allowed_programs.contains(&binding.canonical_identity))
                .map(|(alias, _)| alias.clone()),
        );
        commands.into_iter().collect()
    }

    pub(super) fn advertised_programs(&self, allowed_programs: &BTreeSet<String>) -> String {
        let mut advertised = self
            .by_alias
            .iter()
            .filter(|(_, binding)| allowed_programs.contains(&binding.canonical_identity))
            .map(|(alias, binding)| format!("{alias} ({})", binding.canonical_identity))
            .collect::<Vec<_>>();
        let aliased = self
            .by_alias
            .values()
            .map(|binding| binding.canonical_identity.clone())
            .collect::<BTreeSet<_>>();
        advertised.extend(allowed_programs.difference(&aliased).cloned());
        advertised.join(", ")
    }
}

fn validate_program_alias(alias: &str) -> Result<(), String> {
    if alias.trim() != alias || alias.is_empty() || alias.contains('/') || alias.contains('\\') {
        return Err("executable alias must be one bare program name".to_owned());
    }
    let mut components = Path::new(alias).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err("executable alias must be one bare program name".to_owned());
    }
    Ok(())
}

#[derive(Default)]
pub struct GuardedFileReadExecutor;

#[async_trait]
impl GuardedToolExecutor for GuardedFileReadExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let bounds = execution.effective_policy.bounds();
        let roots = match canonical_roots(&bounds.filesystem.readable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => {
                return rejected(
                    "filesystem_root_denied",
                    "effective policy contains no readable filesystem root",
                )
            }
            Err(message) => return rejected("filesystem_root_invalid", message),
        };
        let Some(raw_path) = execution
            .invocation
            .arguments
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty())
        else {
            return rejected(
                "file_path_missing",
                "file_read path must be a non-empty string",
            );
        };
        if Path::new(raw_path)
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return rejected(
                "file_path_escape",
                "file_read path contains a parent traversal",
            );
        }
        let unresolved = if Path::new(raw_path).is_absolute() {
            PathBuf::from(raw_path)
        } else {
            match std::env::current_dir() {
                Ok(cwd) => cwd.join(raw_path),
                Err(error) => return failed("file_cwd_failed", error.to_string(), false),
            }
        };
        let path = match tokio::fs::canonicalize(&unresolved).await {
            Ok(path) => path,
            Err(error) => return failed("file_read_failed", error.to_string(), false),
        };
        if !roots.iter().any(|root| path.starts_with(root)) {
            return rejected(
                "file_path_escape",
                "resolved file is outside Host-approved readable roots",
            );
        }

        let host_limit = usize::try_from(
            bounds
                .max_output_bytes
                .unwrap_or(512 * 1024)
                .saturating_sub(4 * 1024)
                .max(1),
        )
        .unwrap_or(usize::MAX);
        let max_bytes = execution
            .invocation
            .arguments
            .get("max_bytes")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(host_limit)
            .min(host_limit)
            .max(1);
        let truncate = execution
            .invocation
            .arguments
            .get("truncate")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let read = tokio::select! {
            biased;
            _ = execution.cancellation.cancelled() => return ToolOutcome::Cancelled,
            result = tokio::fs::read(&path) => result,
        };
        let mut bytes = match read {
            Ok(bytes) => bytes,
            Err(error) => return failed("file_read_failed", error.to_string(), false),
        };
        let truncated = bytes.len() > max_bytes;
        if truncated && !truncate {
            return failed(
                "file_read_too_large",
                format!(
                    "file contains {} bytes, exceeding the effective limit {max_bytes}",
                    bytes.len()
                ),
                false,
            );
        }
        bytes.truncate(max_bytes);
        let byte_count = bytes.len() as u64;
        let content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(error) => return failed("file_not_utf8", error.to_string(), false),
        };
        ToolOutcome::Completed {
            output: json!({
                "content": content,
                "path": path.to_string_lossy(),
                "bytes": byte_count,
                "truncated": truncated,
            })
            .into(),
        }
    }
}

pub fn guarded_file_read_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/file_read/v1"),
        model_schema: ModelToolSchema {
            name: "file_read".to_owned(),
            description: "Read a UTF-8 text file visible inside the Host-approved workspace"
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": { "type": "string" },
                    "max_bytes": { "type": "integer", "minimum": 1 },
                    "truncate": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["content", "path", "bytes", "truncated"],
            "properties": {
                "content": { "type": "string" },
                "path": { "type": "string" },
                "bytes": { "type": "integer" },
                "truncated": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::FilesystemRead]),
        restriction,
        idempotency: ToolIdempotency::Pure,
        concurrency: ToolConcurrency::ParallelSafe,
    }
}

#[derive(Clone, Default)]
pub struct GuardedShellExecutor {
    program_aliases: GuardedProgramAliases,
}

impl GuardedShellExecutor {
    pub fn new(program_aliases: GuardedProgramAliases) -> Self {
        Self { program_aliases }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedShellExecutor {
    fn approval_summary(
        &self,
        invocation: &orchestral_core::tool_protocol::ToolInvocation,
    ) -> String {
        let command = invocation
            .arguments
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("<invalid-command>");
        let args = invocation
            .arguments
            .get("args")
            .and_then(Value::as_array)
            .map(|args| {
                args.iter()
                    .filter_map(Value::as_str)
                    .map(|argument| {
                        serde_json::to_string(argument).unwrap_or_else(|_| "<invalid>".to_owned())
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();
        format!("Execute in workspace sandbox: {command} {args}")
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        let Some(approval) = execution.approval.as_ref() else {
            return rejected(
                "approval_proof_missing",
                "shell execution requires a verified Host approval capability",
            );
        };
        if approval.binding().run_id != execution.invocation.run_id
            || approval.binding().call_id != execution.invocation.call_id
            || approval.binding().tool_id != execution.invocation.tool_id
        {
            return rejected(
                "approval_binding_mismatch",
                "verified approval does not belong to this shell invocation",
            );
        }
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let bounds = execution.effective_policy.bounds();
        if !bounds.sandbox.required
            || !bounds
                .sandbox
                .allowed_profiles
                .contains(GUARDED_SHELL_SANDBOX_PROFILE)
        {
            return rejected(
                "shell_sandbox_denied",
                "effective policy does not authorize the guarded shell sandbox profile",
            );
        }
        if !bounds.process.interactive.enabled || bounds.process.interactive.allow_child_processes {
            return rejected(
                "shell_expression_unsupported",
                "guarded shell accepts an executable and argument vector only",
            );
        }
        let Some(command) = execution
            .invocation
            .arguments
            .get("command")
            .and_then(Value::as_str)
        else {
            return rejected("shell_command_missing", "shell command must be a string");
        };
        let command = match self
            .program_aliases
            .resolve(command, &bounds.process.interactive.command_shells)
        {
            Ok(command) => command,
            Err(message) => return rejected("shell_program_denied", message),
        };
        let args = match string_arguments(&execution.invocation.arguments) {
            Ok(args) => args,
            Err(message) => return rejected("shell_args_invalid", message),
        };
        let readable_roots = match canonical_roots(&bounds.filesystem.readable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => {
                return rejected(
                    "shell_read_root_denied",
                    "guarded shell requires a Host-approved readable root",
                )
            }
            Err(message) => return rejected("shell_read_root_invalid", message),
        };
        let writable_roots = match canonical_roots(&bounds.filesystem.writable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => {
                return rejected(
                    "shell_write_root_denied",
                    "guarded shell requires a Host-approved writable root",
                )
            }
            Err(message) => return rejected("shell_write_root_invalid", message),
        };
        let cwd = writable_roots
            .first()
            .or_else(|| readable_roots.first())
            .expect("non-empty roots were checked")
            .clone();
        let sandbox_policy = ShellSandboxPolicy {
            readable_roots: readable_roots.clone(),
            readable_files: Vec::new(),
            writable_roots: writable_roots.clone(),
            allow_child_processes: false,
            launcher_programs: bounds
                .process
                .interactive
                .command_shells
                .iter()
                .map(PathBuf::from)
                .collect(),
            network_targets: BTreeSet::new(),
            linux_bwrap_path: None,
        };
        let sandboxed = match sandbox_command(command, args, &cwd, &sandbox_policy) {
            Ok(command) => command,
            Err(message) => return failed("shell_sandbox_setup", message, false),
        };
        let allowed_environment = bounds
            .environment
            .allowed_variables
            .iter()
            .map(|name| name.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let environment = build_allowlisted_env(&allowed_environment, &sandboxed.env);
        let max_output_bytes = usize::try_from(bounds.max_output_bytes.unwrap_or(64 * 1024).max(1))
            .unwrap_or(usize::MAX);
        let timeout_duration = Duration::from_millis(bounds.max_timeout_ms.unwrap_or(30_000));
        let fail_on_non_zero = execution
            .invocation
            .arguments
            .get("fail_on_non_zero")
            .and_then(Value::as_bool)
            .unwrap_or(true);

        let mut command = Command::new(&sandboxed.program);
        command
            .args(&sandboxed.args)
            .env_clear()
            .envs(environment)
            .current_dir(&cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        isolate_process_group(&mut command, sandboxed.backend_starts_new_session);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => return failed("shell_spawn_failed", error.to_string(), false),
        };
        let process_group_id = child.id();
        let Some(stdout) = child.stdout.take() else {
            let _ = terminate_child_tree(&mut child, process_group_id).await;
            return failed("shell_stdout_missing", "stdout pipe was not created", false);
        };
        let Some(stderr) = child.stderr.take() else {
            let _ = terminate_child_tree(&mut child, process_group_id).await;
            return failed("shell_stderr_missing", "stderr pipe was not created", false);
        };
        let mut stdout_task =
            tokio::spawn(async move { read_stream_limited(stdout, max_output_bytes).await });
        let mut stderr_task =
            tokio::spawn(async move { read_stream_limited(stderr, max_output_bytes).await });

        let wait = tokio::select! {
            biased;
            _ = execution.cancellation.cancelled() => {
                let _ = terminate_child_tree(&mut child, process_group_id).await;
                stdout_task.abort();
                stderr_task.abort();
                return ToolOutcome::Cancelled;
            }
            result = timeout(timeout_duration, child.wait()) => result,
        };
        let (status, timed_out) = match wait {
            Ok(Ok(status)) => (Some(status), false),
            Ok(Err(error)) => {
                stdout_task.abort();
                stderr_task.abort();
                return failed("shell_wait_failed", error.to_string(), true);
            }
            Err(_) => (
                terminate_child_tree(&mut child, process_group_id)
                    .await
                    .ok(),
                true,
            ),
        };
        let stdout = tokio::select! {
            biased;
            _ = execution.cancellation.cancelled() => {
                let _ = terminate_child_tree(&mut child, process_group_id).await;
                stdout_task.abort();
                stderr_task.abort();
                return ToolOutcome::Cancelled;
            }
            output = &mut stdout_task => output,
        };
        let (stdout, stdout_stream_truncated) = match stdout {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                let _ = terminate_child_tree(&mut child, process_group_id).await;
                stderr_task.abort();
                return failed("shell_stdout_read_failed", error.to_string(), true);
            }
            Err(error) => {
                let _ = terminate_child_tree(&mut child, process_group_id).await;
                stderr_task.abort();
                return failed("shell_stdout_join_failed", error.to_string(), true);
            }
        };
        let stderr = tokio::select! {
            biased;
            _ = execution.cancellation.cancelled() => {
                let _ = terminate_child_tree(&mut child, process_group_id).await;
                stderr_task.abort();
                return ToolOutcome::Cancelled;
            }
            output = &mut stderr_task => output,
        };
        let (stderr, stderr_stream_truncated) = match stderr {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                let _ = terminate_child_tree(&mut child, process_group_id).await;
                return failed("shell_stderr_read_failed", error.to_string(), true);
            }
            Err(error) => {
                let _ = terminate_child_tree(&mut child, process_group_id).await;
                return failed("shell_stderr_join_failed", error.to_string(), true);
            }
        };
        terminate_process_group(process_group_id);
        let (stdout, stdout_truncated, _) = truncate_utf8_lossy(&stdout, max_output_bytes);
        let (stderr, stderr_truncated, _) = truncate_utf8_lossy(&stderr, max_output_bytes);
        let stdout = stdout.trim_end().to_owned();
        let stderr = stderr.trim_end().to_owned();
        let code = status.as_ref().and_then(ExitStatus::code).unwrap_or(-1);
        if timed_out {
            return failed(
                "shell_timed_out",
                format!(
                    "shell command timed out after {} ms",
                    timeout_duration.as_millis()
                ),
                true,
            );
        }
        if fail_on_non_zero && code != 0 {
            return failed(
                "shell_non_zero",
                format!(
                    "shell command exited with status {code}; stderr={}",
                    stderr_preview(&stderr, 280)
                ),
                false,
            );
        }
        ToolOutcome::Completed {
            output: json!({
                "stdout": stdout,
                "stderr": stderr,
                "status": code,
                "timed_out": false,
                "stdout_truncated": stdout_truncated || stdout_stream_truncated,
                "stderr_truncated": stderr_truncated || stderr_stream_truncated,
                "sandbox_mode": "workspace_write",
                "sandboxed": true,
                "sandbox_backend": sandboxed.backend,
                "env_policy": "allowlist",
            })
            .into(),
        }
    }
}

pub fn guarded_shell_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    guarded_shell_descriptor_with_program_aliases(restriction, &GuardedProgramAliases::default())
}

pub fn guarded_shell_descriptor_with_program_aliases(
    mut restriction: ToolRestriction,
    program_aliases: &GuardedProgramAliases,
) -> ToolDescriptor {
    restriction.bounds.approval = match restriction.bounds.approval {
        ApprovalPolicy::Deny => ApprovalPolicy::Deny,
        ApprovalPolicy::NotRequired | ApprovalPolicy::Required => ApprovalPolicy::Required,
        _ => ApprovalPolicy::Deny,
    };
    restriction.bounds.sandbox.required = true;
    restriction.bounds.sandbox.allowed_profiles =
        BTreeSet::from([GUARDED_SHELL_SANDBOX_PROFILE.to_owned()]);
    restriction.bounds.process.interactive.enabled = true;
    restriction.bounds.process.interactive.allow_child_processes = false;
    restriction.bounds.environment.inherit_host_environment = false;
    let advertised_programs =
        program_aliases.advertised_programs(&restriction.bounds.process.interactive.command_shells);
    let accepted_commands =
        program_aliases.accepted_commands(&restriction.bounds.process.interactive.command_shells);
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/shell_exec/v1"),
        model_schema: ModelToolSchema {
            name: "shell".to_owned(),
            description: format!(
                "Execute one Host-approved program with an argument vector inside the workspace sandbox. Use an advertised alias or absolute executable path; shell expressions are unsupported. Allowed programs: {advertised_programs}"
            ),
            input_schema: json!({
                "type": "object",
                "required": ["command"],
                "properties": {
                    "command": {
                        "type": "string",
                        "enum": accepted_commands,
                        "description": "One advertised executable alias or absolute path; put every argument in args"
                    },
                    "args": { "type": "array", "items": { "type": "string" } },
                    "fail_on_non_zero": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["stdout", "stderr", "status", "timed_out", "stdout_truncated", "stderr_truncated", "sandbox_mode", "sandboxed", "sandbox_backend", "env_policy"],
            "properties": {
                "stdout": { "type": "string" },
                "stderr": { "type": "string" },
                "status": { "type": "integer" },
                "timed_out": { "type": "boolean" },
                "stdout_truncated": { "type": "boolean" },
                "stderr_truncated": { "type": "boolean" },
                "sandbox_mode": { "type": "string" },
                "sandboxed": { "type": "boolean" },
                "sandbox_backend": { "type": "string" },
                "env_policy": { "type": "string" }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
        ]),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

fn canonical_absolute_program(
    command: &str,
    allowed_programs: &BTreeSet<String>,
) -> Result<String, String> {
    let path = Path::new(command);
    if !path.is_absolute() {
        return Err("guarded absolute executable path is required".to_owned());
    }
    let canonical = std::fs::canonicalize(path)
        .map_err(|error| format!("canonicalize executable '{command}' failed: {error}"))?;
    let canonical = canonical.to_string_lossy().to_string();
    if !allowed_programs.contains(&canonical) {
        return Err(format!(
            "executable is absent from the effective Host allowlist: {canonical}"
        ));
    }
    Ok(canonical)
}

fn string_arguments(arguments: &Value) -> Result<Vec<String>, String> {
    let Some(arguments) = arguments.get("args") else {
        return Ok(Vec::new());
    };
    let Some(arguments) = arguments.as_array() else {
        return Err("shell args must be an array".to_owned());
    };
    arguments
        .iter()
        .map(|argument| {
            argument
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "every shell argument must be a string".to_owned())
        })
        .collect()
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

#[cfg(unix)]
fn isolate_process_group(command: &mut Command, backend_starts_new_session: bool) {
    if !backend_starts_new_session {
        command.process_group(0);
    }
}

#[cfg(not(unix))]
fn isolate_process_group(_command: &mut Command, _backend_starts_new_session: bool) {}

async fn terminate_child_tree(
    child: &mut Child,
    process_group_id: Option<u32>,
) -> io::Result<ExitStatus> {
    terminate_process_group(process_group_id);
    let _ = child.start_kill();
    child.wait().await
}

#[cfg(unix)]
fn terminate_process_group(process_group_id: Option<u32>) {
    if let Some(process_group_id) = process_group_id.filter(|id| *id <= i32::MAX as u32) {
        // SAFETY: the child was spawned as the leader of a fresh process group.
        unsafe {
            libc::kill(-(process_group_id as i32), libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn terminate_process_group(_process_group_id: Option<u32>) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn current_executable() -> String {
        std::fs::canonicalize(std::env::current_exe().unwrap())
            .unwrap()
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn program_alias_resolves_to_the_host_identity_without_path_lookup() {
        let executable = current_executable();
        let aliases =
            GuardedProgramAliases::new([("fixture".to_owned(), executable.clone())]).unwrap();
        let allowed = BTreeSet::from([executable.clone()]);

        assert_eq!(aliases.resolve("fixture", &allowed), Ok(executable));
    }

    #[test]
    fn program_alias_cannot_widen_the_effective_run_allowlist() {
        let executable = current_executable();
        let aliases = GuardedProgramAliases::new([("fixture".to_owned(), executable)]).unwrap();

        assert!(aliases.resolve("fixture", &BTreeSet::new()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn program_alias_preserves_the_host_resolved_launch_path() {
        let parent =
            std::env::temp_dir().join(format!("orchestral-alias-launch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&parent).unwrap();
        let link = parent.join("fixture");
        let executable = current_executable();
        std::os::unix::fs::symlink(&executable, &link).unwrap();
        let launch_path = link.to_string_lossy().into_owned();
        let aliases =
            GuardedProgramAliases::new([("fixture".to_owned(), launch_path.clone())]).unwrap();
        let allowed = BTreeSet::from([executable]);

        assert_eq!(aliases.resolve("fixture", &allowed), Ok(launch_path));
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn program_alias_rejects_paths_and_duplicates() {
        let executable = current_executable();
        for alias in ["", " fixture", "./fixture", "../fixture", "a/b", "a\\b"] {
            assert!(GuardedProgramAliases::new([(alias.to_owned(), executable.clone())]).is_err());
        }
        assert!(GuardedProgramAliases::new([
            ("fixture".to_owned(), executable.clone()),
            ("fixture".to_owned(), executable),
        ])
        .is_err());
    }
}
