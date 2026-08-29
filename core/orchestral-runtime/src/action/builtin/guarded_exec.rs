//! Unified model-facing command execution Tools.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId,
    ToolIdempotency, ToolInvocation, ToolOperationPlan, ToolOperationRisk, ToolOutcome,
    ToolRestriction,
};
use serde_json::{json, Map, Value};

use crate::exec_process::{
    ExecPollResult, ExecProcessError, ExecSessionId, ExecSessionStatus, ExecSpawnSpec,
    ProcessSupervisor,
};
use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};
use crate::tools::shell_sandbox::{sandbox_command, ShellSandboxPolicy};

use super::support::{canonical_roots, truncate_utf8_lossy};

pub const GUARDED_EXEC_SANDBOX_PROFILE: &str = "orchestral.exec_command.v1";

/// Immutable Host environment captured when the Agent runtime is composed.
/// Tool calls only receive the intersection with their effective policy.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommandEnvironmentSnapshot {
    values: BTreeMap<String, String>,
}

impl CommandEnvironmentSnapshot {
    pub fn capture(names: impl IntoIterator<Item = String>) -> Self {
        Self::from_values(
            names
                .into_iter()
                .filter_map(|name| std::env::var(&name).ok().map(|value| (name, value))),
        )
    }

    pub fn from_values(values: impl IntoIterator<Item = (String, String)>) -> Self {
        Self {
            values: values
                .into_iter()
                .filter(|(name, _)| !name.trim().is_empty())
                .map(|(name, value)| (name.to_ascii_uppercase(), value))
                .collect(),
        }
    }

    pub fn names(&self) -> BTreeSet<String> {
        self.values.keys().cloned().collect()
    }

    fn filtered(&self, allowed: &BTreeSet<String>) -> BTreeMap<String, String> {
        self.values
            .iter()
            .filter(|(name, _)| allowed.contains(*name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }
}

#[derive(Clone)]
pub struct GuardedExecCommandExecutor {
    manager: Arc<ProcessSupervisor>,
    shell: PathBuf,
    runtime_readable_roots: Vec<PathBuf>,
    runtime_readable_files: Vec<PathBuf>,
    environment: CommandEnvironmentSnapshot,
}

#[derive(Clone)]
pub struct GuardedWriteStdinExecutor {
    manager: Arc<ProcessSupervisor>,
}

impl GuardedExecCommandExecutor {
    pub fn new(
        manager: Arc<ProcessSupervisor>,
        shell: impl Into<PathBuf>,
        runtime_readable_roots: impl IntoIterator<Item = PathBuf>,
        runtime_readable_files: impl IntoIterator<Item = PathBuf>,
        environment: CommandEnvironmentSnapshot,
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
        let files = runtime_readable_files
            .into_iter()
            .filter_map(|file| std::fs::canonicalize(file).ok())
            .filter(|file| file.is_file())
            .collect::<BTreeSet<_>>();
        Ok(Self {
            manager,
            shell,
            runtime_readable_roots: roots.into_iter().collect(),
            runtime_readable_files: files.into_iter().collect(),
            environment,
        })
    }
}

impl GuardedWriteStdinExecutor {
    pub fn new(manager: Arc<ProcessSupervisor>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedExecCommandExecutor {
    fn planning_contract(&self) -> Value {
        json!({
            "contract": "orchestral.exec-command-operation-planner/v3",
            "shell": self.shell,
            "runtime_readable_roots": self.runtime_readable_roots,
            "runtime_readable_files": self.runtime_readable_files,
            "environment_names": self.environment.names(),
        })
    }

    fn plan_operation(
        &self,
        invocation: &ToolInvocation,
        descriptor: &ToolDescriptor,
        effective_policy: &orchestral_core::tool_protocol::EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        let Some(cmd) = invocation
            .arguments
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|cmd| !cmd.trim().is_empty())
        else {
            return Err(rejected(
                "exec_command_missing",
                "cmd must be a non-empty string",
            ));
        };
        let readable_roots = canonical_roots(&effective_policy.bounds().filesystem.readable_roots)
            .map_err(|message| rejected("exec_read_root_invalid", message))?;
        let writable_roots = canonical_roots(&effective_policy.bounds().filesystem.writable_roots)
            .map_err(|message| rejected("exec_write_root_invalid", message))?;
        if readable_roots.is_empty() || writable_roots.is_empty() {
            return Err(rejected(
                "exec_workspace_denied",
                "exec_command requires readable and writable workspace roots",
            ));
        }
        let cwd = resolve_workdir(
            invocation.arguments.get("workdir"),
            &readable_roots,
            &writable_roots,
        )
        .map_err(|message| rejected("exec_workdir_invalid", message))?;
        let classification = classify_command(cmd);
        let interactive = invocation
            .arguments
            .get("tty")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let strictly_read_only = classification.read_only && !interactive;
        let mut effect_scopes = BTreeSet::from([
            EffectScope::Process,
            EffectScope::FilesystemRead,
            // Even a read-only command needs the Host-owned runtime temp
            // directory. The executor grants no workspace write access for
            // this class of operation.
            EffectScope::FilesystemWrite,
        ]);
        if !effective_policy
            .bounds()
            .environment
            .allowed_variables
            .is_empty()
        {
            effect_scopes.insert(EffectScope::EnvironmentRead);
        }
        if classification.network && !effective_policy.bounds().network.allowed_targets.is_empty() {
            effect_scopes.insert(EffectScope::Network);
            effect_scopes.insert(EffectScope::ExternalSideEffect);
        }
        let mut targets = BTreeSet::from([
            format!("process-shell:{}", self.shell.display()),
            format!("workdir:{}", cwd.display()),
        ]);
        for root in readable_roots
            .iter()
            .chain(self.runtime_readable_roots.iter())
        {
            targets.insert(format!("read:{}", root.display()));
        }
        for file in &self.runtime_readable_files {
            targets.insert(format!("read:{}", file.display()));
        }
        if strictly_read_only {
            for root in &writable_roots {
                targets.insert(format!("write:{}/.orchestral/tmp", root.display()));
            }
        } else {
            for root in &writable_roots {
                targets.insert(format!("write:{}", root.display()));
            }
            if classification.network {
                for target in &effective_policy.bounds().network.allowed_targets {
                    targets.insert(format!("network:{target}"));
                }
            }
        }
        for name in &effective_policy.bounds().environment.allowed_variables {
            targets.insert(format!("environment:{name}"));
        }
        let operation = ToolOperationPlan {
            effect_scopes,
            targets,
            risk: if classification.destructive {
                ToolOperationRisk::Destructive
            } else if strictly_read_only {
                ToolOperationRisk::Routine
            } else {
                ToolOperationRisk::Elevated
            },
            summary: format!("Execute in workspace sandbox: {}", display_command(cmd)),
        };
        operation
            .validate_envelope(&descriptor.effect_scopes)
            .map_err(|error| rejected("exec_operation_invalid", error.message))?;
        Ok(operation)
    }

    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let cmd = invocation
            .arguments
            .get("cmd")
            .and_then(Value::as_str)
            .unwrap_or("<invalid-command>");
        format!("Execute in workspace sandbox: {}", display_command(cmd))
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
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
        if !bounds.process.interactive.enabled || !bounds.process.interactive.allow_child_processes
        {
            return rejected(
                "exec_interactive_policy_denied",
                "effective policy does not authorize interactive commands and sandboxed descendants",
            );
        }
        let shell_identity = self.shell.to_string_lossy().into_owned();
        if !bounds
            .process
            .interactive
            .command_shells
            .contains(&shell_identity)
        {
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
        let runtime_temp = match prepare_runtime_temp(&writable_roots) {
            Ok(path) => path,
            Err(message) => return failed("exec_runtime_temp", message, false),
        };
        let tty = execution
            .invocation
            .arguments
            .get("tty")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let classification = classify_command(cmd);
        let strictly_read_only = classification.read_only && !tty;
        if strictly_read_only != (execution.operation.risk == ToolOperationRisk::Routine) {
            return rejected(
                "exec_operation_mismatch",
                "planned command risk no longer matches the executable sandbox profile",
            );
        }
        let sandbox_writes = if strictly_read_only {
            vec![runtime_temp.clone()]
        } else {
            writable_roots
        };
        let network_targets = if execution
            .operation
            .effect_scopes
            .contains(&EffectScope::Network)
        {
            bounds.network.allowed_targets.clone()
        } else {
            BTreeSet::new()
        };
        let mut sandbox_reads = readable_roots;
        sandbox_reads.extend(self.runtime_readable_roots.iter().cloned());
        sandbox_reads.push(runtime_temp.clone());
        sandbox_reads.sort();
        sandbox_reads.dedup();
        let sandboxed = match sandbox_command(
            shell_identity,
            shell_arguments(cmd),
            &cwd,
            &ShellSandboxPolicy {
                readable_roots: sandbox_reads,
                readable_files: self.runtime_readable_files.clone(),
                writable_roots: sandbox_writes,
                allow_child_processes: true,
                launcher_programs: vec![self.shell.clone()],
                network_targets,
                linux_bwrap_path: None,
            },
        ) {
            Ok(command) => command,
            Err(message) => return failed("exec_sandbox_setup", message, false),
        };
        let mut environment = if execution
            .operation
            .effect_scopes
            .contains(&EffectScope::EnvironmentRead)
        {
            self.environment
                .filtered(&bounds.environment.allowed_variables)
        } else {
            BTreeMap::new()
        };
        environment.extend(sandboxed.env);
        let runtime_temp = runtime_temp.to_string_lossy().into_owned();
        for name in ["TMPDIR", "TMP", "TEMP"] {
            environment.insert(name.to_owned(), runtime_temp.clone());
        }
        environment.insert("TMPPREFIX".to_owned(), format!("{runtime_temp}/zsh"));
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
                operation: execution.operation.clone(),
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
    fn planning_contract(&self) -> Value {
        json!({
            "contract": "orchestral.write-stdin-operation-planner/v2"
        })
    }

    fn plan_operation(
        &self,
        invocation: &ToolInvocation,
        descriptor: &ToolDescriptor,
        _effective_policy: &orchestral_core::tool_protocol::EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        let Some(session_id) = invocation
            .arguments
            .get("session_id")
            .and_then(Value::as_u64)
        else {
            return Err(rejected(
                "exec_session_missing",
                "session_id must be a positive integer",
            ));
        };
        let has_input = invocation
            .arguments
            .get("chars")
            .and_then(Value::as_str)
            .is_some_and(|chars| !chars.is_empty());
        let session_id = ExecSessionId::new(session_id)
            .map_err(|error| rejected("exec_session_invalid", error.to_string()))?;
        let snapshot = self
            .manager
            .snapshot(&invocation.run_id, session_id)
            .map_err(|error| rejected("exec_session_unavailable", error.to_string()))?;
        if has_input && snapshot.status != ExecSessionStatus::Running {
            return Err(rejected(
                "exec_session_exited",
                "cannot send input to a terminal exec session",
            ));
        }
        let origin = snapshot.operation;
        // Input can trigger only the authority already held by this exact
        // supervised process. A pure poll cannot trigger new process behavior.
        let effect_scopes = if has_input {
            origin.effect_scopes
        } else {
            BTreeSet::from([EffectScope::Process])
        };
        let input_preview = invocation
            .arguments
            .get("chars")
            .and_then(Value::as_str)
            .map(display_payload);
        let mut targets = if has_input {
            origin.targets
        } else {
            BTreeSet::new()
        };
        targets.insert(format!("exec-session:{}", session_id.get()));
        let operation = ToolOperationPlan {
            effect_scopes,
            targets,
            risk: if has_input {
                ToolOperationRisk::Elevated
            } else {
                ToolOperationRisk::Routine
            },
            summary: if let Some(input) = input_preview {
                format!("Send input to exec session {}: {input}", session_id.get())
            } else {
                format!("Poll exec session {}", session_id.get())
            },
        };
        operation
            .validate_envelope(&descriptor.effect_scopes)
            .map_err(|error| rejected("exec_operation_invalid", error.message))?;
        Ok(operation)
    }

    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let session_id = invocation
            .arguments
            .get("session_id")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        match invocation.arguments.get("chars").and_then(Value::as_str) {
            Some(chars) if !chars.is_empty() => format!(
                "Send input to exec session {session_id}: {}",
                display_payload(chars)
            ),
            _ => format!("Poll exec session {session_id}"),
        }
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

/// Safe SDK default: every shell command requires exact Host approval.
pub fn guarded_exec_command_descriptor(mut restriction: ToolRestriction) -> ToolDescriptor {
    restriction.bounds.approval = ApprovalPolicy::Required;
    build_exec_command_descriptor(restriction)
}

/// Interactive CLI profile: the workspace permission policy may auto-run an
/// invocation only after its operation planner selected a constrained routine
/// sandbox. Applications must opt in explicitly.
pub fn workspace_exec_command_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    build_exec_command_descriptor(restriction)
}

fn build_exec_command_descriptor(mut restriction: ToolRestriction) -> ToolDescriptor {
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

/// Safe SDK default: input to a live process requires exact Host approval.
/// Polls use the same static descriptor and therefore inherit this default.
pub fn guarded_write_stdin_descriptor(mut restriction: ToolRestriction) -> ToolDescriptor {
    restriction.bounds.approval = ApprovalPolicy::Required;
    build_write_stdin_descriptor(restriction)
}

/// Interactive CLI profile. Empty polls auto-run; non-empty input is planned
/// as elevated and is reviewed by the workspace permission policy.
pub fn workspace_write_stdin_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    build_write_stdin_descriptor(restriction)
}

fn build_write_stdin_descriptor(mut restriction: ToolRestriction) -> ToolDescriptor {
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
    restriction.bounds.process.interactive.enabled = true;
    restriction.bounds.process.interactive.allow_child_processes = true;
    restriction.bounds.environment.inherit_host_environment = false;
}

fn exec_effects() -> BTreeSet<EffectScope> {
    BTreeSet::from([
        EffectScope::Process,
        EffectScope::Network,
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::EnvironmentRead,
        EffectScope::ExternalSideEffect,
    ])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommandClassification {
    read_only: bool,
    destructive: bool,
    network: bool,
}

/// Conservative lexical classification only affects prompting and display;
/// the mandatory OS sandbox remains the authority boundary. Unknown commands
/// are treated as workspace-mutating, while destructive commands always need
/// explicit review under the interactive workspace policy.
fn classify_command(command: &str) -> CommandClassification {
    let tokens = shell_words::split(command).unwrap_or_else(|_| {
        command
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    });
    let normalized = tokens
        .iter()
        .map(|token| {
            Path::new(token)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(token)
                .to_ascii_lowercase()
        })
        .collect::<Vec<_>>();
    let destructive = normalized.iter().any(|token| {
        matches!(
            token.as_str(),
            "rm" | "rmdir"
                | "unlink"
                | "shred"
                | "truncate"
                | "dd"
                | "mkfs"
                | "kill"
                | "killall"
                | "pkill"
                | "shutdown"
                | "reboot"
                | "sudo"
        )
    }) || git_is_destructive(&normalized)
        || normalized.iter().any(|token| token == "-delete");
    let network = normalized.iter().any(|token| {
        matches!(
            token.as_str(),
            "curl" | "wget" | "ssh" | "scp" | "sftp" | "nc" | "ncat" | "telnet" | "ftp" | "rsync"
        )
    }) || git_uses_network(&normalized);
    let read_only = !destructive
        && !network
        && command_is_simple(command)
        && known_read_only_command(&normalized);
    CommandClassification {
        read_only,
        destructive,
        network,
    }
}

fn command_is_simple(command: &str) -> bool {
    !["\n", "&&", "||", ";", "|", ">", "<", "`", "$("]
        .iter()
        .any(|operator| command.contains(operator))
}

fn known_read_only_command(tokens: &[String]) -> bool {
    let Some(command) = tokens.first().map(String::as_str) else {
        return false;
    };
    match command {
        "cat" | "cut" | "echo" | "head" | "id" | "ls" | "nl" | "pwd" | "stat" | "tail" | "true"
        | "false" | "uname" | "uniq" | "wc" | "which" | "whoami" => true,
        "grep" => !tokens.iter().any(|token| token == "--include-zero"),
        "rg" => !tokens.iter().any(|token| {
            matches!(
                token.as_str(),
                "--pre" | "--hostname-bin" | "--search-zip" | "-z"
            ) || token.starts_with("--pre=")
                || token.starts_with("--hostname-bin=")
        }),
        "find" => !tokens.iter().any(|token| {
            matches!(
                token.as_str(),
                "-delete"
                    | "-exec"
                    | "-execdir"
                    | "-ok"
                    | "-okdir"
                    | "-fls"
                    | "-fprint"
                    | "-fprint0"
                    | "-fprintf"
            )
        }),
        "git" => git_is_read_only(tokens),
        _ => false,
    }
}

fn git_subcommand(tokens: &[String]) -> Option<&str> {
    let git = tokens.iter().position(|token| token == "git")?;
    let mut index = git + 1;
    while let Some(token) = tokens.get(index) {
        match token.as_str() {
            "-C" | "-c" | "--git-dir" | "--work-tree" | "--namespace" => index += 2,
            value if value.starts_with('-') => index += 1,
            value => return Some(value),
        }
    }
    None
}

fn git_is_read_only(tokens: &[String]) -> bool {
    matches!(
        git_subcommand(tokens),
        Some("status" | "log" | "diff" | "show" | "rev-parse" | "ls-files" | "grep")
    ) || matches!(git_subcommand(tokens), Some("branch"))
        && tokens
            .iter()
            .skip_while(|token| token.as_str() != "branch")
            .skip(1)
            .all(|token| {
                matches!(
                    token.as_str(),
                    "--list"
                        | "-l"
                        | "--show-current"
                        | "-a"
                        | "--all"
                        | "-r"
                        | "--remotes"
                        | "-v"
                        | "-vv"
                        | "--verbose"
                ) || token.starts_with("--format=")
            })
}

fn git_is_destructive(tokens: &[String]) -> bool {
    match git_subcommand(tokens) {
        Some("clean") => true,
        Some("reset") => tokens.iter().any(|token| token == "--hard"),
        Some("checkout" | "restore") => tokens.iter().any(|token| token == "--"),
        Some("branch") => tokens
            .iter()
            .any(|token| matches!(token.as_str(), "-d" | "-D")),
        _ => false,
    }
}

fn git_uses_network(tokens: &[String]) -> bool {
    matches!(
        git_subcommand(tokens),
        Some("clone" | "fetch" | "pull" | "push" | "ls-remote" | "submodule")
    )
}

fn display_command(command: &str) -> String {
    display_payload(command)
}

fn display_payload(payload: &str) -> String {
    const HEAD_CHARS: usize = 120;
    const TAIL_CHARS: usize = 80;

    let normalized = payload
        .chars()
        .flat_map(|character| match character {
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            value if value.is_control() => "�".chars().collect::<Vec<_>>(),
            value => vec![value],
        })
        .collect::<Vec<_>>();
    if normalized.len() <= HEAD_CHARS + TAIL_CHARS {
        return normalized.into_iter().collect();
    }
    let omitted = normalized.len() - HEAD_CHARS - TAIL_CHARS;
    let head = normalized.iter().take(HEAD_CHARS).collect::<String>();
    let tail = normalized
        .iter()
        .skip(normalized.len() - TAIL_CHARS)
        .collect::<String>();
    format!("{head} … <{omitted} chars omitted> … {tail}")
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

fn prepare_runtime_temp(writable_roots: &[PathBuf]) -> Result<PathBuf, String> {
    let root = writable_roots
        .first()
        .ok_or_else(|| "exec_command requires one writable root".to_owned())?;
    let state_root = ensure_controlled_directory(root, &root.join(".orchestral"))?;
    let directory = ensure_controlled_directory(root, &state_root.join("tmp"))?;
    Ok(directory)
}

fn ensure_controlled_directory(root: &Path, directory: &Path) -> Result<PathBuf, String> {
    match std::fs::symlink_metadata(directory) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(directory).map_err(|error| {
                format!(
                    "create controlled runtime directory '{}' failed: {error}",
                    directory.display()
                )
            })?;
        }
        Err(error) => {
            return Err(format!(
                "inspect controlled runtime directory '{}' failed: {error}",
                directory.display()
            ));
        }
    }
    let canonical = std::fs::canonicalize(directory).map_err(|error| {
        format!(
            "canonicalize controlled runtime directory '{}' failed: {error}",
            directory.display()
        )
    })?;
    if !canonical.starts_with(root) || !canonical.is_dir() {
        return Err("controlled runtime directory escaped its writable root".to_owned());
    }
    Ok(canonical)
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

#[cfg(test)]
mod tests {
    use super::{
        classify_command, display_payload, guarded_exec_command_descriptor,
        guarded_write_stdin_descriptor, prepare_runtime_temp, workspace_exec_command_descriptor,
        workspace_write_stdin_descriptor,
    };
    use orchestral_core::tool_protocol::{ApprovalPolicy, ToolPolicyBounds, ToolRestriction};

    #[test]
    fn operation_classifier_distinguishes_read_mutating_and_destructive_commands() {
        let read = classify_command("ls -F");
        assert!(read.read_only);
        assert!(!read.destructive);
        assert!(!read.network);

        let build = classify_command("cargo test -p orchestral-runtime");
        assert!(!build.read_only);
        assert!(!build.destructive);
        assert!(!build.network);

        let destructive = classify_command("git reset --hard HEAD~1");
        assert!(destructive.destructive);

        let network = classify_command("curl https://example.com/status");
        assert!(network.network);
    }

    #[test]
    fn read_only_classifier_rejects_shell_composition_that_can_write() {
        assert!(!classify_command("ls > inventory.txt").read_only);
        assert!(!classify_command("find . -delete").read_only);
        assert!(classify_command("find . -delete").destructive);
        assert!(!classify_command("rg TODO | head").read_only);
        assert!(!classify_command("sh -c 'rm -rf target'").read_only);
        assert!(!classify_command("eval 'touch owned'").read_only);
        assert!(!classify_command("sed 'e touch owned' input.txt").read_only);
    }

    #[test]
    fn sdk_descriptors_are_safe_by_default_and_cli_opt_in_is_explicit() {
        let bounds = ToolPolicyBounds {
            approval: ApprovalPolicy::NotRequired,
            ..ToolPolicyBounds::default()
        };
        let restriction = ToolRestriction { bounds };

        assert_eq!(
            guarded_exec_command_descriptor(restriction.clone())
                .restriction
                .bounds
                .approval,
            ApprovalPolicy::Required
        );
        assert_eq!(
            guarded_write_stdin_descriptor(restriction.clone())
                .restriction
                .bounds
                .approval,
            ApprovalPolicy::Required
        );
        assert_eq!(
            workspace_exec_command_descriptor(restriction.clone())
                .restriction
                .bounds
                .approval,
            ApprovalPolicy::NotRequired
        );
        assert_eq!(
            workspace_write_stdin_descriptor(restriction)
                .restriction
                .bounds
                .approval,
            ApprovalPolicy::NotRequired
        );
    }

    #[test]
    fn approval_preview_keeps_the_dangerous_tail_and_marks_omissions() {
        let payload = format!("{}rm -rf important", "safe ".repeat(80));
        let preview = display_payload(&payload);
        assert!(preview.contains("chars omitted"));
        assert!(preview.ends_with("rm -rf important"));
    }

    fn temporary_root(label: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "orchestral-guarded-exec-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).expect("create guarded exec test root");
        std::fs::canonicalize(root).expect("canonical guarded exec test root")
    }

    #[test]
    fn controlled_runtime_temp_is_created_inside_the_writable_root() {
        let root = temporary_root("runtime-temp");
        let directory = prepare_runtime_temp(std::slice::from_ref(&root))
            .expect("prepare controlled runtime temp");
        assert_eq!(directory, root.join(".orchestral/tmp"));
        assert!(directory.is_dir());
        std::fs::remove_dir_all(root).expect("remove guarded exec test root");
    }

    #[cfg(unix)]
    #[test]
    fn controlled_runtime_temp_rejects_a_symlink_escape_before_writing() {
        let root = temporary_root("runtime-temp-escape");
        let outside = temporary_root("runtime-temp-outside");
        std::os::unix::fs::symlink(&outside, root.join(".orchestral"))
            .expect("create state root escape symlink");

        let error = prepare_runtime_temp(std::slice::from_ref(&root))
            .expect_err("runtime temp symlink escape must be rejected");
        assert!(error.contains("escaped its writable root"), "{error}");
        assert!(!outside.join("tmp").exists());

        std::fs::remove_dir_all(root).expect("remove guarded exec escape root");
        std::fs::remove_dir_all(outside).expect("remove guarded exec outside root");
    }
}
