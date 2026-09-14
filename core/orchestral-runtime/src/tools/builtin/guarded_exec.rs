//! Unified model-facing command execution Tools.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::ToolActivityEvidence;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, CapabilityRequest, CapabilitySelector, EffectScope, ModelToolSchema,
    ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency, ToolInvocation, ToolOperationPlan,
    ToolOperationRisk, ToolOutcome, ToolRestriction,
};
use serde_json::{json, Map, Value};

use crate::exec_process::{
    ExecPollResult, ExecProcessError, ExecSessionId, ExecSessionStatus, ExecSpawnSpec,
    ExecWaitMode, ExecWaitOptions, ProcessSupervisor,
};
use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};
use crate::tools::shell_sandbox::{sandbox_command, SandboxNetworkAccess, ShellSandboxPolicy};

use super::support::canonical_roots;

pub const GUARDED_EXEC_SANDBOX_PROFILE: &str = "orchestral.exec_command.v1";
const DEFAULT_SANDBOX_PERMISSION: &str = "use_default";
const REQUIRE_ESCALATED_PERMISSION: &str = "require_escalated";

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
    sandboxed_execution_enabled: bool,
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
            sandboxed_execution_enabled: true,
        })
    }

    /// Restrict this executor to explicit, approved Host execution when false.
    /// No request silently escalates because the workspace sandbox is disabled.
    pub fn with_sandboxed_execution_enabled(mut self, enabled: bool) -> Self {
        self.sandboxed_execution_enabled = enabled;
        self
    }

    fn requested_host_execution(&self, arguments: &Value) -> Result<bool, ToolOutcome> {
        let requested = requested_host_execution(arguments)?;
        if !self.sandboxed_execution_enabled && !requested {
            return Err(rejected(
                "exec_default_sandbox_disabled",
                "This Host offers only approved command execution; explicitly request sandbox_permissions='require_escalated' with a justification",
            ));
        }
        Ok(requested)
    }

    fn workspace_roots(&self, roots: &BTreeSet<String>) -> Result<Vec<PathBuf>, String> {
        let temp_root = self.manager.runtime_temp_root();
        let roots = canonical_roots(
            &roots
                .iter()
                .filter(|root| Path::new(root.as_str()) != temp_root)
                .cloned()
                .collect(),
        )?;
        if roots
            .iter()
            .any(|root| temp_root.starts_with(root) || root.starts_with(temp_root))
        {
            return Err("Host runtime temp root must be separate from workspace roots".into());
        }
        Ok(roots)
    }
}

impl GuardedWriteStdinExecutor {
    pub fn new(manager: Arc<ProcessSupervisor>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedExecCommandExecutor {
    fn model_output_contract(&self) -> Value {
        super::model_output::contract("exec")
    }

    fn project_model_output(&self, _invocation: &ToolInvocation, output: &Value) -> Value {
        super::model_output::exec(output)
    }

    fn planning_contract(&self) -> Value {
        json!({
            "contract": "orchestral.exec-command-operation-planner/v6",
            "sandboxed_execution_enabled": self.sandboxed_execution_enabled,
            "shell": self.shell,
            "runtime_readable_roots": self.runtime_readable_roots,
            "runtime_readable_files": self.runtime_readable_files,
            "environment_names": self.environment.names(),
            "runtime_temp_root": self.manager.runtime_temp_root(),
        })
    }

    fn activity_evidence(
        &self,
        invocation: &ToolInvocation,
        _outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        invocation
            .arguments
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|command| !command.trim().is_empty())
            .map(display_command)
            .map(|command| ToolActivityEvidence::Command { command })
            .into_iter()
            .collect()
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
        let explicitly_requested_host_execution =
            self.requested_host_execution(&invocation.arguments)?;
        let justification =
            escalation_justification(&invocation.arguments, explicitly_requested_host_execution)?;
        let readable_roots = self
            .workspace_roots(&effective_policy.bounds().filesystem.readable_roots)
            .map_err(|message| rejected("exec_read_root_invalid", message))?;
        let writable_roots = self
            .workspace_roots(&effective_policy.bounds().filesystem.writable_roots)
            .map_err(|message| rejected("exec_write_root_invalid", message))?;
        if readable_roots.is_empty() || writable_roots.is_empty() {
            return Err(rejected(
                "exec_workspace_denied",
                "exec_command requires readable and writable workspace roots",
            ));
        }
        let (host_execution, sandboxed_cwd, implicit_escalation_reason) =
            if explicitly_requested_host_execution {
                validate_host_workdir_argument(invocation.arguments.get("workdir"))?;
                (true, None, None)
            } else {
                match resolve_workdir(
                    invocation.arguments.get("workdir"),
                    &readable_roots,
                    &writable_roots,
                ) {
                    Ok(cwd) => (false, Some(cwd), None),
                    Err(WorkdirResolutionError::Outside(cwd)) => (
                        true,
                        None,
                        Some(format!(
                            "Requested workdir is outside the configured workspace roots: {}",
                            cwd.display()
                        )),
                    ),
                    Err(WorkdirResolutionError::Invalid(message)) => {
                        return Err(rejected("exec_workdir_invalid", message));
                    }
                }
            };
        if host_execution
            && !effective_policy
                .bounds()
                .allowed_effects
                .contains(&EffectScope::HostExecution)
        {
            return Err(rejected(
                "exec_host_execution_denied",
                "Host configuration does not permit execution outside the default sandbox",
            ));
        };
        let classification = classify_command(cmd);
        let interactive = invocation
            .arguments
            .get("tty")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let strictly_read_only = classification.read_only && !interactive;
        let mut required_capabilities = CapabilityRequest::from_effects(BTreeSet::from([
            EffectScope::Process,
            EffectScope::FilesystemRead,
            // Even a read-only command needs the Host-owned runtime temp
            // directory. The executor grants no workspace write access for
            // this class of operation.
            EffectScope::FilesystemWrite,
        ]));
        required_capabilities.insert_resource(
            EffectScope::Process,
            CapabilitySelector::Exact(self.shell.to_string_lossy().into_owned()),
        );
        if let Some(cwd) = &sandboxed_cwd {
            required_capabilities.insert_resource(
                EffectScope::FilesystemRead,
                CapabilitySelector::Exact(cwd.to_string_lossy().into_owned()),
            );
        }
        if !effective_policy
            .bounds()
            .environment
            .allowed_variables
            .is_empty()
        {
            required_capabilities
                .effects
                .insert(EffectScope::EnvironmentRead);
        }
        if host_execution {
            // Host execution intentionally leaves the default OS sandbox. Its
            // broad authority is represented explicitly instead of silently
            // widening workspace selectors during execution.
            required_capabilities
                .effects
                .insert(EffectScope::HostExecution);
            required_capabilities.insert_resource(
                EffectScope::FilesystemRead,
                CapabilitySelector::Unrestricted,
            );
            required_capabilities.insert_resource(
                EffectScope::FilesystemWrite,
                CapabilitySelector::Unrestricted,
            );
            required_capabilities
                .insert_resource(EffectScope::Network, CapabilitySelector::Unrestricted);
            required_capabilities.insert_resource(
                EffectScope::ExternalSideEffect,
                CapabilitySelector::Unrestricted,
            );
        } else if classification.network {
            let network = &effective_policy.bounds().network;
            if network.allow_unrestricted {
                required_capabilities
                    .insert_resource(EffectScope::Network, CapabilitySelector::Unrestricted);
            } else if network.allowed_targets.is_empty() {
                return Err(rejected(
                    "exec_network_denied",
                    "Host configuration does not permit network access for this command",
                ));
            } else {
                for target in &network.allowed_targets {
                    required_capabilities.insert_resource(
                        EffectScope::Network,
                        CapabilitySelector::Exact(target.clone()),
                    );
                }
            }
            required_capabilities.insert_resource(
                EffectScope::ExternalSideEffect,
                CapabilitySelector::Unrestricted,
            );
        }
        // Runtime/toolchain roots are sealed Host dependencies captured by
        // this executor's planning contract. They are not invocation-selected
        // authority and therefore do not belong in the user's per-operation
        // lease. The sandbox still materializes those fixed read-only roots.
        if !host_execution {
            for root in &readable_roots {
                required_capabilities.insert_resource(
                    EffectScope::FilesystemRead,
                    CapabilitySelector::Subtree(root.to_string_lossy().into_owned()),
                );
            }
            if !strictly_read_only {
                for root in &writable_roots {
                    required_capabilities.insert_resource(
                        EffectScope::FilesystemWrite,
                        CapabilitySelector::Subtree(root.to_string_lossy().into_owned()),
                    );
                }
            }
            let runtime_temp = self.manager.runtime_temp_path(&invocation.run_id);
            for effect in [EffectScope::FilesystemRead, EffectScope::FilesystemWrite] {
                required_capabilities.insert_resource(
                    effect,
                    CapabilitySelector::Subtree(runtime_temp.to_string_lossy().into_owned()),
                );
            }
        }
        for name in &effective_policy.bounds().environment.allowed_variables {
            required_capabilities.insert_resource(
                EffectScope::EnvironmentRead,
                CapabilitySelector::Exact(name.clone()),
            );
        }
        let risk = if classification.destructive {
            ToolOperationRisk::Destructive
        } else if host_execution || classification.network {
            ToolOperationRisk::Elevated
        } else if strictly_read_only {
            ToolOperationRisk::Routine
        } else {
            ToolOperationRisk::Elevated
        };
        let summary = if host_execution {
            host_execution_summary(
                &invocation.arguments,
                cmd,
                justification
                    .or(implicit_escalation_reason.as_deref())
                    .unwrap_or("Host execution requested"),
            )
        } else {
            format!("Execute in workspace sandbox: {}", display_command(cmd))
        };
        let operation = ToolOperationPlan {
            required_capabilities,
            risk,
            session_approval_scope: None,
            summary,
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
        if requested_host_execution(&invocation.arguments).unwrap_or(false) {
            let reason = invocation
                .arguments
                .get("justification")
                .and_then(Value::as_str)
                .unwrap_or("No justification supplied");
            host_execution_summary(&invocation.arguments, cmd, reason)
        } else {
            format!("Execute in workspace sandbox: {}", display_command(cmd))
        }
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let explicitly_requested_host_execution =
            match self.requested_host_execution(&execution.invocation.arguments) {
                Ok(value) => value,
                Err(outcome) => return outcome,
            };
        if let Err(outcome) = escalation_justification(
            &execution.invocation.arguments,
            explicitly_requested_host_execution,
        ) {
            return outcome;
        }
        let host_execution = execution
            .operation
            .required_capabilities
            .requires(EffectScope::HostExecution);
        if explicitly_requested_host_execution && !host_execution {
            return rejected(
                "exec_operation_mismatch",
                "planned execution no longer matches the invocation's Host execution request",
            );
        }
        if host_execution
            && (!host_execution_capabilities_are_complete(execution.lease.granted())
                || !execution.lease.was_approved())
        {
            return rejected(
                "exec_escalation_not_approved",
                "Host execution requires an exact verified approval capability",
            );
        }
        let bounds = execution.effective_policy.bounds();
        if !host_execution
            && (!bounds.sandbox.required
                || !bounds
                    .sandbox
                    .allowed_profiles
                    .contains(GUARDED_EXEC_SANDBOX_PROFILE))
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
        let readable_roots = match self.workspace_roots(&bounds.filesystem.readable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => return rejected("exec_read_root_denied", "no readable workspace root"),
            Err(message) => return rejected("exec_read_root_invalid", message),
        };
        let writable_roots = match self.workspace_roots(&bounds.filesystem.writable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => return rejected("exec_write_root_denied", "no writable workspace root"),
            Err(message) => return rejected("exec_write_root_invalid", message),
        };
        let cwd = match if host_execution {
            resolve_host_workdir(
                execution.invocation.arguments.get("workdir"),
                &readable_roots,
                &writable_roots,
            )
        } else {
            resolve_workdir(
                execution.invocation.arguments.get("workdir"),
                &readable_roots,
                &writable_roots,
            )
            .map_err(WorkdirResolutionError::into_message)
        } {
            Ok(cwd) => cwd,
            Err(message) => return rejected("exec_workdir_invalid", message),
        };
        let runtime_temp = match self.manager.prepare_runtime_temp(
            &execution.invocation.run_id,
            execution.run_cancellation.clone(),
        ) {
            Ok(path) => path,
            Err(error) => return exec_error(error),
        };
        let tty = execution
            .invocation
            .arguments
            .get("tty")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mode = match wait_mode(&execution.invocation.arguments, tty, false) {
            Ok(mode) => mode,
            Err(outcome) => return outcome,
        };
        let classification = classify_command(cmd);
        let strictly_read_only = classification.read_only && !tty;
        let expected_risk = if classification.destructive {
            ToolOperationRisk::Destructive
        } else if host_execution || classification.network {
            ToolOperationRisk::Elevated
        } else if strictly_read_only {
            ToolOperationRisk::Routine
        } else {
            ToolOperationRisk::Elevated
        };
        if execution.operation.risk != expected_risk {
            return rejected(
                "exec_operation_mismatch",
                "planned command risk no longer matches the executable sandbox profile",
            );
        }
        let (program, args, sandbox_environment, backend_starts_new_session, backend) =
            if host_execution {
                (
                    shell_identity,
                    shell_arguments(&self.shell, cmd),
                    BTreeMap::new(),
                    false,
                    "host-approved".to_owned(),
                )
            } else {
                let mut sandbox_writes = if strictly_read_only {
                    Vec::new()
                } else {
                    writable_roots
                };
                sandbox_writes.push(runtime_temp.clone());
                let network = match sandbox_network_access(execution.lease.granted()) {
                    Ok(network) => network,
                    Err(outcome) => return outcome,
                };
                let mut sandbox_reads = readable_roots;
                sandbox_reads.extend(self.runtime_readable_roots.iter().cloned());
                sandbox_reads.push(runtime_temp.clone());
                sandbox_reads.sort();
                sandbox_reads.dedup();
                let sandboxed = match sandbox_command(
                    shell_identity,
                    shell_arguments(&self.shell, cmd),
                    &cwd,
                    &ShellSandboxPolicy {
                        readable_roots: sandbox_reads,
                        readable_files: self.runtime_readable_files.clone(),
                        writable_roots: sandbox_writes,
                        allow_child_processes: true,
                        allow_host_ui: false,
                        launcher_programs: vec![self.shell.clone()],
                        network,
                        linux_bwrap_path: None,
                    },
                ) {
                    Ok(command) => command,
                    Err(message) => return failed("exec_sandbox_setup", message, false),
                };
                (
                    sandboxed.program,
                    sandboxed.args,
                    sandboxed.env.into_iter().collect::<BTreeMap<_, _>>(),
                    sandboxed.backend_starts_new_session,
                    sandboxed.backend.to_owned(),
                )
            };
        let mut environment = if execution
            .lease
            .granted()
            .requires(EffectScope::EnvironmentRead)
        {
            self.environment
                .filtered(&bounds.environment.allowed_variables)
        } else {
            BTreeMap::new()
        };
        environment.extend(sandbox_environment);
        let runtime_temp = runtime_temp.to_string_lossy().into_owned();
        for name in ["TMPDIR", "TMP", "TEMP"] {
            environment.insert(name.to_owned(), runtime_temp.clone());
        }
        environment.insert("TMPPREFIX".to_owned(), format!("{runtime_temp}/zsh"));
        let wait = command_wait(&execution.invocation.arguments, bounds.max_timeout_ms, mode);
        let max_output_bytes =
            output_byte_limit(&execution.invocation.arguments, bounds.max_output_bytes);
        let session_id = match self
            .manager
            .spawn(ExecSpawnSpec {
                run_id: execution.invocation.run_id.clone(),
                program,
                args,
                cwd,
                environment,
                tty,
                backend_starts_new_session,
                operation: execution.operation.clone(),
            })
            .await
        {
            Ok(session_id) => session_id,
            Err(error) => return exec_error(error),
        };

        let manager = Arc::downgrade(&self.manager);
        let run_id = execution.invocation.run_id.clone();
        let cancellation = execution.cancellation.clone();
        tokio::spawn(async move {
            cancellation.cancelled().await;
            if let Some(manager) = manager.upgrade() {
                let _ = manager.close(&run_id, session_id).await;
            }
        });

        let result = match self
            .manager
            .write_and_poll_with_options(
                &execution.invocation.run_id,
                session_id,
                None,
                ExecWaitOptions {
                    duration: observation_window(wait, execution.deadline),
                    mode,
                    yield_requested: execution.yield_requested.clone(),
                },
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
            output: render_result(result, session_id, max_output_bytes, &backend).into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedWriteStdinExecutor {
    fn model_output_contract(&self) -> Value {
        super::model_output::contract("exec")
    }

    fn project_model_output(&self, _invocation: &ToolInvocation, output: &Value) -> Value {
        super::model_output::exec(output)
    }

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
        let session_is_running = snapshot.status == ExecSessionStatus::Running;
        let session_has_exited = matches!(snapshot.status, ExecSessionStatus::Exited { .. });
        if has_input && !session_is_running && !session_has_exited {
            return Err(rejected(
                "exec_session_terminal",
                "cannot send input because the exec session is no longer running",
            ));
        }
        let origin = snapshot.operation;
        // Input can trigger only the authority already held by this exact
        // supervised process. A pure poll, including observation after an exit
        // race, cannot trigger new process behavior.
        let input_will_be_delivered = has_input && session_is_running;
        let mut required_capabilities = if input_will_be_delivered {
            origin.required_capabilities
        } else {
            CapabilityRequest::from_effects(BTreeSet::from([EffectScope::Process]))
        };
        let input_preview = invocation
            .arguments
            .get("chars")
            .and_then(Value::as_str)
            .map(display_payload);
        if input_will_be_delivered {
            required_capabilities.insert_resource(
                EffectScope::ExternalSideEffect,
                CapabilitySelector::Exact(format!("exec-session:{}", session_id.get())),
            );
        }
        let operation = ToolOperationPlan {
            required_capabilities,
            risk: if input_will_be_delivered {
                ToolOperationRisk::Elevated
            } else {
                ToolOperationRisk::Routine
            },
            session_approval_scope: None,
            summary: if has_input && session_has_exited {
                format!(
                    "Read final output from exited exec session {}; requested input will not be delivered",
                    session_id.get()
                )
            } else if let Some(input) = input_preview {
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
        let snapshot = match self
            .manager
            .snapshot(&execution.invocation.run_id, session_id)
        {
            Ok(snapshot) => snapshot,
            Err(error) => return exec_error(error),
        };
        let mode = match wait_mode(
            &execution.invocation.arguments,
            snapshot.tty,
            input.is_some_and(|value| !value.is_empty()),
        ) {
            Ok(mode) => mode,
            Err(outcome) => return outcome,
        };
        let wait = bounded_wait(
            &execution.invocation.arguments,
            bounds.max_timeout_ms,
            match mode {
                ExecWaitMode::Output => 5_000,
                ExecWaitMode::Completion => 30_000,
            },
        );
        let max_output_bytes =
            output_byte_limit(&execution.invocation.arguments, bounds.max_output_bytes);
        match self
            .manager
            .write_and_poll_with_options(
                &execution.invocation.run_id,
                session_id,
                input,
                ExecWaitOptions {
                    duration: observation_window(wait, execution.deadline),
                    mode,
                    yield_requested: execution.yield_requested.clone(),
                },
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
    build_exec_command_descriptor(restriction, true)
}

/// Exposes only explicit Host execution with exact approval. Pair with an
/// executor whose `sandboxed_execution_enabled` is false. Suitable for Hosts
/// providing their own isolation; this descriptor does not grant authority.
pub fn approved_host_exec_command_descriptor(mut restriction: ToolRestriction) -> ToolDescriptor {
    restriction.bounds.approval = ApprovalPolicy::Required;
    build_exec_command_descriptor(restriction, false)
}

/// Interactive CLI profile: the workspace permission policy may auto-run an
/// invocation only after its operation planner selected a constrained routine
/// sandbox. Applications must opt in explicitly.
pub fn workspace_exec_command_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    build_exec_command_descriptor(restriction, true)
}

fn build_exec_command_descriptor(
    mut restriction: ToolRestriction,
    sandboxed_execution_enabled: bool,
) -> ToolDescriptor {
    apply_exec_restriction(&mut restriction);
    let effect_scopes = restricted_exec_effects(&restriction);
    let host_execution_available = effect_scopes.contains(&EffectScope::HostExecution);
    let mut input_schema = json!({
        "type": "object",
        "required": if sandboxed_execution_enabled { vec!["cmd"] } else { vec!["cmd", "sandbox_permissions", "justification"] },
        "properties": {
            "cmd": { "type": "string", "minLength": 1 },
            "workdir": { "type": "string", "minLength": 1 },
            "tty": { "type": "boolean" },
            "wait_mode": wait_mode_schema(),
            "yield_time_ms": { "type": "integer", "minimum": 1 },
            "max_output_tokens": { "type": "integer", "minimum": 1 },
            "sandbox_permissions": {
                "type": "string",
                "enum": if sandboxed_execution_enabled { vec!["use_default", "require_escalated"] } else { vec!["require_escalated"] }
            },
            "justification": { "type": "string", "minLength": 1 }
        },
        "additionalProperties": false
    });
    if sandboxed_execution_enabled && !host_execution_available {
        let properties = input_schema["properties"].as_object_mut().unwrap();
        properties.remove("sandbox_permissions");
        properties.remove("justification");
    }
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/exec_command/v2"),
        model_schema: ModelToolSchema {
            name: "exec_command".to_owned(),
            description: format!("{}{} {}", if sandboxed_execution_enabled && !host_execution_available {
                "Run a shell command in the workspace sandbox."
            } else if sandboxed_execution_enabled {
                concat!(
                    "Run a shell command. Commands use the workspace sandbox by default. ",
                    "When that sandbox prevents an operation the user requested, retry with ",
                    "sandbox_permissions='require_escalated' and a concise justification; ",
                    "the Host will decide whether to ask the user."
                )
            } else {
                concat!(
                    "Run a shell command after exact Host approval. This Host does not offer ",
                    "the workspace sandbox. Set sandbox_permissions='require_escalated' and ",
                    "provide a concise justification on every command."
                )
            }, if host_execution_available || !sandboxed_execution_enabled {
                " Never tell the user to run the command manually merely because escalation is required."
            } else {
                ""
            }, concat!(
                "Non-TTY defaults to completion (up to 60s); TTY defaults to output (up to 10s). ",
                "yield_time_ms changes the observation window, not process lifetime. ",
                "Running commands return session_id for write_stdin. Output shares a stdout/stderr ",
                "budget and retains each stream's beginning/end where space permits. ",
                "Preserve validation commands' own exit status; do not pipe through tail or filters ",
                "just to shorten output."
            )),
            input_schema,
        },
        output_schema: exec_output_schema(),
        effect_scopes,
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
    let effect_scopes = restricted_exec_effects(&restriction);
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/write_stdin/v2"),
        model_schema: ModelToolSchema {
            name: "write_stdin".to_owned(),
            description: concat!(
                "Send input to an exec session, or omit chars to poll. Polls return only new output. ",
                "If the process exits before input can be delivered, the call returns its final ",
                "output and exit status instead of failing. Wait defaults: empty non-TTY polls use ",
                "completion (30s); TTY/input uses output. yield_time_ms changes the observation window, ",
                "not process lifetime. Output shares a stdout/stderr budget and retains each stream's ",
                "beginning/end where space permits."
            )
            .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["session_id"],
                "properties": {
                    "session_id": { "type": "integer", "minimum": 1 },
                    "chars": { "type": "string" },
                    "wait_mode": wait_mode_schema(),
                    "yield_time_ms": { "type": "integer", "minimum": 1 },
                    "max_output_tokens": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }),
        },
        output_schema: exec_output_schema(),
        effect_scopes,
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
        EffectScope::HostExecution,
    ])
}

fn restricted_exec_effects(restriction: &ToolRestriction) -> BTreeSet<EffectScope> {
    exec_effects()
        .intersection(&restriction.bounds.allowed_effects)
        .copied()
        .collect()
}

fn requested_host_execution(arguments: &Value) -> Result<bool, ToolOutcome> {
    match arguments
        .get("sandbox_permissions")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_SANDBOX_PERMISSION)
    {
        DEFAULT_SANDBOX_PERMISSION => Ok(false),
        REQUIRE_ESCALATED_PERMISSION => Ok(true),
        _ => Err(rejected(
            "exec_sandbox_permissions_invalid",
            "sandbox_permissions must be 'use_default' or 'require_escalated'",
        )),
    }
}

fn escalation_justification(
    arguments: &Value,
    host_execution: bool,
) -> Result<Option<&str>, ToolOutcome> {
    let justification = arguments
        .get("justification")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if host_execution && justification.is_none() {
        return Err(rejected(
            "exec_escalation_justification_missing",
            "require_escalated needs a concise non-empty justification for Host review",
        ));
    }
    Ok(justification)
}

fn host_execution_capabilities_are_complete(request: &CapabilityRequest) -> bool {
    [
        EffectScope::HostExecution,
        EffectScope::Network,
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::ExternalSideEffect,
    ]
    .into_iter()
    .all(|effect| request.requires(effect))
        && [
            EffectScope::Network,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::ExternalSideEffect,
        ]
        .into_iter()
        .all(|effect| {
            request
                .resources_for(effect)
                .any(|selector| matches!(selector, CapabilitySelector::Unrestricted))
        })
}

fn host_execution_summary(arguments: &Value, command: &str, justification: &str) -> String {
    let workdir = arguments
        .get("workdir")
        .and_then(Value::as_str)
        .map(display_payload);
    match workdir {
        Some(workdir) => format!(
            "Execute outside the workspace sandbox (workdir: {workdir}): {}; Reason: {}",
            display_command(command),
            display_payload(justification)
        ),
        None => format!(
            "Execute outside the workspace sandbox: {}; Reason: {}",
            display_command(command),
            display_payload(justification)
        ),
    }
}

fn sandbox_network_access(
    request: &CapabilityRequest,
) -> Result<SandboxNetworkAccess, ToolOutcome> {
    if !request.requires(EffectScope::Network) {
        return Ok(SandboxNetworkAccess::Disabled);
    }
    let mut exact = BTreeSet::new();
    for selector in request.resources_for(EffectScope::Network) {
        match selector {
            CapabilitySelector::Exact(target) => {
                exact.insert(target.clone());
            }
            CapabilitySelector::Unrestricted => return Ok(SandboxNetworkAccess::Unrestricted),
            _ => {
                return Err(rejected(
                    "exec_network_lease_invalid",
                    "network lease cannot be materialized by the process sandbox",
                ))
            }
        }
    }
    if exact.is_empty() {
        Err(rejected(
            "exec_network_lease_missing",
            "network effect has no granted network resource",
        ))
    } else {
        Ok(SandboxNetworkAccess::ExactTargets(exact))
    }
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
        "required": ["stdout", "stderr", "alive", "wall_time_seconds", "truncated", "dropped_bytes", "sandbox_backend"],
        "properties": {
            "stdout": { "type": "string" },
            "stderr": { "type": "string" },
            "alive": { "type": "boolean" },
            "exit_code": { "type": "integer" },
            "session_id": { "type": "integer" },
            "wall_time_seconds": { "type": "number" },
            "truncated": { "type": "boolean" },
            "dropped_bytes": {
                "type": "integer",
                "description": "Bytes omitted by capture limits or by shortening the decoded output."
            },
            "sandbox_backend": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn validate_host_workdir_argument(value: Option<&Value>) -> Result<(), ToolOutcome> {
    match value {
        Some(Value::String(path)) if !path.trim().is_empty() => Ok(()),
        Some(_) => Err(rejected(
            "exec_workdir_invalid",
            "workdir must be a non-empty string",
        )),
        None => Ok(()),
    }
}

#[derive(Debug)]
enum WorkdirResolutionError {
    Invalid(String),
    Outside(PathBuf),
}

impl WorkdirResolutionError {
    fn into_message(self) -> String {
        match self {
            Self::Invalid(message) => message,
            Self::Outside(cwd) => format!(
                "workdir is outside the Host-approved workspace: {}",
                cwd.display()
            ),
        }
    }
}

fn resolve_workdir(
    value: Option<&Value>,
    readable_roots: &[PathBuf],
    writable_roots: &[PathBuf],
) -> Result<PathBuf, WorkdirResolutionError> {
    let base = writable_roots
        .first()
        .or_else(|| readable_roots.first())
        .ok_or_else(|| {
            WorkdirResolutionError::Invalid("no workspace root is available".to_owned())
        })?;
    let requested = match value {
        Some(Value::String(path)) if !path.trim().is_empty() => {
            let path = Path::new(path);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                base.join(path)
            }
        }
        Some(_) => {
            return Err(WorkdirResolutionError::Invalid(
                "workdir must be a non-empty string".to_owned(),
            ));
        }
        None => base.clone(),
    };
    let cwd = std::fs::canonicalize(&requested).map_err(|error| {
        WorkdirResolutionError::Invalid(format!(
            "resolve workdir '{}': {error}",
            requested.display()
        ))
    })?;
    if !cwd.is_dir() {
        return Err(WorkdirResolutionError::Invalid(format!(
            "workdir is not a directory: {}",
            cwd.display()
        )));
    }
    if !writable_roots.iter().any(|root| cwd.starts_with(root)) {
        return Err(WorkdirResolutionError::Outside(cwd));
    }
    Ok(cwd)
}

fn resolve_host_workdir(
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
    Ok(cwd)
}

#[cfg(unix)]
fn shell_arguments(_shell: &Path, cmd: &str) -> Vec<String> {
    vec!["-c".to_owned(), cmd.to_owned()]
}

#[cfg(windows)]
fn shell_arguments(shell: &Path, cmd: &str) -> Vec<String> {
    match shell
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "powershell" | "pwsh" => vec![
            "-NoLogo".to_owned(),
            "-NoProfile".to_owned(),
            "-Command".to_owned(),
            format!("[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; {cmd}"),
        ],
        "cmd" => vec![
            "/D".to_owned(),
            "/S".to_owned(),
            "/C".to_owned(),
            cmd.to_owned(),
        ],
        _ => vec!["-c".to_owned(), cmd.to_owned()],
    }
}

fn wait_mode_schema() -> Value {
    json!({
        "type": "string",
        "enum": ["output", "completion"],
        "description": "output yields after an output pause; completion waits for exit/deadline. Host input can yield either; neither terminates the process."
    })
}

fn wait_mode(arguments: &Value, tty: bool, has_input: bool) -> Result<ExecWaitMode, ToolOutcome> {
    match arguments.get("wait_mode") {
        None => Ok(ExecWaitMode::for_interaction(tty, has_input)),
        Some(Value::String(value)) if value == "output" => Ok(ExecWaitMode::Output),
        Some(Value::String(value)) if value == "completion" => Ok(ExecWaitMode::Completion),
        Some(_) => Err(rejected(
            "exec_wait_mode_invalid",
            "wait_mode must be 'output' or 'completion'",
        )),
    }
}

fn command_wait(arguments: &Value, maximum_ms: Option<u64>, mode: ExecWaitMode) -> Duration {
    bounded_wait(
        arguments,
        maximum_ms,
        match mode {
            ExecWaitMode::Completion => 60_000,
            ExecWaitMode::Output => 10_000,
        },
    )
}

fn bounded_wait(arguments: &Value, maximum_ms: Option<u64>, default_ms: u64) -> Duration {
    let requested = arguments
        .get("yield_time_ms")
        .and_then(Value::as_u64)
        .unwrap_or(default_ms)
        .max(1);
    Duration::from_millis(requested.min(maximum_ms.unwrap_or(requested)))
}

fn observation_window(requested: Duration, deadline: Option<tokio::time::Instant>) -> Duration {
    // Reserve a short interval for draining and returning the observation.
    // Otherwise a wait equal to the Host bound races the outer tool timeout
    // and misclassifies a still-running session as an unknown effect.
    let remaining = deadline.map(|deadline| {
        deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .saturating_sub(Duration::from_millis(50))
            .max(Duration::from_millis(1))
    });
    remaining.map_or(requested, |remaining| requested.min(remaining))
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
    // Reserve space for both streams. A short or absent stream gives its unused
    // share to the other; verbose stdout must not hide stderr diagnostics.
    let stdout_budget = result.stdout.len().min(max_output_bytes / 2);
    let stderr_budget = result.stderr.len().min(max_output_bytes - stdout_budget);
    let stdout_budget = result.stdout.len().min(max_output_bytes - stderr_budget);
    let (stdout, stdout_omitted) = shorten_exec_output(&result.stdout, stdout_budget);
    let (stderr, stderr_omitted) = shorten_exec_output(&result.stderr, stderr_budget);
    let dropped_bytes = result
        .dropped_bytes
        .saturating_add(stdout_omitted)
        .saturating_add(stderr_omitted);
    let mut value = Map::from_iter([
        ("stdout".to_owned(), json!(stdout)),
        ("stderr".to_owned(), json!(stderr)),
        ("alive".to_owned(), json!(result.alive)),
        (
            "wall_time_seconds".to_owned(),
            json!(result.wall_time_seconds),
        ),
        ("truncated".to_owned(), json!(dropped_bytes > 0)),
        ("dropped_bytes".to_owned(), json!(dropped_bytes)),
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

fn shorten_exec_output(text: &str, max_bytes: usize) -> (String, u64) {
    if text.len() <= max_bytes {
        return (text.to_owned(), 0);
    }
    const OMITTED: &str = "\n[... omitted ...]\n";
    // Tiny budgets cannot fit both framing and useful text. Return a suffix;
    // the enclosing result still carries the truncation flag and omitted count.
    let (head_budget, tail_budget) = if max_bytes > OMITTED.len() + 1 {
        let content_budget = max_bytes - OMITTED.len();
        (content_budget / 2, content_budget - content_budget / 2)
    } else {
        (0, max_bytes)
    };
    let mut head_end = head_budget;
    while !text.is_char_boundary(head_end) {
        head_end -= 1;
    }
    let mut tail_start = text.len() - tail_budget;
    while !text.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    let head = &text[..head_end];
    let tail = &text[tail_start..];
    let omitted = text.len() - head.len() - tail.len();
    let output = if head_budget > 0 {
        format!("{head}{OMITTED}{tail}")
    } else {
        tail.to_owned()
    };
    (output, omitted as u64)
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
        approved_host_exec_command_descriptor, classify_command, display_payload, exec_effects,
        guarded_exec_command_descriptor, guarded_write_stdin_descriptor, render_result,
        workspace_exec_command_descriptor, workspace_write_stdin_descriptor, ExecPollResult,
        ExecSessionId,
    };
    use orchestral_core::tool_protocol::{ApprovalPolicy, ToolPolicyBounds, ToolRestriction};
    use serde_json::json;

    #[test]
    fn command_wait_keeps_explicit_windows_and_interactions_within_host_limits() {
        use super::{command_wait, wait_mode, ExecWaitMode};
        use std::time::Duration;

        for (arguments, tty, has_input, expected_seconds) in [
            (json!({}), false, false, 60),
            (json!({}), true, false, 10),
            (json!({}), false, true, 10),
            (json!({"wait_mode":"output"}), false, false, 10),
            (json!({"wait_mode":"completion"}), true, false, 60),
        ] {
            let mode = wait_mode(&arguments, tty, has_input).unwrap();
            assert_eq!(
                command_wait(&arguments, None, mode),
                Duration::from_secs(expected_seconds)
            );
        }
        for (explicit, host_ms, expected_ms) in [
            (None, Some(130_000), 60_000),
            (None, Some(8_000), 8_000),
            (Some(1_234), Some(130_000), 1_234),
            (Some(90_000), Some(130_000), 90_000),
            (Some(90_000), Some(20_000), 20_000),
        ] {
            let arguments =
                explicit.map_or_else(|| json!({}), |value| json!({"yield_time_ms":value}));
            assert_eq!(
                command_wait(&arguments, host_ms, ExecWaitMode::Completion),
                Duration::from_millis(expected_ms)
            );
        }
    }

    #[test]
    fn command_observation_respects_the_remaining_dispatch_deadline() {
        use super::observation_window;
        use std::time::Duration;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
        let remaining = observation_window(Duration::from_secs(60), Some(deadline));
        assert!(remaining <= Duration::from_millis(11_950));
        assert!(remaining > Duration::from_secs(10));
        assert_eq!(
            observation_window(Duration::from_millis(1_234), Some(deadline)),
            Duration::from_millis(1_234)
        );
        assert_eq!(
            observation_window(Duration::from_secs(60), None),
            Duration::from_secs(60)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_completion_observes_exit_after_the_previous_default_window() {
        use super::{command_wait, wait_mode, ExecWaitOptions};
        use crate::exec_process::{ExecSpawnSpec, ProcessSupervisor};
        use orchestral_core::agent_protocol::wire::RunId;
        use orchestral_core::tool_protocol::{
            CapabilityRequest, EffectScope, ToolOperationPlan, ToolOperationRisk,
        };
        use std::collections::BTreeSet;
        use tokio_util::sync::CancellationToken;

        let manager = ProcessSupervisor::new(1024).unwrap();
        let run_id = RunId::new("completion-window");
        let session = manager
            .spawn(ExecSpawnSpec {
                run_id: run_id.clone(),
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    "/bin/sleep 12; printf completed; exit 7".to_owned(),
                ],
                cwd: std::fs::canonicalize(".").unwrap(),
                environment: Default::default(),
                tty: false,
                backend_starts_new_session: false,
                operation: ToolOperationPlan {
                    required_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                        EffectScope::Process,
                    ])),
                    risk: ToolOperationRisk::Routine,
                    session_approval_scope: None,
                    summary: "Observe delayed process completion".to_owned(),
                },
            })
            .await
            .unwrap();
        let arguments = json!({});
        let mode = wait_mode(&arguments, false, false).unwrap();
        let result = manager
            .write_and_poll_with_options(
                &run_id,
                session,
                None,
                ExecWaitOptions {
                    duration: command_wait(&arguments, Some(20_000), mode),
                    mode,
                    yield_requested: CancellationToken::new(),
                },
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        manager.close_run(&run_id).await.unwrap();
        assert!(!result.alive);
        assert_eq!(result.exit_code, Some(7));
        assert_eq!(result.stdout, "completed");
    }

    #[test]
    fn exec_v2_output_preserves_each_stream_and_terminal_metadata_once() {
        let restriction = ToolRestriction {
            bounds: ToolPolicyBounds::default(),
        };
        let descriptors = [
            guarded_exec_command_descriptor(restriction.clone()),
            guarded_write_stdin_descriptor(restriction),
        ];
        for (stdout, stderr) in [
            ("\tlet text = \"\\n\";\r\n", ""),
            ("", "error: 読み取り\n"),
            ("first\nlast", "warning\r\n"),
        ] {
            let output = render_result(
                ExecPollResult {
                    stdout: stdout.to_owned(),
                    stderr: stderr.to_owned(),
                    dropped_bytes: 0,
                    alive: false,
                    exit_code: Some(7),
                    wall_time_seconds: 0.25,
                },
                ExecSessionId::new(1).unwrap(),
                1024,
                "test-sandbox",
            );
            assert_eq!(
                output,
                json!({
                    "stdout": stdout,
                    "stderr": stderr,
                    "alive": false,
                    "exit_code": 7,
                    "wall_time_seconds": 0.25,
                    "truncated": false,
                    "dropped_bytes": 0,
                    "sandbox_backend": "test-sandbox",
                })
            );
            for descriptor in &descriptors {
                descriptor.validate_output(&output).unwrap();
                let mut legacy = output.clone();
                legacy["output"] = json!(format!("{stdout}{stderr}"));
                assert!(descriptor.validate_output(&legacy).is_err());
            }
        }
    }

    #[test]
    fn exec_v2_output_keeps_poll_identity_and_shared_stream_budget() {
        let descriptor = guarded_write_stdin_descriptor(ToolRestriction {
            bounds: ToolPolicyBounds::default(),
        });
        for (capture_dropped, budget, stderr, total_dropped) in
            [(0, 4, "ef", 2), (9, 64, "cdef", 9), (0, 64, "cdef", 0)]
        {
            let output = render_result(
                ExecPollResult {
                    stdout: "ab".to_owned(),
                    stderr: "cdef".to_owned(),
                    dropped_bytes: capture_dropped,
                    alive: true,
                    exit_code: None,
                    wall_time_seconds: 0.5,
                },
                ExecSessionId::new(42).unwrap(),
                budget,
                "existing_session",
            );
            descriptor.validate_output(&output).unwrap();
            assert_eq!(output["stdout"], "ab");
            assert_eq!(output["stderr"], stderr);
            assert_eq!(output["session_id"], 42);
            assert_eq!(output["alive"], true);
            assert_eq!(output["truncated"], total_dropped > 0);
            assert_eq!(output["dropped_bytes"], total_dropped);
            assert!(output.get("exit_code").is_none());
            assert!(output.get("output").is_none());
        }
    }

    #[test]
    fn bounded_exec_output_keeps_stderr_and_each_streams_terminal_diagnostics() {
        let stdout = format!("build started\n{}\nbuild failed\n", "progress\n".repeat(32));
        let stderr = format!(
            "warning: deprecated\n{}\nerror: unavailable\n",
            "detail\n".repeat(32)
        );
        let output = render_result(
            ExecPollResult {
                stdout: stdout.clone(),
                stderr: stderr.clone(),
                dropped_bytes: 11,
                alive: false,
                exit_code: Some(9),
                wall_time_seconds: 0.1,
            },
            ExecSessionId::new(1).unwrap(),
            256,
            "test-sandbox",
        );
        let visible_stdout = output["stdout"].as_str().unwrap();
        let visible_stderr = output["stderr"].as_str().unwrap();
        assert!(visible_stdout.starts_with("build started\n"));
        assert!(visible_stdout.ends_with("build failed\n"));
        assert!(visible_stderr.starts_with("warning: deprecated\n"));
        assert!(visible_stderr.ends_with("error: unavailable\n"));
        assert!(visible_stdout.len() + visible_stderr.len() <= 256);
        let marker_bytes = 2 * "\n[... omitted ...]\n".len();
        assert_eq!(
            output["dropped_bytes"],
            11 + stdout.len() + stderr.len() - visible_stdout.len() - visible_stderr.len()
                + marker_bytes
        );
        assert_eq!(output["exit_code"], 9);
        assert_eq!(output["alive"], false);
        assert_eq!(output["truncated"], true);
    }

    #[test]
    fn bounded_exec_output_respects_utf8_and_tiny_or_exact_capacity() {
        let text = "初期🙂\n処理中\nошибка\n終端";
        for budget in 0..=text.len() + 1 {
            let (visible, omitted) = super::shorten_exec_output(text, budget);
            assert!(visible.len() <= budget);
            assert!(!visible.contains('\u{fffd}'));
            if budget >= text.len() {
                assert_eq!(visible, text);
                assert_eq!(omitted, 0);
            } else {
                assert!(omitted > 0);
                let pieces = visible.split("\n[... omitted ...]\n").collect::<Vec<_>>();
                let retained = pieces.iter().map(|piece| piece.len()).sum::<usize>();
                assert_eq!(omitted as usize + retained, text.len());
                assert!(text.ends_with(pieces.last().unwrap()));
                if pieces.len() == 2 {
                    assert!(text.starts_with(pieces[0]));
                }
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounded_exec_output_reports_capture_and_render_loss_once_across_polls() {
        use crate::exec_process::{ExecSpawnSpec, ProcessSupervisor};
        use orchestral_core::agent_protocol::wire::RunId;
        use orchestral_core::tool_protocol::{
            CapabilityRequest, EffectScope, ToolOperationPlan, ToolOperationRisk,
        };
        use std::collections::BTreeSet;
        use std::time::Duration;
        use tokio_util::sync::CancellationToken;

        let capacity = 128;
        let manager = ProcessSupervisor::new(capacity).unwrap();
        let run_id = RunId::new("bounded-output");
        let stdout = format!("start:{}:stdout-end", "x".repeat(4 * capacity));
        let stderr = format!("start:{}:stderr-end", "y".repeat(4 * capacity));
        let session = manager
            .spawn(ExecSpawnSpec {
                run_id: run_id.clone(),
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    concat!(
                        "printf '%s' \"$1\"; printf '%s' \"$2\" >&2; ",
                        "read value; printf done; printf fatal >&2; exit 7"
                    )
                    .to_owned(),
                    "bounded-output".to_owned(),
                    stdout.clone(),
                    stderr.clone(),
                ],
                cwd: std::fs::canonicalize(".").unwrap(),
                environment: Default::default(),
                tty: false,
                backend_starts_new_session: false,
                operation: ToolOperationPlan {
                    required_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                        EffectScope::Process,
                    ])),
                    risk: ToolOperationRisk::Routine,
                    session_approval_scope: None,
                    summary: "Exercise bounded command output".to_owned(),
                },
            })
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        let captured = manager
            .write_and_poll(
                &run_id,
                session,
                None,
                Duration::from_secs(1),
                &cancellation,
            )
            .await
            .unwrap();
        assert!(captured.alive);
        assert_eq!(captured.stdout.len(), capacity);
        assert_eq!(captured.stderr.len(), capacity);
        assert_eq!(
            captured.dropped_bytes as usize,
            stdout.len() + stderr.len() - 2 * capacity
        );
        let output = render_result(captured, session, capacity, "test");
        let kept_stdout = output["stdout"].as_str().unwrap();
        let kept_stderr = output["stderr"].as_str().unwrap();
        assert!(kept_stdout.ends_with(":stdout-end"));
        assert!(kept_stderr.ends_with(":stderr-end"));
        assert!(kept_stdout.len() + kept_stderr.len() <= capacity);
        let kept_bytes = kept_stdout.len() + kept_stderr.len() - 2 * "\n[... omitted ...]\n".len();
        assert_eq!(
            output["dropped_bytes"],
            stdout.len() + stderr.len() - kept_bytes
        );

        let empty = manager
            .write_and_poll(
                &run_id,
                session,
                None,
                Duration::from_millis(10),
                &cancellation,
            )
            .await
            .unwrap();
        let empty = render_result(empty, session, capacity, "test");
        assert_eq!(empty["alive"], true);
        assert_eq!(empty["session_id"], session.get());
        assert_eq!(empty["stdout"], "");
        assert_eq!(empty["stderr"], "");
        assert_eq!(empty["dropped_bytes"], 0);
        assert_eq!(empty["truncated"], false);

        let final_output = manager
            .write_and_poll(
                &run_id,
                session,
                Some("finish\n"),
                Duration::from_secs(1),
                &cancellation,
            )
            .await
            .unwrap();
        let final_output = render_result(final_output, session, capacity, "test");
        assert_eq!(final_output["stdout"], "done");
        assert_eq!(final_output["stderr"], "fatal");
        assert_eq!(final_output["exit_code"], 7);
        assert_eq!(final_output["alive"], false);
        assert_eq!(final_output["dropped_bytes"], 0);
        assert!(manager.list(&run_id).unwrap().is_empty());
    }

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
    fn exec_schema_exposes_only_available_host_escalation() {
        use orchestral_core::tool_protocol::EffectScope;

        let mut bounds = ToolPolicyBounds {
            allowed_effects: exec_effects(),
            approval: ApprovalPolicy::NotRequired,
            ..Default::default()
        };
        bounds.allowed_effects.remove(&EffectScope::HostExecution);
        let restriction = ToolRestriction { bounds };
        for descriptor in [
            workspace_exec_command_descriptor(restriction.clone()),
            guarded_exec_command_descriptor(restriction.clone()),
        ] {
            descriptor
                .model_schema
                .validate_arguments(&json!({
                    "cmd":"echo ready", "workdir":"subdirectory", "tty":true,
                    "wait_mode":"output", "yield_time_ms":1, "max_output_tokens":16,
                }))
                .unwrap();
            for unavailable in [
                json!({"cmd":"echo ready", "sandbox_permissions":"require_escalated", "justification":"requested operation"}),
                json!({"cmd":"echo ready", "justification":"requested operation"}),
            ] {
                assert!(descriptor
                    .model_schema
                    .validate_arguments(&unavailable)
                    .is_err());
            }
            assert!(!descriptor
                .model_schema
                .description
                .contains("require_escalated"));
            assert!(!descriptor
                .model_schema
                .description
                .contains("justification"));
            assert!(!descriptor
                .effect_scopes
                .contains(&EffectScope::HostExecution));
        }
        let sandbox_only = workspace_exec_command_descriptor(restriction.clone());
        let mut enabled = restriction;
        enabled
            .bounds
            .allowed_effects
            .insert(EffectScope::HostExecution);
        let dual = workspace_exec_command_descriptor(enabled.clone());
        dual.model_schema
            .validate_arguments(&json!({"cmd":"echo ready"}))
            .unwrap();
        dual.model_schema
            .validate_arguments(&json!({"cmd":"echo ready", "sandbox_permissions":"use_default"}))
            .unwrap();
        dual.model_schema.validate_arguments(&json!({
            "cmd":"echo ready", "sandbox_permissions":"require_escalated", "justification":"requested operation",
        })).unwrap();
        assert!(dual.model_schema.description.contains("require_escalated"));
        // The existing recovery digest includes the projected schema, even if
        // a caller were to keep the underlying Host restrictions identical.
        let mut same_authority = dual.clone();
        same_authority.model_schema = sandbox_only.model_schema;
        assert_ne!(same_authority.digest().unwrap(), dual.digest().unwrap());
        let host_only = approved_host_exec_command_descriptor(enabled);
        assert_eq!(
            host_only.restriction.bounds.approval,
            ApprovalPolicy::Required
        );
        assert!(host_only
            .model_schema
            .validate_arguments(&json!({"cmd":"echo ready"}))
            .is_err());
        assert!(host_only
            .model_schema
            .validate_arguments(
                &json!({"cmd":"echo ready", "sandbox_permissions":"require_escalated"})
            )
            .is_err());
        host_only.model_schema.validate_arguments(&json!({
            "cmd":"echo ready", "sandbox_permissions":"require_escalated", "justification":"requested operation",
        })).unwrap();
    }

    #[tokio::test]
    async fn exec_planner_rejects_explicit_and_implicit_host_execution_without_authority() {
        use super::{CommandEnvironmentSnapshot, GuardedExecCommandExecutor, ProcessSupervisor};
        use crate::tool_runtime::GuardedToolExecutor;
        use orchestral_core::agent_protocol::wire::RunId;
        use orchestral_core::tool_protocol::{
            EffectScope, EffectiveToolPolicy, HostToolPolicy, RunToolGrant, ToolCallId,
            ToolInvocation, ToolOutcome,
        };

        let directory = tempfile::tempdir().unwrap();
        let workspace = std::fs::canonicalize(directory.path()).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let manager = std::sync::Arc::new(ProcessSupervisor::new(1024).unwrap());
        let executor = GuardedExecCommandExecutor::new(
            manager,
            std::env::current_exe().unwrap(),
            [],
            [],
            CommandEnvironmentSnapshot::default(),
        )
        .unwrap();
        let mut bounds = ToolPolicyBounds {
            allowed_effects: exec_effects(),
            ..Default::default()
        };
        bounds
            .filesystem
            .readable_roots
            .insert(workspace.to_string_lossy().into_owned());
        bounds.filesystem.writable_roots = bounds.filesystem.readable_roots.clone();
        let descriptor = workspace_exec_command_descriptor(ToolRestriction {
            bounds: bounds.clone(),
        });
        bounds.allowed_effects.remove(&EffectScope::HostExecution);
        let policy = EffectiveToolPolicy::resolve(
            &HostToolPolicy {
                bounds: bounds.clone(),
            },
            &RunToolGrant { bounds },
            &descriptor.restriction,
        )
        .unwrap();
        for arguments in [
            json!({"cmd":"echo denied", "sandbox_permissions":"require_escalated", "justification":"requested operation"}),
            json!({"cmd":"echo denied", "workdir":outside.path()}),
        ] {
            // Deliberately use the broader schema and invoke the planner directly:
            // hiding model fields must not become the authorization boundary.
            descriptor
                .model_schema
                .validate_arguments(&arguments)
                .unwrap();
            let result = executor.plan_operation(
                &ToolInvocation {
                    run_id: RunId::new("host-ceiling"),
                    call_id: ToolCallId::new("denied"),
                    tool_id: descriptor.tool_id.clone(),
                    arguments,
                },
                &descriptor,
                &policy,
            );
            assert!(
                matches!(result, Err(ToolOutcome::Rejected { ref code, .. }) if code == "exec_host_execution_denied"),
                "{result:?}"
            );
        }
    }

    #[test]
    fn approval_preview_keeps_the_dangerous_tail_and_marks_omissions() {
        let payload = format!("{}rm -rf important", "safe ".repeat(80));
        let preview = display_payload(&payload);
        assert!(preview.contains("chars omitted"));
        assert!(preview.ends_with("rm -rf important"));
    }
}
