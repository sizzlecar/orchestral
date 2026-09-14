//! Guarded Tool adapters for run-scoped PTY processes.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, ModelToolSchema, ToolConcurrency, ToolDescriptor, ToolId,
    ToolIdempotency, ToolInvocation, ToolOutcome, ToolRestriction,
};
use serde_json::{json, Value};

use crate::pty_process::{PtyProcessError, PtyProcessId, PtyProcessManager, PtySpawnSpec};
use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor};

use super::guarded::GuardedProgramAliases;
use super::support::{build_allowlisted_env, canonical_roots};
use crate::tools::shell_sandbox::{sandbox_command, SandboxNetworkAccess, ShellSandboxPolicy};

pub const GUARDED_PTY_SANDBOX_PROFILE: &str = "orchestral.pty.process.v1";

#[derive(Clone)]
pub struct GuardedPtyCreateExecutor {
    manager: Arc<PtyProcessManager>,
    program_aliases: GuardedProgramAliases,
}

#[derive(Clone)]
pub struct GuardedPtyWriteExecutor {
    manager: Arc<PtyProcessManager>,
}

#[derive(Clone)]
pub struct GuardedPtyReadExecutor {
    manager: Arc<PtyProcessManager>,
}

#[derive(Clone)]
pub struct GuardedPtyCloseExecutor {
    manager: Arc<PtyProcessManager>,
}

#[derive(Clone)]
pub struct GuardedPtyListExecutor {
    manager: Arc<PtyProcessManager>,
}

macro_rules! manager_constructor {
    ($type:ty) => {
        impl $type {
            pub fn new(manager: Arc<PtyProcessManager>) -> Self {
                Self { manager }
            }
        }
    };
}

manager_constructor!(GuardedPtyWriteExecutor);
manager_constructor!(GuardedPtyReadExecutor);
manager_constructor!(GuardedPtyCloseExecutor);
manager_constructor!(GuardedPtyListExecutor);

impl GuardedPtyCreateExecutor {
    pub fn new(manager: Arc<PtyProcessManager>) -> Self {
        Self {
            manager,
            program_aliases: GuardedProgramAliases::default(),
        }
    }

    pub fn new_with_program_aliases(
        manager: Arc<PtyProcessManager>,
        program_aliases: GuardedProgramAliases,
    ) -> Self {
        Self {
            manager,
            program_aliases,
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedPtyCreateExecutor {
    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let program = invocation
            .arguments
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("<invalid-command>");
        let args = invocation
            .arguments
            .get("args")
            .and_then(Value::as_array)
            .map(|arguments| display_arguments(arguments))
            .unwrap_or_default();
        format!("Create sandboxed PTY process: {program} {args}")
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if let Err(outcome) = require_approval(&execution) {
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
                .contains(GUARDED_PTY_SANDBOX_PROFILE)
        {
            return rejected(
                "pty_sandbox_denied",
                "effective policy does not authorize the guarded PTY sandbox profile",
            );
        }
        let command = match execution
            .invocation
            .arguments
            .get("command")
            .and_then(Value::as_str)
        {
            Some(command) => command,
            None => return rejected("pty_command_missing", "PTY command must be a string"),
        };
        let command = match self
            .program_aliases
            .resolve(command, &bounds.process.interactive.command_shells)
        {
            Ok(command) => command,
            Err(message) => return rejected("pty_program_denied", message),
        };
        let args = execution
            .invocation
            .arguments
            .get("args")
            .and_then(Value::as_array)
            .map(|args| {
                args.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let readable_roots = match canonical_roots(&bounds.filesystem.readable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => {
                return rejected(
                    "pty_read_root_denied",
                    "guarded PTY requires a Host-approved readable root",
                )
            }
            Err(message) => return rejected("pty_read_root_invalid", message),
        };
        let writable_roots = match canonical_roots(&bounds.filesystem.writable_roots) {
            Ok(roots) if !roots.is_empty() => roots,
            Ok(_) => {
                return rejected(
                    "pty_write_root_denied",
                    "guarded PTY requires a Host-approved writable root",
                )
            }
            Err(message) => return rejected("pty_write_root_invalid", message),
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
            allow_host_ui: false,
            launcher_programs: bounds
                .process
                .interactive
                .command_shells
                .iter()
                .map(PathBuf::from)
                .collect(),
            network: SandboxNetworkAccess::Disabled,
            linux_bwrap_path: None,
        };
        let sandboxed = match sandbox_command(command, args, &cwd, &sandbox_policy) {
            Ok(sandboxed) => sandboxed,
            Err(message) => return failed("pty_sandbox_setup", message, false),
        };
        let allowlist = bounds
            .environment
            .allowed_variables
            .iter()
            .map(|variable| variable.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        let environment = build_allowlisted_env(&allowlist, &sandboxed.env)
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let rows = bounded_dimension(&execution.invocation.arguments, "rows", 24);
        let cols = bounded_dimension(&execution.invocation.arguments, "cols", 120);
        let process_id =
            match PtyProcessId::new(format!("pty:{}", execution.invocation.call_id.as_str())) {
                Ok(process_id) => process_id,
                Err(error) => return pty_error(error),
            };
        let spec = PtySpawnSpec {
            run_id: execution.invocation.run_id.clone(),
            process_id: process_id.clone(),
            program: sandboxed.program,
            args: sandboxed.args,
            cwd,
            environment,
            rows,
            cols,
        };
        let manager = self.manager.clone();
        let created = match tokio::task::spawn_blocking(move || manager.create(spec)).await {
            Ok(result) => result,
            Err(error) => return failed("pty_create_join", error.to_string(), true),
        };
        let process_id = match created {
            Ok(process_id) => process_id,
            Err(error) => return pty_error(error),
        };
        if execution.cancellation.is_cancelled() {
            let _ = self
                .manager
                .close(&execution.invocation.run_id, &process_id);
            return ToolOutcome::Cancelled;
        }
        let manager = self.manager.clone();
        let run_id = execution.invocation.run_id.clone();
        let watched_process_id = process_id.clone();
        let cancellation = execution.cancellation.clone();
        tokio::spawn(async move {
            cancellation.cancelled().await;
            let _ =
                tokio::task::spawn_blocking(move || manager.close(&run_id, &watched_process_id))
                    .await;
        });
        ToolOutcome::Completed {
            output: json!({
                "process_id": process_id.as_str(),
                "alive": true,
                "sandbox_backend": sandboxed.backend,
            })
            .into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedPtyWriteExecutor {
    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let process_id = invocation
            .arguments
            .get("process_id")
            .and_then(Value::as_str)
            .unwrap_or("<invalid-process>");
        let input = invocation
            .arguments
            .get("input")
            .and_then(Value::as_str)
            .unwrap_or("<invalid-input>");
        let preview = input.chars().take(160).collect::<String>();
        format!("Send input to PTY {process_id}: {preview}")
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if let Err(outcome) = require_approval(&execution) {
            return outcome;
        }
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let (process_id, input) = match process_and_input(&execution.invocation.arguments) {
            Ok(values) => values,
            Err(outcome) => return outcome,
        };
        let manager = self.manager.clone();
        let run_id = execution.invocation.run_id.clone();
        let bytes_sent = input.len() as u64;
        match tokio::task::spawn_blocking(move || manager.send(&run_id, &process_id, &input)).await
        {
            Ok(Ok(())) => ToolOutcome::Completed {
                output: json!({ "bytes_sent": bytes_sent }).into(),
            },
            Ok(Err(error)) => pty_error(error),
            Err(error) => failed("pty_write_join", error.to_string(), true),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedPtyReadExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        let process_id = match process_id(&execution.invocation.arguments) {
            Ok(process_id) => process_id,
            Err(outcome) => return outcome,
        };
        let timeout_ms = bounded_millis(
            &execution.invocation.arguments,
            "timeout_ms",
            5_000,
            execution
                .effective_policy
                .bounds()
                .max_timeout_ms
                .unwrap_or(30_000),
        );
        let settle_ms = bounded_millis(
            &execution.invocation.arguments,
            "settle_ms",
            250,
            timeout_ms,
        );
        let manager = self.manager.clone();
        let run_id = execution.invocation.run_id.clone();
        let cancellation = execution.cancellation.clone();
        match tokio::task::spawn_blocking(move || {
            manager.read(
                &run_id,
                &process_id,
                Duration::from_millis(timeout_ms),
                Duration::from_millis(settle_ms),
                &cancellation,
            )
        })
        .await
        {
            Ok(Ok(result)) => ToolOutcome::Completed {
                output: json!({
                    "output": result.output,
                    "dropped_bytes": result.dropped_bytes,
                    "alive": result.alive,
                })
                .into(),
            },
            Ok(Err(error)) => pty_error(error),
            Err(error) => failed("pty_read_join", error.to_string(), true),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedPtyCloseExecutor {
    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let process_id = invocation
            .arguments
            .get("process_id")
            .and_then(Value::as_str)
            .unwrap_or("<invalid-process>");
        format!("Terminate PTY process: {process_id}")
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if let Err(outcome) = require_approval(&execution) {
            return outcome;
        }
        let process_id = match process_id(&execution.invocation.arguments) {
            Ok(process_id) => process_id,
            Err(outcome) => return outcome,
        };
        let closed_id = process_id.as_str().to_owned();
        let manager = self.manager.clone();
        let run_id = execution.invocation.run_id.clone();
        match tokio::task::spawn_blocking(move || manager.close(&run_id, &process_id)).await {
            Ok(Ok(())) => ToolOutcome::Completed {
                output: json!({ "process_id": closed_id, "closed": true }).into(),
            },
            Ok(Err(error)) => pty_error(error),
            Err(error) => failed("pty_close_join", error.to_string(), true),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedPtyListExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        match self.manager.list(&execution.invocation.run_id) {
            Ok(processes) => ToolOutcome::Completed {
                output: json!({
                    "process_ids": processes
                        .iter()
                        .map(PtyProcessId::as_str)
                        .collect::<Vec<_>>()
                })
                .into(),
            },
            Err(error) => pty_error(error),
        }
    }
}

pub fn guarded_pty_create_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    guarded_pty_create_descriptor_with_program_aliases(
        restriction,
        &GuardedProgramAliases::default(),
    )
}

pub fn guarded_pty_create_descriptor_with_program_aliases(
    restriction: ToolRestriction,
    program_aliases: &GuardedProgramAliases,
) -> ToolDescriptor {
    let restriction = guarded_pty_restriction(restriction, true);
    let programs =
        program_aliases.advertised_programs(&restriction.bounds.process.interactive.command_shells);
    let accepted_commands =
        program_aliases.accepted_commands(&restriction.bounds.process.interactive.command_shells);
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/pty_create/v1"),
        model_schema: ModelToolSchema {
            name: "pty_create".to_owned(),
            description: format!(
                "Create a run-scoped interactive process inside the Host sandbox. Use this only when the process requires ongoing input or incremental output. Allowed programs: {programs}"
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
                    "rows": { "type": "integer", "minimum": 1, "maximum": 500 },
                    "cols": { "type": "integer", "minimum": 1, "maximum": 500 }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["process_id", "alive", "sandbox_backend"],
            "properties": {
                "process_id": { "type": "string" },
                "alive": { "type": "boolean" },
                "sandbox_backend": { "type": "string" }
            },
            "additionalProperties": false
        }),
        effect_scopes: guarded_process_effects(),
        restriction,
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

pub fn guarded_pty_write_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/pty_write/v1"),
        model_schema: ModelToolSchema {
            name: "pty_write".to_owned(),
            description: "Send bounded input to a PTY process owned by this Agent Run".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["process_id", "input"],
                "properties": {
                    "process_id": { "type": "string" },
                    "input": { "type": "string", "minLength": 1, "maxLength": 65536 }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["bytes_sent"],
            "properties": { "bytes_sent": { "type": "integer" } },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::Process, EffectScope::ExternalSideEffect]),
        restriction: guarded_pty_restriction(restriction, true),
        idempotency: ToolIdempotency::NonIdempotent,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

pub fn guarded_pty_read_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/pty_read/v1"),
        model_schema: ModelToolSchema {
            name: "pty_read".to_owned(),
            description: "Read bounded buffered output from a PTY process owned by this Agent Run"
                .to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["process_id"],
                "properties": {
                    "process_id": { "type": "string" },
                    "timeout_ms": { "type": "integer", "minimum": 1 },
                    "settle_ms": { "type": "integer", "minimum": 1 }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["output", "dropped_bytes", "alive"],
            "properties": {
                "output": { "type": "string" },
                "dropped_bytes": { "type": "integer" },
                "alive": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::new(),
        restriction,
        idempotency: ToolIdempotency::IdempotentWithKey,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

pub fn guarded_pty_close_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/pty_close/v1"),
        model_schema: ModelToolSchema {
            name: "pty_close".to_owned(),
            description: "Terminate a PTY process owned by this Agent Run".to_owned(),
            input_schema: process_id_schema(),
        },
        output_schema: json!({
            "type": "object",
            "required": ["process_id", "closed"],
            "properties": {
                "process_id": { "type": "string" },
                "closed": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::Process]),
        restriction: guarded_pty_restriction(restriction, true),
        idempotency: ToolIdempotency::IdempotentWithKey,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

pub fn guarded_pty_list_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/pty_list/v1"),
        model_schema: ModelToolSchema {
            name: "pty_list".to_owned(),
            description: "List PTY process handles owned by this Agent Run".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["process_ids"],
            "properties": {
                "process_ids": { "type": "array", "items": { "type": "string" } }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::new(),
        restriction,
        idempotency: ToolIdempotency::Pure,
        concurrency: ToolConcurrency::PerRunSerial,
    }
}

fn guarded_pty_restriction(
    mut restriction: ToolRestriction,
    approval_required: bool,
) -> ToolRestriction {
    if approval_required {
        restriction.bounds.approval = match restriction.bounds.approval {
            ApprovalPolicy::Deny => ApprovalPolicy::Deny,
            ApprovalPolicy::NotRequired | ApprovalPolicy::Required => ApprovalPolicy::Required,
            _ => ApprovalPolicy::Deny,
        };
    }
    restriction.bounds.sandbox.required = true;
    restriction.bounds.sandbox.allowed_profiles =
        BTreeSet::from([GUARDED_PTY_SANDBOX_PROFILE.to_owned()]);
    restriction.bounds.process.interactive.enabled = true;
    restriction.bounds.process.interactive.allow_child_processes = false;
    restriction.bounds.environment.inherit_host_environment = false;
    restriction
}

fn guarded_process_effects() -> BTreeSet<EffectScope> {
    BTreeSet::from([
        EffectScope::Process,
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::EnvironmentRead,
        EffectScope::ExternalSideEffect,
    ])
}

fn require_approval(execution: &GuardedToolExecution) -> Result<(), ToolOutcome> {
    let Some(approval) = execution.lease.approval() else {
        return Err(rejected(
            "approval_proof_missing",
            "PTY mutation requires a verified Host approval capability",
        ));
    };
    let binding = approval.binding();
    if binding.run_id != execution.invocation.run_id
        || binding.call_id != execution.invocation.call_id
        || binding.tool_id != execution.invocation.tool_id
    {
        return Err(rejected(
            "approval_binding_mismatch",
            "verified approval does not belong to this PTY invocation",
        ));
    }
    Ok(())
}

fn process_and_input(arguments: &Value) -> Result<(PtyProcessId, String), ToolOutcome> {
    let process_id = process_id(arguments)?;
    let input = arguments
        .get("input")
        .and_then(Value::as_str)
        .filter(|input| !input.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| rejected("pty_input_missing", "PTY input must not be empty"))?;
    Ok((process_id, input))
}

fn process_id(arguments: &Value) -> Result<PtyProcessId, ToolOutcome> {
    let process_id = arguments
        .get("process_id")
        .and_then(Value::as_str)
        .ok_or_else(|| rejected("pty_process_id_missing", "PTY process_id must be a string"))?;
    PtyProcessId::new(process_id).map_err(pty_error)
}

fn process_id_schema() -> Value {
    json!({
        "type": "object",
        "required": ["process_id"],
        "properties": { "process_id": { "type": "string" } },
        "additionalProperties": false
    })
}

fn display_arguments(arguments: &[Value]) -> String {
    arguments
        .iter()
        .filter_map(Value::as_str)
        .map(|argument| serde_json::to_string(argument).unwrap_or_else(|_| "<invalid>".to_owned()))
        .collect::<Vec<_>>()
        .join(" ")
}

fn bounded_dimension(arguments: &Value, name: &str, default: u16) -> u16 {
    arguments
        .get(name)
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| (1..=500).contains(value))
        .unwrap_or(default)
}

fn bounded_millis(arguments: &Value, name: &str, default: u64, maximum: u64) -> u64 {
    arguments
        .get(name)
        .and_then(Value::as_u64)
        .unwrap_or(default)
        .clamp(1, maximum.max(1))
}

fn pty_error(error: PtyProcessError) -> ToolOutcome {
    match error {
        PtyProcessError::Cancelled => ToolOutcome::Cancelled,
        PtyProcessError::Invalid(message) => rejected("pty_invalid", message),
        PtyProcessError::Conflict(process_id) => rejected(
            "pty_process_conflict",
            format!("PTY process already exists: {process_id}"),
        ),
        PtyProcessError::NotFound(process_id) => rejected(
            "pty_process_not_found",
            format!("PTY process is not owned by this Run: {process_id}"),
        ),
        PtyProcessError::Unavailable => failed("pty_manager_unavailable", error.to_string(), true),
        PtyProcessError::Io(message) => failed("pty_io", message, false),
    }
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
