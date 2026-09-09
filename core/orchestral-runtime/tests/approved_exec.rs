#![cfg(unix)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, HostApprovalIssuer, HostApprovalVerifier, HostToolPolicy,
    InMemoryApprovalCapabilityStore, InteractiveCommandPolicy, NetworkPolicy, ProcessPolicy,
    RunToolGrant, SandboxPolicy, ToolCallId, ToolId, ToolInvocation, ToolOutcome, ToolOutput,
    ToolPolicyBounds, ToolRestriction,
};
use orchestral_runtime::tools::{
    approved_host_exec_command_descriptor, CommandEnvironmentSnapshot, GuardedExecCommandExecutor,
    GUARDED_EXEC_SANDBOX_PROFILE,
};
use orchestral_runtime::{
    GuardedToolResult, GuardedToolRuntime, ProcessSupervisor, WorkspacePermissionPolicy,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

fn compose(
    workspace: &Path,
) -> (
    GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
    ToolPolicyBounds,
) {
    let shell = std::fs::canonicalize("/bin/sh").unwrap();
    let manager = Arc::new(ProcessSupervisor::new(4096).unwrap());
    let roots = BTreeSet::from([
        workspace.to_string_lossy().into_owned(),
        manager.runtime_temp_root().to_string_lossy().into_owned(),
    ]);
    let bounds = ToolPolicyBounds {
        allowed_effects: BTreeSet::from([
            EffectScope::Process,
            EffectScope::Network,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
            EffectScope::HostExecution,
        ]),
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: BTreeSet::from([GUARDED_EXEC_SANDBOX_PROFILE.to_owned()]),
        },
        process: ProcessPolicy {
            interactive: InteractiveCommandPolicy {
                enabled: true,
                command_shells: BTreeSet::from([shell.to_string_lossy().into_owned()]),
                allow_child_processes: true,
            },
            ..Default::default()
        },
        filesystem: orchestral_core::tool_protocol::FilesystemPolicy {
            readable_roots: roots.clone(),
            writable_roots: roots,
        },
        network: NetworkPolicy {
            allow_unrestricted: true,
            ..Default::default()
        },
        max_timeout_ms: Some(1000),
        max_output_bytes: Some(4096),
        ..Default::default()
    };
    let verifier =
        HostApprovalVerifier::new(KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    let runtime = GuardedToolRuntime::new(
        HostToolPolicy {
            bounds: bounds.clone(),
        },
        verifier,
    )
    .unwrap()
    .with_permission_policy(Arc::new(WorkspacePermissionPolicy));
    let descriptor = approved_host_exec_command_descriptor(ToolRestriction {
        bounds: bounds.clone(),
    });
    assert_eq!(
        descriptor.model_schema.input_schema["properties"]["sandbox_permissions"]["enum"],
        json!(["require_escalated"])
    );
    let executor = GuardedExecCommandExecutor::new(
        manager,
        shell,
        [PathBuf::from("/bin")],
        [],
        CommandEnvironmentSnapshot::default(),
    )
    .unwrap()
    .with_sandboxed_execution_enabled(false);
    runtime.register(descriptor, Arc::new(executor)).unwrap();
    (runtime, bounds)
}

#[tokio::test]
async fn approved_only_commands_cannot_silently_escalate_or_bypass_run_grants() {
    let path =
        std::env::temp_dir().join(format!("orchestral-approved-only-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&path).unwrap();
    let workspace = std::fs::canonicalize(&path).unwrap();
    let (runtime, bounds) = compose(&workspace);
    let invocation = |call: &str, arguments| ToolInvocation {
        run_id: RunId::new("approved-only"),
        call_id: ToolCallId::new(call),
        tool_id: ToolId::new("orchestral/exec_command/v1"),
        arguments,
    };
    for (call, arguments) in [
        ("omitted", json!({"cmd":"printf unexpected > marker"})),
        (
            "default",
            json!({"cmd":"printf unexpected > marker", "sandbox_permissions":"use_default"}),
        ),
        (
            "unjustified",
            json!({"cmd":"printf unexpected > marker", "sandbox_permissions":"require_escalated"}),
        ),
    ] {
        let result = runtime
            .invoke(
                invocation(call, arguments),
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(
                result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Rejected { .. },
                    ..
                }
            ),
            "{result:?}"
        );
        assert!(!workspace.join("marker").exists());
    }
    let arguments = json!({
        "cmd":"printf approved > marker", "sandbox_permissions":"require_escalated",
        "justification":"Write the requested marker in the isolated Host workspace",
        "yield_time_ms":1000,
    });
    let mut denied = bounds.clone();
    denied.allowed_effects.remove(&EffectScope::HostExecution);
    let result = runtime
        .invoke(
            invocation("denied", arguments.clone()),
            RunToolGrant { bounds: denied },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { .. },
                ..
            }
        ),
        "{result:?}"
    );
    assert!(!workspace.join("marker").exists());

    let call = invocation("approved", arguments);
    let pending = runtime
        .invoke(
            call.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::ApprovalRequired { binding, .. } = pending else {
        panic!("{pending:?}")
    };
    assert!(binding
        .requested_capabilities
        .requires(EffectScope::HostExecution));
    assert!(!workspace.join("marker").exists());
    let approval = HostApprovalIssuer::new(KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let result = runtime
        .invoke(
            call,
            RunToolGrant { bounds },
            Some(approval),
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::Outcome {
        outcome: ToolOutcome::Completed {
            output: ToolOutput::Inline(output),
        },
        ..
    } = result
    else {
        panic!("{result:?}")
    };
    assert_eq!(output["sandbox_backend"], "host-approved");
    assert_eq!(
        std::fs::read_to_string(workspace.join("marker")).unwrap(),
        "approved"
    );
    std::fs::remove_dir_all(path).unwrap();
}
