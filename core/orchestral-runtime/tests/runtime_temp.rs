#![cfg(any(target_os = "macos", target_os = "linux"))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, FilesystemPolicy, HostApprovalVerifier, HostToolPolicy,
    InMemoryApprovalCapabilityStore, InteractiveCommandPolicy, ProcessPolicy, RunToolGrant,
    SandboxPolicy, ToolCallId, ToolId, ToolInvocation, ToolOutcome, ToolOutput, ToolPolicyBounds,
    ToolRestriction,
};
use orchestral_runtime::tools::{
    workspace_exec_command_descriptor, CommandEnvironmentSnapshot, GuardedExecCommandExecutor,
    GUARDED_EXEC_SANDBOX_PROFILE,
};
use orchestral_runtime::{
    GuardedToolResult, GuardedToolRuntime, ProcessSupervisor, WorkspacePermissionPolicy,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

struct Host {
    runtime: GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
    manager: Arc<ProcessSupervisor>,
    bounds: ToolPolicyBounds,
    workspace: tempfile::TempDir,
}

impl Host {
    fn new(grant_temp: bool) -> Self {
        let workspace = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(workspace.path()).unwrap();
        std::fs::create_dir(root.join(".git")).unwrap();
        std::fs::write(root.join("AGENTS.md"), "workspace-only instructions").unwrap();
        let shell = std::fs::canonicalize("/bin/sh").unwrap();
        let manager = Arc::new(ProcessSupervisor::new(16 * 1024).unwrap());
        let mut roots = BTreeSet::from([root.to_string_lossy().into_owned()]);
        if grant_temp {
            roots.insert(manager.runtime_temp_root().to_string_lossy().into_owned());
        }
        let bounds = ToolPolicyBounds {
            allowed_effects: BTreeSet::from([
                EffectScope::Process,
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
            ]),
            approval: ApprovalPolicy::NotRequired,
            sandbox: SandboxPolicy {
                required: true,
                allowed_profiles: BTreeSet::from([GUARDED_EXEC_SANDBOX_PROFILE.into()]),
            },
            process: ProcessPolicy {
                interactive: InteractiveCommandPolicy {
                    enabled: true,
                    command_shells: BTreeSet::from([shell.to_string_lossy().into_owned()]),
                    allow_child_processes: true,
                },
                ..ProcessPolicy::default()
            },
            filesystem: FilesystemPolicy {
                readable_roots: roots.clone(),
                writable_roots: roots,
            },
            max_timeout_ms: Some(3_000),
            max_output_bytes: Some(16 * 1024),
            ..ToolPolicyBounds::default()
        };
        let verifier = HostApprovalVerifier::new(
            b"0123456789abcdef0123456789abcdef",
            InMemoryApprovalCapabilityStore::default(),
        )
        .unwrap();
        let runtime = GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
        )
        .unwrap()
        .with_permission_policy(Arc::new(WorkspacePermissionPolicy));
        runtime
            .register(
                workspace_exec_command_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(
                    GuardedExecCommandExecutor::new(
                        manager.clone(),
                        shell,
                        [PathBuf::from("/bin"), PathBuf::from("/usr/bin")],
                        [],
                        CommandEnvironmentSnapshot::default(),
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        Self {
            runtime,
            manager,
            bounds,
            workspace,
        }
    }

    async fn exec(
        &self,
        run: &str,
        call: &str,
        command: &str,
        cancellation: CancellationToken,
    ) -> GuardedToolResult {
        self.runtime
            .invoke(
                ToolInvocation {
                    run_id: RunId::new(run),
                    call_id: ToolCallId::new(call),
                    tool_id: ToolId::new("orchestral/exec_command/v1"),
                    arguments: json!({"cmd": command, "yield_time_ms": 100}),
                },
                RunToolGrant {
                    bounds: self.bounds.clone(),
                },
                None,
                cancellation,
            )
            .await
    }
}

fn output(result: GuardedToolResult) -> Value {
    match result {
        GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(value),
                },
            ..
        } => value,
        other => panic!("expected completed observation: {other:?}"),
    }
}

async fn removed(path: &Path) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Run temporary directory was not reclaimed");
}

#[tokio::test]
async fn temporary_files_are_outside_the_project_shared_within_a_run_and_isolated_between_runs() {
    let host = Host::new(true);
    let first = CancellationToken::new();
    let second = CancellationToken::new();
    let result = output(
        host.exec(
            "first",
            "create",
            concat!(
                "test \"$TMPDIR\" = \"$TMP\" && test \"$TMP\" = \"$TEMP\" || exit 11; ",
                "current=\"$TMPDIR\"; while test \"$current\" != /; do ",
                "test ! -e \"$current/.git\" || exit 12; current=$(dirname \"$current\"); done; ",
                "printf original > \"$TMPDIR/probe\"; printf '%s' \"$TMPDIR\""
            ),
            first.clone(),
        )
        .await,
    );
    assert_eq!(result["exit_code"], 0);
    let first_path = PathBuf::from(result["stdout"].as_str().unwrap());
    assert!(!first_path.starts_with(std::fs::canonicalize(host.workspace.path()).unwrap()));
    assert!(!host.workspace.path().join(".orchestral/tmp").exists());
    let continued = output(
        host.exec("first", "continue", "cat \"$TMPDIR/probe\"", first.clone())
            .await,
    );
    assert_eq!(continued["stdout"], "original");

    let command = format!(
        "ln -s '{}' \"$TMPDIR/other\"; if cat \"$TMPDIR/other/probe\"; then exit 21; fi; \
         if printf changed > \"$TMPDIR/other/probe\"; then exit 22; fi; printf '%s' \"$TMPDIR\"",
        first_path.display()
    );
    let isolated = output(
        host.exec("second", "isolate", &command, second.clone())
            .await,
    );
    assert_eq!(isolated["exit_code"], 0, "{isolated}");
    let second_path = PathBuf::from(isolated["stdout"].as_str().unwrap());
    assert_ne!(first_path, second_path);
    second.cancel();
    removed(&second_path).await;
    assert_eq!(
        std::fs::read_to_string(first_path.join("probe")).unwrap(),
        "original"
    );
    first.cancel();
    removed(&first_path).await;
}

#[tokio::test]
async fn temporary_storage_requires_an_explicit_host_grant_before_dispatch() {
    let host = Host::new(false);
    let result = host
        .exec(
            "denied",
            "create",
            "printf no > \"$TMPDIR/probe\"",
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
    assert_eq!(
        std::fs::read_dir(host.manager.runtime_temp_root())
            .unwrap()
            .count(),
        0
    );
    assert!(host.manager.list(&RunId::new("denied")).unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_reaps_a_running_command_and_releases_its_temporary_files() {
    let host = Host::new(true);
    let cancellation = CancellationToken::new();
    let result = output(
        host.exec(
            "cancelled",
            "start",
            "printf '%s' \"$TMPDIR\"; sleep 30",
            cancellation.clone(),
        )
        .await,
    );
    assert_eq!(result["alive"], true);
    let path = PathBuf::from(result["stdout"].as_str().unwrap());
    assert!(path.is_dir());
    cancellation.cancel();
    removed(&path).await;
    assert!(host
        .manager
        .list(&RunId::new("cancelled"))
        .unwrap()
        .is_empty());
}
