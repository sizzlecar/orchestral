use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalIssuer,
    HostApprovalVerifier, HostToolPolicy, InMemoryApprovalCapabilityStore, NetworkPolicy,
    ProcessPolicy, RunToolGrant, SandboxPolicy, ToolCallId, ToolId, ToolInvocation, ToolOutcome,
    ToolOutput, ToolPolicyBounds, ToolRestriction,
};
use orchestral_runtime::{
    tools::{guarded_apply_patch_descriptor, GuardedApplyPatchExecutor},
    GuardedToolResult, GuardedToolRuntime,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

const SIGNING_KEY: &[u8] = b"apply-patch-test-signing-key-32b";

struct TestWorkspace {
    root: PathBuf,
}

impl TestWorkspace {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "orchestral-apply-patch-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self {
            root: std::fs::canonicalize(root).unwrap(),
        }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn effects(values: &[EffectScope]) -> BTreeSet<EffectScope> {
    values.iter().copied().collect()
}

fn patch_bounds(root: &Path, approval: ApprovalPolicy) -> ToolPolicyBounds {
    let root = root.to_string_lossy().into_owned();
    ToolPolicyBounds {
        allowed_effects: effects(&[EffectScope::FilesystemRead, EffectScope::FilesystemWrite]),
        approval,
        sandbox: SandboxPolicy {
            required: false,
            allowed_profiles: BTreeSet::new(),
        },
        process: ProcessPolicy::default(),
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([root.clone()]),
            writable_roots: BTreeSet::from([root]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(64 * 1024),
    }
}

fn patch_runtime(
    workspace: &TestWorkspace,
    approval: ApprovalPolicy,
) -> (
    GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
    ToolPolicyBounds,
) {
    let bounds = patch_bounds(&workspace.root, approval);
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    let runtime = GuardedToolRuntime::new(
        HostToolPolicy {
            bounds: bounds.clone(),
        },
        verifier,
    )
    .unwrap();
    runtime
        .register(
            guarded_apply_patch_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedApplyPatchExecutor::new(&workspace.root).unwrap()),
        )
        .unwrap();
    (runtime, bounds)
}

fn invocation(call_id: impl Into<String>, patch: impl Into<String>) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new("apply-patch-run"),
        call_id: ToolCallId::new(call_id),
        tool_id: ToolId::new("orchestral/apply_patch/v1"),
        arguments: json!({ "patch": patch.into() }),
    }
}

async fn invoke(
    runtime: &GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
    bounds: &ToolPolicyBounds,
    call_id: impl Into<String>,
    patch: impl Into<String>,
) -> GuardedToolResult {
    runtime
        .invoke(
            invocation(call_id, patch),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await
}

fn completed(result: &GuardedToolResult) -> &serde_json::Value {
    let GuardedToolResult::Outcome {
        outcome: ToolOutcome::Completed {
            output: ToolOutput::Inline(output),
        },
        ..
    } = result
    else {
        panic!("expected completed inline result, got {result:?}")
    };
    output
}

#[tokio::test]
async fn one_patch_tool_adds_updates_and_deletes_without_shell() {
    let workspace = TestWorkspace::new("lifecycle");
    std::fs::create_dir_all(workspace.path("src")).unwrap();
    std::fs::write(
        workspace.path("src/lib.rs"),
        "pub fn value() -> u32 {\n    1\n}\n",
    )
    .unwrap();
    std::fs::write(workspace.path("obsolete.txt"), "remove me\n").unwrap();
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::NotRequired);

    let schemas = runtime.model_tool_schemas().unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "apply_patch");
    assert_eq!(
        schemas[0].input_schema["properties"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["patch"]
    );

    let result = invoke(
        &runtime,
        &bounds,
        "lifecycle",
        "*** Begin Patch\n*** Add File: src/new.rs\n+pub fn added() -> bool { true }\n*** Update File: src/lib.rs\n@@\n pub fn value() -> u32 {\n-    1\n+    2\n }\n*** Delete File: obsolete.txt\n*** End Patch",
    )
    .await;
    let output = completed(&result);
    assert_eq!(output["changed_files"], json!(3));
    assert_eq!(output["changes"][0]["operation"], json!("add"));
    assert_eq!(output["changes"][1]["operation"], json!("update"));
    assert_eq!(output["changes"][2]["operation"], json!("delete"));
    assert_eq!(
        std::fs::read_to_string(workspace.path("src/new.rs")).unwrap(),
        "pub fn added() -> bool { true }\n"
    );
    assert_eq!(
        std::fs::read_to_string(workspace.path("src/lib.rs")).unwrap(),
        "pub fn value() -> u32 {\n    2\n}\n"
    );
    assert!(!workspace.path("obsolete.txt").exists());
}

#[tokio::test]
async fn invalid_or_conflicting_batch_changes_zero_files() {
    let workspace = TestWorkspace::new("preflight");
    std::fs::write(workspace.path("stable.txt"), "stable\n").unwrap();
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::NotRequired);

    let result = invoke(
        &runtime,
        &bounds,
        "invalid-context",
        "*** Begin Patch\n*** Add File: created.txt\n+must not exist\n*** Update File: stable.txt\n@@\n-missing\n+changed\n*** End Patch",
    )
    .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            cached: false,
        } if code == "patch_conflict"
    ));
    assert!(!workspace.path("created.txt").exists());
    assert_eq!(
        std::fs::read_to_string(workspace.path("stable.txt")).unwrap(),
        "stable\n"
    );

    let malformed = invoke(
        &runtime,
        &bounds,
        "malformed",
        "*** Begin Patch\n*** Add File: malformed.txt\nmissing-prefix\n*** End Patch",
    )
    .await;
    assert!(matches!(
        malformed,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "patch_invalid"
    ));
    assert!(!workspace.path("malformed.txt").exists());
}

#[tokio::test]
async fn one_thousand_parse_and_precondition_mutations_change_zero_bytes() {
    let workspace = TestWorkspace::new("conflict-gate");
    let stable = workspace.path("stable.txt");
    let original = "alpha\nrepeat\nrepeat\nomega\n";
    std::fs::write(&stable, original).unwrap();
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::NotRequired);

    for index in 0..1_000 {
        let patch = match index % 7 {
            0 => "*** Begin Patch\n*** Add File: malformed.txt\nno-plus\n*** End Patch"
                .to_owned(),
            1 => "*** Begin Patch\n*** Update File: stable.txt\n@@\n-missing\n+changed\n*** End Patch"
                .to_owned(),
            2 => "*** Begin Patch\n*** Update File: stable.txt\n@@\n-repeat\n+changed\n*** End Patch"
                .to_owned(),
            3 => "*** Begin Patch\n*** Add File: stable.txt\n+overwrite\n*** End Patch"
                .to_owned(),
            4 => "*** Begin Patch\n*** Update File: absent.txt\n@@\n-old\n+new\n*** End Patch"
                .to_owned(),
            5 => "*** Begin Patch\n*** Delete File: absent.txt\n*** End Patch".to_owned(),
            _ => "*** Begin Patch\n*** Add File: missing/child.txt\n+new\n*** End Patch"
                .to_owned(),
        };
        let result = invoke(&runtime, &bounds, format!("conflict-{index}"), patch).await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { .. },
                ..
            }
        ));
        assert_eq!(std::fs::read_to_string(&stable).unwrap(), original);
    }
    assert_eq!(std::fs::read_dir(&workspace.root).unwrap().count(), 1);
}

#[tokio::test]
async fn one_hundred_replays_mutate_once_and_identity_conflicts_are_rejected() {
    let workspace = TestWorkspace::new("replay");
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::NotRequired);
    let patch = "*** Begin Patch\n*** Add File: once.txt\n+exactly once\n*** End Patch";

    for replay in 0..100 {
        let result = invoke(&runtime, &bounds, "same-call", patch).await;
        completed(&result);
        assert!(matches!(
            result,
            GuardedToolResult::Outcome { cached, .. } if cached == (replay > 0)
        ));
    }
    assert_eq!(
        std::fs::read_to_string(workspace.path("once.txt")).unwrap(),
        "exactly once\n"
    );

    let conflict = invoke(
        &runtime,
        &bounds,
        "same-call",
        "*** Begin Patch\n*** Add File: other.txt\n+different\n*** End Patch",
    )
    .await;
    assert!(matches!(
        conflict,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "call_identity_conflict"
    ));
    assert!(!workspace.path("other.txt").exists());
}

#[tokio::test]
async fn one_thousand_lexical_escape_mutations_write_zero_outside_bytes() {
    let workspace = TestWorkspace::new("lexical-escape");
    let outside = workspace
        .root
        .parent()
        .unwrap()
        .join(format!("orchestral-patch-outside-{}", uuid::Uuid::new_v4()));
    std::fs::write(&outside, "sentinel\n").unwrap();
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::NotRequired);

    for index in 0..1_000 {
        let path = match index % 5 {
            0 => format!("../outside-{index}.txt"),
            1 => format!("nested/../../outside-{index}.txt"),
            2 => format!("./inside-{index}.txt"),
            3 => format!("/tmp/orchestral-outside-{index}.txt"),
            _ => format!("nested\\outside-{index}.txt"),
        };
        let result = invoke(
            &runtime,
            &bounds,
            format!("escape-{index}"),
            format!("*** Begin Patch\n*** Add File: {path}\n+escape\n*** End Patch"),
        )
        .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { .. },
                ..
            }
        ));
    }
    assert_eq!(std::fs::read_to_string(&outside).unwrap(), "sentinel\n");
    std::fs::remove_file(outside).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn one_thousand_symlink_escape_mutations_write_zero_outside_bytes() {
    let workspace = TestWorkspace::new("symlink-escape");
    let outside = workspace.root.parent().unwrap().join(format!(
        "orchestral-patch-outside-dir-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&outside).unwrap();
    let secret = outside.join("secret.txt");
    std::fs::write(&secret, "sentinel\n").unwrap();
    std::os::unix::fs::symlink(&outside, workspace.path("escape")).unwrap();
    std::os::unix::fs::symlink(&secret, workspace.path("secret-link.txt")).unwrap();
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::NotRequired);

    for index in 0..1_000 {
        let (path, directive) = if index % 2 == 0 {
            (format!("escape/new-{index}.txt"), "Add")
        } else {
            ("secret-link.txt".to_owned(), "Update")
        };
        let body = if directive == "Add" {
            "+escape".to_owned()
        } else {
            "@@\n-sentinel\n+changed".to_owned()
        };
        let result = invoke(
            &runtime,
            &bounds,
            format!("symlink-{index}"),
            format!("*** Begin Patch\n*** {directive} File: {path}\n{body}\n*** End Patch"),
        )
        .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { ref code, .. },
                ..
            } if code == "patch_path_escape"
        ));
    }
    assert_eq!(std::fs::read_to_string(&secret).unwrap(), "sentinel\n");
    assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
    std::fs::remove_dir_all(outside).unwrap();
}

#[tokio::test]
async fn host_required_approval_is_exact_and_model_schema_has_no_authority() {
    let workspace = TestWorkspace::new("approval");
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::Required);
    let call = invocation(
        "approval-call",
        "*** Begin Patch\n*** Add File: approved.txt\n+approved\n*** End Patch",
    );
    let first = runtime
        .invoke(
            call.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::ApprovalRequired { binding, summary } = first else {
        panic!("expected exact approval request")
    };
    assert!(summary.contains("add approved.txt"));
    assert!(!summary.contains("approved\n"));
    assert!(!workspace.path("approved.txt").exists());

    let capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let result = runtime
        .invoke(
            call,
            RunToolGrant { bounds },
            Some(capability),
            CancellationToken::new(),
        )
        .await;
    completed(&result);
    assert_eq!(
        std::fs::read_to_string(workspace.path("approved.txt")).unwrap(),
        "approved\n"
    );
}

#[tokio::test]
async fn cancelled_before_dispatch_changes_zero_files() {
    let workspace = TestWorkspace::new("cancel");
    let (runtime, bounds) = patch_runtime(&workspace, ApprovalPolicy::NotRequired);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = runtime
        .invoke(
            invocation(
                "cancelled",
                "*** Begin Patch\n*** Add File: cancelled.txt\n+never\n*** End Patch",
            ),
            RunToolGrant { bounds },
            None,
            cancellation,
        )
        .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Cancelled,
            ..
        }
    ));
    assert!(!workspace.path("cancelled.txt").exists());
}
