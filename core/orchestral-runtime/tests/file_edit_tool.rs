use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use orchestral_core::agent_protocol::wire::{Digest, RunId};
use orchestral_core::tool_effect::InMemoryToolEffectJournalStore;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalIssuer,
    HostApprovalVerifier, HostToolPolicy, InMemoryApprovalCapabilityStore, NetworkPolicy,
    ProcessPolicy, RunToolGrant, SandboxPolicy, ToolCallId, ToolId, ToolInvocation, ToolOutcome,
    ToolOutput, ToolPolicyBounds, ToolRestriction,
};
use orchestral_runtime::{
    tools::{guarded_file_edit_descriptor, GuardedFileEditExecutor},
    GuardedToolResult, GuardedToolRuntime, WorkspacePermissionPolicy,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

const SIGNING_KEY: &[u8] = b"file-edit-test-signing-key-32bytes";
type Runtime = GuardedToolRuntime<InMemoryApprovalCapabilityStore>;

struct Workspace(PathBuf);

impl Workspace {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("orchestral-file-edit-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(std::fs::canonicalize(path).unwrap())
    }

    fn file(&self) -> PathBuf {
        self.0.join("source.rs")
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn bounds(roots: &[&Path], approval: ApprovalPolicy) -> ToolPolicyBounds {
    let roots: BTreeSet<_> = roots
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    ToolPolicyBounds {
        allowed_effects: BTreeSet::from([
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
        ]),
        approval,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: BTreeSet::from(["workspace".to_owned()]),
        },
        process: ProcessPolicy::default(),
        filesystem: FilesystemPolicy {
            readable_roots: roots.clone(),
            writable_roots: roots,
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(64 * 1024),
    }
}

fn runtime(primary: &Workspace, additional: &[&Path], policy: &ToolPolicyBounds) -> Runtime {
    runtime_with_journal(
        primary,
        additional,
        policy,
        Arc::new(InMemoryToolEffectJournalStore::default()),
    )
}

fn runtime_with_journal(
    primary: &Workspace,
    additional: &[&Path],
    policy: &ToolPolicyBounds,
    journal: Arc<InMemoryToolEffectJournalStore>,
) -> Runtime {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    let runtime = GuardedToolRuntime::new_with_effect_journal(
        HostToolPolicy {
            bounds: policy.clone(),
        },
        verifier,
        journal,
    )
    .unwrap()
    .with_permission_policy(Arc::new(WorkspacePermissionPolicy));
    runtime
        .register(
            guarded_file_edit_descriptor(ToolRestriction {
                bounds: policy.clone(),
            }),
            Arc::new(
                GuardedFileEditExecutor::new_with_roots(&primary.0, additional.iter().copied())
                    .unwrap(),
            ),
        )
        .unwrap();
    runtime
}

fn call(id: &str, arguments: Value) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new("file-edit-run"),
        call_id: ToolCallId::new(id),
        tool_id: ToolId::new("orchestral/file_edit/v1"),
        arguments,
    }
}

fn args(old: &str, new: &str) -> Value {
    json!({ "path": "source.rs", "old_text": old, "new_text": new })
}

async fn invoke(
    runtime: &Runtime,
    bounds: &ToolPolicyBounds,
    id: &str,
    arguments: Value,
) -> GuardedToolResult {
    runtime
        .invoke(
            call(id, arguments),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await
}

fn completed(result: &GuardedToolResult) -> &Value {
    let GuardedToolResult::Outcome {
        outcome: ToolOutcome::Completed {
            output: ToolOutput::Inline(value),
        },
        ..
    } = result
    else {
        panic!("expected completed edit, got {result:?}");
    };
    value
}

fn assert_rejected(result: &GuardedToolResult) {
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
}

#[tokio::test]
async fn file_edit_preserves_multiline_source_bytes_and_reports_real_digests() {
    for newline in ["\n", "\r\n"] {
        let workspace = Workspace::new();
        let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
        let runtime = runtime(&workspace, &[], &policy);
        let old = "\tlet key = \"λ\";\n\tformat!(\"\\\"{key}\\\"\")".replace('\n', newline);
        let new = "\tlet key = \"β\";\n\tformat!(\"[\\\"{key}\\\"]\")".replace('\n', newline);
        let before = format!(
            "// unchanged  {newline}fn quoted() -> String {{{newline}{old}{newline}}}{newline}"
        );
        let after = format!(
            "// unchanged  {newline}fn quoted() -> String {{{newline}{new}{newline}}}{newline}"
        );
        std::fs::write(workspace.file(), &before).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(workspace.file(), std::fs::Permissions::from_mode(0o640))
                .unwrap();
        }
        let result = invoke(&runtime, &policy, "replace", args(&old, &new)).await;
        let output = completed(&result);
        assert_eq!(std::fs::read(workspace.file()).unwrap(), after.as_bytes());
        assert_eq!(output["changed_files"], 1);
        assert_eq!(
            output["changes"][0]["before_digest"],
            json!(Digest::sha256(before.as_bytes()))
        );
        assert_eq!(
            output["changes"][0]["after_digest"],
            json!(Digest::sha256(after.as_bytes()))
        );
        assert_eq!(output["changes"][0]["bytes"], after.len());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(workspace.file())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o640
            );
        }
        assert_eq!(std::fs::read_dir(&workspace.0).unwrap().count(), 1);
    }
}

#[tokio::test]
async fn file_edit_rejects_missing_ambiguous_and_overlapping_text_without_normalization() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    for (index, (source, old, code)) in [
        ("aaa", "aa", "file_edit_ambiguous"),
        ("ééé", "éé", "file_edit_ambiguous"),
        ("same\nsame\n", "same", "file_edit_ambiguous"),
        ("  code\r\n", "code\n", "file_edit_no_match"),
        ("\tcode", "  code", "file_edit_no_match"),
        ("sentinel", "absent", "file_edit_no_match"),
    ]
    .into_iter()
    .enumerate()
    {
        std::fs::write(workspace.file(), source).unwrap();
        let result = invoke(
            &runtime,
            &policy,
            &format!("conflict-{index}"),
            args(old, "changed"),
        )
        .await;
        assert!(
            matches!(&result, GuardedToolResult::Outcome { outcome: ToolOutcome::Rejected { code: actual, .. }, .. } if actual == code),
            "{result:?}"
        );
        assert_eq!(std::fs::read(workspace.file()).unwrap(), source.as_bytes());
    }
}

#[tokio::test]
async fn file_edit_allows_deletion_and_unique_noop_but_not_creation() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    assert_rejected(&invoke(&runtime, &policy, "missing", args("old", "new")).await);
    assert!(!workspace.file().exists());
    std::fs::write(workspace.file(), "keep/remove/end").unwrap();
    assert_eq!(
        completed(&invoke(&runtime, &policy, "delete", args("remove/", "")).await)["changed_files"],
        1
    );
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "keep/end"
    );
    let result = invoke(&runtime, &policy, "noop", args("keep/", "keep/")).await;
    assert_eq!(completed(&result)["changed_files"], 0);
    assert_eq!(completed(&result)["changes"], json!([]));
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "keep/end"
    );
}

#[tokio::test]
async fn file_edit_rejects_invalid_fields_and_escaping_paths_before_writing() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    std::fs::write(workspace.file(), "original").unwrap();
    let mut invalid = vec![
        json!(null),
        json!([]),
        json!({}),
        args("", "changed"),
        args("original", "bad\0text"),
        args("bad\0text", "changed"),
        json!({"path":"source.rs", "old_text":"original"}),
        json!({"path":"source.rs", "old_text":1, "new_text":"changed"}),
        json!({"path":"source.rs", "old_text":"original", "new_text":false}),
    ];
    for workspace_value in [
        json!(null),
        json!(false),
        json!(""),
        json!(" "),
        json!(format!(" {}", workspace.0.display())),
    ] {
        let mut value = args("original", "changed");
        value["workspace"] = workspace_value;
        invalid.push(value);
    }
    for extra in ["patch", "expected_digest", "replace_all", "permission"] {
        let mut value = args("original", "changed");
        value[extra] = json!("model-supplied");
        invalid.push(value);
    }
    for path in [
        "../source.rs",
        "/source.rs",
        "./source.rs",
        "dir/../source.rs",
        "dir//source.rs",
        "dir\\source.rs",
        "source.rs\0",
    ] {
        let mut value = args("original", "changed");
        value["path"] = json!(path);
        invalid.push(value);
    }
    for (index, value) in invalid.into_iter().enumerate() {
        assert_rejected(&invoke(&runtime, &policy, &format!("invalid-{index}"), value).await);
        assert_eq!(
            std::fs::read_to_string(workspace.file()).unwrap(),
            "original"
        );
    }
    assert_eq!(std::fs::read_dir(&workspace.0).unwrap().count(), 1);
}

#[tokio::test]
async fn file_edit_enforces_text_and_resulting_file_size_limits() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    for (index, source) in [vec![b'x', 0xff], vec![b'x', 0]].into_iter().enumerate() {
        std::fs::write(workspace.file(), &source).unwrap();
        assert_rejected(
            &invoke(
                &runtime,
                &policy,
                &format!("binary-{index}"),
                args("x", "y"),
            )
            .await,
        );
        assert_eq!(std::fs::read(workspace.file()).unwrap(), source);
    }
    let mut source = vec![b'a'; 8 * 1024 * 1024];
    source[0] = b'x';
    std::fs::write(workspace.file(), &source).unwrap();
    assert_rejected(&invoke(&runtime, &policy, "too-large", args("x", "xx")).await);
    assert_eq!(std::fs::read(workspace.file()).unwrap(), source);
    assert_eq!(std::fs::read_dir(&workspace.0).unwrap().count(), 1);
}

#[tokio::test]
async fn file_edit_selects_only_host_workspaces_and_requires_both_read_and_write() {
    let primary = Workspace::new();
    let additional = Workspace::new();
    let foreign = Workspace::new();
    for workspace in [&primary, &additional, &foreign] {
        std::fs::write(workspace.file(), "original").unwrap();
    }
    let policy = bounds(&[&primary.0, &additional.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&primary, &[&additional.0], &policy);
    let mut selected = args("original", "selected");
    selected["workspace"] = json!(additional.0.to_string_lossy());
    completed(&invoke(&runtime, &policy, "select", selected).await);
    assert_eq!(
        std::fs::read_to_string(additional.file()).unwrap(),
        "selected"
    );
    assert_eq!(std::fs::read_to_string(primary.file()).unwrap(), "original");
    let mut unknown = args("original", "escape");
    unknown["workspace"] = json!(foreign.0.to_string_lossy());
    assert_rejected(&invoke(&runtime, &policy, "unknown", unknown).await);
    for (index, denied) in [EffectScope::FilesystemRead, EffectScope::FilesystemWrite]
        .into_iter()
        .enumerate()
    {
        let mut grant = policy.clone();
        grant.allowed_effects.remove(&denied);
        assert_rejected(
            &invoke(
                &runtime,
                &grant,
                &format!("denied-{index}"),
                args("original", "forbidden"),
            )
            .await,
        );
    }
    assert_eq!(std::fs::read_to_string(primary.file()).unwrap(), "original");
    assert_eq!(std::fs::read_to_string(foreign.file()).unwrap(), "original");
}

#[tokio::test]
async fn file_edit_replay_mutates_once_and_rejects_changed_arguments() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let first_runtime = runtime_with_journal(&workspace, &[], &policy, journal.clone());
    std::fs::write(workspace.file(), "needle").unwrap();
    // A second execution would append again; replay must return the saved result.
    let first = invoke(
        &first_runtime,
        &policy,
        "same-call",
        args("needle", "needle!"),
    )
    .await;
    drop(first_runtime);
    let runtime = runtime_with_journal(&workspace, &[], &policy, journal);
    let replay = invoke(&runtime, &policy, "same-call", args("needle", "needle!")).await;
    assert_eq!(completed(&first), completed(&replay));
    assert!(matches!(
        first,
        GuardedToolResult::Outcome { cached: false, .. }
    ));
    assert!(matches!(
        replay,
        GuardedToolResult::Outcome { cached: true, .. }
    ));
    let conflict = invoke(&runtime, &policy, "same-call", args("needle", "different")).await;
    assert!(
        matches!(conflict, GuardedToolResult::Outcome { outcome: ToolOutcome::Rejected { ref code, .. }, .. } if code == "call_identity_conflict")
    );
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "needle!"
    );
}

#[tokio::test]
async fn file_edit_host_approval_is_bound_to_the_exact_edit() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::Required);
    let runtime = runtime(&workspace, &[], &policy);
    std::fs::write(workspace.file(), "original").unwrap();
    let invocation = call("approved", args("original", "approved"));
    let pending = runtime
        .invoke(
            invocation.clone(),
            RunToolGrant {
                bounds: policy.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::ApprovalRequired { binding, summary } = pending else {
        panic!("expected approval, got {pending:?}")
    };
    assert!(summary.contains("source.rs"));
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "original"
    );
    let capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let denied = runtime
        .invoke(
            call("approved", args("original", "different")),
            RunToolGrant {
                bounds: policy.clone(),
            },
            Some(capability.clone()),
            CancellationToken::new(),
        )
        .await;
    assert_rejected(&denied);
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "original"
    );
    let result = runtime
        .invoke(
            invocation,
            RunToolGrant { bounds: policy },
            Some(capability),
            CancellationToken::new(),
        )
        .await;
    completed(&result);
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "approved"
    );
}

#[tokio::test]
async fn file_edit_cancelled_before_dispatch_leaves_no_mutation() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    std::fs::write(workspace.file(), "original").unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = runtime
        .invoke(
            call("cancelled", args("original", "changed")),
            RunToolGrant { bounds: policy },
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
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "original"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn file_edit_rejects_symlink_targets_and_parent_traversal() {
    let workspace = Workspace::new();
    let outside = Workspace::new();
    std::fs::write(outside.file(), "sentinel").unwrap();
    std::os::unix::fs::symlink(outside.file(), workspace.file()).unwrap();
    std::os::unix::fs::symlink(&outside.0, workspace.0.join("linked")).unwrap();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    for (index, path) in ["source.rs", "linked/source.rs"].into_iter().enumerate() {
        let mut arguments = args("sentinel", "escape");
        arguments["path"] = json!(path);
        assert_rejected(&invoke(&runtime, &policy, &format!("symlink-{index}"), arguments).await);
    }
    assert_eq!(std::fs::read_to_string(outside.file()).unwrap(), "sentinel");
}
