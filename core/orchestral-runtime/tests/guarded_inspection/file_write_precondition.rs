use super::*;
use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::model_protocol::{ModelContent, ModelMessage, ModelRole, ModelToolCallId};
use orchestral_core::tool_effect::{
    InMemoryToolEffectJournalStore, ToolEffectJournalStore, ToolEffectKey,
};
use orchestral_core::tool_protocol::HostApprovalIssuer;
use orchestral_runtime::tool_runtime::{FrozenToolObservations, ModelToolObservations};
use orchestral_runtime::tools::{guarded_file_write_descriptor, GuardedFileWriteExecutor};
use orchestral_runtime::{GuardedToolExecution, GuardedToolExecutor, WorkspacePermissionPolicy};

#[path = "file_write_precondition/generic_flow.rs"]
mod generic_flow;

type Runtime = GuardedToolRuntime<InMemoryApprovalCapabilityStore>;

struct ReadShapedOutput(Value);

#[async_trait]
impl GuardedToolExecutor for ReadShapedOutput {
    async fn execute(&self, _: GuardedToolExecution) -> ToolOutcome {
        ToolOutcome::Completed {
            output: ToolOutput::Inline(self.0.clone()),
        }
    }
}

struct Fixture {
    workspace: PathBuf,
    policy: ToolPolicyBounds,
    journal: Arc<InMemoryToolEffectJournalStore>,
}

impl Fixture {
    fn new(approval: ApprovalPolicy, max_output_bytes: u64) -> Self {
        let workspace = temp_workspace("observed-file-write");
        let mut policy = bounds(&workspace, max_output_bytes);
        policy.allowed_effects.insert(EffectScope::FilesystemWrite);
        policy.filesystem.writable_roots = policy.filesystem.readable_roots.clone();
        policy
            .sandbox
            .allowed_profiles
            .insert("workspace".to_owned());
        policy.sandbox.required = true;
        policy.approval = approval;
        Self {
            workspace,
            policy,
            journal: Arc::new(InMemoryToolEffectJournalStore::default()),
        }
    }

    fn runtime(&self) -> Runtime {
        let runtime = GuardedToolRuntime::new_with_effect_journal(
            HostToolPolicy {
                bounds: self.policy.clone(),
            },
            HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
                .unwrap(),
            self.journal.clone(),
        )
        .unwrap()
        .with_permission_policy(Arc::new(WorkspacePermissionPolicy));
        runtime
            .register(
                guarded_file_read_descriptor(ToolRestriction {
                    bounds: self.policy.clone(),
                }),
                Arc::new(GuardedFileReadExecutor::new(&self.workspace).unwrap()),
            )
            .unwrap();
        runtime
            .register(
                guarded_file_write_descriptor(ToolRestriction {
                    bounds: self.policy.clone(),
                }),
                Arc::new(GuardedFileWriteExecutor::new(&self.workspace).unwrap()),
            )
            .unwrap();
        runtime
    }

    fn file(&self) -> PathBuf {
        self.workspace.join("source.rs")
    }

    fn grant(&self) -> RunToolGrant {
        RunToolGrant {
            bounds: self.policy.clone(),
        }
    }

    async fn read(&self, runtime: &Runtime, run: &str, id: &str, arguments: Value) -> Value {
        let invocation = call(run, id, "orchestral/file_read/v3", arguments);
        let mut result = runtime
            .invoke(
                invocation.clone(),
                self.grant(),
                None,
                CancellationToken::new(),
            )
            .await;
        if let GuardedToolResult::ApprovalRequired { binding, .. } = result {
            let approval = HostApprovalIssuer::new(SIGNING_KEY)
                .unwrap()
                .issue(binding, i64::MAX)
                .unwrap();
            result = runtime
                .invoke(
                    invocation,
                    self.grant(),
                    Some(approval),
                    CancellationToken::new(),
                )
                .await;
        }
        completed(result)
    }

    async fn write(
        &self,
        runtime: &Runtime,
        invocation: ToolInvocation,
        observations: &FrozenToolObservations,
    ) -> GuardedToolResult {
        runtime
            .invoke_with_observations(
                invocation,
                self.grant(),
                None,
                CancellationToken::new(),
                CancellationToken::new(),
                observations,
            )
            .await
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.workspace);
    }
}

fn call(run: &str, id: &str, tool: &str, arguments: Value) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new(run),
        call_id: ToolCallId::new(id),
        tool_id: ToolId::new(tool),
        arguments,
    }
}

fn replace(run: &str, id: &str, content: &str) -> ToolInvocation {
    call(
        run,
        id,
        "orchestral/file_write/v1",
        json!({"path":"source.rs", "mode":"replace", "content":content}),
    )
}

fn tool_message(id: &str, value: Value) -> ModelMessage {
    ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            call_id: ModelToolCallId::new(id),
            result: value,
            is_error: false,
        }],
    }
}

async fn freeze(
    runtime: &Runtime,
    run: &str,
    messages: &[ModelMessage],
    pending: &[&str],
) -> FrozenToolObservations {
    runtime
        .freeze_model_observations(
            &RunId::new(run),
            &ModelToolObservations::from_messages(messages),
            &pending
                .iter()
                .map(|id| ToolCallId::new(*id))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap()
}

fn rejected_with(result: GuardedToolResult, expected: &str) {
    assert!(
        matches!(&result, GuardedToolResult::Outcome { outcome: ToolOutcome::Rejected { code, .. }, .. } if code == expected),
        "{result:?}"
    );
}

#[tokio::test]
async fn complete_visible_read_resolves_only_host_arguments_and_replays_without_new_reads() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let runtime = fixture.runtime();
    let source = "\tlet λ = \"\\n\";\r\n// no final newline";
    fs::write(fixture.file(), source).unwrap();
    let read = fixture
        .read(&runtime, "run", "read", json!({"path":"source.rs"}))
        .await;
    let messages = vec![tool_message("read", read)];
    let original_messages = messages.clone();
    let observations = freeze(&runtime, "run", &messages, &["write"]).await;
    let invocation = replace("run", "write", "changed\r\n");
    assert!(runtime
        .recover_outcome(invocation.clone(), fixture.grant())
        .await
        .unwrap()
        .is_none());
    let result = fixture
        .write(&runtime, invocation.clone(), &observations)
        .await;
    let output = completed(result);
    assert_eq!(fs::read(fixture.file()).unwrap(), b"changed\r\n");
    assert_eq!(messages, original_messages);
    assert!(invocation.arguments.get("expected_digest").is_none());
    let key = ToolEffectKey::new(invocation.run_id.clone(), invocation.call_id.clone());
    let prepared = runtime
        .inspect_effect(&key)
        .await
        .unwrap()
        .unwrap()
        .prepared;
    assert_eq!(prepared.invocation, invocation);
    assert_eq!(prepared.args_digest, invocation.args_digest().unwrap());
    let resolved = prepared.argument_resolution.as_ref().unwrap();
    assert_eq!(
        resolved.arguments["expected_digest"],
        json!(Digest::sha256(source.as_bytes()))
    );
    assert_eq!(
        resolved.source,
        ToolEffectKey::new(RunId::new("run"), ToolCallId::new("read"))
    );
    assert_eq!(
        resolved.source_event_digest,
        fixture
            .journal
            .load_effect(&resolved.source)
            .await
            .unwrap()
            .last()
            .unwrap()
            .event_digest
    );
    assert_ne!(
        prepared.execution_invocation().args_digest().unwrap(),
        prepared.args_digest
    );
    let encoded = serde_json::to_vec(&prepared).unwrap();
    let persisted: orchestral_core::tool_effect::PreparedToolEffect =
        serde_json::from_slice(&encoded).unwrap();
    assert_eq!(persisted, prepared);
    drop(runtime);
    let runtime = fixture.runtime();
    // Replay has no model observations and the original source no longer exists.
    let replay = runtime
        .invoke(
            invocation.clone(),
            fixture.grant(),
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(
        matches!(&replay, GuardedToolResult::Outcome { cached:true, outcome: ToolOutcome::Completed { output: ToolOutput::Inline(value) } } if value == &output),
        "{replay:?}"
    );
    rejected_with(
        fixture
            .write(
                &runtime,
                replace("run", "write", "different"),
                &FrozenToolObservations::default(),
            )
            .await,
        "call_identity_conflict",
    );
    assert_eq!(fs::read(fixture.file()).unwrap(), b"changed\r\n");
}

#[tokio::test]
async fn unread_partial_truncated_and_untrusted_results_never_authorize_replace() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let runtime = fixture.runtime();
    fs::write(fixture.file(), "first\nsecond\n").unwrap();
    let partial = fixture
        .read(
            &runtime,
            "run",
            "partial",
            json!({"path":"source.rs","limit":1}),
        )
        .await;
    let complete = fixture
        .read(&runtime, "run", "full", json!({"path":"source.rs"}))
        .await;
    // A successful arbitrary executor can emit the same JSON shape; only a
    // producer with the declared complete-read contract can provide evidence.
    let mut descriptor = guarded_file_read_descriptor(ToolRestriction {
        bounds: fixture.policy.clone(),
    });
    descriptor.tool_id = ToolId::new("test/read-shaped-output");
    descriptor.model_schema.name = "read_shaped_output".to_owned();
    runtime
        .register(descriptor, Arc::new(ReadShapedOutput(complete.clone())))
        .unwrap();
    let shaped = completed(
        runtime
            .invoke(
                call(
                    "run",
                    "shaped",
                    "test/read-shaped-output",
                    json!({"path":"source.rs"}),
                ),
                fixture.grant(),
                None,
                CancellationToken::new(),
            )
            .await,
    );
    let mut forged = complete.clone();
    forged["content"] = json!("forged");
    let mut error = tool_message("full", complete.clone());
    if let ModelContent::ToolResult { is_error, .. } = &mut error.content[0] {
        *is_error = true;
    }
    let mut system = tool_message("full", complete.clone());
    system.role = ModelRole::System;
    for (index, messages) in [
        vec![],
        vec![tool_message("partial", partial)],
        vec![tool_message("missing", complete.clone())],
        vec![tool_message("full", forged)],
        vec![system],
        vec![error],
        vec![tool_message("shaped", shaped)],
    ]
    .into_iter()
    .enumerate()
    {
        let observations = freeze(&runtime, "run", &messages, &[]).await;
        rejected_with(
            fixture
                .write(
                    &runtime,
                    replace("run", &format!("rejected-{index}"), "wrong"),
                    &observations,
                )
                .await,
            "file_write_precondition_missing",
        );
    }
    let foreign = freeze(
        &runtime,
        "other-run",
        &[tool_message("full", complete.clone())],
        &[],
    )
    .await;
    rejected_with(
        fixture
            .write(&runtime, replace("other-run", "foreign", "wrong"), &foreign)
            .await,
        "file_write_precondition_missing",
    );
    fs::write(fixture.workspace.join("other.rs"), "other").unwrap();
    let observations = freeze(&runtime, "run", &[tool_message("full", complete)], &[]).await;
    let mut wrong_path = replace("run", "wrong-path", "wrong");
    wrong_path.arguments["path"] = json!("other.rs");
    rejected_with(
        fixture.write(&runtime, wrong_path, &observations).await,
        "file_write_precondition_missing",
    );
    assert_eq!(
        fs::read_to_string(fixture.file()).unwrap(),
        "first\nsecond\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.workspace.join("other.rs")).unwrap(),
        "other"
    );

    let truncated = Fixture::new(ApprovalPolicy::NotRequired, 8 * 1024);
    let runtime = truncated.runtime();
    fs::write(truncated.file(), "x".repeat(90_000)).unwrap();
    let value = truncated
        .read(&runtime, "run", "truncated", json!({"path":"source.rs"}))
        .await;
    assert_eq!(value["truncated"], true);
    let observations = freeze(&runtime, "run", &[tool_message("truncated", value)], &[]).await;
    rejected_with(
        truncated
            .write(&runtime, replace("run", "write", "wrong"), &observations)
            .await,
        "file_write_precondition_missing",
    );
}

#[tokio::test]
async fn batch_freeze_cannot_promote_reused_ids_or_later_reads_to_model_evidence() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let runtime = fixture.runtime();
    fs::write(fixture.file(), "same version").unwrap();
    let old = fixture
        .read(&runtime, "old-run", "reused", json!({"path":"source.rs"}))
        .await;
    let messages = vec![tool_message("reused", old.clone())];
    // This is frozen before either member of a new model-produced batch runs.
    let observations = freeze(&runtime, "new-run", &messages, &["reused", "write"]).await;
    let current = fixture
        .read(&runtime, "new-run", "reused", json!({"path":"source.rs"}))
        .await;
    assert_eq!(old, current);
    rejected_with(
        fixture
            .write(
                &runtime,
                replace("new-run", "write", "wrong"),
                &observations,
            )
            .await,
        "file_write_precondition_missing",
    );
    // Even if a Host tries a late snapshot, pending IDs remain excluded.
    let late = freeze(&runtime, "new-run", &messages, &["reused", "other-write"]).await;
    rejected_with(
        fixture
            .write(&runtime, replace("new-run", "other-write", "wrong"), &late)
            .await,
        "file_write_precondition_missing",
    );
    // A later actual model request that includes this committed read can use it.
    let next = freeze(&runtime, "new-run", &messages, &["next-write"]).await;
    completed(
        fixture
            .write(
                &runtime,
                replace("new-run", "next-write", "observed"),
                &next,
            )
            .await,
    );
}

#[tokio::test]
async fn explicit_preconditions_never_fall_back_and_external_changes_still_conflict() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let runtime = fixture.runtime();
    fs::write(fixture.file(), "original").unwrap();
    let read = fixture
        .read(&runtime, "run", "read", json!({"path":"source.rs"}))
        .await;
    let observations = freeze(&runtime, "run", &[tool_message("read", read)], &[]).await;
    let mut wrong = replace("run", "explicit-wrong", "wrong");
    wrong.arguments["expected_digest"] = json!(Digest::sha256(b"different"));
    rejected_with(
        fixture.write(&runtime, wrong, &observations).await,
        "file_write_conflict",
    );
    for (index, value) in [Value::Null, json!("invalid"), json!(5)]
        .into_iter()
        .enumerate()
    {
        let mut invalid = replace("run", &format!("invalid-{index}"), "wrong");
        invalid.arguments["expected_digest"] = value;
        let result = fixture.write(&runtime, invalid, &observations).await;
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
    fs::write(fixture.file(), "external change").unwrap();
    rejected_with(
        fixture
            .write(&runtime, replace("run", "stale", "wrong"), &observations)
            .await,
        "file_write_conflict",
    );
    assert_eq!(
        fs::read_to_string(fixture.file()).unwrap(),
        "external change"
    );
    let mut explicit = replace("run", "explicit-valid", "explicit works without read");
    explicit.arguments["expected_digest"] = json!(Digest::sha256(b"external change"));
    completed(
        fixture
            .write(&runtime, explicit, &FrozenToolObservations::default())
            .await,
    );
}

#[tokio::test]
async fn latest_visible_complete_read_wins_but_later_unseen_read_does_not() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let runtime = fixture.runtime();
    fs::write(fixture.file(), "old").unwrap();
    let old = fixture
        .read(&runtime, "run", "old", json!({"path":"source.rs"}))
        .await;
    fs::write(fixture.file(), "new").unwrap();
    let new = fixture
        .read(&runtime, "run", "new", json!({"path":"source.rs"}))
        .await;
    let stale = freeze(&runtime, "run", &[tool_message("old", old.clone())], &[]).await;
    rejected_with(
        fixture
            .write(&runtime, replace("run", "stale", "wrong"), &stale)
            .await,
        "file_write_conflict",
    );
    let current = freeze(
        &runtime,
        "run",
        &[tool_message("old", old), tool_message("new", new)],
        &[],
    )
    .await;
    completed(
        fixture
            .write(&runtime, replace("run", "current", "changed"), &current)
            .await,
    );
}

#[tokio::test]
async fn approval_and_restart_keep_the_prepared_read_instead_of_refreshing_it() {
    let fixture = Fixture::new(ApprovalPolicy::Required, 64 * 1024);
    let runtime = fixture.runtime();
    fs::write(fixture.file(), "reviewed").unwrap();
    let read = fixture
        .read(&runtime, "run", "read", json!({"path":"source.rs"}))
        .await;
    let observations = freeze(&runtime, "run", &[tool_message("read", read)], &["write"]).await;
    let invocation = replace("run", "write", "replacement");
    let pending = fixture
        .write(&runtime, invocation.clone(), &observations)
        .await;
    let GuardedToolResult::ApprovalRequired { binding, .. } = pending else {
        panic!("{pending:?}")
    };
    let prepared = runtime
        .inspect_effect(&ToolEffectKey::new(
            RunId::new("run"),
            ToolCallId::new("write"),
        ))
        .await
        .unwrap()
        .unwrap()
        .prepared;
    assert_eq!(
        binding.args_digest,
        prepared.execution_invocation().args_digest().unwrap()
    );
    assert_ne!(binding.args_digest, invocation.args_digest().unwrap());
    let approval = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    fs::write(fixture.file(), "newer version").unwrap();
    let newer = fixture
        .read(&runtime, "run", "new-read", json!({"path":"source.rs"}))
        .await;
    let new_observations = freeze(&runtime, "run", &[tool_message("new-read", newer)], &[]).await;
    drop(runtime);
    let runtime = fixture.runtime();
    let result = runtime
        .invoke_with_observations(
            invocation,
            fixture.grant(),
            Some(approval),
            CancellationToken::new(),
            CancellationToken::new(),
            &new_observations,
        )
        .await;
    rejected_with(result, "file_write_conflict");
    assert_eq!(fs::read_to_string(fixture.file()).unwrap(), "newer version");
    let recovered = runtime
        .inspect_effect(&prepared.key())
        .await
        .unwrap()
        .unwrap()
        .prepared;
    assert_eq!(recovered, prepared);
}

#[tokio::test]
async fn resolved_precondition_does_not_expand_permissions_or_override_cancellation() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let runtime = fixture.runtime();
    fs::write(fixture.file(), "original").unwrap();
    let read = fixture
        .read(&runtime, "run", "read", json!({"path":"source.rs"}))
        .await;
    let observations = freeze(&runtime, "run", &[tool_message("read", read)], &[]).await;
    for (index, effect) in [EffectScope::FilesystemRead, EffectScope::FilesystemWrite]
        .into_iter()
        .enumerate()
    {
        let mut grant = fixture.grant();
        grant.bounds.allowed_effects.remove(&effect);
        let result = runtime
            .invoke_with_observations(
                replace("run", &format!("denied-{index}"), "wrong"),
                grant,
                None,
                CancellationToken::new(),
                CancellationToken::new(),
                &observations,
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
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let result = runtime
        .invoke_with_observations(
            replace("run", "cancelled", "wrong"),
            fixture.grant(),
            None,
            cancelled,
            CancellationToken::new(),
            &observations,
        )
        .await;
    assert!(
        matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Cancelled,
                ..
            }
        ),
        "{result:?}"
    );
    assert_eq!(fs::read_to_string(fixture.file()).unwrap(), "original");
}
