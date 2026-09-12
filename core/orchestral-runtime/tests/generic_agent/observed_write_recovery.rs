use super::*;
use std::{fs, path::Path, time::Duration};

use orchestral_core::tool_effect::{ToolEffectJournalStore, ToolEffectKey, ToolEffectPhase};
use orchestral_core::tool_protocol::{FilesystemPolicy, SandboxPolicy};
use orchestral_runtime::tools::{
    guarded_file_read_descriptor, guarded_file_write_descriptor, GuardedFileReadExecutor,
    GuardedFileWriteExecutor,
};
use orchestral_runtime::WorkspacePermissionPolicy;
use serde_json::Value;

const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const SOURCE: &str = "reviewed original\n";
const REPLACEMENT: &str = "approved replacement\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_unprepared_file_write_recovers_read_precondition_without_model_replay() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = fs::canonicalize(directory.path()).unwrap();
    let file = workspace.join("source.rs");
    fs::write(&file, SOURCE).unwrap();
    let policy = workspace_policy(&workspace);
    let run_id = RunId::new("unprepared-write-recovery");
    let session_id = AgentSessionId::new("unprepared-write-session");
    let checkpoints = Arc::new(AckLostAfterModelObservationCheckpointStore {
        target_round: Some(2),
        ..Default::default()
    });
    let effects = Arc::new(InMemoryToolEffectJournalStore::default());
    let sessions = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host = Arc::new(InMemoryAgentJournalStore::default());
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let first_model = Arc::new(ReadThenApprovedWriteModel {
        rounds: AtomicUsize::new(0),
    });
    let first_runtime = runtime(&workspace, &policy, effects.clone());
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            first_model.clone(),
            config.clone(),
            first_runtime.clone(),
            RunToolGrant {
                bounds: policy.clone(),
            },
            Arc::new(InMemoryHostApprovalBroker::new(SIGNING_KEY).unwrap()),
            sessions.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap()
        .with_checkpoint_store(checkpoints.clone())
        .unwrap(),
    );
    let controller = Arc::new(
        AgentController::with_journal_store(
            provider,
            ProviderBindingRef::new("unprepared-write-binding"),
            host.clone(),
        )
        .unwrap(),
    );
    controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text(
                    "Read the source and replace it after approval.",
                )],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        controller.wait_for_terminal(&run_id),
    )
    .await
    .unwrap()
    .expect_err("durable observation acknowledgement was lost");
    assert!(matches!(error, AgentControlError::ContinuityUnknown(ref id) if id == &run_id));
    assert_eq!(first_model.rounds.load(Ordering::SeqCst), 2);
    assert!(matches!(
        checkpoints
            .load_run(&run_id)
            .unwrap()
            .unwrap()
            .validate()
            .unwrap()
            .phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 2, .. }
    ));
    let key = ToolEffectKey::new(run_id.clone(), ToolCallId::new("write"));
    assert!(
        effects.load_effect(&key).await.unwrap().is_empty(),
        "write has no Prepared event"
    );
    assert_eq!(fs::read_to_string(&file).unwrap(), SOURCE);
    assert!(!controller
        .events(&run_id, 0)
        .await
        .unwrap()
        .iter()
        .any(|r| matches!(&r.event.payload, AgentEvent::RequestOpened { .. })));
    let session_before = sessions.load_session(&session_id).await.unwrap();
    drop(controller);
    drop(first_runtime);
    checkpoints.allow_recovery_writes();

    let broker = Arc::new(InMemoryHostApprovalBroker::new(SIGNING_KEY).unwrap());
    let replacement_model = Arc::new(ReadThenApprovedWriteModel {
        rounds: AtomicUsize::new(2),
    });
    let replacement_runtime = runtime(&workspace, &policy, effects.clone());
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            replacement_model.clone(),
            config,
            replacement_runtime.clone(),
            RunToolGrant { bounds: policy },
            broker.clone(),
            sessions.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap()
        .with_checkpoint_store(checkpoints.clone())
        .unwrap(),
    );
    let replacement = Arc::new(
        AgentController::with_journal_store(
            provider,
            ProviderBindingRef::new("unprepared-write-binding"),
            host,
        )
        .unwrap(),
    );
    replacement.recover(&run_id).await.unwrap();
    let pending = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let view = replacement.inspect(&run_id).await.unwrap();
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("recovered observation still requires real approval");
    assert_eq!(
        replacement_model.rounds.load(Ordering::SeqCst),
        2,
        "recovery must not resend the observed write model request"
    );
    assert_eq!(fs::read_to_string(&file).unwrap(), SOURCE);
    let prepared = replacement_runtime
        .inspect_effect(&key)
        .await
        .unwrap()
        .unwrap()
        .prepared;
    assert_eq!(prepared.invocation.arguments, write_arguments());
    assert!(prepared
        .invocation
        .arguments
        .get("expected_digest")
        .is_none());
    let resolution = prepared
        .argument_resolution
        .as_ref()
        .expect("recovered complete read supplies the precondition");
    assert_eq!(
        resolution.arguments["expected_digest"],
        json!(Digest::sha256(SOURCE.as_bytes()))
    );
    assert_eq!(
        resolution.source,
        ToolEffectKey::new(run_id.clone(), ToolCallId::new("read"))
    );
    assert_eq!(
        resolution.source_event_digest,
        effects
            .load_effect(&resolution.source)
            .await
            .unwrap()
            .last()
            .unwrap()
            .event_digest
    );
    let grant_ref = broker.approve(&pending.request_id, i64::MAX).unwrap();
    let command_id = CommandId::new("allow-recovered-write");
    let ack = replacement
        .command(
            AgentCommandEnvelope::new(
                command_id.clone(),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Approval {
                        decision: ApprovalDecision::Allow,
                        grant_ref: Some(grant_ref),
                    },
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    let view = tokio::time::timeout(
        Duration::from_secs(1),
        replacement.wait_for_terminal(&run_id),
    )
    .await
    .expect("approved recovered write completes")
    .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 3);
    assert_eq!(fs::read_to_string(&file).unwrap(), REPLACEMENT);
    let effect = replacement_runtime
        .inspect_effect(&key)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        effect.phase,
        ToolEffectPhase::Committed {
            outcome: ToolOutcome::Completed { .. },
            ..
        }
    ));
    assert_eq!(effect.prepared, prepared);
    let projection = checkpoints
        .load_run(&run_id)
        .unwrap()
        .unwrap()
        .validate()
        .unwrap();
    let binding = &projection.commands[&command_id]
        .approval_capability
        .as_ref()
        .unwrap()
        .claims
        .binding;
    assert_eq!(
        binding.args_digest,
        prepared.execution_invocation().args_digest().unwrap()
    );
    assert_ne!(binding.args_digest, prepared.args_digest);
    assert_eq!(projection.phase, GenericCheckpointPhase::Terminal);
    let session_after = sessions.load_session(&session_id).await.unwrap();
    assert_eq!(&session_after[..session_before.len()], &session_before);
    let calls = session_after
        .iter()
        .filter_map(|r| match &r.payload {
            AgentSessionEvent::ToolExchangeCommitted { assistant, .. } => Some(assistant),
            _ => None,
        })
        .flat_map(|m| &m.content)
        .filter_map(|c| match c {
            ModelContent::ToolCall {
                call_id, arguments, ..
            } if call_id.as_str() == "write" => Some(arguments),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(calls, vec![&write_arguments()]);
    let events = replacement.events(&run_id, 0).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|r| matches!(&r.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|r| matches!(&r.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
}

struct ReleasePausedCheckpoint(Arc<PausingCheckpointStore>);

impl Drop for ReleasePausedCheckpoint {
    fn drop(&mut self) {
        // A failing assertion must not leave the worker blocked on the crash cut.
        self.0.release_as_crash();
    }
}

fn write_arguments() -> Value {
    json!({"path":"source.rs", "mode":"replace", "content":REPLACEMENT})
}

struct ReadThenApprovedWriteModel {
    rounds: AtomicUsize,
}

#[async_trait]
impl ModelBackend for ReadThenApprovedWriteModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "read-then-approved-write".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..Default::default()
            },
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        let round = self.rounds.fetch_add(1, Ordering::SeqCst);
        let events =
            match round {
                0 => call_events("read", "file_read", json!({"path":"source.rs"})),
                1 => {
                    assert!(request.messages.iter().flat_map(|m| &m.content).any(
                        |block| matches!(
                            block, ModelContent::ToolResult {call_id, result, is_error:false}
                                if call_id.as_str() == "read" && result["content"] == SOURCE
                                    && result["eof"] == true && result["truncated"] == false
                        )
                    ));
                    call_events("write", "file_write", write_arguments())
                }
                2 => {
                    let calls = request
                        .messages
                        .iter()
                        .flat_map(|m| &m.content)
                        .filter_map(|block| match block {
                            ModelContent::ToolCall {
                                call_id, arguments, ..
                            } if call_id.as_str() == "write" => Some(arguments),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(calls, vec![&write_arguments()]);
                    assert!(request.messages.iter().flat_map(|m| &m.content).any(
                        |block| matches!(
                            block, ModelContent::ToolResult {call_id, is_error:false, ..}
                                if call_id.as_str() == "write"
                        )
                    ));
                    vec![
                        ModelEvent::TextDelta {
                            delta: "The approved update completed.".to_owned(),
                        },
                        ModelEvent::Finish {
                            reason: ModelFinishReason::Stop,
                        },
                    ]
                }
                _ => panic!("unexpected model replay"),
            };
        Ok(Box::pin(stream::iter(events.into_iter().enumerate().map(
            move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("observed-write-{round}-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            },
        ))))
    }
}

fn call_events(id: &str, name: &str, arguments: Value) -> Vec<ModelEvent> {
    let call_id = ModelToolCallId::new(id);
    vec![
        ModelEvent::ToolCallStart {
            call_id: call_id.clone(),
            name: name.to_owned(),
            extensions: Default::default(),
        },
        ModelEvent::ToolCallArgumentsDelta {
            call_id: call_id.clone(),
            delta: arguments.to_string(),
        },
        ModelEvent::ToolCallEnd { call_id },
        ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls,
        },
    ]
}

fn runtime(
    workspace: &Path,
    policy: &ToolPolicyBounds,
    journal: Arc<InMemoryToolEffectJournalStore>,
) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
    let runtime = Arc::new(
        GuardedToolRuntime::new_with_effect_journal(
            HostToolPolicy {
                bounds: policy.clone(),
            },
            HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
                .unwrap(),
            journal,
        )
        .unwrap()
        .with_permission_policy(Arc::new(WorkspacePermissionPolicy)),
    );
    runtime
        .register(
            guarded_file_read_descriptor(ToolRestriction {
                bounds: policy.clone(),
            }),
            Arc::new(GuardedFileReadExecutor::new(workspace).unwrap()),
        )
        .unwrap();
    let mut write_policy = policy.clone();
    write_policy.approval = ApprovalPolicy::Required;
    runtime
        .register(
            guarded_file_write_descriptor(ToolRestriction {
                bounds: write_policy,
            }),
            Arc::new(GuardedFileWriteExecutor::new(workspace).unwrap()),
        )
        .unwrap();
    runtime
}

fn workspace_policy(workspace: &Path) -> ToolPolicyBounds {
    let root = workspace.to_string_lossy().into_owned();
    ToolPolicyBounds {
        allowed_effects: BTreeSet::from([
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
        ]),
        approval: ApprovalPolicy::NotRequired,
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([root.clone()]),
            writable_roots: BTreeSet::from([root]),
        },
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: BTreeSet::from(["workspace_read".to_owned(), "workspace".to_owned()]),
        },
        max_timeout_ms: Some(5_000),
        max_output_bytes: Some(64 * 1024),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_observed_file_write_recovers_original_call_and_resolved_approval_without_rewrite(
) {
    let directory = tempfile::tempdir().unwrap();
    let workspace = fs::canonicalize(directory.path()).unwrap();
    let file = workspace.join("source.rs");
    fs::write(&file, SOURCE).unwrap();
    let policy = workspace_policy(&workspace);
    let run_id = RunId::new("observed-write-recovery");
    let session_id = AgentSessionId::new("observed-write-session");
    let command_id = CommandId::new("allow-observed-write");
    let checkpoints = Arc::new(PausingCheckpointStore::at(
        CheckpointCrashCut::ApprovalToolExchangeBoundary(3),
    ));
    let _release_on_failure = ReleasePausedCheckpoint(checkpoints.clone());
    let effects = Arc::new(InMemoryToolEffectJournalStore::default());
    let sessions = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host = Arc::new(InMemoryAgentJournalStore::default());
    let first_runtime = runtime(&workspace, &policy, effects.clone());
    let broker = Arc::new(InMemoryHostApprovalBroker::new(SIGNING_KEY).unwrap());
    let first_model = Arc::new(ReadThenApprovedWriteModel {
        rounds: AtomicUsize::new(0),
    });
    let config = GenericAgentConfig::new("internal-provider", "generic-agent");
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            first_model.clone(),
            config.clone(),
            first_runtime.clone(),
            RunToolGrant {
                bounds: policy.clone(),
            },
            broker.clone(),
            sessions.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap()
        .with_checkpoint_store(checkpoints.clone())
        .unwrap(),
    );
    let controller = Arc::new(
        AgentController::with_journal_store(
            provider,
            ProviderBindingRef::new("observed-write-binding"),
            host.clone(),
        )
        .unwrap(),
    );
    controller
        .start(
            AgentRunEnvelope::new(
                AGENT_PROTOCOL_V1,
                session_id.clone(),
                run_id.clone(),
                vec![Content::text(
                    "Read the source and replace it after approval.",
                )],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let pending = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let view = controller.inspect(&run_id).await.unwrap();
            if let Some(request) = view.pending_requests.first() {
                break request.clone();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write opens an approval after the complete read");
    assert_eq!(first_model.rounds.load(Ordering::SeqCst), 2);
    assert_eq!(fs::read_to_string(&file).unwrap(), SOURCE);
    let key = ToolEffectKey::new(run_id.clone(), ToolCallId::new("write"));
    let prepared = first_runtime
        .inspect_effect(&key)
        .await
        .unwrap()
        .unwrap()
        .prepared;
    assert_eq!(prepared.invocation.arguments, write_arguments());
    assert!(prepared
        .invocation
        .arguments
        .get("expected_digest")
        .is_none());
    let resolution = prepared
        .argument_resolution
        .as_ref()
        .expect("committed read supplies the precondition");
    assert_eq!(
        resolution.arguments["expected_digest"],
        json!(Digest::sha256(SOURCE.as_bytes()))
    );
    assert_eq!(
        resolution.source,
        ToolEffectKey::new(run_id.clone(), ToolCallId::new("read"))
    );
    let read_records = effects.load_effect(&resolution.source).await.unwrap();
    assert_eq!(
        resolution.source_event_digest,
        read_records.last().unwrap().event_digest
    );
    let paused = checkpoints.paused.notified();
    let grant_ref = broker.approve(&pending.request_id, i64::MAX).unwrap();
    let ack = controller
        .command(
            AgentCommandEnvelope::new(
                command_id.clone(),
                run_id.clone(),
                Some(pending.request_id),
                AgentCommand::ResolveRequest {
                    response: RequestResolution::Approval {
                        decision: ApprovalDecision::Allow,
                        grant_ref: Some(grant_ref),
                    },
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(ack.state, CommandAckState::Accepted { .. }));
    tokio::time::timeout(Duration::from_secs(1), paused)
        .await
        .expect("pause after committed write exchange");
    let projection = checkpoints
        .load_run(&run_id)
        .unwrap()
        .unwrap()
        .validate()
        .unwrap();
    assert!(matches!(
        projection.phase,
        GenericCheckpointPhase::ModelAttemptObserved { round: 2, .. }
    ));
    let binding = &projection.commands[&command_id]
        .approval_capability
        .as_ref()
        .unwrap()
        .claims
        .binding;
    assert_eq!(
        binding.args_digest,
        prepared.execution_invocation().args_digest().unwrap()
    );
    assert_ne!(binding.args_digest, prepared.args_digest);
    assert_eq!(
        prepared.args_digest,
        prepared.invocation.args_digest().unwrap()
    );
    let effect = first_runtime.inspect_effect(&key).await.unwrap().unwrap();
    assert!(matches!(
        effect.phase,
        ToolEffectPhase::Committed {
            outcome: ToolOutcome::Completed { .. },
            ..
        }
    ));
    assert_eq!(effect.prepared, prepared);
    let effects_before = effects.load_effect(&key).await.unwrap();
    let session_before = sessions.load_session(&session_id).await.unwrap();
    assert_eq!(fs::read_to_string(&file).unwrap(), REPLACEMENT);
    checkpoints.release_as_crash();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        controller.wait_for_terminal(&run_id),
    )
    .await
    .unwrap()
    .expect_err("private loop boundary was not committed");
    assert!(matches!(error, AgentControlError::ContinuityUnknown(ref id) if id == &run_id));
    drop(controller);
    drop(first_runtime);
    checkpoints.allow_recovery_writes();

    // A cached Committed result must not overwrite a subsequent independent edit.
    fs::write(&file, "external edit after committed write\n").unwrap();
    let replacement_model = Arc::new(ReadThenApprovedWriteModel {
        rounds: AtomicUsize::new(2),
    });
    let replacement_runtime = runtime(&workspace, &policy, effects.clone());
    let provider = Arc::new(
        InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            replacement_model.clone(),
            config,
            replacement_runtime,
            RunToolGrant { bounds: policy },
            Arc::new(InMemoryHostApprovalBroker::new(SIGNING_KEY).unwrap()),
            sessions.clone(),
            Arc::new(JsonSizeTokenMeter::default()),
        )
        .unwrap()
        .with_checkpoint_store(checkpoints.clone())
        .unwrap(),
    );
    let replacement = Arc::new(
        AgentController::with_journal_store(
            provider,
            ProviderBindingRef::new("observed-write-binding"),
            host,
        )
        .unwrap(),
    );
    replacement.recover(&run_id).await.unwrap();
    let view = tokio::time::timeout(
        Duration::from_secs(1),
        replacement.wait_for_terminal(&run_id),
    )
    .await
    .expect("cached write advances without approval or re-execution")
    .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert!(view.pending_requests.is_empty());
    assert_eq!(replacement_model.rounds.load(Ordering::SeqCst), 3);
    assert_eq!(
        fs::read_to_string(&file).unwrap(),
        "external edit after committed write\n"
    );
    assert_eq!(effects.load_effect(&key).await.unwrap(), effects_before);
    let session_after = sessions.load_session(&session_id).await.unwrap();
    assert_eq!(&session_after[..session_before.len()], &session_before);
    let writes = session_after
        .iter()
        .filter_map(|record| match &record.payload {
            AgentSessionEvent::ToolExchangeCommitted { assistant, .. } => Some(assistant),
            _ => None,
        })
        .flat_map(|assistant| &assistant.content)
        .filter_map(|block| match block {
            ModelContent::ToolCall {
                call_id, arguments, ..
            } if call_id.as_str() == "write" => Some(arguments),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(writes, vec![&write_arguments()]);
    let events = replacement.events(&run_id, 0).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|r| matches!(&r.event.payload, AgentEvent::RequestOpened { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|r| matches!(&r.event.payload, AgentEvent::RequestResolved { .. }))
            .count(),
        1
    );
    assert!(matches!(
        replacement
            .command_ack(&run_id, &command_id)
            .await
            .unwrap()
            .state,
        CommandAckState::Applied { .. }
    ));
    assert_eq!(
        checkpoints
            .load_run(&run_id)
            .unwrap()
            .unwrap()
            .validate()
            .unwrap()
            .phase,
        GenericCheckpointPhase::Terminal
    );
}
