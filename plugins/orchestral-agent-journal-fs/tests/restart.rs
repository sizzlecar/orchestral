use std::sync::Arc;

use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_agent_protocol_testkit::{
    ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, TestProbes,
};
use orchestral_core::agent_protocol::{
    reference::{AgentRunReducer, AgentRunStatus},
    spi::{AgentJournalStore, AgentRunRegistration, StoredAgentRun},
    wire::{
        AgentAdmission, AgentCapabilities, AgentDescriptor, AgentDescriptorEnvelope, AgentEvent,
        AgentEventDraft, AgentEventId, AgentExecutionRef, AgentId, AgentProviderId,
        AgentRunEnvelope, AgentSessionId, AgentStartRequest, Content, Digest, PendingRequest,
        PendingRequestKind, PendingRequestPayload, ProviderBindingRef, RequestId, RunId,
    },
    AGENT_PROTOCOL_V1,
};

#[test]
fn single_writer_lease_rejects_competing_control_and_releases_on_drop() {
    let root = std::env::temp_dir().join(format!(
        "orchestral-agent-writer-lease-test-{}",
        uuid::Uuid::new_v4()
    ));
    let writer = FileAgentJournalStore::open_single_writer(&root).expect("writer acquires lease");
    let error = FileAgentJournalStore::open_single_writer(&root)
        .err()
        .expect("second writer is rejected");
    assert!(error.to_string().contains("active control writer"));
    FileAgentJournalStore::open_read_only(&root).expect("read-only discovery remains available");

    drop(writer);
    FileAgentJournalStore::open_single_writer(&root).expect("lease releases with writer");
    std::fs::remove_dir_all(root).expect("temporary journal cleans up");
}

#[tokio::test]
async fn read_only_discovery_controller_cannot_create_a_control_run() {
    let root = std::env::temp_dir().join(format!(
        "orchestral-agent-read-only-test-{}",
        uuid::Uuid::new_v4()
    ));
    let factory = ScriptedStatelessFactory::conformant().expect("fixture descriptor");
    let scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    let store = Arc::new(FileAgentJournalStore::open_read_only(&root).expect("reader opens"));
    let controller = Arc::new(
        AgentController::with_journal_store(
            factory.create(scenario.clone(), TestProbes::default()),
            ProviderBindingRef::new("filesystem-binding"),
            store,
        )
        .expect("controller binds"),
    );
    let error = controller
        .start(scenario.start_request.run)
        .await
        .expect_err("read-only controller cannot register a Run");
    assert!(error.to_string().contains("open read-only"));
    std::fs::remove_dir_all(root).expect("temporary journal cleans up");
}

#[tokio::test]
async fn legacy_approval_digest_is_verified_and_upgraded_without_hiding_the_run() {
    let root = std::env::temp_dir().join(format!(
        "orchestral-agent-journal-legacy-test-{}",
        uuid::Uuid::new_v4()
    ));
    let mut capabilities = AgentCapabilities::default();
    capabilities
        .pending_request_kinds
        .insert(PendingRequestKind::Approval);
    let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
        provider_id: AgentProviderId::new("test/legacy-journal"),
        agent_id: AgentId::new("legacy-journal-v1"),
        supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
        accepted_content_types: std::collections::BTreeSet::from(["text/plain".to_owned()]),
        capabilities,
        extensions: Default::default(),
    })
    .expect("descriptor seals");
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("legacy-session"),
        RunId::new("legacy-run"),
        vec![Content::text("exercise durable compatibility")],
    )
    .expect("Run seals");
    let request =
        AgentStartRequest::new(run, ProviderBindingRef::new("legacy-binding"), &descriptor)
            .expect("start request is valid");
    let execution =
        AgentExecutionRef::for_start(&request, &descriptor).expect("execution reference is valid");
    let admission = AgentAdmission::default();
    let mut reducer =
        AgentRunReducer::new(execution.clone(), &request, &descriptor, admission.clone())
            .expect("reducer starts");
    let opened = reducer
        .apply_provider_draft(AgentEventDraft {
            event_id: AgentEventId::new("legacy-approval-opened"),
            run_id: execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: RequestId::new("legacy-approval"),
                    blocking: true,
                    payload: PendingRequestPayload::Approval {
                        operation_digest: Digest::sha256("legacy-operation"),
                        requested_scope: vec!["filesystem_read".to_owned()],
                        session_approval_scope: None,
                        reason: "verify a legacy approval record".to_owned(),
                    },
                },
            },
        })
        .expect("approval event is sequenced");
    let stored = StoredAgentRun {
        registration: AgentRunRegistration {
            request,
            execution: execution.clone(),
            admission,
        },
        records: vec![opened.record],
    };
    stored.validate_shape().expect("current Run is valid");

    let store = FileAgentJournalStore::open(&root).expect("journal opens");
    AgentJournalStore::create_run(&store, stored.clone())
        .await
        .expect("current Run persists");
    let run_path = root.join(format!(
        "run-{}.json",
        Digest::sha256(execution.run_id.as_str()).as_str()
    ));
    let current = serde_json::from_slice::<serde_json::Value>(
        &std::fs::read(&run_path).expect("versioned Run reads"),
    )
    .expect("versioned Run is JSON");
    assert_eq!(current["schema_version"], 1);

    let mut legacy = serde_json::to_value(stored).expect("legacy Run serializes");
    let record = &mut legacy["records"][0];
    record["event"]["payload"]["request"]["payload"]
        .as_object_mut()
        .expect("approval payload is an object")
        .remove("session_approval_scope");
    let mut event_view = record["event"].clone();
    event_view
        .as_object_mut()
        .expect("event is an object")
        .remove("event_digest");
    let legacy_event_digest =
        Digest::sha256(serde_jcs::to_vec(&event_view).expect("legacy event canonicalizes"));
    record["event"]["event_digest"] =
        serde_json::Value::String(legacy_event_digest.as_str().to_owned());
    event_view
        .as_object_mut()
        .expect("event is an object")
        .remove("run_seq");
    let legacy_draft_digest =
        Digest::sha256(serde_jcs::to_vec(&event_view).expect("legacy draft canonicalizes"));
    record["draft_digest"] = serde_json::Value::String(legacy_draft_digest.as_str().to_owned());
    std::fs::write(
        &run_path,
        serde_json::to_vec(&legacy).expect("legacy Run encodes"),
    )
    .expect("legacy Run replaces current fixture");

    let reopened = FileAgentJournalStore::open(&root).expect("journal reopens");
    let loaded = AgentJournalStore::load_run(&reopened, &execution.run_id)
        .await
        .expect("legacy Run loads")
        .expect("legacy Run exists");
    loaded.validate_shape().expect("upgraded Run is valid");
    assert_eq!(
        reopened
            .catalog_runs()
            .await
            .expect("legacy Run remains discoverable")
            .len(),
        1
    );

    legacy["records"][0]["event"]["payload"]["request"]["payload"]["reason"] =
        serde_json::Value::String("tampered after sealing".to_owned());
    std::fs::write(
        &run_path,
        serde_json::to_vec(&legacy).expect("tampered Run encodes"),
    )
    .expect("tampered Run replaces legacy fixture");
    let error = AgentJournalStore::load_run(&reopened, &execution.run_id)
        .await
        .expect_err("tampered legacy semantics are rejected");
    assert!(error.to_string().contains("legacy event digest mismatch"));

    std::fs::remove_dir_all(root).expect("temporary journal cleans up");
}
use orchestral_core::agent_session::{
    AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId, AgentSessionJournalStore,
    SessionSourceRange,
};
use orchestral_core::model_protocol::{ModelMessage, ModelRequestId, ModelRole, ModelUsage};
use orchestral_core::tool_effect::{
    replay_tool_effect, PreparedToolEffect, ToolAuthorizationEvidence, ToolEffectAttemptId,
    ToolEffectEvent, ToolEffectEventDraft, ToolEffectEventId, ToolEffectJournalStore,
    ToolEffectPhase,
};
use orchestral_core::tool_protocol::{
    EffectScope, ToolCallId, ToolId, ToolIdempotency, ToolInvocation,
};
use orchestral_runtime::{
    AgentController, AppendGenericCheckpointOutcome, CreateGenericRunOutcome,
    GenericAgentCheckpointStore, GenericAgentRunRegistration, GenericCheckpointDraft,
    GenericCheckpointEvent, GenericCheckpointEventId, GenericCheckpointPhase,
    GenericModelContextTrace,
};

#[test]
fn generic_private_wal_rehydrates_from_a_new_store_instance() {
    let root = std::env::temp_dir().join(format!(
        "orchestral-generic-checkpoint-test-{}",
        uuid::Uuid::new_v4()
    ));
    let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
        provider_id: AgentProviderId::new("test/generic"),
        agent_id: AgentId::new("generic-v1"),
        supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
        accepted_content_types: std::collections::BTreeSet::from(["text/plain".to_owned()]),
        capabilities: Default::default(),
        extensions: Default::default(),
    })
    .expect("descriptor seals");
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("generic-checkpoint-session"),
        RunId::new("generic-checkpoint-run"),
        vec![Content::text("resume me safely")],
    )
    .expect("Run is valid");
    let request = AgentStartRequest::new(
        run,
        ProviderBindingRef::new("generic-checkpoint-binding"),
        &descriptor,
    )
    .expect("start request is valid");
    let registration = GenericAgentRunRegistration {
        execution: AgentExecutionRef::for_start(&request, &descriptor)
            .expect("execution reference is valid"),
        request,
        admission: AgentAdmission::default(),
        config_digest: Digest::sha256("generic-config-v1"),
    };
    let run_id = registration.run_id().clone();
    let boundary = GenericCheckpointDraft {
        event_id: GenericCheckpointEventId::new("boundary-1"),
        run_id: run_id.clone(),
        payload: GenericCheckpointEvent::LoopBoundaryCommitted {
            next_model_round: 1,
            usage: ModelUsage::default(),
            tool_call_count: 0,
            last_response: String::new(),
            supporting_event_ids: Vec::new(),
        },
    };
    let attempt = GenericCheckpointDraft {
        event_id: GenericCheckpointEventId::new("attempt-1"),
        run_id: run_id.clone(),
        payload: GenericCheckpointEvent::ModelAttemptStarted {
            round: 1,
            request_id: ModelRequestId::new("model-attempt-1"),
            request_digest: Digest::sha256("model-request-1"),
            max_output_tokens: None,
            context: GenericModelContextTrace {
                through_session_seq: 1,
                included_ranges: vec![SessionSourceRange {
                    first_session_seq: 1,
                    last_session_seq: 1,
                }],
                deferred_ranges: Vec::new(),
                config_digest: Digest::sha256("generic-config-v1"),
                history_limit: 128,
                used_input_tokens: 10,
                input_budget_tokens: 100,
            },
        },
    };

    let first = FileAgentJournalStore::open(&root).expect("journal opens");
    assert_eq!(
        GenericAgentCheckpointStore::create_run(&first, registration)
            .expect("private Run registration persists"),
        CreateGenericRunOutcome::Created
    );
    assert_eq!(
        GenericAgentCheckpointStore::append(&first, &run_id, 0, boundary)
            .expect("stable boundary persists"),
        AppendGenericCheckpointOutcome::Appended
    );
    assert_eq!(
        GenericAgentCheckpointStore::append(&first, &run_id, 1, attempt.clone())
            .expect("model attempt persists"),
        AppendGenericCheckpointOutcome::Appended
    );
    drop(first);

    let second = FileAgentJournalStore::open(&root).expect("journal reopens");
    let stored = GenericAgentCheckpointStore::load_run(&second, &run_id)
        .expect("private WAL reloads")
        .expect("private Run exists");
    assert_eq!(stored.records.len(), 2);
    assert!(matches!(
        stored.validate().expect("private WAL replays").phase,
        GenericCheckpointPhase::ModelAttemptOpen { round: 1, .. }
    ));
    assert_eq!(
        GenericAgentCheckpointStore::append(&second, &run_id, 2, attempt)
            .expect("exact retry is idempotent"),
        AppendGenericCheckpointOutcome::ExactDuplicate
    );

    std::fs::remove_dir_all(root).expect("temporary journal cleans up");
}

#[tokio::test]
async fn terminal_run_rehydrates_from_a_new_store_and_controller_instance() {
    let root = std::env::temp_dir().join(format!(
        "orchestral-agent-journal-test-{}",
        uuid::Uuid::new_v4()
    ));
    let factory = ScriptedStatelessFactory::conformant().expect("fixture descriptor");
    let scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    let first_store = Arc::new(FileAgentJournalStore::open(&root).expect("journal opens"));
    let first = Arc::new(
        AgentController::with_journal_store(
            factory.create(scenario.clone(), TestProbes::default()),
            ProviderBindingRef::new("filesystem-binding"),
            first_store,
        )
        .expect("controller binds"),
    );
    let execution = first
        .start(scenario.start_request.run.clone())
        .await
        .expect("run starts");
    let before = first
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("run completes");
    drop(first);

    let second_store = Arc::new(FileAgentJournalStore::open(&root).expect("journal reopens"));
    let second = AgentController::with_journal_store(
        factory.create(scenario, TestProbes::default()),
        ProviderBindingRef::new("filesystem-binding"),
        second_store,
    )
    .expect("new controller binds");
    let after = second
        .inspect(&execution.run_id)
        .await
        .expect("durable run rehydrates");
    let catalog = second
        .catalog_runs()
        .await
        .expect("durable Run catalog remains discoverable");

    assert_eq!(after.state.status(), AgentRunStatus::Delivered);
    assert_eq!(after, before);
    assert_eq!(catalog.len(), 1);
    assert_eq!(catalog[0].run_id, execution.run_id);
    assert_eq!(catalog[0].session_id, execution.session_id);
    std::fs::remove_dir_all(root).expect("temporary journal cleans up");
}

#[tokio::test]
async fn session_context_rehydrates_from_a_new_store_instance() {
    let root = std::env::temp_dir().join(format!(
        "orchestral-session-journal-test-{}",
        uuid::Uuid::new_v4()
    ));
    let session_id = AgentSessionId::new("durable-session");
    let draft = AgentSessionEventDraft {
        event_id: AgentSessionEventId::new("input-run-1"),
        session_id: session_id.clone(),
        run_id: RunId::new("run-1"),
        payload: AgentSessionEvent::RunInputCommitted {
            message: ModelMessage::text(ModelRole::User, "remember this"),
        },
    };

    let first = FileAgentJournalStore::open(&root).expect("journal opens");
    let append = AgentSessionJournalStore::append(&first, draft.clone())
        .await
        .expect("event appends");
    assert!(!append.exact_duplicate);
    drop(first);

    let second = FileAgentJournalStore::open(&root).expect("journal reopens");
    let records = second
        .load_session(&session_id)
        .await
        .expect("session reloads");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].event_id, draft.event_id);
    assert!(
        AgentSessionJournalStore::append(&second, draft)
            .await
            .expect("retry is accepted")
            .exact_duplicate
    );

    std::fs::remove_dir_all(root).expect("temporary journal cleans up");
}

#[tokio::test]
async fn invoked_tool_effect_rehydrates_without_losing_its_uncertain_boundary() {
    let root = std::env::temp_dir().join(format!(
        "orchestral-effect-journal-test-{}",
        uuid::Uuid::new_v4()
    ));
    let invocation = ToolInvocation {
        run_id: RunId::new("effect-run"),
        call_id: ToolCallId::new("effect-call"),
        tool_id: ToolId::new("test/write"),
        arguments: serde_json::json!({ "value": "hello" }),
    };
    let prepared = PreparedToolEffect {
        args_digest: invocation.args_digest().unwrap(),
        invocation,
        operation_digest: Digest::sha256("operation"),
        permission_digest: Digest::sha256("permission"),
        policy_digest: Digest::sha256("policy"),
        descriptor_digest: Digest::sha256("descriptor"),
        idempotency: ToolIdempotency::NonIdempotent,
        effect_scopes: std::collections::BTreeSet::from([EffectScope::ExternalSideEffect]),
    };
    let key = prepared.key();
    let first = FileAgentJournalStore::open(&root).expect("journal opens");
    ToolEffectJournalStore::append(
        &first,
        0,
        ToolEffectEventDraft {
            event_id: ToolEffectEventId::new("prepared"),
            key: key.clone(),
            payload: ToolEffectEvent::Prepared { effect: prepared },
        },
    )
    .await
    .expect("preparation persists");
    ToolEffectJournalStore::append(
        &first,
        1,
        ToolEffectEventDraft {
            event_id: ToolEffectEventId::new("invoked"),
            key: key.clone(),
            payload: ToolEffectEvent::Invoked {
                attempt_id: ToolEffectAttemptId::new("attempt-1"),
                authorization: ToolAuthorizationEvidence::Policy,
            },
        },
    )
    .await
    .expect("invocation persists");
    drop(first);

    let second = FileAgentJournalStore::open(&root).expect("journal reopens");
    let records = second
        .load_effect(&key)
        .await
        .expect("effect trace reloads");
    let projection = replay_tool_effect(&key, &records)
        .expect("effect trace validates")
        .expect("effect exists");
    assert!(matches!(projection.phase, ToolEffectPhase::Invoked { .. }));

    std::fs::remove_dir_all(root).expect("temporary journal cleans up");
}
