use std::sync::Arc;

use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_agent_protocol_testkit::{
    ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, TestProbes,
};
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    wire::{
        AgentAdmission, AgentDescriptor, AgentDescriptorEnvelope, AgentExecutionRef, AgentId,
        AgentProviderId, AgentRunEnvelope, AgentSessionId, AgentStartRequest, Content, Digest,
        ProviderBindingRef, RunId,
    },
    AGENT_PROTOCOL_V1,
};
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

    assert_eq!(after.state.status(), AgentRunStatus::Delivered);
    assert_eq!(after, before);
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
