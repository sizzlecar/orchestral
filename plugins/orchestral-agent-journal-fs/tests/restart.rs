use std::sync::Arc;

use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_agent_protocol_testkit::{
    ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, TestProbes,
};
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    wire::{AgentSessionId, Digest, ProviderBindingRef, RunId},
};
use orchestral_core::agent_session::{
    AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId, AgentSessionJournalStore,
};
use orchestral_core::model_protocol::{ModelMessage, ModelRole};
use orchestral_core::tool_effect::{
    replay_tool_effect, PreparedToolEffect, ToolAuthorizationEvidence, ToolEffectAttemptId,
    ToolEffectEvent, ToolEffectEventDraft, ToolEffectEventId, ToolEffectJournalStore,
    ToolEffectPhase,
};
use orchestral_core::tool_protocol::{
    EffectScope, ToolCallId, ToolId, ToolIdempotency, ToolInvocation,
};
use orchestral_runtime::AgentController;

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
