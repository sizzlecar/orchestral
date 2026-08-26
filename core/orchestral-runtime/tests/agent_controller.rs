use std::sync::Arc;

use orchestral_agent_protocol_testkit::{
    ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, TestProbes,
};
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus, spi::InMemoryAgentJournalStore, wire::ProviderBindingRef,
};
use orchestral_runtime::AgentController;

#[tokio::test]
async fn controller_drives_provider_stream_into_one_inspectable_terminal_run() {
    let factory = ScriptedStatelessFactory::conformant().expect("fixture descriptor");
    let scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    let provider = factory.create(scenario.clone(), TestProbes::default());
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("conformance-binding"))
            .expect("controller binds provider"),
    );

    let execution = controller
        .start(scenario.start_request.run.clone())
        .await
        .expect("run starts");
    let terminal = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("provider reaches terminal");
    let journal = controller
        .events(&execution.run_id, 0)
        .await
        .expect("journal replays");

    assert_eq!(terminal.state.status(), AgentRunStatus::Delivered);
    assert_eq!(terminal.last_run_seq, Some(3));
    assert_eq!(
        journal
            .iter()
            .map(|record| record.event.run_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        controller
            .inspect(&execution.run_id)
            .await
            .expect("run remains inspectable"),
        terminal
    );
}

#[tokio::test]
async fn a_new_controller_rehydrates_inspect_and_events_from_the_agent_journal() {
    let factory = ScriptedStatelessFactory::conformant().expect("fixture descriptor");
    let scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    let journal = Arc::new(InMemoryAgentJournalStore::default());
    let first = Arc::new(
        AgentController::with_journal_store(
            factory.create(scenario.clone(), TestProbes::default()),
            ProviderBindingRef::new("durable-binding"),
            journal.clone(),
        )
        .expect("first controller binds provider"),
    );
    let execution = first
        .start(scenario.start_request.run.clone())
        .await
        .expect("run starts");
    let before_restart = first
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("run reaches terminal before restart");
    drop(first);

    let restarted = AgentController::with_journal_store(
        factory.create(scenario, TestProbes::default()),
        ProviderBindingRef::new("durable-binding"),
        journal,
    )
    .expect("restarted controller binds the same Provider contract");
    let after_restart = restarted
        .inspect(&execution.run_id)
        .await
        .expect("inspect lazily rehydrates without restarting native work");
    let records = restarted
        .events(&execution.run_id, 1)
        .await
        .expect("durable pagination remains available");

    assert_eq!(after_restart, before_restart);
    assert_eq!(
        records
            .iter()
            .map(|record| record.event.run_seq)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}
