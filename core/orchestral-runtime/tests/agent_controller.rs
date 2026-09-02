use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use orchestral_agent_protocol_testkit::{
    ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, SessionfulRecoverFactory,
    TestProbes,
};
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    spi::{
        AgentJournalStore, AgentProvider, AgentRecovery, AgentRecoveryRequest, AgentStart,
        AgentStartError, InMemoryAgentJournalStore,
    },
    wire::{
        AgentCommand, AgentCommandEnvelope, AgentDescriptorEnvelope, AgentEvent, AgentExecutionRef,
        AgentProtocolError, AgentProtocolErrorCode, AgentStartRequest, CommandId, Content,
        ProviderBindingRef, ProviderCommandDisposition,
    },
};
use orchestral_runtime::{AgentClient, AgentController};
use tokio::sync::Notify;

struct RecoveryOrderProvider {
    inner: Arc<dyn AgentProvider>,
    journal: Arc<InMemoryAgentJournalStore>,
    confirmed_after_restore: Arc<AtomicBool>,
    fail_after_restore: bool,
}

struct CommandThenDisconnectProvider {
    inner: Arc<dyn AgentProvider>,
    disconnect: Arc<Notify>,
}

struct DescriptorOverrideProvider {
    inner: Arc<dyn AgentProvider>,
    descriptor: AgentDescriptorEnvelope,
}

struct StalledRecoveryProvider {
    inner: Arc<dyn AgentProvider>,
}

#[async_trait]
impl AgentProvider for StalledRecoveryProvider {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.inner.describe()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        self.inner.start(request).await
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        self.inner.command(execution, command).await
    }

    async fn recover(
        &self,
        _request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        Ok(AgentRecovery::reattached(stream::pending().boxed()))
    }
}

#[async_trait]
impl AgentProvider for DescriptorOverrideProvider {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.descriptor.clone()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        self.inner.start(request).await
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        self.inner.command(execution, command).await
    }

    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        self.inner.recover(request).await
    }
}

#[async_trait]
impl AgentProvider for CommandThenDisconnectProvider {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.inner.describe()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        let started = self.inner.start(request).await?;
        let disconnect = self.disconnect.clone();
        let stream = started
            .stream
            .take(1)
            .chain(stream::once(async move {
                disconnect.notified().await;
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    "fixture stream disconnected after command",
                ))
            }))
            .boxed();
        Ok(AgentStart {
            execution: started.execution,
            admission: started.admission,
            stream,
        })
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        self.inner.command(execution, command).await
    }

    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        self.inner.recover(request).await
    }
}

#[async_trait]
impl AgentProvider for RecoveryOrderProvider {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.inner.describe()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        self.inner.start(request).await
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        self.inner.command(execution, command).await
    }

    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        let run_id = request.execution.run_id.clone();
        let recovered = self.inner.recover(request).await?;
        let (stream, inner_confirmation) = recovered.into_parts();
        let journal = self.journal.clone();
        let confirmed_after_restore = self.confirmed_after_restore.clone();
        let fail_after_restore = self.fail_after_restore;
        Ok(AgentRecovery::staged(stream, async move {
            let stored = journal.load_run(&run_id).await.map_err(|error| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    format!("could not inspect Host recovery journal: {error}"),
                )
            })?;
            let restored_is_durable = stored.is_some_and(|run| {
                run.records.iter().any(|record| {
                    matches!(&record.event.payload, AgentEvent::ContinuityRestored { .. })
                })
            });
            if !restored_is_durable {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Provider recovery was confirmed before ContinuityRestored was durable",
                ));
            }
            inner_confirmation.await?;
            confirmed_after_restore.store(true, Ordering::SeqCst);
            if fail_after_restore {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    "reconstructed Provider work could not be resumed",
                ));
            }
            Ok(())
        }))
    }
}

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
async fn one_thousand_fake_provider_runs_keep_control_plane_p95_below_100ms() {
    const RUNS: usize = 1_000;
    let mut latencies = Vec::with_capacity(RUNS);

    for _ in 0..RUNS {
        let factory = ScriptedStatelessFactory::conformant().expect("fixture descriptor");
        let scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
        let provider = factory.create(scenario.clone(), TestProbes::default());
        let controller = Arc::new(
            AgentController::new(
                provider,
                ProviderBindingRef::new("latency-conformance-binding"),
            )
            .expect("controller binds the fake Provider"),
        );

        let started = Instant::now();
        let execution = controller
            .start(scenario.start_request.run)
            .await
            .expect("fake Run starts");
        let terminal = controller
            .wait_for_terminal(&execution.run_id)
            .await
            .expect("fake Run reaches terminal");
        latencies.push(started.elapsed());
        assert_eq!(terminal.state.status(), AgentRunStatus::Delivered);
    }

    latencies.sort_unstable();
    let p95 = latencies[(RUNS * 95).div_ceil(100) - 1];
    assert!(
        p95 <= Duration::from_millis(100),
        "Agent Protocol control-plane p95 was {p95:?}"
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

#[tokio::test]
async fn a_descriptor_upgrade_identifies_an_old_run_without_rehydrating_it() {
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
    first
        .wait_for_terminal(&execution.run_id)
        .await
        .expect("run reaches terminal before upgrade");
    drop(first);

    let mut upgraded_descriptor = factory.descriptor().descriptor;
    upgraded_descriptor
        .accepted_content_types
        .insert("image/png".to_owned());
    let upgraded_descriptor = AgentDescriptorEnvelope::seal(upgraded_descriptor)
        .expect("upgraded descriptor remains valid");
    let upgraded_provider = Arc::new(DescriptorOverrideProvider {
        inner: factory.create(scenario, TestProbes::default()),
        descriptor: upgraded_descriptor,
    });
    let upgraded = AgentController::with_journal_store(
        upgraded_provider,
        ProviderBindingRef::new("durable-binding"),
        journal,
    )
    .expect("upgraded controller binds provider");

    assert!(!upgraded
        .can_control_run(&execution.run_id)
        .await
        .expect("old registration remains discoverable"));
    assert!(matches!(
        upgraded.inspect(&execution.run_id).await,
        Err(orchestral_runtime::AgentControlError::Protocol(error))
            if error.code == AgentProtocolErrorCode::RunIdConflict
    ));
}

#[tokio::test]
async fn recovery_continuation_opens_only_after_continuity_restore_is_durable() {
    let factory = SessionfulRecoverFactory::new().expect("fixture descriptor");
    let mut scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    scenario.immediate_events.truncate(1);
    let journal = Arc::new(InMemoryAgentJournalStore::default());
    let confirmed_after_restore = Arc::new(AtomicBool::new(false));
    let provider = Arc::new(RecoveryOrderProvider {
        inner: factory.create(scenario.clone(), TestProbes::default()),
        journal: journal.clone(),
        confirmed_after_restore: confirmed_after_restore.clone(),
        fail_after_restore: false,
    });
    let controller = Arc::new(
        AgentController::with_journal_store(
            provider,
            ProviderBindingRef::new("conformance-binding"),
            journal.clone(),
        )
        .expect("controller binds provider"),
    );

    let execution = controller
        .start(scenario.start_request.run)
        .await
        .expect("non-terminal run starts");
    let first_loss = controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect_err("finite non-terminal stream becomes Unknown");
    assert!(matches!(
        first_loss,
        orchestral_runtime::AgentControlError::ContinuityUnknown(_)
    ));

    controller
        .recover(&execution.run_id)
        .await
        .expect("matching Provider prefix restores continuity");

    assert!(confirmed_after_restore.load(Ordering::SeqCst));
    let stored = journal
        .load_run(&execution.run_id)
        .await
        .expect("journal remains readable")
        .expect("run remains registered");
    assert!(stored
        .records
        .iter()
        .any(|record| { matches!(&record.event.payload, AgentEvent::ContinuityRestored { .. }) }));
}

#[tokio::test]
async fn recovery_rejects_a_provider_that_never_replays_its_committed_prefix() {
    let factory = SessionfulRecoverFactory::new().expect("fixture descriptor");
    let mut scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    scenario.immediate_events.truncate(1);
    let provider = Arc::new(StalledRecoveryProvider {
        inner: factory.create(scenario.clone(), TestProbes::default()),
    });
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("conformance-binding"))
            .expect("controller binds provider"),
    );

    let execution = controller
        .start(scenario.start_request.run)
        .await
        .expect("non-terminal run starts");
    controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect_err("finite non-terminal stream becomes Unknown");

    let started = Instant::now();
    let error = controller
        .recover(&execution.run_id)
        .await
        .expect_err("a missing recovery prefix is rejected");
    assert!(matches!(
        error,
        orchestral_runtime::AgentControlError::RecoveryMismatch(ref run_id)
            if run_id == &execution.run_id
    ));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn recovery_uses_host_committed_command_dispositions_without_stream_replay() {
    let factory = SessionfulRecoverFactory::new().expect("fixture descriptor");
    let mut scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    scenario.immediate_events.truncate(1);
    let disconnect = Arc::new(Notify::new());
    let provider = Arc::new(CommandThenDisconnectProvider {
        inner: factory.create(scenario.clone(), TestProbes::default()),
        disconnect: disconnect.clone(),
    });
    let controller = Arc::new(
        AgentController::new(
            provider,
            ProviderBindingRef::new("command-recovery-binding"),
        )
        .expect("controller binds provider"),
    );

    let execution = controller
        .start(scenario.start_request.run)
        .await
        .expect("non-terminal run starts");
    let command = AgentCommandEnvelope::new(
        CommandId::new("command-before-restart"),
        execution.run_id.clone(),
        None,
        AgentCommand::Steer {
            content: vec![Content::text("continue")],
        },
    )
    .expect("command is valid");
    controller
        .command(command)
        .await
        .expect("command disposition is Host-committed");
    disconnect.notify_one();
    controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect_err("fixture disconnect makes continuity unknown");

    controller
        .recover(&execution.run_id)
        .await
        .expect("native replay plus Host command evidence restores continuity");
    let events = controller
        .events(&execution.run_id, 0)
        .await
        .expect("journal remains readable");
    assert!(events.iter().any(|record| matches!(
        record.event.payload,
        AgentEvent::CommandDispositionRecorded { .. }
    )));
    assert!(events
        .iter()
        .any(|record| matches!(record.event.payload, AgentEvent::ContinuityRestored { .. })));
}

#[tokio::test]
async fn failed_recovery_confirmation_returns_the_run_to_unknown() {
    let factory = SessionfulRecoverFactory::new().expect("fixture descriptor");
    let mut scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    scenario.immediate_events.truncate(1);
    let journal = Arc::new(InMemoryAgentJournalStore::default());
    let provider = Arc::new(RecoveryOrderProvider {
        inner: factory.create(scenario.clone(), TestProbes::default()),
        journal: journal.clone(),
        confirmed_after_restore: Arc::new(AtomicBool::new(false)),
        fail_after_restore: true,
    });
    let controller = Arc::new(
        AgentController::with_journal_store(
            provider,
            ProviderBindingRef::new("conformance-binding"),
            journal,
        )
        .expect("controller binds provider"),
    );

    let execution = controller
        .start(scenario.start_request.run)
        .await
        .expect("non-terminal run starts");
    controller
        .wait_for_terminal(&execution.run_id)
        .await
        .expect_err("finite non-terminal stream becomes Unknown");

    let error = controller
        .recover(&execution.run_id)
        .await
        .expect_err("failed Provider continuation rejects recovery");
    assert!(matches!(
        error,
        orchestral_runtime::AgentControlError::Protocol(ref error)
            if error.code == AgentProtocolErrorCode::ProviderUnavailable
    ));
    let view = controller
        .inspect(&execution.run_id)
        .await
        .expect("Run remains inspectable after failed continuation");
    assert_eq!(view.state.status(), AgentRunStatus::Unknown);
}

#[tokio::test]
async fn sdk_returns_unknown_as_a_recoverable_turn_boundary() {
    let factory = SessionfulRecoverFactory::new().expect("fixture descriptor");
    let mut scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    scenario.immediate_events.truncate(1);
    let controller = Arc::new(
        AgentController::new(
            factory.create(scenario, TestProbes::default()),
            ProviderBindingRef::new("conformance-binding"),
        )
        .expect("controller binds provider"),
    );
    let client = AgentClient::new(
        controller,
        orchestral_core::agent_protocol::wire::AgentSessionId::new("conformance-session"),
    );
    let handle = client
        .start_with_run_id(
            orchestral_core::agent_protocol::wire::RunId::new("conformance-run"),
            vec![orchestral_core::agent_protocol::wire::Content::text(
                "complete the deterministic fixture",
            )],
        )
        .await
        .expect("non-terminal fixture starts");

    let turn = tokio::time::timeout(Duration::from_secs(1), handle.wait_until_blocked())
        .await
        .expect("SDK does not hang after Provider continuity is lost")
        .expect("Unknown is returned as an inspectable boundary");

    assert_eq!(turn.status(), AgentRunStatus::Unknown);
    assert!(!turn.is_waiting());
}
