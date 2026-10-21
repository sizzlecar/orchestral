use super::*;
use orchestral_core::agent_protocol::{
    spi::{
        AgentJournalStoreError, AgentRunCatalogEntry, AppendAgentRecordOutcome,
        CreateAgentRunOutcome, StoredAgentRun,
    },
    wire::{AgentJournalRecord, AgentTelemetry, AgentTelemetryEnvelope, RunId, TelemetryId},
};
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct FailingJournal {
    inner: InMemoryAgentJournalStore,
    unavailable: AtomicBool,
    commit_before_error: AtomicBool,
    failures: AtomicUsize,
}

#[async_trait]
impl AgentJournalStore for FailingJournal {
    async fn load_run(&self, id: &RunId) -> Result<Option<StoredAgentRun>, AgentJournalStoreError> {
        self.inner.load_run(id).await
    }

    async fn create_run(
        &self,
        run: StoredAgentRun,
    ) -> Result<CreateAgentRunOutcome, AgentJournalStoreError> {
        self.inner.create_run(run).await
    }

    async fn append_record(
        &self,
        id: &RunId,
        previous: u64,
        record: AgentJournalRecord,
    ) -> Result<AppendAgentRecordOutcome, AgentJournalStoreError> {
        if !matches!(record.event.payload, AgentEvent::RunStarted)
            && self.unavailable.load(Ordering::SeqCst)
        {
            if self.commit_before_error.load(Ordering::SeqCst) {
                self.inner.append_record(id, previous, record).await?;
            }
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err(AgentJournalStoreError::Unavailable(
                "fixture: no space left on device".to_owned(),
            ));
        }
        self.inner.append_record(id, previous, record).await
    }

    async fn records(
        &self,
        id: &RunId,
        after: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentJournalStoreError> {
        self.inner.records(id, after).await
    }

    async fn catalog_runs(&self) -> Result<Vec<AgentRunCatalogEntry>, AgentJournalStoreError> {
        self.inner.catalog_runs().await
    }
}

#[tokio::test]
async fn failed_continuity_write_is_retried_without_inventing_a_terminal() {
    let factory = SessionfulRecoverFactory::new().unwrap();
    let scenario = ProviderScenario::standard(&factory.descriptor()).unwrap();
    let provider = factory.create(scenario.clone(), TestProbes::default());
    let journal = Arc::new(FailingJournal::default());
    journal.unavailable.store(true, Ordering::SeqCst);
    let binding = ProviderBindingRef::new("conformance-binding");
    let controller = Arc::new(
        AgentController::with_journal_store(provider.clone(), binding.clone(), journal.clone())
            .unwrap(),
    );
    let execution = controller.start(scenario.start_request.run).await.unwrap();
    let id = &execution.run_id;
    tokio::time::timeout(Duration::from_secs(1), async {
        while journal.failures.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal append and continuity append both fail");

    // Both a late waiter and UI polling used to see Running forever.
    let error = tokio::time::timeout(Duration::from_secs(1), controller.wait_for_terminal(id))
        .await
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        error,
        orchestral_runtime::AgentControlError::Journal(_)
    ));
    assert!(controller.inspect(id).await.is_err());
    assert!(controller.events(id, 0).await.is_err());
    journal.unavailable.store(false, Ordering::SeqCst);
    let (view, replay) = tokio::join!(controller.inspect(id), controller.events(id, 0));
    let view = view.unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Unknown);
    let records = replay.unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r.event.payload, AgentEvent::ContinuityLost { .. }))
            .count(),
        1
    );
    assert!(records.iter().all(|r| !matches!(
        r.event.payload,
        AgentEvent::DeliveryCommitted { .. }
            | AgentEvent::RunCancelled { .. }
            | AgentEvent::RunFailed { .. }
    )));
    match &records.last().unwrap().event.payload {
        AgentEvent::ContinuityLost { reason, .. } => {
            assert!(reason.contains("Provider event could not be journaled"));
            assert!(reason.contains("no space left on device"));
        }
        _ => panic!("lost stream is now durable"),
    }

    let restarted =
        Arc::new(AgentController::with_journal_store(provider, binding, journal).unwrap());
    assert_eq!(restarted.inspect(id).await.unwrap().state, view.state);
    assert_eq!(restarted.events(id, 0).await.unwrap(), records);
    // Only verified Provider replay may now deliver the interrupted Run.
    assert_eq!(
        restarted.recover(id).await.unwrap().state.status(),
        AgentRunStatus::Delivered
    );
    assert_eq!(
        restarted
            .events(id, 0)
            .await
            .unwrap()
            .iter()
            .filter(|r| matches!(r.event.payload, AgentEvent::DeliveryCommitted { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn append_error_after_commit_replays_the_authoritative_terminal() {
    let factory = ScriptedStatelessFactory::conformant().unwrap();
    let scenario = ProviderScenario::standard(&factory.descriptor()).unwrap();
    let journal = Arc::new(FailingJournal::default());
    journal.unavailable.store(true, Ordering::SeqCst);
    journal.commit_before_error.store(true, Ordering::SeqCst);
    let controller = Arc::new(
        AgentController::with_journal_store(
            factory.create(scenario.clone(), TestProbes::default()),
            ProviderBindingRef::new("conformance-binding"),
            journal,
        )
        .unwrap(),
    );
    let execution = controller.start(scenario.start_request.run).await.unwrap();
    let view = tokio::time::timeout(
        Duration::from_secs(1),
        controller.wait_for_terminal(&execution.run_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    let records = controller.events(&execution.run_id, 0).await.unwrap();
    assert_eq!(records.len(), 3);
    assert!(records
        .iter()
        .all(|r| !matches!(r.event.payload, AgentEvent::ContinuityLost { .. })));
}

struct RecoveryErrorProvider {
    inner: Arc<dyn AgentProvider>,
    telemetry: bool,
}

#[async_trait]
impl AgentProvider for RecoveryErrorProvider {
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
        let mut prefix: Vec<_> = request
            .committed_provider_prefix
            .into_iter()
            .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft))))
            .collect();
        if self.telemetry {
            prefix.push(Ok(AgentProviderStreamItem::Telemetry(
                AgentTelemetryEnvelope {
                    telemetry_id: TelemetryId::new("recovery-progress"),
                    run_id: request.execution.run_id,
                    provider_seq: None,
                    payload: AgentTelemetry::ProgressReported {
                        message: "reconciling".into(),
                        fraction: None,
                    },
                },
            )));
        }
        let stream = stream::iter(prefix)
            .chain(stream::iter([Err(AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                "fixture checkpoint failed during reattachment",
            ))]))
            .chain(stream::pending())
            .boxed();
        Ok(AgentRecovery::reattached(stream))
    }
}

#[tokio::test]
async fn ready_recovery_error_is_not_consumed_and_discarded() {
    for telemetry in [false, true] {
        let factory = SessionfulRecoverFactory::new().unwrap();
        let mut scenario = ProviderScenario::standard(&factory.descriptor()).unwrap();
        scenario.immediate_events.truncate(1);
        let provider = Arc::new(RecoveryErrorProvider {
            inner: factory.create(scenario.clone(), TestProbes::default()),
            telemetry,
        });
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("conformance-binding")).unwrap(),
        );
        let execution = controller.start(scenario.start_request.run).await.unwrap();
        controller
            .wait_for_terminal(&execution.run_id)
            .await
            .unwrap_err();
        let mut observer = controller.subscribe(&execution.run_id).await.unwrap();
        let view = controller.recover(&execution.run_id).await.unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Unknown);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(
            error,
            orchestral_runtime::AgentControlError::ContinuityUnknown(_)
        ));
        let records = controller.events(&execution.run_id, 0).await.unwrap();
        assert!(
            matches!(&records.last().unwrap().event.payload, AgentEvent::ContinuityLost { reason, .. } if reason.contains("checkpoint failed during reattachment"))
        );
        let mut observed_telemetry = 0;
        while let Ok(event) = observer.try_recv() {
            if matches!(event, orchestral_runtime::AgentControlEvent::Telemetry(_)) {
                observed_telemetry += 1;
            }
        }
        assert_eq!(observed_telemetry, usize::from(telemetry));
    }
}
