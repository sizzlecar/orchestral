use super::*;

struct CountedFailure {
    calls: Arc<AtomicUsize>,
    error: bool,
}

#[async_trait]
impl AgentSessionSummarizer for CountedFailure {
    fn descriptor(&self) -> SessionSummarizerDescriptor {
        FallbackSummarizer.descriptor()
    }

    async fn summarize(
        &self,
        input: SessionCompactionInput,
    ) -> Result<ModelMessage, SessionContextError> {
        Ok(self.generate(input).await?.message)
    }

    async fn generate(
        &self,
        input: SessionCompactionInput,
    ) -> Result<SessionSummaryGeneration, SessionContextError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        if self.error {
            Err(SessionContextError::Compaction(
                "unavailable semantic summary".to_owned(),
            ))
        } else {
            FallbackSummarizer.generate(input).await
        }
    }

    async fn generate_with_char_budget(
        &self,
        input: SessionCompactionInput,
        _: usize,
    ) -> Result<SessionSummaryGeneration, SessionContextError> {
        self.generate(input).await
    }
}

fn counted_compactor(fixture: &Fixture, error: bool) -> (AgentSessionCompactor, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let compactor = AgentSessionCompactor::new(
        fixture.store.clone(),
        Arc::new(CountedFailure {
            calls: calls.clone(),
            error,
        }),
        SessionCompactionPolicy {
            minimum_source_records: 2,
            keep_recent_records: 1,
        },
    )
    .unwrap();
    (compactor, calls)
}

#[tokio::test]
async fn active_soft_failure_is_attempted_once_per_source_but_hard_pressure_can_retry() {
    let fixture = Fixture::new(&[20_000; 4], "Inspect and implement").await;
    let (compactor, calls) = counted_compactor(&fixture, false);
    let before = fixture.records().await;
    for _ in 0..3 {
        assert!(compactor
            .compact_active_run_for_working_set(
                &fixture.engine,
                fixture.request(5_000),
                ContextTokenPolicy::UpperBound,
            )
            .await
            .unwrap()
            .is_none());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.records().await, before);
    append_tool_exchange(&fixture.store, 100, "current", 20_000).await;
    assert!(compactor
        .compact_active_run_for_working_set(
            &fixture.engine,
            fixture.request(5_000),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap()
        .is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(compactor
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(5_000),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap()
        .is_some());
    assert!(calls.load(Ordering::SeqCst) > 2);
}

#[tokio::test]
async fn history_soft_fallback_and_compaction_error_keep_raw_and_do_not_repeat() {
    for error in [false, true] {
        let fixture = Fixture::new(&[12_000; 4], "Initial task").await;
        append_input(&fixture.store, 2, "next", "Continue the task".to_owned()).await;
        let (compactor, calls) = counted_compactor(&fixture, error);
        let session = AgentSessionId::new("session-1");
        let run = RunId::new("next");
        let before = fixture.records().await;
        assert!(compactor
            .has_working_set_candidate(&session, &run)
            .await
            .unwrap());
        for _ in 0..3 {
            assert!(compactor
                .compact_history_for_working_set(&session, &run)
                .await
                .unwrap()
                .is_none());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!compactor
            .has_working_set_candidate(&session, &run)
            .await
            .unwrap());
        assert!(compactor
            .has_compaction_candidate(&session, &run)
            .await
            .unwrap());
        assert_eq!(fixture.records().await, before);
        let hard = compactor.compact_if_needed(&session, &run).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        if error {
            assert!(matches!(hard, Err(SessionContextError::Compaction(_))));
        } else {
            assert!(hard.unwrap().is_some());
        }
    }
}

#[tokio::test]
async fn active_compaction_error_is_optional_and_new_run_gets_a_fresh_attempt() {
    let fixture = Fixture::new(&[12_000; 4], "Initial task").await;
    let (compactor, calls) = counted_compactor(&fixture, true);
    for _ in 0..2 {
        assert!(compactor
            .compact_active_run_for_working_set(
                &fixture.engine,
                fixture.request(5_000),
                ContextTokenPolicy::UpperBound,
            )
            .await
            .unwrap()
            .is_none());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    append_input(&fixture.store, 2, "next", "Continue the task".to_owned()).await;
    let session = AgentSessionId::new("session-1");
    let run = RunId::new("next");
    assert!(compactor
        .has_working_set_candidate(&session, &run)
        .await
        .unwrap());
    assert!(compactor
        .compact_history_for_working_set(&session, &run)
        .await
        .unwrap()
        .is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!compactor
        .has_working_set_candidate(&session, &run)
        .await
        .unwrap());
}

#[tokio::test]
async fn concurrent_soft_requests_share_one_source_attempt() {
    let fixture = Fixture::new(&[12_000; 4], "Initial task").await;
    append_input(&fixture.store, 2, "next", "Continue the task".to_owned()).await;
    let (compactor, calls) = counted_compactor(&fixture, false);
    let session = AgentSessionId::new("session-1");
    let run = RunId::new("next");
    let before = fixture.records().await;
    let (first, second) = tokio::join!(
        compactor.compact_history_for_working_set(&session, &run),
        compactor.compact_history_for_working_set(&session, &run),
    );
    assert!(first.unwrap().is_none());
    assert!(second.unwrap().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.records().await, before);
}
