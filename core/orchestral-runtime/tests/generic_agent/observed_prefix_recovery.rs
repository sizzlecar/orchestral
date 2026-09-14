use super::*;
use orchestral_core::model_protocol::ModelContextEstimate;

struct PrefixMeter;

impl ModelTokenMeter for PrefixMeter {
    fn supports_observed_prefix_estimation(&self) -> bool {
        true
    }
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        ModelTokenMeterDescriptor {
            strategy: "fixture/observed-prefix".to_owned(),
            version: "1".to_owned(),
            accounting: ModelTokenAccounting::ConservativeUpperBound,
            config_digest: Digest::sha256("fixture/observed-prefix/v1"),
        }
    }
    fn count_request_input(
        &self,
        _: &[ModelMessage],
        _: &[ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        Ok(20_000)
    }
    fn estimate_context_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<ModelContextEstimate, ModelError> {
        Ok(ModelContextEstimate {
            tokens: 1_000 + messages.len() as u64 * 100 + tools.len() as u64 * 10,
            accounting: ModelTokenAccounting::Estimated,
        })
    }
}

struct PrefixModel {
    rounds: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    invalid_usage: bool,
}

#[async_trait]
impl ModelBackend for PrefixModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "fixture-prefix-loop".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                ..ModelCapabilities::default()
            },
            extensions: Default::default(),
        }
    }
    async fn start(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        let round = self.rounds.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(round <= 3);
        self.requests.lock().unwrap().push(request.clone());
        let mut events = if round < 3 {
            let call_id = ModelToolCallId::new(format!("observed-call-{round}"));
            vec![
                ModelEvent::ToolCallStart {
                    call_id: call_id.clone(),
                    name: "echo".to_owned(),
                    extensions: Default::default(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: call_id.clone(),
                    delta: format!("{{\"value\":\"record-{round}\"}}"),
                },
                ModelEvent::ToolCallEnd { call_id },
            ]
        } else {
            vec![ModelEvent::TextDelta {
                delta: "review complete".to_owned(),
            }]
        };
        events.push(ModelEvent::Usage {
            usage: ModelUsage {
                input_tokens: Some(200 + round as u64 * 100),
                output_tokens: Some(if self.invalid_usage && round == 2 {
                    101
                } else {
                    7
                }),
            },
        });
        events.push(ModelEvent::Finish {
            reason: if round < 3 {
                ModelFinishReason::ToolCalls
            } else {
                ModelFinishReason::Stop
            },
        });
        Ok(Box::pin(stream::iter(events.into_iter().enumerate().map(
            move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("prefix-{round}-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            },
        ))))
    }
}

#[derive(Clone, Copy)]
enum Cut {
    Started,
    Observed,
    ToolExchange,
    InvalidUsage,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_prefix_started_crash_closes_incomplete_without_model_or_tool_replay() {
    run_cut(Cut::Started).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_prefix_observed_crash_recovers_old_anchor_then_advances() {
    run_cut(Cut::Observed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_prefix_committed_exchange_recovers_without_reexecuting_effect() {
    run_cut(Cut::ToolExchange).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observed_prefix_ack_lost_over_output_cap_is_rejected_before_effects() {
    run_cut(Cut::InvalidUsage).await;
}

async fn run_cut(cut: Cut) {
    let run_id = RunId::new("prefix-recovery-run");
    let session_id = AgentSessionId::new("prefix-recovery-session");
    let pausing = Arc::new(PausingCheckpointStore::at(match cut {
        Cut::ToolExchange => CheckpointCrashCut::ToolExchangeRound(3),
        _ => CheckpointCrashCut::ModelObservationRound(2),
    }));
    let ack_lost = Arc::new(AckLostAfterModelObservationCheckpointStore {
        target_round: Some(2),
        ..Default::default()
    });
    let checkpoints: Arc<dyn GenericAgentCheckpointStore> = match cut {
        Cut::Observed | Cut::InvalidUsage => ack_lost.clone(),
        _ => pausing.clone(),
    };
    let sessions = Arc::new(InMemoryAgentSessionJournalStore::default());
    let host = Arc::new(InMemoryAgentJournalStore::default());
    let effects = Arc::new(InMemoryToolEffectJournalStore::default());
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
        ..Default::default()
    };
    let mut config = GenericAgentConfig::new("internal-provider", "generic-agent");
    config.max_context_tokens = if matches!(cut, Cut::InvalidUsage) {
        5_000
    } else {
        2_250
    };
    config.reserved_output_tokens = 1_000;
    let first_model = Arc::new(PrefixModel {
        rounds: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        invalid_usage: matches!(cut, Cut::InvalidUsage),
    });
    let first_tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let provider = InternalGenericAgentProvider::new_with_tools_and_session_journal(
        first_model.clone(),
        config.clone(),
        durable_direct_runtime(&bounds, effects.clone(), first_tool.clone()),
        RunToolGrant {
            bounds: bounds.clone(),
        },
        sessions.clone(),
        Arc::new(PrefixMeter),
    )
    .unwrap()
    .with_checkpoint_store(checkpoints.clone())
    .unwrap();
    let first = Arc::new(
        AgentController::with_journal_store(
            Arc::new(provider),
            ProviderBindingRef::new("prefix-binding"),
            host.clone(),
        )
        .unwrap(),
    );
    let paused = pausing.paused.notified();
    let mut spec = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session_id.clone(),
        run_id.clone(),
        vec![Content::text("Inspect both records and finish")],
    )
    .unwrap()
    .spec;
    spec.limits.max_output_tokens = Some(100);
    first
        .start(AgentRunEnvelope::seal(spec).unwrap())
        .await
        .unwrap();
    if !matches!(cut, Cut::Observed | Cut::InvalidUsage) {
        tokio::time::timeout(std::time::Duration::from_secs(2), paused)
            .await
            .expect("second-round crash boundary reached");
        pausing.release_as_crash();
    }
    let failed = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        first.wait_for_terminal(&run_id),
    )
    .await
    .unwrap();
    assert!(matches!(
        failed,
        Err(AgentControlError::ContinuityUnknown(_))
    ));
    let stored = checkpoints.load_run(&run_id).unwrap().unwrap();
    let round_two = stored
        .records
        .iter()
        .find_map(|record| match &record.payload {
            GenericCheckpointEvent::ModelAttemptStarted {
                round: 2, context, ..
            } => Some(context.clone()),
            _ => None,
        })
        .unwrap();
    let planning = round_two.planning.as_ref().unwrap();
    if matches!(cut, Cut::InvalidUsage) {
        // Even a full-estimate fallback must fit, so ContextOverflow cannot
        // disguise a missing write-ahead usage validation on recovery.
        assert!(planning.input.raw_estimate_tokens < round_two.input_budget_tokens);
    } else {
        assert!(planning.input.raw_estimate_tokens > round_two.input_budget_tokens);
    }
    assert!(round_two.context_estimate.as_ref().unwrap().tokens < round_two.input_budget_tokens);
    assert_eq!(planning.anchor.as_ref().unwrap().observed_input_tokens, 300);
    assert_eq!(first_model.requests.lock().unwrap().len(), 2);
    let committed_count = if matches!(cut, Cut::ToolExchange) {
        2
    } else {
        1
    };
    assert_eq!(first_tool.calls.load(Ordering::SeqCst), committed_count);
    let session_before = sessions.load_session(&session_id).await.unwrap();
    assert_eq!(
        session_before
            .iter()
            .filter(|record| matches!(
                record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            ))
            .count(),
        committed_count
    );
    drop(first);
    pausing.allow_recovery_writes();
    ack_lost.unavailable.store(false, Ordering::SeqCst);
    let model = Arc::new(PrefixModel {
        rounds: AtomicUsize::new(2),
        requests: Mutex::new(Vec::new()),
        invalid_usage: matches!(cut, Cut::InvalidUsage),
    });
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let provider = InternalGenericAgentProvider::new_with_tools_and_session_journal(
        model.clone(),
        config,
        durable_direct_runtime(&bounds, effects, tool.clone()),
        RunToolGrant { bounds },
        sessions.clone(),
        Arc::new(PrefixMeter),
    )
    .unwrap()
    .with_checkpoint_store(checkpoints.clone())
    .unwrap();
    let recovered = Arc::new(
        AgentController::with_journal_store(
            Arc::new(provider),
            ProviderBindingRef::new("prefix-binding"),
            host,
        )
        .unwrap(),
    );
    let recovery = recovered.recover(&run_id).await;
    if matches!(cut, Cut::InvalidUsage) {
        assert!(
            recovery.is_err(),
            "over-cap observation cannot authorize recovered effects"
        );
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert!(model.requests.lock().unwrap().is_empty());
        assert_eq!(
            session_before,
            sessions.load_session(&session_id).await.unwrap()
        );
        assert!(stored.validate().unwrap().observed_prefix.is_none());
        return;
    }
    recovery.expect("recovery validates original Started cursor and old anchor");
    let view = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        recovered.wait_for_terminal(&run_id),
    )
    .await
    .unwrap()
    .unwrap();
    if matches!(cut, Cut::Started) {
        assert_eq!(view.state.status(), AgentRunStatus::Incomplete);
        assert!(model.requests.lock().unwrap().is_empty());
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            session_before,
            sessions.load_session(&session_id).await.unwrap()
        );
    } else {
        assert_eq!(view.state.status(), AgentRunStatus::Delivered);
        assert_eq!(model.requests.lock().unwrap().len(), 1);
        assert_eq!(
            tool.calls.load(Ordering::SeqCst),
            usize::from(matches!(cut, Cut::Observed | Cut::InvalidUsage))
        );
        let final_wal = checkpoints.load_run(&run_id).unwrap().unwrap();
        final_wal.validate().unwrap();
        let round_three = final_wal
            .records
            .iter()
            .find_map(|record| match &record.payload {
                GenericCheckpointEvent::ModelAttemptStarted {
                    round: 3, context, ..
                } => Some(context),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            round_three
                .planning
                .as_ref()
                .unwrap()
                .anchor
                .as_ref()
                .unwrap()
                .observed_input_tokens,
            400
        );
        assert_eq!(round_three.context_estimate.as_ref().unwrap().tokens, 600);
        let mut forged = final_wal.clone();
        let record = forged
            .records
            .iter_mut()
            .find(|record| {
                matches!(
                    record.payload,
                    GenericCheckpointEvent::ModelAttemptStarted { round: 3, .. }
                )
            })
            .unwrap();
        let mut payload = record.payload.clone();
        if let GenericCheckpointEvent::ModelAttemptStarted { context, .. } = &mut payload {
            context
                .planning
                .as_mut()
                .unwrap()
                .anchor
                .as_mut()
                .unwrap()
                .observed_input_tokens += 1;
        }
        // Re-seal to test semantic provenance, rather than only checksum rejection.
        *record = orchestral_runtime::generic_agent_checkpoint::GenericCheckpointRecord::seal(
            GenericCheckpointDraft {
                event_id: record.event_id.clone(),
                run_id: record.run_id.clone(),
                payload,
            },
            record.checkpoint_seq,
        )
        .unwrap();
        assert!(forged.validate().is_err());
        // The saved old anchor did not change after recovery/new observations.
        assert!(final_wal.records.iter().any(|record| matches!(&record.payload, GenericCheckpointEvent::ModelAttemptStarted { round: 2, context, .. } if context == &round_two)));
        let after = sessions.load_session(&session_id).await.unwrap();
        assert_eq!(&after[..session_before.len()], session_before.as_slice());
        assert_eq!(
            after
                .iter()
                .filter(|record| matches!(
                    record.payload,
                    AgentSessionEvent::ToolExchangeCommitted { .. }
                ))
                .count(),
            2
        );
    }
}
