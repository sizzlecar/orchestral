use super::*;
use orchestral_core::model_protocol::ModelErrorCode;

#[derive(Clone, Copy)]
enum Rejection {
    BeforeGeneration,
    AfterUsage,
    AfterText,
    AfterToolStart,
    Always,
    Initial,
}

struct CapacityModel {
    requests: Mutex<Vec<ModelRequest>>,
    rejection: Rejection,
    fixed_input_tokens: u64,
}

struct CapacityTokenMeter(u64);

impl ModelTokenMeter for CapacityTokenMeter {
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        ModelTokenMeterDescriptor {
            strategy: "test/capacity-fixed-and-exchange-tokens".to_owned(),
            version: "1".to_owned(),
            accounting: ModelTokenAccounting::Exact,
            config_digest: Digest::sha256(self.0.to_be_bytes()),
        }
    }

    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        Ok(ExchangeCountingTokenMeter.count_request_input(messages, tools)? + self.0)
    }
}

struct CapacitySummarizer;

#[async_trait]
impl orchestral_runtime::AgentSessionSummarizer for CapacitySummarizer {
    fn descriptor(&self) -> SessionSummarizerDescriptor {
        SessionSummarizerDescriptor {
            strategy: "capacity-recovery-summary".to_owned(),
            model: None,
            version: "1".to_owned(),
            config_digest: Digest::sha256("capacity-recovery-summary/v1"),
        }
    }
    async fn summarize(
        &self,
        input: SessionCompactionInput,
    ) -> Result<ModelMessage, SessionContextError> {
        assert!(!input.groups.is_empty());
        Ok(ModelMessage::text(
            ModelRole::Assistant,
            "active pressure summary",
        ))
    }
}

#[async_trait]
impl ModelBackend for CapacityModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "capacity-recovery-fixture".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                max_context_tokens: Some(4_500),
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
        let text = serde_json::to_string(&request.messages).unwrap();
        assert!(text.contains("Preserve stable_api"));
        self.requests.lock().unwrap().push(request.clone());
        let has_result = request
            .messages
            .iter()
            .any(|message| message.role == ModelRole::Tool);
        let summarized = text.contains("active pressure summary");
        let rejection = ModelError::new(
            ModelErrorCode::ContextLengthExceeded,
            "context rejected before generation",
        );
        if matches!(self.rejection, Rejection::Initial) {
            return Err(rejection);
        }
        let mut events =
            if has_result || (summarized && matches!(self.rejection, Rejection::Always)) {
                let prefix = match self.rejection {
                    Rejection::BeforeGeneration | Rejection::Always | Rejection::Initial => {
                        return Err(rejection)
                    }
                    Rejection::AfterUsage => ModelEvent::Usage {
                        usage: ModelUsage {
                            input_tokens: Some(1),
                            output_tokens: Some(1),
                        },
                    },
                    Rejection::AfterText => ModelEvent::TextDelta {
                        delta: "partial".to_owned(),
                    },
                    Rejection::AfterToolStart => ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("incomplete-call"),
                        name: "echo".to_owned(),
                        extensions: Default::default(),
                    },
                };
                return Ok(stream::iter([
                    Ok(ModelStreamEvent {
                        request_id: request.request_id.clone(),
                        event_id: ModelEventId::new("partial"),
                        sequence: 1,
                        payload: prefix,
                    }),
                    Err(rejection),
                ])
                .boxed());
            } else if summarized {
                vec![ModelEvent::TextDelta {
                    delta: "continued after rejection".to_owned(),
                }]
            } else {
                let call_id = ModelToolCallId::new("record-once");
                vec![
                    ModelEvent::ToolCallStart {
                        call_id: call_id.clone(),
                        name: "echo".to_owned(),
                        extensions: Default::default(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: call_id.clone(),
                        delta: json!({"value":"one observation"}).to_string(),
                    },
                    ModelEvent::ToolCallEnd { call_id },
                ]
            };
        events.push(ModelEvent::Usage {
            usage: ModelUsage {
                input_tokens: Some(self.fixed_input_tokens + if summarized { 800 } else { 700 }),
                output_tokens: Some(7),
            },
        });
        events.push(ModelEvent::Finish {
            reason: if summarized {
                ModelFinishReason::Stop
            } else {
                ModelFinishReason::ToolCalls
            },
        });
        Ok(
            stream::iter(events.into_iter().enumerate().map(move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("capacity-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            }))
            .boxed(),
        )
    }
}

#[derive(Default)]
struct RejectionAckLostStore {
    inner: InMemoryGenericAgentCheckpointStore,
    cut_once: AtomicBool,
    unavailable: AtomicBool,
}

impl GenericAgentCheckpointStore for RejectionAckLostStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.inner.load_run(run_id)
    }
    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        self.inner.create_run(registration)
    }
    fn append(
        &self,
        run_id: &RunId,
        previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(GenericCheckpointError::Unavailable(
                "process lost".to_owned(),
            ));
        }
        let rejected = matches!(
            draft.payload,
            GenericCheckpointEvent::ModelContextRejected { .. }
        );
        let result = self.inner.append(run_id, previous, draft)?;
        if rejected && !self.cut_once.swap(true, Ordering::SeqCst) {
            self.unavailable.store(true, Ordering::SeqCst);
            return Err(GenericCheckpointError::Unavailable(
                "lost ack after rejection commit".to_owned(),
            ));
        }
        Ok(result)
    }
}

fn provider(
    model: Arc<CapacityModel>,
    config: GenericAgentConfig,
    sessions: Arc<InMemoryAgentSessionJournalStore>,
    effects: Arc<InMemoryToolEffectJournalStore>,
    checkpoints: Arc<dyn GenericAgentCheckpointStore>,
    tool: Arc<EchoTool>,
) -> Arc<InternalGenericAgentProvider> {
    let meter = Arc::new(CapacityTokenMeter(model.fixed_input_tokens));
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1000),
        max_output_bytes: Some(1024),
        ..Default::default()
    };
    Arc::new(
        InternalGenericAgentProvider::new_with_tools_and_session_journal(
            model,
            config,
            durable_direct_runtime(&bounds, effects, tool),
            RunToolGrant { bounds },
            sessions,
            meter,
        )
        .unwrap()
        .with_checkpoint_store(checkpoints)
        .unwrap()
        .with_session_compaction(
            Arc::new(CapacitySummarizer),
            SessionCompactionPolicy {
                minimum_source_records: 32,
                keep_recent_records: 16,
            },
        )
        .unwrap(),
    )
}

fn config() -> GenericAgentConfig {
    let mut config = GenericAgentConfig::new("capacity-provider", "capacity-agent");
    config.max_context_tokens = 4500;
    config.reserved_output_tokens = 500;
    config
}

fn run() -> AgentRunEnvelope {
    let mut spec = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("capacity-session"),
        RunId::new("capacity-run"),
        vec![Content::text("Inspect the evidence. Preserve stable_api.")],
    )
    .unwrap()
    .spec;
    spec.limits.max_input_tokens = Some(10_000);
    spec.limits.max_output_tokens = Some(100);
    AgentRunEnvelope::seal(spec).unwrap()
}

#[tokio::test]
async fn capacity_recovery_compacts_whole_exchanges_and_preserves_usage_and_effects() {
    for (rejection, retries, fixed_input_tokens, delivered, calls) in [
        (Rejection::BeforeGeneration, 1, 0, true, 3),
        // Required context alone exceeds half the rejected request. A smaller
        // complete exchange summary still fits the original context ceiling.
        (Rejection::BeforeGeneration, 1, 1500, true, 3),
        (Rejection::BeforeGeneration, 0, 0, false, 2),
        (Rejection::Always, 1, 0, false, 3),
        (Rejection::Always, 1, 1500, false, 3),
        (Rejection::AfterUsage, 1, 0, false, 2),
        (Rejection::AfterText, 1, 0, false, 2),
        (Rejection::AfterToolStart, 1, 0, false, 2),
        (Rejection::Initial, 1, 0, false, 1),
        (Rejection::Initial, 1, 1500, false, 1),
    ] {
        let model = Arc::new(CapacityModel {
            requests: Mutex::new(Vec::new()),
            rejection,
            fixed_input_tokens,
        });
        let tool = Arc::new(EchoTool {
            calls: AtomicUsize::new(0),
        });
        let checkpoints = Arc::new(InMemoryGenericAgentCheckpointStore::default());
        let mut config = config();
        config.context_recovery.max_retries = retries;
        let controller = Arc::new(
            AgentController::new(
                provider(
                    model.clone(),
                    config,
                    Arc::new(InMemoryAgentSessionJournalStore::default()),
                    Arc::new(InMemoryToolEffectJournalStore::default()),
                    checkpoints.clone(),
                    tool.clone(),
                ),
                ProviderBindingRef::new("capacity-binding"),
            )
            .unwrap(),
        );
        let run = run();
        let run_id = run.spec.run_id.clone();
        controller.start(run).await.unwrap();
        let view = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            controller.wait_for_terminal(&run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            view.state.status(),
            if delivered {
                AgentRunStatus::Delivered
            } else {
                AgentRunStatus::Failed
            },
            "fixed_input_tokens={fixed_input_tokens}, retries={retries}"
        );
        assert_eq!(
            tool.calls.load(Ordering::SeqCst),
            usize::from(!matches!(rejection, Rejection::Initial))
        );
        assert_eq!(model.requests.lock().unwrap().len(), calls);
        let stored = checkpoints.load_run(&run_id).unwrap().unwrap();
        stored.validate().unwrap();
        let rejections = stored
            .records
            .iter()
            .filter(|record| {
                matches!(
                    record.payload,
                    GenericCheckpointEvent::ModelContextRejected { .. }
                )
            })
            .count();
        assert_eq!(
            rejections,
            usize::from(
                retries > 0
                    && matches!(
                        rejection,
                        Rejection::BeforeGeneration | Rejection::Always | Rejection::Initial
                    )
            )
        );
        if delivered {
            assert_rejection_history_integrity(&stored);
            let usage = view.delivery.unwrap().usage.unwrap();
            assert_eq!(usage.input_tokens, Some(1500 + 2 * fixed_input_tokens));
            assert_eq!(usage.output_tokens, Some(14));
            let requests = model.requests.lock().unwrap();
            assert_ne!(requests[1].request_id, requests[2].request_id);
            assert_eq!(&requests[1].messages[..2], &requests[2].messages[..2]);
            assert!(requests[2]
                .messages
                .iter()
                .all(|message| message.role != ModelRole::Tool));
        }
    }
}

fn assert_rejection_history_integrity(stored: &StoredGenericAgentRun) {
    #[derive(Clone, Copy)]
    enum Corruption {
        Identity,
        Count,
        Budget,
        ErrorCode,
    }
    for corruption in [
        Corruption::Identity,
        Corruption::Count,
        Corruption::Budget,
        Corruption::ErrorCode,
    ] {
        let mut forged = stored.clone();
        let record = forged
            .records
            .iter_mut()
            .find(|record| {
                matches!(
                    record.payload,
                    GenericCheckpointEvent::ModelContextRejected { .. }
                )
            })
            .unwrap();
        let mut payload = record.payload.clone();
        if let GenericCheckpointEvent::ModelContextRejected {
            request_id,
            retry_number,
            input_budget_tokens,
            error,
            ..
        } = &mut payload
        {
            match corruption {
                Corruption::Identity => *request_id = ModelRequestId::new("unrelated-request"),
                Corruption::Count => *retry_number += 1,
                Corruption::Budget => *input_budget_tokens = 4500,
                Corruption::ErrorCode => error.code = ModelErrorCode::InvalidRequest,
            }
        }
        let resealed = orchestral_runtime::generic_agent_checkpoint::GenericCheckpointRecord::seal(
            GenericCheckpointDraft {
                event_id: record.event_id.clone(),
                run_id: record.run_id.clone(),
                payload,
            },
            record.checkpoint_seq,
        );
        if let Ok(resealed) = resealed {
            *record = resealed;
            assert!(forged.validate().is_err());
        }
    }
}

#[tokio::test]
async fn capacity_recovery_cannot_bypass_the_run_model_step_limit() {
    let model = Arc::new(CapacityModel {
        requests: Mutex::new(Vec::new()),
        rejection: Rejection::BeforeGeneration,
        fixed_input_tokens: 0,
    });
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let checkpoints = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let controller = Arc::new(
        AgentController::new(
            provider(
                model.clone(),
                config(),
                Arc::new(InMemoryAgentSessionJournalStore::default()),
                Arc::new(InMemoryToolEffectJournalStore::default()),
                checkpoints.clone(),
                tool.clone(),
            ),
            ProviderBindingRef::new("capacity-binding"),
        )
        .unwrap(),
    );
    let mut spec = run().spec;
    spec.limits.max_model_steps = Some(2);
    let run_id = spec.run_id.clone();
    controller
        .start(AgentRunEnvelope::seal(spec).unwrap())
        .await
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        controller.wait_for_terminal(&run_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.state.status(), AgentRunStatus::Incomplete);
    assert_eq!(model.requests.lock().unwrap().len(), 2);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert!(controller
        .events(&run_id, 0)
        .await
        .unwrap()
        .iter()
        .any(|record| matches!(
            record.event.payload,
            AgentEvent::RunIncomplete {
                reason: IncompleteReason::LimitReached {
                    limit: RunLimitKind::ModelSteps,
                    ..
                },
                ..
            }
        )));
    checkpoints
        .load_run(&run_id)
        .unwrap()
        .unwrap()
        .validate()
        .unwrap();
}

#[tokio::test]
async fn committed_capacity_rejection_recovers_after_lost_ack_without_repeating_tools() {
    let checkpoints = Arc::new(RejectionAckLostStore::default());
    let sessions = Arc::new(InMemoryAgentSessionJournalStore::default());
    let effects = Arc::new(InMemoryToolEffectJournalStore::default());
    let host = Arc::new(InMemoryAgentJournalStore::default());
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let first_model = Arc::new(CapacityModel {
        requests: Mutex::new(Vec::new()),
        rejection: Rejection::BeforeGeneration,
        fixed_input_tokens: 1500,
    });
    let first = Arc::new(
        AgentController::with_journal_store(
            provider(
                first_model.clone(),
                config(),
                sessions.clone(),
                effects.clone(),
                checkpoints.clone(),
                tool.clone(),
            ),
            ProviderBindingRef::new("capacity-binding"),
            host.clone(),
        )
        .unwrap(),
    );
    let run = run();
    let run_id = run.spec.run_id.clone();
    first.start(run).await.unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        first.wait_for_terminal(&run_id),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(AgentControlError::ContinuityUnknown(_))
    ));
    let stored = checkpoints.load_run(&run_id).unwrap().unwrap();
    let projection = stored.validate().unwrap();
    assert!(
        matches!(projection.phase,GenericCheckpointPhase::Stable(ref boundary) if boundary.next_model_round==3)
    );
    assert_eq!(projection.context_recovery.unwrap().retry_number, 1);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    checkpoints.unavailable.store(false, Ordering::SeqCst);
    let model = Arc::new(CapacityModel {
        requests: Mutex::new(Vec::new()),
        rejection: Rejection::BeforeGeneration,
        fixed_input_tokens: 1500,
    });
    let restored = Arc::new(
        AgentController::with_journal_store(
            provider(
                model.clone(),
                config(),
                sessions,
                effects,
                checkpoints.clone(),
                tool.clone(),
            ),
            ProviderBindingRef::new("capacity-binding"),
            host,
        )
        .unwrap(),
    );
    restored.recover(&run_id).await.unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        restored.wait_for_terminal(&run_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.state.status(), AgentRunStatus::Delivered);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    assert_eq!(first_model.requests.lock().unwrap().len(), 2);
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    assert!(checkpoints
        .load_run(&run_id)
        .unwrap()
        .unwrap()
        .validate()
        .unwrap()
        .context_recovery
        .is_none());
}
