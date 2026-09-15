use super::*;
use orchestral_core::model_protocol::ModelErrorCode;

struct GrowingInputMeter;

impl ModelTokenMeter for GrowingInputMeter {
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        ModelTokenMeterDescriptor {
            strategy: "test/growing-tool-input".to_owned(),
            version: "1".to_owned(),
            accounting: ModelTokenAccounting::Exact,
            config_digest: Digest::sha256("growing-tool-input/v1"),
        }
    }

    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        _: &[ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        Ok(
            if messages
                .iter()
                .any(|message| message.role == ModelRole::Tool)
            {
                1600
            } else {
                1000
            },
        )
    }
}

struct GrowingInputModel(Mutex<Vec<ModelRequest>>);

#[async_trait]
impl ModelBackend for GrowingInputModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "growing-input".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                max_context_tokens: Some(2200),
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
        self.0.lock().unwrap().push(request.clone());
        if request.max_output_tokens.unwrap() > 500 {
            return Err(ModelError::new(
                ModelErrorCode::ContextLengthExceeded,
                "output reservation exceeds backend capacity",
            ));
        }
        let has_result = request
            .messages
            .iter()
            .any(|message| message.role == ModelRole::Tool);
        let call_id = ModelToolCallId::new("one-observation");
        let mut events = if has_result {
            vec![ModelEvent::TextDelta {
                delta: "Completed after tool observation".to_owned(),
            }]
        } else {
            vec![
                ModelEvent::ToolCallStart {
                    call_id: call_id.clone(),
                    name: "echo".to_owned(),
                    extensions: Default::default(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: call_id.clone(),
                    delta: json!({"value": "observation"}).to_string(),
                },
                ModelEvent::ToolCallEnd { call_id },
            ]
        };
        events.push(ModelEvent::Finish {
            reason: if has_result {
                ModelFinishReason::Stop
            } else {
                ModelFinishReason::ToolCalls
            },
        });
        Ok(
            stream::iter(events.into_iter().enumerate().map(move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("growing-input-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            }))
            .boxed(),
        )
    }
}

#[tokio::test]
async fn output_only_recovery_releases_the_old_input_ceiling_after_success() {
    output_recovery_with_growth(false).await;
}

#[tokio::test]
async fn output_only_recovery_releases_input_after_restart_at_observed_tool_boundary() {
    output_recovery_with_growth(true).await;
}

async fn output_recovery_with_growth(restart: bool) {
    let model = Arc::new(GrowingInputModel(Mutex::new(Vec::new())));
    let lost_ack =
        restart.then(|| Arc::new(context_recovery::RejectionAckLostStore::after_observation()));
    let checkpoints: Arc<dyn GenericAgentCheckpointStore> = match &lost_ack {
        Some(store) => store.clone(),
        None => Arc::new(InMemoryGenericAgentCheckpointStore::default()),
    };
    let sessions = Arc::new(InMemoryAgentSessionJournalStore::default());
    let effects = Arc::new(InMemoryToolEffectJournalStore::default());
    let host = Arc::new(InMemoryAgentJournalStore::default());
    let tool = Arc::new(EchoTool {
        calls: AtomicUsize::new(0),
    });
    let bounds = ToolPolicyBounds {
        approval: ApprovalPolicy::NotRequired,
        ..Default::default()
    };
    let mut config = GenericAgentConfig::new("growth-provider", "growth-agent");
    config.max_context_tokens = 2200;
    config.reserved_output_tokens = 1000;
    config.context_recovery.minimum_output_tokens = std::num::NonZeroU64::new(500);
    let controller = || {
        let provider = InternalGenericAgentProvider::new_with_tools_and_session_journal(
            model.clone(),
            config.clone(),
            durable_direct_runtime(&bounds, effects.clone(), tool.clone()),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            sessions.clone(),
            Arc::new(GrowingInputMeter),
        )
        .unwrap()
        .with_checkpoint_store(checkpoints.clone())
        .unwrap();
        Arc::new(
            AgentController::with_journal_store(
                Arc::new(provider),
                ProviderBindingRef::new("growth-binding"),
                host.clone(),
            )
            .unwrap(),
        )
    };
    let first = controller();
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        AgentSessionId::new("growth-session"),
        RunId::new("growth-run"),
        vec![Content::text("Inspect and summarize")],
    )
    .unwrap();
    let run_id = run.spec.run_id.clone();
    first.start(run).await.unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        first.wait_for_terminal(&run_id),
    )
    .await
    .unwrap();
    let view = if let Some(store) = lost_ack {
        assert!(matches!(
            result,
            Err(AgentControlError::ContinuityUnknown(_))
        ));
        assert_eq!(tool.calls.load(Ordering::SeqCst), 0);
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
        store.resume();
        let restored = controller();
        restored.recover(&run_id).await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            restored.wait_for_terminal(&run_id),
        )
        .await
        .unwrap()
        .unwrap()
    } else {
        result.unwrap()
    };
    assert_eq!(view.state.status(), AgentRunStatus::Delivered);
    assert_eq!(tool.calls.load(Ordering::SeqCst), 1);
    let requests = model.0.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].max_output_tokens, Some(1000));
    assert_eq!(requests[1].max_output_tokens, Some(500));
    assert_eq!(requests[2].max_output_tokens, Some(500));
    assert_eq!(requests[0].messages, requests[1].messages);
    assert!(requests[2]
        .messages
        .iter()
        .any(|message| message.role == ModelRole::Tool));
    let stored = checkpoints.load_run(&run_id).unwrap().unwrap();
    let mut budgets = Vec::new();
    for record in &stored.records {
        if let GenericCheckpointEvent::ModelAttemptStarted { context, .. } = &record.payload {
            budgets.push(context.input_budget_tokens);
        }
    }
    assert_eq!(budgets, vec![1200, 1200, 1700]);
    assert_eq!(
        stored.validate().unwrap().phase,
        GenericCheckpointPhase::Terminal
    );
}
