use super::*;
use orchestral_runtime::{AgentSessionSummarizer, GenericModelContextTrace};

const CONTEXT_CAPACITY: u64 = 128_000;
const OUTPUT_RESERVE: u64 = 1_024;
const HANDOFF: &str = "Historical observations retained in a complete task handoff.";

struct RequestRecordingModel {
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

#[async_trait]
impl ModelBackend for RequestRecordingModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "maintenance-regression-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                max_context_tokens: Some(CONTEXT_CAPACITY),
                max_output_tokens: Some(OUTPUT_RESERVE),
                ..Default::default()
            },
            extensions: BTreeMap::new(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        self.requests.lock().unwrap().push(request.clone());
        Ok(stream::iter(
            [
                ModelEvent::TextDelta {
                    delta: "Hello.".to_owned(),
                },
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            ]
            .into_iter()
            .enumerate()
            .map(move |(index, payload)| {
                Ok(ModelStreamEvent {
                    request_id: request.request_id.clone(),
                    event_id: ModelEventId::new(format!("reply-{index}")),
                    sequence: index as u64 + 1,
                    payload,
                })
            }),
        )
        .boxed())
    }
}

struct CountedSemanticHandoff(Arc<AtomicUsize>);

#[async_trait]
impl AgentSessionSummarizer for CountedSemanticHandoff {
    // Keep the conservative may_call_model=true default: these tests verify
    // the pressure gate, not the separate cumulative-usage exclusion gate.
    fn descriptor(&self) -> SessionSummarizerDescriptor {
        SessionSummarizerDescriptor {
            strategy: "maintenance-regression-summary".to_owned(),
            model: Some("offline-fixture".to_owned()),
            version: "1".to_owned(),
            config_digest: Digest::sha256("maintenance-regression-summary/v1"),
        }
    }

    async fn summarize(
        &self,
        input: SessionCompactionInput,
    ) -> Result<ModelMessage, SessionContextError> {
        assert!(!input.groups.is_empty());
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ModelMessage::text(ModelRole::Assistant, HANDOFF))
    }
}

struct Outcome {
    summary_calls: usize,
    request: ModelRequest,
    context: GenericModelContextTrace,
    complete_input_tokens: u64,
    compactions: usize,
}

async fn resume_with_history(
    history_records: usize,
    record_size: usize,
    history_limit: usize,
    current_input: String,
) -> Outcome {
    let session = AgentSessionId::new("maintenance-session");
    let run_id = RunId::new("resumed-run");
    let journal = Arc::new(InMemoryAgentSessionJournalStore::default());
    let mut all_messages = Vec::new();
    let mut config = GenericAgentConfig::new("maintenance-provider", "maintenance-agent");
    config.system_prompt = "Follow the current task and preserve earlier constraints.".to_owned();
    config.max_context_tokens = CONTEXT_CAPACITY;
    config.reserved_output_tokens = OUTPUT_RESERVE;
    config.history_limit = history_limit;
    all_messages.push(ModelMessage::text(
        ModelRole::System,
        config.system_prompt.clone(),
    ));
    for index in 0..history_records {
        let message = ModelMessage::text(
            ModelRole::User,
            format!("Observation {index}: {}", "x".repeat(record_size)),
        );
        all_messages.push(message.clone());
        journal
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!("historical-{index}")),
                session_id: session.clone(),
                run_id: RunId::new("previous-run"),
                payload: AgentSessionEvent::RunInputCommitted { message },
            })
            .await
            .unwrap();
    }
    all_messages.push(ModelMessage::text(ModelRole::User, current_input.clone()));
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let checkpoints = Arc::new(InMemoryGenericAgentCheckpointStore::default());
    let provider = InternalGenericAgentProvider::new_with_session_journal(
        Arc::new(RequestRecordingModel {
            requests: requests.clone(),
        }),
        config,
        journal.clone(),
        Arc::new(JsonSizeTokenMeter::default()),
    )
    .unwrap()
    .with_checkpoint_store(checkpoints.clone())
    .unwrap()
    .with_session_compaction(
        Arc::new(CountedSemanticHandoff(calls.clone())),
        SessionCompactionPolicy {
            minimum_source_records: 2,
            keep_recent_records: 2,
        },
    )
    .unwrap();
    let controller = Arc::new(
        AgentController::new(
            Arc::new(provider),
            ProviderBindingRef::new("maintenance-binding"),
        )
        .unwrap(),
    );
    let run = AgentRunEnvelope::new(
        AGENT_PROTOCOL_V1,
        session.clone(),
        run_id.clone(),
        vec![Content::text(current_input)],
    )
    .unwrap();
    controller.start(run).await.unwrap();
    let terminal = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        controller.wait_for_terminal(&run_id),
    )
    .await
    .expect("offline model turn must finish")
    .unwrap();
    assert_eq!(terminal.state.status(), AgentRunStatus::Delivered);
    let request = {
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "the normal user model request must run");
        requests[0].clone()
    };
    let stored = checkpoints.load_run(&run_id).unwrap().unwrap();
    let context = stored
        .records
        .iter()
        .find_map(|record| match &record.payload {
            GenericCheckpointEvent::ModelAttemptStarted { context, .. } => Some(context.clone()),
            _ => None,
        })
        .expect("model context is durably recorded");
    let records = journal.load_session(&session).await.unwrap();
    let compactions = records
        .iter()
        .filter(|record| {
            matches!(
                record.payload,
                AgentSessionEvent::CompactionCommitted { .. }
                    | AgentSessionEvent::ActiveRunCompactionCommitted { .. }
            )
        })
        .count();
    Outcome {
        summary_calls: calls.load(Ordering::SeqCst),
        complete_input_tokens: JsonSizeTokenMeter::default()
            .count_request_input(&all_messages, &request.tools)
            .unwrap(),
        request,
        context,
        compactions,
    }
}

#[tokio::test]
async fn record_limit_deferral_does_not_summarize_before_a_resumed_user_turn() {
    let result = resume_with_history(40, 128, 4, "Hello".to_owned()).await;
    assert!(
        result.complete_input_tokens < result.context.input_budget_tokens / 4,
        "even all history fits the historical byte allowance; deferral is from record count"
    );
    assert!(!result.context.deferred_ranges.is_empty());
    assert_eq!(result.context.history_limit, 4);
    assert_eq!(result.summary_calls, 0);
    assert_eq!(result.compactions, 0);
    assert!(serde_json::to_string(&result.request.messages)
        .unwrap()
        .contains("Hello"));
}

#[tokio::test]
async fn historical_share_deferral_below_real_capacity_does_not_invoke_a_summary_model() {
    let result = resume_with_history(8, 8_000, 100, "Hello".to_owned()).await;
    assert!(result.complete_input_tokens > result.context.input_budget_tokens / 4);
    assert!(
        result.complete_input_tokens < result.context.input_budget_tokens,
        "the full source fits model capacity even though the history share excludes some groups"
    );
    assert!(!result.context.deferred_ranges.is_empty());
    assert!(result.context.used_input_tokens < result.context.input_budget_tokens / 2);
    assert_eq!(result.summary_calls, 0);
    assert_eq!(result.compactions, 0);
}

#[tokio::test]
async fn actual_model_derived_soft_pressure_still_compacts_history_before_dispatch() {
    let result = resume_with_history(8, 3_000, 100, "current task details ".repeat(4_600)).await;
    let high_watermark =
        result.context.input_budget_tokens - result.context.input_budget_tokens / 10;
    assert!(result.complete_input_tokens > high_watermark);
    assert!(
        result.complete_input_tokens < result.context.input_budget_tokens,
        "the regression exercises optional maintenance, not hard overflow recovery"
    );
    assert_eq!(result.summary_calls, 1);
    assert_eq!(result.compactions, 1);
    assert!(result.context.used_input_tokens < high_watermark);
    assert!(serde_json::to_string(&result.request.messages)
        .unwrap()
        .contains(HANDOFF));
}
