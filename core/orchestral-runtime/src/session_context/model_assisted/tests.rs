use super::*;
use std::sync::Mutex;

use futures_util::stream;
use orchestral_core::agent_protocol::wire::{AgentSessionId, RunId};
use orchestral_core::agent_session::{
    AgentSessionEvent, AgentSessionEventDraft, AgentSessionEventId, AgentSessionJournalStore,
    InMemoryAgentSessionJournalStore,
};
use orchestral_core::model_protocol::{
    ModelCapabilities, ModelContent, ModelDescriptor, ModelError, ModelErrorCode, ModelEventId,
    ModelStream, ModelStreamEvent, ModelToolCallId, ModelUsage,
};
use serde_json::json;

use crate::session_context::{AgentSessionCompactor, SessionCompactionPolicy};

struct MockSummaryBackend {
    response: Result<Vec<ModelEvent>, ModelError>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

#[async_trait]
impl ModelBackend for MockSummaryBackend {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "mock-summary-backend".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                parallel_tool_calls: true,
                structured_output: false,
                max_context_tokens: Some(128_000),
                max_output_tokens: Some(8_192),
            },
            extensions: BTreeMap::new(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        self.requests.lock().unwrap().push(request.clone());
        match &self.response {
            Ok(events) => {
                let req_id = request.request_id;
                let items = events
                    .clone()
                    .into_iter()
                    .enumerate()
                    .map(move |(idx, payload)| {
                        Ok(ModelStreamEvent {
                            event_id: ModelEventId::new(format!("ev-{}", idx + 1)),
                            request_id: req_id.clone(),
                            sequence: (idx + 1) as u64,
                            payload,
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(stream::iter(items).boxed())
            }
            Err(err) => Err(err.clone()),
        }
    }
}

async fn build_sample_journal() -> Arc<InMemoryAgentSessionJournalStore> {
    let store = Arc::new(InMemoryAgentSessionJournalStore::default());
    store
        .append(AgentSessionEventDraft {
            event_id: AgentSessionEventId::new("in-1"),
            session_id: AgentSessionId::new("s-1"),
            run_id: RunId::new("run-1"),
            payload: AgentSessionEvent::RunInputCommitted {
                message: ModelMessage::text(
                    ModelRole::User,
                    "Implement /goal mode in orchestral-cli",
                ),
            },
        })
        .await
        .unwrap();
    for seq in 1..=6 {
        let call_id = ModelToolCallId::new(format!("call-{seq}"));
        store
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!("tx-{seq}")),
                session_id: AgentSessionId::new("s-1"),
                run_id: RunId::new("run-1"),
                payload: AgentSessionEvent::ToolExchangeCommitted {
                    request_id: ModelRequestId::new(format!("req-{seq}")),
                    assistant: ModelMessage {
                        role: ModelRole::Assistant,
                        content: vec![ModelContent::ToolCall {
                            call_id: call_id.clone(),
                            name: "file_read".to_owned(),
                            arguments: json!({
                                "path": format!("src/mod_{seq}.rs"),
                                "offset": 1,
                                "limit": 80
                            }),
                            extensions: BTreeMap::new(),
                        }],
                    },
                    tool: ModelMessage {
                        role: ModelRole::Tool,
                        content: vec![ModelContent::ToolResult {
                            call_id,
                            is_error: false,
                            result: json!({
                                "path": format!("src/mod_{seq}.rs"),
                                "start_line": 1,
                                "end_line": 80,
                                "eof": true,
                                "content": format!("pub fn step_{seq}() {{}}\n")
                            }),
                        }],
                    },
                    retained_artifacts: Vec::new(),
                    usage: Some(ModelUsage {
                        input_tokens: Some(1000),
                        output_tokens: Some(100),
                    }),
                },
            })
            .await
            .unwrap();
    }
    store
}

#[tokio::test]
async fn model_assisted_summarizer_combines_structured_handoff_and_observation_ledger() {
    let store = build_sample_journal().await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let handoff = "### 1. Primary Request & Intent\n- Implement /goal mode in orchestral-cli.\n\n### 6. Pending Tasks & Immediate Next Step\n- Edit src/mod_6.rs.";
    let backend = Arc::new(MockSummaryBackend {
        response: Ok(vec![
            ModelEvent::TextDelta {
                delta: handoff.to_owned(),
            },
            ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            },
        ]),
        requests: requests.clone(),
    });
    let summarizer = Arc::new(
        ModelAssistedSessionSummarizer::new(backend, Some("test-model".to_owned()), 8192).unwrap(),
    );
    let compactor = AgentSessionCompactor::new(
        store.clone(),
        summarizer,
        SessionCompactionPolicy {
            minimum_source_records: 2,
            keep_recent_records: 3,
        },
    )
    .unwrap();
    let record = compactor
        .compact_active_run_for_pressure(&AgentSessionId::new("s-1"), &RunId::new("run-1"))
        .await
        .unwrap()
        .unwrap();
    let AgentSessionEvent::ActiveRunCompactionCommitted {
        summary,
        strategy,
        model,
        source,
        ..
    } = record.payload
    else {
        panic!("expected active run compaction");
    };
    assert_eq!(strategy, "model-assisted");
    assert_eq!(model.as_deref(), Some("test-model"));
    // keep_recent_records = 3 retains seq 5, 6, 7 (tool exchanges 4, 5, 6) and compacts 2..=4.
    assert_eq!(source.first_session_seq, 2);
    assert_eq!(source.last_session_seq, 4);

    let ModelContent::Text { text } = &summary.content[0] else {
        panic!("expected text summary");
    };
    assert!(text.contains("shadowed_session_seq=2..4"));
    assert!(text.contains("[working state handoff]"));
    assert!(text.contains("Implement /goal mode in orchestral-cli"));
    assert!(text.contains("file_inspections_v1"));
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn model_assisted_summarizer_falls_back_to_deterministic_extractive_on_backend_error() {
    let store = build_sample_journal().await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = Arc::new(MockSummaryBackend {
        response: Err(ModelError::new(
            ModelErrorCode::Unavailable,
            "simulated backend outage",
        )),
        requests: requests.clone(),
    });
    let summarizer = Arc::new(
        ModelAssistedSessionSummarizer::new(backend, Some("test-model".to_owned()), 8192).unwrap(),
    );
    let compactor = AgentSessionCompactor::new(
        store,
        summarizer,
        SessionCompactionPolicy {
            minimum_source_records: 2,
            keep_recent_records: 3,
        },
    )
    .unwrap();
    let record = compactor
        .compact_active_run_for_pressure(&AgentSessionId::new("s-1"), &RunId::new("run-1"))
        .await
        .unwrap()
        .unwrap();
    let AgentSessionEvent::ActiveRunCompactionCommitted { summary, .. } = record.payload else {
        panic!("expected active run compaction");
    };
    let ModelContent::Text { text } = &summary.content[0] else {
        panic!("expected text summary");
    };
    assert!(text.contains("shadowed_session_seq=2..4"));
    assert!(text.contains("src/mod_1.rs"));
    assert!(!text.contains("[working state handoff]"));
}

#[tokio::test]
async fn repeated_active_compaction_respects_keep_recent_records_instead_of_keeping_only_one() {
    let store = build_sample_journal().await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = Arc::new(MockSummaryBackend {
        response: Ok(vec![
            ModelEvent::TextDelta {
                delta: "### 1. Primary Request & Intent\n- First summary.".to_owned(),
            },
            ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            },
        ]),
        requests,
    });
    let summarizer = Arc::new(
        ModelAssistedSessionSummarizer::new(backend, Some("test-model".to_owned()), 8192).unwrap(),
    );
    let compactor = AgentSessionCompactor::new(
        store.clone(),
        summarizer,
        SessionCompactionPolicy {
            minimum_source_records: 2,
            keep_recent_records: 3,
        },
    )
    .unwrap();
    // First compaction: records 1 (input) + 2..=7 (6 tool exchanges).
    // Retains recent 3 exchanges (5, 6, 7), compacts 2..=4 -> record 8.
    let first = compactor
        .compact_active_run_for_pressure(&AgentSessionId::new("s-1"), &RunId::new("run-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.session_seq, 8);

    // Append 4 more tool exchanges (records 9, 10, 11, 12).
    // Post-summary exchanges = 4 > keep_recent_records (3), so the newest 3
    // (10, 11, 12) are retained verbatim and records 5..=9 are folded.
    for seq in 7..=10 {
        let call_id = ModelToolCallId::new(format!("call-{seq}"));
        store
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!("tx-{seq}")),
                session_id: AgentSessionId::new("s-1"),
                run_id: RunId::new("run-1"),
                payload: AgentSessionEvent::ToolExchangeCommitted {
                    request_id: ModelRequestId::new(format!("req-{seq}")),
                    assistant: ModelMessage {
                        role: ModelRole::Assistant,
                        content: vec![ModelContent::ToolCall {
                            call_id: call_id.clone(),
                            name: "file_read".to_owned(),
                            arguments: json!({ "path": format!("src/mod_{seq}.rs") }),
                            extensions: BTreeMap::new(),
                        }],
                    },
                    tool: ModelMessage {
                        role: ModelRole::Tool,
                        content: vec![ModelContent::ToolResult {
                            call_id,
                            is_error: false,
                            result: json!({ "path": format!("src/mod_{seq}.rs") }),
                        }],
                    },
                    retained_artifacts: Vec::new(),
                    usage: None,
                },
            })
            .await
            .unwrap();
    }

    // Second compaction with keep_recent_records = 3:
    // Must retain the newest 3 tool exchanges (records 10, 11, 12) and fold
    // records 5, 6, 7, prior summary 8, and 9 (range 5..=9), NOT 5..=11!
    let second = compactor
        .compact_active_run_for_pressure(&AgentSessionId::new("s-1"), &RunId::new("run-1"))
        .await
        .unwrap()
        .unwrap();
    let AgentSessionEvent::ActiveRunCompactionCommitted { source, .. } = second.payload else {
        panic!("expected active run compaction");
    };
    assert_eq!(
        source,
        crate::session_context::SessionSourceRange {
            first_session_seq: 5,
            last_session_seq: 9,
        }
    );
}

fn text_input(source_text: &str, focus: &str) -> SessionCompactionInput {
    use crate::session_context::{SessionCompactionGroup, SessionSourceRange};
    SessionCompactionInput {
        session_id: AgentSessionId::new("handoff-tests"),
        source: SessionSourceRange {
            first_session_seq: 1,
            last_session_seq: 3,
        },
        groups: (1..=3)
            .map(|seq| SessionCompactionGroup {
                source: SessionSourceRange {
                    first_session_seq: seq,
                    last_session_seq: seq,
                },
                messages: vec![ModelMessage::text(ModelRole::User, source_text)],
            })
            .collect(),
        focus_messages: vec![ModelMessage::text(ModelRole::User, focus)],
    }
}

fn responding_summarizer(
    events: Vec<ModelEvent>,
) -> (
    ModelAssistedSessionSummarizer,
    Arc<Mutex<Vec<ModelRequest>>>,
) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = Arc::new(MockSummaryBackend {
        response: Ok(events),
        requests: requests.clone(),
    });
    (
        ModelAssistedSessionSummarizer::new(backend, Some("test-model".to_owned()), 16_384)
            .unwrap(),
        requests,
    )
}

fn summary_text(generation: &SessionSummaryGeneration) -> &str {
    let ModelContent::Text { text } = &generation.message.content[0] else {
        panic!("expected text summary");
    };
    text
}

#[tokio::test]
async fn length_limited_output_is_not_a_semantic_handoff() {
    let (summarizer, requests) = responding_summarizer(vec![
        ModelEvent::TextDelta {
            delta: "INCOMPLETE_HANDOFF_MARKER".to_owned(),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::Length,
        },
    ]);
    let generation = summarizer
        .generate(text_input(
            "Implement a bounded queue",
            "Preserve cancellation",
        ))
        .await
        .unwrap();
    assert_eq!(
        generation.fallback_reason.as_deref(),
        Some("model_length_limited")
    );
    assert_eq!(generation.descriptor.strategy, "deterministic-extractive");
    assert!(!summary_text(&generation).contains("INCOMPLETE_HANDOFF_MARKER"));
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn overlong_finished_output_is_rejected_without_post_generation_truncation() {
    let (summarizer, _) = responding_summarizer(vec![
        ModelEvent::TextDelta {
            delta: "UNBOUNDED_NARRATIVE".repeat(2000),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::Stop,
        },
    ]);
    let generation = summarizer
        .generate(text_input(
            "Implement a bounded queue",
            "Preserve cancellation",
        ))
        .await
        .unwrap();
    assert_eq!(
        generation.fallback_reason.as_deref(),
        Some("output_budget_exceeded")
    );
    assert!(!summary_text(&generation).contains("UNBOUNDED_NARRATIVE"));
    assert!(summary_text(&generation).chars().count() <= summarizer.max_summary_chars());
}

#[tokio::test]
async fn source_middle_and_late_task_constraints_reach_the_model_unabridged() {
    let handoff = "Primary Goal & Constraints: implement bounded buffering; never discard queued data.\nDiscovered Code & Architecture: the callback owns the queue lock.\nCompleted Changes & Verifications: inspected only; no edits or checks completed.\nOpen Issues & Next Steps: move the callback outside the lock and test cancellation.";
    let (summarizer, requests) = responding_summarizer(vec![
        ModelEvent::TextDelta {
            delta: handoff.to_owned(),
        },
        ModelEvent::Finish {
            reason: ModelFinishReason::Stop,
        },
    ]);
    let source = format!(
        "{}\nCRITICAL_MIDDLE_FACT: callback owns queue lock\n{}",
        "code line\n".repeat(600),
        "other code\n".repeat(600)
    );
    let focus = format!(
        "{}\nFINAL_CONSTRAINT: never discard queued data",
        "Context for the active task.\n".repeat(150)
    );
    let generation = summarizer
        .generate(text_input(&source, &focus))
        .await
        .unwrap();
    assert_eq!(generation.fallback_reason, None);
    assert_eq!(generation.descriptor.strategy, "model-assisted");
    assert!(summary_text(&generation).ends_with(handoff));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let ModelContent::Text { text } = &requests[0].messages[1].content[0] else {
        panic!("expected prompt")
    };
    assert!(text.contains(&source));
    assert!(text.contains(&focus));
    assert_eq!(text.matches("CRITICAL_MIDDLE_FACT").count(), 3);
    assert!(!text.contains("excerpt omitted"));
    assert_eq!(requests[0].max_output_tokens, Some(8192));
}

struct CapacityBackend {
    inner: MockSummaryBackend,
    capacity: Option<u64>,
}

#[async_trait]
impl ModelBackend for CapacityBackend {
    fn descriptor(&self) -> ModelDescriptor {
        let mut descriptor = self.inner.descriptor();
        descriptor.capabilities.max_context_tokens = self.capacity;
        descriptor
    }

    async fn start(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        self.inner.start(request, cancellation).await
    }
}

#[tokio::test]
async fn source_that_exceeds_metadata_capacity_is_not_silently_excerpted() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = Arc::new(CapacityBackend {
        inner: MockSummaryBackend {
            response: Ok(Vec::new()),
            requests: requests.clone(),
        },
        capacity: Some(16_384),
    });
    let summarizer = ModelAssistedSessionSummarizer::new(backend, None, 8192).unwrap();
    let generation = summarizer
        .generate(text_input(
            &"large original content\n".repeat(1000),
            "Keep the full task",
        ))
        .await
        .unwrap();
    assert_eq!(
        generation.fallback_reason.as_deref(),
        Some("input_capacity_exceeded")
    );
    assert_eq!(generation.descriptor.strategy, "deterministic-extractive");
    assert!(requests.lock().unwrap().is_empty());
    assert!(summary_text(&generation).contains("shadowed_session_seq=1..3"));
}

#[tokio::test]
async fn unknown_model_capacity_is_an_explicit_fallback() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = Arc::new(CapacityBackend {
        inner: MockSummaryBackend {
            response: Ok(Vec::new()),
            requests: requests.clone(),
        },
        capacity: None,
    });
    let summarizer = ModelAssistedSessionSummarizer::new(backend, None, 8192).unwrap();
    let generation = summarizer
        .generate(text_input("original content", "Keep the full task"))
        .await
        .unwrap();
    assert_eq!(
        generation.fallback_reason.as_deref(),
        Some("input_capacity_unknown")
    );
    assert!(requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn backend_error_reports_actual_strategy_and_a_stable_safe_reason() {
    let backend = Arc::new(MockSummaryBackend {
        response: Err(ModelError::new(
            ModelErrorCode::Unavailable,
            "sensitive provider message",
        )),
        requests: Arc::new(Mutex::new(Vec::new())),
    });
    let summarizer = ModelAssistedSessionSummarizer::new(backend, None, 8192).unwrap();
    let generation = summarizer
        .generate(text_input("Original facts", "Current objective"))
        .await
        .unwrap();
    assert_eq!(
        generation.fallback_reason.as_deref(),
        Some("model_backend_error")
    );
    assert_eq!(generation.descriptor.strategy, "deterministic-extractive");
    assert_eq!(generation.descriptor.model, None);
    assert!(!summary_text(&generation).contains("sensitive provider message"));
}

struct PendingBackend {
    streaming: bool,
    cancellation: Arc<Mutex<Option<CancellationToken>>>,
}

#[async_trait]
impl ModelBackend for PendingBackend {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "pending-summary".to_owned(),
            capabilities: ModelCapabilities {
                max_context_tokens: Some(128_000),
                max_output_tokens: Some(8192),
                ..Default::default()
            },
            extensions: BTreeMap::new(),
        }
    }

    async fn start(
        &self,
        _request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        *self.cancellation.lock().unwrap() = Some(cancellation);
        if self.streaming {
            Ok(stream::pending().boxed())
        } else {
            futures_util::future::pending().await
        }
    }
}

#[tokio::test(start_paused = true)]
async fn configurable_timeout_covers_start_and_stream_and_cancels_the_backend() {
    for streaming in [false, true] {
        let cancellation = Arc::new(Mutex::new(None));
        let backend = Arc::new(PendingBackend {
            streaming,
            cancellation: cancellation.clone(),
        });
        let summarizer = ModelAssistedSessionSummarizer::new(backend, None, 8192)
            .unwrap()
            .with_request_timeout(Duration::from_secs(61))
            .unwrap();
        let start = tokio::time::Instant::now();
        let generation = summarizer
            .generate(text_input("Original facts", "Current objective"))
            .await
            .unwrap();
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(61));
        assert_eq!(generation.fallback_reason.as_deref(), Some("model_timeout"));
        assert!(cancellation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled());
    }
}

#[tokio::test]
async fn missing_finish_and_empty_stop_are_diagnosed() {
    for (events, reason) in [
        (
            vec![ModelEvent::TextDelta {
                delta: "unfinished handoff".to_owned(),
            }],
            "model_incomplete_response",
        ),
        (
            vec![ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            }],
            "model_empty_response",
        ),
    ] {
        let (summarizer, _) = responding_summarizer(events);
        let generation = summarizer
            .generate(text_input("Original facts", "Current objective"))
            .await
            .unwrap();
        assert_eq!(generation.fallback_reason.as_deref(), Some(reason));
    }
}

struct PlanningMeter {
    estimate: u64,
}

impl ModelTokenMeter for PlanningMeter {
    fn meter_descriptor(&self) -> orchestral_core::model_protocol::ModelTokenMeterDescriptor {
        JsonSizeTokenMeter::default().meter_descriptor()
    }

    fn count_request_input(
        &self,
        _messages: &[ModelMessage],
        _tools: &[orchestral_core::model_protocol::ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        Ok(200_000)
    }

    fn estimate_context_input(
        &self,
        _messages: &[ModelMessage],
        _tools: &[orchestral_core::model_protocol::ModelToolDefinition],
    ) -> Result<orchestral_core::model_protocol::ModelContextEstimate, ModelError> {
        Ok(orchestral_core::model_protocol::ModelContextEstimate {
            tokens: self.estimate,
            accounting: orchestral_core::model_protocol::ModelTokenAccounting::Estimated,
        })
    }
}

#[tokio::test]
async fn planning_estimate_controls_capacity_without_treating_bytes_as_actual_tokens() {
    for (estimate, expected_reason, expected_requests) in [
        (1000, None, 1),
        (127_000, Some("input_capacity_exceeded"), 0),
    ] {
        let (summarizer, requests) = responding_summarizer(vec![
            ModelEvent::TextDelta {
                delta: "Goal: finish the parser. Next: test cancellation before changing code."
                    .to_owned(),
            },
            ModelEvent::Finish {
                reason: ModelFinishReason::Stop,
            },
        ]);
        let summarizer = summarizer
            .with_token_meter(Arc::new(PlanningMeter { estimate }))
            .unwrap();
        let generation = summarizer
            .generate(text_input("Original facts", "Current objective"))
            .await
            .unwrap();
        assert_eq!(generation.fallback_reason.as_deref(), expected_reason);
        assert_eq!(requests.lock().unwrap().len(), expected_requests);
    }
}

#[tokio::test]
async fn tight_budget_fallback_keeps_a_complete_original_source_index() {
    use crate::session_context::{SessionCompactionGroup, SessionSourceRange};
    let (summarizer, requests) = responding_summarizer(Vec::new());
    let source = SessionSourceRange {
        first_session_seq: 2300,
        last_session_seq: 2305,
    };
    let input = SessionCompactionInput {
        session_id: AgentSessionId::new("tight-budget"),
        source: source.clone(),
        groups: vec![SessionCompactionGroup {
            source,
            messages: vec![ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::ToolCall {
                    call_id: ModelToolCallId::new("read-with-missing-result"),
                    name: "file_read".to_owned(),
                    arguments: json!({"path": "src/parser.rs"}),
                    extensions: BTreeMap::new(),
                }],
            }],
        }],
        focus_messages: Vec::new(),
    };
    let generation = summarizer
        .generate_with_char_budget(input, 256)
        .await
        .unwrap();
    assert!(generation.fallback_reason.is_some());
    assert!(requests.lock().unwrap().is_empty());
    let text = summary_text(&generation);
    assert!(text.chars().count() <= 256);
    let receipt = text.lines().find(|line| line.starts_with('{')).unwrap();
    let receipt: serde_json::Value = serde_json::from_str(receipt).unwrap();
    assert_eq!(receipt["omitted_sources"], json!([[2300, 2305]]));
}

#[derive(Clone, Copy)]
enum StreamViolation {
    WrongRequest,
    DuplicateSequence,
    MissingSequence,
    EmptyEventId,
    InvalidPayload,
}

struct InvalidStreamBackend(StreamViolation);

#[async_trait]
impl ModelBackend for InvalidStreamBackend {
    fn descriptor(&self) -> ModelDescriptor {
        MockSummaryBackend {
            response: Ok(Vec::new()),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
        .descriptor()
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        let mut events = vec![
            ModelStreamEvent {
                request_id: request.request_id.clone(),
                event_id: ModelEventId::new("text"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "UNVERIFIED_STREAM_HANDOFF".to_owned(),
                },
            },
            ModelStreamEvent {
                request_id: request.request_id,
                event_id: ModelEventId::new("finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            },
        ];
        match self.0 {
            StreamViolation::WrongRequest => {
                events[1].request_id = ModelRequestId::new("different-request")
            }
            StreamViolation::DuplicateSequence => events[1].sequence = 1,
            StreamViolation::MissingSequence => events[1].sequence = 3,
            StreamViolation::EmptyEventId => events[1].event_id = ModelEventId::new(""),
            StreamViolation::InvalidPayload => {
                events[0].payload = ModelEvent::TextDelta {
                    delta: String::new(),
                }
            }
        }
        Ok(stream::iter(events.into_iter().map(Ok)).boxed())
    }
}

#[tokio::test]
async fn invalid_stream_identity_order_and_payload_never_become_a_handoff() {
    for violation in [
        StreamViolation::WrongRequest,
        StreamViolation::DuplicateSequence,
        StreamViolation::MissingSequence,
        StreamViolation::EmptyEventId,
        StreamViolation::InvalidPayload,
    ] {
        let summarizer = ModelAssistedSessionSummarizer::new(
            Arc::new(InvalidStreamBackend(violation)),
            None,
            8192,
        )
        .unwrap();
        let generation = summarizer
            .generate(text_input("Original facts", "Current objective"))
            .await
            .unwrap();
        assert_eq!(
            generation.fallback_reason.as_deref(),
            Some("model_protocol_error")
        );
        assert_eq!(generation.descriptor.strategy, "deterministic-extractive");
        assert!(!summary_text(&generation).contains("UNVERIFIED_STREAM_HANDOFF"));
    }
}

#[tokio::test]
async fn fallback_descriptor_matches_the_budget_actually_used() {
    let (summarizer, _) = responding_summarizer(vec![ModelEvent::Finish {
        reason: ModelFinishReason::Length,
    }]);
    let generation = summarizer
        .generate_with_char_budget(text_input("Original facts", "Current objective"), 2048)
        .await
        .unwrap();
    assert_eq!(
        generation.fallback_reason.as_deref(),
        Some("model_length_limited")
    );
    assert_eq!(
        generation.descriptor,
        DeterministicExtractiveSessionSummarizer::new(2048)
            .unwrap()
            .descriptor()
    );
}
