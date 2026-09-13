use super::super::tests::{append_input, append_tool_exchange};
use super::*;
use orchestral_core::agent_session::InMemoryAgentSessionJournalStore;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture {
    store: Arc<InMemoryAgentSessionJournalStore>,
    engine: AgentSessionContextEngine,
}

impl Fixture {
    async fn new(widths: &[usize], task: &str) -> Self {
        let store = Arc::new(InMemoryAgentSessionJournalStore::default());
        append_input(&store, 1, "current", task.to_owned()).await;
        for (index, width) in widths.iter().copied().enumerate() {
            append_tool_exchange(&store, index as u64 + 1, "current", width).await;
        }
        let engine =
            AgentSessionContextEngine::new(store.clone(), Arc::new(JsonSizeTokenMeter::default()));
        Self { store, engine }
    }

    fn request(&self, input_budget: u64) -> SessionContextRequest {
        SessionContextRequest {
            session_id: AgentSessionId::new("session-1"),
            current_run_id: RunId::new("current"),
            through_session_seq: None,
            system_message: Some(ModelMessage::text(
                ModelRole::System,
                "Preserve Host authority.",
            )),
            tools: vec![ModelToolDefinition {
                name: "file_read".to_owned(),
                description: "Inspect a permitted source.".to_owned(),
                input_schema: serde_json::json!({"type": "object"}),
            }],
            history_limit: 100,
            max_context_tokens: input_budget + 64,
            reserved_output_tokens: 64,
            config_digest: Digest::sha256("pressure-fixture"),
            allowed_skill_digests: BTreeMap::new(),
        }
    }

    fn compactor(&self, summarizer: Arc<dyn AgentSessionSummarizer>) -> AgentSessionCompactor {
        AgentSessionCompactor::new(
            self.store.clone(),
            summarizer,
            SessionCompactionPolicy {
                minimum_source_records: 32,
                keep_recent_records: 1,
            },
        )
        .unwrap()
    }

    async fn records(&self) -> Vec<AgentSessionRecord> {
        self.store
            .load_session(&AgentSessionId::new("session-1"))
            .await
            .unwrap()
    }

    async fn input_use(&self) -> u64 {
        self.engine
            .project(self.request(1_000_000))
            .await
            .unwrap()
            .used_input_tokens
    }
}

fn default_summarizer() -> Arc<dyn AgentSessionSummarizer> {
    Arc::new(DeterministicExtractiveSessionSummarizer::new(16_384).unwrap())
}

#[tokio::test]
async fn default_extractive_expansion_is_tightened_before_commit() {
    // The unrestricted extractive renderer adds observation and transcript
    // framing to these small completed exchanges. Exercise that real renderer
    // rather than a fixed short summary that cannot expose expansion.
    let old = Fixture::new(&[96, 96, 96], "Inspect the sources, then continue.").await;
    let before = old.input_use().await;
    old.compactor(default_summarizer())
        .compact_active_run_for_pressure(&AgentSessionId::new("session-1"), &RunId::new("current"))
        .await
        .unwrap()
        .unwrap();
    assert!(old.input_use().await > before);

    let fixture = Fixture::new(&[96, 96, 96], "Inspect the sources, then continue.").await;
    let original_records = fixture.records().await;
    let original_projection = fixture
        .engine
        .project(fixture.request(1_000_000))
        .await
        .unwrap();
    let budget = before - 1;
    fixture
        .compactor(default_summarizer())
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(budget),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap()
        .expect("a bounded version of the extractive summary should fit");
    let projection = fixture
        .engine
        .project(fixture.request(budget))
        .await
        .unwrap();
    assert!(projection.used_input_tokens < before);
    assert!(projection.used_input_tokens <= budget);
    let records = fixture.records().await;
    assert_eq!(
        &records[..original_records.len()],
        original_records.as_slice()
    );
    assert_eq!(records.len(), original_records.len() + 1);
    let replay = fixture
        .engine
        .project(SessionContextRequest {
            through_session_seq: Some(original_records.len() as u64),
            ..fixture.request(1_000_000)
        })
        .await
        .unwrap();
    assert_eq!(replay.messages, original_projection.messages);
}

#[tokio::test]
async fn summary_only_context_can_be_compacted_again_without_new_exchange() {
    let fixture = Fixture::new(&[12_000], "Continue using the recorded inspection.").await;
    let originals = fixture.records().await;
    let compactor = fixture.compactor(default_summarizer());
    let first_budget = fixture.input_use().await / 2;
    let first = compactor
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(first_budget),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap()
        .unwrap();
    let first_use = fixture.input_use().await;
    let second_budget = first_use * 3 / 4;
    let second = compactor
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(second_budget),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap()
        .expect("the live summary remains a compactable producer");
    let AgentSessionEvent::ActiveRunCompactionCommitted { source, .. } = &second.payload else {
        panic!("expected pressure compaction");
    };
    assert_eq!(*source, single_range(first.session_seq));
    let records = fixture.records().await;
    assert_eq!(&records[..originals.len()], originals.as_slice());
    let expanded = original_compaction_groups(&records, source).unwrap();
    assert_eq!(expanded.len(), 1);
    assert_eq!(expanded[0].source, single_range(2));
    assert!(expanded[0]
        .messages
        .iter()
        .all(|message| message.role != ModelRole::System));
    let projection = fixture
        .engine
        .project(fixture.request(second_budget))
        .await
        .unwrap();
    assert!(projection.used_input_tokens < first_use);
    assert!(projection.used_input_tokens <= second_budget);
}

#[tokio::test]
async fn oversized_latest_exchange_is_compacted_as_a_complete_group() {
    let fixture = Fixture::new(&[32, 16_000], "Inspect the evidence and finish.").await;
    let originals = fixture.records().await;
    let budget = fixture.input_use().await / 3;
    let record = fixture
        .compactor(default_summarizer())
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(budget),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap()
        .expect("retaining the newest exchange is optional under pressure");
    let AgentSessionEvent::ActiveRunCompactionCommitted { source, .. } = &record.payload else {
        panic!("expected pressure compaction");
    };
    assert_eq!(source.first_session_seq, 2);
    assert_eq!(source.last_session_seq, 3);
    let records = fixture.records().await;
    assert_eq!(&records[..originals.len()], originals.as_slice());
    let expanded = original_compaction_groups(&records, source).unwrap();
    assert_eq!(expanded.len(), 2);
    for group in expanded {
        assert!(group
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|content| matches!(content, ModelContent::ToolCall { .. })));
        assert!(group
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|content| matches!(content, ModelContent::ToolResult { .. })));
    }
    let projection = fixture
        .engine
        .project(fixture.request(budget))
        .await
        .unwrap();
    assert!(projection.used_input_tokens <= budget);
    assert!(projection
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .all(|content| !matches!(
            content,
            ModelContent::ToolCall { .. } | ModelContent::ToolResult { .. }
        )));
}

struct ExpandingSummarizer {
    calls: AtomicUsize,
}

#[async_trait]
impl AgentSessionSummarizer for ExpandingSummarizer {
    fn descriptor(&self) -> SessionSummarizerDescriptor {
        SessionSummarizerDescriptor {
            strategy: "test-expanding-summary".to_owned(),
            model: None,
            version: "1".to_owned(),
            config_digest: Digest::sha256("test-expanding-summary/v1"),
        }
    }

    // Deliberately use the trait's default budget method: an external
    // summarizer may ignore the hint, but cannot bypass full-input metering.
    async fn summarize(
        &self,
        _input: SessionCompactionInput,
    ) -> Result<ModelMessage, SessionContextError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ModelMessage::text(
            ModelRole::System,
            "expanded ".repeat(8_000),
        ))
    }
}

#[tokio::test]
async fn immutable_context_overflow_does_not_summarize_or_append() {
    let task = "Keep this caller instruction intact. ".repeat(300);
    let fixture = Fixture::new(&[96], &task).await;
    let originals = fixture.records().await;
    let summarizer = Arc::new(ExpandingSummarizer {
        calls: AtomicUsize::new(0),
    });
    let compacted = fixture
        .compactor(summarizer.clone())
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(1_000),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap();
    assert!(compacted.is_none());
    assert_eq!(summarizer.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.records().await, originals);
    assert!(matches!(
        fixture.engine.project(fixture.request(1_000)).await,
        Err(SessionContextError::ContextOverflow { .. })
    ));
}

#[tokio::test]
async fn summarizer_ignoring_budget_cannot_commit_an_expanding_candidate() {
    let fixture = Fixture::new(&[300, 300], "Continue after inspection.").await;
    let originals = fixture.records().await;
    let before = fixture.input_use().await;
    let summarizer = Arc::new(ExpandingSummarizer {
        calls: AtomicUsize::new(0),
    });
    let compacted = fixture
        .compactor(summarizer.clone())
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(before - 1),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap();
    assert!(compacted.is_none());
    assert!(summarizer.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(fixture.records().await, originals);
    assert_eq!(fixture.input_use().await, before);
}

struct PlanningMeter;

impl ModelTokenMeter for PlanningMeter {
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        ModelTokenMeterDescriptor {
            strategy: "test-json-planning-and-bound".to_owned(),
            version: "1".to_owned(),
            accounting: ModelTokenAccounting::ConservativeUpperBound,
            config_digest: Digest::sha256("test-json-planning-and-bound/v1"),
        }
    }

    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        JsonSizeTokenMeter::default().count_request_input(messages, tools)
    }

    fn estimate_context_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<ModelContextEstimate, ModelError> {
        Ok(ModelContextEstimate {
            tokens: self.count_request_input(messages, tools)?.div_ceil(3),
            accounting: ModelTokenAccounting::Estimated,
        })
    }
}

struct BudgetFillingSummarizer;

#[async_trait]
impl AgentSessionSummarizer for BudgetFillingSummarizer {
    fn descriptor(&self) -> SessionSummarizerDescriptor {
        SessionSummarizerDescriptor {
            strategy: "test-budget-sized-summary".to_owned(),
            model: None,
            version: "1".to_owned(),
            config_digest: Digest::sha256("test-budget-sized-summary/v1"),
        }
    }

    async fn summarize(
        &self,
        _input: SessionCompactionInput,
    ) -> Result<ModelMessage, SessionContextError> {
        Ok(ModelMessage::text(
            ModelRole::System,
            "Recorded observation.",
        ))
    }

    async fn summarize_with_char_budget(
        &self,
        _input: SessionCompactionInput,
        max_chars: usize,
    ) -> Result<ModelMessage, SessionContextError> {
        Ok(ModelMessage::text(ModelRole::System, ".".repeat(max_chars)))
    }
}

#[tokio::test]
async fn pressure_uses_selected_policy_with_full_system_and_tool_overhead() {
    let fixture = Fixture::new(&[12_000], "Keep the inspection available.").await;
    let engine = AgentSessionContextEngine::new(fixture.store.clone(), Arc::new(PlanningMeter));
    let request = |budget| {
        let mut request = fixture.request(budget);
        request.system_message = Some(ModelMessage::text(
            ModelRole::System,
            "Host permission policy remains authoritative. ".repeat(12),
        ));
        request.tools[0].description = "Only inspect sources within the granted roots. ".repeat(12);
        request
    };
    let before = engine
        .project(request(1_000_000))
        .await
        .unwrap()
        .used_input_tokens;
    let budget = before / 4;
    assert!(matches!(
        engine
            .project_with_policy(request(budget), ContextTokenPolicy::Planning)
            .await,
        Err(SessionContextError::ContextOverflow { .. })
    ));
    let compactor = fixture.compactor(Arc::new(BudgetFillingSummarizer));
    compactor
        .compact_active_run_for_context(&engine, request(budget), ContextTokenPolicy::Planning)
        .await
        .unwrap()
        .unwrap();
    let planning = engine
        .project_with_policy(request(budget), ContextTokenPolicy::Planning)
        .await
        .unwrap();
    assert!(planning.context_estimate.unwrap().tokens <= budget);
    assert!(planning.used_input_tokens > budget);
    assert!(matches!(
        engine.project(request(budget)).await,
        Err(SessionContextError::ContextOverflow { .. })
    ));

    // The same immutable facts and tool schemas must now fit the strict
    // accounting policy; the earlier planning estimate cannot authorize it.
    compactor
        .compact_active_run_for_context(&engine, request(budget), ContextTokenPolicy::UpperBound)
        .await
        .unwrap()
        .unwrap();
    let strict = engine.project(request(budget)).await.unwrap();
    assert!(strict.used_input_tokens <= budget);
    assert!(strict.context_estimate.is_none());
    assert_eq!(
        strict.messages.first(),
        request(budget).system_message.as_ref()
    );
}
