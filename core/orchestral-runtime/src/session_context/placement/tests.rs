use super::super::tests::{append_input, append_tool_exchange};
use super::*;
use orchestral_core::agent_protocol::wire::{ArtifactRef, ArtifactRefWithDigest};
use orchestral_core::agent_session::InMemoryAgentSessionJournalStore;
use orchestral_core::model_protocol::{ModelRequestId, ModelToolCallId};
use orchestral_core::skill_protocol::{SkillLoad, SkillPackage, SkillSource, SkillSourceKind};
use orchestral_core::tool_protocol::ToolCallId;
use serde_json::json;

struct Fixture {
    store: Arc<InMemoryAgentSessionJournalStore>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            store: Arc::new(InMemoryAgentSessionJournalStore::default()),
        }
    }

    fn engine(&self) -> AgentSessionContextEngine {
        AgentSessionContextEngine::new(self.store.clone(), Arc::new(JsonSizeTokenMeter::default()))
    }

    fn request(&self, budget: u64) -> SessionContextRequest {
        SessionContextRequest {
            session_id: AgentSessionId::new("session-1"),
            current_run_id: RunId::new("current"),
            through_session_seq: None,
            system_message: Some(ModelMessage::text(ModelRole::System, "Host policy")),
            tools: Vec::new(),
            history_limit: 100,
            max_context_tokens: budget + 64,
            reserved_output_tokens: 64,
            config_digest: Digest::sha256("placement-fixture"),
            allowed_skill_digests: BTreeMap::new(),
        }
    }

    fn compactor(&self) -> AgentSessionCompactor {
        AgentSessionCompactor::new(
            self.store.clone(),
            Arc::new(DeterministicExtractiveSessionSummarizer::new(1024).unwrap()),
            SessionCompactionPolicy {
                minimum_source_records: 2,
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

    async fn append(&self, payload: AgentSessionEvent) -> AgentSessionRecord {
        self.store
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!(
                    "placement-{}",
                    self.records().await.len()
                )),
                session_id: AgentSessionId::new("session-1"),
                run_id: RunId::new("current"),
                payload,
            })
            .await
            .unwrap()
            .record
    }

    async fn append_summary(
        &self,
        source: SessionSourceRange,
        role: ModelRole,
    ) -> AgentSessionRecord {
        self.append(AgentSessionEvent::ActiveRunCompactionCommitted {
            source_digest: session_range_digest(&self.records().await, &source).unwrap(),
            source,
            policy_digest: self.compactor().policy().digest().unwrap(),
            summary_config_digest: Digest::sha256("saved-summary"),
            summary: ModelMessage::text(role, "Earlier observations"),
            strategy: "saved-fixture".to_owned(),
            model: None,
            version: "1".to_owned(),
        })
        .await
    }

    async fn restart(&self) -> Self {
        // Rebuild the store and engine from serialized canonical records, not
        // an in-memory MessageGroup cache or the rendered summary text.
        let bytes = serde_json::to_vec(&self.records().await).unwrap();
        let records: Vec<AgentSessionRecord> = serde_json::from_slice(&bytes).unwrap();
        let fresh = Self::new();
        for record in records {
            let appended = fresh
                .store
                .append(AgentSessionEventDraft {
                    event_id: record.event_id.clone(),
                    session_id: record.session_id.clone(),
                    run_id: record.run_id.clone(),
                    payload: record.payload.clone(),
                })
                .await
                .unwrap();
            assert_eq!(appended.record, record);
        }
        fresh
    }
}

fn saved_summary(record: &AgentSessionRecord) -> ModelMessage {
    match &record.payload {
        AgentSessionEvent::CompactionCommitted { summary, .. }
        | AgentSessionEvent::ActiveRunCompactionCommitted { summary, .. } => summary.clone(),
        _ => panic!("expected a durable summary"),
    }
}

#[tokio::test]
async fn recompaction_across_a_shadowed_producer_keeps_checkpoint_ranges_disjoint() {
    let fixture = Fixture::new();
    append_input(&fixture.store, 1, "current", "Inspect the state".into()).await;
    for i in 1..=4 {
        append_tool_exchange(&fixture.store, i, "current", 64).await;
    }
    let first = fixture
        .append_summary(
            SessionSourceRange {
                first_session_seq: 2,
                last_session_seq: 3,
            },
            ModelRole::Assistant,
        )
        .await;
    append_tool_exchange(&fixture.store, 5, "current", 64).await;
    let nested = fixture
        .append_summary(single_range(first.session_seq), ModelRole::Assistant)
        .await;
    let spanning = fixture
        .append_summary(
            SessionSourceRange {
                first_session_seq: 4,
                last_session_seq: 7,
            },
            ModelRole::Assistant,
        )
        .await;

    for fixture in [fixture.restart().await, fixture] {
        let projected = fixture
            .engine()
            .project(fixture.request(50_000))
            .await
            .unwrap();
        assert!(projected.messages.contains(&saved_summary(&nested)));
        assert!(projected.messages.contains(&saved_summary(&spanning)));
        let trace = crate::generic_agent_checkpoint::GenericModelContextTrace {
            through_session_seq: projected.through_session_seq,
            included_ranges: projected.included_ranges.clone(),
            deferred_ranges: projected.deferred_ranges.clone(),
            config_digest: projected.config_digest.clone(),
            history_limit: 100,
            used_input_tokens: projected.used_input_tokens,
            context_estimate: projected.context_estimate.clone(),
            planning: projected.planning.clone(),
            input_budget_tokens: projected.input_budget_tokens,
        };
        trace
            .validate()
            .expect("valid nested summaries must produce a valid durable model trace");
        assert!(trace
            .included_ranges
            .contains(&single_range(first.session_seq)));
        assert!(trace.included_ranges.contains(&SessionSourceRange {
            first_session_seq: 4,
            last_session_seq: 5
        }));
        assert!(trace.included_ranges.contains(&single_range(7)));
        let earlier = fixture
            .engine()
            .project(SessionContextRequest {
                through_session_seq: Some(nested.session_seq),
                ..fixture.request(50_000)
            })
            .await
            .unwrap();
        assert!(earlier.included_ranges.contains(&single_range(4)));
    }
}

#[tokio::test]
async fn legacy_system_summary_keeps_its_projection_and_historical_cursor() {
    let fixture = Fixture::new();
    append_input(&fixture.store, 1, "current", "Inspect the state".into()).await;
    for i in 1..=3 {
        append_tool_exchange(&fixture.store, i, "current", 64).await;
    }
    let before = fixture
        .engine()
        .project(fixture.request(50_000))
        .await
        .unwrap();
    let original_records = fixture.records().await;
    let record = fixture
        .append_summary(
            SessionSourceRange {
                first_session_seq: 2,
                last_session_seq: 3,
            },
            ModelRole::System,
        )
        .await;
    let projected = fixture
        .engine()
        .project(fixture.request(50_000))
        .await
        .unwrap();
    let mut expected = vec![
        before.messages[0].clone(),
        saved_summary(&record),
        before.messages[1].clone(),
    ];
    expected.extend_from_slice(&before.messages[6..]);
    assert_eq!(projected.messages, expected);
    let restarted = fixture.restart().await;
    let replay = restarted
        .engine()
        .project(restarted.request(50_000))
        .await
        .unwrap();
    assert_eq!(replay.messages, projected.messages);
    let old_cursor = restarted
        .engine()
        .project(SessionContextRequest {
            through_session_seq: Some(original_records.len() as u64),
            ..restarted.request(50_000)
        })
        .await
        .unwrap();
    assert_eq!(old_cursor.messages, before.messages);
    assert_eq!(
        &restarted.records().await[..original_records.len()],
        original_records.as_slice()
    );
}

#[tokio::test]
async fn assistant_summary_and_repeated_compaction_keep_the_original_history_position() {
    let fixture = Fixture::new();
    append_input(&fixture.store, 1, "current", "Inspect the state".into()).await;
    for i in 1..=3 {
        append_tool_exchange(&fixture.store, i, "current", 64).await;
    }
    let before = fixture
        .engine()
        .project(fixture.request(50_000))
        .await
        .unwrap();
    let first = fixture
        .compactor()
        .compact_active_run_for_pressure(&AgentSessionId::new("session-1"), &RunId::new("current"))
        .await
        .unwrap()
        .unwrap();
    let projected = fixture
        .engine()
        .project(fixture.request(50_000))
        .await
        .unwrap();
    let mut expected = before.messages[..2].to_vec();
    assert_eq!(saved_summary(&first).role, ModelRole::Assistant);
    expected.push(saved_summary(&first));
    expected.extend_from_slice(&before.messages[6..]);
    assert_eq!(projected.messages, expected);

    for i in 4..=5 {
        append_tool_exchange(&fixture.store, i, "current", 64).await;
    }
    let second = fixture
        .compactor()
        .compact_active_run_for_pressure(&AgentSessionId::new("session-1"), &RunId::new("current"))
        .await
        .unwrap()
        .unwrap();
    let records = fixture.records().await;
    let groups = replay_groups(&records, &RunId::new("current"), &BTreeMap::new()).unwrap();
    assert_eq!(
        groups[&second.session_seq].logical_source,
        SessionSourceRange {
            first_session_seq: 2,
            last_session_seq: 6,
        }
    );
    let projected = fixture
        .engine()
        .project(fixture.request(50_000))
        .await
        .unwrap();
    assert_eq!(&projected.messages[..2], &before.messages[..2]);
    assert_eq!(projected.messages[2], saved_summary(&second));
    let restarted = fixture.restart().await;
    assert_eq!(
        restarted
            .engine()
            .project(restarted.request(50_000))
            .await
            .unwrap()
            .messages,
        projected.messages
    );
}

fn barriers() -> Vec<AgentSessionEvent> {
    let call = ModelToolCallId::new("retained-call");
    let artifact = ArtifactRefWithDigest {
        artifact_ref: ArtifactRef::new("retained-artifact"),
        digest: Digest::sha256("retained-content"),
    };
    vec![
        AgentSessionEvent::RunInputCommitted {
            message: ModelMessage::text(ModelRole::User, "Correction: preserve the existing mode"),
        },
        AgentSessionEvent::EffectUncertaintyCommitted {
            effect_call_id: ToolCallId::new("uncertain-effect"),
            model_call_id: ModelToolCallId::new("uncertain-call"),
            tool_name: "update".into(),
            message: "The operation may already have happened".into(),
        },
        AgentSessionEvent::ToolExchangeCommitted {
            request_id: ModelRequestId::new("retained-request"),
            assistant: ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::ToolCall {
                    call_id: call.clone(),
                    name: "inspect".into(),
                    arguments: json!({}),
                    extensions: Default::default(),
                }],
            },
            tool: ModelMessage {
                role: ModelRole::Tool,
                content: vec![ModelContent::ToolResult {
                    call_id: call,
                    result: json!({"artifact": artifact.clone()}),
                    is_error: false,
                }],
            },
            retained_artifacts: vec![artifact],
            usage: None,
        },
        AgentSessionEvent::SkillLoaded {
            load: Box::new(SkillLoad {
                package: SkillPackage::seal(
                    SkillId::new("inspection"),
                    "inspection",
                    "Inspection policy",
                    None,
                    SkillSource {
                        kind: SkillSourceKind::BuiltIn,
                        locator: "built-in:inspection".into(),
                    },
                    Default::default(),
                    Default::default(),
                    "Retain the caller's public interface",
                )
                .unwrap(),
            }),
        },
    ]
}

#[tokio::test]
async fn late_summary_producers_cannot_move_observations_across_surviving_facts() {
    for barrier in barriers() {
        let fixture = Fixture::new();
        append_input(
            &fixture.store,
            1,
            "current",
            "Continue the inspection".into(),
        )
        .await;
        append_tool_exchange(&fixture.store, 1, "current", 7000).await;
        let protected = fixture.append(barrier).await;
        append_tool_exchange(&fixture.store, 2, "current", 7000).await;
        fixture
            .append_summary(single_range(2), ModelRole::Assistant)
            .await;
        let records = fixture.records().await;
        let preferred = select_active_run_compaction_source(
            &records,
            &RunId::new("current"),
            fixture.compactor().policy(),
        )
        .unwrap();
        assert_eq!(
            preferred,
            SessionSourceRange {
                first_session_seq: 4,
                last_session_seq: 5
            }
        );

        // A forged durable merge is invalid on replay, including when a Skill
        // is deliberately absent from the allowed instruction projection.
        let forged = fixture.restart().await;
        forged
            .append_summary(preferred.clone(), ModelRole::Assistant)
            .await;
        assert!(matches!(
            forged.engine().project(forged.request(50_000)).await,
            Err(SessionContextError::Journal(AgentSessionError::Corrupt(_)))
        ));

        // Preserve the older System contract even for a producer merge that
        // crossed this barrier. Converting that combined summary to Assistant
        // cannot silently relocate its transitive original observations.
        let legacy = fixture.restart().await;
        let old_merge = legacy.append_summary(preferred, ModelRole::System).await;
        legacy
            .engine()
            .project(legacy.request(50_000))
            .await
            .unwrap();
        legacy
            .append_summary(single_range(old_merge.session_seq), ModelRole::Assistant)
            .await;
        assert!(matches!(
            legacy.engine().project(legacy.request(50_000)).await,
            Err(SessionContextError::Journal(AgentSessionError::Corrupt(_)))
        ));

        let engine = fixture.engine();
        let before = engine.project(fixture.request(50_000)).await.unwrap();
        let reduced = fixture
            .compactor()
            .compact_active_run_for_context(
                &engine,
                fixture.request(before.used_input_tokens - 1),
                ContextTokenPolicy::UpperBound,
            )
            .await
            .unwrap()
            .expect("a safe independent segment still reduces context");
        let AgentSessionEvent::ActiveRunCompactionCommitted { source, .. } = &reduced.payload
        else {
            panic!("expected active compaction");
        };
        assert_eq!(*source, single_range(4));
        let after_records = fixture.records().await;
        assert_eq!(after_records[2], protected);
        let groups =
            replay_groups(&after_records, &RunId::new("current"), &BTreeMap::new()).unwrap();
        assert_eq!(groups[&5].logical_source, single_range(2));
        assert_eq!(groups[&reduced.session_seq].logical_source, single_range(4));
        let after = engine
            .project(fixture.request(before.used_input_tokens - 1))
            .await
            .unwrap();
        assert!(after.used_input_tokens < before.used_input_tokens);
        if let AgentSessionEvent::RunInputCommitted { message } = &protected.payload {
            assert_eq!(after.messages[2], saved_summary(&records[4]));
            assert_eq!(after.messages[3], *message);
            assert_eq!(after.messages[4], saved_summary(&reduced));
        }
    }
}

#[tokio::test]
async fn historical_summary_keeps_cross_run_user_anchor_and_disjoint_provenance() {
    let fixture = Fixture::new();
    append_input(&fixture.store, 1, "old", "Preserve public names".into()).await;
    append_input(
        &fixture.store,
        2,
        "old",
        "Correction: preserve the output format too".into(),
    )
    .await;
    append_tool_exchange(&fixture.store, 1, "old", 64).await;
    append_input(&fixture.store, 3, "current", "Continue verification".into()).await;
    let record = fixture
        .compactor()
        .compact_if_needed(&AgentSessionId::new("session-1"), &RunId::new("current"))
        .await
        .unwrap()
        .unwrap();
    let request = SessionContextRequest {
        history_limit: 2,
        ..fixture.request(50_000)
    };
    let projection = fixture.engine().project(request).await.unwrap();
    assert_eq!(projection.messages[1], saved_summary(&record));
    assert_eq!(
        projection.messages[2],
        ModelMessage::text(
            ModelRole::User,
            "Correction: preserve the output format too"
        )
    );
    assert_eq!(
        projection.messages[3],
        ModelMessage::text(ModelRole::User, "Continue verification")
    );
    for seq in 1..=4 {
        assert_eq!(
            projection
                .included_ranges
                .iter()
                .chain(&projection.deferred_ranges)
                .filter(|range| range.contains(seq))
                .count(),
            1
        );
    }
    let restarted = fixture.restart().await;
    let replay = restarted
        .engine()
        .project(SessionContextRequest {
            history_limit: 2,
            through_session_seq: Some(record.session_seq),
            ..restarted.request(50_000)
        })
        .await
        .unwrap();
    assert_eq!(replay.messages, projection.messages);
    assert_eq!(replay.included_ranges, projection.included_ranges);
    assert_eq!(replay.deferred_ranges, projection.deferred_ranges);
}
