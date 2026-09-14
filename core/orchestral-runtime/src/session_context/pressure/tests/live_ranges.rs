use super::*;

async fn separated_summaries() -> (Fixture, AgentSessionCompactor, Vec<u64>) {
    let fixture = Fixture::new(&[], "Inspect the recorded sources and continue.").await;
    let compactor = fixture.compactor(Arc::new(
        DeterministicExtractiveSessionSummarizer::new(256).unwrap(),
    ));
    let mut producers = Vec::new();
    for index in 1..=3 {
        append_tool_exchange(&fixture.store, index, "current", 8_000).await;
        let records = fixture.records().await;
        let source = single_range(records.last().unwrap().session_seq);
        let summary = compactor
            .summarizer
            .summarize(SessionCompactionInput {
                session_id: AgentSessionId::new("session-1"),
                groups: original_compaction_groups(&records, &source).unwrap(),
                source: source.clone(),
                focus_messages: Vec::new(),
            })
            .await
            .unwrap();
        let record = compactor
            .commit_active_run_summary(&records, &RunId::new("current"), source, summary)
            .await
            .unwrap()
            .unwrap();
        producers.push(record.session_seq);
    }
    (fixture, compactor, producers)
}

#[tokio::test]
async fn pressure_merges_live_summaries_across_shadowed_records_and_replays_originals_once() {
    let (fixture, compactor, producers) = separated_summaries().await;
    let records_before = fixture.records().await;
    let projection_before = fixture
        .engine
        .project(fixture.request(1_000_000))
        .await
        .unwrap();
    let before = fixture.input_use().await;
    let budget = before - 1;
    let merged = compactor
        .compact_active_run_for_context(
            &fixture.engine,
            fixture.request(budget),
            ContextTokenPolicy::UpperBound,
        )
        .await
        .unwrap()
        .expect("separate minimum summaries must remain mergeable");
    let AgentSessionEvent::ActiveRunCompactionCommitted { source, .. } = &merged.payload else {
        panic!("expected active compaction");
    };
    assert!(producers.iter().all(|seq| source.contains(*seq)));
    let records = fixture.records().await;
    assert_eq!(&records[..records_before.len()], records_before.as_slice());
    let originals = original_compaction_groups(&records, source).unwrap();
    let expected = records_before
        .iter()
        .filter(|record| {
            matches!(
                record.payload,
                AgentSessionEvent::ToolExchangeCommitted { .. }
            )
        })
        .map(|record| single_range(record.session_seq))
        .collect::<Vec<_>>();
    assert_eq!(
        originals
            .iter()
            .map(|group| group.source.clone())
            .collect::<Vec<_>>(),
        expected
    );
    let projection = fixture
        .engine
        .project(fixture.request(budget))
        .await
        .unwrap();
    assert!(projection.used_input_tokens < before);

    // Reconstruct every append from durable wire records, including the holes
    // occupied by Tool exchanges that an earlier summary already shadows.
    let wire = serde_json::to_vec(&records).unwrap();
    let records: Vec<AgentSessionRecord> = serde_json::from_slice(&wire).unwrap();
    let fresh = Arc::new(InMemoryAgentSessionJournalStore::default());
    for record in records {
        let appended = fresh
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
    let restarted = AgentSessionContextEngine::new(fresh, Arc::new(JsonSizeTokenMeter::default()));
    let replayed = restarted.project(fixture.request(budget)).await.unwrap();
    assert_eq!(replayed.messages, projection.messages);
    assert_eq!(replayed.included_ranges, projection.included_ranges);
    assert_eq!(replayed.used_input_tokens, projection.used_input_tokens);
    let historical = restarted
        .project(SessionContextRequest {
            through_session_seq: Some(records_before.len() as u64),
            ..fixture.request(1_000_000)
        })
        .await
        .unwrap();
    assert_eq!(historical.messages, projection_before.messages);
}

#[tokio::test]
async fn explicit_pressure_compaction_can_fold_summary_only_live_ranges() {
    let (fixture, compactor, producers) = separated_summaries().await;
    let record = compactor
        .compact_active_run_for_pressure(&AgentSessionId::new("session-1"), &RunId::new("current"))
        .await
        .unwrap()
        .expect("live summaries still represent complete Tool exchanges");
    let AgentSessionEvent::ActiveRunCompactionCommitted { source, .. } = record.payload else {
        panic!("expected active compaction");
    };
    assert_eq!(source.first_session_seq, producers[0]);
    assert_eq!(source.last_session_seq, *producers.last().unwrap());
    fixture
        .engine
        .project(fixture.request(1_000_000))
        .await
        .unwrap();
}

#[tokio::test]
async fn merging_shadowed_ranges_preserves_the_position_of_a_later_user_correction() {
    let (fixture, compactor, _) = separated_summaries().await;
    let correction = "Correction: preserve the existing public mode.";
    append_input(&fixture.store, 2, "current", correction.to_owned()).await;
    let barrier = fixture.records().await.last().unwrap().session_seq;
    for index in 4..=5 {
        append_tool_exchange(&fixture.store, index, "current", 8_000).await;
        let records = fixture.records().await;
        let source = single_range(records.last().unwrap().session_seq);
        let summary = compactor
            .summarizer
            .summarize(SessionCompactionInput {
                session_id: AgentSessionId::new("session-1"),
                groups: original_compaction_groups(&records, &source).unwrap(),
                source: source.clone(),
                focus_messages: Vec::new(),
            })
            .await
            .unwrap();
        compactor
            .commit_active_run_summary(&records, &RunId::new("current"), source, summary)
            .await
            .unwrap()
            .unwrap();
    }
    let records_before = fixture.records().await;
    let mut earlier = None;
    let mut later = None;
    for _ in 0..2 {
        let before = fixture.input_use().await;
        let record = compactor
            .compact_active_run_for_context(
                &fixture.engine,
                fixture.request(before - 1),
                ContextTokenPolicy::UpperBound,
            )
            .await
            .unwrap()
            .expect("each side can compact independently");
        let AgentSessionEvent::ActiveRunCompactionCommitted {
            source, summary, ..
        } = record.payload
        else {
            panic!("expected active compaction");
        };
        assert!(!source.contains(barrier));
        if source.last_session_seq < barrier {
            earlier = Some(summary);
        } else {
            later = Some(summary);
        }
    }
    let records = fixture.records().await;
    assert_eq!(&records[..records_before.len()], records_before.as_slice());
    let projection = fixture
        .engine
        .project(fixture.request(1_000_000))
        .await
        .unwrap();
    let user = ModelMessage::text(ModelRole::User, correction);
    let at = projection
        .messages
        .iter()
        .position(|message| *message == user)
        .unwrap();
    assert_eq!(projection.messages[at - 1], earlier.unwrap());
    assert_eq!(projection.messages[at + 1], later.unwrap());
    assert_eq!(
        projection
            .messages
            .iter()
            .filter(|message| **message == user)
            .count(),
        1
    );
}

#[tokio::test]
async fn shadowed_endpoints_and_outside_live_producers_cannot_resurrect_old_sources() {
    for role in [ModelRole::Assistant, ModelRole::System] {
        let (fixture, compactor, producers) = separated_summaries().await;
        let records = fixture.records().await;
        let invalid = SessionSourceRange {
            first_session_seq: producers[0] - 1,
            last_session_seq: *producers.last().unwrap(),
        };
        assert!(compactor
            .commit_active_run_summary(
                &records,
                &RunId::new("current"),
                invalid,
                ModelMessage::text(role, "Invalid source"),
            )
            .await
            .is_err());
        assert_eq!(fixture.records().await, records);

        // Move the middle summary to a later producer. A range spanning its
        // old record must not absorb those originals while the new producer
        // survives outside that range, regardless of the summary's role.
        compactor
            .commit_active_run_summary(
                &records,
                &RunId::new("current"),
                single_range(producers[1]),
                ModelMessage::text(ModelRole::Assistant, "Middle observations"),
            )
            .await
            .unwrap()
            .unwrap();
        let records = fixture.records().await;
        let invalid = SessionSourceRange {
            first_session_seq: producers[0],
            last_session_seq: *producers.last().unwrap(),
        };
        assert!(compactor
            .commit_active_run_summary(
                &records,
                &RunId::new("current"),
                invalid.clone(),
                ModelMessage::text(role, "Invalid source"),
            )
            .await
            .unwrap()
            .is_none());
        assert_eq!(fixture.records().await, records);

        // A store can contain a syntactically valid but semantically corrupt
        // event. Restart must reject the same invalid source before projection.
        fixture
            .store
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new("invalid-merge"),
                session_id: AgentSessionId::new("session-1"),
                run_id: RunId::new("current"),
                payload: AgentSessionEvent::ActiveRunCompactionCommitted {
                    source_digest: session_range_digest(&records, &invalid).unwrap(),
                    source: invalid,
                    policy_digest: compactor.policy.digest().unwrap(),
                    summary_config_digest: compactor.summarizer_descriptor.config_digest.clone(),
                    summary: ModelMessage::text(role, "Invalid source"),
                    strategy: compactor.summarizer_descriptor.strategy.clone(),
                    model: None,
                    version: compactor.summarizer_descriptor.version.clone(),
                },
            })
            .await
            .unwrap();
        assert!(matches!(
            fixture.engine.project(fixture.request(1_000_000)).await,
            Err(SessionContextError::Journal(AgentSessionError::Corrupt(_)))
        ));
    }
}
