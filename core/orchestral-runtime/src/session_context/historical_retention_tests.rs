use super::*;
use orchestral_core::agent_session::AgentSessionEventDraft;

fn input(records: &mut Vec<AgentSessionRecord>, run: &str, size: usize) {
    let sequence = records.len() as u64 + 1;
    records.push(
        AgentSessionRecord::seal(
            AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!("input-{sequence}")),
                session_id: AgentSessionId::new("retention-test"),
                run_id: RunId::new(run),
                payload: AgentSessionEvent::RunInputCommitted {
                    message: ModelMessage::text(ModelRole::User, "x".repeat(size)),
                },
            },
            sequence,
        )
        .unwrap(),
    );
}

fn policy() -> SessionCompactionPolicy {
    SessionCompactionPolicy {
        minimum_source_records: 2,
        keep_recent_records: 2,
    }
}

#[test]
fn historical_tail_is_independent_of_current_run_growth() {
    let mut records = Vec::new();
    for _ in 0..6 {
        input(&mut records, "previous", 10);
    }
    let run = RunId::new("current");
    let expected = Some(SessionSourceRange {
        first_session_seq: 1,
        last_session_seq: 4,
    });
    assert_eq!(
        select_compaction_source(&records, &run, &policy()),
        expected
    );
    for _ in 0..40 {
        input(&mut records, "current", 10);
        assert_eq!(
            select_compaction_source(&records, &run, &policy()),
            expected
        );
    }
}

#[test]
fn large_recent_results_do_not_consume_the_retained_tail() {
    let mut records = Vec::new();
    input(&mut records, "previous", 10);
    input(&mut records, "previous", 10);
    input(&mut records, "previous", 80 * 1024);
    input(&mut records, "previous", 80 * 1024);
    assert_eq!(
        select_compaction_source(&records, &RunId::new("current"), &policy()),
        Some(SessionSourceRange {
            first_session_seq: 1,
            last_session_seq: 2,
        })
    );
    let mut tail_only = Vec::new();
    input(&mut tail_only, "previous", 80 * 1024);
    input(&mut tail_only, "previous", 80 * 1024);
    assert!(select_compaction_source(&tail_only, &RunId::new("current"), &policy()).is_none());
}

#[test]
fn shadowed_records_and_late_summaries_do_not_replace_recent_raw_history() {
    let mut records = Vec::new();
    for _ in 0..8 {
        input(&mut records, "previous", 10);
    }
    let source = SessionSourceRange {
        first_session_seq: 6,
        last_session_seq: 8,
    };
    let digest = session_range_digest(&records, &source).unwrap();
    records.push(
        AgentSessionRecord::seal(
            AgentSessionEventDraft {
                event_id: AgentSessionEventId::new("late-summary"),
                session_id: AgentSessionId::new("retention-test"),
                run_id: RunId::new("previous"),
                payload: AgentSessionEvent::CompactionCommitted {
                    source,
                    source_digest: digest,
                    policy_digest: policy().digest().unwrap(),
                    summary_config_digest: Digest::sha256("test-summary"),
                    summary: ModelMessage::text(ModelRole::System, "previous observations"),
                    strategy: "test".to_owned(),
                    model: None,
                    version: "1".to_owned(),
                },
            },
            9,
        )
        .unwrap(),
    );
    input(&mut records, "current", 10);
    assert_eq!(
        select_compaction_source(&records, &RunId::new("current"), &policy()),
        Some(SessionSourceRange {
            first_session_seq: 1,
            last_session_seq: 3,
        })
    );
}

#[test]
fn volume_can_compact_an_older_record_without_relaxing_recent_count() {
    let mut records = Vec::new();
    input(&mut records, "previous", 80 * 1024);
    input(&mut records, "previous", 10);
    input(&mut records, "previous", 10);
    assert_eq!(
        select_compaction_source(&records, &RunId::new("current"), &policy()),
        Some(single_range(1))
    );
}
