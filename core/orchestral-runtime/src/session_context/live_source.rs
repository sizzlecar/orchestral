//! Ranges name durable records, while eligibility follows the live projection.
//! Superseded records inside a range must not split otherwise adjacent history.

use super::*;

pub(super) fn valid_active_source(
    groups: &BTreeMap<u64, MessageGroup>,
    records: &[AgentSessionRecord],
    source: &SessionSourceRange,
    run: &RunId,
) -> bool {
    if source.first_session_seq == 0
        || source.first_session_seq > source.last_session_seq
        || source.last_session_seq > records.len() as u64
        // Shadowed endpoints could expand the original source without a
        // corresponding live producer and resurrect discarded observations.
        || !groups.contains_key(&source.first_session_seq)
        || !groups.contains_key(&source.last_session_seq)
    {
        return false;
    }
    groups
        .range(source.first_session_seq..=source.last_session_seq)
        .all(|(_, group)| {
            group.active_compactable && records[(group.producer_seq - 1) as usize].run_id == *run
        })
        && records[(source.first_session_seq - 1) as usize..source.last_session_seq as usize]
            .iter()
            .all(|record| {
                record.run_id == *run
                    && match &record.payload {
                        AgentSessionEvent::ToolExchangeCommitted {
                            retained_artifacts, ..
                        } => retained_artifacts.is_empty(),
                        AgentSessionEvent::ActiveRunCompactionCommitted { .. } => true,
                        _ => false,
                    }
            })
}

pub(super) fn has_shadowed_records(
    groups: &BTreeMap<u64, MessageGroup>,
    source: &SessionSourceRange,
) -> bool {
    groups
        .range(source.first_session_seq..=source.last_session_seq)
        .count() as u64
        != source.last_session_seq - source.first_session_seq + 1
}

pub(super) fn compactable_segments(
    groups: &BTreeMap<u64, MessageGroup>,
    records: &[AgentSessionRecord],
    run: &RunId,
) -> Vec<SessionSourceRange> {
    let mut segments: Vec<SessionSourceRange> = Vec::new();
    for group in groups.values().filter(|group| {
        group.active_compactable && records[(group.producer_seq - 1) as usize].run_id == *run
    }) {
        if let Some(last) = segments.last_mut() {
            let joined = SessionSourceRange {
                first_session_seq: last.first_session_seq,
                last_session_seq: group.producer_seq,
            };
            if valid_active_source(groups, records, &joined, run)
                && placement::can_replace_source(groups, records, &joined)
            {
                *last = joined;
                continue;
            }
        }
        segments.push(single_range(group.producer_seq));
    }
    segments
}
