use super::*;

/// The original logical extent is independent of the record that publishes a
/// summary. Keep it derived from the journal rather than accepting a second
/// persisted source identity.
fn source_extent(
    groups: &BTreeMap<u64, MessageGroup>,
    source: &SessionSourceRange,
) -> Option<SessionSourceRange> {
    let mut covered = groups
        .values()
        .filter(|group| source.contains(group.producer_seq));
    let mut extent = covered.next()?.logical_source.clone();
    for group in covered {
        extent.first_session_seq = extent
            .first_session_seq
            .min(group.logical_source.first_session_seq);
        extent.last_session_seq = extent
            .last_session_seq
            .max(group.logical_source.last_session_seq);
    }
    Some(extent)
}

pub(super) fn can_replace_source(
    groups: &BTreeMap<u64, MessageGroup>,
    records: &[AgentSessionRecord],
    source: &SessionSourceRange,
) -> bool {
    let Some(extent) = source_extent(groups, source) else {
        return false;
    };
    // Adjacent producer records need not be adjacent history: a late summary
    // can refer to exchanges before a surviving User/Steer or protected fact.
    // Never move later observations across that fact by merging the producers.
    let crosses_group = groups.values().any(|group| {
        !source.contains(group.producer_seq)
            && group.logical_source.first_session_seq <= extent.last_session_seq
            && extent.first_session_seq <= group.logical_source.last_session_seq
    });
    // Loaded Skills may be absent from this projection because they belong to
    // another Run or its allowed digest set changed. They remain durable
    // barriers, just like retained artifacts and unresolved effects.
    let crosses_protected_record = records.iter().any(|record| {
        extent.contains(record.session_seq)
            && match &record.payload {
                AgentSessionEvent::SkillLoaded { .. }
                | AgentSessionEvent::EffectUncertaintyCommitted { .. } => true,
                AgentSessionEvent::ToolExchangeCommitted {
                    retained_artifacts, ..
                } => !retained_artifacts.is_empty(),
                _ => false,
            }
    });
    !crosses_group && !crosses_protected_record
}

pub(super) fn summary_group(
    groups: &BTreeMap<u64, MessageGroup>,
    source: &SessionSourceRange,
    producer_seq: u64,
    summary: &ModelMessage,
    pinned: bool,
    active_compactable: bool,
) -> Result<MessageGroup, SessionContextError> {
    let logical_source = source_extent(groups, source).ok_or_else(|| {
        SessionContextError::Compaction("summary source has no live Context groups".to_owned())
    })?;
    Ok(MessageGroup {
        key: producer_seq,
        producer_seq,
        source: source.clone(),
        logical_source,
        messages: vec![summary.clone()],
        pinned,
        active_compactable,
    })
}

#[cfg(test)]
mod tests;
