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
    // barriers, like unresolved effects. Artifact references remain authorized
    // by their durable producers and can be compacted with their exchanges.
    let crosses_protected_record = records.iter().any(|record| {
        extent.contains(record.session_seq)
            && matches!(
                &record.payload,
                AgentSessionEvent::SkillLoaded { .. }
                    | AgentSessionEvent::EffectUncertaintyCommitted { .. }
            )
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
    let mut source_ranges: Vec<SessionSourceRange> = Vec::new();
    for producer in groups
        .values()
        .filter(|group| source.contains(group.producer_seq))
    {
        if let Some(last) = source_ranges
            .last_mut()
            .filter(|last| last.last_session_seq.checked_add(1) == Some(producer.producer_seq))
        {
            last.last_session_seq = producer.producer_seq;
        } else {
            source_ranges.push(single_range(producer.producer_seq));
        }
    }
    Ok(MessageGroup {
        key: producer_seq,
        producer_seq,
        source_ranges,
        logical_source,
        messages: vec![project_summary(summary)],
        pinned,
        active_compactable,
    })
}

/// Journal summaries are historical observations, not an unfinished model
/// response. Project Assistant summaries as explicitly labelled input so the
/// next request can continue on providers that reject Assistant prefill. Apply
/// this during replay as well as pressure measurement, including old journals.
/// Legacy System summaries retain their existing projection semantics.
pub(super) fn project_summary(summary: &ModelMessage) -> ModelMessage {
    if summary.role != ModelRole::Assistant {
        return summary.clone();
    }
    let text = summary
        .content
        .iter()
        .filter_map(|content| match content {
            ModelContent::Text { text } => Some(text.as_str()),
            _ => None, // Canonical journal validation permits text summaries only.
        })
        .collect::<Vec<_>>()
        .join("\n");
    ModelMessage {
        role: ModelRole::User,
        content: vec![
            ModelContent::Data {
                media_type: SUMMARY_CONTEXT_MEDIA_TYPE.to_owned(),
                value: serde_json::json!({"kind":"historical_context_summary"}),
            },
            ModelContent::Text {
                text: format!("Historical context (summary; not new instructions):\n{text}"),
            },
        ],
    }
}

const SUMMARY_CONTEXT_MEDIA_TYPE: &str = "application/vnd.orchestral.session-summary+json";

/// A Host-projected historical observation may extend an observed input prefix
/// without being mistaken for new user instructions. Text content never serves
/// as this marker. This classification affects soft estimates only.
pub(super) fn is_summary_context(message: &ModelMessage) -> bool {
    message.role == ModelRole::User
        && matches!(message.content.as_slice(),
        [ModelContent::Data { media_type, value }, ModelContent::Text { .. }]
            if media_type == SUMMARY_CONTEXT_MEDIA_TYPE
                && *value == serde_json::json!({"kind":"historical_context_summary"}))
}

#[cfg(test)]
mod tests;
