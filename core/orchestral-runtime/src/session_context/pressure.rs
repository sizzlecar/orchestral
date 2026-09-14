use super::*;

impl AgentSessionCompactor {
    /// Compact against the same full-input meter and policy used by context
    /// projection. Candidate summaries are measured before they become durable
    /// facts; a failed candidate cannot enlarge the next model request.
    pub async fn compact_active_run_for_context(
        &self,
        engine: &AgentSessionContextEngine,
        request: SessionContextRequest,
        policy: ContextTokenPolicy,
    ) -> Result<Option<AgentSessionRecord>, SessionContextError> {
        validate_context_request(&request)?;
        if request.through_session_seq.is_some() {
            return Err(SessionContextError::InvalidRequest(
                "cannot compact a historical Context cursor".to_owned(),
            ));
        }
        let records = self.journal.load_session(&request.session_id).await?;
        validate_session_trace(&request.session_id, &records)?;
        let groups = replay_groups(
            &records,
            &request.current_run_id,
            &request.allowed_skill_digests,
        )?;
        let measure = |groups: &BTreeMap<u64, MessageGroup>| {
            let selected = groups
                .values()
                .filter(|group| group.pinned)
                .map(|group| group.key)
                .collect();
            engine.context_input_tokens(
                &assemble_messages(&request.system_message, groups, &selected),
                &request.tools,
                policy,
            )
        };
        let before = measure(&groups)?;
        let budget = request.max_context_tokens - request.reserved_output_tokens;
        if before <= budget {
            return Ok(None);
        }
        let immutable = groups
            .iter()
            .filter(|(_, group)| !is_current_compactable(group, &records, &request.current_run_id))
            .map(|(key, group)| (*key, group.clone()))
            .collect();
        if policy == ContextTokenPolicy::UpperBound && measure(&immutable)? > budget {
            // Task, Skills, retained artifacts and safety facts cannot be
            // removed to manufacture space, even if tool history is available.
            return Ok(None);
        }
        // Planning estimates may use a measured prefix. The immutable subset
        // can lose that prefix and fall back to a larger raw estimate, while a
        // candidate retaining the prefix still fits. Only full candidates can
        // establish progress under Planning; the subset is not a lower bound.

        let mut sources = Vec::new();
        if let Some(preferred) =
            select_active_run_compaction_source(&records, &request.current_run_id, &self.policy)
        {
            sources.push(preferred);
        }
        // Keeping recent exchanges is a quality preference. If the preferred
        // source cannot fit, consider each entire live segment, including its
        // newest exchange or a summary whose budget has since become smaller.
        for source in live_source::compactable_segments(&groups, &records, &request.current_run_id)
        {
            if !sources.contains(&source) {
                sources.push(source);
            }
        }
        // A late summary producer may be adjacent to exchanges on the other
        // side of a surviving fact. Single groups remain useful safe candidates
        // when their combined logical extent cannot be replaced in place.
        for group in groups
            .values()
            .filter(|group| is_current_compactable(group, &records, &request.current_run_id))
        {
            let source = single_range(group.producer_seq);
            if !sources.contains(&source) {
                sources.push(source);
            }
        }
        let mut best = None;
        let mut best_tokens = before;
        'sources: for source in sources {
            let originals = original_compaction_groups(&records, &source)?;
            let current_messages = groups
                .values()
                .filter(|group| source.contains(group.producer_seq))
                .flat_map(|group| group.messages.clone())
                .collect::<Vec<_>>();
            let serialized = serde_json::to_string(&current_messages)
                .map_err(|error| SessionContextError::Compaction(error.to_string()))?;
            // This is a generation hint, not token accounting. Full-input
            // metering below is authoritative for the selected token policy.
            let mut max_chars = (serialized.chars().count() / 2).max(256);
            let focus_messages = groups
                .values()
                .filter(|group| group.pinned && !source.contains(group.producer_seq))
                .flat_map(|group| group.messages.clone())
                .collect::<Vec<_>>();
            let mut previous_summary = None;
            loop {
                let summary = self
                    .summarizer
                    .summarize_with_char_budget(
                        SessionCompactionInput {
                            session_id: request.session_id.clone(),
                            source: source.clone(),
                            groups: originals.clone(),
                            focus_messages: focus_messages.clone(),
                        },
                        max_chars,
                    )
                    .await?;
                summary.validate().map_err(|error| {
                    SessionContextError::Compaction(format!("invalid active-Run summary: {error}"))
                })?;
                if previous_summary.as_ref() == Some(&summary) {
                    break;
                }
                if summary.role == ModelRole::Assistant
                    && !placement::can_replace_source(&groups, &records, &source)
                {
                    continue 'sources;
                }
                let next_seq = records.len() as u64 + 1;
                let replacement =
                    placement::summary_group(&groups, &source, next_seq, &summary, true, true)?;
                let mut candidate = groups.clone();
                candidate.retain(|_, group| !source.contains(group.producer_seq));
                candidate.insert(next_seq, replacement);
                let used = measure(&candidate)?;
                if used < best_tokens {
                    best_tokens = used;
                    best = Some((source.clone(), summary.clone()));
                    if used <= budget {
                        break 'sources;
                    }
                }
                if max_chars == 256 {
                    break;
                }
                previous_summary = Some(summary);
                max_chars = (max_chars / 2).max(256);
            }
        }
        let Some((source, summary)) = best else {
            return Ok(None);
        };
        // Separate live segments can straddle protected facts. A strictly
        // smaller projection may need another pass; never persist no progress.
        self.commit_active_run_summary(&records, &request.current_run_id, source, summary)
            .await
    }
}

fn is_current_compactable(
    group: &MessageGroup,
    records: &[AgentSessionRecord],
    current_run_id: &RunId,
) -> bool {
    group.active_compactable && records[(group.producer_seq - 1) as usize].run_id == *current_run_id
}

#[cfg(test)]
mod tests;
