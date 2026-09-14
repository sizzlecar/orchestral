use super::*;
use crate::session_context::observed_prefix::ObservedPrefixAnchor;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ModelContextBudget<'a> {
    pub(super) remaining_input_tokens: Option<u64>,
    /// A smaller per-request capacity after a backend rejection, distinct
    /// from cumulative Run token/cost limits.
    pub(super) input_capacity_tokens: Option<u64>,
    /// Preferred recovery compaction size, which may be relaxed only within
    /// input_capacity_tokens and the cumulative Run budget.
    pub(super) input_compaction_target_tokens: Option<u64>,
    pub(super) reserved_output_tokens: Option<u64>,
    pub(super) observed_prefix: Option<&'a ObservedPrefixAnchor>,
}

pub(super) fn backend_context_limit(inner: &GenericInner) -> u64 {
    inner
        .backend
        .descriptor()
        .capabilities
        .max_context_tokens
        .unwrap_or(inner.config.max_context_tokens)
        .min(inner.config.max_context_tokens)
}

pub(super) fn context_output_cap(
    inner: &GenericInner,
    projection: &SessionContextProjection,
    preferred: u64,
) -> u64 {
    if inner.config.minimum_output_reserve_tokens.is_none() {
        return preferred;
    }
    let planned_input = projection
        .context_estimate
        .as_ref()
        .map(|estimate| estimate.tokens)
        .unwrap_or(projection.used_input_tokens);
    preferred.min(backend_context_limit(inner).saturating_sub(planned_input))
}

pub(super) async fn project_model_context(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
    initial_input: Option<ModelMessage>,
    through_session_seq: Option<u64>,
    budget: ModelContextBudget<'_>,
) -> Result<SessionContextProjection, SessionContextError> {
    if let Some(message) = initial_input {
        inner
            .session_journal
            .append(AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!(
                    "generic-{}-input",
                    request.run.spec.run_id.as_str()
                )),
                session_id: request.run.spec.session_id.clone(),
                run_id: request.run.spec.run_id.clone(),
                payload: AgentSessionEvent::RunInputCommitted { message },
            })
            .await?;
    }
    // A cursor is a request to reproduce an earlier durable model boundary.
    // Replaying it must never append a new compaction fact to the Session.
    if through_session_seq.is_none() {
        if let Some(compactor) = &inner.session_compactor {
            compactor
                .compact_if_needed(&request.run.spec.session_id, &request.run.spec.run_id)
                .await?;
        }
    }
    let backend_context_limit = backend_context_limit(inner);
    let mut reserved_output_tokens = budget
        .reserved_output_tokens
        .unwrap_or(inner.config.reserved_output_tokens)
        .min(inner.config.reserved_output_tokens);
    let input_limit = budget
        .remaining_input_tokens
        .or(request.run.spec.limits.max_input_tokens)
        .into_iter()
        .chain(budget.input_capacity_tokens)
        .min();
    let mut active_input_limit = input_limit
        .into_iter()
        .chain(budget.input_compaction_target_tokens)
        .min();
    let mut recovery_ceiling_fallback =
        input_limit.filter(|ceiling| active_input_limit.is_some_and(|target| target < *ceiling));
    let system_message = system_message_for_run(&inner.config, run_skills);
    let allowed_skill_digests: std::collections::BTreeMap<_, _> = run_skills
        .map(|skills| {
            skills
                .catalog()
                .skills
                .iter()
                .map(|descriptor| (descriptor.skill_id.clone(), descriptor.digest.clone()))
                .collect()
        })
        .unwrap_or_default();
    let make_request = |reserved_output_tokens, input_limit: Option<u64>| SessionContextRequest {
        session_id: request.run.spec.session_id.clone(),
        current_run_id: request.run.spec.run_id.clone(),
        through_session_seq,
        system_message: system_message.clone(),
        tools: model_definitions.to_vec(),
        history_limit: inner.config.history_limit,
        max_context_tokens: input_limit
            .map(|limit| {
                limit
                    .saturating_add(reserved_output_tokens)
                    .min(backend_context_limit)
            })
            .unwrap_or(backend_context_limit),
        reserved_output_tokens,
        config_digest: inner.config_digest.clone(),
        allowed_skill_digests: allowed_skill_digests.clone(),
    };
    let context_engine = inner.context_engine.with_observed_prefix(
        &request.run.spec.run_id,
        &inner.config_digest,
        budget.observed_prefix,
    );

    let mut previous_overflow = None;
    let mut tried_smaller_reserve = false;
    loop {
        let policy = if request.run.spec.limits.max_input_tokens.is_some()
            || request.run.spec.limits.max_cost.is_some()
        {
            crate::session_context::ContextTokenPolicy::UpperBound
        } else {
            crate::session_context::ContextTokenPolicy::Planning
        };
        match context_engine
            .project_with_policy(
                make_request(reserved_output_tokens, active_input_limit),
                policy,
            )
            .await
        {
            Ok(mut projection) => {
                if policy == crate::session_context::ContextTokenPolicy::Planning {
                    projection.planning = inner.context_engine.planning_trace(
                        &projection.messages,
                        model_definitions,
                        budget.observed_prefix,
                    )?;
                }
                return Ok(projection);
            }
            Err(SessionContextError::ContextOverflow {
                used,
                budget: input_budget,
            }) if through_session_seq.is_none() => {
                // Keep the current raw exchanges before changing their prefix
                // with a summary. This is a soft capacity decision under the
                // Planning policy, never a replacement for hard Run limits.
                if !tried_smaller_reserve {
                    tried_smaller_reserve = true;
                    if let Some(minimum) = inner.config.minimum_output_reserve_tokens {
                        let minimum = minimum.min(reserved_output_tokens);
                        let available = backend_context_limit.saturating_sub(used);
                        if available >= minimum
                            && available < reserved_output_tokens
                            && input_limit.is_none_or(|limit| used <= limit)
                        {
                            // The new input budget is exactly the already
                            // measured candidate: do not use the extra room
                            // to pull more old history into this request.
                            reserved_output_tokens = available;
                            continue;
                        }
                    }
                }
                let Some(compactor) = &inner.session_compactor else {
                    return Err(SessionContextError::ContextOverflow {
                        used,
                        budget: input_budget,
                    });
                };
                if previous_overflow.is_some_and(|previous| used >= previous) {
                    if let Some(ceiling) = recovery_ceiling_fallback.take() {
                        active_input_limit = Some(ceiling);
                        previous_overflow = None;
                        continue;
                    }
                    return Err(SessionContextError::ContextOverflow {
                        used,
                        budget: input_budget,
                    });
                }
                previous_overflow = Some(used);
                publish_telemetry(
                    inner,
                    &request.run.spec.run_id,
                    AgentTelemetryEnvelope {
                        telemetry_id: TelemetryId::new(format!(
                            "generic-{}-context-pressure-{used}-{input_budget}",
                            request.run.spec.run_id.as_str()
                        )),
                        run_id: request.run.spec.run_id.clone(),
                        provider_seq: None,
                        payload: AgentTelemetry::ProgressReported {
                            message: format!(
                                "Compacting context ({used} tokens exceed the {input_budget}-token input budget)"
                            ),
                            fraction: None,
                        },
                    },
                );
                let compacted = compactor
                    .compact_active_run_for_context(
                        &context_engine,
                        make_request(reserved_output_tokens, active_input_limit),
                        policy,
                    )
                    .await?;
                if compacted.is_none() {
                    // Required facts may exceed the preferred half-budget.
                    // Try compaction once within the smaller durable ceiling;
                    // this never grants an unchanged rejected request or a
                    // larger cumulative Run token/cost reservation.
                    if let Some(ceiling) = recovery_ceiling_fallback.take() {
                        active_input_limit = Some(ceiling);
                        previous_overflow = None;
                        continue;
                    }
                    return Err(SessionContextError::ContextOverflow {
                        used,
                        budget: input_budget,
                    });
                }
                let compacted_seq = compacted
                    .as_ref()
                    .expect("compaction presence was checked")
                    .session_seq;
                publish_telemetry(
                    inner,
                    &request.run.spec.run_id,
                    AgentTelemetryEnvelope {
                        telemetry_id: TelemetryId::new(format!(
                            "generic-{}-context-compacted-{compacted_seq}",
                            request.run.spec.run_id.as_str()
                        )),
                        run_id: request.run.spec.run_id.clone(),
                        provider_seq: None,
                        payload: AgentTelemetry::ProgressReported {
                            message: "Context compacted; continuing".to_owned(),
                            fraction: None,
                        },
                    },
                );
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) async fn project_model_messages(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
    initial_input: Option<ModelMessage>,
    through_session_seq: Option<u64>,
    remaining_input_tokens: Option<u64>,
) -> Result<Vec<ModelMessage>, SessionContextError> {
    // This wrapper is used by recovery, not the live per-round loop. Resolve
    // durable evidence once for the current-head projection; historical
    // attempts use replay_started_context with their original trace instead.
    let anchor = if through_session_seq.is_none() {
        observed_prefix_for_run(inner, request)?
    } else {
        None
    };
    let recovery = if through_session_seq.is_none() {
        super::context_recovery::context_recovery_for_run(inner, request)?
    } else {
        None
    };
    project_model_context(
        inner,
        request,
        model_definitions,
        run_skills,
        initial_input,
        through_session_seq,
        ModelContextBudget {
            remaining_input_tokens,
            input_capacity_tokens: recovery
                .as_ref()
                .map(|recovery| recovery.input_budget_tokens),
            input_compaction_target_tokens: recovery
                .as_ref()
                .and_then(|recovery| recovery.compaction_target_tokens()),
            reserved_output_tokens: None,
            observed_prefix: anchor.as_ref(),
        },
    )
    .await
    .map(|projection| projection.messages)
}

pub(super) async fn project_committed_model_messages(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
) -> Result<Vec<ModelMessage>, AgentFailure> {
    project_model_messages(
        inner,
        request,
        model_definitions,
        run_skills,
        None,
        None,
        None,
    )
    .await
    .map_err(session_failure)
}
