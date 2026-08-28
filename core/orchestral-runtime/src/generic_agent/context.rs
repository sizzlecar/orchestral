use super::*;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ModelContextBudget {
    pub(super) remaining_input_tokens: Option<u64>,
    pub(super) reserved_output_tokens: Option<u64>,
}

pub(super) async fn project_model_context(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_definitions: &[ModelToolDefinition],
    run_skills: Option<&SkillRuntime>,
    initial_input: Option<ModelMessage>,
    through_session_seq: Option<u64>,
    budget: ModelContextBudget,
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
    let backend_context_limit = inner
        .backend
        .descriptor()
        .capabilities
        .max_context_tokens
        .unwrap_or(inner.config.max_context_tokens)
        .min(inner.config.max_context_tokens);
    let reserved_output_tokens = budget
        .reserved_output_tokens
        .unwrap_or(inner.config.reserved_output_tokens)
        .min(inner.config.reserved_output_tokens);
    let max_context_tokens = budget
        .remaining_input_tokens
        .or(request.run.spec.limits.max_input_tokens)
        .map(|limit| {
            limit
                .saturating_add(reserved_output_tokens)
                .min(backend_context_limit)
        })
        .unwrap_or(backend_context_limit);
    inner
        .context_engine
        .project(SessionContextRequest {
            session_id: request.run.spec.session_id.clone(),
            current_run_id: request.run.spec.run_id.clone(),
            through_session_seq,
            system_message: system_message_for_run(&inner.config, run_skills),
            tools: model_definitions.to_vec(),
            history_limit: inner.config.history_limit,
            max_context_tokens,
            reserved_output_tokens,
            config_digest: inner.config_digest.clone(),
            allowed_skill_digests: run_skills
                .map(|skills| {
                    skills
                        .catalog()
                        .skills
                        .iter()
                        .map(|descriptor| (descriptor.skill_id.clone(), descriptor.digest.clone()))
                        .collect()
                })
                .unwrap_or_default(),
        })
        .await
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
    project_model_context(
        inner,
        request,
        model_definitions,
        run_skills,
        initial_input,
        through_session_seq,
        ModelContextBudget {
            remaining_input_tokens,
            reserved_output_tokens: None,
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
