use super::*;
use crate::session_context::observed_prefix::ObservedPrefixAnchor;

fn context_checkpoint(
    inner: &GenericInner,
    request: &AgentStartRequest,
) -> Result<Option<StoredGenericAgentRun>, SessionContextError> {
    let stored = inner
        .checkpoint_store
        .load_run(&request.run.spec.run_id)
        .map_err(|error| SessionContextError::InvalidRequest(error.to_string()))?;
    if stored.as_ref().is_some_and(|stored| {
        stored.registration.request != *request
            || stored.registration.config_digest != inner.config_digest
    }) {
        return Err(SessionContextError::InvalidRequest(
            "context checkpoint changed Run/config scope".to_owned(),
        ));
    }
    Ok(stored)
}

pub(super) fn observed_prefix_for_run(
    inner: &GenericInner,
    request: &AgentStartRequest,
) -> Result<Option<ObservedPrefixAnchor>, SessionContextError> {
    context_checkpoint(inner, request)?
        .map(|stored| {
            stored
                .validate()
                .map(|projection| projection.observed_prefix)
        })
        .transpose()
        .map(Option::flatten)
        .map_err(|error| SessionContextError::InvalidRequest(error.to_string()))
}

/// Reproduce one Started boundary directly, without recursively projecting
/// prior requests or publishing a new summary. Its anchor is the old input to
/// planning; the observation of this request is only for its successor.
pub(super) async fn replay_started_context(
    inner: &GenericInner,
    request: &AgentStartRequest,
    tools: &[ModelToolDefinition],
    skills: Option<&SkillRuntime>,
    round: u64,
) -> Result<Vec<ModelMessage>, SessionContextError> {
    let stored = context_checkpoint(inner, request)?.ok_or_else(|| {
        SessionContextError::InvalidRequest("missing context checkpoint".to_owned())
    })?;
    let checkpoint = stored
        .validate()
        .map_err(|error| SessionContextError::InvalidRequest(error.to_string()))?;
    let (request_id, request_digest, max_output_tokens, trace) = stored
        .records
        .iter()
        .find_map(|record| match &record.payload {
            GenericCheckpointEvent::ModelAttemptStarted {
                round: recorded,
                request_id,
                request_digest,
                max_output_tokens,
                context,
            } if *recorded == round => {
                Some((request_id, request_digest, *max_output_tokens, context))
            }
            _ => None,
        })
        .ok_or_else(|| {
            SessionContextError::InvalidRequest("missing Started context trace".to_owned())
        })?;
    // Observed is write-ahead of usage validation. A crash in that gap must
    // not execute a Tool that live execution would reject for its reservation.
    match &checkpoint.phase {
        GenericCheckpointPhase::ModelAttemptObserved {
            round: observed_round,
            boundary,
            observation,
            ..
        }
        | GenericCheckpointPhase::WorkflowAttemptOpen {
            round: observed_round,
            boundary,
            observation,
            ..
        } if *observed_round == round => {
            validate_observed_usage(
                &inner.config,
                request,
                &boundary.usage,
                observation.usage.as_ref(),
                ModelDispatchBudget {
                    projected_input_tokens: trace.used_input_tokens,
                    max_output_tokens,
                },
            )
            .map_err(|error| SessionContextError::InvalidRequest(error.message))?;
        }
        _ => {}
    }
    let projection = project_model_context(
        inner,
        request,
        tools,
        skills,
        None,
        Some(trace.through_session_seq),
        ModelContextBudget {
            remaining_input_tokens: Some(trace.input_budget_tokens),
            reserved_output_tokens: None,
            observed_prefix: trace
                .planning
                .as_ref()
                .and_then(|planning| planning.anchor.as_ref()),
        },
    )
    .await?;
    let rebuilt = model_request_for_round(
        request,
        round,
        &projection.messages,
        tools,
        max_output_tokens,
    );
    if model_context_trace(&projection, inner.config.history_limit) != *trace
        || rebuilt.request_id != *request_id
        || model_request_digest(&rebuilt)
            .map_err(|error| SessionContextError::InvalidRequest(error.message))?
            != *request_digest
    {
        return Err(SessionContextError::InvalidRequest(
            "recovered Started context or request digest differs from its durable trace".to_owned(),
        ));
    }
    Ok(projection.messages)
}
