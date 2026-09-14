use super::*;
use crate::generic_agent_checkpoint::GenericContextRecovery;

pub(super) fn context_recovery_for_run(
    inner: &GenericInner,
    request: &AgentStartRequest,
) -> Result<Option<GenericContextRecovery>, SessionContextError> {
    let recovery = super::context_anchor::context_checkpoint(inner, request)?
        .map(|stored| {
            stored
                .validate()
                .map(|projection| projection.context_recovery)
        })
        .transpose()
        .map(Option::flatten)
        .map_err(|error| SessionContextError::InvalidRequest(error.to_string()))?;
    if recovery
        .as_ref()
        .is_some_and(|recovery| recovery.retry_number > inner.config.context_recovery.max_retries)
    {
        return Err(SessionContextError::InvalidRequest(
            "durable context recovery exceeds the Host retry limit".to_owned(),
        ));
    }
    Ok(recovery)
}

/// Certify only the rejected attempt. Reprojection then uses the normal
/// context engine/compactor, preserving immutable task and tool facts. A
/// restart resumes at this stable boundary without replaying prior tools.
pub(super) fn commit_context_recovery(
    inner: &GenericInner,
    request: &AgentStartRequest,
    round: u64,
    model_request: &ModelRequest,
    trace: &GenericModelContextTrace,
    previous: Option<&GenericContextRecovery>,
    error: ModelError,
) -> Result<GenericContextRecovery, AgentFailure> {
    let retry_number = previous.map_or(Some(1), |prior| prior.retry_number.checked_add(1));
    let planned_input = trace
        .context_estimate
        .as_ref()
        .map_or(trace.used_input_tokens, |estimate| estimate.tokens);
    let rejected_input_tokens = trace.input_budget_tokens.min(planned_input);
    // The retry must be smaller, but halving a hard ceiling can exclude the
    // immutable task itself. Keep half as the preferred compaction target.
    let input_budget_tokens = rejected_input_tokens.saturating_sub(1);
    let target_input_tokens = rejected_input_tokens / 2;
    let Some(retry_number) = retry_number.filter(|number| {
        *number <= inner.config.context_recovery.max_retries && input_budget_tokens > 0
    }) else {
        return Err(model_failure(error));
    };
    if round.checked_add(1).is_none() {
        return Err(model_failure(error));
    }
    let run_id = &request.run.spec.run_id;
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-context-rejected-{round}",
            run_id.as_str()
        )),
        GenericCheckpointEvent::ModelContextRejected {
            round,
            request_id: model_request.request_id.clone(),
            retry_number,
            input_budget_tokens,
            error,
        },
    )?;
    publish_telemetry(inner, run_id, AgentTelemetryEnvelope {
        telemetry_id: TelemetryId::new(format!(
            "generic-{}-context-recovery-{round}", run_id.as_str()
        )),
        run_id: run_id.clone(),
        provider_seq: None,
        payload: AgentTelemetry::ProgressReported {
            message: format!(
                "Model rejected context capacity; compacting toward {target_input_tokens} input tokens within a {input_budget_tokens}-token ceiling (recovery {retry_number}/{})",
                inner.config.context_recovery.max_retries
            ),
            fraction: None,
        },
    });
    Ok(GenericContextRecovery {
        retry_number,
        input_budget_tokens,
    })
}
