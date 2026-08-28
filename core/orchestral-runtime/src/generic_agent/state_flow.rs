use super::*;

pub(super) fn model_tool_result(outcome: ToolOutcome) -> (serde_json::Value, bool) {
    match outcome {
        ToolOutcome::Completed {
            output: ToolOutput::Inline(output),
        } => (output, false),
        ToolOutcome::Completed {
            output: ToolOutput::Artifact(artifact),
        } => (
            serde_json::json!({
                "kind": "artifact",
                "artifact": artifact.artifact,
                "media_type": artifact.media_type,
                "byte_size": artifact.byte_size,
                "summary": artifact.summary,
            }),
            false,
        ),
        other => (
            serde_json::to_value(other).unwrap_or_else(|error| {
                serde_json::json!({
                    "status": "failed",
                    "code": "tool_result_serialization",
                    "message": error.to_string(),
                })
            }),
            true,
        ),
    }
}

pub(super) fn retained_artifacts_for_outcome(outcome: &ToolOutcome) -> Vec<ArtifactRefWithDigest> {
    match outcome {
        ToolOutcome::Completed {
            output: ToolOutput::Artifact(artifact),
        } => vec![artifact.artifact.clone()],
        _ => Vec::new(),
    }
}

pub(super) fn merge_usage(total: &mut ModelUsage, observed: ModelUsage) {
    total.input_tokens = add_optional(total.input_tokens, observed.input_tokens);
    total.output_tokens = add_optional(total.output_tokens, observed.output_tokens);
}

pub(super) fn add_optional(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ModelDispatchBudget {
    pub(super) projected_input_tokens: u64,
    pub(super) max_output_tokens: Option<u64>,
}

pub(super) fn reserve_tool_calls(
    current: u64,
    additional: u64,
    limit: Option<u64>,
) -> Result<u64, RunLimitKind> {
    let next = current
        .checked_add(additional)
        .ok_or(RunLimitKind::ToolCalls)?;
    if limit.is_some_and(|limit| next > limit) {
        return Err(RunLimitKind::ToolCalls);
    }
    Ok(next)
}

pub(super) fn reserve_tool_call(current: u64, limit: Option<u64>) -> Result<u64, RunLimitKind> {
    reserve_tool_calls(current, 1, limit)
}

pub(super) fn remaining_input_tokens(
    request: &AgentStartRequest,
    usage: &ModelUsage,
) -> Result<Option<u64>, RunLimitKind> {
    let Some(limit) = request.run.spec.limits.max_input_tokens else {
        return Ok(None);
    };
    let remaining = limit.saturating_sub(usage.input_tokens.unwrap_or(0));
    (remaining > 0)
        .then_some(Some(remaining))
        .ok_or(RunLimitKind::InputTokens)
}

pub(super) fn output_reserve_tokens(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
) -> Result<u64, RunLimitKind> {
    let remaining = request
        .run
        .spec
        .limits
        .max_output_tokens
        .map(|limit| limit.saturating_sub(usage.output_tokens.unwrap_or(0)));
    if remaining == Some(0) {
        return Err(RunLimitKind::OutputTokens);
    }
    Ok(remaining
        .unwrap_or(config.reserved_output_tokens)
        .min(config.reserved_output_tokens))
}

pub(super) fn model_dispatch_budget(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
    projected_input_tokens: u64,
    output_reserve_tokens: u64,
) -> Result<ModelDispatchBudget, RunLimitKind> {
    if request
        .run
        .spec
        .limits
        .max_input_tokens
        .is_some_and(|limit| {
            usage
                .input_tokens
                .unwrap_or(0)
                .saturating_add(projected_input_tokens)
                > limit
        })
    {
        return Err(RunLimitKind::InputTokens);
    }

    let mut output_cap = output_reserve_tokens;
    let mut bounded_output = request.run.spec.limits.max_output_tokens.is_some();
    if let Some(ceiling) = &request.run.spec.limits.max_cost {
        let policy = config
            .model_cost_policy
            .as_ref()
            .expect("cost limits are admitted only with a bound cost policy");
        let total_input = usage
            .input_tokens
            .unwrap_or(0)
            .saturating_add(projected_input_tokens);
        let total_output = usage.output_tokens.unwrap_or(0);
        let Some(allowed_output) = policy.max_output_tokens_within(
            total_input,
            total_output.saturating_add(output_cap),
            ceiling,
        ) else {
            return Err(RunLimitKind::Cost);
        };
        output_cap = allowed_output.saturating_sub(total_output).min(output_cap);
        if output_cap == 0 {
            return Err(RunLimitKind::Cost);
        }
        bounded_output = true;
    }

    Ok(ModelDispatchBudget {
        projected_input_tokens,
        max_output_tokens: bounded_output.then_some(output_cap),
    })
}

pub(super) fn validate_observed_usage(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    previous: &ModelUsage,
    observed: Option<&ModelUsage>,
    dispatch: ModelDispatchBudget,
) -> Result<(), AgentFailure> {
    let needs_input = request.run.spec.limits.max_input_tokens.is_some()
        || request.run.spec.limits.max_cost.as_ref().is_some_and(|_| {
            config
                .model_cost_policy
                .as_ref()
                .is_some_and(|policy| policy.input_microunits_per_million_tokens > 0)
        });
    let needs_output = request.run.spec.limits.max_output_tokens.is_some()
        || request.run.spec.limits.max_cost.as_ref().is_some_and(|_| {
            config
                .model_cost_policy
                .as_ref()
                .is_some_and(|policy| policy.output_microunits_per_million_tokens > 0)
        });
    let Some(observed) = observed else {
        if needs_input || needs_output {
            return Err(agent_failure(
                "model_usage_missing",
                "model usage is required to enforce the requested Run token or cost limit",
                false,
            ));
        }
        return Ok(());
    };
    if needs_input && observed.input_tokens.is_none() {
        return Err(agent_failure(
            "model_input_usage_missing",
            "model input usage is required to enforce the requested Run limit",
            false,
        ));
    }
    if needs_output && observed.output_tokens.is_none() {
        return Err(agent_failure(
            "model_output_usage_missing",
            "model output usage is required to enforce the requested Run limit",
            false,
        ));
    }
    if observed
        .input_tokens
        .is_some_and(|tokens| tokens > dispatch.projected_input_tokens)
    {
        return Err(agent_failure(
            "model_input_usage_exceeded_reservation",
            "model reported more input tokens than the bound token meter reserved",
            false,
        ));
    }
    if let Some(output_cap) = dispatch.max_output_tokens {
        if observed
            .output_tokens
            .is_some_and(|tokens| tokens > output_cap)
        {
            return Err(agent_failure(
                "model_output_usage_exceeded_reservation",
                "model reported more output tokens than the request budget allowed",
                false,
            ));
        }
    }

    let next_input = previous
        .input_tokens
        .unwrap_or(0)
        .saturating_add(observed.input_tokens.unwrap_or(0));
    let next_output = previous
        .output_tokens
        .unwrap_or(0)
        .saturating_add(observed.output_tokens.unwrap_or(0));
    if request
        .run
        .spec
        .limits
        .max_input_tokens
        .is_some_and(|limit| next_input > limit)
    {
        return Err(agent_failure(
            "model_input_limit_violated",
            "model input usage exceeded the immutable Run limit",
            false,
        ));
    }
    if request
        .run
        .spec
        .limits
        .max_output_tokens
        .is_some_and(|limit| next_output > limit)
    {
        return Err(agent_failure(
            "model_output_limit_violated",
            "model output usage exceeded the immutable Run limit",
            false,
        ));
    }
    if let Some(ceiling) = &request.run.spec.limits.max_cost {
        let actual = config
            .model_cost_policy
            .as_ref()
            .expect("cost limits are admitted only with a bound cost policy")
            .quote(next_input, next_output);
        if actual.currency != ceiling.currency || actual.microunits > ceiling.microunits {
            return Err(agent_failure(
                "model_cost_limit_violated",
                "model usage exceeded the immutable Run cost limit",
                false,
            ));
        }
    }
    Ok(())
}

pub(super) fn exhausted_usage_limit(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
) -> Option<RunLimitKind> {
    if request
        .run
        .spec
        .limits
        .max_input_tokens
        .is_some_and(|limit| usage.input_tokens.unwrap_or(0) >= limit)
    {
        return Some(RunLimitKind::InputTokens);
    }
    if request
        .run
        .spec
        .limits
        .max_output_tokens
        .is_some_and(|limit| usage.output_tokens.unwrap_or(0) >= limit)
    {
        return Some(RunLimitKind::OutputTokens);
    }
    if let (Some(ceiling), Some(policy)) = (
        request.run.spec.limits.max_cost.as_ref(),
        config.model_cost_policy.as_ref(),
    ) {
        let actual = policy.quote(
            usage.input_tokens.unwrap_or(0),
            usage.output_tokens.unwrap_or(0),
        );
        if actual.microunits >= ceiling.microunits {
            return Some(RunLimitKind::Cost);
        }
    }
    None
}

pub(super) fn continuation_limit(
    config: &GenericAgentConfig,
    request: &AgentStartRequest,
    usage: &ModelUsage,
    completed_round: u64,
    model_step_limit: Option<u64>,
) -> Option<RunLimitKind> {
    if model_step_limit.is_some_and(|limit| completed_round >= limit) {
        return Some(RunLimitKind::ModelSteps);
    }
    exhausted_usage_limit(config, request, usage)
}

pub(super) fn agent_input_message(
    request: &AgentStartRequest,
) -> Result<ModelMessage, AgentProtocolError> {
    agent_content_message(&request.run.spec.input)
}

pub(super) fn agent_content_message(items: &[Content]) -> Result<ModelMessage, AgentProtocolError> {
    let mut content = Vec::with_capacity(items.len());
    for item in items {
        match (&item.media_type[..], &item.body) {
            ("text/plain", ContentBody::Inline(serde_json::Value::String(text)))
                if !text.is_empty() =>
            {
                content.push(ModelContent::Text { text: text.clone() });
            }
            _ => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "text-first Generic Agent accepts inline text/plain input only",
                ));
            }
        }
    }
    Ok(ModelMessage {
        role: ModelRole::User,
        content,
    })
}

pub(super) fn commit_loop_boundary(
    inner: &GenericInner,
    run_id: &RunId,
    next_model_round: u64,
    usage: &ModelUsage,
    tool_call_count: u64,
    last_response: &str,
    supporting_event_ids: &[AgentEventId],
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-boundary-{next_model_round}",
            run_id.as_str()
        )),
        GenericCheckpointEvent::LoopBoundaryCommitted {
            next_model_round,
            usage: usage.clone(),
            tool_call_count,
            last_response: last_response.to_owned(),
            supporting_event_ids: supporting_event_ids.to_vec(),
        },
    )
}

pub(super) fn commit_model_attempt(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    request: &ModelRequest,
    context: &GenericModelContextTrace,
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!("generic-{}-model-attempt-{round}", run_id.as_str())),
        GenericCheckpointEvent::ModelAttemptStarted {
            round,
            request_id: request.request_id.clone(),
            request_digest: model_request_digest(request)?,
            max_output_tokens: request.max_output_tokens,
            context: context.clone(),
        },
    )
}

pub(super) fn model_context_trace(
    projection: &SessionContextProjection,
    history_limit: usize,
) -> GenericModelContextTrace {
    GenericModelContextTrace {
        through_session_seq: projection.through_session_seq,
        included_ranges: projection.included_ranges.clone(),
        deferred_ranges: projection.deferred_ranges.clone(),
        config_digest: projection.config_digest.clone(),
        history_limit,
        used_input_tokens: projection.used_input_tokens,
        input_budget_tokens: projection.input_budget_tokens,
    }
}

pub(super) fn model_request_for_round(
    request: &AgentStartRequest,
    round: u64,
    messages: &[ModelMessage],
    tools: &[ModelToolDefinition],
    max_output_tokens: Option<u64>,
) -> ModelRequest {
    ModelRequest {
        request_id: ModelRequestId::new(format!(
            "model-{}-{round}",
            request.run.spec.run_id.as_str()
        )),
        messages: messages.to_vec(),
        tools: tools.to_vec(),
        output_schema: None,
        max_output_tokens,
        extensions: Default::default(),
    }
}

pub(super) fn model_request_digest(request: &ModelRequest) -> Result<Digest, AgentFailure> {
    serde_jcs::to_vec(request)
        .map(Digest::sha256)
        .map_err(|error| {
            agent_failure(
                "generic_checkpoint",
                format!("could not digest model request: {error}"),
                true,
            )
        })
}

pub(super) fn commit_model_observation(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    request_id: &ModelRequestId,
    observation: GenericModelObservation,
) -> Result<(), AgentFailure> {
    append_checkpoint(
        inner,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-model-observed-{round}",
            run_id.as_str()
        )),
        GenericCheckpointEvent::ModelAttemptObserved {
            round,
            request_id: request_id.clone(),
            observation,
        },
    )
}

pub(super) fn append_checkpoint(
    inner: &GenericInner,
    run_id: &RunId,
    event_id: GenericCheckpointEventId,
    payload: GenericCheckpointEvent,
) -> Result<(), AgentFailure> {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let run = state.runs.get_mut(run_id).ok_or_else(|| {
        agent_failure(
            "generic_checkpoint",
            "Run disappeared before its private checkpoint was committed",
            true,
        )
    })?;
    append_checkpoint_to_run(inner, run, run_id, event_id, payload)
}

pub(super) fn append_checkpoint_to_run(
    inner: &GenericInner,
    run: &mut GenericRun,
    run_id: &RunId,
    event_id: GenericCheckpointEventId,
    payload: GenericCheckpointEvent,
) -> Result<(), AgentFailure> {
    let expected_previous = run.checkpoint_seq;
    let outcome = inner
        .checkpoint_store
        .append(
            run_id,
            expected_previous,
            GenericCheckpointDraft {
                event_id,
                run_id: run_id.clone(),
                payload,
            },
        )
        .map_err(checkpoint_failure)?;
    if matches!(outcome, AppendGenericCheckpointOutcome::Appended) {
        run.checkpoint_seq = expected_previous.saturating_add(1);
    }
    Ok(())
}

pub(super) fn checkpoint_provider_events(
    inner: &GenericInner,
    run: &mut GenericRun,
    run_id: &RunId,
    events: &[AgentEventDraft],
) -> Result<(), AgentFailure> {
    let first = events
        .first()
        .expect("Provider checkpoint event batch is never empty");
    let last = events
        .last()
        .expect("Provider checkpoint event batch is never empty");
    append_checkpoint_to_run(
        inner,
        run,
        run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-provider-{}-{}",
            run_id.as_str(),
            first.event_id.as_str(),
            last.event_id.as_str()
        )),
        GenericCheckpointEvent::ProviderEventsCommitted {
            events: events.to_vec(),
        },
    )
}

pub(super) fn take_queued_steers(inner: &GenericInner, run_id: &RunId) -> Vec<QueuedSteer> {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state
        .runs
        .get_mut(run_id)
        .map(|run| run.queued_steers.drain(..).collect())
        .unwrap_or_default()
}

pub(super) async fn commit_queued_steers(
    inner: &GenericInner,
    request: &AgentStartRequest,
    model_messages: &mut Vec<ModelMessage>,
) -> Result<usize, AgentFailure> {
    let run_id = &request.run.spec.run_id;
    let queued = take_queued_steers(inner, run_id);
    let count = queued.len();
    for steer in queued {
        append_session_event(
            inner,
            AgentSessionEventDraft {
                event_id: AgentSessionEventId::new(format!(
                    "generic-{}-steer-{}",
                    run_id.as_str(),
                    steer.command_id.as_str()
                )),
                session_id: request.run.spec.session_id.clone(),
                run_id: run_id.clone(),
                payload: AgentSessionEvent::RunInputCommitted {
                    message: steer.message.clone(),
                },
            },
        )
        .await?;
        if !publish_durable(
            inner,
            run_id,
            AgentEventDraft {
                event_id: AgentEventId::new(format!(
                    "generic-{}-steer-{}-committed",
                    run_id.as_str(),
                    steer.command_id.as_str()
                )),
                run_id: run_id.clone(),
                causation_id: Some(steer.command_id),
                source_fingerprint: None,
                payload: AgentEvent::InputCommitted {
                    content: steer.content,
                },
            },
        ) {
            return Err(agent_failure(
                "generic_checkpoint",
                "steer input could not be committed to the private WAL",
                true,
            ));
        }
        model_messages.push(steer.message);
    }
    Ok(count)
}

pub(super) fn publish_durable(
    inner: &GenericInner,
    run_id: &RunId,
    draft: AgentEventDraft,
) -> bool {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(run) = state.runs.get_mut(run_id) else {
        return false;
    };
    if run.terminal {
        return false;
    }
    if let Err(failure) =
        checkpoint_provider_events(inner, run, run_id, std::slice::from_ref(&draft))
    {
        poison_run_after_checkpoint_failure(run, failure);
        return false;
    }
    let terminal = matches!(
        draft.payload,
        AgentEvent::DeliveryCommitted { .. }
            | AgentEvent::RunIncomplete { .. }
            | AgentEvent::RunFailed { .. }
            | AgentEvent::RunCancelled { .. }
    );
    run.durable_events.push(draft.clone());
    run.terminal = terminal;
    let _ = run
        .sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(draft))));
    true
}

pub(super) fn publish_telemetry(
    inner: &GenericInner,
    run_id: &RunId,
    telemetry: AgentTelemetryEnvelope,
) {
    let state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(run) = state.runs.get(run_id) {
        if !run.terminal {
            let _ = run
                .sender
                .send(Ok(AgentProviderStreamItem::Telemetry(telemetry)));
        }
    }
}

pub(super) fn publish_tool_activity(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    call_id: &ModelToolCallId,
    tool_name: &str,
    state: ToolActivityState,
) {
    let activity_id = format!(
        "generic-{}-tool-{round}-{}",
        run_id.as_str(),
        call_id.as_str()
    );
    publish_telemetry(
        inner,
        run_id,
        AgentTelemetryEnvelope {
            telemetry_id: TelemetryId::new(format!("{activity_id}-{state:?}")),
            run_id: run_id.clone(),
            provider_seq: None,
            payload: AgentTelemetry::ToolActivity {
                activity_id: ToolActivityId::new(activity_id),
                tool_name: tool_name.to_owned(),
                state,
            },
        },
    );
}
