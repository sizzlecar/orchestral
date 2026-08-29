use super::*;

pub(super) fn checkpoint_recovery_events(
    stored: &StoredGenericAgentRun,
) -> Result<Vec<AgentEventDraft>, AgentProtocolError> {
    let mut events = Vec::new();
    for record in &stored.records {
        match &record.payload {
            GenericCheckpointEvent::ProviderEventsCommitted { events: committed } => {
                events.extend(committed.iter().cloned());
            }
            GenericCheckpointEvent::CommandCommitted {
                command, outcome, ..
            } => {
                events.push(
                    ProviderCommandDisposition {
                        command_id: command.command_id.clone(),
                        run_id: command.run_id.clone(),
                        outcome: outcome.clone(),
                        duplicate: false,
                    }
                    .to_event_draft()?,
                );
            }
            GenericCheckpointEvent::LoopBoundaryCommitted { .. }
            | GenericCheckpointEvent::ModelAttemptStarted { .. }
            | GenericCheckpointEvent::ModelAttemptObserved { .. }
            | GenericCheckpointEvent::WorkflowAttemptStarted { .. } => {}
        }
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stage_observed_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if !matches!(
        observation.finish_reason,
        ModelFinishReason::ToolCalls | ModelFinishReason::Stop
    ) || observation.tool_calls.len() != 1
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "observed model recovery currently requires one fresh input request",
        )
        .with_details(serde_json::json!({
            "boundary": "model_attempt_observed",
            "round": round,
            "request_id": request_id,
        })));
    }
    let call = observation
        .tool_calls
        .first()
        .cloned()
        .expect("one observed Tool call was checked");
    if !call.ended {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "observed model recovery requires a complete Tool call",
        )
        .with_details(serde_json::json!({
            "boundary": "model_attempt_observed",
            "round": round,
            "request_id": request_id,
        })));
    }
    let pending_call = PendingModelToolCall {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        arguments: call.arguments.clone(),
        extensions: call.extensions.clone(),
        ended: call.ended,
    };
    let arguments = parse_tool_arguments(&pending_call).map_err(observed_recovery_error)?;
    if call.name != REQUEST_INPUT_TOOL_NAME {
        if call.name == SKILL_READ_TOOL_NAME {
            return stage_skill_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            );
        }
        if call.name == WORKFLOW_TOOL_NAME {
            return stage_workflow_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            );
        }
        let approval_id = approval_request_id(stored.registration.run_id(), round, &call.call_id);
        let has_approval_interaction = recovery_events.iter().any(|event| match &event.payload {
            AgentEvent::RequestOpened { request } => request.request_id == approval_id,
            AgentEvent::RequestResolved { request_id, .. } => request_id == &approval_id,
            _ => false,
        });
        return if has_approval_interaction {
            stage_approval_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            )
        } else {
            stage_tool_recovery(
                inner,
                stored,
                boundary,
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_events,
            )
        };
    }
    let prompt = parse_input_request(arguments.clone()).map_err(observed_recovery_error)?;
    let input_request_id = input_request_id(stored.registration.run_id(), round, &call.call_id);
    let expected_request = PendingRequest {
        request_id: input_request_id.clone(),
        blocking: true,
        payload: PendingRequestPayload::Input {
            prompt: vec![Content::text(prompt.clone())],
            input_schema: None,
        },
    };
    let mut request_open = false;
    let mut resolved_response = None;
    for event in &recovery_events {
        match &event.payload {
            AgentEvent::RequestOpened { request } if request.request_id == input_request_id => {
                if request_open || request != &expected_request {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered input request does not match its observed model call",
                    ));
                }
                request_open = true;
            }
            AgentEvent::RequestResolved {
                request_id,
                resolution,
                ..
            } if request_id == &input_request_id => {
                if !request_open
                    || resolved_response.is_some()
                    || !matches!(resolution, RequestResolution::Input { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered input resolution does not match its pending request",
                    ));
                }
                let command_id = event.causation_id.clone().ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered input resolution has no causating command",
                    )
                })?;
                resolved_response = Some(InputResponse {
                    command_id,
                    resolution: resolution.clone(),
                });
            }
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. } => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered interaction crossed the observed input request boundary",
                ));
            }
            _ => {}
        }
    }
    if let Some(response) = &resolved_response {
        validate_recovered_input_resolution(&stored.records, &input_request_id, response)?;
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Input {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            prompt,
            request_open,
            committed_response: None,
            resolved_response,
            response: None,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stage_workflow_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if recovery_events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. }
        )
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Workflow crossed an interaction boundary",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Workflow {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            recovery_replay: false,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stage_started_workflow_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call_id: ModelToolCallId,
    arguments_digest: Digest,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    let call = observation
        .tool_calls
        .iter()
        .find(|call| call.call_id == call_id)
        .cloned()
        .ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "Workflow start fence has no matching observed model call",
            )
        })?;
    if call.name != WORKFLOW_TOOL_NAME
        || !call.ended
        || Digest::sha256(call.arguments.as_bytes()) != arguments_digest
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "Workflow start fence differs from its observed model call",
        ));
    }
    let arguments = parse_tool_arguments(&PendingModelToolCall {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        arguments: call.arguments.clone(),
        extensions: call.extensions.clone(),
        ended: call.ended,
    })
    .map_err(observed_recovery_error)?;
    let recovered_output = recovered_workflow_output(
        stored.registration.run_id(),
        round,
        &call.call_id,
        &recovery_events,
    )?;
    let Some((workflow_event_id, outcome)) = recovered_output else {
        let workflow = inner
            .tools
            .as_ref()
            .and_then(|tools| tools.workflow.as_ref())
            .ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Workflow call has no bound execution strategy",
                )
            })?;
        if !workflow.supports_recovery_replay() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "Workflow execution contract does not support deterministic recovery replay",
            )
            .with_details(serde_json::json!({
                "boundary": "workflow_attempt_open",
                "round": round,
                "request_id": request_id,
                "call_id": call_id,
            })));
        }
        return stage_loop_recovery(
            inner,
            stored,
            boundary,
            recovery_events,
            GenericRecoveryContinuation::Workflow {
                round,
                request_id,
                request_digest,
                observation,
                call,
                arguments,
                recovery_replay: true,
            },
        );
    };
    let tool_call_limit = inner
        .config
        .continuation
        .effective_tool_calls(stored.registration.request.run.spec.limits.max_tool_calls);
    let durable_count = reserve_tool_call(boundary.tool_call_count, tool_call_limit)
        .and_then(|outer| reserve_tool_calls(outer, outcome.tool_calls, tool_call_limit));
    if durable_count.is_err() {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "durable Workflow output exceeds the Run Tool call limit",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::WorkflowOutput {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            outcome,
            workflow_event_id,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stage_skill_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if recovery_events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. }
        )
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill load crossed an interaction boundary",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Skill {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            recovered_observation: None,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stage_tool_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    if recovery_events.iter().any(|event| {
        matches!(
            &event.payload,
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. }
        )
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered direct Tool crossed an interaction boundary",
        ));
    }
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Tool {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn stage_approval_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    round: u64,
    request_id: ModelRequestId,
    request_digest: Digest,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    let approval_request_id =
        approval_request_id(stored.registration.run_id(), round, &call.call_id);
    let mut opened_request = None;
    let mut resolved_response = None;
    for event in &recovery_events {
        match &event.payload {
            AgentEvent::RequestOpened { request } if request.request_id == approval_request_id => {
                if opened_request.is_some()
                    || !request.blocking
                    || !matches!(request.payload, PendingRequestPayload::Approval { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered approval request is not a unique blocking request",
                    ));
                }
                opened_request = Some(request.clone());
            }
            AgentEvent::RequestResolved {
                request_id: resolved,
                resolution,
                ..
            } if resolved == &approval_request_id => {
                if opened_request.is_none()
                    || resolved_response.is_some()
                    || !matches!(resolution, RequestResolution::Approval { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered approval resolution does not match its pending request",
                    ));
                }
                let command_id = event.causation_id.clone().ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered approval resolution has no causating command",
                    )
                })?;
                resolved_response = Some(recovered_approval_response(
                    &stored.records,
                    &approval_request_id,
                    &command_id,
                    resolution,
                )?);
            }
            AgentEvent::RequestOpened { .. } | AgentEvent::RequestResolved { .. } => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered interaction crossed the observed approval request boundary",
                ));
            }
            _ => {}
        }
    }
    let request = opened_request.ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "observed effect Tool recovery currently requires a durable approval request",
        )
    })?;
    stage_loop_recovery(
        inner,
        stored,
        boundary,
        recovery_events,
        GenericRecoveryContinuation::Approval {
            round,
            request_id,
            request_digest,
            observation,
            call,
            arguments,
            request,
            binding: None,
            committed_response: None,
            resolved_response,
            response: None,
        },
    )
}

pub(super) async fn prepare_recovered_tool(
    inner: &GenericInner,
    run_id: &RunId,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    cancellation: CancellationToken,
) -> Result<GuardedToolResult, AgentFailure> {
    let tools = inner.tools.as_ref().ok_or_else(|| {
        agent_failure(
            "tool_runtime_unavailable",
            "recovered Tool call has no bound Tool Runtime",
            false,
        )
    })?;
    let tool_id = tools
        .runtime
        .resolve_tool_id(&call.name)
        .map_err(|error| {
            agent_failure(
                "tool_runtime_unavailable",
                format!("recovered Tool catalog is unavailable: {error}"),
                true,
            )
        })?
        .ok_or_else(|| {
            agent_failure(
                "tool_not_found",
                "recovered Tool is no longer registered",
                false,
            )
        })?;
    Ok(tools
        .runtime
        .invoke(
            ToolInvocation {
                run_id: run_id.clone(),
                call_id: ToolCallId::new(call.call_id.as_str()),
                tool_id,
                arguments: arguments.clone(),
            },
            tools.run_grant.clone(),
            None,
            cancellation,
        )
        .await)
}

pub(super) async fn recover_committed_tool_outcome(
    inner: &GenericInner,
    run_id: &RunId,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
) -> Result<ToolOutcome, AgentProtocolError> {
    let tools = inner.tools.as_ref().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Tool exchange has no bound Tool Runtime",
        )
    })?;
    let tool_id = tools
        .runtime
        .resolve_tool_id(&call.name)
        .map_err(|error| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                format!("recovered Tool catalog is unavailable: {error}"),
            )
            .with_retryable(true)
        })?
        .ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Tool is no longer registered",
            )
        })?;
    let recovered = tools
        .runtime
        .recover_outcome(
            ToolInvocation {
                run_id: run_id.clone(),
                call_id: ToolCallId::new(call.call_id.as_str()),
                tool_id,
                arguments: arguments.clone(),
            },
            tools.run_grant.clone(),
        )
        .await
        .map_err(|error| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "durable Tool effect cannot validate the recovered Session exchange",
            )
            .with_details(serde_json::json!({
                "code": error.code,
                "message": error.message,
            }))
        })?;
    recovered.ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "Session Tool exchange has no durable Tool outcome",
        )
    })
}
