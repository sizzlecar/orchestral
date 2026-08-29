use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputRequestArguments {
    prompt: String,
}

pub(super) fn parse_input_request(arguments: serde_json::Value) -> Result<String, AgentFailure> {
    let arguments =
        serde_json::from_value::<InputRequestArguments>(arguments).map_err(|error| {
            agent_failure(
                "input_request_arguments_invalid",
                format!("model emitted invalid input request arguments: {error}"),
                false,
            )
        })?;
    if arguments.prompt.trim().is_empty() {
        return Err(agent_failure(
            "input_request_arguments_invalid",
            "input request prompt must not be empty",
            false,
        ));
    }
    Ok(arguments.prompt)
}

pub(super) enum InputWaitOutcome {
    Resolved(serde_json::Value),
    Cancelled,
    Failed(AgentFailure),
}

pub(super) fn input_request_id(
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
) -> RequestId {
    RequestId::new(format!(
        "input:{}:{round}:{}",
        run_id.as_str(),
        model_call_id.as_str()
    ))
}

pub(super) fn approval_request_id(
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
) -> RequestId {
    RequestId::new(format!(
        "approval:{}:{round}:{}",
        run_id.as_str(),
        model_call_id.as_str()
    ))
}

pub(super) async fn await_agent_input(
    inner: Arc<GenericInner>,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    prompt: String,
    cancellation: CancellationToken,
) -> InputWaitOutcome {
    let request_id = input_request_id(run_id, round, model_call_id);
    let (responder, response) = oneshot::channel();
    let registration = {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.runs.get_mut(run_id) {
            None => Err(agent_failure(
                "input_run_missing",
                "Run disappeared before its input request was opened",
                true,
            )),
            Some(run) if run.terminal || run.pending_inputs.contains_key(&request_id) => {
                Err(agent_failure(
                    "input_request_conflict",
                    "input request identity is no longer available",
                    false,
                ))
            }
            Some(run) => {
                run.pending_inputs.insert(
                    request_id.clone(),
                    PendingInput {
                        responder: Some(responder),
                    },
                );
                Ok(())
            }
        }
    };
    if let Err(failure) = registration {
        return InputWaitOutcome::Failed(failure);
    }
    if !publish_durable(
        &inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-input-{round}-{}-opened",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: request_id.clone(),
                    blocking: true,
                    payload: PendingRequestPayload::Input {
                        prompt: vec![Content::text(prompt)],
                        input_schema: None,
                    },
                },
            },
        },
    ) {
        remove_pending_input(&inner, run_id, &request_id);
        return InputWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "input request could not be committed to the private WAL",
            true,
        ));
    }

    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        response = response => Some(response),
    };
    let Some(response) = response else {
        remove_pending_input(&inner, run_id, &request_id);
        return InputWaitOutcome::Cancelled;
    };
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            remove_pending_input(&inner, run_id, &request_id);
            return InputWaitOutcome::Failed(agent_failure(
                "input_waiter_closed",
                "input response channel closed before resolution",
                true,
            ));
        }
    };
    commit_input_response(&inner, run_id, round, model_call_id, request_id, response)
}

pub(super) async fn await_recovered_agent_input(
    inner: Arc<GenericInner>,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    response: oneshot::Receiver<InputResponse>,
    cancellation: CancellationToken,
) -> InputWaitOutcome {
    let request_id = input_request_id(run_id, round, model_call_id);
    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        response = response => Some(response),
    };
    let Some(response) = response else {
        remove_pending_input(&inner, run_id, &request_id);
        return InputWaitOutcome::Cancelled;
    };
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            remove_pending_input(&inner, run_id, &request_id);
            return InputWaitOutcome::Failed(agent_failure(
                "input_waiter_closed",
                "recovered input response channel closed before resolution",
                true,
            ));
        }
    };
    commit_input_response(&inner, run_id, round, model_call_id, request_id, response)
}

pub(super) fn commit_input_response(
    inner: &GenericInner,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    request_id: RequestId,
    response: InputResponse,
) -> InputWaitOutcome {
    let resolution_digest = match response.resolution.digest() {
        Ok(digest) => digest,
        Err(error) => {
            return InputWaitOutcome::Failed(agent_failure(
                "input_resolution_invalid",
                error.to_string(),
                false,
            ));
        }
    };
    if !publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-input-{round}-{}-resolved",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: Some(response.command_id),
            source_fingerprint: None,
            payload: AgentEvent::RequestResolved {
                request_id,
                resolution: response.resolution.clone(),
                resolution_digest,
            },
        },
    ) {
        return InputWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "input resolution could not be committed to the private WAL",
            true,
        ));
    }
    input_resolution_outcome(response.resolution)
}

pub(super) fn input_resolution_outcome(resolution: RequestResolution) -> InputWaitOutcome {
    match input_resolution_result(&resolution) {
        Ok(result) => InputWaitOutcome::Resolved(result),
        Err(failure) => InputWaitOutcome::Failed(failure),
    }
}

pub(super) fn input_resolution_result(
    resolution: &RequestResolution,
) -> Result<serde_json::Value, AgentFailure> {
    match resolution {
        RequestResolution::Input { content } => Ok(serde_json::json!({
            "content": content,
        })),
        _ => Err(agent_failure(
            "input_resolution_invalid",
            "input request received a non-input resolution",
            false,
        )),
    }
}

pub(super) fn observed_input_exchange_messages(
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    result: &serde_json::Value,
) -> (ModelMessage, ModelMessage) {
    observed_tool_exchange_messages(observation, call, arguments, result, false)
}

pub(super) fn observed_tool_exchange_messages(
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    result: &serde_json::Value,
    is_error: bool,
) -> (ModelMessage, ModelMessage) {
    let mut assistant_content = Vec::new();
    if !observation.response.is_empty() {
        assistant_content.push(ModelContent::Text {
            text: observation.response.clone(),
        });
    }
    assistant_content.push(ModelContent::ToolCall {
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        arguments: arguments.clone(),
        extensions: call.extensions.clone(),
    });
    (
        ModelMessage {
            role: ModelRole::Assistant,
            content: assistant_content,
        },
        ModelMessage {
            role: ModelRole::Tool,
            content: vec![ModelContent::ToolResult {
                call_id: call.call_id.clone(),
                result: result.clone(),
                is_error,
            }],
        },
    )
}

pub(super) fn recovered_approval_exchange_messages(
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
    response: &ApprovalResponse,
    replayed_outcome: Option<&ToolOutcome>,
) -> Result<Option<(ModelMessage, ModelMessage)>, AgentProtocolError> {
    let outcome = match &response.resolution {
        RequestResolution::Approval {
            decision: ApprovalDecision::Allow,
            ..
        } => replayed_outcome.cloned(),
        RequestResolution::Approval {
            decision: ApprovalDecision::Deny,
            ..
        } => {
            if replayed_outcome.is_some() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "denied approval unexpectedly has a durable Tool outcome",
                ));
            }
            Some(ToolOutcome::Rejected {
                code: "approval_denied".to_owned(),
                message: "Host denied this Tool invocation".to_owned(),
            })
        }
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval exchange has a non-approval resolution",
            ))
        }
    };
    let Some(outcome) = outcome else {
        return Ok(None);
    };
    if matches!(outcome, ToolOutcome::UnknownEffect { .. }) {
        return Ok(None);
    }
    let (result, is_error) = model_tool_result(outcome);
    Ok(Some(observed_tool_exchange_messages(
        observation,
        call,
        arguments,
        &result,
        is_error,
    )))
}

pub(super) fn remove_pending_input(inner: &GenericInner, run_id: &RunId, request_id: &RequestId) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(run) = state.runs.get_mut(run_id) {
        run.pending_inputs.remove(request_id);
    }
}

pub(super) enum ApprovalWaitOutcome {
    Allowed(ApprovalCapability),
    Denied,
    Cancelled,
    Failed(AgentFailure),
}

pub(super) struct ToolApprovalWaitRequest<'a> {
    pub(super) run_id: &'a RunId,
    pub(super) round: u64,
    pub(super) model_call_id: &'a ModelToolCallId,
    pub(super) binding: ApprovalBinding,
    pub(super) summary: String,
    pub(super) cancellation: CancellationToken,
}

pub(super) async fn await_tool_approval(
    inner: Arc<GenericInner>,
    tools: &GenericTools,
    request: ToolApprovalWaitRequest<'_>,
) -> ApprovalWaitOutcome {
    let ToolApprovalWaitRequest {
        run_id,
        round,
        model_call_id,
        binding,
        summary,
        cancellation,
    } = request;
    let Some(bridge) = tools.approval_bridge.clone() else {
        return ApprovalWaitOutcome::Failed(AgentFailure {
            code: "approval_interaction_not_connected".to_owned(),
            message:
                "Tool requires Host approval, but this Agent has no approval interaction bridge"
                    .to_owned(),
            retryable: false,
            details: serde_json::to_value(binding).unwrap_or(serde_json::Value::Null),
        });
    };
    if binding.run_id != *run_id || binding.call_id.as_str() != model_call_id.as_str() {
        return ApprovalWaitOutcome::Failed(agent_failure(
            "approval_binding_mismatch",
            "Tool Runtime returned an approval binding for another invocation",
            false,
        ));
    }
    let operation_digest = match binding.digest() {
        Ok(digest) => digest,
        Err(error) => {
            return ApprovalWaitOutcome::Failed(agent_failure(
                "approval_binding_invalid",
                error.to_string(),
                false,
            ));
        }
    };
    let requested_scope = match approval_scope_names(&binding) {
        Ok(scopes) => scopes,
        Err(failure) => return ApprovalWaitOutcome::Failed(failure),
    };
    let request_id = approval_request_id(run_id, round, model_call_id);
    if let Err(error) = bridge.stage(&request_id, binding.clone()).await {
        return ApprovalWaitOutcome::Failed(agent_failure(
            "approval_bridge",
            error.to_string(),
            true,
        ));
    }

    let (responder, response) = oneshot::channel();
    let registration = {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.runs.get_mut(run_id) {
            None => Err(agent_failure(
                "approval_run_missing",
                "Run disappeared before its approval request was opened",
                true,
            )),
            Some(run) if run.terminal || run.pending_approvals.contains_key(&request_id) => {
                Err(agent_failure(
                    "approval_request_conflict",
                    "approval request identity is no longer available",
                    false,
                ))
            }
            Some(run) => {
                run.pending_approvals.insert(
                    request_id.clone(),
                    PendingApproval {
                        binding: binding.clone(),
                        responder: Some(responder),
                    },
                );
                Ok(())
            }
        }
    };
    if let Err(failure) = registration {
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Failed(failure);
    }
    if !publish_durable(
        &inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-approval-{round}-{}-opened",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: request_id.clone(),
                    blocking: true,
                    payload: PendingRequestPayload::Approval {
                        operation_digest,
                        requested_scope,
                        reason: summary,
                    },
                },
            },
        },
    ) {
        remove_pending_approval(&inner, run_id, &request_id);
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "approval request could not be committed to the private WAL",
            true,
        ));
    }

    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        response = response => Some(response),
    };
    let Some(response) = response else {
        remove_pending_approval(&inner, run_id, &request_id);
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Cancelled;
    };
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            remove_pending_approval(&inner, run_id, &request_id);
            let _ = bridge.clear(&request_id).await;
            return ApprovalWaitOutcome::Failed(agent_failure(
                "approval_waiter_closed",
                "approval response channel closed before resolution",
                true,
            ));
        }
    };
    commit_approval_response(
        &inner,
        bridge.as_ref(),
        run_id,
        round,
        model_call_id,
        request_id,
        response,
    )
    .await
}

pub(super) async fn commit_approval_response(
    inner: &GenericInner,
    bridge: &dyn AgentApprovalBridge,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    request_id: RequestId,
    response: ApprovalResponse,
) -> ApprovalWaitOutcome {
    let resolution_digest = match response.resolution.digest() {
        Ok(digest) => digest,
        Err(error) => {
            let _ = bridge.clear(&request_id).await;
            return ApprovalWaitOutcome::Failed(agent_failure(
                "approval_resolution_invalid",
                error.to_string(),
                false,
            ));
        }
    };
    if !publish_durable(
        inner,
        run_id,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "generic-{}-approval-{round}-{}-resolved",
                run_id.as_str(),
                model_call_id.as_str()
            )),
            run_id: run_id.clone(),
            causation_id: Some(response.command_id.clone()),
            source_fingerprint: None,
            payload: AgentEvent::RequestResolved {
                request_id: request_id.clone(),
                resolution: response.resolution.clone(),
                resolution_digest,
            },
        },
    ) {
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Failed(agent_failure(
            "generic_checkpoint",
            "approval resolution could not be committed to the private WAL",
            true,
        ));
    }
    if let Err(error) = bridge.clear(&request_id).await {
        return ApprovalWaitOutcome::Failed(agent_failure(
            "approval_bridge",
            error.to_string(),
            true,
        ));
    }
    approval_response_outcome(response)
}

pub(super) fn approval_response_outcome(response: ApprovalResponse) -> ApprovalWaitOutcome {
    match (response.resolution, response.capability) {
        (
            RequestResolution::Approval {
                decision: ApprovalDecision::Allow,
                ..
            },
            Some(capability),
        ) => ApprovalWaitOutcome::Allowed(capability),
        (
            RequestResolution::Approval {
                decision: ApprovalDecision::Deny,
                ..
            },
            None,
        ) => ApprovalWaitOutcome::Denied,
        _ => ApprovalWaitOutcome::Failed(agent_failure(
            "approval_resolution_invalid",
            "approval resolution and capability do not agree",
            false,
        )),
    }
}

pub(super) async fn await_recovered_tool_approval(
    inner: Arc<GenericInner>,
    bridge: Arc<dyn AgentApprovalBridge>,
    run_id: &RunId,
    round: u64,
    model_call_id: &ModelToolCallId,
    response: oneshot::Receiver<ApprovalResponse>,
    cancellation: CancellationToken,
) -> ApprovalWaitOutcome {
    let request_id = approval_request_id(run_id, round, model_call_id);
    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => None,
        response = response => Some(response),
    };
    let Some(response) = response else {
        remove_pending_approval(&inner, run_id, &request_id);
        let _ = bridge.clear(&request_id).await;
        return ApprovalWaitOutcome::Cancelled;
    };
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            remove_pending_approval(&inner, run_id, &request_id);
            let _ = bridge.clear(&request_id).await;
            return ApprovalWaitOutcome::Failed(agent_failure(
                "approval_waiter_closed",
                "recovered approval response channel closed before resolution",
                true,
            ));
        }
    };
    commit_approval_response(
        &inner,
        bridge.as_ref(),
        run_id,
        round,
        model_call_id,
        request_id,
        response,
    )
    .await
}

pub(super) fn remove_pending_approval(
    inner: &GenericInner,
    run_id: &RunId,
    request_id: &RequestId,
) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(run) = state.runs.get_mut(run_id) {
        run.pending_approvals.remove(request_id);
    }
}

pub(super) fn approval_scope_names(binding: &ApprovalBinding) -> Result<Vec<String>, AgentFailure> {
    binding
        .requested_scopes
        .iter()
        .map(|scope| {
            serde_json::to_value(scope)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or_else(|| {
                    agent_failure(
                        "approval_scope_invalid",
                        "Tool effect scope could not be represented in Agent Protocol",
                        false,
                    )
                })
        })
        .collect()
}
