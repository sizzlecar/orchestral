use super::*;

pub(super) struct RecoveredApprovalPreparation<'a> {
    pub(super) run_id: &'a RunId,
    pub(super) round: u64,
    pub(super) call: &'a GenericObservedToolCall,
    pub(super) arguments: &'a serde_json::Value,
    pub(super) opened_request: &'a PendingRequest,
    pub(super) persisted_response: Option<&'a ApprovalResponse>,
    pub(super) attach_waiter: bool,
    pub(super) cancellation: CancellationToken,
}

pub(super) async fn prepare_recovered_approval(
    inner: &GenericInner,
    preparation: RecoveredApprovalPreparation<'_>,
) -> Result<RecoveredApprovalWaiter, AgentProtocolError> {
    let RecoveredApprovalPreparation {
        run_id,
        round,
        call,
        arguments,
        opened_request,
        persisted_response,
        attach_waiter,
        cancellation,
    } = preparation;
    let tools = inner.tools.as_ref().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered approval has no bound Tool Runtime",
        )
    })?;
    let bridge = tools.approval_bridge.clone().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered approval has no Host approval bridge",
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
                "recovered approval Tool is no longer registered",
            )
        })?;
    let invocation = ToolInvocation {
        run_id: run_id.clone(),
        call_id: ToolCallId::new(call.call_id.as_str()),
        tool_id,
        arguments: arguments.clone(),
    };
    let (binding, summary, replayed_outcome) = match tools
        .runtime
        .invoke(
            invocation.clone(),
            tools.run_grant.clone(),
            None,
            cancellation,
        )
        .await
    {
        GuardedToolResult::ApprovalRequired { binding, summary } => (binding, Some(summary), None),
        GuardedToolResult::Outcome {
            outcome,
            cached: true,
        } => {
            let capability = persisted_response
                .and_then(|response| match &response.resolution {
                    RequestResolution::Approval {
                        decision: ApprovalDecision::Allow,
                        ..
                    } => response.capability.as_ref(),
                    _ => None,
                })
                .ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "recovered Tool outcome has no durable Allow capability",
                    )
                })?;
            let binding = capability.claims.binding.clone();
            let args_digest = invocation.args_digest().map_err(|error| {
                AgentProtocolError::new(AgentProtocolErrorCode::InvalidDigest, error.to_string())
            })?;
            if binding.run_id != invocation.run_id
                || binding.call_id != invocation.call_id
                || binding.tool_id != invocation.tool_id
                || binding.args_digest != args_digest
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "persisted approval capability crossed its recovered Tool invocation",
                ));
            }
            (binding, None, Some(outcome))
        }
        GuardedToolResult::Outcome {
            outcome,
            cached: false,
        } => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovery executed a Tool before validating its durable approval state",
            )
            .with_details(serde_json::to_value(outcome).unwrap_or(serde_json::Value::Null)))
        }
    };
    if binding.run_id != *run_id || binding.call_id.as_str() != call.call_id.as_str() {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "reconstructed approval binding crossed its Run or Tool call",
        ));
    }
    let operation_digest = binding.digest().map_err(|error| {
        AgentProtocolError::new(AgentProtocolErrorCode::InvalidDigest, error.to_string())
    })?;
    let requested_scope = approval_scope_names(&binding).map_err(observed_recovery_error)?;
    let request_id = approval_request_id(run_id, round, &call.call_id);
    // A committed effect replays before the Tool Runtime can re-emit its
    // presentation-only summary. The authority-bearing request fields remain
    // fully derivable from the persisted capability binding.
    let request_matches = opened_request.request_id == request_id
        && opened_request.blocking
        && matches!(
            &opened_request.payload,
            PendingRequestPayload::Approval {
                operation_digest: actual_digest,
                requested_scope: actual_scope,
                session_approval_scope,
                reason,
            } if actual_digest == &operation_digest
                && actual_scope == &requested_scope
                && session_approval_scope == &binding.session_approval_scope
                && summary.as_ref().is_none_or(|expected| reason == expected)
        );
    if !request_matches {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "reconstructed approval does not match the durable pending request",
        ));
    }
    let (responder, response) = if attach_waiter {
        bridge
            .stage(&request_id, binding.clone())
            .await
            .map_err(|error| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    format!("Host approval bridge could not restage the request: {error}"),
                )
                .with_retryable(true)
            })?;
        let (responder, response) = oneshot::channel();
        (Some(responder), Some(response))
    } else {
        (None, None)
    };
    Ok(RecoveredApprovalWaiter {
        request_id,
        binding,
        replayed_outcome,
        responder,
        response,
        bridge,
    })
}

pub(super) fn validate_recovered_input_resolution(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    request_id: &RequestId,
    response: &InputResponse,
) -> Result<(), AgentProtocolError> {
    let mut matching_commands = 0_usize;
    for record in records {
        let GenericCheckpointEvent::CommandCommitted {
            command, outcome, ..
        } = &record.payload
        else {
            continue;
        };
        if command.command_id != response.command_id {
            continue;
        }
        matching_commands = matching_commands.saturating_add(1);
        let matches_resolution = matches!(
            &command.payload,
            AgentCommand::ResolveRequest { response: command_response }
                if command_response == &response.resolution
        );
        if outcome != &ProviderCommandOutcome::Accepted
            || command.request_id.as_ref() != Some(request_id)
            || !matches_resolution
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered input resolution does not match its accepted command",
            ));
        }
    }
    if matching_commands != 1 {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered input resolution has no unique accepted command",
        ));
    }
    Ok(())
}

pub(super) fn recovered_approval_response(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    request_id: &RequestId,
    command_id: &CommandId,
    resolution: &RequestResolution,
) -> Result<ApprovalResponse, AgentProtocolError> {
    let mut matching = None;
    for record in records {
        let GenericCheckpointEvent::CommandCommitted {
            command,
            outcome,
            approval_capability,
        } = &record.payload
        else {
            continue;
        };
        if &command.command_id != command_id {
            continue;
        }
        if matching.is_some() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval resolution has duplicate causating commands",
            ));
        }
        let matches_resolution = matches!(
            &command.payload,
            AgentCommand::ResolveRequest { response } if response == resolution
        );
        if outcome != &ProviderCommandOutcome::Accepted
            || command.request_id.as_ref() != Some(request_id)
            || !matches_resolution
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval resolution does not match its accepted command",
            ));
        }
        let valid_capability = matches!(
            (resolution, approval_capability),
            (
                RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    ..
                },
                Some(_)
            ) | (
                RequestResolution::Approval {
                    decision: ApprovalDecision::Deny,
                    ..
                },
                None
            )
        );
        if !valid_capability {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval resolution has inconsistent capability evidence",
            ));
        }
        matching = Some(ApprovalResponse {
            command_id: command.command_id.clone(),
            resolution: resolution.clone(),
            capability: approval_capability.clone(),
        });
    }
    matching.ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered approval resolution has no unique accepted command",
        )
    })
}
