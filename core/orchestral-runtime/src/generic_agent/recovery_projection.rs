use super::*;

pub(super) fn resolve_recovery_skill_binding(
    inner: &GenericInner,
    registration: &GenericAgentRunRegistration,
) -> Result<Option<Arc<SkillRuntime>>, AgentProtocolError> {
    let mut skipped = registration.admission.skipped_optional_bindings.clone();
    let skills =
        resolve_run_skill_binding(inner.skills.as_ref(), &registration.request, &mut skipped)
            .map_err(recovery_start_error)?;
    if skipped != registration.admission.skipped_optional_bindings {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovery resource admission does not match the immutable start",
        ));
    }
    Ok(skills)
}

pub(super) type RecoveredCommandProjection = (
    BTreeMap<CommandId, StoredCommand>,
    VecDeque<QueuedSteer>,
    BTreeMap<RequestId, RecoveredResolution>,
);

pub(super) fn reconstruct_recovery_commands(
    records: &[crate::generic_agent_checkpoint::GenericCheckpointRecord],
    recovery_events: &[AgentEventDraft],
) -> Result<RecoveredCommandProjection, AgentProtocolError> {
    let applied_commands = recovery_events
        .iter()
        .filter_map(|event| {
            matches!(
                &event.payload,
                AgentEvent::InputCommitted { .. }
                    | AgentEvent::RequestResolved { .. }
                    | AgentEvent::StopRequested { .. }
            )
            .then(|| event.causation_id.clone())
            .flatten()
        })
        .collect::<BTreeSet<_>>();
    let mut commands = BTreeMap::new();
    let mut queued_steers = VecDeque::new();
    let mut pending_resolutions = BTreeMap::new();
    for record in records {
        let GenericCheckpointEvent::CommandCommitted {
            command,
            outcome,
            approval_capability,
        } = &record.payload
        else {
            continue;
        };
        commands.insert(
            command.command_id.clone(),
            StoredCommand {
                digest: command.command_digest.clone(),
                outcome: outcome.clone(),
            },
        );
        if outcome != &ProviderCommandOutcome::Accepted {
            continue;
        }
        match &command.payload {
            AgentCommand::Cancel { .. } => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "stable recovery cannot restart a Run with an accepted cancellation",
                )
                .with_details(serde_json::json!({
                    "boundary": "accepted_cancel_pending",
                    "command_id": command.command_id,
                })))
            }
            AgentCommand::Steer { content } if !applied_commands.contains(&command.command_id) => {
                queued_steers.push_back(QueuedSteer {
                    command_id: command.command_id.clone(),
                    content: content.clone(),
                    message: agent_content_message(content)?,
                })
            }
            AgentCommand::ResolveRequest { response }
                if !applied_commands.contains(&command.command_id) =>
            {
                let request_id = command.request_id.clone().ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "accepted request resolution has no request identity",
                    )
                })?;
                if pending_resolutions
                    .insert(
                        request_id,
                        RecoveredResolution {
                            command_id: command.command_id.clone(),
                            resolution: response.clone(),
                            capability: approval_capability.clone(),
                        },
                    )
                    .is_some()
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "multiple accepted resolutions target the same pending request",
                    ));
                }
            }
            AgentCommand::Steer { .. } | AgentCommand::ResolveRequest { .. } => {}
            _ => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::Unsupported,
                    "stable recovery encountered an unsupported accepted command",
                ))
            }
        }
    }
    Ok((commands, queued_steers, pending_resolutions))
}

pub(super) fn validate_execution_and_duplicate(
    run: &GenericRun,
    execution: &AgentExecutionRef,
    command: &AgentCommandEnvelope,
) -> Result<(), AgentProtocolError> {
    if run.execution != *execution {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::RunIdConflict,
            "execution reference does not match the Generic Agent Run",
        ));
    }
    if let Some(existing) = run.commands.get(&command.command_id) {
        if existing.digest != command.command_digest {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::DuplicateConflict,
                "command_id was reused with different content",
            ));
        }
    }
    Ok(())
}

pub(super) fn record_command(
    inner: &GenericInner,
    run: &mut GenericRun,
    command: &AgentCommandEnvelope,
    outcome: ProviderCommandOutcome,
) -> Result<ProviderCommandDisposition, AgentProtocolError> {
    record_command_with_approval(inner, run, command, outcome, None)
}

pub(super) fn record_command_with_approval(
    inner: &GenericInner,
    run: &mut GenericRun,
    command: &AgentCommandEnvelope,
    outcome: ProviderCommandOutcome,
    approval_capability: Option<ApprovalCapability>,
) -> Result<ProviderCommandDisposition, AgentProtocolError> {
    let disposition = ProviderCommandDisposition {
        command_id: command.command_id.clone(),
        run_id: command.run_id.clone(),
        outcome: outcome.clone(),
        duplicate: false,
    };
    let durable_disposition = disposition.to_event_draft()?;
    if let Err(failure) = append_checkpoint_to_run(
        inner,
        run,
        &command.run_id,
        GenericCheckpointEventId::new(format!(
            "generic-{}-command-{}",
            command.run_id.as_str(),
            command.command_id.as_str()
        )),
        GenericCheckpointEvent::CommandCommitted {
            command: command.clone(),
            outcome: outcome.clone(),
            approval_capability,
        },
    ) {
        return Err(poison_run_after_checkpoint_failure(run, failure));
    }
    run.commands.insert(
        command.command_id.clone(),
        StoredCommand {
            digest: command.command_digest.clone(),
            outcome: outcome.clone(),
        },
    );
    run.durable_events.push(durable_disposition);
    Ok(disposition)
}
