use super::*;

pub(super) struct RecoveryActivation {
    pub(super) inner: Arc<GenericInner>,
    pub(super) request: AgentStartRequest,
    pub(super) run_id: RunId,
    pub(super) user_message: ModelMessage,
    pub(super) run_skills: Option<Arc<SkillRuntime>>,
    pub(super) model_definitions: Vec<ModelToolDefinition>,
    pub(super) model_output_budgets: BTreeMap<u64, Option<u64>>,
    pub(super) recovered_attempt_input_budget: Option<u64>,
    pub(super) run: GenericRun,
    pub(super) seed: GenericExecutionSeed,
    pub(super) cancellation: CancellationToken,
    pub(super) steer_updates: watch::Receiver<u64>,
    pub(super) continuation: GenericRecoveryContinuation,
}

pub(super) fn recovered_model_output_tokens(
    budgets: &BTreeMap<u64, Option<u64>>,
    round: u64,
) -> Result<Option<u64>, AgentProtocolError> {
    budgets.get(&round).copied().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered model attempt has no durable output-budget reservation",
        )
    })
}

/// Closes a Run whose process disappeared after the model request was
/// durably opened but before its outcome was observed.
///
/// Retrying that model request could duplicate work, while leaving the Run
/// permanently Unknown prevents every later Run in the same Session. The
/// only safe forward transition is therefore an explicit incomplete terminal.
/// Its private-WAL commit remains behind the Host recovery confirmation gate,
/// just like reconstructed executable work.
pub(super) fn stage_interrupted_model_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    round: u64,
    request_id: ModelRequestId,
    recovery_events: Vec<AgentEventDraft>,
) -> Result<AgentRecovery, AgentProtocolError> {
    let run_id = stored.registration.execution.run_id.clone();
    let expected_previous = stored.last_checkpoint_seq();
    let terminal = AgentEventDraft {
        event_id: AgentEventId::new(format!(
            "generic-{}-model-attempt-interrupted-{round}",
            run_id.as_str()
        )),
        run_id: run_id.clone(),
        causation_id: None,
        source_fingerprint: None,
        payload: AgentEvent::RunIncomplete {
            reason: IncompleteReason::Interrupted {
                reason: "Host restarted before the model attempt outcome was observed".to_owned(),
            },
            partial_delivery: None,
        },
    };
    terminal.validate_integrity()?;

    let (sender, _) = broadcast::channel(inner.config.stream_buffer);
    let receiver = sender.subscribe();
    let replay = stream::iter(
        recovery_events
            .into_iter()
            .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft)))),
    );
    let live = stream::unfold(receiver, |mut receiver| async move {
        match receiver.recv().await {
            Ok(item) => Some((item, receiver)),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Some((
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::SequenceGap,
                    format!("Generic Agent recovery stream lagged by {skipped}"),
                )),
                receiver,
            )),
            Err(broadcast::error::RecvError::Closed) => None,
        }
    });
    let recovery_stream = replay.chain(live).boxed();
    let checkpoint_store = inner.checkpoint_store.clone();
    let confirmation_run_id = run_id.clone();
    let confirmation_event = terminal.clone();
    let confirmation = async move {
        checkpoint_store
            .append(
                &confirmation_run_id,
                expected_previous,
                GenericCheckpointDraft {
                    event_id: GenericCheckpointEventId::new(format!(
                        "generic-{}-model-attempt-interrupted-{round}",
                        confirmation_run_id.as_str()
                    )),
                    run_id: confirmation_run_id.clone(),
                    payload: GenericCheckpointEvent::ProviderEventsCommitted {
                        events: vec![confirmation_event.clone()],
                    },
                },
            )
            .map_err(checkpoint_recovery_error)?;
        sender
            .send(Ok(AgentProviderStreamItem::Event(Box::new(
                confirmation_event,
            ))))
            .map_err(|_| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    "Host detached before the interrupted model Run was closed",
                )
                .with_retryable(true)
                .with_details(serde_json::json!({
                    "boundary": "model_attempt_open",
                    "round": round,
                    "request_id": request_id,
                }))
            })?;
        Ok(())
    };
    Ok(AgentRecovery::staged(recovery_stream, confirmation))
}

pub(super) fn stage_loop_recovery(
    inner: Arc<GenericInner>,
    stored: StoredGenericAgentRun,
    boundary: GenericLoopBoundary,
    recovery_events: Vec<AgentEventDraft>,
    mut continuation: GenericRecoveryContinuation,
) -> Result<AgentRecovery, AgentProtocolError> {
    let model_output_budgets = stored
        .records
        .iter()
        .filter_map(|record| match &record.payload {
            GenericCheckpointEvent::ModelAttemptStarted {
                round,
                max_output_tokens,
                ..
            } => Some((*round, *max_output_tokens)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let checkpoint_seq = stored.last_checkpoint_seq();
    let registration = stored.registration;
    let request = registration.request.clone();
    let execution = registration.execution.clone();
    let admission = registration.admission.clone();
    let run_id = execution.run_id.clone();
    let recovered_attempt_input_budget = request
        .run
        .spec
        .limits
        .max_input_tokens
        .map(|limit| limit.saturating_sub(boundary.usage.input_tokens.unwrap_or(0)));
    let user_message = agent_input_message(&request)?;
    let run_skills = resolve_recovery_skill_binding(&inner, &registration)?;
    let model_definitions = model_definitions_for_run(&inner, run_skills.is_some());
    let (commands, queued_steers, mut pending_resolutions) =
        reconstruct_recovery_commands(&stored.records, &recovery_events)?;
    match &mut continuation {
        GenericRecoveryContinuation::ModelLoop { .. } if !pending_resolutions.is_empty() => {
            let pending = pending_resolutions
                .values()
                .next()
                .expect("non-empty pending resolution map was checked");
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "stable recovery cannot apply an accepted request resolution",
            )
            .with_details(serde_json::json!({
                "boundary": "accepted_resolution_pending",
                "command_id": pending.command_id,
            })));
        }
        GenericRecoveryContinuation::Skill { .. }
        | GenericRecoveryContinuation::Workflow { .. }
        | GenericRecoveryContinuation::WorkflowOutput { .. }
        | GenericRecoveryContinuation::Tool { .. }
            if !pending_resolutions.is_empty() =>
        {
            let pending = pending_resolutions
                .values()
                .next()
                .expect("non-empty pending resolution map was checked");
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "direct Tool recovery cannot apply an accepted request resolution",
            )
            .with_details(serde_json::json!({
                "boundary": "accepted_resolution_pending",
                "command_id": pending.command_id,
            })));
        }
        GenericRecoveryContinuation::Input {
            round,
            call,
            request_open,
            committed_response,
            resolved_response,
            ..
        } => {
            let expected_request_id = input_request_id(&run_id, *round, &call.call_id);
            if let Some(recovered) = pending_resolutions.remove(&expected_request_id) {
                if recovered.capability.is_some() {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "input resolution unexpectedly carried an approval capability",
                    ));
                }
                *committed_response = Some(InputResponse {
                    command_id: recovered.command_id,
                    resolution: recovered.resolution,
                });
            }
            if !pending_resolutions.is_empty() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "accepted request resolution crossed the recovered input boundary",
                ));
            }
            if committed_response.is_some() && resolved_response.is_some() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered input resolution was both pending and already applied",
                ));
            }
            if let Some(response) = committed_response.as_ref().or(resolved_response.as_ref()) {
                if !*request_open || !matches!(response.resolution, RequestResolution::Input { .. })
                {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "accepted resolution does not match the recovered pending input request",
                    ));
                }
            }
        }
        GenericRecoveryContinuation::Approval {
            round,
            call,
            committed_response,
            resolved_response,
            ..
        } => {
            let expected_request_id = approval_request_id(&run_id, *round, &call.call_id);
            if let Some(recovered) = pending_resolutions.remove(&expected_request_id) {
                let valid = matches!(
                    (&recovered.resolution, &recovered.capability),
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
                if !valid {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "accepted approval resolution has inconsistent capability evidence",
                    ));
                }
                *committed_response = Some(ApprovalResponse {
                    command_id: recovered.command_id,
                    resolution: recovered.resolution,
                    capability: recovered.capability,
                });
            }
            if !pending_resolutions.is_empty() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "accepted request resolution crossed the recovered approval boundary",
                ));
            }
            if committed_response.is_some() && resolved_response.is_some() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered approval resolution was both pending and already applied",
                ));
            }
        }
        GenericRecoveryContinuation::ModelLoop { .. }
        | GenericRecoveryContinuation::Skill { .. }
        | GenericRecoveryContinuation::Workflow { .. }
        | GenericRecoveryContinuation::WorkflowOutput { .. }
        | GenericRecoveryContinuation::Tool { .. } => {}
    }
    let mut pending_inputs = BTreeMap::new();
    if let GenericRecoveryContinuation::Input {
        round,
        call,
        request_open: true,
        committed_response: None,
        resolved_response: None,
        response,
        ..
    } = &mut continuation
    {
        let request_id = input_request_id(&run_id, *round, &call.call_id);
        let (responder, receiver) = oneshot::channel();
        pending_inputs.insert(
            request_id,
            PendingInput {
                responder: Some(responder),
            },
        );
        *response = Some(receiver);
    }
    let run_started = recovery_events
        .iter()
        .any(|event| matches!(&event.payload, AgentEvent::RunStarted));
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    let mut supporting_event_ids = boundary.supporting_event_ids;
    if run_started && !supporting_event_ids.contains(&started_event_id) {
        supporting_event_ids.push(started_event_id);
    }
    let seed = GenericExecutionSeed {
        run_started,
        next_model_round: boundary.next_model_round,
        total_usage: boundary.usage,
        tool_call_count: boundary.tool_call_count,
        last_response: boundary.last_response,
        supporting_event_ids,
    };
    let (sender, _) = broadcast::channel(inner.config.stream_buffer);
    let cancellation = CancellationToken::new();
    let stop_cause = Arc::new(AtomicU8::new(RUN_STOP_RUNNING));
    arm_run_deadline(
        request.run.spec.limits.deadline_unix_ms,
        cancellation.clone(),
        stop_cause.clone(),
    );
    let (steer_signal, steer_updates) = watch::channel(0_u64);
    let run = GenericRun {
        request: request.clone(),
        execution: execution.clone(),
        admission: admission.clone(),
        durable_events: recovery_events,
        sender,
        terminal: false,
        cancellation: cancellation.clone(),
        stop_cause,
        cancel_command: None,
        commands,
        queued_steers,
        steer_signal,
        pending_inputs,
        pending_approvals: BTreeMap::new(),
        checkpoint_seq,
    };
    let replay = InternalGenericAgentProvider::stream_for(&run);

    let activation = RecoveryActivation {
        inner,
        request,
        run_id,
        user_message,
        run_skills,
        model_definitions,
        model_output_budgets,
        recovered_attempt_input_budget,
        run,
        seed,
        cancellation,
        steer_updates,
        continuation,
    };
    Ok(AgentRecovery::staged(replay, activate_recovery(activation)))
}
