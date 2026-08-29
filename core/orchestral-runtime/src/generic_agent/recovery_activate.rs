use super::*;

pub(super) async fn activate_recovery(
    activation: RecoveryActivation,
) -> Result<(), AgentProtocolError> {
    let RecoveryActivation {
        inner,
        request,
        run_id,
        user_message,
        run_skills,
        model_definitions,
        model_output_budgets,
        recovered_attempt_input_budget,
        mut run,
        seed,
        cancellation,
        steer_updates,
        mut continuation,
    } = activation;
    let restore_initial_input = matches!(
        &continuation,
        GenericRecoveryContinuation::ModelLoop {
            restore_initial_input: true
        }
    );
    let initial_input = restore_initial_input.then(|| user_message.clone());
    let model_messages = project_model_messages(
        &inner,
        &request,
        &model_definitions,
        run_skills.as_deref(),
        initial_input,
        None,
        recovered_attempt_input_budget,
    )
    .await
    .map_err(session_context_recovery_error)?;
    let mut session_exchange_committed = false;
    if let GenericRecoveryContinuation::Input {
        round,
        request_id,
        request_digest,
        observation,
        call,
        arguments,
        resolved_response,
        ..
    } = &continuation
    {
        let expected_exchange = resolved_response
            .as_ref()
            .map(|response| {
                input_resolution_result(&response.resolution).map(|result| {
                    observed_input_exchange_messages(observation, call, arguments, &result)
                })
            })
            .transpose()
            .map_err(checkpoint_stream_error)?;
        let session_exchange_seq = recovered_tool_exchange_committed(
            &inner,
            &request,
            *round,
            request_id,
            expected_exchange.as_ref(),
            &[],
            observation.usage.as_ref(),
        )
        .await?;
        session_exchange_committed = session_exchange_seq.is_some();
        let prior_model_messages = if let Some(exchange_seq) = session_exchange_seq {
            let (assistant, tool) = expected_exchange
                .as_ref()
                .expect("a committed input exchange has a private resolved response");
            if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Session input exchange is not the final projected context",
                ));
            }
            Some(
                project_model_messages(
                    &inner,
                    &request,
                    &model_definitions,
                    run_skills.as_deref(),
                    None,
                    Some(exchange_seq.saturating_sub(1)),
                    recovered_attempt_input_budget,
                )
                .await
                .map_err(session_context_recovery_error)?,
            )
        } else {
            None
        };
        let request_messages = prior_model_messages
            .as_deref()
            .unwrap_or(model_messages.as_slice());
        let rebuilt = model_request_for_round(
            &request,
            *round,
            request_messages,
            &model_definitions,
            recovered_model_output_tokens(&model_output_budgets, *round)?,
        );
        if rebuilt.request_id != *request_id
            || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)? != *request_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered model request no longer matches the observed private WAL attempt",
            ));
        }
    }
    let mut staged_approval = None;
    if let GenericRecoveryContinuation::Approval {
        round,
        request_id,
        request_digest,
        observation,
        call,
        arguments,
        request: opened_request,
        binding,
        committed_response,
        resolved_response,
        response,
        ..
    } = &mut continuation
    {
        let persisted_response = committed_response.as_ref().or(resolved_response.as_ref());
        let prepared = prepare_recovered_approval(
            &inner,
            RecoveredApprovalPreparation {
                run_id: &run_id,
                round: *round,
                call,
                arguments,
                opened_request,
                persisted_response,
                attach_waiter: committed_response.is_none() && resolved_response.is_none(),
                cancellation: cancellation.clone(),
            },
        )
        .await?;
        let expected_exchange = match resolved_response.as_ref() {
            Some(resolution) => recovered_approval_exchange_messages(
                observation,
                call,
                arguments,
                resolution,
                prepared.replayed_outcome.as_ref(),
            )?,
            None => None,
        };
        let expected_retained_artifacts = prepared
            .replayed_outcome
            .as_ref()
            .map(retained_artifacts_for_outcome)
            .unwrap_or_default();
        let session_exchange_seq = recovered_tool_exchange_committed(
            &inner,
            &request,
            *round,
            request_id,
            expected_exchange.as_ref(),
            &expected_retained_artifacts,
            observation.usage.as_ref(),
        )
        .await?;
        session_exchange_committed = session_exchange_seq.is_some();
        let prior_model_messages = if let Some(exchange_seq) = session_exchange_seq {
            let (assistant, tool) = expected_exchange
                .as_ref()
                .expect("a committed approval exchange has a recoverable durable result");
            if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Session approval exchange is not the final projected context",
                ));
            }
            Some(
                project_model_messages(
                    &inner,
                    &request,
                    &model_definitions,
                    run_skills.as_deref(),
                    None,
                    Some(exchange_seq.saturating_sub(1)),
                    recovered_attempt_input_budget,
                )
                .await
                .map_err(session_context_recovery_error)?,
            )
        } else {
            None
        };
        let request_messages = prior_model_messages
            .as_deref()
            .unwrap_or(model_messages.as_slice());
        let rebuilt = model_request_for_round(
            &request,
            *round,
            request_messages,
            &model_definitions,
            recovered_model_output_tokens(&model_output_budgets, *round)?,
        );
        if rebuilt.request_id != *request_id
            || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)? != *request_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered approval model request no longer matches the private WAL attempt",
            ));
        }
        if committed_response
            .as_ref()
            .or(resolved_response.as_ref())
            .is_some_and(|response| {
                response
                    .capability
                    .as_ref()
                    .is_some_and(|capability| capability.claims.binding != prepared.binding)
            })
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "persisted approval capability does not match the reconstructed binding",
            ));
        }
        if let Some(responder) = prepared.responder {
            run.pending_approvals.insert(
                prepared.request_id.clone(),
                PendingApproval {
                    binding: prepared.binding.clone(),
                    responder: Some(responder),
                },
            );
            staged_approval = Some((prepared.bridge.clone(), prepared.request_id.clone()));
        }
        *binding = Some(prepared.binding);
        *response = prepared.response;
    }
    if let GenericRecoveryContinuation::Skill {
        round,
        request_id,
        request_digest,
        observation,
        call,
        arguments,
        recovered_observation,
    } = &mut continuation
    {
        let skills = run_skills.as_deref().ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill call has no bound Skill catalog",
            )
        })?;
        let prepared = prepare_recovered_skill(
            &inner,
            &request,
            skills,
            *round,
            request_id,
            observation,
            call,
            arguments,
        )
        .await?;
        if prepared.load_committed {
            let context_message =
                prepared
                    .observation
                    .context_message
                    .clone()
                    .ok_or_else(|| {
                        AgentProtocolError::new(
                            AgentProtocolErrorCode::InvalidDigest,
                            "recovered Skill load has no immutable context message",
                        )
                    })?;
            if model_messages
                .iter()
                .filter(|message| *message == &context_message)
                .count()
                != 1
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Skill load is not uniquely projected into context",
                ));
            }
        }
        if let Some(record) = &prepared.exchange_record {
            let AgentSessionEvent::ToolExchangeCommitted {
                assistant, tool, ..
            } = &record.payload
            else {
                unreachable!("recovered Skill exchange shape was checked");
            };
            if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Skill exchange is not the final projected Session context",
                ));
            }
        }
        let prior_model_messages = if let Some(prior_seq) = prepared.prior_session_seq {
            Some(
                project_model_messages(
                    &inner,
                    &request,
                    &model_definitions,
                    run_skills.as_deref(),
                    None,
                    Some(prior_seq),
                    recovered_attempt_input_budget,
                )
                .await
                .map_err(session_context_recovery_error)?,
            )
        } else {
            None
        };
        let request_messages = prior_model_messages
            .as_deref()
            .unwrap_or(model_messages.as_slice());
        let rebuilt = model_request_for_round(
            &request,
            *round,
            request_messages,
            &model_definitions,
            recovered_model_output_tokens(&model_output_budgets, *round)?,
        );
        if rebuilt.request_id != *request_id
            || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)? != *request_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill model request no longer matches the private WAL attempt",
            ));
        }
        session_exchange_committed = prepared.exchange_record.is_some();
        if prepared.load_committed {
            *recovered_observation = Some(prepared.observation);
        }
    }
    if let GenericRecoveryContinuation::Workflow {
        round,
        request_id,
        request_digest,
        call,
        ..
    } = &continuation
    {
        if inner
            .tools
            .as_ref()
            .and_then(|tools| tools.workflow.as_ref())
            .is_none()
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Workflow call has no bound execution strategy",
            ));
        }
        if recovered_tool_exchange_record(&inner, &request, *round, request_id)
            .await?
            .is_some()
            || run.durable_events.iter().any(|event| {
                event.event_id == workflow_output_event_id(&run_id, *round, &call.call_id)
            })
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "Workflow outcome exists without its private start fence",
            ));
        }
        let rebuilt = model_request_for_round(
            &request,
            *round,
            &model_messages,
            &model_definitions,
            recovered_model_output_tokens(&model_output_budgets, *round)?,
        );
        if rebuilt.request_id != *request_id
            || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)? != *request_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Workflow model request no longer matches the private WAL attempt",
            ));
        }
    }
    if let GenericRecoveryContinuation::WorkflowOutput {
        round,
        request_id,
        request_digest,
        observation,
        call,
        arguments,
        outcome,
        ..
    } = &continuation
    {
        let expected_exchange = observed_tool_exchange_messages(
            observation,
            call,
            arguments,
            &outcome.result,
            outcome.is_error,
        );
        let session_exchange_seq = recovered_tool_exchange_committed(
            &inner,
            &request,
            *round,
            request_id,
            Some(&expected_exchange),
            &[],
            observation.usage.as_ref(),
        )
        .await?;
        session_exchange_committed = session_exchange_seq.is_some();
        let prior_model_messages = if let Some(exchange_seq) = session_exchange_seq {
            let (assistant, tool) = &expected_exchange;
            if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered Workflow exchange is not the final projected Session context",
                ));
            }
            Some(
                project_model_messages(
                    &inner,
                    &request,
                    &model_definitions,
                    run_skills.as_deref(),
                    None,
                    Some(exchange_seq.saturating_sub(1)),
                    recovered_attempt_input_budget,
                )
                .await
                .map_err(session_context_recovery_error)?,
            )
        } else {
            None
        };
        let request_messages = prior_model_messages
            .as_deref()
            .unwrap_or(model_messages.as_slice());
        let rebuilt = model_request_for_round(
            &request,
            *round,
            request_messages,
            &model_definitions,
            recovered_model_output_tokens(&model_output_budgets, *round)?,
        );
        if rebuilt.request_id != *request_id
            || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)? != *request_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Workflow model request no longer matches the private WAL attempt",
            ));
        }
    }
    if let GenericRecoveryContinuation::Tool {
        round,
        request_id,
        request_digest,
        observation,
        call,
        arguments,
        ..
    } = &mut continuation
    {
        let recovered_exchange =
            recovered_tool_exchange_record(&inner, &request, *round, request_id).await?;
        let prior_model_messages = if let Some(record) = &recovered_exchange {
            let AgentSessionEvent::ToolExchangeCommitted {
                assistant, tool, ..
            } = &record.payload
            else {
                unreachable!("recovered Tool exchange shape was checked");
            };
            if !model_messages.ends_with(&[assistant.clone(), tool.clone()]) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "recovered direct Tool exchange is not the final projected context",
                ));
            }
            Some(
                project_model_messages(
                    &inner,
                    &request,
                    &model_definitions,
                    run_skills.as_deref(),
                    None,
                    Some(record.session_seq.saturating_sub(1)),
                    recovered_attempt_input_budget,
                )
                .await
                .map_err(session_context_recovery_error)?,
            )
        } else {
            None
        };
        let request_messages = prior_model_messages
            .as_deref()
            .unwrap_or(model_messages.as_slice());
        let rebuilt = model_request_for_round(
            &request,
            *round,
            request_messages,
            &model_definitions,
            recovered_model_output_tokens(&model_output_budgets, *round)?,
        );
        if rebuilt.request_id != *request_id
            || model_request_digest(&rebuilt).map_err(checkpoint_stream_error)? != *request_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered direct Tool model request no longer matches the private WAL attempt",
            ));
        }
        if let Some(record) = recovered_exchange {
            let outcome = recover_committed_tool_outcome(&inner, &run_id, call, arguments).await?;
            if matches!(outcome, ToolOutcome::UnknownEffect { .. }) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "Session Tool exchange cannot be backed by an unknown effect",
                ));
            }
            let retained_artifacts = retained_artifacts_for_outcome(&outcome);
            let (result, is_error) = model_tool_result(outcome);
            let (assistant, tool) =
                observed_tool_exchange_messages(observation, call, arguments, &result, is_error);
            let expected_payload = AgentSessionEvent::ToolExchangeCommitted {
                request_id: request_id.clone(),
                assistant,
                tool,
                retained_artifacts,
                usage: observation.usage.clone(),
            };
            if record.payload != expected_payload {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidDigest,
                    "direct Tool Session exchange differs from its durable Effect outcome",
                ));
            }
            session_exchange_committed = true;
        }
    }
    let install_result = {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.runs.contains_key(&run_id) {
            Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Generic Agent Run was recovered concurrently",
            ))
        } else if state
            .sessions
            .get(&request.run.spec.session_id)
            .and_then(|session| session.active_run.as_ref())
            .is_some_and(|active| active != &run_id)
        {
            Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "another Generic Agent Run already owns this Session",
            ))
        } else {
            state
                .sessions
                .entry(request.run.spec.session_id.clone())
                .or_default()
                .active_run = Some(run_id.clone());
            state.runs.insert(run_id.clone(), run);
            Ok(())
        }
    };
    if let Err(error) = install_result {
        if let Some((bridge, request_id)) = staged_approval {
            let _ = bridge.clear(&request_id).await;
        }
        return Err(error);
    }

    if restore_initial_input {
        if let Err(failure) =
            commit_loop_boundary(&inner, &run_id, 1, &ModelUsage::default(), 0, "", &[])
        {
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let error = state
                .runs
                .get_mut(&run_id)
                .map(|run| poison_run_after_checkpoint_failure(run, failure.clone()))
                .unwrap_or_else(|| checkpoint_stream_error(failure));
            return Err(error);
        }
    }

    spawn_recovered_continuation(RecoveryDispatch {
        inner,
        request,
        user_message,
        model_messages,
        model_definitions,
        run_skills,
        seed,
        cancellation,
        steer_updates,
        continuation,
        session_exchange_committed,
    });
    Ok(())
}
