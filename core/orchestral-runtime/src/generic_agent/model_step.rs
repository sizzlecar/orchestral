use super::*;

pub(super) struct ModelRunExecution {
    pub(super) inner: Arc<GenericInner>,
    pub(super) request: AgentStartRequest,
    pub(super) user_message: ModelMessage,
    pub(super) model_messages: Vec<ModelMessage>,
    pub(super) model_tools: Vec<ModelToolDefinition>,
    pub(super) run_skills: Option<Arc<SkillRuntime>>,
    pub(super) seed: GenericExecutionSeed,
    pub(super) cancellation: CancellationToken,
    pub(super) steer_updates: watch::Receiver<u64>,
}

pub(super) async fn execute_model_run(execution: ModelRunExecution) {
    let ModelRunExecution {
        inner,
        request,
        user_message,
        mut model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        mut steer_updates,
    } = execution;
    let run_id = request.run.spec.run_id.clone();
    let GenericExecutionSeed {
        run_started,
        next_model_round,
        mut total_usage,
        mut tool_call_count,
        mut last_response,
        mut supporting_event_ids,
    } = seed;
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    if !run_started
        && !publish_durable(
            &inner,
            &run_id,
            AgentEventDraft {
                event_id: started_event_id.clone(),
                run_id: run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunStarted,
            },
        )
    {
        return;
    }
    if !supporting_event_ids.contains(&started_event_id) {
        supporting_event_ids.push(started_event_id.clone());
    }
    if cancellation.is_cancelled() {
        emit_cancel(&inner, &request, &user_message);
        return;
    }

    let model_step_limit = inner
        .config
        .continuation
        .effective_model_steps(request.run.spec.limits.max_model_steps);
    let tool_call_limit = inner
        .config
        .continuation
        .effective_tool_calls(request.run.spec.limits.max_tool_calls);
    let mut has_usage = total_usage.input_tokens.is_some() || total_usage.output_tokens.is_some();

    'model_rounds: for round in
        std::iter::successors(Some(next_model_round), |round| round.checked_add(1))
    {
        if model_step_limit.is_some_and(|limit| round > limit) {
            emit_limit_reached(
                &inner,
                &request,
                last_response,
                has_usage.then_some(total_usage),
                tool_call_count,
                started_event_id,
                RunLimitKind::ModelSteps,
            );
            return;
        }
        steer_updates.borrow_and_update();
        if cancellation.is_cancelled() {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        if let Err(failure) = commit_queued_steers(&inner, &request, &mut model_messages).await {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        let remaining_input = match remaining_input_tokens(&request, &total_usage) {
            Ok(remaining) => remaining,
            Err(limit) => {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    limit,
                );
                return;
            }
        };
        let output_reserve = match output_reserve_tokens(&inner.config, &request, &total_usage) {
            Ok(reserve) => reserve,
            Err(limit) => {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    limit,
                );
                return;
            }
        };
        let model_context = match project_model_context(
            &inner,
            &request,
            &model_tools,
            run_skills.as_deref(),
            None,
            None,
            ModelContextBudget {
                remaining_input_tokens: remaining_input,
                reserved_output_tokens: Some(inner.config.reserved_output_tokens),
            },
        )
        .await
        {
            Ok(context) => context,
            Err(SessionContextError::ContextOverflow { budget, .. })
                if remaining_input == Some(budget) =>
            {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    RunLimitKind::InputTokens,
                );
                return;
            }
            Err(error) => {
                emit_failure(&inner, &request, &user_message, session_failure(error));
                return;
            }
        };
        let dispatch_budget = match model_dispatch_budget(
            &inner.config,
            &request,
            &total_usage,
            model_context.used_input_tokens,
            output_reserve,
        ) {
            Ok(budget) => budget,
            Err(limit) => {
                emit_limit_reached(
                    &inner,
                    &request,
                    last_response,
                    has_usage.then_some(total_usage),
                    tool_call_count,
                    started_event_id,
                    limit,
                );
                return;
            }
        };
        let context_trace = model_context_trace(&model_context, inner.config.history_limit);
        model_messages = model_context.messages;
        let model_request = model_request_for_round(
            &request,
            round,
            &model_messages,
            &model_tools,
            dispatch_budget.max_output_tokens,
        );
        if let Err(failure) =
            commit_model_attempt(&inner, &run_id, round, &model_request, &context_trace)
        {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        let model_cancellation = cancellation.child_token();
        let mut model_stream = match tokio::select! {
            _ = cancellation.cancelled() => {
                emit_cancel(&inner, &request, &user_message);
                return;
            }
            changed = steer_updates.changed() => {
                if changed.is_err() {
                    emit_failure(
                        &inner,
                        &request,
                        &user_message,
                        agent_failure("steer_channel_closed", "Steer control channel closed", true),
                    );
                    return;
                }
                model_cancellation.cancel();
                if let Err(failure) =
                    commit_queued_steers(&inner, &request, &mut model_messages).await
                {
                    emit_failure(&inner, &request, &user_message, failure);
                    return;
                }
                if let Err(failure) = commit_loop_boundary(
                    &inner,
                    &run_id,
                    round.saturating_add(1),
                    &total_usage,
                    tool_call_count,
                    &last_response,
                    &supporting_event_ids,
                ) {
                    emit_failure(&inner, &request, &user_message, failure);
                    return;
                }
                continue 'model_rounds;
            }
            result = inner.backend.start(model_request.clone(), model_cancellation.clone()) => result,
        } {
            Ok(stream) => stream,
            Err(error) => {
                if cancellation.is_cancelled() {
                    emit_cancel(&inner, &request, &user_message);
                } else {
                    emit_failure(&inner, &request, &user_message, model_failure(error));
                }
                return;
            }
        };

        let mut expected_sequence = 1;
        let mut response = String::new();
        let mut round_usage = None;
        let mut tool_calls = Vec::<PendingModelToolCall>::new();
        loop {
            let item = tokio::select! {
                _ = cancellation.cancelled() => {
                    emit_cancel(&inner, &request, &user_message);
                    return;
                }
                changed = steer_updates.changed() => {
                    if changed.is_err() {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            agent_failure("steer_channel_closed", "Steer control channel closed", true),
                        );
                        return;
                    }
                    model_cancellation.cancel();
                    if let Err(failure) =
                        commit_queued_steers(&inner, &request, &mut model_messages).await
                    {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    if let Err(failure) = commit_loop_boundary(
                        &inner,
                        &run_id,
                        round.saturating_add(1),
                        &total_usage,
                        tool_call_count,
                        &last_response,
                        &supporting_event_ids,
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    continue 'model_rounds;
                }
                item = model_stream.next() => item,
            };
            let event = match item {
                Some(Ok(event)) => event,
                Some(Err(error)) => {
                    emit_failure(&inner, &request, &user_message, model_failure(error));
                    return;
                }
                None => {
                    emit_failure(
                        &inner,
                        &request,
                        &user_message,
                        agent_failure(
                            "model_stream_ended",
                            "model stream ended without Finish",
                            true,
                        ),
                    );
                    return;
                }
            };
            if let Err(error) = event.validate_for(&model_request.request_id, expected_sequence) {
                emit_failure(&inner, &request, &user_message, model_failure(error));
                return;
            }
            expected_sequence += 1;
            match event.payload {
                ModelEvent::TextDelta { delta } => {
                    response.push_str(&delta);
                    publish_telemetry(
                        &inner,
                        &run_id,
                        AgentTelemetryEnvelope {
                            telemetry_id: TelemetryId::new(format!(
                                "generic-{}-round-{round}-delta-{}",
                                run_id.as_str(),
                                event.sequence
                            )),
                            run_id: run_id.clone(),
                            provider_seq: Some(event.sequence),
                            payload: AgentTelemetry::OutputDelta {
                                output_id: OutputId::new(format!(
                                    "generic-{}-response",
                                    run_id.as_str()
                                )),
                                delta: Content::text(delta),
                            },
                        },
                    );
                }
                ModelEvent::ToolCallStart {
                    call_id,
                    name,
                    extensions,
                } => {
                    if tool_calls.iter().any(|call| call.call_id == call_id) {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("duplicate model Tool call id"),
                        );
                        return;
                    }
                    tool_calls.push(PendingModelToolCall {
                        call_id,
                        name,
                        arguments: String::new(),
                        extensions,
                        ended: false,
                    });
                }
                ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                    let Some(call) = tool_calls
                        .iter_mut()
                        .find(|call| call.call_id == call_id && !call.ended)
                    else {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("Tool arguments arrived before start or after end"),
                        );
                        return;
                    };
                    call.arguments.push_str(&delta);
                }
                ModelEvent::ToolCallEnd { call_id } => {
                    let Some(call) = tool_calls
                        .iter_mut()
                        .find(|call| call.call_id == call_id && !call.ended)
                    else {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("Tool call ended without one active start"),
                        );
                        return;
                    };
                    call.ended = true;
                }
                ModelEvent::Usage { usage: observed } => round_usage = Some(observed),
                ModelEvent::Finish { reason } => {
                    let committed_usage = round_usage.take();
                    if let Err(failure) = commit_model_observation(
                        &inner,
                        &run_id,
                        round,
                        &model_request.request_id,
                        GenericModelObservation {
                            finish_reason: reason.clone(),
                            response: response.clone(),
                            usage: committed_usage.clone(),
                            tool_calls: tool_calls
                                .iter()
                                .map(|call| GenericObservedToolCall {
                                    call_id: call.call_id.clone(),
                                    name: call.name.clone(),
                                    arguments: call.arguments.clone(),
                                    extensions: call.extensions.clone(),
                                    ended: call.ended,
                                })
                                .collect(),
                        },
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    if let Err(failure) = validate_observed_usage(
                        &inner.config,
                        &request,
                        &total_usage,
                        committed_usage.as_ref(),
                        dispatch_budget,
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    if let Some(observed) = committed_usage.clone() {
                        merge_usage(&mut total_usage, observed);
                        has_usage = true;
                    }
                    match reason {
                        ModelFinishReason::Stop
                            if tool_calls.is_empty() && !response.is_empty() =>
                        {
                            if let Err(failure) = append_session_event(
                                &inner,
                                AgentSessionEventDraft {
                                    event_id: AgentSessionEventId::new(format!(
                                        "generic-{}-output-{round}",
                                        run_id.as_str(),
                                    )),
                                    session_id: request.run.spec.session_id.clone(),
                                    run_id: run_id.clone(),
                                    payload: AgentSessionEvent::RunOutputCommitted {
                                        request_id: model_request.request_id.clone(),
                                        message: ModelMessage::text(
                                            ModelRole::Assistant,
                                            response.clone(),
                                        ),
                                        usage: committed_usage,
                                    },
                                },
                            )
                            .await
                            {
                                emit_failure(&inner, &request, &user_message, failure);
                                return;
                            }
                            match try_emit_delivery(
                                &inner,
                                &request,
                                response.clone(),
                                has_usage.then_some(total_usage.clone()),
                                tool_call_count,
                                supporting_event_ids.clone(),
                            ) {
                                DeliveryCommit::Committed => {
                                    finish_session(&inner, &request);
                                    return;
                                }
                                DeliveryCommit::SteerPending => {
                                    if let Some(limit) = continuation_limit(
                                        &inner.config,
                                        &request,
                                        &total_usage,
                                        round,
                                        model_step_limit,
                                    ) {
                                        emit_limit_reached(
                                            &inner,
                                            &request,
                                            response,
                                            has_usage.then_some(total_usage),
                                            tool_call_count,
                                            started_event_id,
                                            limit,
                                        );
                                        return;
                                    }
                                    last_response = response.clone();
                                    model_messages
                                        .push(ModelMessage::text(ModelRole::Assistant, response));
                                    if let Err(failure) =
                                        commit_queued_steers(&inner, &request, &mut model_messages)
                                            .await
                                    {
                                        emit_failure(&inner, &request, &user_message, failure);
                                        return;
                                    }
                                    if let Err(failure) = commit_loop_boundary(
                                        &inner,
                                        &run_id,
                                        round.saturating_add(1),
                                        &total_usage,
                                        tool_call_count,
                                        &last_response,
                                        &supporting_event_ids,
                                    ) {
                                        emit_failure(&inner, &request, &user_message, failure);
                                        return;
                                    }
                                    continue 'model_rounds;
                                }
                                DeliveryCommit::InteractionPending => {
                                    emit_failure(
                                        &inner,
                                        &request,
                                        &user_message,
                                        agent_failure(
                                            "turn_not_settled",
                                            "model stopped while a Host input or approval request remained pending",
                                            true,
                                        ),
                                    );
                                    return;
                                }
                                DeliveryCommit::TerminationPending => {
                                    emit_cancel(&inner, &request, &user_message);
                                    return;
                                }
                                DeliveryCommit::CheckpointFailed => return,
                                DeliveryCommit::AlreadyTerminal => return,
                            }
                        }
                        ModelFinishReason::Length => {
                            emit_incomplete(
                                &inner,
                                &request,
                                IncompleteRun {
                                    response,
                                    usage: has_usage.then_some(total_usage),
                                    tool_calls: tool_call_count,
                                    started_event_id,
                                    limit: RunLimitKind::OutputTokens,
                                    unresolved_issue: "model output limit reached",
                                },
                            );
                            return;
                        }
                        ModelFinishReason::Cancelled => {
                            emit_cancel(&inner, &request, &user_message);
                            return;
                        }
                        ModelFinishReason::ToolCalls | ModelFinishReason::Stop
                            if !tool_calls.is_empty() => {}
                        _ => {
                            emit_failure(
                                &inner,
                                &request,
                                &user_message,
                                agent_failure(
                                    "model_incomplete",
                                    format!(
                                        "model ended without a deliverable response: {reason:?}"
                                    ),
                                    false,
                                ),
                            );
                            return;
                        }
                    }

                    if let Some(limit) = continuation_limit(
                        &inner.config,
                        &request,
                        &total_usage,
                        round,
                        model_step_limit,
                    ) {
                        emit_limit_reached(
                            &inner,
                            &request,
                            response,
                            has_usage.then_some(total_usage),
                            tool_call_count,
                            started_event_id,
                            limit,
                        );
                        return;
                    }
                    if tool_calls.iter().any(|call| !call.ended) {
                        emit_failure(
                            &inner,
                            &request,
                            &user_message,
                            model_event_failure("model finished with an incomplete Tool call"),
                        );
                        return;
                    }
                    let mut assistant_content = Vec::new();
                    if !response.is_empty() {
                        last_response = response.clone();
                        assistant_content.push(ModelContent::Text {
                            text: response.clone(),
                        });
                    }
                    let mut parsed_calls = Vec::with_capacity(tool_calls.len());
                    for call in tool_calls {
                        let arguments = match parse_tool_arguments(&call) {
                            Ok(arguments) => arguments,
                            Err(failure) => {
                                emit_failure(&inner, &request, &user_message, failure);
                                return;
                            }
                        };
                        assistant_content.push(ModelContent::ToolCall {
                            call_id: call.call_id.clone(),
                            name: call.name.clone(),
                            arguments: arguments.clone(),
                            extensions: call.extensions.clone(),
                        });
                        parsed_calls.push((call, arguments));
                    }
                    let assistant_message = ModelMessage {
                        role: ModelRole::Assistant,
                        content: assistant_content,
                    };

                    let batch = execute_tool_batch(ToolBatchRequest {
                        inner: inner.clone(),
                        request: request.clone(),
                        user_message: user_message.clone(),
                        run_skills: run_skills.clone(),
                        round,
                        model_request_id: model_request.request_id.clone(),
                        parsed_calls,
                        cancellation: cancellation.clone(),
                        tool_call_count,
                        tool_call_limit,
                        last_response: last_response.clone(),
                        total_usage: total_usage.clone(),
                        has_usage,
                        started_event_id: started_event_id.clone(),
                    })
                    .await;
                    let ToolBatchExecution::Completed {
                        tool_results,
                        retained_artifacts,
                        tool_call_count: observed_tool_call_count,
                        supporting_event_ids: workflow_event_ids,
                    } = batch
                    else {
                        return;
                    };
                    tool_call_count = observed_tool_call_count;
                    supporting_event_ids.extend(workflow_event_ids);
                    let tool_message = ModelMessage {
                        role: ModelRole::Tool,
                        content: tool_results,
                    };
                    if let Err(failure) = append_session_event(
                        &inner,
                        AgentSessionEventDraft {
                            event_id: AgentSessionEventId::new(format!(
                                "generic-{}-tool-exchange-{round}",
                                run_id.as_str()
                            )),
                            session_id: request.run.spec.session_id.clone(),
                            run_id: run_id.clone(),
                            payload: AgentSessionEvent::ToolExchangeCommitted {
                                request_id: model_request.request_id.clone(),
                                assistant: assistant_message.clone(),
                                tool: tool_message.clone(),
                                retained_artifacts,
                                usage: committed_usage,
                            },
                        },
                    )
                    .await
                    {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    model_messages = match project_committed_model_messages(
                        &inner,
                        &request,
                        &model_tools,
                        run_skills.as_deref(),
                    )
                    .await
                    {
                        Ok(messages) => messages,
                        Err(failure) => {
                            emit_failure(&inner, &request, &user_message, failure);
                            return;
                        }
                    };
                    if let Err(failure) = commit_loop_boundary(
                        &inner,
                        &run_id,
                        round.saturating_add(1),
                        &total_usage,
                        tool_call_count,
                        &last_response,
                        &supporting_event_ids,
                    ) {
                        emit_failure(&inner, &request, &user_message, failure);
                        return;
                    }
                    continue 'model_rounds;
                }
                _ => {
                    emit_failure(
                        &inner,
                        &request,
                        &user_message,
                        agent_failure(
                            "unknown_model_event",
                            "model backend emitted an unsupported event",
                            false,
                        ),
                    );
                    return;
                }
            }
        }
    }

    emit_failure(
        &inner,
        &request,
        &user_message,
        agent_failure(
            "model_step_counter_exhausted",
            "model step counter exhausted before the turn settled",
            false,
        ),
    );
}
