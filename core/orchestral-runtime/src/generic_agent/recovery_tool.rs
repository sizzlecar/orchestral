use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_observed_tool(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    model_messages: Vec<ModelMessage>,
    model_tools: Vec<ModelToolDefinition>,
    run_skills: Option<Arc<SkillRuntime>>,
    mut seed: GenericExecutionSeed,
    cancellation: CancellationToken,
    steer_updates: watch::Receiver<u64>,
    round: u64,
    model_request_id: ModelRequestId,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    session_exchange_committed: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    let Some(tools) = inner.tools.as_ref() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "tool_runtime_unavailable",
                "recovered Tool call has no Host Tool runtime",
                false,
            ),
        );
        return;
    };
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    let tool_call_limit = inner
        .config
        .continuation
        .effective_tool_calls(request.run.spec.limits.max_tool_calls);
    seed.tool_call_count = match reserve_tool_call(seed.tool_call_count, tool_call_limit) {
        Ok(next) => next,
        Err(limit) => {
            let has_usage =
                seed.total_usage.input_tokens.is_some() || seed.total_usage.output_tokens.is_some();
            emit_limit_reached(
                &inner,
                &request,
                seed.last_response,
                has_usage.then_some(seed.total_usage),
                seed.tool_call_count,
                AgentEventId::new(format!("generic-{}-started", run_id.as_str())),
                limit,
            );
            return;
        }
    };

    if session_exchange_committed {
        seed.next_model_round = round.saturating_add(1);
        if let Err(failure) = commit_loop_boundary(
            &inner,
            &run_id,
            seed.next_model_round,
            &seed.total_usage,
            seed.tool_call_count,
            &seed.last_response,
            &seed.supporting_event_ids,
        ) {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
        execute_model_run(ModelRunExecution {
            inner,
            request,
            user_message,
            model_messages,
            model_tools,
            run_skills,
            seed,
            cancellation,
            steer_updates,
        })
        .await;
        return;
    }

    let prepared = match prepare_recovered_tool(
        &inner,
        &run_id,
        &call,
        &arguments,
        cancellation.clone(),
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
    let guarded = match prepared {
        GuardedToolResult::ApprovalRequired { binding, summary } => {
            let invocation = ToolInvocation {
                run_id: run_id.clone(),
                call_id: ToolCallId::new(call.call_id.as_str()),
                tool_id: binding.tool_id.clone(),
                arguments: arguments.clone(),
            };
            match await_tool_approval(
                inner.clone(),
                tools,
                ToolApprovalWaitRequest {
                    run_id: &run_id,
                    round,
                    model_call_id: &call.call_id,
                    binding,
                    summary,
                    cancellation: cancellation.clone(),
                },
            )
            .await
            {
                ApprovalWaitOutcome::Allowed(capability) => {
                    tools
                        .runtime
                        .invoke(
                            invocation,
                            tools.run_grant.clone(),
                            Some(capability),
                            cancellation.clone(),
                        )
                        .await
                }
                ApprovalWaitOutcome::Denied => GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Rejected {
                        code: "approval_denied".to_owned(),
                        message: "Host denied this Tool invocation".to_owned(),
                    },
                    cached: false,
                },
                ApprovalWaitOutcome::Cancelled => {
                    emit_cancel(&inner, &request, &user_message);
                    return;
                }
                ApprovalWaitOutcome::Failed(failure) => {
                    emit_failure(&inner, &request, &user_message, failure);
                    return;
                }
            }
        }
        outcome @ GuardedToolResult::Outcome { .. } => outcome,
    };
    continue_observed_tool(
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
        round,
        model_request_id,
        observation,
        call,
        arguments,
        guarded,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn continue_observed_tool(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    _model_messages: Vec<ModelMessage>,
    model_tools: Vec<ModelToolDefinition>,
    run_skills: Option<Arc<SkillRuntime>>,
    mut seed: GenericExecutionSeed,
    cancellation: CancellationToken,
    steer_updates: watch::Receiver<u64>,
    round: u64,
    model_request_id: ModelRequestId,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    guarded: GuardedToolResult,
) {
    let run_id = request.run.spec.run_id.clone();
    let (result, is_error, retained_artifacts) = match guarded {
        GuardedToolResult::ApprovalRequired { binding, .. } => {
            emit_failure(
                &inner,
                &request,
                &user_message,
                AgentFailure {
                    code: "approval_capability_rejected".to_owned(),
                    message:
                        "Tool still requires approval after recovery resolved the exact request"
                            .to_owned(),
                    retryable: false,
                    details: serde_json::to_value(binding).unwrap_or(serde_json::Value::Null),
                },
            );
            return;
        }
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { message },
            ..
        } if cancellation.is_cancelled() => {
            if let Err(failure) = append_effect_uncertainty(
                &inner,
                &request,
                round,
                &call.call_id,
                &call.name,
                &message,
            )
            .await
            {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { message },
            ..
        } => {
            if let Err(failure) = append_effect_uncertainty(
                &inner,
                &request,
                round,
                &call.call_id,
                &call.name,
                &message,
            )
            .await
            {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
            emit_failure(
                &inner,
                &request,
                &user_message,
                agent_failure("tool_unknown_effect", message, false),
            );
            return;
        }
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Cancelled,
            ..
        } if cancellation.is_cancelled() => {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        GuardedToolResult::Outcome { outcome, .. } => {
            let retained_artifacts = retained_artifacts_for_outcome(&outcome);
            let (result, is_error) = model_tool_result(outcome);
            (result, is_error, retained_artifacts)
        }
    };
    let (assistant_message, tool_message) =
        observed_tool_exchange_messages(&observation, &call, &arguments, &result, is_error);
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
                request_id: model_request_id,
                assistant: assistant_message.clone(),
                tool: tool_message.clone(),
                retained_artifacts,
                usage: observation.usage,
            },
        },
    )
    .await
    {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    let model_messages = match project_committed_model_messages(
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
    seed.next_model_round = round.saturating_add(1);
    if let Err(failure) = commit_loop_boundary(
        &inner,
        &run_id,
        seed.next_model_round,
        &seed.total_usage,
        seed.tool_call_count,
        &seed.last_response,
        &seed.supporting_event_ids,
    ) {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    execute_model_run(ModelRunExecution {
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    })
    .await;
}

pub(super) async fn recovered_tool_exchange_record(
    inner: &GenericInner,
    request: &AgentStartRequest,
    round: u64,
    request_id: &ModelRequestId,
) -> Result<Option<AgentSessionRecord>, AgentProtocolError> {
    let records = inner
        .session_journal
        .load_session(&request.run.spec.session_id)
        .await
        .map_err(|error| session_context_recovery_error(SessionContextError::Journal(error)))?;
    recovered_tool_exchange_record_from(&records, request, round, request_id)
}

fn recovered_tool_exchange_record_from(
    records: &[AgentSessionRecord],
    request: &AgentStartRequest,
    round: u64,
    request_id: &ModelRequestId,
) -> Result<Option<AgentSessionRecord>, AgentProtocolError> {
    let mut matching = records
        .iter()
        .filter(|record| match &record.payload {
            AgentSessionEvent::ToolExchangeCommitted {
                request_id: actual, ..
            }
            | AgentSessionEvent::RunOutputCommitted {
                request_id: actual, ..
            } => actual == request_id,
            _ => false,
        })
        .cloned()
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered model attempt has multiple Session outcomes",
        ));
    }
    let Some(record) = matching.pop() else {
        return Ok(None);
    };
    let expected_event_id = AgentSessionEventId::new(format!(
        "generic-{}-tool-exchange-{round}",
        request.run.spec.run_id.as_str()
    ));
    if record.run_id != request.run.spec.run_id
        || record.session_id != request.run.spec.session_id
        || record.event_id != expected_event_id
        || !matches!(
            &record.payload,
            AgentSessionEvent::ToolExchangeCommitted {
                request_id: actual,
                ..
            } if actual == request_id
        )
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Session Tool exchange crossed its Run or model attempt",
        ));
    }
    Ok(Some(record))
}

pub(super) async fn recovered_tool_exchange_committed(
    inner: &GenericInner,
    request: &AgentStartRequest,
    round: u64,
    request_id: &ModelRequestId,
    expected_messages: Option<&(ModelMessage, ModelMessage)>,
    retained_artifacts: &[ArtifactRefWithDigest],
    usage: Option<&ModelUsage>,
) -> Result<Option<u64>, AgentProtocolError> {
    let Some(record) = recovered_tool_exchange_record(inner, request, round, request_id).await?
    else {
        return Ok(None);
    };
    let Some((assistant, tool)) = expected_messages else {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "Session contains a Tool exchange without a recoverable durable result",
        ));
    };
    let expected_payload = AgentSessionEvent::ToolExchangeCommitted {
        request_id: request_id.clone(),
        assistant: assistant.clone(),
        tool: tool.clone(),
        retained_artifacts: retained_artifacts.to_vec(),
        usage: usage.cloned(),
    };
    if record.payload != expected_payload {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Session Tool exchange does not match the private model observation",
        ));
    }
    Ok(Some(record.session_seq))
}

pub(super) struct RecoveredSkillPreparation {
    pub(super) observation: SkillCallObservation,
    pub(super) load_committed: bool,
    pub(super) exchange_record: Option<AgentSessionRecord>,
    pub(super) prior_session_seq: Option<u64>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_recovered_skill(
    inner: &GenericInner,
    request: &AgentStartRequest,
    skills: &SkillRuntime,
    round: u64,
    request_id: &ModelRequestId,
    observation: &GenericModelObservation,
    call: &GenericObservedToolCall,
    arguments: &serde_json::Value,
) -> Result<RecoveredSkillPreparation, AgentProtocolError> {
    let records = inner
        .session_journal
        .load_session(&request.run.spec.session_id)
        .await
        .map_err(|error| session_context_recovery_error(SessionContextError::Journal(error)))?;
    let expected_load_id = skill_load_event_id(&request.run.spec.run_id, round, &call.call_id);
    let load_record = records
        .iter()
        .find(|record| record.event_id == expected_load_id)
        .cloned();
    if load_record.as_ref().is_some_and(|record| {
        record.run_id != request.run.spec.run_id
            || record.session_id != request.run.spec.session_id
            || !matches!(record.payload, AgentSessionEvent::SkillLoaded { .. })
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill load event crossed its Run or has the wrong shape",
        ));
    }
    let exchange_record =
        recovered_tool_exchange_record_from(&records, request, round, request_id)?;
    if load_record
        .as_ref()
        .zip(exchange_record.as_ref())
        .is_some_and(|(load, exchange)| load.session_seq >= exchange.session_seq)
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill load was not committed before its Tool exchange",
        ));
    }
    let final_outcome_seq = exchange_record
        .as_ref()
        .map(|record| record.session_seq)
        .or_else(|| load_record.as_ref().map(|record| record.session_seq));
    if final_outcome_seq.is_some_and(|sequence| {
        records
            .last()
            .is_none_or(|record| record.session_seq != sequence)
    }) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "recovered Skill outcome is not the final Session record",
        ));
    }
    let first_outcome_seq = load_record
        .as_ref()
        .map(|record| record.session_seq)
        .into_iter()
        .chain(exchange_record.as_ref().map(|record| record.session_seq))
        .min();
    let prior_records = records
        .iter()
        .filter(|record| first_outcome_seq.is_none_or(|first| record.session_seq < first))
        .cloned()
        .collect::<Vec<_>>();
    let loaded = LoadedSkillSet::replay(&prior_records).map_err(|error| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            format!("recovered Skill state is invalid: {error}"),
        )
    })?;
    let evaluation = evaluate_skill_read(skills, arguments.clone(), &loaded);
    match (&load_record, &evaluation.load) {
        (
            Some(AgentSessionRecord {
                payload: AgentSessionEvent::SkillLoaded { load },
                ..
            }),
            Some(expected),
        ) if load.as_ref() == expected => {}
        (Some(_), _) => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill load differs from the observed model call",
            ))
        }
        (None, Some(_)) if exchange_record.is_some() => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill Tool exchange is missing its load event",
            ))
        }
        _ => {}
    }
    if let Some(record) = &exchange_record {
        let (assistant, tool) = observed_tool_exchange_messages(
            observation,
            call,
            arguments,
            &evaluation.observation.result,
            evaluation.observation.is_error,
        );
        let expected = AgentSessionEvent::ToolExchangeCommitted {
            request_id: request_id.clone(),
            assistant,
            tool,
            retained_artifacts: Vec::new(),
            usage: observation.usage.clone(),
        };
        if record.payload != expected {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "recovered Skill Tool exchange differs from its load outcome",
            ));
        }
    }
    Ok(RecoveredSkillPreparation {
        observation: evaluation.observation,
        load_committed: load_record.is_some(),
        exchange_record,
        prior_session_seq: first_outcome_seq.map(|sequence| sequence.saturating_sub(1)),
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_observed_input(
    inner: Arc<GenericInner>,
    request: AgentStartRequest,
    user_message: ModelMessage,
    mut model_messages: Vec<ModelMessage>,
    model_tools: Vec<ModelToolDefinition>,
    run_skills: Option<Arc<SkillRuntime>>,
    mut seed: GenericExecutionSeed,
    cancellation: CancellationToken,
    steer_updates: watch::Receiver<u64>,
    round: u64,
    model_request_id: ModelRequestId,
    observation: GenericModelObservation,
    call: GenericObservedToolCall,
    arguments: serde_json::Value,
    prompt: String,
    request_open: bool,
    committed_response: Option<InputResponse>,
    resolved_response: Option<InputResponse>,
    session_exchange_committed: bool,
    response: Option<oneshot::Receiver<InputResponse>>,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    let input = if let Some(resolved_response) = resolved_response {
        input_resolution_outcome(resolved_response.resolution)
    } else if let Some(committed_response) = committed_response {
        commit_input_response(
            &inner,
            &run_id,
            round,
            &call.call_id,
            input_request_id(&run_id, round, &call.call_id),
            committed_response,
        )
    } else if request_open {
        await_recovered_agent_input(
            inner.clone(),
            &run_id,
            round,
            &call.call_id,
            response.expect("reattached input request owns a response channel"),
            cancellation.clone(),
        )
        .await
    } else {
        await_agent_input(
            inner.clone(),
            &run_id,
            round,
            &call.call_id,
            prompt,
            cancellation.clone(),
        )
        .await
    };
    let result = match input {
        InputWaitOutcome::Resolved(result) => result,
        InputWaitOutcome::Cancelled => {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        InputWaitOutcome::Failed(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
    let (assistant_message, tool_message) =
        observed_input_exchange_messages(&observation, &call, &arguments, &result);
    if !session_exchange_committed {
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
                    request_id: model_request_id,
                    assistant: assistant_message.clone(),
                    tool: tool_message.clone(),
                    retained_artifacts: Vec::new(),
                    usage: observation.usage,
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
    }
    seed.next_model_round = round.saturating_add(1);
    if let Err(failure) = commit_loop_boundary(
        &inner,
        &run_id,
        seed.next_model_round,
        &seed.total_usage,
        seed.tool_call_count,
        &seed.last_response,
        &seed.supporting_event_ids,
    ) {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    execute_model_run(ModelRunExecution {
        inner,
        request,
        user_message,
        model_messages,
        model_tools,
        run_skills,
        seed,
        cancellation,
        steer_updates,
    })
    .await;
}
