use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_observed_workflow_output(
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
    outcome: WorkflowCallObservation,
    workflow_event_id: AgentEventId,
    session_exchange_committed: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    seed.tool_call_count = seed
        .tool_call_count
        .saturating_add(1)
        .saturating_add(outcome.tool_calls);
    if !seed.supporting_event_ids.contains(&workflow_event_id) {
        seed.supporting_event_ids.push(workflow_event_id);
    }
    if !session_exchange_committed {
        let (assistant_message, tool_message) = observed_tool_exchange_messages(
            &observation,
            &call,
            &arguments,
            &outcome.result,
            outcome.is_error,
        );
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
                    assistant: assistant_message,
                    tool: tool_message,
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_observed_workflow(
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
    recovery_replay: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
    let tool_call_limit = request
        .run
        .spec
        .limits
        .max_tool_calls
        .unwrap_or(inner.config.max_tool_calls)
        .min(inner.config.max_tool_calls);
    let has_usage =
        seed.total_usage.input_tokens.is_some() || seed.total_usage.output_tokens.is_some();
    let started_event_id = AgentEventId::new(format!("generic-{}-started", run_id.as_str()));
    seed.tool_call_count = match reserve_tool_call(seed.tool_call_count, tool_call_limit) {
        Ok(next) => next,
        Err(limit) => {
            emit_limit_reached(
                &inner,
                &request,
                seed.last_response,
                has_usage.then_some(seed.total_usage),
                seed.tool_call_count,
                started_event_id,
                limit,
            );
            return;
        }
    };
    let remaining_tool_calls = tool_call_limit.saturating_sub(seed.tool_call_count);
    if remaining_tool_calls == 0 {
        emit_incomplete(
            &inner,
            &request,
            IncompleteRun {
                response: seed.last_response,
                usage: has_usage.then_some(seed.total_usage),
                tool_calls: seed.tool_call_count,
                started_event_id,
                limit: RunLimitKind::ToolCalls,
                unresolved_issue: "Workflow has no remaining Tool call budget",
            },
        );
        return;
    }
    if let Err(failure) = commit_workflow_attempt_started(
        &inner,
        &run_id,
        round,
        &model_request_id,
        &call.call_id,
        &call.arguments,
    ) {
        emit_failure(&inner, &request, &user_message, failure);
        return;
    }
    let Some(tools) = inner.tools.as_ref() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "tool_runtime_unavailable",
                "recovered Workflow has no Host Tool runtime",
                false,
            ),
        );
        return;
    };
    let workflow_observation = match execute_workflow_call(
        inner.clone(),
        tools,
        WorkflowCallRequest {
            run_id: &run_id,
            call_id: &call.call_id,
            arguments: arguments.clone(),
            remaining_tool_calls,
            cancellation: cancellation.clone(),
            recovery_replay,
        },
    )
    .await
    {
        WorkflowCallExecution::Observed(observation) => observation,
        WorkflowCallExecution::Cancelled => {
            emit_cancel(&inner, &request, &user_message);
            return;
        }
        WorkflowCallExecution::UnknownEffect(message) => {
            if let Err(failure) = append_effect_uncertainty(
                &inner,
                &request,
                round,
                &call.call_id,
                WORKFLOW_TOOL_NAME,
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
        WorkflowCallExecution::RecoveryFailed(failure) => {
            emit_failure(&inner, &request, &user_message, failure);
            return;
        }
    };
    seed.tool_call_count = seed
        .tool_call_count
        .saturating_add(workflow_observation.tool_calls);
    let Some(workflow_event_id) = publish_workflow_output(
        &inner,
        &run_id,
        round,
        &call.call_id,
        workflow_observation.result.clone(),
    ) else {
        return;
    };
    seed.supporting_event_ids.push(workflow_event_id);
    let (assistant_message, tool_message) = observed_tool_exchange_messages(
        &observation,
        &call,
        &arguments,
        &workflow_observation.result,
        workflow_observation.is_error,
    );
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
                assistant: assistant_message,
                tool: tool_message,
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_observed_skill(
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
    recovered_observation: Option<SkillCallObservation>,
    session_exchange_committed: bool,
) {
    let run_id = request.run.spec.run_id.clone();
    if let Some(usage) = observation.usage.clone() {
        merge_usage(&mut seed.total_usage, usage);
    }
    if !observation.response.is_empty() {
        seed.last_response = observation.response.clone();
    }
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
    let skill_observation = if let Some(observation) = recovered_observation {
        observation
    } else {
        let Some(skills) = run_skills.as_deref() else {
            emit_failure(
                &inner,
                &request,
                &user_message,
                agent_failure(
                    "skill_catalog_unavailable",
                    "recovered Skill load has no bound Skill catalog",
                    false,
                ),
            );
            return;
        };
        match execute_skill_read(
            &inner,
            &request,
            skills,
            round,
            &call.call_id,
            arguments.clone(),
        )
        .await
        {
            Ok(observation) => observation,
            Err(failure) => {
                emit_failure(&inner, &request, &user_message, failure);
                return;
            }
        }
    };
    let (assistant_message, tool_message) = observed_tool_exchange_messages(
        &observation,
        &call,
        &arguments,
        &skill_observation.result,
        skill_observation.is_error,
    );
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

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_observed_approval(
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
    binding: ApprovalBinding,
    committed_response: Option<ApprovalResponse>,
    resolved_response: Option<ApprovalResponse>,
    session_exchange_committed: bool,
    response: Option<oneshot::Receiver<ApprovalResponse>>,
) {
    let run_id = request.run.spec.run_id.clone();
    let Some(tools) = inner.tools.as_ref() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "tool_runtime_unavailable",
                "recovered approval has no Host Tool runtime",
                false,
            ),
        );
        return;
    };
    let Some(bridge) = tools.approval_bridge.clone() else {
        emit_failure(
            &inner,
            &request,
            &user_message,
            agent_failure(
                "approval_interaction_not_connected",
                "recovered approval has no Host approval bridge",
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
    seed.tool_call_count = seed.tool_call_count.saturating_add(1);

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

    let approval = if let Some(resolved_response) = resolved_response {
        approval_response_outcome(resolved_response)
    } else if let Some(committed_response) = committed_response {
        commit_approval_response(
            &inner,
            bridge.as_ref(),
            &run_id,
            round,
            &call.call_id,
            approval_request_id(&run_id, round, &call.call_id),
            committed_response,
        )
        .await
    } else {
        await_recovered_tool_approval(
            inner.clone(),
            bridge,
            &run_id,
            round,
            &call.call_id,
            response.expect("pending recovered approval owns a response channel"),
            cancellation.clone(),
        )
        .await
    };
    let invocation = ToolInvocation {
        run_id: run_id.clone(),
        call_id: ToolCallId::new(call.call_id.as_str()),
        tool_id: binding.tool_id.clone(),
        arguments: arguments.clone(),
    };
    let guarded = match approval {
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
