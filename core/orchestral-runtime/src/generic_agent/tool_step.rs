use super::*;

pub(super) struct ToolBatchRequest {
    pub(super) inner: Arc<GenericInner>,
    pub(super) request: AgentStartRequest,
    pub(super) user_message: ModelMessage,
    pub(super) run_skills: Option<Arc<SkillRuntime>>,
    pub(super) round: u64,
    pub(super) model_request_id: ModelRequestId,
    pub(super) parsed_calls: Vec<(PendingModelToolCall, serde_json::Value)>,
    pub(super) cancellation: CancellationToken,
    pub(super) tool_call_count: u64,
    pub(super) tool_call_limit: Option<u64>,
    pub(super) last_response: String,
    pub(super) total_usage: ModelUsage,
    pub(super) has_usage: bool,
    pub(super) started_event_id: AgentEventId,
}

pub(super) enum ToolBatchExecution {
    Completed {
        tool_results: Vec<ModelContent>,
        retained_artifacts: Vec<ArtifactRefWithDigest>,
        tool_call_count: u64,
        supporting_event_ids: Vec<AgentEventId>,
    },
    Terminal,
}

pub(super) async fn execute_tool_batch(request: ToolBatchRequest) -> ToolBatchExecution {
    let ToolBatchRequest {
        inner,
        request,
        user_message,
        run_skills,
        round,
        model_request_id,
        parsed_calls,
        cancellation,
        mut tool_call_count,
        tool_call_limit,
        last_response,
        total_usage,
        has_usage,
        started_event_id,
    } = request;
    let run_id = request.run.spec.run_id.clone();
    let mut supporting_event_ids = Vec::new();
    let mut tool_results = Vec::with_capacity(parsed_calls.len());
    let mut retained_artifacts = BTreeMap::<String, ArtifactRefWithDigest>::new();
    for (call, arguments) in parsed_calls {
        let internal_activity_evidence = internal_tool_activity_evidence(&call.name, &arguments);
        if cancellation.is_cancelled() {
            emit_cancel(&inner, &request, &user_message);
            return ToolBatchExecution::Terminal;
        }
        if call.name == REQUEST_INPUT_TOOL_NAME {
            let prompt = match parse_input_request(arguments) {
                Ok(prompt) => prompt,
                Err(failure) => {
                    emit_failure(&inner, &request, &user_message, failure);
                    return ToolBatchExecution::Terminal;
                }
            };
            let result = match await_agent_input(
                inner.clone(),
                &run_id,
                round,
                &call.call_id,
                prompt,
                cancellation.clone(),
            )
            .await
            {
                InputWaitOutcome::Resolved(result) => result,
                InputWaitOutcome::Cancelled => {
                    emit_cancel(&inner, &request, &user_message);
                    return ToolBatchExecution::Terminal;
                }
                InputWaitOutcome::Failed(failure) => {
                    emit_failure(&inner, &request, &user_message, failure);
                    return ToolBatchExecution::Terminal;
                }
            };
            tool_results.push(ModelContent::ToolResult {
                call_id: call.call_id,
                result,
                is_error: false,
            });
            continue;
        }
        if call.name == SKILL_READ_TOOL_NAME {
            let Some(skills) = run_skills.as_ref() else {
                emit_failure(
                    &inner,
                    &request,
                    &user_message,
                    agent_failure(
                        "skill_catalog_unavailable",
                        "model requested a Skill read without a bound Skill catalog",
                        false,
                    ),
                );
                return ToolBatchExecution::Terminal;
            };
            publish_tool_activity(
                &inner,
                &run_id,
                round,
                &call.call_id,
                &call.name,
                ToolActivityState::Running,
                &internal_activity_evidence,
            );
            let observation =
                match execute_skill_read(&inner, &request, skills, round, &call.call_id, arguments)
                    .await
                {
                    Ok(observation) => observation,
                    Err(failure) => {
                        emit_failure(&inner, &request, &user_message, failure);
                        return ToolBatchExecution::Terminal;
                    }
                };
            publish_tool_activity(
                &inner,
                &run_id,
                round,
                &call.call_id,
                &call.name,
                if observation.is_error {
                    ToolActivityState::Failed
                } else {
                    ToolActivityState::Succeeded
                },
                &internal_activity_evidence,
            );
            tool_results.push(ModelContent::ToolResult {
                call_id: call.call_id,
                result: observation.result,
                is_error: observation.is_error,
            });
            continue;
        }
        let Some(tools) = inner.tools.as_ref() else {
            emit_failure(
                &inner,
                &request,
                &user_message,
                agent_failure(
                    "tool_runtime_unavailable",
                    "model requested an effect Tool but this Agent has no Host Tool runtime",
                    false,
                ),
            );
            return ToolBatchExecution::Terminal;
        };
        tool_call_count = match reserve_tool_call(tool_call_count, tool_call_limit) {
            Ok(next) => next,
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
                return ToolBatchExecution::Terminal;
            }
        };
        if call.name == WORKFLOW_TOOL_NAME {
            let remaining_tool_calls =
                tool_call_limit.map(|limit| limit.saturating_sub(tool_call_count));
            if remaining_tool_calls == Some(0) {
                emit_incomplete(
                    &inner,
                    &request,
                    IncompleteRun {
                        response: last_response,
                        usage: has_usage.then_some(total_usage),
                        tool_calls: tool_call_count,
                        started_event_id,
                        limit: RunLimitKind::ToolCalls,
                        unresolved_issue: "Workflow has no remaining Tool call budget",
                    },
                );
                return ToolBatchExecution::Terminal;
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
                return ToolBatchExecution::Terminal;
            }
            publish_tool_activity(
                &inner,
                &run_id,
                round,
                &call.call_id,
                &call.name,
                ToolActivityState::Running,
                &internal_activity_evidence,
            );
            let observation = match execute_workflow_call(
                inner.clone(),
                tools,
                WorkflowCallRequest {
                    run_id: &run_id,
                    call_id: &call.call_id,
                    arguments,
                    remaining_tool_calls,
                    cancellation: cancellation.clone(),
                    recovery_replay: false,
                },
            )
            .await
            {
                WorkflowCallExecution::Observed(observation) => observation,
                WorkflowCallExecution::Cancelled => {
                    emit_cancel(&inner, &request, &user_message);
                    return ToolBatchExecution::Terminal;
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
                        return ToolBatchExecution::Terminal;
                    }
                    emit_failure(
                        &inner,
                        &request,
                        &user_message,
                        agent_failure("tool_unknown_effect", message, false),
                    );
                    return ToolBatchExecution::Terminal;
                }
                WorkflowCallExecution::RecoveryFailed(failure) => {
                    emit_failure(&inner, &request, &user_message, failure);
                    return ToolBatchExecution::Terminal;
                }
            };
            tool_call_count = match reserve_tool_calls(
                tool_call_count,
                observation.tool_calls,
                tool_call_limit,
            ) {
                Ok(next) => next,
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
                    return ToolBatchExecution::Terminal;
                }
            };
            publish_tool_activity(
                &inner,
                &run_id,
                round,
                &call.call_id,
                &call.name,
                if observation.is_error {
                    ToolActivityState::Failed
                } else {
                    ToolActivityState::Succeeded
                },
                &internal_activity_evidence,
            );
            let Some(workflow_event_id) = publish_workflow_output(
                &inner,
                &run_id,
                round,
                &call.call_id,
                observation.result.clone(),
            ) else {
                return ToolBatchExecution::Terminal;
            };
            supporting_event_ids.push(workflow_event_id);
            tool_results.push(ModelContent::ToolResult {
                call_id: call.call_id,
                result: observation.result,
                is_error: observation.is_error,
            });
            continue;
        }
        let tool_id = match tools.runtime.resolve_tool_id(&call.name) {
            Ok(Some(tool_id)) => tool_id,
            Ok(None) => {
                emit_failure(
                    &inner,
                    &request,
                    &user_message,
                    agent_failure(
                        "tool_not_found",
                        format!("model requested an unknown Tool: {}", call.name),
                        false,
                    ),
                );
                return ToolBatchExecution::Terminal;
            }
            Err(error) => {
                emit_failure(
                    &inner,
                    &request,
                    &user_message,
                    agent_failure("tool_runtime_unavailable", error.to_string(), true),
                );
                return ToolBatchExecution::Terminal;
            }
        };
        let invocation = ToolInvocation {
            run_id: run_id.clone(),
            call_id: ToolCallId::new(call.call_id.as_str()),
            tool_id,
            arguments,
        };
        let activity_evidence = tools
            .runtime
            .activity_evidence(&invocation, None)
            .unwrap_or_default();
        publish_tool_activity(
            &inner,
            &run_id,
            round,
            &call.call_id,
            &call.name,
            ToolActivityState::Running,
            &activity_evidence,
        );
        let result = tools
            .runtime
            .invoke(
                invocation.clone(),
                tools.run_grant.clone(),
                None,
                cancellation.clone(),
            )
            .await;
        let result = match result {
            GuardedToolResult::ApprovalRequired { binding, summary } => {
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
                                invocation.clone(),
                                tools.run_grant.clone(),
                                Some(*capability),
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
                        return ToolBatchExecution::Terminal;
                    }
                    ApprovalWaitOutcome::Failed(failure) => {
                        emit_failure(&inner, &request, &user_message, failure);
                        return ToolBatchExecution::Terminal;
                    }
                }
            }
            result => result,
        };
        let terminal_activity_evidence = match &result {
            GuardedToolResult::Outcome { outcome, .. } => {
                let base = tools
                    .runtime
                    .activity_evidence(&invocation, Some(outcome))
                    .unwrap_or_else(|_| activity_evidence.clone());
                tool_terminal_activity_evidence(&base, outcome)
            }
            GuardedToolResult::ApprovalRequired { .. } => activity_evidence.clone(),
        };
        match result {
            GuardedToolResult::ApprovalRequired { binding, .. } => {
                publish_tool_activity(
                    &inner,
                    &run_id,
                    round,
                    &call.call_id,
                    &call.name,
                    ToolActivityState::Failed,
                    &terminal_activity_evidence,
                );
                emit_failure(
                    &inner,
                    &request,
                    &user_message,
                    AgentFailure {
                        code: "approval_capability_rejected".to_owned(),
                        message:
                            "Tool still requires approval after the Host resolved the exact request"
                                .to_owned(),
                        retryable: false,
                        details: serde_json::to_value(binding).unwrap_or(serde_json::Value::Null),
                    },
                );
                return ToolBatchExecution::Terminal;
            }
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::UnknownEffect { message },
                ..
            } if cancellation.is_cancelled() => {
                publish_tool_activity(
                    &inner,
                    &run_id,
                    round,
                    &call.call_id,
                    &call.name,
                    ToolActivityState::Cancelled,
                    &terminal_activity_evidence,
                );
                // The effect journal deliberately retains UnknownEffect, while
                // the Agent Run still observes the user's cancellation as its
                // terminal control outcome. A late Tool result is never accepted.
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
                    return ToolBatchExecution::Terminal;
                }
                emit_cancel(&inner, &request, &user_message);
                return ToolBatchExecution::Terminal;
            }
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::UnknownEffect { message },
                ..
            } => {
                publish_tool_activity(
                    &inner,
                    &run_id,
                    round,
                    &call.call_id,
                    &call.name,
                    ToolActivityState::Failed,
                    &terminal_activity_evidence,
                );
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
                    return ToolBatchExecution::Terminal;
                }
                emit_failure(
                    &inner,
                    &request,
                    &user_message,
                    agent_failure("tool_unknown_effect", message, false),
                );
                return ToolBatchExecution::Terminal;
            }
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Cancelled,
                ..
            } if cancellation.is_cancelled() => {
                publish_tool_activity(
                    &inner,
                    &run_id,
                    round,
                    &call.call_id,
                    &call.name,
                    ToolActivityState::Cancelled,
                    &terminal_activity_evidence,
                );
                emit_cancel(&inner, &request, &user_message);
                return ToolBatchExecution::Terminal;
            }
            GuardedToolResult::Outcome { outcome, .. } => {
                for artifact in retained_artifacts_for_outcome(&outcome) {
                    retained_artifacts.insert(artifact.artifact_ref.as_str().to_owned(), artifact);
                }
                let (result, is_error) = model_tool_result(outcome);
                publish_tool_activity(
                    &inner,
                    &run_id,
                    round,
                    &call.call_id,
                    &call.name,
                    if is_error {
                        ToolActivityState::Failed
                    } else {
                        ToolActivityState::Succeeded
                    },
                    &terminal_activity_evidence,
                );
                tool_results.push(ModelContent::ToolResult {
                    call_id: call.call_id,
                    result,
                    is_error,
                });
            }
        }
    }
    ToolBatchExecution::Completed {
        tool_results,
        retained_artifacts: retained_artifacts.into_values().collect(),
        tool_call_count,
        supporting_event_ids,
    }
}

/// Internal control Tools are owned by the Generic Agent rather than the
/// guarded Tool registry, so their small semantic projection stays here.
fn internal_tool_activity_evidence(
    tool_name: &str,
    arguments: &serde_json::Value,
) -> Vec<ToolActivityEvidence> {
    match tool_name {
        SKILL_READ_TOOL_NAME => note_argument(arguments, "name"),
        WORKFLOW_TOOL_NAME => note_argument(arguments, "workflow_id"),
        _ => Vec::new(),
    }
}

fn note_argument(arguments: &serde_json::Value, name: &str) -> Vec<ToolActivityEvidence> {
    arguments
        .get(name)
        .and_then(serde_json::Value::as_str)
        .and_then(bounded_activity_detail)
        .map(|text| ToolActivityEvidence::Note { text })
        .into_iter()
        .collect()
}

fn tool_terminal_activity_evidence(
    base: &[ToolActivityEvidence],
    outcome: &ToolOutcome,
) -> Vec<ToolActivityEvidence> {
    let (code, message) = match outcome {
        ToolOutcome::Rejected { code, message } | ToolOutcome::Failed { code, message, .. } => {
            (code.as_str(), message.as_str())
        }
        ToolOutcome::UnknownEffect { message } => ("unknown_effect", message.as_str()),
        _ => return base.to_vec(),
    };
    let Some(code) = bounded_activity_detail(code) else {
        return base.to_vec();
    };
    let Some(message) = bounded_activity_detail(message) else {
        return base.to_vec();
    };
    let mut evidence = base.iter().take(15).cloned().collect::<Vec<_>>();
    evidence.push(ToolActivityEvidence::Error { code, message });
    evidence
}

fn bounded_activity_detail(value: &str) -> Option<String> {
    const MAX_CHARS: usize = 240;
    let mut normalized = String::new();
    let mut previous_space = false;
    for character in value.trim().chars() {
        let character = if character.is_control() || character.is_whitespace() {
            ' '
        } else {
            character
        };
        if character == ' ' {
            if previous_space {
                continue;
            }
            previous_space = true;
        } else {
            previous_space = false;
        }
        normalized.push(character);
    }
    if normalized.is_empty() {
        return None;
    }
    if normalized.chars().count() > MAX_CHARS {
        normalized = normalized
            .chars()
            .take(MAX_CHARS - 1)
            .chain(['…'])
            .collect();
    }
    Some(normalized)
}

#[cfg(test)]
mod activity_detail_tests {
    use super::*;

    #[test]
    fn internal_tool_evidence_is_bounded_and_selective() {
        assert_eq!(
            internal_tool_activity_evidence(
                SKILL_READ_TOOL_NAME,
                &serde_json::json!({ "name": "rust-workflow" }),
            ),
            vec![ToolActivityEvidence::Note {
                text: "rust-workflow".to_owned()
            }]
        );
        assert!(internal_tool_activity_evidence(
            "unknown_tool",
            &serde_json::json!({ "secret": "must-not-render" }),
        )
        .is_empty());
    }

    #[test]
    fn terminal_evidence_includes_the_real_tool_error() {
        let evidence = tool_terminal_activity_evidence(
            &[ToolActivityEvidence::File {
                operation: orchestral_core::agent_protocol::wire::ToolFileActivityKind::Create,
                path: "src/new.rs".to_owned(),
                diff: Vec::new(),
                diff_omitted: 0,
            }],
            &ToolOutcome::Rejected {
                code: "patch_invalid".to_owned(),
                message: "Add File lines must start with '+'".to_owned(),
            },
        );
        assert_eq!(
            evidence,
            vec![
                ToolActivityEvidence::File {
                    operation: orchestral_core::agent_protocol::wire::ToolFileActivityKind::Create,
                    path: "src/new.rs".to_owned(),
                    diff: Vec::new(),
                    diff_omitted: 0,
                },
                ToolActivityEvidence::Error {
                    code: "patch_invalid".to_owned(),
                    message: "Add File lines must start with '+'".to_owned(),
                }
            ]
        );
    }
}
