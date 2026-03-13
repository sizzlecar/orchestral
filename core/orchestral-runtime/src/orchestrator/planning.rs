use super::*;
use orchestral_core::planner::ActionCall;
use orchestral_core::types::{Plan, Step};

impl Orchestrator {
    pub(super) async fn run_planning_pipeline(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let turn_started_at = Instant::now();
        let actions = self.available_actions().await;
        let mut history = self
            .history_for_planner(interaction_id.as_str(), task.id.as_str())
            .await?;
        drop_current_turn_user_input(&mut history, &task.intent.content);
        self.emit_lifecycle_event(
            "planning_started",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("planning started"),
            serde_json::json!({
                "available_actions": actions.len(),
                "history_items": history.len(),
            }),
        )
        .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            available_actions = actions.len(),
            history_items = history.len(),
            "orchestrator planning started"
        );
        let skill_instructions = self.skill_catalog.build_instructions(&task.intent.content);
        let runtime_info = PlannerRuntimeInfo::detect();
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions.clone());
        let planner_started_at = Instant::now();
        let planner_output = self.planner.plan(&task.intent, &context).await?;
        let planner_elapsed_ms = planner_started_at.elapsed().as_millis() as u64;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            planner_elapsed_ms = planner_elapsed_ms,
            "orchestrator planner latency"
        );
        match planner_output {
            PlannerOutput::SkeletonChoice(choice) => {
                if !self.config.reactor_enabled {
                    return Err(OrchestratorError::Planner(PlanError::Generation(
                        "planner returned skeleton_choice while runtime.reactor.enabled=false"
                            .to_string(),
                    )));
                }
                let stage_choice = resolve_stage_choice_from_skeleton_choice(
                    choice,
                    None,
                    self.config.reactor_default_derivation_policy,
                    true,
                )?;
                self.run_reactor_pipeline(interaction_id, started_kind, task, stage_choice)
                    .await
            }
            PlannerOutput::StageChoice(choice) => {
                if !self.config.reactor_enabled {
                    return Err(OrchestratorError::Planner(PlanError::Generation(
                        "planner returned stage_choice while runtime.reactor.enabled=false"
                            .to_string(),
                    )));
                }
                let choice = normalize_reactor_stage_choice(choice)?;
                self.run_reactor_pipeline(interaction_id, started_kind, task, choice)
                    .await
            }
            PlannerOutput::ActionCall(call) => {
                let action_meta = validate_action_call(&call, &context.available_actions)?;
                let plan = build_action_call_plan(&task.intent.content, &call);
                task.set_plan(plan.clone());
                task.start_executing();
                self.task_store.save(&task).await?;

                self.emit_lifecycle_event(
                    "planning_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "action_call",
                        "action": call.action,
                        "step_count": 1,
                        "steps": summarize_plan_steps(&plan),
                    }),
                )
                .await;
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution started"),
                    serde_json::json!({
                        "step_count": 1,
                        "execution_mode": "action_call",
                        "action": action_meta.name,
                    }),
                )
                .await;

                let result = self
                    .execute_existing_task(&mut task, interaction_id.as_str(), None)
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution completed"),
                    {
                        let mut meta = execution_result_metadata(&result);
                        if let Some(obj) = meta.as_object_mut() {
                            obj.insert(
                                "execution_mode".to_string(),
                                serde_json::Value::String("action_call".to_string()),
                            );
                            obj.insert(
                                "action".to_string(),
                                serde_json::Value::String(action_meta.name.clone()),
                            );
                        }
                        meta
                    },
                )
                .await;
                self.emit_interpreted_output(interaction_id.as_str(), &task, &result)
                    .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
                    "orchestrator action_call latency"
                );

                let response = match started_kind {
                    "started" => OrchestratorResult::Started {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                    _ => OrchestratorResult::Merged {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                };
                Ok(response)
            }
            PlannerOutput::DirectResponse(message) => {
                self.emit_lifecycle_event(
                    "planning_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "direct_response",
                        "step_count": 0,
                    }),
                )
                .await;
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution skipped"),
                    serde_json::json!({
                        "step_count": 0,
                        "execution_mode": "direct_response",
                    }),
                )
                .await;

                let result = ExecutionResult::Completed;
                task.set_state(TaskState::Done);
                self.task_store.save(&task).await?;
                self.thread_runtime
                    .update_interaction_state(interaction_id.as_str(), InteractionState::Completed)
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution completed"),
                    serde_json::json!({
                        "status": "completed",
                        "execution_mode": "direct_response",
                    }),
                )
                .await;
                self.emit_assistant_output_message(
                    interaction_id.as_str(),
                    task.id.as_str(),
                    message,
                    Value::Null,
                    Some(serde_json::json!({
                        "status": "completed",
                        "execution_mode": "direct_response",
                    })),
                )
                .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
                    "orchestrator direct_response latency"
                );

                let response = match started_kind {
                    "started" => OrchestratorResult::Started {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                    _ => OrchestratorResult::Merged {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                };
                Ok(response)
            }
            PlannerOutput::Clarification(question) => {
                self.emit_lifecycle_event(
                    "planning_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "clarification",
                        "step_count": 0,
                    }),
                )
                .await;
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution skipped"),
                    serde_json::json!({
                        "step_count": 0,
                        "execution_mode": "clarification",
                    }),
                )
                .await;

                let result = ExecutionResult::WaitingUser {
                    step_id: "planner".into(),
                    prompt: question.clone(),
                    approval: None,
                };
                task.wait_for_user(question.clone());
                self.task_store.save(&task).await?;
                self.thread_runtime
                    .update_interaction_state(
                        interaction_id.as_str(),
                        InteractionState::WaitingUser,
                    )
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution completed"),
                    serde_json::json!({
                        "status": "clarification",
                        "prompt": truncate_for_log(&question, 400),
                        "waiting_kind": "input",
                        "execution_mode": "clarification",
                    }),
                )
                .await;
                self.emit_assistant_output_message(
                    interaction_id.as_str(),
                    task.id.as_str(),
                    question,
                    Value::Null,
                    Some(serde_json::json!({
                        "status": "clarification",
                        "waiting_kind": "input",
                        "execution_mode": "clarification",
                    })),
                )
                .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
                    "orchestrator clarification latency"
                );

                let response = match started_kind {
                    "started" => OrchestratorResult::Started {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                    _ => OrchestratorResult::Merged {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                };
                Ok(response)
            }
        }
    }
}

const ACTION_CALL_ALLOWLIST: &[&str] = &["shell", "file_read", "http"];

fn validate_action_call<'a>(
    call: &ActionCall,
    available_actions: &'a [ActionMeta],
) -> Result<&'a ActionMeta, OrchestratorError> {
    if call.action.starts_with("reactor_") || call.action.starts_with("mcp__") {
        return Err(OrchestratorError::Planner(PlanError::Generation(format!(
            "action_call does not allow action '{}'",
            call.action
        ))));
    }

    if !ACTION_CALL_ALLOWLIST.contains(&call.action.as_str()) {
        return Err(OrchestratorError::Planner(PlanError::Generation(format!(
            "action_call '{}' is outside the direct-action allowlist",
            call.action
        ))));
    }

    available_actions
        .iter()
        .find(|meta| meta.name == call.action)
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(format!(
                "action_call references unavailable action '{}'",
                call.action
            )))
        })
}

fn build_action_call_plan(intent: &str, call: &ActionCall) -> Plan {
    let step = Step::action("action_call", call.action.clone()).with_params(call.params.clone());
    let mut plan = Plan::with_confidence(
        call.reason
            .clone()
            .unwrap_or_else(|| format!("execute direct action for intent: {}", intent)),
        vec![step],
        1.0,
    );
    plan.on_complete = action_call_on_complete_template(&call.action);
    plan.on_failure = Some("Action failed: {{error}}".to_string());
    plan
}

fn action_call_on_complete_template(action: &str) -> Option<String> {
    match action {
        "shell" => Some("{{action_call.stdout}}".to_string()),
        "file_read" => Some("{{action_call.content}}".to_string()),
        "http" => Some("{{action_call.body}}".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::planner::ActionCall;
    use serde_json::json;

    #[test]
    fn test_validate_action_call_rejects_reactor_action() {
        let available = vec![ActionMeta::new(
            "reactor_spreadsheet_inspect",
            "Inspect spreadsheet",
        )];
        let err = validate_action_call(
            &ActionCall {
                action: "reactor_spreadsheet_inspect".to_string(),
                params: json!({}),
                reason: None,
            },
            &available,
        )
        .expect_err("should reject reactor action");
        assert!(err
            .to_string()
            .contains("action_call does not allow action"));
    }

    #[test]
    fn test_validate_action_call_rejects_unavailable_action() {
        let available = vec![ActionMeta::new("shell", "Run shell")];
        let err = validate_action_call(
            &ActionCall {
                action: "http".to_string(),
                params: json!({}),
                reason: None,
            },
            &available,
        )
        .expect_err("should reject unavailable action");
        assert!(err.to_string().contains("unavailable action 'http'"));
    }

    #[test]
    fn test_build_action_call_plan_uses_single_step_template() {
        let plan = build_action_call_plan(
            "list docs",
            &ActionCall {
                action: "shell".to_string(),
                params: json!({"command":"find ./docs -maxdepth 1 -type f"}),
                reason: Some("list docs".to_string()),
            },
        );
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].id.as_str(), "action_call");
        assert_eq!(plan.steps[0].action, "shell");
        assert_eq!(plan.on_complete.as_deref(), Some("{{action_call.stdout}}"));
        assert_eq!(plan.on_failure.as_deref(), Some("Action failed: {{error}}"));
    }
}
