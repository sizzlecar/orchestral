use super::*;

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
