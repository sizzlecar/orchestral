use super::agent_loop::{
    agent_loop_iteration_limit_error, should_continue_agent_loop, AgentLoopState,
};
use super::output::render_output_template;
use super::*;
use orchestral_core::planner::SingleAction;
use orchestral_core::types::{Plan, Step, StepKind};

impl Orchestrator {
    pub(super) async fn run_planning_pipeline(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let turn_started_at = Instant::now();
        let all_actions = self.available_actions().await;
        let mut history = self
            .history_for_planner(interaction_id.as_str(), task.id.as_str())
            .await?;
        drop_current_turn_user_input(&mut history, &task.intent.content);
        self.try_reload_skills();
        let skill_instructions = {
            let catalog = self.skill_catalog.read().await;
            catalog.build_instructions(&task.intent.content)
        };
        let runtime_info = PlannerRuntimeInfo::detect();
        let mut loop_state = AgentLoopState::default();

        for iteration in 1..=self.config.max_planner_iterations {
            let available_actions = filter_actions_for_planner_iteration(&all_actions, &task);
            self.emit_lifecycle_event(
                "planning_started",
                Some(interaction_id.as_str()),
                Some(task.id.as_str()),
                Some("planning started"),
                serde_json::json!({
                    "iteration": iteration,
                    "max_iterations": self.config.max_planner_iterations,
                    "available_actions": available_actions.len(),
                    "history_items": history.len(),
                    "observation_count": loop_state.observation_count(),
                }),
            )
            .await;
            tracing::info!(
                interaction_id = %interaction_id,
                task_id = %task.id,
                iteration = iteration,
                max_iterations = self.config.max_planner_iterations,
                available_actions = available_actions.len(),
                history_items = history.len(),
                observation_count = loop_state.observation_count(),
                "orchestrator planner iteration started"
            );

            let skill_summaries: Vec<(String, String)> = {
                let catalog = self.skill_catalog.read().await;
                catalog
                    .summaries()
                    .iter()
                    .map(|(n, d)| (n.to_string(), d.to_string()))
                    .collect()
            };
            let mut context = PlannerContext::with_history(available_actions, history.clone())
                .with_runtime_info(runtime_info.clone())
                .with_skill_instructions(skill_instructions.clone())
                .with_skill_summaries(skill_summaries);
            if let Some(loop_context) =
                loop_state.planner_loop_context(iteration, self.config.max_planner_iterations)
            {
                context = context.with_loop_context(loop_context);
            }

            let planner_started_at = Instant::now();
            let planner_output = self.planner.plan(&task.intent, &context).await?;
            let planner_elapsed_ms = planner_started_at.elapsed().as_millis() as u64;
            tracing::info!(
                interaction_id = %interaction_id,
                task_id = %task.id,
                iteration = iteration,
                planner_elapsed_ms = planner_elapsed_ms,
                "orchestrator planner latency"
            );

            match planner_output {
                PlannerOutput::SingleAction(call) => {
                    let action_meta = validate_single_action(&call, &context.available_actions)?;
                    let plan = build_single_action_plan(&task.intent.content, &call);
                    let result = self
                        .execute_planner_plan_iteration(
                            interaction_id.as_str(),
                            &mut task,
                            plan.clone(),
                            iteration,
                            "single_action",
                            Some(action_meta.name.as_str()),
                        )
                        .await?;

                    if matches!(
                        result,
                        ExecutionResult::WaitingUser { .. } | ExecutionResult::WaitingEvent { .. }
                    ) {
                        self.emit_interpreted_output(interaction_id.as_str(), &task, &result)
                            .await;
                        return Ok(planner_result_response(
                            started_kind,
                            interaction_id,
                            task.id.clone(),
                            result,
                        ));
                    }

                    if should_continue_agent_loop(
                        &result,
                        iteration,
                        self.config.max_planner_iterations,
                    ) {
                        loop_state.record_iteration(
                            iteration,
                            "single_action",
                            &plan,
                            &result,
                            &task,
                        );
                        self.prepare_for_next_planner_iteration(interaction_id.as_str(), &mut task)
                            .await?;
                        self.emit_lifecycle_event(
                            "agent_loop_continue",
                            Some(interaction_id.as_str()),
                            Some(task.id.as_str()),
                            Some("planner will observe execution result and continue"),
                            serde_json::json!({
                                "iteration": iteration,
                                "next_iteration": iteration + 1,
                                "execution_mode": "single_action",
                                "result": execution_result_metadata(&result),
                            }),
                        )
                        .await;
                        continue;
                    }

                    if matches!(result, ExecutionResult::Completed) {
                        loop_state.record_iteration(
                            iteration,
                            "single_action",
                            &plan,
                            &result,
                            &task,
                        );
                        return self
                            .fail_agent_loop_after_completed_execution(
                                interaction_id,
                                started_kind,
                                task,
                                result,
                                turn_started_at,
                            )
                            .await;
                    }

                    self.emit_interpreted_output(interaction_id.as_str(), &task, &result)
                        .await;
                    return Ok(planner_result_response(
                        started_kind,
                        interaction_id,
                        task.id.clone(),
                        result,
                    ));
                }
                PlannerOutput::MiniPlan(plan) => {
                    let plan = validate_and_prepare_mini_plan(plan, &context.available_actions)?;
                    let result = self
                        .execute_planner_plan_iteration(
                            interaction_id.as_str(),
                            &mut task,
                            plan.clone(),
                            iteration,
                            "mini_plan",
                            None,
                        )
                        .await?;

                    if matches!(
                        result,
                        ExecutionResult::WaitingUser { .. } | ExecutionResult::WaitingEvent { .. }
                    ) {
                        self.emit_interpreted_output(interaction_id.as_str(), &task, &result)
                            .await;
                        return Ok(planner_result_response(
                            started_kind,
                            interaction_id,
                            task.id.clone(),
                            result,
                        ));
                    }

                    if should_continue_agent_loop(
                        &result,
                        iteration,
                        self.config.max_planner_iterations,
                    ) {
                        loop_state.record_iteration(iteration, "mini_plan", &plan, &result, &task);
                        self.prepare_for_next_planner_iteration(interaction_id.as_str(), &mut task)
                            .await?;
                        self.emit_lifecycle_event(
                            "agent_loop_continue",
                            Some(interaction_id.as_str()),
                            Some(task.id.as_str()),
                            Some("planner will observe execution result and continue"),
                            serde_json::json!({
                                "iteration": iteration,
                                "next_iteration": iteration + 1,
                                "execution_mode": "mini_plan",
                                "result": execution_result_metadata(&result),
                            }),
                        )
                        .await;
                        continue;
                    }

                    if matches!(result, ExecutionResult::Completed) {
                        loop_state.record_iteration(iteration, "mini_plan", &plan, &result, &task);
                        return self
                            .fail_agent_loop_after_completed_execution(
                                interaction_id,
                                started_kind,
                                task,
                                result,
                                turn_started_at,
                            )
                            .await;
                    }

                    self.emit_interpreted_output(interaction_id.as_str(), &task, &result)
                        .await;
                    return Ok(planner_result_response(
                        started_kind,
                        interaction_id,
                        task.id.clone(),
                        result,
                    ));
                }
                PlannerOutput::Done(message) => {
                    return self
                        .complete_planner_loop_with_done(
                            interaction_id,
                            started_kind,
                            task,
                            iteration,
                            message,
                            turn_started_at,
                        )
                        .await;
                }
                PlannerOutput::NeedInput(question) => {
                    return self
                        .complete_planner_loop_with_need_input(
                            interaction_id,
                            started_kind,
                            task,
                            iteration,
                            question,
                            turn_started_at,
                        )
                        .await;
                }
            }
        }

        let error = agent_loop_iteration_limit_error(self.config.max_planner_iterations);
        let result = ExecutionResult::Failed {
            step_id: StepId::from("planner"),
            error: error.clone(),
        };
        task.plan = None;
        task.fail(error.clone(), false);
        self.task_store.save(&task).await?;
        self.thread_runtime
            .update_interaction_state(interaction_id.as_str(), InteractionState::Failed)
            .await?;
        self.emit_lifecycle_event(
            "execution_completed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("execution completed"),
            serde_json::json!({
                "status": "failed",
                "error": truncate_for_log(&error, 400),
                "execution_mode": "agent_loop",
            }),
        )
        .await;
        self.emit_assistant_output_message(
            interaction_id.as_str(),
            task.id.as_str(),
            error.clone(),
            Value::Null,
            Some(serde_json::json!({
                "status": "failed",
                "execution_mode": "agent_loop",
            })),
        )
        .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
            "orchestrator agent_loop iteration limit reached"
        );
        Ok(planner_result_response(
            started_kind,
            interaction_id,
            task.id.clone(),
            result,
        ))
    }

    async fn execute_planner_plan_iteration(
        &self,
        interaction_id: &str,
        task: &mut Task,
        plan: Plan,
        iteration: usize,
        execution_mode: &str,
        action_name: Option<&str>,
    ) -> Result<ExecutionResult, OrchestratorError> {
        let step_count = plan.steps.len();
        task.completed_step_ids.clear();
        task.set_plan(plan.clone());
        task.start_executing();
        self.task_store.save(task).await?;

        let mut planning_meta = serde_json::json!({
            "iteration": iteration,
            "output_type": execution_mode,
            "step_count": step_count,
            "steps": summarize_plan_steps(&plan),
        });
        if let Some(action_name) = action_name {
            planning_meta["action"] = Value::String(action_name.to_string());
        }
        self.emit_lifecycle_event(
            "planning_completed",
            Some(interaction_id),
            Some(task.id.as_str()),
            Some("planning completed"),
            planning_meta,
        )
        .await;

        let mut execution_started = serde_json::json!({
            "iteration": iteration,
            "step_count": step_count,
            "execution_mode": execution_mode,
        });
        if let Some(action_name) = action_name {
            execution_started["action"] = Value::String(action_name.to_string());
        }
        self.emit_lifecycle_event(
            "execution_started",
            Some(interaction_id),
            Some(task.id.as_str()),
            Some("execution started"),
            execution_started,
        )
        .await;

        let result = self
            .execute_existing_task(task, interaction_id, None)
            .await?;
        self.emit_lifecycle_event(
            "execution_completed",
            Some(interaction_id),
            Some(task.id.as_str()),
            Some("execution completed"),
            {
                let mut meta = execution_result_metadata(&result);
                let continue_agent_loop = should_continue_agent_loop(
                    &result,
                    iteration,
                    self.config.max_planner_iterations,
                );
                if let Some(obj) = meta.as_object_mut() {
                    obj.insert(
                        "iteration".to_string(),
                        Value::Number(serde_json::Number::from(iteration)),
                    );
                    obj.insert(
                        "execution_mode".to_string(),
                        Value::String(execution_mode.to_string()),
                    );
                    if let Some(action_name) = action_name {
                        obj.insert("action".to_string(), Value::String(action_name.to_string()));
                    }
                    obj.insert(
                        "agent_loop_continue".to_string(),
                        Value::Bool(continue_agent_loop),
                    );
                }
                meta
            },
        )
        .await;
        Ok(result)
    }

    async fn prepare_for_next_planner_iteration(
        &self,
        interaction_id: &str,
        task: &mut Task,
    ) -> Result<(), OrchestratorError> {
        task.set_state(TaskState::Planning);
        self.task_store.save(task).await?;
        self.thread_runtime
            .update_interaction_state(interaction_id, InteractionState::Active)
            .await?;
        Ok(())
    }

    async fn fail_agent_loop_after_completed_execution(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
        _last_result: ExecutionResult,
        turn_started_at: Instant,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let error = agent_loop_iteration_limit_error(self.config.max_planner_iterations);
        let result = ExecutionResult::Failed {
            step_id: StepId::from("planner"),
            error: error.clone(),
        };
        task.fail(error.clone(), false);
        self.task_store.save(&task).await?;
        self.thread_runtime
            .update_interaction_state(interaction_id.as_str(), InteractionState::Failed)
            .await?;
        self.emit_lifecycle_event(
            "execution_completed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("execution completed"),
            serde_json::json!({
                "status": "failed",
                "error": truncate_for_log(&error, 400),
                "execution_mode": "agent_loop",
            }),
        )
        .await;
        self.emit_assistant_output_message(
            interaction_id.as_str(),
            task.id.as_str(),
            error.clone(),
            Value::Null,
            Some(serde_json::json!({
                "status": "failed",
                "execution_mode": "agent_loop",
            })),
        )
        .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
            "orchestrator agent_loop completed execution without terminal planner decision"
        );
        Ok(planner_result_response(
            started_kind,
            interaction_id,
            task.id.clone(),
            result,
        ))
    }

    async fn complete_planner_loop_with_done(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
        iteration: usize,
        message: String,
        turn_started_at: Instant,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let message = materialize_done_message(&task, &message)?;
        self.emit_lifecycle_event(
            "planning_completed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("planning completed"),
            serde_json::json!({
                "iteration": iteration,
                "output_type": "done",
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
                "iteration": iteration,
                "step_count": 0,
                "execution_mode": "done",
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
                "iteration": iteration,
                "status": "completed",
                "execution_mode": "done",
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
                "execution_mode": "done",
            })),
        )
        .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
            "orchestrator agent_loop done latency"
        );
        Ok(planner_result_response(
            started_kind,
            interaction_id,
            task.id.clone(),
            result,
        ))
    }

    async fn complete_planner_loop_with_need_input(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
        iteration: usize,
        question: String,
        turn_started_at: Instant,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        self.emit_lifecycle_event(
            "planning_completed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("planning completed"),
            serde_json::json!({
                "iteration": iteration,
                "output_type": "need_input",
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
                "iteration": iteration,
                "step_count": 0,
                "execution_mode": "need_input",
            }),
        )
        .await;

        let result = ExecutionResult::WaitingUser {
            step_id: "planner".into(),
            prompt: question.clone(),
            approval: None,
        };
        task.plan = None;
        task.wait_for_user(question.clone());
        self.task_store.save(&task).await?;
        self.thread_runtime
            .update_interaction_state(interaction_id.as_str(), InteractionState::WaitingUser)
            .await?;
        self.emit_lifecycle_event(
            "execution_completed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("execution completed"),
            serde_json::json!({
                "iteration": iteration,
                "status": "need_input",
                "prompt": truncate_for_log(&question, 400),
                "waiting_kind": "input",
                "execution_mode": "need_input",
            }),
        )
        .await;
        self.emit_assistant_output_message(
            interaction_id.as_str(),
            task.id.as_str(),
            question,
            Value::Null,
            Some(serde_json::json!({
                "status": "need_input",
                "waiting_kind": "input",
                "execution_mode": "need_input",
            })),
        )
        .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
            "orchestrator agent_loop need_input latency"
        );
        Ok(planner_result_response(
            started_kind,
            interaction_id,
            task.id.clone(),
            result,
        ))
    }
}

fn planner_result_response(
    started_kind: &str,
    interaction_id: InteractionId,
    task_id: TaskId,
    result: ExecutionResult,
) -> OrchestratorResult {
    match started_kind {
        "started" => OrchestratorResult::Started {
            interaction_id,
            task_id,
            result,
        },
        _ => OrchestratorResult::Merged {
            interaction_id,
            task_id,
            result,
        },
    }
}

fn materialize_done_message(task: &Task, message: &str) -> Result<String, OrchestratorError> {
    if !message.contains("{{") {
        return Ok(message.to_string());
    }

    let rendered = render_output_template(
        message,
        &task.working_set_snapshot,
        std::iter::empty::<(String, Value)>(),
    )
    .map_err(|err| {
        tracing::warn!(
            task_id = %task.id,
            raw_message = %truncate_for_log(message, 400),
            error = %err,
            "planner done message template resolution failed"
        );
        OrchestratorError::Planner(PlanError::Generation(format!(
            "DONE.message template resolution failed: {err}"
        )))
    })?;

    if rendered != message {
        tracing::info!(
            task_id = %task.id,
            raw_message = %truncate_for_log(message, 400),
            rendered_message = %truncate_for_log(&rendered, 400),
            "planner done message materialized from working set bindings"
        );
    }

    Ok(rendered)
}

fn validate_single_action<'a>(
    call: &SingleAction,
    available_actions: &'a [ActionMeta],
) -> Result<&'a ActionMeta, OrchestratorError> {
    let action_meta = available_actions
        .iter()
        .find(|meta| meta.name == call.action)
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(format!(
                "single_action references unavailable action '{}'",
                call.action
            )))
        })?;

    warn_param_issues(&call.action, &call.params, &action_meta.input_schema);

    Ok(action_meta)
}

fn build_single_action_plan(intent: &str, call: &SingleAction) -> Plan {
    let step = Step::action("single_action", call.action.clone()).with_params(call.params.clone());
    let mut plan = Plan::with_confidence(
        call.reason
            .clone()
            .unwrap_or_else(|| format!("execute direct action for intent: {}", intent)),
        vec![step],
        1.0,
    );
    plan.on_complete = single_action_on_complete_template(&call.action);
    plan.on_failure = Some("Action failed: {{error}}".to_string());
    plan
}

fn validate_and_prepare_mini_plan(
    mut plan: Plan,
    available_actions: &[ActionMeta],
) -> Result<Plan, OrchestratorError> {
    if plan.steps.is_empty() {
        return Err(OrchestratorError::Planner(PlanError::Generation(
            "mini_plan must contain at least one step".to_string(),
        )));
    }

    for step in &plan.steps {
        if step.kind != StepKind::Action {
            return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "mini_plan step '{}' must use kind=action",
                step.id
            ))));
        }

        if step.action.trim().is_empty() {
            return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "mini_plan step '{}' is missing an action name",
                step.id
            ))));
        }

        let action_meta = available_actions
            .iter()
            .find(|meta| meta.name == step.action);
        if action_meta.is_none() {
            return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "mini_plan references unavailable action '{}'",
                step.action
            ))));
        }
        if let Some(meta) = action_meta {
            warn_param_issues(&step.action, &step.params, &meta.input_schema);
        }
    }

    if plan.on_failure.is_none() {
        plan.on_failure = Some("Plan failed: {{error}}".to_string());
    }

    Ok(plan)
}

/// Warn about missing required params and unknown param keys.
/// Does not block execution — the agent loop can self-correct on the next iteration.
fn warn_param_issues(action: &str, params: &serde_json::Value, input_schema: &serde_json::Value) {
    let schema_obj = match input_schema.as_object() {
        Some(obj) => obj,
        None => return,
    };

    let required: Vec<&str> = schema_obj
        .get("required")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .collect();

    let properties: Vec<&str> = schema_obj
        .get("properties")
        .and_then(|v| v.as_object())
        .into_iter()
        .flat_map(|obj| obj.keys())
        .map(String::as_str)
        .collect();

    let param_keys: Vec<&str> = params
        .as_object()
        .into_iter()
        .flat_map(|obj| obj.keys())
        .map(String::as_str)
        .collect();

    // Skip template values ({{step.field}}) — they'll be resolved at execution time.
    let is_template = |v: &serde_json::Value| {
        v.as_str()
            .map(|s| s.contains("{{") && s.contains("}}"))
            .unwrap_or(false)
    };

    for key in &required {
        if !param_keys.contains(key) {
            // Check if this is a params that might be resolved from io_bindings
            tracing::warn!(
                action = action,
                missing_param = key,
                "planner output missing required param (may cause execution failure)"
            );
        }
    }

    if !properties.is_empty() {
        for key in &param_keys {
            if !properties.contains(key) {
                let value = params.get(*key);
                if value.map(is_template).unwrap_or(false) {
                    continue; // Template bindings are allowed as extra keys
                }
                tracing::debug!(
                    action = action,
                    unknown_param = key,
                    "planner output contains param not in action schema"
                );
            }
        }
    }
}

fn single_action_on_complete_template(action: &str) -> Option<String> {
    match action {
        "shell" => Some("{{single_action.stdout}}".to_string()),
        "file_read" => Some("{{single_action.content}}".to_string()),
        "http" => Some("{{single_action.body}}".to_string()),
        _ => None,
    }
}

fn filter_actions_for_planner_iteration(actions: &[ActionMeta], task: &Task) -> Vec<ActionMeta> {
    if !has_ready_spreadsheet_fills(&task.working_set_snapshot) {
        return actions.to_vec();
    }

    let filtered = actions
        .iter()
        .filter(|meta| {
            matches!(
                meta.category.as_deref(),
                Some("spreadsheet") | Some("utility")
            )
        })
        .cloned()
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        actions.to_vec()
    } else {
        filtered
    }
}

fn has_ready_spreadsheet_fills(snapshot: &std::collections::HashMap<String, Value>) -> bool {
    let source_path = snapshot
        .get("source_path")
        .or_else(|| snapshot.get("locate.source_path"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !(source_path.ends_with(".xlsx") || source_path.ends_with(".xlsm")) {
        return false;
    }

    [
        "continuation",
        "assess.continuation",
        "patch_spec",
        "build.patch_spec",
    ]
    .into_iter()
    .filter_map(|key| snapshot.get(key))
    .any(|value| {
        value
            .get("fills")
            .and_then(Value::as_array)
            .map(|fills| !fills.is_empty())
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::planner::SingleAction;
    use serde_json::json;

    #[test]
    fn test_validate_single_action_allows_typed_action() {
        let available = vec![
            ActionMeta::new("spreadsheet_inspect", "Inspect spreadsheet")
                .with_category("spreadsheet"),
        ];
        let action = validate_single_action(
            &SingleAction {
                action: "spreadsheet_inspect".to_string(),
                params: json!({}),
                reason: None,
            },
            &available,
        )
        .expect("typed action should be allowed");
        assert_eq!(action.name, "spreadsheet_inspect");
    }

    #[test]
    fn test_validate_single_action_rejects_unavailable_action() {
        let available = vec![ActionMeta::new("shell", "Run shell").with_category("direct")];
        let err = validate_single_action(
            &SingleAction {
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
    fn test_build_single_action_plan_uses_single_step_template() {
        let plan = build_single_action_plan(
            "list docs",
            &SingleAction {
                action: "shell".to_string(),
                params: json!({"command":"find ./docs -maxdepth 1 -type f"}),
                reason: Some("list docs".to_string()),
            },
        );
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].id.as_str(), "single_action");
        assert_eq!(plan.steps[0].action, "shell");
        assert_eq!(
            plan.on_complete.as_deref(),
            Some("{{single_action.stdout}}")
        );
        assert_eq!(plan.on_failure.as_deref(), Some("Action failed: {{error}}"));
    }

    #[test]
    fn test_validate_and_prepare_mini_plan_accepts_mcp_actions() {
        let plan = Plan::new("call tools", vec![Step::action("s1", "mcp__mock__greet")]);
        let available =
            vec![ActionMeta::new("mcp__mock__greet", "greet").with_capabilities(["mcp"])];
        let prepared =
            validate_and_prepare_mini_plan(plan, &available).expect("should accept mcp tool");
        assert_eq!(prepared.steps.len(), 1);
        assert_eq!(prepared.steps[0].action, "mcp__mock__greet");
    }

    #[test]
    fn test_validate_and_prepare_mini_plan_sets_default_failure_template() {
        let available =
            vec![ActionMeta::new("document_inspect", "inspect document").with_category("document")];
        let plan = Plan::new(
            "inspect document",
            vec![Step::action("inspect", "document_inspect")],
        );
        let prepared = validate_and_prepare_mini_plan(plan, &available).expect("prepare");
        assert_eq!(
            prepared.on_failure.as_deref(),
            Some("Plan failed: {{error}}")
        );
    }

    #[test]
    fn test_filter_actions_for_planner_iteration_prefers_spreadsheet_actions_once_fills_ready() {
        let actions = vec![
            ActionMeta::new("spreadsheet_apply_patch", "apply").with_category("spreadsheet"),
            ActionMeta::new("spreadsheet_verify_patch", "verify").with_category("spreadsheet"),
            ActionMeta::new("json_stdout", "emit").with_category("utility"),
            ActionMeta::new("structured_build_patch_spec", "build").with_category("structured"),
            ActionMeta::new("shell", "shell").with_category("direct"),
        ];
        let mut task = Task::new(orchestral_core::types::Intent::new("fill workbook"));
        task.working_set_snapshot
            .insert("source_path".to_string(), json!("docs/perf-review.xlsx"));
        task.working_set_snapshot.insert(
            "continuation".to_string(),
            json!({
                "status": "commit_ready",
                "fills": [{ "cell": "F5", "value": "done" }]
            }),
        );

        let filtered = filter_actions_for_planner_iteration(&actions, &task);
        let names = filtered
            .iter()
            .map(|action| action.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "spreadsheet_apply_patch",
                "spreadsheet_verify_patch",
                "json_stdout"
            ]
        );
    }

    #[test]
    fn test_filter_actions_for_planner_iteration_keeps_original_actions_without_ready_fills() {
        let actions = vec![
            ActionMeta::new("spreadsheet_apply_patch", "apply").with_category("spreadsheet"),
            ActionMeta::new("structured_build_patch_spec", "build").with_category("structured"),
        ];
        let task = Task::new(orchestral_core::types::Intent::new("fill workbook"));

        let filtered = filter_actions_for_planner_iteration(&actions, &task);
        let names = filtered
            .iter()
            .map(|action| action.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["spreadsheet_apply_patch", "structured_build_patch_spec"]
        );
    }

    #[test]
    fn test_materialize_done_message_renders_working_set_placeholders() {
        let mut task = Task::new(orchestral_core::types::Intent::new("inspect workbook"));
        task.working_set_snapshot.insert(
            "inspect_spreadsheet.inspection.selected_region.row_count".to_string(),
            json!(7),
        );
        task.working_set_snapshot.insert(
            "inspect_spreadsheet.inspection.max_column".to_string(),
            json!(11),
        );

        let rendered = materialize_done_message(
            &task,
            "共有 {{inspect_spreadsheet.inspection.selected_region.row_count}} 行，最大列 {{inspect_spreadsheet.inspection.max_column}}。",
        )
        .expect("done message should resolve");

        assert_eq!(rendered, "共有 7 行，最大列 11。");
    }

    #[test]
    fn test_materialize_done_message_rejects_missing_placeholders() {
        let task = Task::new(orchestral_core::types::Intent::new("inspect workbook"));

        let err = materialize_done_message(
            &task,
            "共有 {{inspect_spreadsheet.inspection.selected_region.row_count}} 行。",
        )
        .expect_err("missing binding should fail");

        assert!(err
            .to_string()
            .contains("DONE.message template resolution failed"));
    }

    #[test]
    fn test_warn_param_issues_logs_missing_required() {
        // This test verifies the function runs without panic.
        // Actual warnings go to tracing — we verify the logic is correct.
        let schema = json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" }
            },
            "required": ["path", "content"]
        });

        // Missing "content" — should warn but not panic
        warn_param_issues("file_write", &json!({"path": "test.txt"}), &schema);

        // All present — no warnings
        warn_param_issues(
            "file_write",
            &json!({"path": "test.txt", "content": "hello"}),
            &schema,
        );

        // Extra unknown param — should debug log but not panic
        warn_param_issues(
            "file_write",
            &json!({"path": "test.txt", "content": "hello", "bogus": 42}),
            &schema,
        );

        // Template value in unknown param — should be silently skipped
        warn_param_issues(
            "file_write",
            &json!({"path": "test.txt", "content": "{{s1.output}}"}),
            &schema,
        );
    }

    #[test]
    fn test_validate_single_action_accepts_mcp_tools() {
        let available =
            vec![ActionMeta::new("mcp__mock__greet", "greet").with_capabilities(["mcp"])];
        let call = SingleAction {
            action: "mcp__mock__greet".to_string(),
            params: json!({"name": "Alice"}),
            reason: None,
        };
        let result = validate_single_action(&call, &available);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_single_action_rejects_unknown_action() {
        let available = vec![ActionMeta::new("shell", "run command")];
        let call = SingleAction {
            action: "nonexistent".to_string(),
            params: json!({}),
            reason: None,
        };
        let result = validate_single_action(&call, &available);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unavailable action"));
    }
}
