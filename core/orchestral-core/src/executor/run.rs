use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::time::sleep;

use crate::action::ActionResult;
use crate::spi::lifecycle::{StepContext, StepDecision};
use crate::types::{Step, StepId, StepKind};

use super::{
    bind_param_value, build_step_completion_metadata, build_step_start_metadata,
    choose_terminal_result, execute_action_with_registry_with_options, report_progress,
    resolve_param_templates, truncate_for_log, truncate_json_for_log, validate_declared_exports,
    ExecutionDag, ExecutionProgressEvent, ExecutionResult, Executor, ExecutorContext,
    MAX_LOG_JSON_CHARS, MAX_LOG_TEXT_CHARS,
};

impl Executor {
    pub(super) async fn resolve_no_ready_nodes(
        &self,
        dag: &ExecutionDag,
        ctx: &ExecutorContext,
    ) -> ExecutionResult {
        if dag.is_completed() {
            report_progress(
                ctx,
                ExecutionProgressEvent::new(ctx.task_id.clone(), None, None, "task_completed"),
            )
            .await;
            return ExecutionResult::Completed;
        }

        if dag.has_failed() {
            let failed = dag.failed_nodes();
            let failed_step_id = failed.first().map(|s| StepId::from(*s)).unwrap_or_default();
            report_progress(
                ctx,
                ExecutionProgressEvent::new(
                    ctx.task_id.clone(),
                    Some(failed_step_id.clone()),
                    None,
                    "task_failed",
                )
                .with_message("execution failed"),
            )
            .await;
            return ExecutionResult::Failed {
                step_id: failed_step_id,
                error: "Execution failed".to_string(),
            };
        }

        report_progress(
            ctx,
            ExecutionProgressEvent::new(ctx.task_id.clone(), None, None, "task_failed")
                .with_message("no ready nodes but DAG not completed"),
        )
        .await;
        ExecutionResult::Failed {
            step_id: StepId::default(),
            error: "No ready nodes but DAG not completed".to_string(),
        }
    }

    pub(super) async fn handle_wait_steps(
        &self,
        dag: &mut ExecutionDag,
        batch: &[String],
        ctx: &ExecutorContext,
    ) -> Option<ExecutionResult> {
        for step_id in batch {
            let wait_data = dag.get_node(step_id).and_then(|node| match node.step.kind {
                StepKind::WaitUser => Some((
                    StepKind::WaitUser,
                    node.step.action.clone(),
                    node.step.params.clone(),
                )),
                StepKind::WaitEvent => Some((
                    StepKind::WaitEvent,
                    node.step.action.clone(),
                    node.step.params.clone(),
                )),
                _ => None,
            });
            if let Some((kind, action, params)) = wait_data {
                dag.mark_running(step_id);
                return match kind {
                    StepKind::WaitUser => {
                        report_progress(
                            ctx,
                            ExecutionProgressEvent::new(
                                ctx.task_id.clone(),
                                Some(step_id.clone().into()),
                                Some(action),
                                "step_waiting_user",
                            ),
                        )
                        .await;
                        Some(ExecutionResult::WaitingUser {
                            step_id: step_id.clone().into(),
                            prompt: params
                                .get("prompt")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Please provide input")
                                .to_string(),
                            approval: None,
                        })
                    }
                    StepKind::WaitEvent => {
                        report_progress(
                            ctx,
                            ExecutionProgressEvent::new(
                                ctx.task_id.clone(),
                                Some(step_id.clone().into()),
                                Some(action),
                                "step_waiting_event",
                            ),
                        )
                        .await;
                        Some(ExecutionResult::WaitingEvent {
                            step_id: step_id.clone().into(),
                            event_type: params
                                .get("event_type")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string(),
                        })
                    }
                    _ => None,
                };
            }
        }
        None
    }

    pub(super) async fn execute_batch(
        &self,
        dag: &mut ExecutionDag,
        batch: Vec<String>,
        ctx: &ExecutorContext,
    ) -> Option<ExecutionResult> {
        let mut in_flight = FuturesUnordered::new();
        for step_id in batch {
            let node_data = dag
                .get_node(&step_id)
                .map(|node| (node.step.clone(), node.execution_id.clone()));
            if let Some((step, execution_id)) = node_data {
                dag.mark_running(&step_id);
                tracing::info!(
                    task_id = %ctx.task_id,
                    step_id = %step_id,
                    action = %step.action,
                    "step execution started"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.task_id.clone(),
                        Some(step_id.clone().into()),
                        Some(step.action.clone()),
                        "step_started",
                    )
                    .with_metadata(build_step_start_metadata(&step.action, &step.params)),
                )
                .await;

                in_flight.push(async move {
                    let result = self
                        .execute_step_with_retry(&step, &execution_id, ctx)
                        .await;
                    (step_id, step, result)
                });
            }
        }

        let mut terminal_result: Option<ExecutionResult> = None;
        while let Some((step_id, step, result)) = in_flight.next().await {
            self.process_step_result(dag, step_id, step, result, ctx, &mut terminal_result)
                .await;
        }
        terminal_result
    }

    pub(super) async fn process_step_result(
        &self,
        dag: &mut ExecutionDag,
        step_id: String,
        step: Step,
        result: ActionResult,
        ctx: &ExecutorContext,
        terminal_result: &mut Option<ExecutionResult>,
    ) {
        // Lifecycle hook: after_step
        if let Some(hooks) = &ctx.lifecycle_hooks {
            let step_ctx = StepContext::new(
                ctx.thread_id.clone().unwrap_or_default(),
                ctx.task_id.to_string(),
                step_id.clone(),
                step.action.clone(),
                ctx.working_set.clone(),
            );
            hooks.after_step(&step, &result, &step_ctx).await;
        }

        match result {
            ActionResult::Success { exports } => {
                if let Err(error) = validate_declared_exports(&step, &exports, self.strict_exports)
                {
                    dag.mark_failed(&step_id);
                    report_progress(
                        ctx,
                        ExecutionProgressEvent::new(
                            ctx.task_id.clone(),
                            Some(step_id.clone().into()),
                            Some(step.action.clone()),
                            "step_failed",
                        )
                        .with_message(error.clone()),
                    )
                    .await;
                    choose_terminal_result(
                        terminal_result,
                        ExecutionResult::Failed {
                            step_id: step_id.into(),
                            error,
                        },
                    );
                    return;
                }

                let completion_metadata = build_step_completion_metadata(&step.action, &exports);
                let mut ws = ctx.working_set.write().await;
                for (key, value) in &exports {
                    ws.set_task(key.clone(), value.clone());
                    ws.set_task(format!("{}.{}", step.id, key), value.clone());
                }
                dag.mark_completed(&step_id);
                tracing::info!(
                    task_id = %ctx.task_id,
                    step_id = %step_id,
                    action = %step.action,
                    "step execution completed"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.task_id.clone(),
                        Some(step_id.clone().into()),
                        Some(step.action.clone()),
                        "step_completed",
                    )
                    .with_metadata(completion_metadata),
                )
                .await;
            }
            ActionResult::NeedClarification { question } => {
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.task_id.clone(),
                        Some(step_id.clone().into()),
                        Some(step.action.clone()),
                        "step_waiting_user",
                    )
                    .with_message(question.clone()),
                )
                .await;
                choose_terminal_result(
                    terminal_result,
                    ExecutionResult::WaitingUser {
                        step_id: step_id.into(),
                        prompt: question,
                        approval: None,
                    },
                );
            }
            ActionResult::NeedApproval { request } => {
                let approval_reason = request.reason.clone();
                let approval_command = request.command.clone();
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.task_id.clone(),
                        Some(step_id.clone().into()),
                        Some(step.action.clone()),
                        "step_waiting_user",
                    )
                    .with_message(approval_reason.clone())
                    .with_metadata(serde_json::json!({
                        "waiting_kind": "approval",
                        "approval_reason": approval_reason,
                        "approval_command": approval_command,
                    })),
                )
                .await;
                choose_terminal_result(
                    terminal_result,
                    ExecutionResult::WaitingUser {
                        step_id: step_id.into(),
                        prompt: "Approval required".to_string(),
                        approval: Some(request),
                    },
                );
            }
            ActionResult::RetryableError { message, .. } => {
                dag.mark_failed(&step_id);
                tracing::warn!(
                    task_id = %ctx.task_id,
                    step_id = %step_id,
                    action = %step.action,
                    error = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                    "step execution retryable error"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.task_id.clone(),
                        Some(step_id.clone().into()),
                        Some(step.action.clone()),
                        "step_failed",
                    )
                    .with_message(message.clone()),
                )
                .await;
                choose_terminal_result(
                    terminal_result,
                    ExecutionResult::Failed {
                        step_id: step_id.into(),
                        error: message,
                    },
                );
            }
            ActionResult::Error { message } => {
                dag.mark_failed(&step_id);
                tracing::error!(
                    task_id = %ctx.task_id,
                    step_id = %step_id,
                    action = %step.action,
                    error = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                    "step execution failed"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.task_id.clone(),
                        Some(step_id.clone().into()),
                        Some(step.action.clone()),
                        "step_failed",
                    )
                    .with_message(message.clone()),
                )
                .await;
                choose_terminal_result(
                    terminal_result,
                    ExecutionResult::Failed {
                        step_id: step_id.into(),
                        error: message,
                    },
                );
            }
        }
    }

    pub(super) async fn execute_step_with_retry(
        &self,
        step: &Step,
        execution_id: &str,
        ctx: &ExecutorContext,
    ) -> ActionResult {
        let mut retries_used: u32 = 0;
        let mut current_execution_id = execution_id.to_string();

        loop {
            let result = self
                .execute_step_data(step, &current_execution_id, ctx)
                .await;
            let ActionResult::RetryableError {
                message,
                retry_after,
                attempt: reported_attempt,
            } = result
            else {
                return result;
            };

            if retries_used >= self.max_retry_attempts {
                let total_attempts = retries_used.saturating_add(1);
                return ActionResult::error(format!(
                    "{} (retry exhausted after {} attempt(s))",
                    message, total_attempts
                ));
            }

            let delay = retry_after.unwrap_or_else(|| self.compute_retry_backoff(retries_used));
            let next_attempt = retries_used.saturating_add(1);
            tracing::warn!(
                task_id = %ctx.task_id,
                step_id = %step.id,
                action = %step.action,
                message = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                retry_attempt = next_attempt,
                reported_attempt = reported_attempt,
                retry_in_ms = delay.as_millis() as u64,
                "retrying step after retryable error"
            );
            report_progress(
                ctx,
                ExecutionProgressEvent::new(
                    ctx.task_id.clone(),
                    Some(step.id.clone()),
                    Some(step.action.clone()),
                    "step_retrying",
                )
                .with_message(message.clone())
                .with_metadata(serde_json::json!({
                    "retry_attempt": next_attempt,
                    "reported_attempt": reported_attempt,
                    "retry_in_ms": delay.as_millis() as u64,
                    "max_retry_attempts": self.max_retry_attempts,
                })),
            )
            .await;

            if !delay.is_zero() {
                sleep(delay).await;
            }
            retries_used = next_attempt;
            current_execution_id = uuid::Uuid::new_v4().to_string();
        }
    }

    pub(super) fn compute_retry_backoff(&self, retries_used: u32) -> Duration {
        let base_ms = self.retry_base_delay.as_millis();
        if base_ms == 0 {
            return Duration::from_millis(0);
        }
        let max_ms = self.retry_max_delay.as_millis().max(base_ms);
        let shift = retries_used.min(20);
        let multiplier = 1u128 << shift;
        let backoff_ms = base_ms.saturating_mul(multiplier).min(max_ms);
        let millis = u64::try_from(backoff_ms).unwrap_or(u64::MAX);
        Duration::from_millis(millis)
    }

    /// Execute a single step using extracted data (avoids borrow conflicts)
    pub(super) async fn execute_step_data(
        &self,
        step: &Step,
        execution_id: &str,
        ctx: &ExecutorContext,
    ) -> ActionResult {
        // Lifecycle hook: before_step
        if let Some(hooks) = &ctx.lifecycle_hooks {
            let step_ctx = StepContext::new(
                ctx.thread_id.clone().unwrap_or_default(),
                ctx.task_id.to_string(),
                step.id.to_string(),
                step.action.clone(),
                ctx.working_set.clone(),
            );
            match hooks.before_step(step, &step_ctx).await {
                StepDecision::Skip { reason } => {
                    tracing::info!(
                        step_id = %step.id,
                        action = %step.action,
                        reason = %reason,
                        "step skipped by lifecycle hook"
                    );
                    return ActionResult::error(format!("skipped by hook: {}", reason));
                }
                StepDecision::Continue => {}
            }
        }

        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                task_id = %ctx.task_id,
                step_id = %step.id,
                action = %step.action,
                params = %truncate_json_for_log(&step.params, MAX_LOG_JSON_CHARS),
                declared_exports = ?step.exports,
                io_bindings = ?step.io_bindings,
                "step execution context"
            );
        }

        let mut resolved_params = step.params.clone();
        {
            let ws = ctx.working_set.read().await;
            for binding in &step.io_bindings {
                if let Some(value) = ws.get_task(&binding.from) {
                    tracing::debug!(
                        task_id = %ctx.task_id,
                        step_id = %step.id,
                        from = %binding.from,
                        to = %binding.to,
                        required = binding.required,
                        value = %truncate_json_for_log(value, MAX_LOG_JSON_CHARS),
                        "io binding resolved"
                    );
                    if let Err(error) = bind_param_value(&mut resolved_params, &binding.to, value) {
                        return ActionResult::error(format!(
                            "Invalid io binding for step '{}': {}",
                            step.id, error
                        ));
                    }
                } else if binding.required {
                    tracing::warn!(
                        task_id = %ctx.task_id,
                        step_id = %step.id,
                        from = %binding.from,
                        to = %binding.to,
                        "required io binding missing"
                    );
                    return ActionResult::error(format!(
                        "Missing required io binding '{}' from '{}' for step '{}'",
                        binding.to, binding.from, step.id
                    ));
                } else {
                    tracing::debug!(
                        task_id = %ctx.task_id,
                        step_id = %step.id,
                        from = %binding.from,
                        to = %binding.to,
                        "optional io binding missing"
                    );
                }
            }
            if let Err(error) = resolve_param_templates(&mut resolved_params, &ws) {
                return ActionResult::error(format!(
                    "Template resolution failed for step '{}': {}",
                    step.id, error
                ));
            }
        }

        if step.kind == StepKind::Agent {
            if let Some(agent_executor) = &self.agent_step_executor {
                return agent_executor
                    .execute_agent_step(
                        step,
                        resolved_params,
                        execution_id,
                        ctx,
                        self.action_registry.clone(),
                    )
                    .await;
            }
            return ActionResult::error(format!(
                "Agent step '{}' is not enabled: missing agent executor",
                step.id
            ));
        }

        execute_action_with_registry_with_options(
            self.action_registry.clone(),
            ctx,
            &step.id,
            &step.action,
            execution_id,
            resolved_params,
            &self.action_execution_options,
        )
        .await
    }
}
