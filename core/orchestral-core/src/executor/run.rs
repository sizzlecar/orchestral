use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::time::sleep;

use crate::types::{Step, StepId, StepKind};

use super::{
    bind_param_value, build_step_completion_metadata, build_step_start_metadata,
    choose_terminal_result, report_progress, resolve_param_templates, truncate_for_log,
    truncate_json_for_log, validate_declared_exports, ExecutionDag, ExecutionProgressEvent,
    ExecutionResult, Executor, ExecutorContext, StepExecutionRequest, StepOutcome,
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
                ExecutionProgressEvent::new(
                    ctx.workflow_id.clone(),
                    None,
                    None,
                    "workflow_completed",
                ),
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
                    ctx.workflow_id.clone(),
                    Some(failed_step_id.clone()),
                    None,
                    "workflow_failed",
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
            ExecutionProgressEvent::new(ctx.workflow_id.clone(), None, None, "workflow_failed")
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
                                ctx.workflow_id.clone(),
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
                        })
                    }
                    StepKind::WaitEvent => {
                        report_progress(
                            ctx,
                            ExecutionProgressEvent::new(
                                ctx.workflow_id.clone(),
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
                    workflow_id = %ctx.workflow_id,
                    step_id = %step_id,
                    action = %step.action,
                    "step execution started"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.workflow_id.clone(),
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

        let mut completed = Vec::new();
        while let Some((step_id, step, result)) = in_flight.next().await {
            completed.push((step_id, step, result));
        }
        // Futures may complete in any order. Apply their state transitions in
        // logical Step order so WorkingSet collisions, progress, and terminal
        // selection replay deterministically.
        completed.sort_by(|left, right| left.0.cmp(&right.0));
        let mut terminal_result: Option<ExecutionResult> = None;
        for (step_id, step, result) in completed {
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
        result: StepOutcome,
        ctx: &ExecutorContext,
        terminal_result: &mut Option<ExecutionResult>,
    ) {
        match result {
            StepOutcome::Success { exports } => {
                if let Err(error) = validate_declared_exports(&step, &exports, self.strict_exports)
                {
                    dag.mark_failed(&step_id);
                    report_progress(
                        ctx,
                        ExecutionProgressEvent::new(
                            ctx.workflow_id.clone(),
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
                    ws.set_workflow(key.clone(), value.clone());
                    ws.set_workflow(format!("{}.{}", step.id, key), value.clone());
                }
                dag.mark_completed(&step_id);
                tracing::info!(
                    workflow_id = %ctx.workflow_id,
                    step_id = %step_id,
                    action = %step.action,
                    "step execution completed"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.workflow_id.clone(),
                        Some(step_id.clone().into()),
                        Some(step.action.clone()),
                        "step_completed",
                    )
                    .with_metadata(completion_metadata),
                )
                .await;
            }
            StepOutcome::RetryableError { message, .. } => {
                dag.mark_failed(&step_id);
                tracing::warn!(
                    workflow_id = %ctx.workflow_id,
                    step_id = %step_id,
                    action = %step.action,
                    error = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                    "step execution retryable error"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.workflow_id.clone(),
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
            StepOutcome::Error { message } => {
                dag.mark_failed(&step_id);
                tracing::error!(
                    workflow_id = %ctx.workflow_id,
                    step_id = %step_id,
                    action = %step.action,
                    error = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                    "step execution failed"
                );
                report_progress(
                    ctx,
                    ExecutionProgressEvent::new(
                        ctx.workflow_id.clone(),
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
    ) -> StepOutcome {
        let mut retries_used: u32 = 0;
        let mut current_execution_id = execution_id.to_string();

        loop {
            if ctx.cancellation_token.is_cancelled() {
                return StepOutcome::error("workflow execution cancelled before Step dispatch");
            }
            let attempt = retries_used.saturating_add(1);
            let result = self
                .execute_step_data(step, &current_execution_id, attempt, ctx)
                .await;
            let StepOutcome::RetryableError {
                message,
                retry_after,
                attempt: reported_attempt,
            } = result
            else {
                return result;
            };

            if retries_used >= self.max_retry_attempts {
                let total_attempts = retries_used.saturating_add(1);
                return StepOutcome::error(format!(
                    "{} (retry exhausted after {} attempt(s))",
                    message, total_attempts
                ));
            }

            let delay = retry_after.unwrap_or_else(|| self.compute_retry_backoff(retries_used));
            let next_attempt = retries_used.saturating_add(1);
            tracing::warn!(
                workflow_id = %ctx.workflow_id,
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
                    ctx.workflow_id.clone(),
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
                tokio::select! {
                    biased;
                    _ = ctx.cancellation_token.cancelled() => {
                        return StepOutcome::error(
                            "workflow execution cancelled during retry backoff",
                        );
                    }
                    _ = sleep(delay) => {}
                }
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
        attempt: u32,
        ctx: &ExecutorContext,
    ) -> StepOutcome {
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                workflow_id = %ctx.workflow_id,
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
                if let Some(value) = ws.get_workflow(&binding.from) {
                    tracing::debug!(
                        workflow_id = %ctx.workflow_id,
                        step_id = %step.id,
                        from = %binding.from,
                        to = %binding.to,
                        required = binding.required,
                        value = %truncate_json_for_log(value, MAX_LOG_JSON_CHARS),
                        "io binding resolved"
                    );
                    if let Err(error) = bind_param_value(&mut resolved_params, &binding.to, value) {
                        return StepOutcome::error(format!(
                            "Invalid io binding for step '{}': {}",
                            step.id, error
                        ));
                    }
                } else if binding.required {
                    tracing::warn!(
                        workflow_id = %ctx.workflow_id,
                        step_id = %step.id,
                        from = %binding.from,
                        to = %binding.to,
                        "required io binding missing"
                    );
                    return StepOutcome::error(format!(
                        "Missing required io binding '{}' from '{}' for step '{}'",
                        binding.to, binding.from, step.id
                    ));
                } else {
                    tracing::debug!(
                        workflow_id = %ctx.workflow_id,
                        step_id = %step.id,
                        from = %binding.from,
                        to = %binding.to,
                        "optional io binding missing"
                    );
                }
            }
            if let Err(error) = resolve_param_templates(&mut resolved_params, &ws) {
                return StepOutcome::error(format!(
                    "Template resolution failed for step '{}': {}",
                    step.id, error
                ));
            }
        }

        ctx.step_execution_port
            .execute_step(
                StepExecutionRequest {
                    step_id: step.id.clone(),
                    step_kind: step.kind.clone(),
                    action: step.action.clone(),
                    execution_id: execution_id.to_string(),
                    attempt,
                    resolved_params,
                },
                ctx,
            )
            .await
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::RwLock;
    use tokio_util::sync::CancellationToken;

    use crate::executor::StepExecutionPort;
    use crate::types::WorkflowId;
    use crate::workflow_state::WorkingSet;

    const CANCELLATION_CASES: usize = 1_000;

    struct AlwaysRetryPort {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl StepExecutionPort for AlwaysRetryPort {
        async fn execute_step(
            &self,
            _request: StepExecutionRequest,
            _ctx: &ExecutorContext,
        ) -> StepOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            StepOutcome::retryable("retry later", Some(Duration::from_secs(60)), 1)
        }
    }

    #[tokio::test]
    async fn one_thousand_retry_backoff_cancellations_dispatch_no_new_attempt_and_finish_under_one_second(
    ) {
        let port = Arc::new(AlwaysRetryPort {
            calls: AtomicUsize::new(0),
        });
        let cancellation = CancellationToken::new();
        let context = Arc::new(
            ExecutorContext::new(
                WorkflowId::new("retry-cancel-workflow"),
                Arc::new(RwLock::new(WorkingSet::new())),
                port.clone(),
            )
            .with_cancellation_token(cancellation.clone()),
        );
        let executor = Arc::new(Executor::new().with_retry_policy(
            3,
            Duration::from_secs(60),
            Duration::from_secs(60),
        ));
        let step = Step::action("retry-cancel-step", "retry");
        let mut tasks = Vec::with_capacity(CANCELLATION_CASES);
        for index in 0..CANCELLATION_CASES {
            let executor = executor.clone();
            let context = context.clone();
            let step = step.clone();
            tasks.push(tokio::spawn(async move {
                let outcome = executor
                    .execute_step_with_retry(&step, &format!("execution-{index}"), &context)
                    .await;
                (outcome, Instant::now())
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while port.calls.load(Ordering::SeqCst) != CANCELLATION_CASES {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all retry waits are reached before cancellation");

        let cancelled_at = Instant::now();
        cancellation.cancel();
        let completed = tokio::time::timeout(
            Duration::from_secs(1),
            futures_util::future::join_all(tasks),
        )
        .await
        .expect("all retry waits observe cancellation within one second");
        let mut latencies = Vec::with_capacity(CANCELLATION_CASES);
        for result in completed {
            let (outcome, finished_at) = result.unwrap();
            assert!(matches!(
                outcome,
                StepOutcome::Error { ref message }
                    if message == "workflow execution cancelled during retry backoff"
            ));
            latencies.push(finished_at.duration_since(cancelled_at));
        }
        latencies.sort_unstable();
        let p99 = latencies[(CANCELLATION_CASES * 99 / 100).saturating_sub(1)];
        assert!(p99 <= Duration::from_secs(1), "cancel p99 was {p99:?}");
        assert_eq!(port.calls.load(Ordering::SeqCst), CANCELLATION_CASES);
    }
}
