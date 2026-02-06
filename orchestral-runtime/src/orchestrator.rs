//! Orchestrator - minimal intent → plan → normalize → execute pipeline
//!
//! This bridges the ThreadRuntime (events + concurrency) with core planning/execution.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

use orchestral_context::{
    ContextBuilder, ContextError, ContextRequest, ContextWindow, TokenBudget,
};
use orchestral_core::action::{extract_meta, ActionMeta};
use orchestral_core::executor::{
    ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, Executor, ExecutorContext,
};
use orchestral_core::interpreter::{
    InterpretDeltaSink, InterpretRequest, NoopResultInterpreter, ResultInterpreter,
};
use orchestral_core::normalizer::{NormalizeError, PlanNormalizer};
use orchestral_core::planner::{
    HistoryItem, PlanError, Planner, PlannerContext, PlannerOutput, PlannerRuntimeInfo,
};
use orchestral_core::store::{ReferenceStore, StoreError, TaskStore, WorkingSet};
use orchestral_core::types::{
    Intent, IntentContext, Step, StepKind, Task, TaskId, TaskState, WaitUserReason,
};
use orchestral_stores::{Event, EventBus, EventStore};

use crate::{HandleEventResult, InteractionState, RuntimeError, ThreadRuntime};

const MAX_LOG_CHARS: usize = 8_000;

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn truncate_debug_for_log(value: &impl std::fmt::Debug, max_chars: usize) -> String {
    truncate_for_log(&format!("{:?}", value), max_chars)
}

/// Orchestrator result for a handled event
#[derive(Debug)]
pub enum OrchestratorResult {
    /// A new interaction was started and executed
    Started {
        interaction_id: String,
        task_id: TaskId,
        result: ExecutionResult,
    },
    /// The event was merged into an existing interaction and executed
    Merged {
        interaction_id: String,
        task_id: TaskId,
        result: ExecutionResult,
    },
    /// The event was rejected
    Rejected { reason: String },
    /// The event was queued
    Queued,
}

/// Orchestrator errors
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("planner error: {0}")]
    Planner(#[from] PlanError),
    #[error("normalize error: {0}")]
    Normalize(#[from] NormalizeError),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("context error: {0}")]
    Context(#[from] ContextError),
    #[error("task not found: {0}")]
    TaskNotFound(String),
    #[error("task has no plan: {0}")]
    MissingPlan(String),
    #[error("resume error: {0}")]
    ResumeError(String),
    #[error("unsupported event: {0}")]
    UnsupportedEvent(String),
}

/// Orchestrator - wires runtime + planner + executor for a minimal pipeline
pub struct Orchestrator {
    pub thread_runtime: ThreadRuntime,
    pub planner: Arc<dyn Planner>,
    pub normalizer: PlanNormalizer,
    pub executor: Executor,
    pub task_store: Arc<dyn TaskStore>,
    pub reference_store: Arc<dyn ReferenceStore>,
    pub context_builder: Option<Arc<dyn ContextBuilder>>,
    pub result_interpreter: Arc<dyn ResultInterpreter>,
    pub config: OrchestratorConfig,
}

/// Orchestrator configuration
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Max events to include in planner history (0 = all)
    pub history_limit: usize,
    /// Token budget for context assembly
    pub context_budget: TokenBudget,
    /// Whether to include history when building context
    pub include_history: bool,
    /// Whether to include references when building context
    pub include_references: bool,
    /// Retry once by replanning only the failed subgraph.
    pub auto_replan_once: bool,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            history_limit: 50,
            context_budget: TokenBudget::default(),
            include_history: true,
            include_references: true,
            auto_replan_once: true,
        }
    }
}

impl Orchestrator {
    /// Create a new orchestrator
    pub fn new(
        thread_runtime: ThreadRuntime,
        planner: Arc<dyn Planner>,
        normalizer: PlanNormalizer,
        executor: Executor,
        task_store: Arc<dyn TaskStore>,
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self::with_config(
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            reference_store,
            OrchestratorConfig::default(),
        )
    }

    /// Create a new orchestrator with config
    pub fn with_config(
        thread_runtime: ThreadRuntime,
        planner: Arc<dyn Planner>,
        normalizer: PlanNormalizer,
        executor: Executor,
        task_store: Arc<dyn TaskStore>,
        reference_store: Arc<dyn ReferenceStore>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            reference_store,
            context_builder: None,
            result_interpreter: Arc::new(NoopResultInterpreter),
            config,
        }
    }

    /// Attach a context builder (optional)
    pub fn with_context_builder(mut self, builder: Arc<dyn ContextBuilder>) -> Self {
        self.context_builder = Some(builder);
        self
    }

    /// Attach a result interpreter (optional override).
    pub fn with_result_interpreter(mut self, interpreter: Arc<dyn ResultInterpreter>) -> Self {
        self.result_interpreter = interpreter;
        self
    }

    /// Handle an event end-to-end (intent → plan → normalize → execute)
    pub async fn handle_event(
        &self,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        if let Some(interaction_id) = self.thread_runtime.find_resume_interaction(&event).await {
            return self.resume_interaction(interaction_id, event).await;
        }

        self.start_new_interaction(event).await
    }

    async fn start_new_interaction(
        &self,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let event_clone = event.clone();
        tracing::info!(
            thread_id = %event_clone.thread_id(),
            event_type = %event_type_label(&event_clone),
            "orchestrator received event"
        );
        let decision = self.thread_runtime.handle_event(event).await?;

        let (interaction_id, started_kind) = match &decision {
            HandleEventResult::Started { interaction_id } => (interaction_id.clone(), "started"),
            HandleEventResult::Merged { interaction_id } => (interaction_id.clone(), "merged"),
            HandleEventResult::Rejected { reason } => {
                self.emit_lifecycle_event(
                    "turn_rejected",
                    None,
                    None,
                    Some(reason),
                    serde_json::json!({ "reason": reason }),
                )
                .await;
                return Ok(OrchestratorResult::Rejected {
                    reason: reason.clone(),
                });
            }
            HandleEventResult::Queued => {
                self.emit_lifecycle_event(
                    "turn_queued",
                    None,
                    None,
                    Some("event queued"),
                    Value::Null,
                )
                .await;
                return Ok(OrchestratorResult::Queued);
            }
        };

        // Build intent from event
        let intent = intent_from_event(&event_clone, Some(interaction_id.clone()))?;

        // Create and persist task
        let task = Task::new(intent);
        self.task_store.save(&task).await?;
        self.thread_runtime
            .add_task_to_interaction(&interaction_id, task.id.clone())
            .await?;
        self.emit_lifecycle_event(
            "turn_started",
            Some(&interaction_id),
            Some(&task.id),
            Some("turn started"),
            serde_json::json!({
                "started_kind": started_kind,
                "event_type": event_type_label(&event_clone),
            }),
        )
        .await;

        self.run_planning_pipeline(interaction_id, started_kind, task)
            .await
    }

    async fn resume_interaction(
        &self,
        interaction_id: String,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        self.thread_runtime
            .append_event_to_interaction(&interaction_id, event.clone())
            .await?;
        self.thread_runtime
            .resume_interaction(&interaction_id)
            .await?;

        let interaction = self
            .thread_runtime
            .get_interaction(&interaction_id)
            .await
            .ok_or_else(|| OrchestratorError::ResumeError("interaction missing".to_string()))?;
        let task_id =
            interaction.task_ids.last().cloned().ok_or_else(|| {
                OrchestratorError::ResumeError("interaction has no task".to_string())
            })?;

        let mut task = self
            .task_store
            .load(&task_id)
            .await?
            .ok_or_else(|| OrchestratorError::TaskNotFound(task_id.clone()))?;
        self.emit_lifecycle_event(
            "turn_resumed",
            Some(&interaction_id),
            Some(&task.id),
            Some("resuming waiting turn"),
            serde_json::json!({
                "event_type": event_type_label(&event),
            }),
        )
        .await;
        if task.plan.is_none() {
            let intent = intent_from_event(&event, Some(interaction_id.clone()))?;
            let new_task = Task::new(intent);
            self.task_store.save(&new_task).await?;
            self.thread_runtime
                .add_task_to_interaction(&interaction_id, new_task.id.clone())
                .await?;
            self.emit_lifecycle_event(
                "turn_started",
                Some(&interaction_id),
                Some(&new_task.id),
                Some("turn started from clarification follow-up"),
                serde_json::json!({
                    "started_kind": "merged",
                    "event_type": event_type_label(&event),
                    "resumed_from_task_id": task.id.clone(),
                }),
            )
            .await;
            return self
                .run_planning_pipeline(interaction_id, "merged", new_task)
                .await;
        }
        task.start_executing();
        self.task_store.save(&task).await?;

        self.emit_lifecycle_event(
            "execution_started",
            Some(&interaction_id),
            Some(&task.id),
            Some("execution resumed"),
            serde_json::json!({ "resume": true }),
        )
        .await;
        let result = self
            .execute_existing_task(&mut task, &interaction_id, Some(&event))
            .await?;
        self.emit_lifecycle_event(
            "execution_completed",
            Some(&interaction_id),
            Some(&task.id),
            Some("execution completed"),
            execution_result_metadata(&result),
        )
        .await;
        self.emit_interpreted_output(&interaction_id, &task, &result)
            .await;

        Ok(OrchestratorResult::Merged {
            interaction_id,
            task_id: task.id.clone(),
            result,
        })
    }

    async fn run_planning_pipeline(
        &self,
        interaction_id: String,
        started_kind: &str,
        mut task: Task,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let actions = self.available_actions().await;
        let history = self.history_for_planner(&interaction_id, &task.id).await?;
        self.emit_lifecycle_event(
            "planning_started",
            Some(&interaction_id),
            Some(&task.id),
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
        let runtime_info = PlannerRuntimeInfo::detect();
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info);
        let planner_output = self.planner.plan(&task.intent, &context).await?;
        match planner_output {
            PlannerOutput::Workflow(plan) => {
                self.emit_lifecycle_event(
                    "planning_completed",
                    Some(&interaction_id),
                    Some(&task.id),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "workflow",
                        "goal": plan.goal.clone(),
                        "step_count": plan.steps.len(),
                        "steps": summarize_plan_steps(&plan),
                    }),
                )
                .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    goal = %plan.goal,
                    step_count = plan.steps.len(),
                    "orchestrator planning completed"
                );
                if tracing::enabled!(tracing::Level::DEBUG) {
                    tracing::debug!(
                        interaction_id = %interaction_id,
                        task_id = %task.id,
                        plan = %truncate_debug_for_log(&plan.steps, MAX_LOG_CHARS),
                        "orchestrator plan detail"
                    );
                }
                task.set_plan(plan);
                task.start_executing();
                self.task_store.save(&task).await?;

                let planned_step_count = task
                    .plan
                    .as_ref()
                    .map(|p| p.steps.len())
                    .unwrap_or_default();
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(&interaction_id),
                    Some(&task.id),
                    Some("execution started"),
                    serde_json::json!({ "step_count": planned_step_count }),
                )
                .await;
                let result = self
                    .execute_existing_task(&mut task, &interaction_id, None)
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(&interaction_id),
                    Some(&task.id),
                    Some("execution completed"),
                    execution_result_metadata(&result),
                )
                .await;
                self.emit_interpreted_output(&interaction_id, &task, &result)
                    .await;

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
                    Some(&interaction_id),
                    Some(&task.id),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "direct_response",
                        "step_count": 0,
                    }),
                )
                .await;
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(&interaction_id),
                    Some(&task.id),
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
                    .update_interaction_state(&interaction_id, InteractionState::Completed)
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(&interaction_id),
                    Some(&task.id),
                    Some("execution completed"),
                    serde_json::json!({
                        "status": "completed",
                        "execution_mode": "direct_response",
                    }),
                )
                .await;
                self.emit_assistant_output_message(
                    &interaction_id,
                    &task.id,
                    message,
                    Value::Null,
                    Some(serde_json::json!({
                        "status": "completed",
                        "execution_mode": "direct_response",
                    })),
                )
                .await;

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
                    Some(&interaction_id),
                    Some(&task.id),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "clarification",
                        "step_count": 0,
                    }),
                )
                .await;
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(&interaction_id),
                    Some(&task.id),
                    Some("execution skipped"),
                    serde_json::json!({
                        "step_count": 0,
                        "execution_mode": "clarification",
                    }),
                )
                .await;

                let result = ExecutionResult::WaitingUser {
                    step_id: "planner".to_string(),
                    prompt: question.clone(),
                    approval: None,
                };
                task.wait_for_user(question.clone());
                self.task_store.save(&task).await?;
                self.thread_runtime
                    .update_interaction_state(&interaction_id, InteractionState::WaitingUser)
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(&interaction_id),
                    Some(&task.id),
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
                    &interaction_id,
                    &task.id,
                    question,
                    Value::Null,
                    Some(serde_json::json!({
                        "status": "clarification",
                        "waiting_kind": "input",
                        "execution_mode": "clarification",
                    })),
                )
                .await;

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

    async fn execute_existing_task(
        &self,
        task: &mut Task,
        interaction_id: &str,
        resume_event: Option<&Event>,
    ) -> Result<ExecutionResult, OrchestratorError> {
        let initial_plan = task
            .plan
            .clone()
            .ok_or_else(|| OrchestratorError::MissingPlan(task.id.clone()))?;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            step_count = initial_plan.steps.len(),
            "orchestrator normalize/execute started"
        );
        let mut current_plan = initial_plan.clone();
        let mut remaining_replans = if self.config.auto_replan_once { 1 } else { 0 };
        let mut resume = resume_event;
        let final_result = loop {
            let run = self
                .execute_plan_once(task, interaction_id, &current_plan, resume)
                .await?;
            task.set_checkpoint(run.completed_step_ids, run.working_set_snapshot);
            self.task_store.save(task).await?;

            if let ExecutionResult::Failed { step_id, error } = &run.result {
                if remaining_replans > 0 {
                    remaining_replans -= 1;
                    self.emit_lifecycle_event(
                        "replanning_started",
                        Some(interaction_id),
                        Some(&task.id),
                        Some("execution failed, attempting one-shot recovery replan"),
                        serde_json::json!({
                            "failed_step_id": step_id,
                            "error": truncate_for_log(error, 400),
                        }),
                    )
                    .await;
                    match self
                        .build_recovery_plan(task, &current_plan, step_id, error, interaction_id)
                        .await
                    {
                        Ok(Some(patched_plan)) => {
                            task.set_plan(patched_plan.clone());
                            task.start_executing();
                            self.task_store.save(task).await?;
                            current_plan = patched_plan;
                            resume = None;
                            self.emit_lifecycle_event(
                                "replanning_completed",
                                Some(interaction_id),
                                Some(&task.id),
                                Some("recovery plan patched"),
                                serde_json::json!({
                                    "step_count": current_plan.steps.len(),
                                }),
                            )
                            .await;
                            continue;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            self.emit_lifecycle_event(
                                "replanning_failed",
                                Some(interaction_id),
                                Some(&task.id),
                                Some("replanning failed, fallback to failure result"),
                                serde_json::json!({
                                    "error": truncate_for_log(&err.to_string(), 400),
                                }),
                            )
                            .await;
                        }
                    }
                }
            }
            break run.result;
        };

        let new_state = task_state_from_execution(&final_result);
        task.set_state(new_state.clone());
        self.task_store.save(task).await?;
        self.thread_runtime
            .update_interaction_state(interaction_id, interaction_state_from_task(&new_state))
            .await?;

        Ok(final_result)
    }

    async fn execute_plan_once(
        &self,
        task: &Task,
        interaction_id: &str,
        plan: &orchestral_core::types::Plan,
        resume_event: Option<&Event>,
    ) -> Result<PlanExecutionSnapshot, OrchestratorError> {
        let normalized = self.normalizer.normalize(plan.clone())?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                interaction_id = %interaction_id,
                task_id = %task.id,
                normalized_steps = %truncate_debug_for_log(&normalized.plan.steps, MAX_LOG_CHARS),
                "orchestrator normalized plan detail"
            );
        }
        let mut dag = normalized.dag;
        restore_checkpoint(&mut dag, task);
        if let Some(event) = resume_event {
            complete_wait_step_for_resume(&mut dag, plan, &task.completed_step_ids, event);
        }

        let mut ws = WorkingSet::new();
        ws.import_task_data(task.working_set_snapshot.clone());
        if let Some(event) = resume_event {
            apply_resume_event_to_working_set(&mut ws, event);
        }
        let working_set = Arc::new(RwLock::new(ws));
        let progress_reporter = Arc::new(RuntimeProgressReporter::new(
            self.thread_runtime.thread_id().await,
            interaction_id.to_string(),
            self.thread_runtime.event_store.clone(),
            self.thread_runtime.event_bus.clone(),
        ));
        let exec_ctx = ExecutorContext::new(
            task.id.clone(),
            working_set.clone(),
            self.reference_store.clone(),
        )
        .with_progress_reporter(progress_reporter);
        let result = self.executor.execute(&mut dag, &exec_ctx).await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            result = %truncate_debug_for_log(&result, MAX_LOG_CHARS),
            "orchestrator execution completed"
        );
        let working_set_snapshot = {
            let ws_guard = working_set.read().await;
            ws_guard.export_task_data()
        };
        let completed_step_ids = dag
            .completed_nodes()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        Ok(PlanExecutionSnapshot {
            result,
            completed_step_ids,
            working_set_snapshot,
        })
    }

    async fn build_recovery_plan(
        &self,
        task: &Task,
        plan: &orchestral_core::types::Plan,
        failed_step_id: &str,
        error: &str,
        interaction_id: &str,
    ) -> Result<Option<orchestral_core::types::Plan>, OrchestratorError> {
        let cut = locate_recovery_cut_step(
            plan,
            failed_step_id,
            error,
            &task.completed_step_ids,
            &task.working_set_snapshot,
        )?;
        let affected = affected_subgraph_steps(plan, &cut.cut_step_id);
        if affected.is_empty() {
            return Ok(None);
        }
        self.emit_lifecycle_event(
            "replanning_cut_selected",
            Some(interaction_id),
            Some(&task.id),
            Some("selected recovery cut step"),
            serde_json::json!({
                "failed_step_id": failed_step_id,
                "cut_step_id": cut.cut_step_id,
                "reason": cut.reason,
                "confidence": cut.confidence,
                "affected_steps": affected,
            }),
        )
        .await;
        let actions = self.available_actions().await;
        let history = self.history_for_planner(interaction_id, &task.id).await?;
        let runtime_info = PlannerRuntimeInfo::detect();
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info);

        let recovery_intent = build_recovery_intent(
            &task.intent,
            plan,
            failed_step_id,
            &cut.cut_step_id,
            error,
            &affected,
            &task.completed_step_ids,
        );
        let recovery_output = self.planner.plan(&recovery_intent, &context).await?;
        let recovery_plan = match recovery_output {
            PlannerOutput::Workflow(plan) => plan,
            PlannerOutput::DirectResponse(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "recovery planner returned direct_response; workflow required".to_string(),
                )));
            }
            PlannerOutput::Clarification(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "recovery planner returned clarification; workflow required".to_string(),
                )));
            }
        };
        let patched =
            patch_plan_with_recovery(plan, &cut.cut_step_id, &affected, &recovery_plan.steps)?;
        Ok(Some(patched))
    }

    async fn available_actions(&self) -> Vec<ActionMeta> {
        let mut actions = Vec::new();
        let registry = self.executor.action_registry.read().await;
        for name in registry.names() {
            if let Some(action) = registry.get(&name) {
                actions.push(extract_meta(action.as_ref()));
            }
        }
        actions
    }

    async fn history_for_planner(
        &self,
        interaction_id: &str,
        task_id: &str,
    ) -> Result<Vec<HistoryItem>, OrchestratorError> {
        if let Some(builder) = &self.context_builder {
            let request = ContextRequest {
                thread_id: self.thread_runtime.thread_id().await,
                task_id: Some(task_id.to_string()),
                interaction_id: Some(interaction_id.to_string()),
                query: None,
                budget: self.config.context_budget.clone(),
                include_history: self.config.include_history,
                include_references: self.config.include_references,
                ref_type_filter: None,
                tags: Vec::new(),
            };
            let window = builder.build(&request).await?;
            return Ok(context_window_to_history(&window));
        }

        let mut events = self
            .thread_runtime
            .query_history(self.config.history_limit)
            .await?;
        events.sort_by(|a, b| a.timestamp().cmp(&b.timestamp()));
        Ok(events.iter().filter_map(event_to_history_item).collect())
    }

    async fn emit_lifecycle_event(
        &self,
        event_type: &str,
        interaction_id: Option<&str>,
        task_id: Option<&str>,
        message: Option<&str>,
        metadata: Value,
    ) {
        let thread_id = self.thread_runtime.thread_id().await;
        let mut payload = serde_json::Map::new();
        payload.insert(
            "category".to_string(),
            Value::String("runtime_lifecycle".to_string()),
        );
        payload.insert(
            "event_type".to_string(),
            Value::String(event_type.to_string()),
        );
        if let Some(interaction_id) = interaction_id {
            payload.insert(
                "interaction_id".to_string(),
                Value::String(interaction_id.to_string()),
            );
        }
        if let Some(task_id) = task_id {
            payload.insert("task_id".to_string(), Value::String(task_id.to_string()));
        }
        if let Some(message) = message {
            payload.insert("message".to_string(), Value::String(message.to_string()));
        }
        if !metadata.is_null() {
            payload.insert("metadata".to_string(), metadata);
        }

        let trace = Event::trace(thread_id, "info", Value::Object(payload));
        if let Err(err) = self.thread_runtime.event_store.append(trace.clone()).await {
            tracing::warn!(
                event_type = %event_type,
                error = %err,
                "failed to persist lifecycle event"
            );
            return;
        }

        if let Err(err) = self.thread_runtime.event_bus.publish(trace).await {
            tracing::warn!(
                event_type = %event_type,
                error = %err,
                "failed to publish lifecycle event"
            );
        }
    }

    async fn emit_interpreted_output(
        &self,
        interaction_id: &str,
        task: &Task,
        result: &ExecutionResult,
    ) {
        let Some(plan) = task.plan.clone() else {
            return;
        };
        let request = InterpretRequest {
            intent: task.intent.content.clone(),
            plan,
            execution_result: result.clone(),
            completed_step_ids: task.completed_step_ids.clone(),
            working_set_snapshot: task.working_set_snapshot.clone(),
        };
        let stream_sink = Arc::new(RuntimeInterpreterDeltaSink::new(
            self.thread_runtime.thread_id().await,
            interaction_id.to_string(),
            task.id.clone(),
            self.thread_runtime.event_store.clone(),
            self.thread_runtime.event_bus.clone(),
        ));
        let interpreted = match self
            .result_interpreter
            .interpret_stream(request.clone(), Some(stream_sink))
            .await
        {
            Ok(output) => output,
            Err(err) => {
                tracing::warn!(
                    task_id = %task.id,
                    interaction_id = %interaction_id,
                    error = %err,
                    "result interpretation failed"
                );
                match NoopResultInterpreter.interpret(request).await {
                    Ok(fallback) => fallback,
                    Err(fallback_err) => {
                        tracing::warn!(
                            task_id = %task.id,
                            interaction_id = %interaction_id,
                            error = %fallback_err,
                            "fallback interpretation failed"
                        );
                        return;
                    }
                }
            }
        };
        self.emit_assistant_output_message(
            interaction_id,
            &task.id,
            interpreted.message,
            interpreted.metadata,
            Some(execution_result_metadata(result)),
        )
        .await;
    }

    async fn emit_assistant_output_message(
        &self,
        interaction_id: &str,
        task_id: &str,
        message: String,
        metadata: Value,
        result: Option<Value>,
    ) {
        let mut payload = serde_json::Map::new();
        payload.insert("task_id".to_string(), Value::String(task_id.to_string()));
        payload.insert("message".to_string(), Value::String(message.clone()));
        if !metadata.is_null() {
            payload.insert("metadata".to_string(), metadata);
        }
        if let Some(result) = result {
            if !result.is_null() {
                payload.insert("result".to_string(), result);
            }
        }
        let assistant_event = Event::assistant_output(
            self.thread_runtime.thread_id().await,
            interaction_id,
            Value::Object(payload),
        );
        if let Err(err) = self
            .thread_runtime
            .event_store
            .append(assistant_event.clone())
            .await
        {
            tracing::warn!(
                task_id = %task_id,
                interaction_id = %interaction_id,
                error = %err,
                "failed to persist assistant output"
            );
            return;
        }
        if let Err(err) = self.thread_runtime.event_bus.publish(assistant_event).await {
            tracing::warn!(
                task_id = %task_id,
                interaction_id = %interaction_id,
                error = %err,
                "failed to publish assistant output"
            );
            return;
        }
        tracing::info!(
            task_id = %task_id,
            interaction_id = %interaction_id,
            message = %truncate_for_log(&message, 400),
            "assistant output emitted"
        );
    }
}

struct RuntimeProgressReporter {
    thread_id: String,
    interaction_id: String,
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
}

impl RuntimeProgressReporter {
    fn new(
        thread_id: String,
        interaction_id: String,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            thread_id,
            interaction_id,
            event_store,
            event_bus,
        }
    }
}

#[async_trait]
impl ExecutionProgressReporter for RuntimeProgressReporter {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
        let mut payload = serde_json::Map::new();
        payload.insert(
            "category".to_string(),
            Value::String("execution_progress".to_string()),
        );
        payload.insert(
            "interaction_id".to_string(),
            Value::String(self.interaction_id.clone()),
        );
        payload.insert("task_id".to_string(), Value::String(event.task_id));
        payload.insert("phase".to_string(), Value::String(event.phase));
        if let Some(step_id) = event.step_id {
            payload.insert("step_id".to_string(), Value::String(step_id));
        }
        if let Some(action) = event.action {
            payload.insert("action".to_string(), Value::String(action));
        }
        if let Some(message) = event.message {
            payload.insert("message".to_string(), Value::String(message));
        }
        if !event.metadata.is_null() {
            payload.insert("metadata".to_string(), event.metadata);
        }

        let trace = Event::trace(
            self.thread_id.clone(),
            "info",
            Value::Object(payload.clone()),
        );
        self.event_store
            .append(trace.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.event_bus
            .publish(trace)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}

struct RuntimeInterpreterDeltaSink {
    thread_id: String,
    interaction_id: String,
    task_id: String,
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
}

impl RuntimeInterpreterDeltaSink {
    fn new(
        thread_id: String,
        interaction_id: String,
        task_id: String,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            thread_id,
            interaction_id,
            task_id,
            event_store,
            event_bus,
        }
    }
}

#[async_trait]
impl InterpretDeltaSink for RuntimeInterpreterDeltaSink {
    async fn on_delta(&self, delta: &str) {
        if delta.is_empty() {
            return;
        }
        let trace = Event::trace(
            self.thread_id.clone(),
            "info",
            serde_json::json!({
                "category": "assistant_stream",
                "interaction_id": self.interaction_id,
                "task_id": self.task_id,
                "delta": delta,
                "done": false,
            }),
        );
        let _ = self.event_store.append(trace.clone()).await;
        let _ = self.event_bus.publish(trace).await;
    }

    async fn on_done(&self) {
        let trace = Event::trace(
            self.thread_id.clone(),
            "info",
            serde_json::json!({
                "category": "assistant_stream",
                "interaction_id": self.interaction_id,
                "task_id": self.task_id,
                "done": true,
            }),
        );
        let _ = self.event_store.append(trace.clone()).await;
        let _ = self.event_bus.publish(trace).await;
    }
}

struct PlanExecutionSnapshot {
    result: ExecutionResult,
    completed_step_ids: Vec<String>,
    working_set_snapshot: HashMap<String, Value>,
}

struct RecoveryCutStep {
    cut_step_id: String,
    reason: String,
    confidence: f32,
}

fn affected_subgraph_steps(plan: &orchestral_core::types::Plan, cut_step_id: &str) -> Vec<String> {
    if plan.get_step(cut_step_id).is_none() {
        return Vec::new();
    }
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for step in &plan.steps {
        for dep in &step.depends_on {
            dependents
                .entry(dep.as_str())
                .or_default()
                .push(step.id.as_str());
        }
    }
    let mut stack = vec![cut_step_id.to_string()];
    let mut visited = std::collections::HashSet::new();
    while let Some(step_id) = stack.pop() {
        if !visited.insert(step_id.clone()) {
            continue;
        }
        if let Some(next) = dependents.get(step_id.as_str()) {
            for child in next {
                stack.push((*child).to_string());
            }
        }
    }
    visited.into_iter().collect()
}

fn locate_recovery_cut_step(
    plan: &orchestral_core::types::Plan,
    failed_step_id: &str,
    error: &str,
    completed_step_ids: &[String],
    _working_set_snapshot: &HashMap<String, Value>,
) -> Result<RecoveryCutStep, OrchestratorError> {
    let failed_step = plan.get_step(failed_step_id).ok_or_else(|| {
        OrchestratorError::ResumeError(format!("failed step '{}' missing", failed_step_id))
    })?;
    let error_lower = error.to_ascii_lowercase();
    let data_contract_error = is_data_contract_error(&error_lower);

    if !data_contract_error {
        return Ok(RecoveryCutStep {
            cut_step_id: failed_step_id.to_string(),
            reason: "step-local/runtime error; keep cut at failed step".to_string(),
            confidence: 0.9,
        });
    }

    if let Some(dep_id) = find_error_referenced_dependency(failed_step, &error_lower) {
        if completed_step_ids.iter().any(|id| id == dep_id) {
            return Ok(RecoveryCutStep {
                cut_step_id: dep_id.to_string(),
                reason: "data-contract error references upstream dependency".to_string(),
                confidence: 0.82,
            });
        }
    }

    if let Some(dep_id) = failed_step
        .io_bindings
        .iter()
        .filter_map(|b| step_id_from_key(&b.from))
        .find(|dep| dep != &failed_step.id)
    {
        if completed_step_ids.iter().any(|id| id == &dep_id) {
            return Ok(RecoveryCutStep {
                cut_step_id: dep_id,
                reason: "data-contract error with upstream io_binding source".to_string(),
                confidence: 0.76,
            });
        }
    }

    if let Some(dep_id) = failed_step.depends_on.last() {
        if completed_step_ids.iter().any(|id| id == dep_id) {
            return Ok(RecoveryCutStep {
                cut_step_id: dep_id.clone(),
                reason: "data-contract error fallback to nearest upstream dependency".to_string(),
                confidence: 0.68,
            });
        }
    }

    Ok(RecoveryCutStep {
        cut_step_id: failed_step_id.to_string(),
        reason: "data-contract error but no reliable upstream producer, keep failed step"
            .to_string(),
        confidence: 0.55,
    })
}

fn is_data_contract_error(error_lower: &str) -> bool {
    const TOKENS: [&str; 11] = [
        "missing declared import",
        "missing required io binding",
        "schema validation failed",
        "invalid io binding",
        "invalid input",
        "invalid output",
        "parse error",
        "failed to parse",
        "json parse",
        "deserializ",
        "invalid format",
    ];
    TOKENS.iter().any(|token| error_lower.contains(token))
}

fn find_error_referenced_dependency<'a>(
    failed_step: &'a Step,
    error_lower: &str,
) -> Option<&'a str> {
    for dep in &failed_step.depends_on {
        if error_lower.contains(&dep.to_ascii_lowercase()) {
            return Some(dep.as_str());
        }
    }
    None
}

fn step_id_from_key(key: &str) -> Option<String> {
    let (step_id, _) = key.split_once('.')?;
    if step_id.is_empty() {
        None
    } else {
        Some(step_id.to_string())
    }
}

fn build_recovery_intent(
    original: &Intent,
    current_plan: &orchestral_core::types::Plan,
    failed_step_id: &str,
    cut_step_id: &str,
    error: &str,
    affected_steps: &[String],
    completed_step_ids: &[String],
) -> Intent {
    let mut context = original.context.clone().unwrap_or_default();
    context.metadata.insert(
        "recovery_mode".to_string(),
        Value::String("subgraph_patch".to_string()),
    );
    let content = format!(
        "Recovery planning request.\n\
Original goal: {}\n\
Original intent: {}\n\
Failed step: {}\n\
Recovery cut step: {}\n\
Error: {}\n\
Affected steps to replace: {}\n\
Completed immutable steps: {}\n\
Rules: return ONLY replacement steps for the affected subgraph; do not redesign completed immutable steps.",
        current_plan.goal,
        original.content,
        failed_step_id,
        cut_step_id,
        truncate_for_log(error, 600),
        serde_json::to_string(affected_steps).unwrap_or_else(|_| "[]".to_string()),
        serde_json::to_string(completed_step_ids).unwrap_or_else(|_| "[]".to_string()),
    );
    Intent::with_context(content, context)
}

fn patch_plan_with_recovery(
    current_plan: &orchestral_core::types::Plan,
    cut_step_id: &str,
    affected_steps: &[String],
    replacement_steps: &[Step],
) -> Result<orchestral_core::types::Plan, OrchestratorError> {
    if replacement_steps.is_empty() {
        return Err(OrchestratorError::ResumeError(
            "recovery planner returned empty replacement steps".to_string(),
        ));
    }
    let affected: std::collections::HashSet<&str> =
        affected_steps.iter().map(String::as_str).collect();
    let mut base_steps = current_plan
        .steps
        .iter()
        .filter(|s| !affected.contains(s.id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut used_ids: std::collections::HashSet<String> =
        base_steps.iter().map(|s| s.id.clone()).collect();
    let mut rename_map: HashMap<String, String> = HashMap::new();

    for step in replacement_steps {
        let mut unique = step.id.clone();
        if used_ids.contains(&unique) {
            let mut idx = 1usize;
            while used_ids.contains(&format!("r{}_{}", idx, step.id)) {
                idx = idx.saturating_add(1);
            }
            unique = format!("r{}_{}", idx, step.id);
        }
        used_ids.insert(unique.clone());
        rename_map.insert(step.id.clone(), unique);
    }

    let replacement_roots = current_plan
        .get_step(cut_step_id)
        .map(|s| s.depends_on.clone())
        .unwrap_or_default();

    let mut patched_replacements = Vec::with_capacity(replacement_steps.len());
    for step in replacement_steps {
        let mut patched = step.clone();
        if let Some(new_id) = rename_map.get(&step.id) {
            patched.id = new_id.clone();
        }
        patched.depends_on = step
            .depends_on
            .iter()
            .filter_map(|dep| {
                if affected.contains(dep.as_str()) {
                    rename_map.get(dep).cloned()
                } else {
                    Some(dep.clone())
                }
            })
            .collect();
        if patched.depends_on.is_empty() && !replacement_roots.is_empty() {
            patched.depends_on = replacement_roots.clone();
        }
        patched_replacements.push(patched);
    }

    base_steps.extend(patched_replacements);
    Ok(orchestral_core::types::Plan {
        goal: current_plan.goal.clone(),
        steps: base_steps,
        confidence: current_plan.confidence,
    })
}

fn intent_from_event(
    event: &Event,
    interaction_id: Option<String>,
) -> Result<Intent, OrchestratorError> {
    let mut metadata = HashMap::new();
    metadata.insert(
        "event_type".to_string(),
        Value::String(event_type_label(event).to_string()),
    );
    if let Some(id) = interaction_id {
        metadata.insert("interaction_id".to_string(), Value::String(id));
    }

    let context = IntentContext {
        thread_id: Some(event.thread_id().to_string()),
        previous_task_id: None,
        metadata,
    };

    match event {
        Event::UserInput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::ExternalEvent { kind, payload, .. } => {
            let content = format!("external:{} {}", kind, payload_to_string(payload));
            Ok(Intent::with_context(content, context))
        }
        Event::AssistantOutput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::SystemTrace { level, payload, .. } => Ok(Intent::with_context(
            format!("trace:{} {}", level, payload_to_string(payload)),
            context,
        )),
        Event::Artifact { reference_id, .. } => Ok(Intent::with_context(
            format!("artifact:{}", reference_id),
            context,
        )),
    }
}

fn payload_to_string(payload: &Value) -> String {
    if let Some(s) = payload.as_str() {
        return s.to_string();
    }
    for key in ["content", "message", "text"] {
        if let Some(s) = payload.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    payload.to_string()
}

fn event_to_history_item(event: &Event) -> Option<HistoryItem> {
    let timestamp = event.timestamp();
    match event {
        Event::UserInput { payload, .. } => Some(HistoryItem {
            role: "user".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        Event::AssistantOutput { payload, .. } => Some(HistoryItem {
            role: "assistant".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        Event::ExternalEvent { kind, payload, .. } => Some(HistoryItem {
            role: "system".to_string(),
            content: format!("external:{} {}", kind, payload_to_string(payload)),
            timestamp,
        }),
        Event::SystemTrace { level, payload, .. } => Some(HistoryItem {
            role: "system".to_string(),
            content: format!("trace:{} {}", level, payload_to_string(payload)),
            timestamp,
        }),
        Event::Artifact { reference_id, .. } => Some(HistoryItem {
            role: "system".to_string(),
            content: format!("artifact:{}", reference_id),
            timestamp,
        }),
    }
}

fn context_window_to_history(window: &ContextWindow) -> Vec<HistoryItem> {
    let mut items = Vec::new();
    for slice in window.core.iter().chain(window.optional.iter()) {
        items.push(HistoryItem {
            role: slice.role.clone(),
            content: slice.content.clone(),
            timestamp: slice.timestamp.unwrap_or_else(chrono::Utc::now),
        });
    }
    items
}

fn event_type_label(event: &Event) -> &'static str {
    match event {
        Event::UserInput { .. } => "user_input",
        Event::AssistantOutput { .. } => "assistant_output",
        Event::Artifact { .. } => "artifact",
        Event::ExternalEvent { .. } => "external_event",
        Event::SystemTrace { .. } => "system_trace",
    }
}

fn summarize_plan_steps(plan: &orchestral_core::types::Plan) -> Value {
    const STEP_PREVIEW_LIMIT: usize = 32;
    let steps: Vec<Value> = plan
        .steps
        .iter()
        .take(STEP_PREVIEW_LIMIT)
        .map(|step| {
            serde_json::json!({
                "id": step.id.clone(),
                "action": step.action.clone(),
                "kind": format!("{:?}", step.kind),
                "depends_on": step.depends_on.clone(),
                "io_bindings": step.io_bindings.clone(),
            })
        })
        .collect();

    if plan.steps.len() > STEP_PREVIEW_LIMIT {
        serde_json::json!({
            "items": steps,
            "truncated": true,
            "total_steps": plan.steps.len(),
        })
    } else {
        serde_json::json!({
            "items": steps,
            "truncated": false,
            "total_steps": plan.steps.len(),
        })
    }
}

fn execution_result_metadata(result: &ExecutionResult) -> Value {
    match result {
        ExecutionResult::Completed => serde_json::json!({
            "status": "completed"
        }),
        ExecutionResult::Failed { step_id, error } => serde_json::json!({
            "status": "failed",
            "step_id": step_id,
            "error": truncate_for_log(error, 400),
        }),
        ExecutionResult::WaitingUser {
            step_id,
            prompt,
            approval,
        } => {
            let mut meta = serde_json::json!({
                "status": "waiting_user",
                "step_id": step_id,
                "prompt": truncate_for_log(prompt, 400),
                "waiting_kind": "input",
            });
            if let Some(req) = approval {
                meta["waiting_kind"] = Value::String("approval".to_string());
                meta["approval_reason"] = Value::String(truncate_for_log(&req.reason, 400));
                if let Some(command) = &req.command {
                    meta["approval_command"] = Value::String(truncate_for_log(command, 400));
                }
            }
            meta
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => serde_json::json!({
            "status": "waiting_event",
            "step_id": step_id,
            "event_type": event_type,
        }),
    }
}

fn task_state_from_execution(result: &ExecutionResult) -> TaskState {
    match result {
        ExecutionResult::Completed => TaskState::Done,
        ExecutionResult::Failed { error, .. } => TaskState::Failed {
            reason: error.clone(),
            recoverable: false,
        },
        ExecutionResult::WaitingUser {
            prompt, approval, ..
        } => TaskState::WaitingUser {
            prompt: prompt.clone(),
            reason: approval
                .as_ref()
                .map(|a| WaitUserReason::Approval {
                    reason: a.reason.clone(),
                    command: a.command.clone(),
                })
                .unwrap_or(WaitUserReason::Input),
        },
        ExecutionResult::WaitingEvent { event_type, .. } => TaskState::WaitingEvent {
            event_type: event_type.clone(),
        },
    }
}

fn restore_checkpoint(dag: &mut orchestral_core::executor::ExecutionDag, task: &Task) {
    for step_id in &task.completed_step_ids {
        dag.mark_completed(step_id);
    }
}

fn complete_wait_step_for_resume(
    dag: &mut orchestral_core::executor::ExecutionDag,
    plan: &orchestral_core::types::Plan,
    completed_steps: &[String],
    event: &Event,
) {
    let target_kind = match event {
        Event::UserInput { .. } => StepKind::WaitUser,
        Event::ExternalEvent { .. } => StepKind::WaitEvent,
        _ => return,
    };

    if let Some(step) = plan
        .steps
        .iter()
        .find(|s| s.kind == target_kind && !completed_steps.iter().any(|id| id == &s.id))
    {
        dag.mark_completed(&step.id);
    }
}

fn apply_resume_event_to_working_set(ws: &mut WorkingSet, event: &Event) {
    match event {
        Event::UserInput { payload, .. } => {
            ws.set_task("resume_user_input", payload.clone());
            ws.set_task("last_event_payload", payload.clone());
        }
        Event::ExternalEvent { payload, kind, .. } => {
            ws.set_task("resume_external_event", payload.clone());
            ws.set_task("last_event_payload", payload.clone());
            ws.set_task("last_event_kind", Value::String(kind.clone()));
        }
        _ => {}
    }
}

fn interaction_state_from_task(state: &TaskState) -> InteractionState {
    match state {
        TaskState::Done => InteractionState::Completed,
        TaskState::Failed { .. } => InteractionState::Failed,
        TaskState::WaitingUser { .. } => InteractionState::WaitingUser,
        TaskState::WaitingEvent { .. } => InteractionState::WaitingEvent,
        TaskState::Paused => InteractionState::Paused,
        TaskState::Planning | TaskState::Executing => InteractionState::Active,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::executor::{ExecutionDag, ExecutionProgressEvent};
    use orchestral_core::types::{Plan, Step, StepIoBinding};
    use orchestral_stores::{BroadcastEventBus, EventBus, EventStore, InMemoryEventStore};
    use serde_json::json;

    #[test]
    fn test_complete_wait_step_for_resume_marks_wait_user() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::wait_user("wait"),
                Step::action("next", "noop").with_depends_on(vec!["wait".to_string()]),
            ],
        );
        let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
        let event = Event::user_input("thread-1", "int-1", json!({"text":"ok"}));

        complete_wait_step_for_resume(&mut dag, &plan, &[], &event);

        assert!(dag.completed_nodes().contains(&"wait"));
    }

    #[test]
    fn test_apply_resume_event_to_working_set_for_external_event() {
        let mut ws = WorkingSet::new();
        let event = Event::external("thread-1", "timer", json!({"due":true}));

        apply_resume_event_to_working_set(&mut ws, &event);

        assert_eq!(
            ws.get_task("resume_external_event"),
            Some(&json!({"due":true}))
        );
        assert_eq!(
            ws.get_task("last_event_payload"),
            Some(&json!({"due":true}))
        );
        assert_eq!(ws.get_task("last_event_kind"), Some(&json!("timer")));
    }

    #[test]
    fn test_runtime_progress_reporter_persists_and_publishes() {
        tokio_test::block_on(async {
            let event_store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());
            let event_bus = Arc::new(BroadcastEventBus::new(16));
            let mut sub = event_bus.subscribe();
            let reporter = RuntimeProgressReporter::new(
                "thread-1".to_string(),
                "int-1".to_string(),
                event_store.clone(),
                event_bus.clone(),
            );

            reporter
                .report(
                    ExecutionProgressEvent::new(
                        "task-1",
                        Some("s1".to_string()),
                        Some("echo".to_string()),
                        "step_started",
                    )
                    .with_message("running"),
                )
                .await
                .expect("report");

            let events = event_store
                .query_by_thread("thread-1")
                .await
                .expect("query");
            assert_eq!(events.len(), 1);
            match &events[0] {
                Event::SystemTrace { payload, .. } => {
                    assert_eq!(payload["category"], "execution_progress");
                    assert_eq!(payload["interaction_id"], "int-1");
                    assert_eq!(payload["task_id"], "task-1");
                    assert_eq!(payload["phase"], "step_started");
                }
                _ => panic!("expected system trace"),
            }

            let bus_event = sub.recv().await.expect("bus event");
            assert!(matches!(bus_event, Event::SystemTrace { .. }));
        });
    }

    #[test]
    fn test_locate_recovery_cut_step_backtracks_to_upstream_on_data_contract_error() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A", "file_read"),
                Step::action("B", "doc_parse")
                    .with_depends_on(vec!["A".to_string()])
                    .with_io_bindings(vec![StepIoBinding::required("A.content", "content")]),
                Step::action("C", "summarize").with_depends_on(vec!["B".to_string()]),
            ],
        );
        let completed = vec!["A".to_string()];
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "input schema validation failed at $.content",
            &completed,
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "A");
        let affected = affected_subgraph_steps(&plan, &cut.cut_step_id);
        let affected_set: std::collections::HashSet<_> = affected.into_iter().collect();
        assert!(affected_set.contains("A"));
        assert!(affected_set.contains("B"));
        assert!(affected_set.contains("C"));
    }

    #[test]
    fn test_locate_recovery_cut_step_keeps_failed_step_on_runtime_error() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A", "file_read"),
                Step::action("B", "http").with_depends_on(vec!["A".to_string()]),
            ],
        );
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "request timeout after 10s",
            &["A".to_string()],
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "B");
    }

    #[test]
    fn test_locate_recovery_cut_step_does_not_backtrack_to_unfinished_dependency() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A", "file_read"),
                Step::action("B", "doc_parse")
                    .with_depends_on(vec!["A".to_string()])
                    .with_io_bindings(vec![StepIoBinding::required("A.content", "content")]),
            ],
        );
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "missing required io binding 'content'",
            &[],
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "B");
    }

    #[test]
    fn test_locate_recovery_cut_step_prefers_error_referenced_dependency() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A1", "fetch_profile"),
                Step::action("A2", "fetch_policy"),
                Step::action("B", "merge")
                    .with_depends_on(vec!["A1".to_string(), "A2".to_string()])
                    .with_io_bindings(vec![
                        StepIoBinding::required("A1.result", "profile"),
                        StepIoBinding::required("A2.result", "policy"),
                    ]),
            ],
        );
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "schema validation failed: dependency A2 returned invalid format",
            &["A1".to_string(), "A2".to_string()],
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "A2");
    }
}
