//! Executor module
//!
//! The Executor is responsible for:
//! - DAG-based topological scheduling
//! - Parallel execution of ready nodes
//! - State-driven execution

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;

use crate::action::{Action, ActionContext, ActionInput, ActionResult, ApprovalRequest};
use crate::store::{ReferenceStore, WorkingSet};
use crate::types::{Plan, Step, StepId, StepKind, TaskId};

const MAX_LOG_TEXT_CHARS: usize = 2_000;
const MAX_LOG_JSON_CHARS: usize = 8_000;
const DEFAULT_MAX_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn truncate_json_for_log(value: &Value, max_chars: usize) -> String {
    truncate_for_log(&value.to_string(), max_chars)
}

fn truncate_json_map_for_log(map: &HashMap<String, Value>, max_chars: usize) -> String {
    let as_value = Value::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<serde_json::Map<String, Value>>(),
    );
    truncate_json_for_log(&as_value, max_chars)
}

/// Node state in the execution DAG
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    /// Not yet ready to execute
    Pending,
    /// Ready to execute (all dependencies completed)
    Ready,
    /// Currently executing
    Running,
    /// Execution completed successfully
    Completed,
    /// Execution failed
    Failed,
    /// Skipped (e.g., due to conditional logic)
    Skipped,
}

/// A node in the execution DAG
#[derive(Debug, Clone)]
pub struct DagNode {
    /// The step definition
    pub step: Step,
    /// Current state
    pub state: NodeState,
    /// Steps this node depends on
    pub depends_on: Vec<StepId>,
    /// Steps that depend on this node (reverse dependencies)
    pub dependents: Vec<StepId>,
    /// Execution ID for this run (distinguishes retry/resume)
    pub execution_id: String,
}

impl DagNode {
    /// Create a new DAG node from a step
    pub fn new(step: Step) -> Self {
        let depends_on = step.depends_on.clone();
        Self {
            step,
            state: NodeState::Pending,
            depends_on,
            dependents: Vec::new(),
            execution_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Check if all dependencies are completed
    pub fn dependencies_satisfied(&self, nodes: &HashMap<String, DagNode>) -> bool {
        self.depends_on.iter().all(|dep_id| {
            nodes
                .get(dep_id.as_str())
                .map(|n| n.state == NodeState::Completed)
                .unwrap_or(false)
        })
    }

    /// Generate a new execution ID (for retry)
    pub fn new_execution_id(&mut self) {
        self.execution_id = uuid::Uuid::new_v4().to_string();
    }
}

/// Execution DAG - supports dynamic modification
#[derive(Debug, Clone)]
pub struct ExecutionDag {
    /// All nodes in the DAG
    pub nodes: HashMap<String, DagNode>,
    /// Currently ready nodes (in-degree = 0 among non-completed)
    pub ready_nodes: Vec<String>,
    /// Whether dynamic modification is allowed
    pub dynamic: bool,
}

impl ExecutionDag {
    /// Create a new empty DAG
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            ready_nodes: Vec::new(),
            dynamic: false,
        }
    }

    /// Create a dynamic DAG (allows modification during execution)
    pub fn new_dynamic() -> Self {
        Self {
            nodes: HashMap::new(),
            ready_nodes: Vec::new(),
            dynamic: true,
        }
    }

    /// Build a DAG from a plan
    pub fn from_plan(plan: &Plan) -> Result<Self, String> {
        let mut dag = Self::new();

        // Create all nodes
        for step in &plan.steps {
            let node = DagNode::new(step.clone());
            dag.nodes.insert(step.id.to_string(), node);
        }

        // Build reverse dependencies
        for step in &plan.steps {
            for dep_id in &step.depends_on {
                if let Some(dep_node) = dag.nodes.get_mut(dep_id.as_str()) {
                    dep_node.dependents.push(step.id.clone());
                } else {
                    return Err(format!("Dependency '{}' not found", dep_id));
                }
            }
        }

        // Find initial ready nodes
        dag.update_ready_nodes();

        Ok(dag)
    }

    /// Update the list of ready nodes
    pub fn update_ready_nodes(&mut self) {
        self.ready_nodes = self
            .nodes
            .iter()
            .filter(|(_, node)| {
                matches!(node.state, NodeState::Pending | NodeState::Ready)
                    && node.dependencies_satisfied(&self.nodes)
            })
            .map(|(id, _)| id.clone())
            .collect();

        // Mark ready nodes
        for id in &self.ready_nodes {
            if let Some(node) = self.nodes.get_mut(id) {
                if node.state == NodeState::Pending {
                    node.state = NodeState::Ready;
                }
            }
        }
    }

    /// Get a node by ID
    pub fn get_node(&self, id: &str) -> Option<&DagNode> {
        self.nodes.get(id)
    }

    /// Get a mutable node by ID
    pub fn get_node_mut(&mut self, id: &str) -> Option<&mut DagNode> {
        self.nodes.get_mut(id)
    }

    /// Mark a node as running
    pub fn mark_running(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.state = NodeState::Running;
            self.ready_nodes.retain(|n| n != id);
        }
    }

    /// Mark a node as completed and update ready nodes
    pub fn mark_completed(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.state = NodeState::Completed;
        }
        self.update_ready_nodes();
    }

    /// Mark a node as failed
    pub fn mark_failed(&mut self, id: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.state = NodeState::Failed;
        }
    }

    /// Check if all nodes are completed
    pub fn is_completed(&self) -> bool {
        self.nodes.values().all(|n| n.state == NodeState::Completed)
    }

    /// Check if any node has failed
    pub fn has_failed(&self) -> bool {
        self.nodes.values().any(|n| n.state == NodeState::Failed)
    }

    /// Get all completed node IDs
    pub fn completed_nodes(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.state == NodeState::Completed)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get all failed node IDs
    pub fn failed_nodes(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.state == NodeState::Failed)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Add a new node dynamically (only if dynamic is enabled)
    pub fn add_node(&mut self, step: Step) -> Result<(), String> {
        if !self.dynamic {
            return Err("DAG is not dynamic".to_string());
        }

        // Validate dependencies exist
        for dep_id in &step.depends_on {
            if !self.nodes.contains_key(dep_id.as_str()) {
                return Err(format!("Dependency '{}' not found", dep_id));
            }
        }

        let id = step.id.clone();
        let node = DagNode::new(step);

        // Update reverse dependencies
        for dep_id in &node.depends_on {
            if let Some(dep_node) = self.nodes.get_mut(dep_id.as_str()) {
                dep_node.dependents.push(id.clone());
            }
        }

        self.nodes.insert(id.to_string(), node);
        self.update_ready_nodes();

        Ok(())
    }
}

impl Default for ExecutionDag {
    fn default() -> Self {
        Self::new()
    }
}

/// Action registry for looking up actions by name
pub struct ActionRegistry {
    actions: HashMap<String, Arc<dyn Action>>,
}

impl ActionRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            actions: HashMap::new(),
        }
    }

    /// Register an action
    pub fn register(&mut self, action: Arc<dyn Action>) {
        self.actions.insert(action.name().to_string(), action);
    }

    /// Get an action by name
    pub fn get(&self, name: &str) -> Option<Arc<dyn Action>> {
        self.actions.get(name).cloned()
    }

    /// Get all action names
    pub fn names(&self) -> Vec<String> {
        self.actions.keys().cloned().collect()
    }
}

impl Default for ActionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Executor context
pub struct ExecutorContext {
    /// Working set for inter-step communication
    pub working_set: Arc<RwLock<WorkingSet>>,
    /// Reference store for historical artifacts
    pub reference_store: Arc<dyn ReferenceStore>,
    /// Task ID
    pub task_id: TaskId,
    /// Optional execution progress reporter.
    pub progress_reporter: Option<Arc<dyn ExecutionProgressReporter>>,
}

impl ExecutorContext {
    /// Create a new executor context
    pub fn new(
        task_id: impl Into<TaskId>,
        working_set: Arc<RwLock<WorkingSet>>,
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            working_set,
            reference_store,
            progress_reporter: None,
        }
    }

    /// Attach a realtime execution progress reporter.
    pub fn with_progress_reporter(mut self, reporter: Arc<dyn ExecutionProgressReporter>) -> Self {
        self.progress_reporter = Some(reporter);
        self
    }
}

/// Execution result
#[derive(Debug, Clone)]
pub enum ExecutionResult {
    /// All steps completed successfully
    Completed,
    /// Execution failed
    Failed { step_id: StepId, error: String },
    /// Waiting for user input
    WaitingUser {
        step_id: StepId,
        prompt: String,
        approval: Option<ApprovalRequest>,
    },
    /// Waiting for external event
    WaitingEvent { step_id: StepId, event_type: String },
}

/// Realtime execution progress event.
#[derive(Debug, Clone)]
pub struct ExecutionProgressEvent {
    pub task_id: TaskId,
    pub step_id: Option<StepId>,
    pub action: Option<String>,
    /// Phase label, e.g. step_started/step_completed/task_completed.
    pub phase: String,
    /// Optional human-readable message.
    pub message: Option<String>,
    /// Extra structured metadata.
    pub metadata: serde_json::Value,
}

impl ExecutionProgressEvent {
    pub fn new(
        task_id: impl Into<TaskId>,
        step_id: Option<StepId>,
        action: Option<String>,
        phase: impl Into<String>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            step_id,
            action,
            phase: phase.into(),
            message: None,
            metadata: serde_json::Value::Null,
        }
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Sink interface for execution progress reporting.
#[async_trait]
pub trait ExecutionProgressReporter: Send + Sync {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String>;
}

/// The executor - orchestrates DAG execution
pub struct Executor {
    /// Action registry
    pub action_registry: Arc<RwLock<ActionRegistry>>,
    /// Maximum parallel executions
    pub max_parallel: usize,
    /// Max retries for ActionResult::RetryableError (excluding initial attempt).
    pub max_retry_attempts: u32,
    /// Base delay for exponential backoff when action does not provide retry_after.
    pub retry_base_delay: Duration,
    /// Cap for exponential backoff delay.
    pub retry_max_delay: Duration,
    /// Whether declared exports are required at runtime.
    pub strict_exports: bool,
}

impl Executor {
    /// Create a new executor
    pub fn new(action_registry: ActionRegistry) -> Self {
        Self::with_registry(Arc::new(RwLock::new(action_registry)))
    }

    /// Create a new executor with a shared registry
    pub fn with_registry(action_registry: Arc<RwLock<ActionRegistry>>) -> Self {
        Self {
            action_registry,
            max_parallel: 4,
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            retry_base_delay: DEFAULT_RETRY_BASE_DELAY,
            retry_max_delay: DEFAULT_RETRY_MAX_DELAY,
            strict_exports: true,
        }
    }

    /// Set maximum parallel executions
    pub fn with_max_parallel(mut self, max: usize) -> Self {
        self.max_parallel = max;
        self
    }

    /// Configure retry policy for retryable action errors.
    pub fn with_retry_policy(
        mut self,
        max_retry_attempts: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Self {
        self.max_retry_attempts = max_retry_attempts;
        self.retry_base_delay = retry_base_delay;
        self.retry_max_delay = retry_max_delay.max(retry_base_delay);
        self
    }

    /// Configure strict runtime checks for step exports.
    pub fn with_export_contract(mut self, strict_exports: bool) -> Self {
        self.strict_exports = strict_exports;
        self
    }

    /// Execute a DAG
    pub async fn execute(&self, dag: &mut ExecutionDag, ctx: &ExecutorContext) -> ExecutionResult {
        loop {
            let ready: Vec<_> = dag.ready_nodes.clone();
            if ready.is_empty() {
                return self.resolve_no_ready_nodes(dag, ctx).await;
            }

            let batch: Vec<_> = ready.into_iter().take(self.max_parallel).collect();
            if let Some(waiting_result) = self.handle_wait_steps(dag, &batch, ctx).await {
                return waiting_result;
            }

            if let Some(result) = self.execute_batch(dag, batch, ctx).await {
                return result;
            }
        }
    }

    async fn resolve_no_ready_nodes(
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

    async fn handle_wait_steps(
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

    async fn execute_batch(
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

    async fn process_step_result(
        &self,
        dag: &mut ExecutionDag,
        step_id: String,
        step: Step,
        result: ActionResult,
        ctx: &ExecutorContext,
        terminal_result: &mut Option<ExecutionResult>,
    ) {
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

    async fn execute_step_with_retry(
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

    fn compute_retry_backoff(&self, retries_used: u32) -> Duration {
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
    async fn execute_step_data(
        &self,
        step: &Step,
        execution_id: &str,
        ctx: &ExecutorContext,
    ) -> ActionResult {
        let action = {
            let registry = self.action_registry.read().await;
            registry.get(&step.action)
        };

        let action = match action {
            Some(a) => a,
            None => return ActionResult::error(format!("Action '{}' not found", step.action)),
        };
        let action_meta = action.metadata();
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

        // Build input from io_bindings only.
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
        }

        if let Err(error) = validate_schema(
            &resolved_params,
            &action_meta.input_schema,
            "input",
            &step.id,
            &step.action,
        ) {
            return ActionResult::error(error);
        }

        let input = ActionInput::with_params(resolved_params);
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                task_id = %ctx.task_id,
                step_id = %step.id,
                action = %step.action,
                resolved_params = %truncate_json_for_log(&input.params, MAX_LOG_JSON_CHARS),
                "action input resolved"
            );
        }

        let action_ctx = ActionContext::new(
            ctx.task_id.clone(),
            step.id.clone(),
            execution_id.to_string(),
            ctx.working_set.clone(),
            ctx.reference_store.clone(),
        );

        let result = action.run(input, action_ctx).await;
        match result {
            ActionResult::Success { exports } => {
                if tracing::enabled!(tracing::Level::DEBUG) {
                    tracing::debug!(
                        task_id = %ctx.task_id,
                        step_id = %step.id,
                        action = %step.action,
                        exports = %truncate_json_map_for_log(&exports, MAX_LOG_JSON_CHARS),
                        "action returned success"
                    );
                }
                let export_value = serde_json::Value::Object(
                    exports
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                );
                if let Err(error) = validate_schema(
                    &export_value,
                    &action_meta.output_schema,
                    "output",
                    &step.id,
                    &step.action,
                ) {
                    return ActionResult::error(error);
                }
                ActionResult::Success { exports }
            }
            ActionResult::NeedClarification { question } => {
                tracing::info!(
                    task_id = %ctx.task_id,
                    step_id = %step.id,
                    action = %step.action,
                    question = %truncate_for_log(&question, MAX_LOG_TEXT_CHARS),
                    "action requested clarification"
                );
                ActionResult::NeedClarification { question }
            }
            ActionResult::NeedApproval { request } => {
                tracing::info!(
                    task_id = %ctx.task_id,
                    step_id = %step.id,
                    action = %step.action,
                    reason = %truncate_for_log(&request.reason, MAX_LOG_TEXT_CHARS),
                    command = ?request.command,
                    "action requested approval"
                );
                ActionResult::NeedApproval { request }
            }
            ActionResult::RetryableError {
                message,
                retry_after,
                attempt,
            } => {
                tracing::warn!(
                    task_id = %ctx.task_id,
                    step_id = %step.id,
                    action = %step.action,
                    message = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                    retry_after_ms = retry_after.map(|d| d.as_millis() as u64),
                    attempt = attempt,
                    "action returned retryable error"
                );
                ActionResult::RetryableError {
                    message,
                    retry_after,
                    attempt,
                }
            }
            ActionResult::Error { message } => {
                tracing::error!(
                    task_id = %ctx.task_id,
                    step_id = %step.id,
                    action = %step.action,
                    error = %truncate_for_log(&message, MAX_LOG_TEXT_CHARS),
                    "action returned terminal error"
                );
                ActionResult::Error { message }
            }
        }
    }
}

fn build_step_completion_metadata(
    action: &str,
    exports: &HashMap<String, serde_json::Value>,
) -> serde_json::Value {
    let mut export_keys: Vec<String> = exports.keys().cloned().collect();
    export_keys.sort();

    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "action".to_string(),
        serde_json::Value::String(action.to_string()),
    );
    metadata.insert(
        "export_count".to_string(),
        serde_json::Value::Number(serde_json::Number::from(export_keys.len())),
    );
    metadata.insert(
        "export_keys".to_string(),
        serde_json::Value::Array(
            export_keys
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );

    if let Some(path) = completion_path_from_exports(exports) {
        metadata.insert(
            "path".to_string(),
            serde_json::Value::String(truncate_for_log(path, 240)),
        );
    }
    if let Some(bytes) = exports.get("bytes").and_then(|v| v.as_u64()) {
        metadata.insert(
            "bytes".to_string(),
            serde_json::Value::Number(serde_json::Number::from(bytes)),
        );
    }
    if let Some(status) = exports.get("status").and_then(|v| v.as_i64()) {
        metadata.insert(
            "status".to_string(),
            serde_json::Value::Number(serde_json::Number::from(status)),
        );
    }
    if let Some(timed_out) = exports.get("timed_out").and_then(|v| v.as_bool()) {
        metadata.insert("timed_out".to_string(), serde_json::Value::Bool(timed_out));
    }

    if let Some(preview) = output_preview(exports) {
        metadata.insert(
            "output_preview".to_string(),
            serde_json::Value::String(preview),
        );
    }

    serde_json::Value::Object(metadata)
}

fn completion_path_from_exports(exports: &HashMap<String, serde_json::Value>) -> Option<&str> {
    const CANDIDATE_KEYS: [&str; 7] = [
        "path",
        "target_path",
        "output_path",
        "file_path",
        "destination_path",
        "dest_path",
        "target",
    ];
    for key in CANDIDATE_KEYS {
        if let Some(path) = exports.get(key).and_then(|v| v.as_str()) {
            let trimmed = path.trim();
            if !trimmed.is_empty() {
                return Some(path);
            }
        }
    }
    None
}

fn build_step_start_metadata(action: &str, params: &serde_json::Value) -> serde_json::Value {
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "action".to_string(),
        serde_json::Value::String(action.to_string()),
    );
    if let Some(summary) = summarize_step_input(action, params) {
        metadata.insert(
            "input_summary".to_string(),
            serde_json::Value::String(summary),
        );
    }
    serde_json::Value::Object(metadata)
}

fn summarize_step_input(action: &str, params: &serde_json::Value) -> Option<String> {
    let obj = params.as_object()?;
    match action {
        "shell" => {
            let command = obj.get("command").and_then(|v| v.as_str())?;
            let args = obj
                .get("args")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .unwrap_or_default();
            let line = if args.is_empty() {
                command.to_string()
            } else {
                format!("{} {}", command, args)
            };
            Some(truncate_for_log(&line, 240))
        }
        "http" => {
            let method = obj.get("method").and_then(|v| v.as_str()).unwrap_or("GET");
            let url = obj.get("url").and_then(|v| v.as_str())?;
            Some(truncate_for_log(&format!("{} {}", method, url), 240))
        }
        "file_read" | "file_write" => obj
            .get("path")
            .and_then(|v| v.as_str())
            .map(|p| truncate_for_log(p, 240)),
        _ => None,
    }
}

fn output_preview(exports: &HashMap<String, serde_json::Value>) -> Option<String> {
    for key in ["result", "content", "stdout", "stderr"] {
        if let Some(value) = exports.get(key).and_then(|v| v.as_str()) {
            return Some(truncate_for_log(value, 320));
        }
    }
    if let Some(headers) = exports.get("headers").and_then(|v| v.as_object()) {
        if !headers.is_empty() {
            let mut pairs = headers
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| format!("{}: {}", k, s)))
                .collect::<Vec<_>>();
            pairs.sort();
            let preview = pairs.into_iter().take(8).collect::<Vec<_>>().join("\n");
            if !preview.is_empty() {
                return Some(truncate_for_log(&preview, 320));
            }
        }
    }
    if let Some(value) = exports.get("body").and_then(|v| v.as_str()) {
        return Some(truncate_for_log(value, 320));
    }
    None
}

async fn report_progress(ctx: &ExecutorContext, event: ExecutionProgressEvent) {
    if let Some(reporter) = &ctx.progress_reporter {
        if let Err(err) = reporter.report(event).await {
            tracing::warn!("failed to report execution progress: {}", err);
        }
    }
}

fn choose_terminal_result(slot: &mut Option<ExecutionResult>, candidate: ExecutionResult) {
    let rank = |result: &ExecutionResult| match result {
        ExecutionResult::Failed { .. } => 3,
        ExecutionResult::WaitingUser { .. } => 2,
        ExecutionResult::WaitingEvent { .. } => 1,
        ExecutionResult::Completed => 0,
    };

    match slot {
        Some(current) if rank(current) >= rank(&candidate) => {}
        _ => *slot = Some(candidate),
    }
}

fn validate_schema(
    value: &serde_json::Value,
    schema: &serde_json::Value,
    label: &str,
    step_id: &StepId,
    action: &str,
) -> Result<(), String> {
    if schema.is_null() {
        return Ok(());
    }

    validate_value_against_schema(value, schema, "$").map_err(|reason| {
        format!(
            "Step '{}' action '{}' {} schema validation failed: {}",
            step_id, action, label, reason
        )
    })
}

fn validate_value_against_schema(
    value: &serde_json::Value,
    schema: &serde_json::Value,
    path: &str,
) -> Result<(), String> {
    let schema_obj = schema
        .as_object()
        .ok_or_else(|| format!("schema at '{}' must be an object", path))?;

    if let Some(type_spec) = schema_obj.get("type") {
        validate_json_type(value, type_spec, path)?;
    }

    if let Some(constant) = schema_obj.get("const") {
        if value != constant {
            return Err(format!("{} expected const {}", path, constant));
        }
    }

    if let Some(variants) = schema_obj.get("enum").and_then(|v| v.as_array()) {
        if !variants.iter().any(|candidate| candidate == value) {
            return Err(format!("{} is not one of the allowed enum values", path));
        }
    }

    if let Some(required) = schema_obj.get("required").and_then(|v| v.as_array()) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{} must be an object for required fields", path))?;
        for key in required.iter().filter_map(|v| v.as_str()) {
            if !object.contains_key(key) {
                return Err(format!("{} missing required field '{}'", path, key));
            }
        }
    }

    if let Some(properties) = schema_obj.get("properties").and_then(|v| v.as_object()) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{} must be an object for properties validation", path))?;
        for (key, property_schema) in properties {
            if let Some(child_value) = object.get(key) {
                let child_path = format!("{}.{}", path, key);
                validate_value_against_schema(child_value, property_schema, &child_path)?;
            }
        }

        if schema_obj
            .get("additionalProperties")
            .and_then(|v| v.as_bool())
            == Some(false)
        {
            for key in object.keys() {
                if !properties.contains_key(key) {
                    return Err(format!("{} contains unknown field '{}'", path, key));
                }
            }
        }
    }

    if let Some(item_schema) = schema_obj.get("items") {
        let array = value
            .as_array()
            .ok_or_else(|| format!("{} must be an array for items validation", path))?;
        for (idx, item) in array.iter().enumerate() {
            let item_path = format!("{}[{}]", path, idx);
            validate_value_against_schema(item, item_schema, &item_path)?;
        }
    }

    Ok(())
}

fn validate_json_type(
    value: &serde_json::Value,
    type_spec: &serde_json::Value,
    path: &str,
) -> Result<(), String> {
    let matches = |t: &str, v: &serde_json::Value| match t {
        "object" => v.is_object(),
        "array" => v.is_array(),
        "string" => v.is_string(),
        "number" => v.is_number(),
        "integer" => v.as_i64().is_some() || v.as_u64().is_some(),
        "boolean" => v.is_boolean(),
        "null" => v.is_null(),
        _ => false,
    };

    match type_spec {
        serde_json::Value::String(type_name) => {
            if matches(type_name, value) {
                Ok(())
            } else {
                Err(format!("{} expected type '{}'", path, type_name))
            }
        }
        serde_json::Value::Array(types) => {
            let mut any_match = false;
            for ty in types {
                if let Some(type_name) = ty.as_str() {
                    if matches(type_name, value) {
                        any_match = true;
                        break;
                    }
                }
            }
            if any_match {
                Ok(())
            } else {
                Err(format!("{} did not match any allowed types", path))
            }
        }
        _ => Err(format!("{} schema.type must be string or array", path)),
    }
}

fn bind_param_value(
    params: &mut serde_json::Value,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    match params {
        serde_json::Value::Object(map) => {
            map.insert(key.to_string(), value.clone());
            Ok(())
        }
        serde_json::Value::Null => {
            let mut map = serde_json::Map::new();
            map.insert(key.to_string(), value.clone());
            *params = serde_json::Value::Object(map);
            Ok(())
        }
        _ => Err("step.params must be an object (or null) when using io_bindings".to_string()),
    }
}

fn validate_declared_exports(
    step: &Step,
    exports: &HashMap<String, serde_json::Value>,
    strict_exports: bool,
) -> Result<(), String> {
    if !strict_exports || step.exports.is_empty() {
        return Ok(());
    }

    for key in &step.exports {
        match exports.get(key) {
            Some(value) if !value.is_null() => {}
            Some(_) => return Err(format!("Step '{}' export '{}' is null", step.id, key)),
            None => {
                return Err(format!(
                    "Step '{}' missing declared export '{}'",
                    step.id, key
                ))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{sleep, Duration};

    use crate::action::ActionMeta;
    use crate::store::{Reference, ReferenceStore, ReferenceType, StoreError};
    use crate::types::{Plan, StepIoBinding};

    struct NoopReferenceStore;

    #[async_trait]
    impl ReferenceStore for NoopReferenceStore {
        async fn add(&self, _reference: Reference) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get(&self, _id: &str) -> Result<Option<Reference>, StoreError> {
            Ok(None)
        }

        async fn query_by_type(
            &self,
            _ref_type: &ReferenceType,
        ) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn query_recent(&self, _limit: usize) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn delete(&self, _id: &str) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    struct CollectProgressReporter {
        events: Arc<RwLock<Vec<ExecutionProgressEvent>>>,
    }

    impl CollectProgressReporter {
        fn new() -> Self {
            Self {
                events: Arc::new(RwLock::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl ExecutionProgressReporter for CollectProgressReporter {
        async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
            self.events.write().await.push(event);
            Ok(())
        }
    }

    struct StaticAction {
        name: String,
        result: ActionResult,
        metadata: Option<ActionMeta>,
    }

    impl StaticAction {
        fn new(name: &str, result: ActionResult) -> Self {
            Self {
                name: name.to_string(),
                result,
                metadata: None,
            }
        }

        fn with_metadata(mut self, metadata: ActionMeta) -> Self {
            self.metadata = Some(metadata);
            self
        }
    }

    #[async_trait]
    impl Action for StaticAction {
        fn name(&self) -> &str {
            &self.name
        }

        fn description(&self) -> &str {
            "test action"
        }

        fn metadata(&self) -> ActionMeta {
            self.metadata
                .clone()
                .unwrap_or_else(|| ActionMeta::new(self.name(), self.description()))
        }

        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            self.result.clone()
        }
    }

    struct ConsumeContentAction;

    #[async_trait]
    impl Action for ConsumeContentAction {
        fn name(&self) -> &str {
            "consume_content"
        }

        fn description(&self) -> &str {
            "consume bound content"
        }

        async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
            match input.params.get("content").and_then(|v| v.as_str()) {
                Some(content) => {
                    ActionResult::success_with_one("written", Value::String(content.to_string()))
                }
                None => ActionResult::error("content binding not applied"),
            }
        }
    }

    struct SlowAction {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        delay_ms: u64,
    }

    impl SlowAction {
        fn new(active: Arc<AtomicUsize>, peak: Arc<AtomicUsize>, delay_ms: u64) -> Self {
            Self {
                active,
                peak,
                delay_ms,
            }
        }
    }

    struct FlakyRetryAction {
        failures_left: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
    }

    impl FlakyRetryAction {
        fn new(failures_left: Arc<AtomicUsize>, calls: Arc<AtomicUsize>) -> Self {
            Self {
                failures_left,
                calls,
            }
        }
    }

    #[async_trait]
    impl Action for FlakyRetryAction {
        fn name(&self) -> &str {
            "flaky_retry"
        }

        fn description(&self) -> &str {
            "returns retryable errors before succeeding"
        }

        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            let current_call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let left = self.failures_left.load(Ordering::SeqCst);
            if left > 0 {
                self.failures_left.fetch_sub(1, Ordering::SeqCst);
                return ActionResult::retryable("temporary failure", None, current_call as u32);
            }
            ActionResult::success_with_one("ok", json!(current_call))
        }
    }

    #[async_trait]
    impl Action for SlowAction {
        fn name(&self) -> &str {
            "slow"
        }

        fn description(&self) -> &str {
            "slow action for parallelism tests"
        }

        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            let in_flight = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let mut peak = self.peak.load(Ordering::SeqCst);
            while in_flight > peak {
                match self.peak.compare_exchange(
                    peak,
                    in_flight,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }
            sleep(Duration::from_millis(self.delay_ms)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            ActionResult::success()
        }
    }

    #[test]
    fn test_io_binding_injects_input_for_downstream_step() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success_with_one("guide_markdown", json!("hello world")),
            )));
            registry.register(Arc::new(ConsumeContentAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "binding flow",
                vec![
                    Step::action("s1", "produce").with_exports(vec!["guide_markdown".to_string()]),
                    Step::action("s2", "consume_content")
                        .with_depends_on(vec![StepId::from("s1")])
                        .with_io_bindings(vec![StepIoBinding::required(
                            "s1.guide_markdown",
                            "content",
                        )])
                        .with_exports(vec!["written".to_string()]),
                ],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let working_set = Arc::new(RwLock::new(WorkingSet::new()));
            let ctx =
                ExecutorContext::new("task-1", working_set.clone(), Arc::new(NoopReferenceStore));

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));

            let ws = working_set.read().await;
            assert_eq!(ws.get_task("s2.written"), Some(&json!("hello world")));
        });
    }

    #[test]
    fn test_missing_required_binding_fails_step() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(ConsumeContentAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "missing binding",
                vec![Step::action("s1", "consume_content")
                    .with_io_bindings(vec![StepIoBinding::required(
                        "missing.step_output",
                        "content",
                    )])
                    .with_exports(vec!["written".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("Missing required io binding"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_missing_declared_export_fails_step_when_strict() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success(),
            )));
            let executor = Executor::new(registry).with_export_contract(true);

            let plan = Plan::new(
                "strict exports",
                vec![Step::action("s1", "produce").with_exports(vec!["result".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("missing declared export"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_input_schema_validation_fails_on_wrong_type() {
        tokio_test::block_on(async {
            let action = StaticAction::new("typed_input", ActionResult::success()).with_metadata(
                ActionMeta::new("typed_input", "typed input").with_input_schema(json!({
                    "type": "object",
                    "properties": {
                        "count": { "type": "integer" }
                    },
                    "required": ["count"]
                })),
            );

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(action));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "typed input fail",
                vec![Step::action("s1", "typed_input").with_params(json!({
                    "count": "not-integer"
                }))],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("input schema validation failed"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_output_schema_validation_fails_on_wrong_type() {
        tokio_test::block_on(async {
            let action = StaticAction::new(
                "typed_output",
                ActionResult::success_with_one("bytes", json!("wrong")),
            )
            .with_metadata(
                ActionMeta::new("typed_output", "typed output").with_output_schema(json!({
                    "type": "object",
                    "properties": {
                        "bytes": { "type": "integer" }
                    },
                    "required": ["bytes"]
                })),
            );

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(action));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "typed output fail",
                vec![Step::action("s1", "typed_output")],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("output schema validation failed"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_progress_reporter_receives_step_lifecycle_events() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success_with_one("result", json!("ok")),
            )));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "progress flow",
                vec![Step::action("s1", "produce").with_exports(vec!["result".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let reporter = Arc::new(CollectProgressReporter::new());
            let events_ref = reporter.events.clone();
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            )
            .with_progress_reporter(reporter);

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));

            let events = events_ref.read().await.clone();
            let phases: Vec<String> = events.into_iter().map(|e| e.phase).collect();
            assert!(phases.iter().any(|p| p == "step_started"));
            assert!(phases.iter().any(|p| p == "step_completed"));
            assert!(phases.iter().any(|p| p == "task_completed"));
        });
    }

    #[test]
    fn test_step_completion_metadata_uses_target_path_when_present() {
        let mut exports = HashMap::new();
        exports.insert("target_path".to_string(), json!("output/统计人数.md"));
        let metadata = build_step_completion_metadata("doc_transform", &exports);
        assert_eq!(
            metadata.get("path").and_then(|v| v.as_str()),
            Some("output/统计人数.md")
        );
    }

    #[test]
    fn test_execute_with_max_parallel_one_keeps_remaining_ready_nodes() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new("noop", ActionResult::success())));
            let executor = Executor::new(registry).with_max_parallel(1);

            let plan = Plan::new(
                "fan-out",
                vec![
                    Step::action("s1", "noop"),
                    Step::action("s2", "noop"),
                    Step::action("s3", "noop"),
                ],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));
            assert_eq!(dag.completed_nodes().len(), 3);
        });
    }

    #[test]
    fn test_execute_runs_ready_steps_in_parallel_batches() {
        tokio_test::block_on(async {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(SlowAction::new(active.clone(), peak.clone(), 40)));
            let executor = Executor::new(registry).with_max_parallel(2);

            let plan = Plan::new(
                "parallel fan-out",
                vec![
                    Step::action("s1", "slow"),
                    Step::action("s2", "slow"),
                    Step::action("s3", "slow"),
                ],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));
            assert!(peak.load(Ordering::SeqCst) >= 2);
        });
    }

    #[test]
    fn test_retryable_error_retries_and_then_succeeds() {
        tokio_test::block_on(async {
            let failures_left = Arc::new(AtomicUsize::new(2));
            let calls = Arc::new(AtomicUsize::new(0));

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(FlakyRetryAction::new(
                failures_left.clone(),
                calls.clone(),
            )));
            let executor = Executor::new(registry).with_retry_policy(
                3,
                Duration::from_millis(0),
                Duration::from_millis(0),
            );

            let plan = Plan::new(
                "retry success",
                vec![Step::action("s1", "flaky_retry").with_exports(vec!["ok".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let working_set = Arc::new(RwLock::new(WorkingSet::new()));
            let ctx =
                ExecutorContext::new("task-1", working_set.clone(), Arc::new(NoopReferenceStore));

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));
            assert_eq!(calls.load(Ordering::SeqCst), 3);

            let ws = working_set.read().await;
            assert_eq!(ws.get_task("s1.ok"), Some(&json!(3)));
        });
    }

    #[test]
    fn test_retryable_error_exhausts_and_fails() {
        tokio_test::block_on(async {
            let failures_left = Arc::new(AtomicUsize::new(10));
            let calls = Arc::new(AtomicUsize::new(0));

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(FlakyRetryAction::new(
                failures_left.clone(),
                calls.clone(),
            )));
            let executor = Executor::new(registry).with_retry_policy(
                2,
                Duration::from_millis(0),
                Duration::from_millis(0),
            );

            let plan = Plan::new(
                "retry failure",
                vec![Step::action("s1", "flaky_retry").with_exports(vec!["ok".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new(
                "task-1",
                Arc::new(RwLock::new(WorkingSet::new())),
                Arc::new(NoopReferenceStore),
            );

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("retry exhausted"));
                }
                other => panic!("expected failed result, got {:?}", other),
            }
            // initial attempt + 2 retries
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        });
    }
}
