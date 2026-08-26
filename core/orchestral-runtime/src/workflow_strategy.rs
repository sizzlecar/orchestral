//! Guarded workflow execution built on the existing Plan/DAG/Executor assets.
//!
//! This module does not introduce a second scheduler. It gives the existing
//! Executor a run-scoped execution port whose only effect path is the same
//! [`AgentToolRuntime`] used by the Generic Agent loop.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::executor::{
    ExecutionDag, ExecutionProgressReporter, ExecutionResult, Executor, ExecutorContext,
    StepExecutionPort, StepExecutionRequest, StepOutcome,
};
use orchestral_core::normalizer::{NormalizeError, PlanNormalizer};
use orchestral_core::spi::{HookRegistry, RuntimeHookContext, RuntimeHookEventEnvelope, SpiMeta};
use orchestral_core::tool_protocol::{
    RunToolGrant, ToolCallId, ToolInvocation, ToolOutcome, ToolOutput,
};
use orchestral_core::types::{Plan, StepKind, WorkflowId};
use orchestral_core::workflow_state::WorkingSet;
use serde_json::Value;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::tool_runtime::{AgentToolRuntime, GuardedToolResult};

const DEFAULT_MAX_WORKFLOW_TOOL_CALLS: u64 = 32;

/// One invocation of the workflow execution strategy.
pub struct WorkflowExecutionRequest {
    pub run_id: RunId,
    pub workflow_id: WorkflowId,
    pub plan: Plan,
    pub run_grant: RunToolGrant,
    pub working_set: WorkingSet,
    pub cancellation: CancellationToken,
    /// Bridge into the owning Agent Run's live progress stream.
    pub progress_reporter: Option<Arc<dyn ExecutionProgressReporter>>,
    /// Optional Run-specific cap. The strategy's Host cap still wins.
    pub max_tool_calls: Option<u64>,
}

impl WorkflowExecutionRequest {
    pub fn new(
        run_id: RunId,
        workflow_id: WorkflowId,
        plan: Plan,
        run_grant: RunToolGrant,
    ) -> Self {
        Self {
            run_id,
            workflow_id,
            plan,
            run_grant,
            working_set: WorkingSet::new(),
            cancellation: CancellationToken::new(),
            progress_reporter: None,
            max_tool_calls: None,
        }
    }

    pub fn with_working_set(mut self, working_set: WorkingSet) -> Self {
        self.working_set = working_set;
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn with_progress_reporter(
        mut self,
        progress_reporter: Arc<dyn ExecutionProgressReporter>,
    ) -> Self {
        self.progress_reporter = Some(progress_reporter);
        self
    }

    pub fn with_max_tool_calls(mut self, max_tool_calls: u64) -> Self {
        self.max_tool_calls = Some(max_tool_calls);
        self
    }
}

/// Result of one normalized DAG execution.
///
/// The caller remains the Agent Run owner and decides how to journal or expose
/// this snapshot. The legacy `Task` type is not made authoritative here.
pub struct WorkflowExecutionSnapshot {
    pub result: ExecutionResult,
    pub normalized_plan: Plan,
    pub dag: ExecutionDag,
    pub working_set: HashMap<String, Value>,
    pub normalizer_fixes: Vec<String>,
    pub tool_calls: u64,
}

impl WorkflowExecutionSnapshot {
    /// Stable model-facing observation. It is an internal workflow result,
    /// never a second Agent Run terminal state.
    pub fn tool_result(&self) -> (Value, bool) {
        let mut output = serde_json::Map::from_iter([
            (
                "working_set".to_owned(),
                serde_json::to_value(&self.working_set).unwrap_or(Value::Null),
            ),
            (
                "normalizer_fixes".to_owned(),
                serde_json::to_value(&self.normalizer_fixes).unwrap_or(Value::Null),
            ),
            ("tool_calls".to_owned(), Value::from(self.tool_calls)),
        ]);
        let is_error = match &self.result {
            ExecutionResult::Completed => {
                output.insert("status".to_owned(), Value::String("completed".to_owned()));
                false
            }
            ExecutionResult::Failed { step_id, error } => {
                output.insert("status".to_owned(), Value::String("failed".to_owned()));
                output.insert("step_id".to_owned(), Value::String(step_id.to_string()));
                output.insert("error".to_owned(), Value::String(error.clone()));
                true
            }
            ExecutionResult::WaitingUser { step_id, prompt } => {
                output.insert(
                    "status".to_owned(),
                    Value::String("waiting_user".to_owned()),
                );
                output.insert("step_id".to_owned(), Value::String(step_id.to_string()));
                output.insert("prompt".to_owned(), Value::String(prompt.clone()));
                true
            }
            ExecutionResult::WaitingEvent {
                step_id,
                event_type,
            } => {
                output.insert(
                    "status".to_owned(),
                    Value::String("waiting_event".to_owned()),
                );
                output.insert("step_id".to_owned(), Value::String(step_id.to_string()));
                output.insert("event_type".to_owned(), Value::String(event_type.clone()));
                true
            }
        };
        (Value::Object(output), is_error)
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowExecutionError {
    #[error("workflow Plan normalization failed: {0}")]
    Normalize(#[from] NormalizeError),
    #[error("invalid workflow execution request: {0}")]
    InvalidRequest(String),
}

/// Thin strategy around the original normalizer and DAG executor.
pub struct WorkflowExecutionStrategy {
    normalizer: Arc<PlanNormalizer>,
    executor: Arc<Executor>,
    tools: Arc<dyn AgentToolRuntime>,
    hooks: Option<Arc<HookRegistry>>,
    max_tool_calls: u64,
}

impl WorkflowExecutionStrategy {
    pub fn new(
        normalizer: Arc<PlanNormalizer>,
        executor: Arc<Executor>,
        tools: Arc<dyn AgentToolRuntime>,
    ) -> Self {
        Self {
            normalizer,
            executor,
            tools,
            hooks: None,
            max_tool_calls: DEFAULT_MAX_WORKFLOW_TOOL_CALLS,
        }
    }

    /// Attaches the stable Runtime Hook SPI to guarded workflow Steps.
    pub fn with_hooks(mut self, hooks: Arc<HookRegistry>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    pub fn with_max_tool_calls(mut self, max_tool_calls: u64) -> Self {
        self.max_tool_calls = max_tool_calls;
        self
    }

    pub fn uses_tool_runtime(&self, runtime: &Arc<dyn AgentToolRuntime>) -> bool {
        Arc::ptr_eq(&self.tools, runtime)
    }

    pub(crate) fn recovery_contract(&self) -> serde_json::Value {
        serde_json::json!({
            "max_tool_calls": self.max_tool_calls,
            "hooks_enabled": self.hooks.is_some(),
        })
    }

    pub async fn execute(
        &self,
        request: WorkflowExecutionRequest,
    ) -> Result<WorkflowExecutionSnapshot, WorkflowExecutionError> {
        if request.run_id.is_empty() || request.workflow_id.as_str().trim().is_empty() {
            return Err(WorkflowExecutionError::InvalidRequest(
                "run_id and workflow_id must not be empty".to_owned(),
            ));
        }
        let progress_reporter = request.progress_reporter.clone().ok_or_else(|| {
            WorkflowExecutionError::InvalidRequest(
                "workflow execution must project progress into its owning Agent Run".to_owned(),
            )
        })?;
        if request
            .plan
            .steps
            .iter()
            .any(|step| !matches!(step.kind, StepKind::Action | StepKind::System))
        {
            return Err(WorkflowExecutionError::InvalidRequest(
                "Generic workflow currently accepts only Action and System steps".to_owned(),
            ));
        }
        request
            .run_grant
            .bounds
            .validate()
            .map_err(|error| WorkflowExecutionError::InvalidRequest(error.message))?;
        if self.max_tool_calls == 0 || request.max_tool_calls == Some(0) {
            return Err(WorkflowExecutionError::InvalidRequest(
                "Tool call limits must be positive".to_owned(),
            ));
        }

        let effective_tool_limit = request
            .max_tool_calls
            .unwrap_or(self.max_tool_calls)
            .min(self.max_tool_calls);
        let normalized = self.normalizer.normalize(request.plan)?;
        let mut dag = normalized.dag;
        let working_set = Arc::new(RwLock::new(request.working_set));
        let port = Arc::new(RunBoundGuardedToolPort::new(
            request.run_id,
            request.run_grant,
            self.tools.clone(),
            self.hooks.clone(),
            effective_tool_limit,
        ));
        let context = ExecutorContext::new(request.workflow_id, working_set.clone(), port.clone())
            .with_cancellation_token(request.cancellation)
            .with_progress_reporter(progress_reporter);
        let result = self.executor.execute(&mut dag, &context).await;
        let working_set = working_set.read().await.export_workflow_data();

        Ok(WorkflowExecutionSnapshot {
            result,
            normalized_plan: normalized.plan,
            dag,
            working_set,
            normalizer_fixes: normalized.fix_summary,
            tool_calls: port.tool_calls(),
        })
    }
}

/// Run-bound adapter from normalized DAG Steps to the guarded Tool boundary.
///
/// It deliberately has no `ActionRegistry` reference, so a missing or denied
/// Tool cannot fall back to legacy execution.
pub struct RunBoundGuardedToolPort {
    run_id: RunId,
    run_grant: RunToolGrant,
    tools: Arc<dyn AgentToolRuntime>,
    hooks: Option<Arc<HookRegistry>>,
    max_tool_calls: u64,
    tool_calls: AtomicU64,
}

impl RunBoundGuardedToolPort {
    fn new(
        run_id: RunId,
        run_grant: RunToolGrant,
        tools: Arc<dyn AgentToolRuntime>,
        hooks: Option<Arc<HookRegistry>>,
        max_tool_calls: u64,
    ) -> Self {
        Self {
            run_id,
            run_grant,
            tools,
            hooks,
            max_tool_calls,
            tool_calls: AtomicU64::new(0),
        }
    }

    pub fn tool_calls(&self) -> u64 {
        self.tool_calls.load(Ordering::Acquire)
    }

    fn reserve_tool_call(&self) -> bool {
        self.tool_calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.max_tool_calls).then_some(current + 1)
            })
            .is_ok()
    }
}

#[async_trait]
impl StepExecutionPort for RunBoundGuardedToolPort {
    async fn execute_step(
        &self,
        request: StepExecutionRequest,
        context: &ExecutorContext,
    ) -> StepOutcome {
        if let Err(error) = self
            .dispatch_step_hook(
                "before_step",
                &request,
                context,
                serde_json::json!({ "phase": "before" }),
            )
            .await
        {
            let result = StepOutcome::error(format!("before_step hook rejected Step: {error}"));
            return self.dispatch_step_error(&request, context, result).await;
        }

        let mut result = self.execute_guarded_step(&request, context).await;
        if let Err(error) = self
            .dispatch_step_hook(
                "after_step",
                &request,
                context,
                serde_json::json!({
                    "phase": "after",
                    "result": result,
                }),
            )
            .await
        {
            result = StepOutcome::error(format!("after_step hook rejected Step: {error}"));
        }
        if matches!(
            result,
            StepOutcome::Error { .. } | StepOutcome::RetryableError { .. }
        ) {
            result = self.dispatch_step_error(&request, context, result).await;
        }
        result
    }
}

impl RunBoundGuardedToolPort {
    async fn execute_guarded_step(
        &self,
        request: &StepExecutionRequest,
        context: &ExecutorContext,
    ) -> StepOutcome {
        if !matches!(request.step_kind, StepKind::Action | StepKind::System) {
            return StepOutcome::error(format!(
                "guarded Workflow does not execute {:?} steps through an Action fallback",
                request.step_kind
            ));
        }
        if !self.reserve_tool_call() {
            return StepOutcome::error("workflow Tool call limit reached");
        }
        let tool_id = match self.tools.resolve_tool_id(&request.action) {
            Ok(Some(tool_id)) => tool_id,
            Ok(None) => {
                return StepOutcome::error(format!(
                    "workflow Tool is not registered: {}",
                    request.action
                ))
            }
            Err(error) => {
                return StepOutcome::retryable(
                    format!("Tool Runtime unavailable: {error}"),
                    None,
                    0,
                )
            }
        };
        let result = self
            .tools
            .invoke(
                ToolInvocation {
                    run_id: self.run_id.clone(),
                    call_id: ToolCallId::new(format!(
                        "workflow:{}:{}",
                        request.step_id, request.execution_id
                    )),
                    tool_id,
                    arguments: request.resolved_params.clone(),
                },
                self.run_grant.clone(),
                None,
                context.cancellation_token.clone(),
            )
            .await;
        match result {
            GuardedToolResult::ApprovalRequired { .. } => StepOutcome::error(
                "Tool approval is required, but Workflow approval interaction is not connected",
            ),
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Completed { output },
                ..
            } => match output {
                ToolOutput::Inline(Value::Object(exports)) => {
                    StepOutcome::success_with(exports.into_iter().collect())
                }
                ToolOutput::Artifact(artifact) => StepOutcome::error(format!(
                    "workflow Tool output was spilled to Artifact {}; it cannot directly satisfy Step exports",
                    artifact.artifact.artifact_ref
                )),
                _ => StepOutcome::error(
                    "workflow Tool output must be an object so it can satisfy Step exports",
                ),
            },
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { code, message },
                ..
            } => StepOutcome::error(format!("Tool rejected [{code}]: {message}")),
            GuardedToolResult::Outcome {
                outcome:
                    ToolOutcome::Failed {
                        code,
                        message,
                        retryable,
                    },
                ..
            } if retryable => {
                StepOutcome::retryable(format!("Tool failed [{code}]: {message}"), None, 0)
            }
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Failed { code, message, .. },
                ..
            } => StepOutcome::error(format!("Tool failed [{code}]: {message}")),
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Cancelled,
                ..
            } => StepOutcome::error("workflow Tool execution cancelled"),
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::UnknownEffect { message },
                ..
            } => StepOutcome::error(format!("workflow Tool effect is unknown: {message}")),
            GuardedToolResult::Outcome { .. } => {
                StepOutcome::error("unsupported Tool outcome returned by Tool Runtime")
            }
        }
    }

    async fn dispatch_step_error(
        &self,
        request: &StepExecutionRequest,
        context: &ExecutorContext,
        result: StepOutcome,
    ) -> StepOutcome {
        let payload = serde_json::json!({
            "phase": "error",
            "result": result,
        });
        match self
            .dispatch_step_hook("on_step_error", request, context, payload)
            .await
        {
            Ok(()) => result,
            Err(error) => StepOutcome::error(format!(
                "{}; on_step_error hook rejected Step: {error}",
                step_error_message(&result)
            )),
        }
    }

    async fn dispatch_step_hook(
        &self,
        event_type: &str,
        request: &StepExecutionRequest,
        context: &ExecutorContext,
        payload: Value,
    ) -> Result<(), String> {
        let Some(hooks) = &self.hooks else {
            return Ok(());
        };
        hooks
            .dispatch_checked(
                &RuntimeHookEventEnvelope {
                    meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
                    event_type: event_type.to_owned(),
                    event_version: "1.0.0".to_owned(),
                    occurred_at_unix_ms: chrono::Utc::now().timestamp_millis(),
                    payload: serde_json::json!({
                        "run_id": self.run_id.as_str(),
                        "workflow_id": context.workflow_id.as_str(),
                        "step_id": request.step_id.as_str(),
                        "execution_id": request.execution_id,
                        "action": request.action,
                        "detail": payload,
                    }),
                    extensions: serde_json::Map::new(),
                },
                &RuntimeHookContext {
                    session_id: None,
                    run_id: Some(self.run_id.clone()),
                    workflow_id: Some(context.workflow_id.clone()),
                    step_id: Some(request.step_id.clone()),
                    tool_name: Some(request.action.clone()),
                    message: None,
                    metadata: serde_json::json!({ "run_id": self.run_id.as_str() }),
                    extensions: serde_json::Map::new(),
                },
            )
            .await
            .map_err(|error| error.to_string())
    }
}

fn step_error_message(result: &StepOutcome) -> &str {
    match result {
        StepOutcome::Error { message } | StepOutcome::RetryableError { message, .. } => message,
        _ => "Step failed",
    }
}
