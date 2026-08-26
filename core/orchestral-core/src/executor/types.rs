use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::types::{StepId, StepKind, WorkflowId};
use crate::workflow_state::WorkingSet;

use super::StepOutcome;

/// Fully resolved request presented to a run-scoped step execution boundary.
///
/// The request contains execution data only. Run authority, Tool grants, and
/// approval capabilities must be captured by the Host-owned port rather than
/// placed in model- or planner-controlled step parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct StepExecutionRequest {
    /// Logical Step identity from the normalized Plan.
    pub step_id: StepId,
    /// Control semantics declared by the normalized Step.
    pub step_kind: StepKind,
    /// Planner-selected action/tool name.
    pub action: String,
    /// Identity of this concrete attempt, stable for the duration of the call.
    pub execution_id: String,
    /// Parameters after WorkingSet bindings and templates have been resolved.
    pub resolved_params: Value,
}

/// The mandatory run-scoped execution seam for every executable DAG Step.
#[async_trait]
pub trait StepExecutionPort: Send + Sync {
    /// Execute one fully resolved Step without falling back to legacy dispatch.
    async fn execute_step(
        &self,
        request: StepExecutionRequest,
        ctx: &ExecutorContext,
    ) -> StepOutcome;
}

/// Executor context
pub struct ExecutorContext {
    /// Working set for inter-step communication
    pub working_set: Arc<RwLock<WorkingSet>>,
    /// Task ID
    pub workflow_id: WorkflowId,
    /// Root cancellation token for this execution.
    ///
    /// Every action receives a child of this token so cancelling the execution
    /// is propagated without allowing one action to cancel its siblings.
    pub cancellation_token: CancellationToken,
    /// Optional execution progress reporter.
    pub progress_reporter: Option<Arc<dyn ExecutionProgressReporter>>,
    /// Host-owned execution boundary for this Run. It is mandatory so the DAG
    /// cannot fall back to an unguarded Action or nested Agent executor.
    pub step_execution_port: Arc<dyn StepExecutionPort>,
}

impl ExecutorContext {
    /// Create a new executor context
    pub fn new(
        workflow_id: impl Into<WorkflowId>,
        working_set: Arc<RwLock<WorkingSet>>,
        step_execution_port: Arc<dyn StepExecutionPort>,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            working_set,
            cancellation_token: CancellationToken::new(),
            progress_reporter: None,
            step_execution_port,
        }
    }

    /// Attach the root cancellation token owned by the execution caller.
    pub fn with_cancellation_token(mut self, cancellation_token: CancellationToken) -> Self {
        self.cancellation_token = cancellation_token;
        self
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
    WaitingUser { step_id: StepId, prompt: String },
    /// Waiting for external event
    WaitingEvent { step_id: StepId, event_type: String },
}

/// Realtime execution progress event.
#[derive(Debug, Clone)]
pub struct ExecutionProgressEvent {
    pub workflow_id: WorkflowId,
    pub step_id: Option<StepId>,
    pub action: Option<String>,
    /// Phase label, e.g. step_started/step_completed/workflow_completed.
    pub phase: String,
    /// Optional human-readable message.
    pub message: Option<String>,
    /// Extra structured metadata.
    pub metadata: serde_json::Value,
}

impl ExecutionProgressEvent {
    pub fn new(
        workflow_id: impl Into<WorkflowId>,
        step_id: Option<StepId>,
        action: Option<String>,
        phase: impl Into<String>,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
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
