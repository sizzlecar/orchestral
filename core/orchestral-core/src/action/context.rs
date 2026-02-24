//! ActionContext type definition

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::store::{ReferenceStore, WorkingSet};
use crate::types::{StepId, TaskId};

/// Execution context for actions
///
/// Provides access to:
/// - Task and step identification
/// - WorkingSet for inter-step communication
/// - ReferenceStore for historical artifacts (read-only)
/// - CancellationToken for cooperative cancellation
#[derive(Clone)]
pub struct ActionContext {
    /// Task ID
    pub task_id: TaskId,
    /// Step ID (logical ID)
    pub step_id: StepId,
    /// Execution ID (runtime ID for this specific execution)
    /// Distinguishes retry/resume runs of the same step
    pub execution_id: String,
    /// Working set for inter-step communication
    pub working_set: Arc<RwLock<WorkingSet>>,
    /// Reference store for historical artifacts (read-only access)
    pub reference_store: Arc<dyn ReferenceStore>,
    /// Cancellation token for cooperative cancellation
    /// Actions should check this periodically and abort if cancelled
    pub cancellation_token: CancellationToken,
}

impl ActionContext {
    /// Create a new action context
    pub fn new(
        task_id: impl Into<TaskId>,
        step_id: impl Into<StepId>,
        execution_id: impl Into<String>,
        working_set: Arc<RwLock<WorkingSet>>,
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            step_id: step_id.into(),
            execution_id: execution_id.into(),
            working_set,
            reference_store,
            cancellation_token: CancellationToken::new(),
        }
    }

    /// Create a new action context with a specific cancellation token
    pub fn with_cancellation_token(
        task_id: impl Into<TaskId>,
        step_id: impl Into<StepId>,
        execution_id: impl Into<String>,
        working_set: Arc<RwLock<WorkingSet>>,
        reference_store: Arc<dyn ReferenceStore>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            step_id: step_id.into(),
            execution_id: execution_id.into(),
            working_set,
            reference_store,
            cancellation_token,
        }
    }

    /// Generate a new execution ID
    pub fn new_execution_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    /// Check if the action has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }

    /// Get a future that completes when cancellation is requested
    pub async fn cancelled(&self) {
        self.cancellation_token.cancelled().await
    }

    /// Create a child cancellation token
    /// The child will be cancelled when the parent is cancelled
    pub fn child_token(&self) -> CancellationToken {
        self.cancellation_token.child_token()
    }
}

impl std::fmt::Debug for ActionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActionContext")
            .field("task_id", &self.task_id)
            .field("step_id", &self.step_id)
            .field("execution_id", &self.execution_id)
            .finish_non_exhaustive()
    }
}
