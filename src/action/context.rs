//! ActionContext type definition

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::store::{ReferenceStore, WorkingSet};

/// Execution context for actions
///
/// Provides access to:
/// - Task and step identification
/// - WorkingSet for inter-step communication
/// - ReferenceStore for historical artifacts (read-only)
#[derive(Clone)]
pub struct ActionContext {
    /// Task ID
    pub task_id: String,
    /// Step ID (logical ID)
    pub step_id: String,
    /// Execution ID (runtime ID for this specific execution)
    /// Distinguishes retry/resume runs of the same step
    pub execution_id: String,
    /// Working set for inter-step communication
    pub working_set: Arc<RwLock<WorkingSet>>,
    /// Reference store for historical artifacts (read-only access)
    pub reference_store: Arc<dyn ReferenceStore>,
}

impl ActionContext {
    /// Create a new action context
    pub fn new(
        task_id: impl Into<String>,
        step_id: impl Into<String>,
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
        }
    }

    /// Generate a new execution ID
    pub fn new_execution_id() -> String {
        uuid::Uuid::new_v4().to_string()
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
