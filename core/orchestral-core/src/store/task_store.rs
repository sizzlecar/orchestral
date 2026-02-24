//! TaskStore - Task persistence trait

use async_trait::async_trait;

use super::StoreError;
use crate::types::{Task, TaskState};

/// TaskStore trait - async interface for task persistence
#[async_trait]
pub trait TaskStore: Send + Sync {
    /// Save a task (insert or update)
    async fn save(&self, task: &Task) -> Result<(), StoreError>;

    /// Load a task by ID
    async fn load(&self, task_id: &str) -> Result<Option<Task>, StoreError>;

    /// Update task state only
    async fn update_state(&self, task_id: &str, state: TaskState) -> Result<(), StoreError>;

    /// List tasks by state
    async fn list_by_state(&self, state: &TaskState) -> Result<Vec<Task>, StoreError>;

    /// Delete a task
    async fn delete(&self, task_id: &str) -> Result<bool, StoreError>;
}
