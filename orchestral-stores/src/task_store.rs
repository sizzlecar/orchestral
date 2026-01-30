//! TaskStore implementations

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

use orchestral_core::store::{StoreError, TaskStore};
use orchestral_core::types::{Task, TaskState};

/// In-memory implementation for development and testing
pub struct InMemoryTaskStore {
    tasks: RwLock<HashMap<String, Task>>,
}

impl InMemoryTaskStore {
    /// Create a new in-memory store
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryTaskStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TaskStore for InMemoryTaskStore {
    async fn save(&self, task: &Task) -> Result<(), StoreError> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        tasks.insert(task.id.clone(), task.clone());
        Ok(())
    }

    async fn load(&self, task_id: &str) -> Result<Option<Task>, StoreError> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(tasks.get(task_id).cloned())
    }

    async fn update_state(&self, task_id: &str, state: TaskState) -> Result<(), StoreError> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        if let Some(task) = tasks.get_mut(task_id) {
            task.set_state(state);
            Ok(())
        } else {
            Err(StoreError::NotFound(task_id.to_string()))
        }
    }

    async fn list_by_state(&self, state: &TaskState) -> Result<Vec<Task>, StoreError> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        // Compare by discriminant for enum variants with data
        Ok(tasks
            .values()
            .filter(|t| std::mem::discriminant(&t.state) == std::mem::discriminant(state))
            .cloned()
            .collect())
    }

    async fn delete(&self, task_id: &str) -> Result<bool, StoreError> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(tasks.remove(task_id).is_some())
    }
}
