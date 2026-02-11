//! TaskStore in-memory implementation.

use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use orchestral_core::store::{StoreError, TaskStore};
use orchestral_core::types::{Task, TaskState};

const DEFAULT_IN_MEMORY_TASK_LIMIT: usize = 5_000;

/// In-memory implementation for development and testing.
pub struct InMemoryTaskStore {
    tasks: RwLock<HashMap<String, Task>>,
    order: RwLock<VecDeque<String>>,
    max_tasks: usize,
}

impl InMemoryTaskStore {
    /// Create a new in-memory store.
    pub fn new() -> Self {
        Self::with_max_tasks(DEFAULT_IN_MEMORY_TASK_LIMIT)
    }

    /// Create a new in-memory store with a hard capacity limit.
    pub fn with_max_tasks(max_tasks: usize) -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
            order: RwLock::new(VecDeque::new()),
            max_tasks: max_tasks.max(1),
        }
    }

    fn touch_order(order: &mut VecDeque<String>, task_id: &str) {
        order.retain(|id| id != task_id);
        order.push_back(task_id.to_string());
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
        let mut order = self
            .order
            .write()
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if !tasks.contains_key(task.id.as_str()) && tasks.len() >= self.max_tasks {
            if let Some(oldest_id) = order.pop_front() {
                tasks.remove(&oldest_id);
            }
        }
        tasks.insert(task.id.to_string(), task.clone());
        Self::touch_order(&mut order, task.id.as_str());
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
            let mut order = self
                .order
                .write()
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            Self::touch_order(&mut order, task_id);
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
        let removed = tasks.remove(task_id).is_some();
        if removed {
            let mut order = self
                .order
                .write()
                .map_err(|e| StoreError::Internal(e.to_string()))?;
            order.retain(|id| id != task_id);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::types::{Intent, Task};

    #[test]
    fn test_in_memory_task_store_limit() {
        tokio_test::block_on(async {
            let store = InMemoryTaskStore::with_max_tasks(2);
            let t1 = Task::new(Intent::new("intent-a"));
            let t2 = Task::new(Intent::new("intent-b"));
            let t3 = Task::new(Intent::new("intent-c"));
            store.save(&t1).await.unwrap();
            store.save(&t2).await.unwrap();
            store.save(&t3).await.unwrap();

            assert!(store.load(t1.id.as_str()).await.unwrap().is_none());
            assert!(store.load(t2.id.as_str()).await.unwrap().is_some());
            assert!(store.load(t3.id.as_str()).await.unwrap().is_some());
        });
    }
}
