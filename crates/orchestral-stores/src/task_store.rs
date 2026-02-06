//! TaskStore implementations

use async_trait::async_trait;
use redis::AsyncCommands;
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

/// Redis implementation for production persistence.
pub struct RedisTaskStore {
    client: redis::Client,
    key_prefix: String,
}

impl RedisTaskStore {
    /// Create a new Redis task store from a connection URL.
    pub fn new(connection_url: &str, key_prefix: impl Into<String>) -> Result<Self, StoreError> {
        let client = redis::Client::open(connection_url)
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into(),
        })
    }

    fn task_key(&self, task_id: &str) -> String {
        format!("{}:task:{}", self.key_prefix, task_id)
    }

    fn task_ids_key(&self) -> String {
        format!("{}:task:ids", self.key_prefix)
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, StoreError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))
    }
}

#[async_trait]
impl TaskStore for RedisTaskStore {
    async fn save(&self, task: &Task) -> Result<(), StoreError> {
        let mut conn = self.connection().await?;
        let payload =
            serde_json::to_string(task).map_err(|e| StoreError::Serialization(e.to_string()))?;
        let task_key = self.task_key(&task.id);
        let ids_key = self.task_ids_key();
        conn.set::<_, _, ()>(task_key, payload)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.sadd::<_, _, ()>(ids_key, &task.id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn load(&self, task_id: &str) -> Result<Option<Task>, StoreError> {
        let mut conn = self.connection().await?;
        let key = self.task_key(task_id);
        let payload: Option<String> = conn
            .get(key)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        payload
            .map(|s| serde_json::from_str(&s).map_err(|e| StoreError::Serialization(e.to_string())))
            .transpose()
    }

    async fn update_state(&self, task_id: &str, state: TaskState) -> Result<(), StoreError> {
        let mut task = self
            .load(task_id)
            .await?
            .ok_or_else(|| StoreError::NotFound(task_id.to_string()))?;
        task.set_state(state);
        self.save(&task).await
    }

    async fn list_by_state(&self, state: &TaskState) -> Result<Vec<Task>, StoreError> {
        let mut conn = self.connection().await?;
        let ids: Vec<String> = conn
            .smembers(self.task_ids_key())
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let mut tasks = Vec::new();
        for id in ids {
            if let Some(task) = self.load(&id).await? {
                if std::mem::discriminant(&task.state) == std::mem::discriminant(state) {
                    tasks.push(task);
                }
            }
        }
        Ok(tasks)
    }

    async fn delete(&self, task_id: &str) -> Result<bool, StoreError> {
        let mut conn = self.connection().await?;
        let key = self.task_key(task_id);
        let ids_key = self.task_ids_key();
        let deleted: u64 = conn
            .del(key)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let _removed_count: u64 = conn
            .srem(ids_key, task_id)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(deleted > 0)
    }
}
