//! TaskStore implementations

use async_trait::async_trait;
use redis::AsyncCommands;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use orchestral_core::store::{StoreError, TaskStore};
use orchestral_core::types::{Task, TaskState};

const DEFAULT_IN_MEMORY_TASK_LIMIT: usize = 5_000;

/// In-memory implementation for development and testing
pub struct InMemoryTaskStore {
    tasks: RwLock<HashMap<String, Task>>,
    order: RwLock<VecDeque<String>>,
    max_tasks: usize,
}

impl InMemoryTaskStore {
    /// Create a new in-memory store
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
        let task_key = self.task_key(task.id.as_str());
        let ids_key = self.task_ids_key();
        conn.set::<_, _, ()>(task_key, payload)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.sadd::<_, _, ()>(ids_key, task.id.as_str())
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

/// PostgreSQL implementation for durable task persistence.
pub struct PostgresTaskStore {
    pool: PgPool,
    table_name: String,
}

impl PostgresTaskStore {
    /// Create a new PostgreSQL task store from a connection URL.
    pub async fn new(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(connection_url)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let prefix = normalize_table_prefix(&table_prefix.into());
        let table_name = format!("{}_tasks", prefix);
        let this = Self { pool, table_name };
        this.init_schema().await?;
        Ok(this)
    }

    async fn init_schema(&self) -> Result<(), StoreError> {
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                task_id TEXT PRIMARY KEY,
                state_label TEXT NOT NULL,
                task_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let idx_state = format!(
            "CREATE INDEX IF NOT EXISTS {0}_state_idx ON {1} (state_label, updated_at DESC)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_state)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl TaskStore for PostgresTaskStore {
    async fn save(&self, task: &Task) -> Result<(), StoreError> {
        let sql = format!(
            "INSERT INTO {} (task_id, state_label, task_json, updated_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (task_id) DO UPDATE SET
               state_label = EXCLUDED.state_label,
               task_json = EXCLUDED.task_json,
               updated_at = EXCLUDED.updated_at",
            self.table_name
        );
        let task_json =
            serde_json::to_value(task).map_err(|e| StoreError::Serialization(e.to_string()))?;
        sqlx::query(&sql)
            .bind(task.id.as_str())
            .bind(task_state_label(&task.state))
            .bind(task_json)
            .bind(task.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn load(&self, task_id: &str) -> Result<Option<Task>, StoreError> {
        let sql = format!(
            "SELECT task_json FROM {} WHERE task_id = $1",
            self.table_name
        );
        let row = sqlx::query(&sql)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let value: serde_json::Value = row
            .try_get("task_json")
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let task: Task =
            serde_json::from_value(value).map_err(|e| StoreError::Serialization(e.to_string()))?;
        Ok(Some(task))
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
        let sql = format!(
            "SELECT task_json FROM {} WHERE state_label = $1 ORDER BY updated_at DESC",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(task_state_label(state))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let value: serde_json::Value = row
                .try_get("task_json")
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            let task: Task = serde_json::from_value(value)
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            tasks.push(task);
        }
        Ok(tasks)
    }

    async fn delete(&self, task_id: &str) -> Result<bool, StoreError> {
        let sql = format!("DELETE FROM {} WHERE task_id = $1", self.table_name);
        let result = sqlx::query(&sql)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }
}

fn task_state_label(state: &TaskState) -> &'static str {
    match state {
        TaskState::Planning => "planning",
        TaskState::Executing => "executing",
        TaskState::WaitingUser { .. } => "waiting_user",
        TaskState::WaitingEvent { .. } => "waiting_event",
        TaskState::Paused => "paused",
        TaskState::Failed { .. } => "failed",
        TaskState::Done => "done",
    }
}

fn normalize_table_prefix(raw: &str) -> String {
    let candidate = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if candidate.is_empty() {
        "orchestral".to_string()
    } else {
        candidate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::types::Intent;

    fn task_with_id(id: &str) -> Task {
        let mut task = Task::new(Intent::new(format!("intent-{}", id)));
        task.id = id.into();
        task
    }

    #[test]
    fn test_in_memory_task_store_evicts_oldest_when_limit_exceeded() {
        tokio_test::block_on(async {
            let store = InMemoryTaskStore::with_max_tasks(2);

            store.save(&task_with_id("t1")).await.expect("save t1");
            store.save(&task_with_id("t2")).await.expect("save t2");
            store.save(&task_with_id("t3")).await.expect("save t3");

            assert!(store.load("t1").await.expect("load t1").is_none());
            assert!(store.load("t2").await.expect("load t2").is_some());
            assert!(store.load("t3").await.expect("load t3").is_some());
        });
    }
}
