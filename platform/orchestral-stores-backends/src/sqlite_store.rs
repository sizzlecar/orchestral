//! SQLite store backend implementations.

use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use sqlx::{
    sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, sqlite::SqliteRow, Row, SqlitePool,
};
use std::str::FromStr;

use orchestral_core::store::{
    Event, EventStore, Reference, ReferenceMatch, ReferenceStore, ReferenceType, StoreError,
    TaskStore,
};
use orchestral_core::types::{Task, TaskState};

/// SQLite implementation for append-only event persistence.
pub struct SqliteEventStore {
    pool: SqlitePool,
    table_name: String,
}

impl SqliteEventStore {
    /// Create a new SQLite event store from a connection URL.
    pub async fn new(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let pool = connect_sqlite_pool(connection_url).await?;
        let prefix = normalize_table_prefix(&table_prefix.into());
        let table_name = format!("{}_events", prefix);
        let this = Self { pool, table_name };
        this.init_schema().await?;
        Ok(this)
    }

    async fn init_schema(&self) -> Result<(), StoreError> {
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                event_json TEXT NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;

        let idx_thread_time = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_time_idx ON {1} (thread_id, created_at_ms DESC, id DESC)",
            self.table_name, self.table_name
        );
        let idx_time = format!(
            "CREATE INDEX IF NOT EXISTS {0}_time_idx ON {1} (created_at_ms DESC, id DESC)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_thread_time)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        sqlx::query(&idx_time)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl EventStore for SqliteEventStore {
    async fn append(&self, event: Event) -> Result<(), StoreError> {
        let sql = format!(
            "INSERT INTO {} (thread_id, created_at_ms, event_json) VALUES (?, ?, ?)",
            self.table_name
        );
        let payload = serde_json::to_string(&event)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        sqlx::query(&sql)
            .bind(event.thread_id().to_string())
            .bind(event.timestamp().timestamp_millis())
            .bind(payload)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }

    async fn query_by_thread(&self, thread_id: &str) -> Result<Vec<Event>, StoreError> {
        let sql = format!(
            "SELECT event_json FROM {} WHERE thread_id = ? ORDER BY created_at_ms ASC, id ASC",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        decode_event_rows(rows)
    }

    async fn query_by_thread_with_limit(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let sql = format!(
            "SELECT event_json FROM {} WHERE thread_id = ? ORDER BY created_at_ms DESC, id DESC LIMIT ?",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .bind(usize_to_i64(limit)?)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        decode_event_rows(rows)
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Event>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let sql = format!(
            "SELECT event_json FROM {} ORDER BY created_at_ms DESC, id DESC LIMIT ?",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(usize_to_i64(limit)?)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        decode_event_rows(rows)
    }
}

fn decode_event_rows(rows: Vec<SqliteRow>) -> Result<Vec<Event>, StoreError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let payload: String = row
            .try_get("event_json")
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        let event: Event = serde_json::from_str(&payload)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        out.push(event);
    }
    Ok(out)
}

/// SQLite implementation for durable task persistence.
pub struct SqliteTaskStore {
    pool: SqlitePool,
    table_name: String,
}

impl SqliteTaskStore {
    /// Create a new SQLite task store from a connection URL.
    pub async fn new(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let pool = connect_sqlite_pool(connection_url).await?;
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
                updated_at_ms INTEGER NOT NULL,
                task_json TEXT NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;

        let idx_state = format!(
            "CREATE INDEX IF NOT EXISTS {0}_state_idx ON {1} (state_label, updated_at_ms DESC)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_state)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl TaskStore for SqliteTaskStore {
    async fn save(&self, task: &Task) -> Result<(), StoreError> {
        let sql = format!(
            "INSERT INTO {} (task_id, state_label, updated_at_ms, task_json)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(task_id) DO UPDATE SET
               state_label = excluded.state_label,
               updated_at_ms = excluded.updated_at_ms,
               task_json = excluded.task_json",
            self.table_name
        );
        let payload = serde_json::to_string(task)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        sqlx::query(&sql)
            .bind(task.id.as_str())
            .bind(task_state_label(&task.state))
            .bind(task.updated_at.timestamp_millis())
            .bind(payload)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }

    async fn load(&self, task_id: &str) -> Result<Option<Task>, StoreError> {
        let sql = format!(
            "SELECT task_json FROM {} WHERE task_id = ?",
            self.table_name
        );
        let row = sqlx::query(&sql)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };

        let payload: String = row
            .try_get("task_json")
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        let task: Task = serde_json::from_str(&payload)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
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
            "SELECT task_json FROM {} WHERE state_label = ? ORDER BY updated_at_ms DESC",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(task_state_label(state))
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: String = row
                .try_get("task_json")
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
            let task: Task = serde_json::from_str(&payload)
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
            tasks.push(task);
        }
        Ok(tasks)
    }

    async fn delete(&self, task_id: &str) -> Result<bool, StoreError> {
        let sql = format!("DELETE FROM {} WHERE task_id = ?", self.table_name);
        let result = sqlx::query(&sql)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(result.rows_affected() > 0)
    }
}

/// SQLite implementation for durable reference index persistence.
pub struct SqliteReferenceStore {
    pool: SqlitePool,
    table_name: String,
    vector_table_name: Option<String>,
}

impl SqliteReferenceStore {
    /// Create a new SQLite reference store from a connection URL.
    pub async fn new(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        Self::new_with_mode(connection_url, table_prefix, false).await
    }

    /// Create a new SQLite reference store with vector index support.
    pub async fn new_vector(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        Self::new_with_mode(connection_url, table_prefix, true).await
    }

    async fn new_with_mode(
        connection_url: &str,
        table_prefix: impl Into<String>,
        enable_vector: bool,
    ) -> Result<Self, StoreError> {
        let pool = connect_sqlite_pool(connection_url).await?;
        let prefix = normalize_table_prefix(&table_prefix.into());
        let table_name = format!("{}_references", prefix);
        let vector_table_name = if enable_vector {
            Some(format!("{}_reference_vectors", prefix))
        } else {
            None
        };
        let this = Self {
            pool,
            table_name,
            vector_table_name,
        };
        this.init_schema().await?;
        if this.vector_table_name.is_some() {
            this.init_vector_schema().await?;
        }
        Ok(this)
    }

    async fn init_schema(&self) -> Result<(), StoreError> {
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                ref_type TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                reference_json TEXT NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;

        let idx_thread_created = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_created_idx ON {1} (thread_id, created_at_ms DESC)",
            self.table_name, self.table_name
        );
        let idx_thread_type_created = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_type_created_idx ON {1} (thread_id, ref_type, created_at_ms DESC)",
            self.table_name, self.table_name
        );
        let idx_type_created = format!(
            "CREATE INDEX IF NOT EXISTS {0}_type_created_idx ON {1} (ref_type, created_at_ms DESC)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_thread_created)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        sqlx::query(&idx_thread_type_created)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        sqlx::query(&idx_type_created)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }

    async fn init_vector_schema(&self) -> Result<(), StoreError> {
        let Some(vector_table) = self.vector_table_name.as_ref() else {
            return Ok(());
        };
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                reference_id TEXT PRIMARY KEY,
                thread_id TEXT NOT NULL,
                vector_json TEXT NOT NULL,
                dimension INTEGER NOT NULL,
                norm REAL NOT NULL,
                updated_at_ms INTEGER NOT NULL
            )",
            vector_table
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;

        let idx_thread = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_idx ON {1} (thread_id)",
            vector_table, vector_table
        );
        sqlx::query(&idx_thread)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }

    async fn sync_vector_entry(&self, reference: &Reference) -> Result<(), StoreError> {
        let Some(vector_table) = self.vector_table_name.as_ref() else {
            return Ok(());
        };

        let Some(vector) = extract_embedding_vector(reference) else {
            return self.delete_vector_entry(reference.id.as_str()).await;
        };
        if vector.is_empty() {
            return self.delete_vector_entry(reference.id.as_str()).await;
        }
        let norm = l2_norm(&vector);
        if !norm.is_finite() || norm <= f32::EPSILON {
            return self.delete_vector_entry(reference.id.as_str()).await;
        }

        let payload = serde_json::to_string(&vector)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        let sql = format!(
            "INSERT INTO {} (reference_id, thread_id, vector_json, dimension, norm, updated_at_ms)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(reference_id) DO UPDATE SET
               thread_id = excluded.thread_id,
               vector_json = excluded.vector_json,
               dimension = excluded.dimension,
               norm = excluded.norm,
               updated_at_ms = excluded.updated_at_ms",
            vector_table
        );
        sqlx::query(&sql)
            .bind(reference.id.as_str())
            .bind(reference.thread_id.as_str())
            .bind(payload)
            .bind(usize_to_i64(vector.len())?)
            .bind(norm as f64)
            .bind(Utc::now().timestamp_millis())
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }

    async fn delete_vector_entry(&self, reference_id: &str) -> Result<(), StoreError> {
        let Some(vector_table) = self.vector_table_name.as_ref() else {
            return Ok(());
        };
        let sql = format!("DELETE FROM {} WHERE reference_id = ?", vector_table);
        sqlx::query(&sql)
            .bind(reference_id)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl ReferenceStore for SqliteReferenceStore {
    async fn add(&self, reference: Reference) -> Result<(), StoreError> {
        let sql = format!(
            "INSERT INTO {} (id, thread_id, ref_type, created_at_ms, reference_json)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
               thread_id = excluded.thread_id,
               ref_type = excluded.ref_type,
               created_at_ms = excluded.created_at_ms,
               reference_json = excluded.reference_json",
            self.table_name
        );
        let payload = serde_json::to_string(&reference)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        sqlx::query(&sql)
            .bind(&reference.id)
            .bind(&reference.thread_id)
            .bind(ref_type_label(&reference.ref_type))
            .bind(reference.created_at.timestamp_millis())
            .bind(payload)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        self.sync_vector_entry(&reference).await?;
        Ok(())
    }

    async fn get(&self, id: &str) -> Result<Option<Reference>, StoreError> {
        let sql = format!(
            "SELECT reference_json FROM {} WHERE id = ?",
            self.table_name
        );
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };

        let payload: String = row
            .try_get("reference_json")
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        let reference: Reference = serde_json::from_str(&payload)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        Ok(Some(reference))
    }

    async fn query_by_type(&self, ref_type: &ReferenceType) -> Result<Vec<Reference>, StoreError> {
        let sql = format!(
            "SELECT reference_json FROM {} WHERE ref_type = ? ORDER BY created_at_ms DESC",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(ref_type_label(ref_type))
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        decode_reference_rows(rows)
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Reference>, StoreError> {
        let sql = format!(
            "SELECT reference_json FROM {} ORDER BY created_at_ms DESC LIMIT ?",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(usize_to_i64(limit)?)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        decode_reference_rows(rows)
    }

    async fn query_recent_by_thread(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let sql = format!(
            "SELECT reference_json FROM {} WHERE thread_id = ? ORDER BY created_at_ms DESC LIMIT ?",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .bind(usize_to_i64(limit)?)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        decode_reference_rows(rows)
    }

    async fn query_by_type_in_thread(
        &self,
        thread_id: &str,
        ref_type: &ReferenceType,
        limit: usize,
    ) -> Result<Vec<Reference>, StoreError> {
        let sql = format!(
            "SELECT reference_json FROM {} WHERE thread_id = ? AND ref_type = ? ORDER BY created_at_ms DESC LIMIT ?",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .bind(ref_type_label(ref_type))
            .bind(usize_to_i64(limit)?)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        decode_reference_rows(rows)
    }

    async fn search_by_embedding(
        &self,
        thread_id: &str,
        embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<ReferenceMatch>, StoreError> {
        if limit == 0 || embedding.is_empty() {
            return Ok(Vec::new());
        }
        let Some(vector_table) = self.vector_table_name.as_ref() else {
            return Ok(Vec::new());
        };
        let query_norm = l2_norm(embedding);
        if !query_norm.is_finite() || query_norm <= f32::EPSILON {
            return Ok(Vec::new());
        }

        let sql = format!(
            "SELECT reference_id, vector_json, norm FROM {} WHERE thread_id = ?",
            vector_table
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;

        let mut scored: Vec<(String, f32)> = Vec::new();
        for row in rows {
            let reference_id: String = row
                .try_get("reference_id")
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
            let vector_json: String = row
                .try_get("vector_json")
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
            let stored_norm: f64 = row
                .try_get("norm")
                .map_err(|err| StoreError::Serialization(err.to_string()))?;
            let stored_norm = stored_norm as f32;
            if !stored_norm.is_finite() || stored_norm <= f32::EPSILON {
                continue;
            }
            let stored_vector: Vec<f32> = match serde_json::from_str(&vector_json) {
                Ok(vector) => vector,
                Err(_) => continue,
            };
            if stored_vector.len() != embedding.len() {
                continue;
            }
            let denom = query_norm * stored_norm;
            if denom <= f32::EPSILON {
                continue;
            }
            let cosine = dot_product(embedding, &stored_vector) / denom;
            if !cosine.is_finite() {
                continue;
            }
            let score = ((cosine + 1.0) / 2.0).clamp(0.0, 1.0);
            scored.push((reference_id, score));
        }

        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        let mut out = Vec::new();
        for (reference_id, score) in scored.into_iter().take(limit) {
            if let Some(reference) = self.get(reference_id.as_str()).await? {
                out.push(ReferenceMatch { reference, score });
            }
        }
        Ok(out)
    }

    async fn delete(&self, id: &str) -> Result<bool, StoreError> {
        self.delete_vector_entry(id).await?;
        let sql = format!("DELETE FROM {} WHERE id = ?", self.table_name);
        let result = sqlx::query(&sql)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|err| StoreError::Connection(err.to_string()))?;
        Ok(result.rows_affected() > 0)
    }
}

fn decode_reference_rows(rows: Vec<SqliteRow>) -> Result<Vec<Reference>, StoreError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let payload: String = row
            .try_get("reference_json")
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        let reference: Reference = serde_json::from_str(&payload)
            .map_err(|err| StoreError::Serialization(err.to_string()))?;
        out.push(reference);
    }
    Ok(out)
}

fn extract_embedding_vector(reference: &Reference) -> Option<Vec<f32>> {
    if let Some(value) = reference.metadata.get("embedding_vector") {
        if let Some(vector) = parse_embedding_vector_value(value) {
            return Some(vector);
        }
    }
    if let Some(value) = reference.metadata.get("vector") {
        if let Some(vector) = parse_embedding_vector_value(value) {
            return Some(vector);
        }
    }
    if let Some(embedding) = reference.metadata.get("embedding") {
        if let Some(value) = embedding.get("vector") {
            return parse_embedding_vector_value(value);
        }
    }
    None
}

fn parse_embedding_vector_value(value: &Value) -> Option<Vec<f32>> {
    let array = value.as_array()?;
    if array.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(array.len());
    for element in array {
        let number = element.as_f64()?;
        if !number.is_finite() {
            return None;
        }
        out.push(number as f32);
    }
    Some(out)
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn l2_norm(v: &[f32]) -> f32 {
    dot_product(v, v).sqrt()
}

async fn connect_sqlite_pool(connection_url: &str) -> Result<SqlitePool, StoreError> {
    let options = SqliteConnectOptions::from_str(connection_url)
        .map_err(|err| StoreError::Connection(err.to_string()))?
        .create_if_missing(true);
    SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .map_err(|err| StoreError::Connection(err.to_string()))
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

fn ref_type_label(ref_type: &ReferenceType) -> String {
    match ref_type {
        ReferenceType::Document => "document".to_string(),
        ReferenceType::Table => "table".to_string(),
        ReferenceType::Code => "code".to_string(),
        ReferenceType::Text => "text".to_string(),
        ReferenceType::Image => "image".to_string(),
        ReferenceType::Audio => "audio".to_string(),
        ReferenceType::Video => "video".to_string(),
        ReferenceType::Binary => "binary".to_string(),
        ReferenceType::Custom(label) => format!("custom:{}", label),
    }
}

fn usize_to_i64(value: usize) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Internal("limit exceeds i64 range".to_string()))
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
    use serde_json::json;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn temp_sqlite_url(name: &str) -> (String, PathBuf) {
        let path = std::env::temp_dir().join(format!("orchestral-{}-{}.db", name, Uuid::new_v4()));
        (format!("sqlite://{}", path.display()), path)
    }

    fn reference_with_vector(id: &str, thread_id: &str, vector: Vec<f32>) -> Reference {
        let mut metadata = HashMap::new();
        metadata.insert("embedding_vector".to_string(), json!(vector));
        let mut reference = Reference::new(thread_id.to_string(), ReferenceType::Text, json!({}));
        reference.id = id.to_string();
        reference.metadata = metadata;
        reference
    }

    #[test]
    fn test_sqlite_vector_search_returns_ranked_matches() {
        tokio_test::block_on(async {
            let (url, db_path) = temp_sqlite_url("vector-search");
            let store = SqliteReferenceStore::new_vector(url.as_str(), "orchestral_test")
                .await
                .expect("create vector store");

            store
                .add(reference_with_vector("r1", "thread-a", vec![1.0, 0.0, 0.0]))
                .await
                .expect("add r1");
            store
                .add(reference_with_vector("r2", "thread-a", vec![0.8, 0.2, 0.0]))
                .await
                .expect("add r2");
            store
                .add(reference_with_vector("r3", "thread-b", vec![1.0, 0.0, 0.0]))
                .await
                .expect("add r3");

            let hits = store
                .search_by_embedding("thread-a", &[1.0, 0.0, 0.0], 4)
                .await
                .expect("search");
            assert_eq!(hits.len(), 2);
            assert_eq!(hits[0].reference.id, "r1");
            assert_eq!(hits[1].reference.id, "r2");
            assert!(hits[0].score >= hits[1].score);

            let _ = std::fs::remove_file(db_path);
        });
    }

    #[test]
    fn test_sqlite_plain_search_by_embedding_returns_empty() {
        tokio_test::block_on(async {
            let (url, db_path) = temp_sqlite_url("plain-search");
            let store = SqliteReferenceStore::new(url.as_str(), "orchestral_test")
                .await
                .expect("create plain store");

            store
                .add(reference_with_vector("r1", "thread-a", vec![1.0, 0.0]))
                .await
                .expect("add r1");

            let hits = store
                .search_by_embedding("thread-a", &[1.0, 0.0], 1)
                .await
                .expect("search");
            assert!(hits.is_empty());

            let _ = std::fs::remove_file(db_path);
        });
    }
}
