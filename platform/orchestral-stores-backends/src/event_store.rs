//! EventStore - Runtime level event storage
//!
//! Used for:
//! - Historical playback
//! - Planner context
//! - Debug / Audit

use async_trait::async_trait;
use redis::AsyncCommands;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::RwLock;

use orchestral_core::store::{Event, EventStore, StoreError};

const DEFAULT_IN_MEMORY_EVENT_LIMIT: usize = 20_000;

/// In-memory implementation for development and testing
pub struct InMemoryEventStore {
    events: RwLock<Vec<Event>>,
    max_events: usize,
}

impl InMemoryEventStore {
    /// Create a new in-memory event store
    pub fn new() -> Self {
        Self::with_max_events(DEFAULT_IN_MEMORY_EVENT_LIMIT)
    }

    /// Create an in-memory event store with a hard capacity limit.
    pub fn with_max_events(max_events: usize) -> Self {
        Self {
            events: RwLock::new(Vec::new()),
            max_events: max_events.max(1),
        }
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventStore for InMemoryEventStore {
    async fn append(&self, event: Event) -> Result<(), StoreError> {
        let mut events = self
            .events
            .write()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        if events.len() >= self.max_events {
            let overflow = events
                .len()
                .saturating_add(1)
                .saturating_sub(self.max_events);
            if overflow > 0 {
                events.drain(0..overflow);
            }
        }
        events.push(event);
        Ok(())
    }

    async fn query_by_thread(&self, thread_id: &str) -> Result<Vec<Event>, StoreError> {
        let events = self
            .events
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(events
            .iter()
            .filter(|e| e.thread_id() == thread_id)
            .cloned()
            .collect())
    }

    async fn query_by_thread_with_limit(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        let events = self
            .events
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let mut filtered: Vec<_> = events
            .iter()
            .filter(|e| e.thread_id() == thread_id)
            .cloned()
            .collect();
        // Sort by timestamp descending (most recent first)
        filtered.sort_by_key(|b| std::cmp::Reverse(b.timestamp()));
        Ok(filtered.into_iter().take(limit).collect())
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Event>, StoreError> {
        let events = self
            .events
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let mut sorted: Vec<_> = events.iter().cloned().collect();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.timestamp()));
        Ok(sorted.into_iter().take(limit).collect())
    }
}

/// Redis implementation for append-only event persistence.
pub struct RedisEventStore {
    client: redis::Client,
    key_prefix: String,
}

impl RedisEventStore {
    /// Create a new Redis event store from a connection URL.
    pub fn new(connection_url: &str, key_prefix: impl Into<String>) -> Result<Self, StoreError> {
        let client = redis::Client::open(connection_url)
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.into(),
        })
    }

    fn sequence_key(&self) -> String {
        format!("{}:event:seq", self.key_prefix)
    }

    fn event_key(&self, sequence: i64) -> String {
        format!("{}:event:{}", self.key_prefix, sequence)
    }

    fn thread_events_key(&self, thread_id: &str) -> String {
        format!("{}:thread:{}:events", self.key_prefix, thread_id)
    }

    fn recent_events_key(&self) -> String {
        format!("{}:events:recent", self.key_prefix)
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, StoreError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))
    }

    async fn load_events_by_sequences(
        &self,
        sequences: Vec<i64>,
    ) -> Result<Vec<Event>, StoreError> {
        let mut conn = self.connection().await?;
        let mut out = Vec::new();

        for seq in sequences {
            let key = self.event_key(seq);
            let payload: Option<String> = conn
                .get(key)
                .await
                .map_err(|e| StoreError::Connection(e.to_string()))?;
            if let Some(payload) = payload {
                let event: Event = serde_json::from_str(&payload)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                out.push(event);
            }
        }

        Ok(out)
    }
}

#[async_trait]
impl EventStore for RedisEventStore {
    async fn append(&self, event: Event) -> Result<(), StoreError> {
        let mut conn = self.connection().await?;
        let payload =
            serde_json::to_string(&event).map_err(|e| StoreError::Serialization(e.to_string()))?;
        let sequence: i64 = conn
            .incr(self.sequence_key(), 1_i64)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let event_key = self.event_key(sequence);
        let thread_events_key = self.thread_events_key(event.thread_id());
        let recent_events_key = self.recent_events_key();
        let score = event.timestamp().timestamp_millis();

        conn.set::<_, _, ()>(event_key, payload)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.zadd::<_, _, _, ()>(thread_events_key, sequence, score)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        conn.zadd::<_, _, _, ()>(recent_events_key, sequence, score)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn query_by_thread(&self, thread_id: &str) -> Result<Vec<Event>, StoreError> {
        let mut conn = self.connection().await?;
        let sequence_ids: Vec<i64> = conn
            .zrange(self.thread_events_key(thread_id), 0, -1)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        self.load_events_by_sequences(sequence_ids).await
    }

    async fn query_by_thread_with_limit(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.connection().await?;
        let stop = (limit as isize) - 1;
        let sequence_ids: Vec<i64> = conn
            .zrevrange(self.thread_events_key(thread_id), 0, stop)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        self.load_events_by_sequences(sequence_ids).await
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Event>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self.connection().await?;
        let stop = (limit as isize) - 1;
        let sequence_ids: Vec<i64> = conn
            .zrevrange(self.recent_events_key(), 0, stop)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        self.load_events_by_sequences(sequence_ids).await
    }
}

/// PostgreSQL implementation for append-only event persistence.
pub struct PostgresEventStore {
    pool: PgPool,
    table_name: String,
}

impl PostgresEventStore {
    /// Create a new PostgreSQL event store from a connection URL.
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
        let table_name = format!("{}_events", prefix);
        let this = Self { pool, table_name };
        this.init_schema().await?;
        Ok(this)
    }

    async fn init_schema(&self) -> Result<(), StoreError> {
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                thread_id TEXT NOT NULL,
                interaction_id TEXT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                reference_id TEXT NULL,
                task_id TEXT NULL,
                step_id TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                event_json JSONB NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let idx_thread_time = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_time_idx ON {1} (thread_id, created_at DESC, id DESC)",
            self.table_name, self.table_name
        );
        let idx_thread_type_time = format!(
            "CREATE INDEX IF NOT EXISTS {0}_thread_type_time_idx ON {1} (thread_id, event_type, created_at DESC, id DESC)",
            self.table_name, self.table_name
        );
        let idx_interaction = format!(
            "CREATE INDEX IF NOT EXISTS {0}_interaction_idx ON {1} (thread_id, interaction_id, created_at DESC, id DESC)",
            self.table_name, self.table_name
        );
        let idx_time = format!(
            "CREATE INDEX IF NOT EXISTS {0}_time_idx ON {1} (created_at DESC, id DESC)",
            self.table_name, self.table_name
        );
        let idx_reference = format!(
            "CREATE INDEX IF NOT EXISTS {0}_reference_idx ON {1} (reference_id)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_thread_time)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_thread_type_time)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_interaction)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_time)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        sqlx::query(&idx_reference)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl EventStore for PostgresEventStore {
    async fn append(&self, event: Event) -> Result<(), StoreError> {
        let sql = format!(
            "INSERT INTO {} (thread_id, interaction_id, event_type, payload, reference_id, task_id, step_id, created_at, event_json)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            self.table_name
        );
        let event_json =
            serde_json::to_value(&event).map_err(|e| StoreError::Serialization(e.to_string()))?;
        let payload = event_payload(&event);
        sqlx::query(&sql)
            .bind(event.thread_id().to_string())
            .bind(event.interaction_id().map(ToString::to_string))
            .bind(event_type_label(&event))
            .bind(payload)
            .bind(event_reference_id(&event))
            .bind(event.task_id().map(ToString::to_string))
            .bind(event.step_id().map(ToString::to_string))
            .bind(event.timestamp())
            .bind(event_json)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        Ok(())
    }

    async fn query_by_thread(&self, thread_id: &str) -> Result<Vec<Event>, StoreError> {
        let sql = format!(
            "SELECT event_json FROM {} WHERE thread_id = $1 ORDER BY created_at ASC, id ASC",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
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
        let limit_i64 = usize_to_i64(limit)?;
        let sql = format!(
            "SELECT event_json FROM {} WHERE thread_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .bind(limit_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        decode_event_rows(rows)
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Event>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit_i64 = usize_to_i64(limit)?;
        let sql = format!(
            "SELECT event_json FROM {} ORDER BY created_at DESC, id DESC LIMIT $1",
            self.table_name
        );
        let rows = sqlx::query(&sql)
            .bind(limit_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        decode_event_rows(rows)
    }
}

fn decode_event_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Event>, StoreError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let value: serde_json::Value = row
            .try_get("event_json")
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let event: Event =
            serde_json::from_value(value).map_err(|e| StoreError::Serialization(e.to_string()))?;
        out.push(event);
    }
    Ok(out)
}

fn event_type_label(event: &Event) -> &'static str {
    match event {
        Event::UserInput { .. } => "user_input",
        Event::AssistantOutput { .. } => "assistant_output",
        Event::Artifact { .. } => "artifact",
        Event::ExternalEvent { .. } => "external_event",
        Event::SystemTrace { .. } => "system_trace",
    }
}

fn event_payload(event: &Event) -> serde_json::Value {
    match event {
        Event::UserInput { payload, .. } => payload.clone(),
        Event::AssistantOutput { payload, .. } => payload.clone(),
        Event::ExternalEvent { payload, .. } => payload.clone(),
        Event::SystemTrace { payload, .. } => payload.clone(),
        Event::Artifact { reference_id, .. } => serde_json::json!({ "reference_id": reference_id }),
    }
}

fn event_reference_id(event: &Event) -> Option<String> {
    match event {
        Event::Artifact { reference_id, .. } => Some(reference_id.clone()),
        _ => None,
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

    #[test]
    fn test_in_memory_event_store_evicts_oldest_events() {
        tokio_test::block_on(async {
            let store = InMemoryEventStore::with_max_events(2);
            store
                .append(Event::user_input("thread-1", "i1", json!({"n": 1})))
                .await
                .expect("append e1");
            store
                .append(Event::user_input("thread-1", "i2", json!({"n": 2})))
                .await
                .expect("append e2");
            store
                .append(Event::user_input("thread-1", "i3", json!({"n": 3})))
                .await
                .expect("append e3");

            let events = store
                .query_by_thread("thread-1")
                .await
                .expect("query events");
            assert_eq!(events.len(), 2);

            match &events[0] {
                Event::UserInput { interaction_id, .. } => {
                    assert_eq!(interaction_id.as_str(), "i2")
                }
                other => panic!("unexpected event: {:?}", other),
            }
            match &events[1] {
                Event::UserInput { interaction_id, .. } => {
                    assert_eq!(interaction_id.as_str(), "i3")
                }
                other => panic!("unexpected event: {:?}", other),
            }
        });
    }
}
