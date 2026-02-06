//! EventStore - Runtime level event storage
//!
//! Used for:
//! - Historical playback
//! - Planner context
//! - Debug / Audit

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::RwLock;

use orchestral_core::store::StoreError;

/// Type alias for Thread ID
pub type ThreadId = String;

/// Type alias for Interaction ID
pub type InteractionId = String;

/// Event - append-only fact record
///
/// Events represent facts that happened in the system.
/// Message is just one type of Event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// User input event
    UserInput {
        /// Thread this event belongs to
        thread_id: ThreadId,
        /// Interaction this event belongs to
        interaction_id: InteractionId,
        /// Event payload
        payload: Value,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },

    /// Assistant output event
    AssistantOutput {
        /// Thread this event belongs to
        thread_id: ThreadId,
        /// Interaction this event belongs to
        interaction_id: InteractionId,
        /// Event payload
        payload: Value,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },

    /// Artifact event (reference to stored artifact)
    Artifact {
        /// Thread this event belongs to
        thread_id: ThreadId,
        /// Interaction this event belongs to
        interaction_id: InteractionId,
        /// Reference ID to the artifact
        reference_id: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },

    /// External event (webhook, timer, etc.)
    ExternalEvent {
        /// Thread this event belongs to
        thread_id: ThreadId,
        /// Kind of external event
        kind: String,
        /// Event payload
        payload: Value,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },

    /// System trace event (for debugging and auditing)
    SystemTrace {
        /// Thread this event belongs to
        thread_id: ThreadId,
        /// Trace level (debug, info, warn, error)
        level: String,
        /// Trace payload
        payload: Value,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },
}

impl Event {
    /// Create a new user input event
    pub fn user_input(
        thread_id: impl Into<String>,
        interaction_id: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::UserInput {
            thread_id: thread_id.into(),
            interaction_id: interaction_id.into(),
            payload,
            timestamp: Utc::now(),
        }
    }

    /// Create a new assistant output event
    pub fn assistant_output(
        thread_id: impl Into<String>,
        interaction_id: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::AssistantOutput {
            thread_id: thread_id.into(),
            interaction_id: interaction_id.into(),
            payload,
            timestamp: Utc::now(),
        }
    }

    /// Create a new artifact event
    pub fn artifact(
        thread_id: impl Into<String>,
        interaction_id: impl Into<String>,
        reference_id: impl Into<String>,
    ) -> Self {
        Self::Artifact {
            thread_id: thread_id.into(),
            interaction_id: interaction_id.into(),
            reference_id: reference_id.into(),
            timestamp: Utc::now(),
        }
    }

    /// Create a new external event
    pub fn external(thread_id: impl Into<String>, kind: impl Into<String>, payload: Value) -> Self {
        Self::ExternalEvent {
            thread_id: thread_id.into(),
            kind: kind.into(),
            payload,
            timestamp: Utc::now(),
        }
    }

    /// Create a new system trace event
    pub fn trace(thread_id: impl Into<String>, level: impl Into<String>, payload: Value) -> Self {
        Self::SystemTrace {
            thread_id: thread_id.into(),
            level: level.into(),
            payload,
            timestamp: Utc::now(),
        }
    }

    /// Get the thread ID of this event
    pub fn thread_id(&self) -> &str {
        match self {
            Event::UserInput { thread_id, .. } => thread_id,
            Event::AssistantOutput { thread_id, .. } => thread_id,
            Event::Artifact { thread_id, .. } => thread_id,
            Event::ExternalEvent { thread_id, .. } => thread_id,
            Event::SystemTrace { thread_id, .. } => thread_id,
        }
    }

    /// Get the timestamp of this event
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Event::UserInput { timestamp, .. } => *timestamp,
            Event::AssistantOutput { timestamp, .. } => *timestamp,
            Event::Artifact { timestamp, .. } => *timestamp,
            Event::ExternalEvent { timestamp, .. } => *timestamp,
            Event::SystemTrace { timestamp, .. } => *timestamp,
        }
    }

    /// Return a copy of this event with a new interaction ID (if applicable)
    pub fn with_interaction_id(&self, interaction_id: impl Into<String>) -> Self {
        let interaction_id = interaction_id.into();
        match self {
            Event::UserInput {
                thread_id,
                payload,
                timestamp,
                ..
            } => Event::UserInput {
                thread_id: thread_id.clone(),
                interaction_id,
                payload: payload.clone(),
                timestamp: *timestamp,
            },
            Event::AssistantOutput {
                thread_id,
                payload,
                timestamp,
                ..
            } => Event::AssistantOutput {
                thread_id: thread_id.clone(),
                interaction_id,
                payload: payload.clone(),
                timestamp: *timestamp,
            },
            Event::Artifact {
                thread_id,
                reference_id,
                timestamp,
                ..
            } => Event::Artifact {
                thread_id: thread_id.clone(),
                interaction_id,
                reference_id: reference_id.clone(),
                timestamp: *timestamp,
            },
            Event::ExternalEvent { .. } | Event::SystemTrace { .. } => self.clone(),
        }
    }
}

/// EventStore trait - async interface for event storage
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append an event
    async fn append(&self, event: Event) -> Result<(), StoreError>;

    /// Query events by thread ID
    async fn query_by_thread(&self, thread_id: &str) -> Result<Vec<Event>, StoreError>;

    /// Query events by thread ID with limit
    async fn query_by_thread_with_limit(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError>;

    /// Query recent events across all threads
    async fn query_recent(&self, limit: usize) -> Result<Vec<Event>, StoreError>;
}

/// In-memory implementation for development and testing
pub struct InMemoryEventStore {
    events: RwLock<Vec<Event>>,
}

impl InMemoryEventStore {
    /// Create a new in-memory event store
    pub fn new() -> Self {
        Self {
            events: RwLock::new(Vec::new()),
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
        filtered.sort_by(|a, b| b.timestamp().cmp(&a.timestamp()));
        Ok(filtered.into_iter().take(limit).collect())
    }

    async fn query_recent(&self, limit: usize) -> Result<Vec<Event>, StoreError> {
        let events = self
            .events
            .read()
            .map_err(|e| StoreError::Internal(e.to_string()))?;
        let mut sorted: Vec<_> = events.iter().cloned().collect();
        sorted.sort_by(|a, b| b.timestamp().cmp(&a.timestamp()));
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
