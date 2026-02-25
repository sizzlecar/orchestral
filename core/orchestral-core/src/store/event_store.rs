//! EventStore - Runtime fact storage trait

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

use super::StoreError;
use crate::types::{StepId, TaskId};

/// Strongly-typed Thread ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct ThreadId(pub String);

impl ThreadId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ThreadId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ThreadId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&ThreadId> for ThreadId {
    fn from(value: &ThreadId) -> Self {
        value.clone()
    }
}

impl From<ThreadId> for String {
    fn from(value: ThreadId) -> Self {
        value.0
    }
}

impl fmt::Display for ThreadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for ThreadId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<&str> for ThreadId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Strongly-typed Interaction ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct InteractionId(pub String);

impl InteractionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for InteractionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for InteractionId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&InteractionId> for InteractionId {
    fn from(value: &InteractionId) -> Self {
        value.clone()
    }
}

impl From<InteractionId> for String {
    fn from(value: InteractionId) -> Self {
        value.0
    }
}

impl fmt::Display for InteractionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for InteractionId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<&str> for InteractionId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Event - append-only fact record
///
/// Events represent immutable facts that happened in the system.
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
        /// Optional owning task ID
        #[serde(default)]
        task_id: Option<TaskId>,
        /// Optional owning step ID
        #[serde(default)]
        step_id: Option<StepId>,
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
        /// Optional owning task ID
        #[serde(default)]
        task_id: Option<TaskId>,
        /// Optional owning step ID
        #[serde(default)]
        step_id: Option<StepId>,
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
        /// Optional owning task ID
        #[serde(default)]
        task_id: Option<TaskId>,
        /// Optional owning step ID
        #[serde(default)]
        step_id: Option<StepId>,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },

    /// External event (webhook, timer, etc.)
    ExternalEvent {
        /// Thread this event belongs to
        thread_id: ThreadId,
        /// Optional interaction this event belongs to
        #[serde(default)]
        interaction_id: Option<InteractionId>,
        /// Kind of external event
        kind: String,
        /// Event payload
        payload: Value,
        /// Optional owning task ID
        #[serde(default)]
        task_id: Option<TaskId>,
        /// Optional owning step ID
        #[serde(default)]
        step_id: Option<StepId>,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },

    /// System trace event (for debugging and auditing)
    SystemTrace {
        /// Thread this event belongs to
        thread_id: ThreadId,
        /// Optional interaction this event belongs to
        #[serde(default)]
        interaction_id: Option<InteractionId>,
        /// Trace level (debug, info, warn, error)
        level: String,
        /// Trace payload
        payload: Value,
        /// Optional owning task ID
        #[serde(default)]
        task_id: Option<TaskId>,
        /// Optional owning step ID
        #[serde(default)]
        step_id: Option<StepId>,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },
}

impl Event {
    /// Create a new user input event
    pub fn user_input(
        thread_id: impl Into<ThreadId>,
        interaction_id: impl Into<InteractionId>,
        payload: Value,
    ) -> Self {
        Self::UserInput {
            thread_id: thread_id.into(),
            interaction_id: interaction_id.into(),
            payload,
            task_id: None,
            step_id: None,
            timestamp: Utc::now(),
        }
    }

    /// Create a new assistant output event
    pub fn assistant_output(
        thread_id: impl Into<ThreadId>,
        interaction_id: impl Into<InteractionId>,
        payload: Value,
    ) -> Self {
        Self::AssistantOutput {
            thread_id: thread_id.into(),
            interaction_id: interaction_id.into(),
            payload,
            task_id: None,
            step_id: None,
            timestamp: Utc::now(),
        }
    }

    /// Create a new artifact event
    pub fn artifact(
        thread_id: impl Into<ThreadId>,
        interaction_id: impl Into<InteractionId>,
        reference_id: impl Into<String>,
    ) -> Self {
        Self::Artifact {
            thread_id: thread_id.into(),
            interaction_id: interaction_id.into(),
            reference_id: reference_id.into(),
            task_id: None,
            step_id: None,
            timestamp: Utc::now(),
        }
    }

    /// Create a new external event
    pub fn external(
        thread_id: impl Into<ThreadId>,
        kind: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self::ExternalEvent {
            thread_id: thread_id.into(),
            interaction_id: None,
            kind: kind.into(),
            payload,
            task_id: None,
            step_id: None,
            timestamp: Utc::now(),
        }
    }

    /// Create a new system trace event
    pub fn trace(thread_id: impl Into<ThreadId>, level: impl Into<String>, payload: Value) -> Self {
        Self::SystemTrace {
            thread_id: thread_id.into(),
            interaction_id: None,
            level: level.into(),
            payload,
            task_id: None,
            step_id: None,
            timestamp: Utc::now(),
        }
    }

    /// Get the thread ID of this event
    pub fn thread_id(&self) -> &str {
        match self {
            Event::UserInput { thread_id, .. } => thread_id.as_str(),
            Event::AssistantOutput { thread_id, .. } => thread_id.as_str(),
            Event::Artifact { thread_id, .. } => thread_id.as_str(),
            Event::ExternalEvent { thread_id, .. } => thread_id.as_str(),
            Event::SystemTrace { thread_id, .. } => thread_id.as_str(),
        }
    }

    /// Get the interaction ID of this event if present.
    pub fn interaction_id(&self) -> Option<&str> {
        match self {
            Event::UserInput { interaction_id, .. } => Some(interaction_id.as_str()),
            Event::AssistantOutput { interaction_id, .. } => Some(interaction_id.as_str()),
            Event::Artifact { interaction_id, .. } => Some(interaction_id.as_str()),
            Event::ExternalEvent { interaction_id, .. } => {
                interaction_id.as_ref().map(|id| id.as_str())
            }
            Event::SystemTrace { interaction_id, .. } => {
                interaction_id.as_ref().map(|id| id.as_str())
            }
        }
    }

    /// Get the task ID of this event if present.
    pub fn task_id(&self) -> Option<&str> {
        match self {
            Event::UserInput { task_id, .. } => task_id.as_ref().map(|id| id.as_str()),
            Event::AssistantOutput { task_id, .. } => task_id.as_ref().map(|id| id.as_str()),
            Event::Artifact { task_id, .. } => task_id.as_ref().map(|id| id.as_str()),
            Event::ExternalEvent { task_id, .. } => task_id.as_ref().map(|id| id.as_str()),
            Event::SystemTrace { task_id, .. } => task_id.as_ref().map(|id| id.as_str()),
        }
    }

    /// Get the step ID of this event if present.
    pub fn step_id(&self) -> Option<&str> {
        match self {
            Event::UserInput { step_id, .. } => step_id.as_ref().map(|id| id.as_str()),
            Event::AssistantOutput { step_id, .. } => step_id.as_ref().map(|id| id.as_str()),
            Event::Artifact { step_id, .. } => step_id.as_ref().map(|id| id.as_str()),
            Event::ExternalEvent { step_id, .. } => step_id.as_ref().map(|id| id.as_str()),
            Event::SystemTrace { step_id, .. } => step_id.as_ref().map(|id| id.as_str()),
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

    /// Return a copy of this event with a new interaction ID.
    pub fn with_interaction_id(&self, interaction_id: impl Into<InteractionId>) -> Self {
        let interaction_id = interaction_id.into();
        match self {
            Event::UserInput {
                thread_id,
                payload,
                task_id,
                step_id,
                timestamp,
                ..
            } => Event::UserInput {
                thread_id: thread_id.clone(),
                interaction_id,
                payload: payload.clone(),
                task_id: task_id.clone(),
                step_id: step_id.clone(),
                timestamp: *timestamp,
            },
            Event::AssistantOutput {
                thread_id,
                payload,
                task_id,
                step_id,
                timestamp,
                ..
            } => Event::AssistantOutput {
                thread_id: thread_id.clone(),
                interaction_id,
                payload: payload.clone(),
                task_id: task_id.clone(),
                step_id: step_id.clone(),
                timestamp: *timestamp,
            },
            Event::Artifact {
                thread_id,
                reference_id,
                task_id,
                step_id,
                timestamp,
                ..
            } => Event::Artifact {
                thread_id: thread_id.clone(),
                interaction_id,
                reference_id: reference_id.clone(),
                task_id: task_id.clone(),
                step_id: step_id.clone(),
                timestamp: *timestamp,
            },
            Event::ExternalEvent {
                thread_id,
                kind,
                payload,
                task_id,
                step_id,
                timestamp,
                ..
            } => Event::ExternalEvent {
                thread_id: thread_id.clone(),
                interaction_id: Some(interaction_id),
                kind: kind.clone(),
                payload: payload.clone(),
                task_id: task_id.clone(),
                step_id: step_id.clone(),
                timestamp: *timestamp,
            },
            Event::SystemTrace {
                thread_id,
                level,
                payload,
                task_id,
                step_id,
                timestamp,
                ..
            } => Event::SystemTrace {
                thread_id: thread_id.clone(),
                interaction_id: Some(interaction_id),
                level: level.clone(),
                payload: payload.clone(),
                task_id: task_id.clone(),
                step_id: step_id.clone(),
                timestamp: *timestamp,
            },
        }
    }

    /// Return a copy of this event with task and step linkage.
    pub fn with_task_step(
        &self,
        task_id: Option<impl Into<TaskId>>,
        step_id: Option<impl Into<StepId>>,
    ) -> Self {
        let task_id = task_id.map(Into::into);
        let step_id = step_id.map(Into::into);
        match self {
            Event::UserInput {
                thread_id,
                interaction_id,
                payload,
                timestamp,
                ..
            } => Event::UserInput {
                thread_id: thread_id.clone(),
                interaction_id: interaction_id.clone(),
                payload: payload.clone(),
                task_id,
                step_id,
                timestamp: *timestamp,
            },
            Event::AssistantOutput {
                thread_id,
                interaction_id,
                payload,
                timestamp,
                ..
            } => Event::AssistantOutput {
                thread_id: thread_id.clone(),
                interaction_id: interaction_id.clone(),
                payload: payload.clone(),
                task_id,
                step_id,
                timestamp: *timestamp,
            },
            Event::Artifact {
                thread_id,
                interaction_id,
                reference_id,
                timestamp,
                ..
            } => Event::Artifact {
                thread_id: thread_id.clone(),
                interaction_id: interaction_id.clone(),
                reference_id: reference_id.clone(),
                task_id,
                step_id,
                timestamp: *timestamp,
            },
            Event::ExternalEvent {
                thread_id,
                interaction_id,
                kind,
                payload,
                timestamp,
                ..
            } => Event::ExternalEvent {
                thread_id: thread_id.clone(),
                interaction_id: interaction_id.clone(),
                kind: kind.clone(),
                payload: payload.clone(),
                task_id,
                step_id,
                timestamp: *timestamp,
            },
            Event::SystemTrace {
                thread_id,
                interaction_id,
                level,
                payload,
                timestamp,
                ..
            } => Event::SystemTrace {
                thread_id: thread_id.clone(),
                interaction_id: interaction_id.clone(),
                level: level.clone(),
                payload: payload.clone(),
                task_id,
                step_id,
                timestamp: *timestamp,
            },
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
