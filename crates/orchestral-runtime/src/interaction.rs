//! Interaction - One intervention
//!
//! Interaction represents a single trigger → response attempt within a Thread.
//! It replaces the concept of "Turn" with a more general abstraction.
//!
//! Key points:
//! - Usually triggered by an Event
//! - Multiple Interactions can exist in the same Thread concurrently

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::thread::ThreadId;
use orchestral_core::types::TaskId;

/// Type alias for Interaction ID
pub type InteractionId = String;

/// Interaction state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionState {
    /// Interaction is active and processing
    Active,
    /// Interaction is waiting for user input
    WaitingUser,
    /// Interaction is waiting for external event
    WaitingEvent,
    /// Interaction is paused
    Paused,
    /// Interaction completed successfully
    Completed,
    /// Interaction failed
    Failed,
    /// Interaction was cancelled
    Cancelled,
}

impl InteractionState {
    /// Check if the interaction is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            InteractionState::Completed | InteractionState::Failed | InteractionState::Cancelled
        )
    }

    /// Check if the interaction is active
    pub fn is_active(&self) -> bool {
        matches!(self, InteractionState::Active)
    }

    /// Check if the interaction is waiting
    pub fn is_waiting(&self) -> bool {
        matches!(
            self,
            InteractionState::WaitingUser | InteractionState::WaitingEvent
        )
    }
}

/// Interaction - a single trigger → response attempt
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interaction {
    /// Unique identifier
    pub id: InteractionId,
    /// Thread this interaction belongs to
    pub thread_id: ThreadId,
    /// Current state
    pub state: InteractionState,
    /// Task IDs associated with this interaction
    pub task_ids: Vec<TaskId>,
    /// Start timestamp
    pub started_at: DateTime<Utc>,
    /// End timestamp (if completed)
    pub ended_at: Option<DateTime<Utc>>,
}

impl Interaction {
    /// Create a new interaction
    pub fn new(thread_id: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            thread_id: thread_id.into(),
            state: InteractionState::Active,
            task_ids: Vec::new(),
            started_at: Utc::now(),
            ended_at: None,
        }
    }

    /// Create a new interaction with specific ID
    pub fn with_id(id: impl Into<String>, thread_id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            thread_id: thread_id.into(),
            state: InteractionState::Active,
            task_ids: Vec::new(),
            started_at: Utc::now(),
            ended_at: None,
        }
    }

    /// Add a task to this interaction
    pub fn add_task(&mut self, task_id: TaskId) {
        self.task_ids.push(task_id);
    }

    /// Set the interaction state
    pub fn set_state(&mut self, state: InteractionState) {
        let is_terminal = state.is_terminal();
        self.state = state;
        if is_terminal && self.ended_at.is_none() {
            self.ended_at = Some(Utc::now());
        }
    }

    /// Mark the interaction as completed
    pub fn complete(&mut self) {
        self.set_state(InteractionState::Completed);
    }

    /// Mark the interaction as failed
    pub fn fail(&mut self) {
        self.set_state(InteractionState::Failed);
    }

    /// Mark the interaction as cancelled
    pub fn cancel(&mut self) {
        self.set_state(InteractionState::Cancelled);
    }

    /// Mark the interaction as waiting for user
    pub fn wait_for_user(&mut self) {
        self.set_state(InteractionState::WaitingUser);
    }

    /// Mark the interaction as waiting for event
    pub fn wait_for_event(&mut self) {
        self.set_state(InteractionState::WaitingEvent);
    }

    /// Resume the interaction (set back to active)
    pub fn resume(&mut self) {
        if !self.state.is_terminal() {
            self.state = InteractionState::Active;
        }
    }

    /// Get the duration of this interaction (if ended)
    pub fn duration(&self) -> Option<chrono::Duration> {
        self.ended_at.map(|end| end - self.started_at)
    }
}
