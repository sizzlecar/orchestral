//! Task type definitions
//!
//! Task represents the stateful execution context with state machine.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Intent, Plan};

/// Type alias for Task ID
pub type TaskId = String;

/// Task state machine - production-ready states
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TaskState {
    /// Currently planning
    Planning,
    /// Currently executing
    Executing,
    /// Waiting for user input
    WaitingUser {
        /// Prompt/question for the user
        prompt: String,
    },
    /// Waiting for external event
    WaitingEvent {
        /// Type of event being waited for
        event_type: String,
    },
    /// Paused (user-initiated or system-initiated)
    Paused,
    /// Execution failed
    Failed {
        /// Reason for failure
        reason: String,
        /// Whether this failure is recoverable
        recoverable: bool,
    },
    /// Execution completed successfully
    Done,
}

impl TaskState {
    /// Check if the task is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Done | TaskState::Failed { recoverable: false, .. }
        )
    }

    /// Check if the task can be resumed
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            TaskState::WaitingUser { .. }
                | TaskState::WaitingEvent { .. }
                | TaskState::Paused
                | TaskState::Failed { recoverable: true, .. }
        )
    }

    /// Check if the task is actively running
    pub fn is_active(&self) -> bool {
        matches!(self, TaskState::Planning | TaskState::Executing)
    }
}

/// Task - the stateful execution context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique identifier for this task
    pub id: TaskId,
    /// The intent that initiated this task
    pub intent: Intent,
    /// The execution plan (if planning is complete)
    #[serde(default)]
    pub plan: Option<Plan>,
    /// Current state of the task
    pub state: TaskState,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
}

impl Task {
    /// Create a new task from an intent
    pub fn new(intent: Intent) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            intent,
            plan: None,
            state: TaskState::Planning,
            created_at: now,
            updated_at: now,
        }
    }

    /// Update the task state
    pub fn set_state(&mut self, state: TaskState) {
        self.state = state;
        self.updated_at = Utc::now();
    }

    /// Set the plan
    pub fn set_plan(&mut self, plan: Plan) {
        self.plan = Some(plan);
        self.updated_at = Utc::now();
    }

    /// Transition to executing state
    pub fn start_executing(&mut self) {
        self.set_state(TaskState::Executing);
    }

    /// Transition to waiting for user
    pub fn wait_for_user(&mut self, prompt: impl Into<String>) {
        self.set_state(TaskState::WaitingUser {
            prompt: prompt.into(),
        });
    }

    /// Transition to failed state
    pub fn fail(&mut self, reason: impl Into<String>, recoverable: bool) {
        self.set_state(TaskState::Failed {
            reason: reason.into(),
            recoverable,
        });
    }

    /// Transition to done state
    pub fn complete(&mut self) {
        self.set_state(TaskState::Done);
    }

    /// Pause the task
    pub fn pause(&mut self) {
        self.set_state(TaskState::Paused);
    }
}
