//! Task type definitions
//!
//! Task represents the stateful execution context with state machine.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::action::ApprovalRequest;

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
        /// Structured waiting reason
        #[serde(default)]
        reason: WaitUserReason,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitUserReason {
    #[default]
    Input,
    Approval {
        reason: String,
        command: Option<String>,
    },
}

impl TaskState {
    /// Check if the task is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Done
                | TaskState::Failed {
                    recoverable: false,
                    ..
                }
        )
    }

    /// Check if the task can be resumed
    pub fn is_resumable(&self) -> bool {
        matches!(
            self,
            TaskState::WaitingUser { .. }
                | TaskState::WaitingEvent { .. }
                | TaskState::Paused
                | TaskState::Failed {
                    recoverable: true,
                    ..
                }
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
    /// Completed step IDs for resume/replay
    #[serde(default)]
    pub completed_step_ids: Vec<String>,
    /// Task-scope working set snapshot for resume
    #[serde(default)]
    pub working_set_snapshot: HashMap<String, Value>,
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
            completed_step_ids: Vec::new(),
            working_set_snapshot: HashMap::new(),
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
            reason: WaitUserReason::Input,
        });
    }

    pub fn wait_for_approval(&mut self, request: &ApprovalRequest) {
        self.set_state(TaskState::WaitingUser {
            prompt: "Approval required".to_string(),
            reason: WaitUserReason::Approval {
                reason: request.reason.clone(),
                command: request.command.clone(),
            },
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

    /// Update execution checkpoint data
    pub fn set_checkpoint(
        &mut self,
        completed_step_ids: Vec<String>,
        working_set_snapshot: HashMap<String, Value>,
    ) {
        self.completed_step_ids = completed_step_ids;
        self.working_set_snapshot = working_set_snapshot;
        self.updated_at = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Intent;

    fn sample_intent() -> Intent {
        Intent::new("hello".to_string())
    }

    #[test]
    fn test_task_state_classification_flags() {
        assert!(TaskState::Planning.is_active());
        assert!(TaskState::Executing.is_active());
        assert!(!TaskState::Done.is_active());

        assert!(TaskState::Done.is_terminal());
        assert!(TaskState::Failed {
            reason: "fatal".to_string(),
            recoverable: false,
        }
        .is_terminal());
        assert!(!TaskState::Failed {
            reason: "recover".to_string(),
            recoverable: true,
        }
        .is_terminal());

        assert!(TaskState::Paused.is_resumable());
        assert!(TaskState::WaitingUser {
            prompt: "input".to_string(),
            reason: WaitUserReason::Input,
        }
        .is_resumable());
        assert!(TaskState::WaitingEvent {
            event_type: "timer".to_string(),
        }
        .is_resumable());
        assert!(TaskState::Failed {
            reason: "recover".to_string(),
            recoverable: true,
        }
        .is_resumable());
        assert!(!TaskState::Done.is_resumable());
    }

    #[test]
    fn test_task_transition_methods_update_state() {
        let mut task = Task::new(sample_intent());
        assert!(matches!(task.state, TaskState::Planning));

        task.start_executing();
        assert!(matches!(task.state, TaskState::Executing));

        task.wait_for_user("need input");
        assert!(matches!(
            task.state,
            TaskState::WaitingUser {
                reason: WaitUserReason::Input,
                ..
            }
        ));

        let approval = ApprovalRequest {
            reason: "dangerous command".to_string(),
            command: Some("rm -rf /".to_string()),
        };
        task.wait_for_approval(&approval);
        assert!(matches!(
            task.state,
            TaskState::WaitingUser {
                reason: WaitUserReason::Approval { .. },
                ..
            }
        ));

        task.fail("temporary", true);
        assert!(matches!(
            task.state,
            TaskState::Failed {
                recoverable: true,
                ..
            }
        ));

        task.pause();
        assert!(matches!(task.state, TaskState::Paused));

        task.complete();
        assert!(matches!(task.state, TaskState::Done));
    }

    #[test]
    fn test_task_checkpoint_updates_payload() {
        let mut task = Task::new(sample_intent());
        let checkpoint = HashMap::from([
            ("x".to_string(), Value::String("1".to_string())),
            ("y".to_string(), Value::Number(2.into())),
        ]);
        task.set_checkpoint(vec!["s1".to_string(), "s2".to_string()], checkpoint.clone());

        assert_eq!(
            task.completed_step_ids,
            vec!["s1".to_string(), "s2".to_string()]
        );
        assert_eq!(task.working_set_snapshot, checkpoint);
    }
}
