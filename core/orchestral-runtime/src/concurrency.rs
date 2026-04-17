//! Concurrency Policy
//!
//! Defines how the runtime handles new inputs when an interaction is already running.

use orchestral_core::store::Event;

/// Decision on how to handle concurrent interactions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcurrencyDecision {
    /// Interrupt the current interaction and start a new one
    InterruptAndStartNew,
    /// Reject the new input
    Reject {
        /// Reason for rejection
        reason: String,
    },
    /// Queue the new input for later processing
    Queue,
    /// Run in parallel with the current interaction
    Parallel,
    /// Merge the new input into the running interaction
    MergeIntoRunning,
}

impl ConcurrencyDecision {
    /// Create a reject decision
    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }
}

/// Running state information for concurrency decisions
#[derive(Debug, Clone)]
pub struct RunningState {
    /// Number of currently active interactions
    pub active_count: usize,
    /// Whether any interaction is currently processing
    pub is_processing: bool,
    /// Whether any interaction is waiting for user input
    pub is_waiting_user: bool,
    /// Whether any interaction is waiting for external event
    pub is_waiting_event: bool,
}

impl RunningState {
    /// Create a new running state
    pub fn new() -> Self {
        Self {
            active_count: 0,
            is_processing: false,
            is_waiting_user: false,
            is_waiting_event: false,
        }
    }

    /// Check if there are any active interactions
    pub fn has_active(&self) -> bool {
        self.active_count > 0
    }

    /// Check if the runtime is idle
    pub fn is_idle(&self) -> bool {
        self.active_count == 0
    }
}

impl Default for RunningState {
    fn default() -> Self {
        Self::new()
    }
}

/// Concurrency policy trait
///
/// Implementations decide how to handle new inputs based on the current running state.
pub trait ConcurrencyPolicy: Send + Sync {
    /// Decide how to handle a new event given the current running state
    fn decide(&self, running: &RunningState, new_event: &Event) -> ConcurrencyDecision;
}

/// Default concurrency policy: Interrupt and start new
///
/// This is the most common behavior for chat-like applications.
pub struct DefaultConcurrencyPolicy;

impl ConcurrencyPolicy for DefaultConcurrencyPolicy {
    fn decide(&self, running: &RunningState, _new_event: &Event) -> ConcurrencyDecision {
        if running.is_idle() {
            // No active interaction, start new
            ConcurrencyDecision::InterruptAndStartNew
        } else {
            // Interrupt current and start new
            ConcurrencyDecision::InterruptAndStartNew
        }
    }
}

/// Queue policy placeholder.
///
/// Runtime-side queue execution is not implemented yet, so this policy currently
/// rejects when busy with an explicit reason.
pub struct QueueConcurrencyPolicy;

impl ConcurrencyPolicy for QueueConcurrencyPolicy {
    fn decide(&self, running: &RunningState, _new_event: &Event) -> ConcurrencyDecision {
        if running.is_idle() {
            ConcurrencyDecision::InterruptAndStartNew
        } else {
            ConcurrencyDecision::reject(
                "Queue policy is not implemented; use interrupt/parallel/reject policy",
            )
        }
    }
}

/// Parallel policy: Allow multiple interactions to run in parallel
pub struct ParallelConcurrencyPolicy {
    /// Maximum number of parallel interactions
    pub max_parallel: usize,
}

impl ParallelConcurrencyPolicy {
    /// Create a new parallel policy
    pub fn new(max_parallel: usize) -> Self {
        Self { max_parallel }
    }
}

impl Default for ParallelConcurrencyPolicy {
    fn default() -> Self {
        Self::new(4)
    }
}

impl ConcurrencyPolicy for ParallelConcurrencyPolicy {
    fn decide(&self, running: &RunningState, _new_event: &Event) -> ConcurrencyDecision {
        if running.active_count >= self.max_parallel {
            ConcurrencyDecision::reject(format!(
                "Maximum parallel interactions ({}) reached",
                self.max_parallel
            ))
        } else {
            ConcurrencyDecision::Parallel
        }
    }
}

/// Merge policy: Merge new inputs into the running interaction.
///
/// Best for chat bots — new messages join the current conversation turn.
/// If no interaction is active, starts a new one.
pub struct MergeConcurrencyPolicy;

impl ConcurrencyPolicy for MergeConcurrencyPolicy {
    fn decide(&self, running: &RunningState, _new_event: &Event) -> ConcurrencyDecision {
        if running.is_idle() {
            ConcurrencyDecision::InterruptAndStartNew
        } else {
            ConcurrencyDecision::MergeIntoRunning
        }
    }
}

/// Reject policy: Reject new inputs when busy
pub struct RejectWhenBusyConcurrencyPolicy;

impl ConcurrencyPolicy for RejectWhenBusyConcurrencyPolicy {
    fn decide(&self, running: &RunningState, _new_event: &Event) -> ConcurrencyDecision {
        if running.is_idle() {
            ConcurrencyDecision::InterruptAndStartNew
        } else {
            ConcurrencyDecision::reject("System is busy processing another request")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event() -> Event {
        Event::user_input("t1", "i1", json!({}))
    }

    fn busy() -> RunningState {
        RunningState {
            active_count: 1,
            is_processing: true,
            is_waiting_user: false,
            is_waiting_event: false,
        }
    }

    #[test]
    fn running_state_idle_and_active_flags() {
        let idle = RunningState::new();
        assert!(idle.is_idle());
        assert!(!idle.has_active());

        let running = busy();
        assert!(!running.is_idle());
        assert!(running.has_active());
    }

    #[test]
    fn default_policy_always_interrupts() {
        let policy = DefaultConcurrencyPolicy;
        assert_eq!(
            policy.decide(&RunningState::new(), &event()),
            ConcurrencyDecision::InterruptAndStartNew
        );
        assert_eq!(
            policy.decide(&busy(), &event()),
            ConcurrencyDecision::InterruptAndStartNew
        );
    }

    #[test]
    fn queue_policy_rejects_when_busy_with_explicit_reason() {
        let policy = QueueConcurrencyPolicy;
        assert_eq!(
            policy.decide(&RunningState::new(), &event()),
            ConcurrencyDecision::InterruptAndStartNew
        );
        match policy.decide(&busy(), &event()) {
            ConcurrencyDecision::Reject { reason } => {
                assert!(
                    reason.to_lowercase().contains("queue"),
                    "reason should mention queue placeholder, got: {reason}"
                );
            }
            other => panic!("expected Reject, got {other:?}"),
        }
    }

    #[test]
    fn parallel_policy_admits_until_cap_then_rejects() {
        let policy = ParallelConcurrencyPolicy::new(2);
        let one_running = RunningState {
            active_count: 1,
            is_processing: true,
            is_waiting_user: false,
            is_waiting_event: false,
        };
        assert_eq!(
            policy.decide(&one_running, &event()),
            ConcurrencyDecision::Parallel
        );

        let at_cap = RunningState {
            active_count: 2,
            is_processing: true,
            is_waiting_user: false,
            is_waiting_event: false,
        };
        match policy.decide(&at_cap, &event()) {
            ConcurrencyDecision::Reject { reason } => {
                assert!(
                    reason.contains('2'),
                    "reason should show cap, got: {reason}"
                );
            }
            other => panic!("expected Reject at cap, got {other:?}"),
        }
    }

    #[test]
    fn parallel_policy_default_cap_is_four() {
        let policy = ParallelConcurrencyPolicy::default();
        assert_eq!(policy.max_parallel, 4);
    }

    #[test]
    fn merge_policy_merges_only_when_busy() {
        let policy = MergeConcurrencyPolicy;
        assert_eq!(
            policy.decide(&RunningState::new(), &event()),
            ConcurrencyDecision::InterruptAndStartNew
        );
        assert_eq!(
            policy.decide(&busy(), &event()),
            ConcurrencyDecision::MergeIntoRunning
        );
    }

    #[test]
    fn reject_when_busy_policy_idle_interrupts_busy_rejects() {
        let policy = RejectWhenBusyConcurrencyPolicy;
        assert_eq!(
            policy.decide(&RunningState::new(), &event()),
            ConcurrencyDecision::InterruptAndStartNew
        );
        assert!(matches!(
            policy.decide(&busy(), &event()),
            ConcurrencyDecision::Reject { .. }
        ));
    }

    #[test]
    fn reject_helper_preserves_custom_reason() {
        let decision = ConcurrencyDecision::reject("custom reason");
        match decision {
            ConcurrencyDecision::Reject { reason } => assert_eq!(reason, "custom reason"),
            other => panic!("expected Reject, got {other:?}"),
        }
    }
}
