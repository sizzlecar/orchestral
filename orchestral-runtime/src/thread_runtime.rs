//! ThreadRuntime - Thread lifecycle and interaction management
//!
//! ThreadRuntime manages:
//! - Thread lifecycle
//! - Interaction concurrency
//! - Event routing

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::concurrency::{ConcurrencyDecision, ConcurrencyPolicy, DefaultConcurrencyPolicy, RunningState};
use super::interaction::{Interaction, InteractionId, InteractionState};
use super::thread::{Thread, ThreadId};
use orchestral_stores::{Event, EventStore};

/// Configuration for ThreadRuntime
#[derive(Debug, Clone)]
pub struct ThreadRuntimeConfig {
    /// Maximum number of active interactions per thread
    pub max_interactions_per_thread: usize,
    /// Whether to auto-cleanup completed interactions
    pub auto_cleanup: bool,
}

impl Default for ThreadRuntimeConfig {
    fn default() -> Self {
        Self {
            max_interactions_per_thread: 10,
            auto_cleanup: true,
        }
    }
}

/// ThreadRuntime - manages thread lifecycle and interaction concurrency
pub struct ThreadRuntime {
    /// The thread being managed
    pub thread: RwLock<Thread>,
    /// Active interactions in this thread
    pub interactions: RwLock<HashMap<InteractionId, Interaction>>,
    /// Concurrency policy
    pub concurrency_policy: Arc<dyn ConcurrencyPolicy>,
    /// Event store
    pub event_store: Arc<dyn EventStore>,
    /// Configuration
    pub config: ThreadRuntimeConfig,
}

impl ThreadRuntime {
    /// Create a new thread runtime
    pub fn new(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
    ) -> Self {
        Self {
            thread: RwLock::new(thread),
            interactions: RwLock::new(HashMap::new()),
            concurrency_policy: Arc::new(DefaultConcurrencyPolicy),
            event_store,
            config: ThreadRuntimeConfig::default(),
        }
    }

    /// Create a new thread runtime with custom policy
    pub fn with_policy(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        policy: Arc<dyn ConcurrencyPolicy>,
    ) -> Self {
        Self {
            thread: RwLock::new(thread),
            interactions: RwLock::new(HashMap::new()),
            concurrency_policy: policy,
            event_store,
            config: ThreadRuntimeConfig::default(),
        }
    }

    /// Create a new thread runtime with custom config
    pub fn with_config(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        config: ThreadRuntimeConfig,
    ) -> Self {
        Self {
            thread: RwLock::new(thread),
            interactions: RwLock::new(HashMap::new()),
            concurrency_policy: Arc::new(DefaultConcurrencyPolicy),
            event_store,
            config,
        }
    }

    /// Get the thread ID
    pub async fn thread_id(&self) -> ThreadId {
        self.thread.read().await.id.clone()
    }

    /// Get the current running state
    pub async fn running_state(&self) -> RunningState {
        let interactions = self.interactions.read().await;
        let active_count = interactions
            .values()
            .filter(|i| !i.state.is_terminal())
            .count();
        let is_processing = interactions
            .values()
            .any(|i| i.state == InteractionState::Active);
        let is_waiting_user = interactions
            .values()
            .any(|i| i.state == InteractionState::WaitingUser);
        let is_waiting_event = interactions
            .values()
            .any(|i| i.state == InteractionState::WaitingEvent);

        RunningState {
            active_count,
            is_processing,
            is_waiting_user,
            is_waiting_event,
        }
    }

    /// Handle a new event
    pub async fn handle_event(&self, event: Event) -> Result<HandleEventResult, RuntimeError> {
        // Get current running state
        let running_state = self.running_state().await;

        // Ask policy for decision
        let decision = self.concurrency_policy.decide(&running_state, &event);

        // Store the event
        self.event_store
            .append(event.clone())
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?;

        // Handle based on decision
        match decision {
            ConcurrencyDecision::InterruptAndStartNew => {
                // Cancel all active interactions
                self.cancel_all_active().await;

                // Create new interaction
                let thread_id = self.thread_id().await;
                let interaction = Interaction::new(&thread_id);
                let interaction_id = interaction.id.clone();

                let mut interactions = self.interactions.write().await;
                interactions.insert(interaction_id.clone(), interaction);

                // Touch the thread
                self.thread.write().await.touch();

                Ok(HandleEventResult::Started { interaction_id })
            }
            ConcurrencyDecision::Reject { reason } => {
                Ok(HandleEventResult::Rejected { reason })
            }
            ConcurrencyDecision::Queue => {
                Ok(HandleEventResult::Queued)
            }
            ConcurrencyDecision::Parallel => {
                // Create new interaction without interrupting existing ones
                let thread_id = self.thread_id().await;
                let interaction = Interaction::new(&thread_id);
                let interaction_id = interaction.id.clone();

                let mut interactions = self.interactions.write().await;
                interactions.insert(interaction_id.clone(), interaction);

                // Touch the thread
                self.thread.write().await.touch();

                Ok(HandleEventResult::Started { interaction_id })
            }
            ConcurrencyDecision::MergeIntoRunning => {
                // Find the active interaction and merge
                let interactions = self.interactions.read().await;
                let active_id = interactions
                    .values()
                    .find(|i| i.state == InteractionState::Active)
                    .map(|i| i.id.clone());

                if let Some(interaction_id) = active_id {
                    Ok(HandleEventResult::Merged { interaction_id })
                } else {
                    // No active interaction, start new
                    drop(interactions);
                    let thread_id = self.thread_id().await;
                    let interaction = Interaction::new(&thread_id);
                    let interaction_id = interaction.id.clone();

                    let mut interactions = self.interactions.write().await;
                    interactions.insert(interaction_id.clone(), interaction);

                    self.thread.write().await.touch();

                    Ok(HandleEventResult::Started { interaction_id })
                }
            }
        }
    }

    /// Cancel all active interactions
    pub async fn cancel_all_active(&self) {
        let mut interactions = self.interactions.write().await;
        for interaction in interactions.values_mut() {
            if !interaction.state.is_terminal() {
                interaction.cancel();
            }
        }

        // Auto-cleanup if enabled
        if self.config.auto_cleanup {
            interactions.retain(|_, i| !i.state.is_terminal());
        }
    }

    /// Get an interaction by ID
    pub async fn get_interaction(&self, id: &str) -> Option<Interaction> {
        let interactions = self.interactions.read().await;
        interactions.get(id).cloned()
    }

    /// Update an interaction's state
    pub async fn update_interaction_state(
        &self,
        id: &str,
        state: InteractionState,
    ) -> Result<(), RuntimeError> {
        let mut interactions = self.interactions.write().await;
        if let Some(interaction) = interactions.get_mut(id) {
            interaction.set_state(state);
            Ok(())
        } else {
            Err(RuntimeError::InteractionNotFound(id.to_string()))
        }
    }

    /// Complete an interaction
    pub async fn complete_interaction(&self, id: &str) -> Result<(), RuntimeError> {
        self.update_interaction_state(id, InteractionState::Completed)
            .await
    }

    /// Fail an interaction
    pub async fn fail_interaction(&self, id: &str) -> Result<(), RuntimeError> {
        self.update_interaction_state(id, InteractionState::Failed)
            .await
    }

    /// Get all active interaction IDs
    pub async fn active_interaction_ids(&self) -> Vec<InteractionId> {
        let interactions = self.interactions.read().await;
        interactions
            .values()
            .filter(|i| !i.state.is_terminal())
            .map(|i| i.id.clone())
            .collect()
    }

    /// Cleanup completed interactions
    pub async fn cleanup_completed(&self) {
        let mut interactions = self.interactions.write().await;
        interactions.retain(|_, i| !i.state.is_terminal());
    }
}

/// Result of handling an event
#[derive(Debug, Clone)]
pub enum HandleEventResult {
    /// A new interaction was started
    Started {
        /// ID of the new interaction
        interaction_id: InteractionId,
    },
    /// The event was rejected
    Rejected {
        /// Reason for rejection
        reason: String,
    },
    /// The event was queued for later processing
    Queued,
    /// The event was merged into an existing interaction
    Merged {
        /// ID of the interaction the event was merged into
        interaction_id: InteractionId,
    },
}

/// Runtime errors
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("Store error: {0}")]
    StoreError(String),

    #[error("Interaction not found: {0}")]
    InteractionNotFound(String),

    #[error("Thread not found: {0}")]
    ThreadNotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
