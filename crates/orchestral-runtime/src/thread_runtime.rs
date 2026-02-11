//! ThreadRuntime - Thread lifecycle and interaction management
//!
//! ThreadRuntime manages:
//! - Thread lifecycle
//! - Interaction concurrency
//! - Event routing

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::concurrency::{
    ConcurrencyDecision, ConcurrencyPolicy, DefaultConcurrencyPolicy, RunningState,
};
use super::interaction::{Interaction, InteractionId, InteractionState};
use super::thread::{Thread, ThreadId};
use orchestral_core::types::TaskId;
use orchestral_stores::{BroadcastEventBus, Event, EventBus, EventStore};

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
    /// Realtime event bus
    pub event_bus: Arc<dyn EventBus>,
    /// Configuration
    pub config: ThreadRuntimeConfig,
}

impl ThreadRuntime {
    /// Create a new thread runtime
    pub fn new(thread: Thread, event_store: Arc<dyn EventStore>) -> Self {
        Self::new_with_bus(thread, event_store, Arc::new(BroadcastEventBus::default()))
    }

    /// Create a new thread runtime with custom event bus
    pub fn new_with_bus(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            thread: RwLock::new(thread),
            interactions: RwLock::new(HashMap::new()),
            concurrency_policy: Arc::new(DefaultConcurrencyPolicy),
            event_store,
            event_bus,
            config: ThreadRuntimeConfig::default(),
        }
    }

    /// Create a new thread runtime with custom policy
    pub fn with_policy(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        policy: Arc<dyn ConcurrencyPolicy>,
    ) -> Self {
        Self::with_policy_and_bus(
            thread,
            event_store,
            policy,
            Arc::new(BroadcastEventBus::default()),
        )
    }

    /// Create a new thread runtime with custom policy and event bus
    pub fn with_policy_and_bus(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        policy: Arc<dyn ConcurrencyPolicy>,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            thread: RwLock::new(thread),
            interactions: RwLock::new(HashMap::new()),
            concurrency_policy: policy,
            event_store,
            event_bus,
            config: ThreadRuntimeConfig::default(),
        }
    }

    /// Create a new thread runtime with custom config
    pub fn with_config(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        config: ThreadRuntimeConfig,
    ) -> Self {
        Self::with_config_and_bus(
            thread,
            event_store,
            config,
            Arc::new(BroadcastEventBus::default()),
        )
    }

    /// Create a new thread runtime with custom config and event bus
    pub fn with_config_and_bus(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        config: ThreadRuntimeConfig,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            thread: RwLock::new(thread),
            interactions: RwLock::new(HashMap::new()),
            concurrency_policy: Arc::new(DefaultConcurrencyPolicy),
            event_store,
            event_bus,
            config,
        }
    }

    /// Create a new thread runtime with custom policy and config
    pub fn with_policy_and_config(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        policy: Arc<dyn ConcurrencyPolicy>,
        config: ThreadRuntimeConfig,
    ) -> Self {
        Self::with_policy_config_and_bus(
            thread,
            event_store,
            policy,
            config,
            Arc::new(BroadcastEventBus::default()),
        )
    }

    /// Create a new thread runtime with custom policy, config, and event bus
    pub fn with_policy_config_and_bus(
        thread: Thread,
        event_store: Arc<dyn EventStore>,
        policy: Arc<dyn ConcurrencyPolicy>,
        config: ThreadRuntimeConfig,
        event_bus: Arc<dyn EventBus>,
    ) -> Self {
        Self {
            thread: RwLock::new(thread),
            interactions: RwLock::new(HashMap::new()),
            concurrency_policy: policy,
            event_store,
            event_bus,
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
        self.validate_event(&event).await?;

        // Get current running state
        let running_state = self.running_state().await;

        // Ask policy for decision
        let decision = self.concurrency_policy.decide(&running_state, &event);

        // Handle based on decision
        match decision {
            ConcurrencyDecision::InterruptAndStartNew => {
                // Cancel all active interactions
                self.cancel_all_active().await;

                let interaction_id = match self.create_interaction_if_allowed().await {
                    Ok(id) => id,
                    Err(reason) => {
                        self.persist_event(event).await?;
                        return Ok(HandleEventResult::Rejected { reason });
                    }
                };

                // Touch the thread
                self.thread.write().await.touch();

                // Store the event with the runtime-generated interaction_id
                self.persist_event(event.with_interaction_id(&interaction_id))
                    .await?;

                Ok(HandleEventResult::Started { interaction_id })
            }
            ConcurrencyDecision::Reject { reason } => {
                // Store the event as-is (no interaction created)
                self.persist_event(event).await?;
                Ok(HandleEventResult::Rejected { reason })
            }
            ConcurrencyDecision::Queue => {
                // Queueing is not implemented yet. Reject explicitly to avoid silent drops.
                self.persist_event(event).await?;
                Ok(HandleEventResult::Rejected {
                    reason: "Queue policy is configured but queue execution is not implemented"
                        .to_string(),
                })
            }
            ConcurrencyDecision::Parallel => {
                let interaction_id = match self.create_interaction_if_allowed().await {
                    Ok(id) => id,
                    Err(reason) => {
                        self.persist_event(event).await?;
                        return Ok(HandleEventResult::Rejected { reason });
                    }
                };

                // Touch the thread
                self.thread.write().await.touch();

                // Store the event with the runtime-generated interaction_id
                self.persist_event(event.with_interaction_id(&interaction_id))
                    .await?;

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
                    // Store the event with the active interaction_id
                    self.persist_event(event.with_interaction_id(&interaction_id))
                        .await?;
                    Ok(HandleEventResult::Merged { interaction_id })
                } else {
                    // No active interaction, start new
                    drop(interactions);
                    let interaction_id = match self.create_interaction_if_allowed().await {
                        Ok(id) => id,
                        Err(reason) => {
                            self.persist_event(event).await?;
                            return Ok(HandleEventResult::Rejected { reason });
                        }
                    };

                    self.thread.write().await.touch();

                    // Store the event with the runtime-generated interaction_id
                    self.persist_event(event.with_interaction_id(&interaction_id))
                        .await?;

                    Ok(HandleEventResult::Started { interaction_id })
                }
            }
        }
    }

    async fn create_interaction_if_allowed(&self) -> Result<InteractionId, String> {
        let thread_id = self.thread_id().await;
        let interaction = Interaction::new(&thread_id);
        let interaction_id = interaction.id.clone();

        let mut interactions = self.interactions.write().await;
        let active_count = interactions
            .values()
            .filter(|i| !i.state.is_terminal())
            .count();
        if active_count >= self.config.max_interactions_per_thread {
            return Err(format!(
                "Maximum active interactions ({}) reached",
                self.config.max_interactions_per_thread
            ));
        }

        interactions.insert(interaction_id.clone(), interaction);
        Ok(interaction_id)
    }

    async fn validate_event(&self, event: &Event) -> Result<(), RuntimeError> {
        let expected_thread_id = self.thread_id().await;
        let got_thread_id = event.thread_id();
        if expected_thread_id != got_thread_id {
            return Err(RuntimeError::InvalidEvent(format!(
                "thread_id mismatch (expected {}, got {})",
                expected_thread_id, got_thread_id
            )));
        }

        if !payload_is_valid(event) {
            return Err(RuntimeError::InvalidEvent(
                "payload must not be null for user/external events".to_string(),
            ));
        }

        Ok(())
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
        let key: InteractionId = id.into();
        interactions.get(&key).cloned()
    }

    /// Add a task to an interaction
    pub async fn add_task_to_interaction(
        &self,
        id: &str,
        task_id: TaskId,
    ) -> Result<(), RuntimeError> {
        let mut interactions = self.interactions.write().await;
        let key: InteractionId = id.into();
        if let Some(interaction) = interactions.get_mut(&key) {
            interaction.add_task(task_id);
            Ok(())
        } else {
            Err(RuntimeError::InteractionNotFound(id.to_string()))
        }
    }

    /// Find a waiting interaction that can be resumed by this event.
    pub async fn find_resume_interaction(&self, event: &Event) -> Option<InteractionId> {
        let target_state = match event {
            Event::UserInput { .. } => InteractionState::WaitingUser,
            Event::ExternalEvent { .. } => InteractionState::WaitingEvent,
            _ => return None,
        };

        let interactions = self.interactions.read().await;
        interactions
            .values()
            .filter(|i| i.state == target_state)
            .max_by_key(|i| i.started_at)
            .map(|i| i.id.clone())
    }

    /// Append an event to an existing interaction and keep event/interaction IDs consistent.
    pub async fn append_event_to_interaction(
        &self,
        interaction_id: &str,
        event: Event,
    ) -> Result<(), RuntimeError> {
        self.validate_event(&event).await?;

        let exists = {
            let interactions = self.interactions.read().await;
            let key: InteractionId = interaction_id.into();
            interactions.contains_key(&key)
        };
        if !exists {
            return Err(RuntimeError::InteractionNotFound(
                interaction_id.to_string(),
            ));
        }

        self.persist_event(event.with_interaction_id(interaction_id))
            .await?;
        self.thread.write().await.touch();
        Ok(())
    }

    /// Subscribe to the realtime event stream.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.event_bus.subscribe()
    }

    /// Mark a waiting interaction active so execution can continue.
    pub async fn resume_interaction(&self, id: &str) -> Result<(), RuntimeError> {
        let mut interactions = self.interactions.write().await;
        let key: InteractionId = id.into();
        let interaction = interactions
            .get_mut(&key)
            .ok_or_else(|| RuntimeError::InteractionNotFound(id.to_string()))?;
        if interaction.state.is_terminal() {
            return Err(RuntimeError::InvalidEvent(format!(
                "interaction '{}' is terminal and cannot be resumed",
                id
            )));
        }
        interaction.resume();
        Ok(())
    }

    /// Update an interaction's state
    pub async fn update_interaction_state(
        &self,
        id: &str,
        state: InteractionState,
    ) -> Result<(), RuntimeError> {
        let mut interactions = self.interactions.write().await;
        let key: InteractionId = id.into();
        if let Some(interaction) = interactions.get_mut(&key) {
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

    /// Query recent events for this thread
    pub async fn query_history(&self, limit: usize) -> Result<Vec<Event>, RuntimeError> {
        let thread_id = self.thread_id().await;
        let events = if limit == 0 {
            self.event_store
                .query_by_thread(thread_id.as_str())
                .await
                .map_err(|e| RuntimeError::StoreError(e.to_string()))?
        } else {
            self.event_store
                .query_by_thread_with_limit(thread_id.as_str(), limit)
                .await
                .map_err(|e| RuntimeError::StoreError(e.to_string()))?
        };
        Ok(events)
    }

    /// Cleanup completed interactions
    pub async fn cleanup_completed(&self) {
        let mut interactions = self.interactions.write().await;
        interactions.retain(|_, i| !i.state.is_terminal());
    }

    async fn persist_event(&self, event: Event) -> Result<(), RuntimeError> {
        self.event_store
            .append(event.clone())
            .await
            .map_err(|e| RuntimeError::StoreError(e.to_string()))?;
        self.event_bus
            .publish(event)
            .await
            .map_err(|e| RuntimeError::Internal(format!("event bus publish failed: {}", e)))?;
        Ok(())
    }
}

fn payload_is_valid(event: &Event) -> bool {
    match event {
        Event::UserInput { payload, .. } | Event::ExternalEvent { payload, .. } => {
            !payload.is_null()
        }
        _ => true,
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

    #[error("Invalid event: {0}")]
    InvalidEvent(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency::{ParallelConcurrencyPolicy, QueueConcurrencyPolicy};
    use chrono::{Duration, Utc};
    use orchestral_stores::{BroadcastEventBus, InMemoryEventStore};
    use serde_json::json;

    #[test]
    fn test_find_resume_interaction_prefers_latest_waiting_user() {
        tokio_test::block_on(async {
            let thread_id = "thread-1";
            let runtime = ThreadRuntime::new(
                Thread::with_id(thread_id),
                Arc::new(InMemoryEventStore::new()),
            );

            {
                let mut interactions = runtime.interactions.write().await;

                let mut older = Interaction::with_id("older", thread_id);
                older.set_state(InteractionState::WaitingUser);
                older.started_at = Utc::now() - Duration::seconds(10);
                interactions.insert(older.id.clone(), older);

                let mut newer = Interaction::with_id("newer", thread_id);
                newer.set_state(InteractionState::WaitingUser);
                newer.started_at = Utc::now();
                interactions.insert(newer.id.clone(), newer);
            }

            let event = Event::user_input(thread_id, "ignored", json!({"message":"resume"}));
            let found = runtime.find_resume_interaction(&event).await;
            assert_eq!(found.as_ref().map(|id| id.as_str()), Some("newer"));
        });
    }

    #[test]
    fn test_append_event_to_interaction_rewrites_user_interaction_id() {
        tokio_test::block_on(async {
            let thread_id = "thread-1";
            let runtime = ThreadRuntime::new(
                Thread::with_id(thread_id),
                Arc::new(InMemoryEventStore::new()),
            );

            {
                let mut interactions = runtime.interactions.write().await;
                interactions.insert("target".into(), Interaction::with_id("target", thread_id));
            }

            let event = Event::user_input(thread_id, "wrong", json!({"text":"hello"}));
            runtime
                .append_event_to_interaction("target", event)
                .await
                .unwrap();

            let events = runtime.query_history(0).await.unwrap();
            assert_eq!(events.len(), 1);
            match &events[0] {
                Event::UserInput { interaction_id, .. } => {
                    assert_eq!(interaction_id.as_str(), "target");
                }
                _ => panic!("expected user_input event"),
            }
        });
    }

    #[test]
    fn test_handle_event_publishes_to_event_bus() {
        tokio_test::block_on(async {
            let thread_id = "thread-1";
            let runtime = ThreadRuntime::new_with_bus(
                Thread::with_id(thread_id),
                Arc::new(InMemoryEventStore::new()),
                Arc::new(BroadcastEventBus::new(16)),
            );
            let mut sub = runtime.subscribe_events();

            let event = Event::user_input(thread_id, "cli", json!({"message":"hello"}));
            let result = runtime.handle_event(event).await.unwrap();
            assert!(matches!(result, HandleEventResult::Started { .. }));

            let published = sub.recv().await.expect("published event");
            match published {
                Event::UserInput {
                    interaction_id,
                    payload,
                    ..
                } => {
                    assert_ne!(interaction_id.as_str(), "cli");
                    assert_eq!(payload["message"], "hello");
                }
                _ => panic!("expected user_input event"),
            }
        });
    }

    #[test]
    fn test_rejects_when_max_active_interactions_reached() {
        tokio_test::block_on(async {
            let thread_id = "thread-max";
            let runtime = ThreadRuntime::with_policy_and_config(
                Thread::with_id(thread_id),
                Arc::new(InMemoryEventStore::new()),
                Arc::new(ParallelConcurrencyPolicy::new(10)),
                ThreadRuntimeConfig {
                    max_interactions_per_thread: 1,
                    auto_cleanup: false,
                },
            );

            let first = Event::user_input(thread_id, "a", json!({"message":"first"}));
            let first_result = runtime.handle_event(first).await.unwrap();
            assert!(matches!(first_result, HandleEventResult::Started { .. }));

            let second = Event::user_input(thread_id, "b", json!({"message":"second"}));
            let second_result = runtime.handle_event(second).await.unwrap();
            match second_result {
                HandleEventResult::Rejected { reason } => {
                    assert!(reason.contains("Maximum active interactions (1) reached"));
                }
                other => panic!("expected rejected result, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_queue_policy_returns_rejected_not_queued() {
        tokio_test::block_on(async {
            let thread_id = "thread-queue";
            let runtime = ThreadRuntime::with_policy(
                Thread::with_id(thread_id),
                Arc::new(InMemoryEventStore::new()),
                Arc::new(QueueConcurrencyPolicy),
            );

            let first = Event::user_input(thread_id, "a", json!({"message":"first"}));
            let first_result = runtime.handle_event(first).await.unwrap();
            assert!(matches!(first_result, HandleEventResult::Started { .. }));

            let second = Event::user_input(thread_id, "b", json!({"message":"second"}));
            let second_result = runtime.handle_event(second).await.unwrap();
            match second_result {
                HandleEventResult::Rejected { reason } => {
                    assert!(reason.contains("Queue policy"));
                }
                other => panic!("expected rejected result, got {:?}", other),
            }
        });
    }
}
