//! EventBus - realtime event fan-out abstraction.
//!
//! EventBus complements EventStore:
//! - EventStore persists facts (journal).
//! - EventBus pushes the same facts to live subscribers.

use async_trait::async_trait;
use tokio::sync::broadcast;

use orchestral_core::store::StoreError;

use crate::Event;

/// EventBus trait - async interface for realtime event publish/subscribe.
#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publish an event to all active subscribers.
    async fn publish(&self, event: Event) -> Result<(), StoreError>;

    /// Subscribe to realtime events.
    fn subscribe(&self) -> broadcast::Receiver<Event>;
}

/// In-process EventBus based on tokio broadcast channels.
pub struct BroadcastEventBus {
    tx: broadcast::Sender<Event>,
    capacity: usize,
}

impl BroadcastEventBus {
    /// Create a new broadcast bus with channel capacity.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (tx, _) = broadcast::channel(capacity);
        Self { tx, capacity }
    }

    /// Return the configured channel capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Default for BroadcastEventBus {
    fn default() -> Self {
        // Default capacity for local realtime consumers.
        Self::new(1024)
    }
}

#[async_trait]
impl EventBus for BroadcastEventBus {
    async fn publish(&self, event: Event) -> Result<(), StoreError> {
        // Ignore "no receiver" as a non-error; journal remains source-of-truth.
        match self.tx.send(event) {
            Ok(_) => Ok(()),
            Err(broadcast::error::SendError(_)) => Ok(()),
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_broadcast_bus_delivers_event() {
        tokio_test::block_on(async {
            let bus = BroadcastEventBus::new(16);
            let mut rx = bus.subscribe();

            bus.publish(Event::external("thread-1", "tick", json!({"n":1})))
                .await
                .unwrap();

            let event = rx.recv().await.expect("event");
            match event {
                Event::ExternalEvent { kind, .. } => assert_eq!(kind, "tick"),
                _ => panic!("expected external event"),
            }
        });
    }

    #[test]
    fn test_broadcast_bus_publish_without_subscribers_is_ok() {
        tokio_test::block_on(async {
            let bus = BroadcastEventBus::new(4);
            bus.publish(Event::trace("thread-1", "info", json!({"ok":true})))
                .await
                .unwrap();
        });
    }
}
