//! EventStore in-memory implementation.

use async_trait::async_trait;
use std::sync::RwLock;

use orchestral_core::store::{Event, EventStore, StoreError};

const DEFAULT_IN_MEMORY_EVENT_LIMIT: usize = 20_000;

/// In-memory implementation for development and testing.
pub struct InMemoryEventStore {
    events: RwLock<Vec<Event>>,
    max_events: usize,
}

impl InMemoryEventStore {
    /// Create a new in-memory event store.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_in_memory_event_store_limit() {
        tokio_test::block_on(async {
            let store = InMemoryEventStore::with_max_events(2);
            store
                .append(Event::external("thread-1", "kind", json!({"n":1})))
                .await
                .unwrap();
            store
                .append(Event::external("thread-1", "kind", json!({"n":2})))
                .await
                .unwrap();
            store
                .append(Event::external("thread-1", "kind", json!({"n":3})))
                .await
                .unwrap();
            let rows = store.query_by_thread("thread-1").await.unwrap();
            assert_eq!(rows.len(), 2);
        });
    }
}
