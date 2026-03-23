//! Store module
//!
//! This module provides storage abstractions and default in-memory implementations:
//! - EventStore: append-only fact journal
//! - WorkingSet: Scoped KV data container for inter-step communication
//! - TaskStore: Task persistence (async trait)
//! - EventBus: realtime event fan-out abstraction
//!
//! External backends (Redis/Postgres/SQLite) live in separate adapter crates.

pub mod event_bus;
mod event_store;
mod memory_event;
mod memory_task;
mod task_store;
mod working_set;

pub use event_bus::{BroadcastEventBus, EventBus};
pub use event_store::{Event, EventStore, InteractionId, ThreadId};
pub use memory_event::InMemoryEventStore;
pub use memory_task::InMemoryTaskStore;
pub use task_store::TaskStore;
pub use working_set::{Scope, WorkingSet};

use thiserror::Error;

/// Store error types
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Item not found: {0}")]
    NotFound(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
