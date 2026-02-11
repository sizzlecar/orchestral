//! # Orchestral Stores
//!
//! Storage implementations for Orchestral runtime.
//!
//! This crate provides:
//! - TaskStore implementations (InMemory, Redis, DB)
//! - ReferenceStore implementations
//! - EventStore implementations

mod event_bus;
mod event_store;
mod reference_store;
mod task_store;

pub use event_bus::{BroadcastEventBus, EventBus};
pub use event_store::{InMemoryEventStore, PostgresEventStore, RedisEventStore};
pub use reference_store::{InMemoryReferenceStore, PostgresReferenceStore, RedisReferenceStore};
pub use task_store::{InMemoryTaskStore, PostgresTaskStore, RedisTaskStore};

// Re-export core traits for convenience
pub use orchestral_core::store::{
    EmbeddingStatus, Event, EventStore, Reference, ReferenceMatch, ReferenceStore, ReferenceType,
    StoreError, TaskStore,
};
