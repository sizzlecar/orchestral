//! # Orchestral Stores
//!
//! Storage implementations for Orchestral runtime.
//!
//! This crate provides:
//! - TaskStore implementations (InMemory, Redis, DB)
//! - ReferenceStore implementations
//! - EventStore implementations

mod event_store;
mod reference_store;
mod task_store;

pub use event_store::{Event, EventStore, InMemoryEventStore, RedisEventStore};
pub use reference_store::{InMemoryReferenceStore, RedisReferenceStore};
pub use task_store::{InMemoryTaskStore, RedisTaskStore};

// Re-export core traits for convenience
pub use orchestral_core::store::{Reference, ReferenceStore, ReferenceType, StoreError, TaskStore};
