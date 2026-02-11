//! # Orchestral Stores
//!
//! Minimal store implementations for Orchestral runtime.
//!
//! This crate provides:
//! - InMemory TaskStore
//! - InMemory ReferenceStore
//! - InMemory EventStore
//! - In-process EventBus

mod event_bus;
mod event_store;
mod reference_store;
mod task_store;

pub use event_bus::{BroadcastEventBus, EventBus};
pub use event_store::InMemoryEventStore;
pub use reference_store::InMemoryReferenceStore;
pub use task_store::InMemoryTaskStore;

// Re-export core traits for convenience
pub use orchestral_core::store::{
    EmbeddingStatus, Event, EventStore, Reference, ReferenceMatch, ReferenceStore, ReferenceType,
    StoreError, TaskStore,
};
