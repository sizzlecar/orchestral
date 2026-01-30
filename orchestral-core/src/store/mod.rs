//! Store module
//!
//! This module provides storage abstractions:
//! - WorkingSet: Scoped KV data container for inter-step communication
//! - ReferenceStore: Historical artifact storage (async trait)
//! - TaskStore: Task persistence (async trait)
//!
//! Note: Implementations are in orchestral-stores crate

mod reference_store;
mod task_store;
mod working_set;

pub use reference_store::{Reference, ReferenceStore, ReferenceType};
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
