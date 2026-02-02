//! # Orchestral Runtime
//!
//! Thread Runtime with concurrency, interruption, and scheduling.
//!
//! This crate provides:
//! - Thread (context world) management
//! - Interaction (one intervention) management
//! - Event (fact record) system
//! - ThreadRuntime for lifecycle and concurrency management
//! - ConcurrencyPolicy for deciding how to handle new inputs

mod bootstrap;
mod concurrency;
mod interaction;
mod orchestrator;
mod thread;
mod thread_runtime;

pub use bootstrap::{BootstrapError, RuntimeApp};
pub use concurrency::{
    ConcurrencyDecision, ConcurrencyPolicy, DefaultConcurrencyPolicy, ParallelConcurrencyPolicy,
    QueueConcurrencyPolicy, RejectWhenBusyConcurrencyPolicy, RunningState,
};
pub use interaction::{Interaction, InteractionId, InteractionState};
pub use orchestrator::{Orchestrator, OrchestratorError, OrchestratorResult};
pub use thread::{Thread, ThreadId};
pub use thread_runtime::{HandleEventResult, RuntimeError, ThreadRuntime, ThreadRuntimeConfig};

// Re-export core types for convenience
pub use orchestral_core::prelude::*;
