//! # Orchestral Runtime
//!
//! Thread Runtime with concurrency, interruption, scheduling, LLM planners,
//! built-in actions, context building, and application-layer API.

pub mod action;
pub mod agent;
pub mod api;
mod bootstrap;
mod concurrency;
pub mod context;
mod interaction;
#[allow(dead_code)]
mod interpreter;
mod orchestrator;
pub mod planner;
pub mod sdk;
pub mod skill;
#[cfg(test)]
mod system_prompts;
mod thread;
mod thread_runtime;

pub use bootstrap::{
    BootstrapError, DefaultRuntimeComponentFactory, InMemoryBlobStore, RuntimeApp,
};
pub use concurrency::{
    ConcurrencyDecision, ConcurrencyPolicy, DefaultConcurrencyPolicy, MergeConcurrencyPolicy,
    ParallelConcurrencyPolicy, QueueConcurrencyPolicy, RejectWhenBusyConcurrencyPolicy,
    RunningState,
};
pub use interaction::{Interaction, InteractionId, InteractionState};
pub use orchestral_core::spi::{
    ComponentRegistry, HookDispatchMode, HookExecutionPolicy, HookFailurePolicy, HookRegistry,
    RuntimeBuildRequest, RuntimeComponentFactory, RuntimeHook, RuntimeHookContext,
    RuntimeHookEventEnvelope, SpiError, SpiMeta, StoreBundle,
};
pub use orchestrator::{Orchestrator, OrchestratorError, OrchestratorResult};
pub use thread::{Thread, ThreadId};
pub use thread_runtime::{HandleEventResult, RuntimeError, ThreadRuntime, ThreadRuntimeConfig};

// Re-export core types for convenience
pub use orchestral_core::prelude::*;
