//! # Orchestral Core
//!
//! Core abstractions, SPI contracts, configuration, and default in-memory
//! stores for the Orchestral runtime.
//!
//! This crate provides:
//! - Intent / Plan / Step / Task / State definitions
//! - Planner / Normalizer / Executor abstractions
//! - DAG execution and task-internal scheduling
//! - Stable SPI contracts for runtime component and hook extensions
//! - Unified configuration management
//! - In-memory store implementations (EventStore, TaskStore, EventBus)
//!
//! This crate does NOT care about:
//! - Who the user is
//! - Whether input is a message
//! - Whether multiple interactions run concurrently
//! - How output is displayed

pub mod action;
pub mod config;
pub mod executor;
pub mod interpreter;
pub mod io;
pub mod normalizer;
pub mod planner;
pub mod spi;
pub mod store;
pub mod types;

/// Prelude for convenient imports
pub mod prelude {
    pub use crate::action::{
        Action, ActionContext, ActionInput, ActionMeta, ActionResult, CancellationToken,
    };
    pub use crate::executor::{
        ActionExecutionOptions, ActionPreflightHook, ActionRegistry, DagNode, ExecutionDag,
        ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, Executor,
        ExecutorContext, NodeState,
    };
    pub use crate::interpreter::{
        InterpretDeltaSink, InterpretError, InterpretRequest, InterpretResult,
        NoopResultInterpreter, ResultInterpreter,
    };
    pub use crate::io::{
        BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobStream, BlobWriteRequest,
    };
    pub use crate::normalizer::{
        FixError, NormalizeError, NormalizedPlan, PlanFixer, PlanNormalizer, PlanValidator,
        ValidationError,
    };
    pub use crate::planner::{
        ActionCall, HistoryItem, PlanError, Planner, PlannerContext, PlannerLoopContext,
        PlannerOutput, PlannerRuntimeInfo, SingleAction,
    };
    pub use crate::store::{Event, EventStore, Scope, StoreError, TaskStore, WorkingSet};
    pub use crate::types::{
        ContinuationState, ContinuationStatus, Intent, IntentContext, Plan, Step, StepId, StepKind,
        Task, TaskId, TaskState, VerifyDecision, VerifyStatus,
    };
}

// Re-export key types at crate root
pub use action::{Action, ActionContext, ActionInput, ActionResult};
pub use executor::{
    ActionExecutionOptions, ActionPreflightHook, ExecutionDag, ExecutionProgressEvent,
    ExecutionProgressReporter, ExecutionResult, Executor,
};
pub use interpreter::{
    InterpretDeltaSink, InterpretRequest, InterpretResult, NoopResultInterpreter, ResultInterpreter,
};
pub use io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobStream, BlobWriteRequest,
};
pub use normalizer::{NormalizedPlan, PlanNormalizer};
pub use planner::{
    ActionCall, Planner, PlannerContext, PlannerLoopContext, PlannerOutput, PlannerRuntimeInfo,
    SingleAction,
};
pub use store::{Event, EventStore, StoreError, TaskStore, WorkingSet};
pub use types::{
    ContinuationState, ContinuationStatus, Intent, Plan, Step, StepId, Task, TaskId, TaskState,
    VerifyDecision, VerifyStatus,
};
