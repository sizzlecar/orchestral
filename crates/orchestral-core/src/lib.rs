//! # Orchestral Core
//!
//! Core abstractions and deterministic logic for Orchestral runtime.
//!
//! This crate contains:
//! - Intent / Plan / Step / Task / State definitions
//! - Planner / Normalizer / Executor abstractions
//! - DAG execution and task-internal scheduling
//!
//! This crate does NOT care about:
//! - Who the user is
//! - Whether input is a message
//! - Whether multiple interactions run concurrently
//! - How output is displayed

pub mod action;
pub mod executor;
pub mod interpreter;
pub mod io;
pub mod normalizer;
pub mod planner;
pub mod store;
pub mod types;

/// Prelude for convenient imports
pub mod prelude {
    pub use crate::action::{
        Action, ActionContext, ActionInput, ActionMeta, ActionResult, CancellationToken,
    };
    pub use crate::executor::{
        ActionRegistry, DagNode, ExecutionDag, ExecutionProgressEvent, ExecutionProgressReporter,
        ExecutionResult, Executor, ExecutorContext, NodeState,
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
        HistoryItem, PlanError, Planner, PlannerContext, PlannerOutput, PlannerRuntimeInfo,
    };
    pub use crate::store::{
        EmbeddingStatus, Event, EventStore, Reference, ReferenceStore, ReferenceType, Scope,
        StoreError, TaskStore, WorkingSet,
    };
    pub use crate::types::{
        Intent, IntentContext, Plan, Step, StepId, StepKind, Task, TaskId, TaskState,
    };
}

// Re-export key types at crate root
pub use action::{Action, ActionContext, ActionInput, ActionResult};
pub use executor::{
    ExecutionDag, ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, Executor,
};
pub use interpreter::{
    InterpretDeltaSink, InterpretRequest, InterpretResult, NoopResultInterpreter, ResultInterpreter,
};
pub use io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobStream, BlobWriteRequest,
};
pub use normalizer::{NormalizedPlan, PlanNormalizer};
pub use planner::{Planner, PlannerOutput, PlannerRuntimeInfo};
pub use store::{
    EmbeddingStatus, Event, EventStore, ReferenceStore, StoreError, TaskStore, WorkingSet,
};
pub use types::{Intent, Plan, Step, StepId, Task, TaskId, TaskState};
