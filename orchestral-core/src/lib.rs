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
        ActionRegistry, DagNode, ExecutionDag, ExecutionResult, Executor, ExecutorContext,
        NodeState,
    };
    pub use crate::normalizer::{
        FixError, NormalizeError, NormalizedPlan, PlanFixer, PlanNormalizer, PlanValidator,
        ValidationError,
    };
    pub use crate::planner::{HistoryItem, PlanError, Planner, PlannerContext};
    pub use crate::store::{
        Reference, ReferenceStore, ReferenceType, Scope, StoreError, TaskStore, WorkingSet,
    };
    pub use crate::types::{Intent, IntentContext, Plan, Step, StepKind, Task, TaskId, TaskState};
}

// Re-export key types at crate root
pub use action::{Action, ActionContext, ActionInput, ActionResult};
pub use executor::{ExecutionDag, ExecutionResult, Executor};
pub use normalizer::{NormalizedPlan, PlanNormalizer};
pub use planner::Planner;
pub use store::{ReferenceStore, StoreError, TaskStore, WorkingSet};
pub use types::{Intent, Plan, Step, Task, TaskId, TaskState};
