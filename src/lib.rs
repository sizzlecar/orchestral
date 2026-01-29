//! # Orchestral
//!
//! A Rust-native, intent-first agent runtime that transforms user intent into
//! executable plans and orchestrates them as dynamic DAGs.
//!
//! ## Core Concepts
//!
//! - **Intent**: User's high-level goal description
//! - **Plan**: LLM-generated execution plan as a DAG
//! - **Action**: Atomic execution unit (black box to executor)
//! - **Executor**: DAG scheduler and runner
//!
//! ## Architecture
//!
//! ```text
//! User Input
//!    ↓
//! Intent Understanding
//!    ↓
//! Planner (LLM)
//!    ↓
//! Plan (DAG Description)
//!    ↓
//! Plan Normalizer & Validator
//!    ↓
//! Executor
//!    ↓
//! Actions
//!    ↓
//! Result
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use orchestral::prelude::*;
//!
//! // Create an intent
//! let intent = Intent::new("Edit the previous image");
//!
//! // Plan with LLM (implement your own Planner)
//! let plan = planner.plan(&intent, &context).await?;
//!
//! // Normalize and validate
//! let normalized = normalizer.normalize(plan)?;
//!
//! // Execute
//! let result = executor.execute(&mut normalized.dag, &ctx).await;
//! ```

pub mod action;
pub mod executor;
pub mod normalizer;
pub mod planner;
pub mod store;
pub mod types;

/// Prelude for convenient imports
pub mod prelude {
    pub use crate::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
    pub use crate::executor::{ActionRegistry, DagNode, ExecutionDag, ExecutionResult, Executor, ExecutorContext, NodeState};
    pub use crate::normalizer::{NormalizeError, NormalizedPlan, PlanFixer, PlanNormalizer, PlanValidator, ValidationError};
    pub use crate::planner::{HistoryItem, PlanError, Planner, PlannerContext};
    pub use crate::store::{
        InMemoryReferenceStore, InMemoryTaskStore, Reference, ReferenceStore, ReferenceType,
        Scope, StoreError, TaskStore, WorkingSet,
    };
    pub use crate::types::{Intent, IntentContext, Plan, Step, StepKind, Task, TaskState};
}

// Re-export key types at crate root
pub use action::{Action, ActionResult};
pub use executor::{ExecutionDag, ExecutionResult, Executor};
pub use normalizer::{NormalizedPlan, PlanNormalizer};
pub use planner::Planner;
pub use store::{ReferenceStore, TaskStore, WorkingSet};
pub use types::{Intent, Plan, Step, Task, TaskState};
