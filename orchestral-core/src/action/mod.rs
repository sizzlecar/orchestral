//! Action abstraction module
//!
//! This module defines the Action trait and related types:
//! - Action: The core trait for atomic execution units
//! - ActionInput: Input data for action execution
//! - ActionContext: Execution context with access to stores
//! - ActionResult: Execution result with retry semantics

mod context;
mod input;
mod result;

use async_trait::async_trait;

pub use context::ActionContext;
pub use input::ActionInput;
pub use result::{ActionResult, ApprovalRequest};

// Re-export CancellationToken for convenience
pub use tokio_util::sync::CancellationToken;

/// Action trait - the core abstraction for atomic execution units
///
/// Actions are black boxes to the Executor. They can:
/// - Perform side effects
/// - Return typed outputs
/// - Request user clarification
/// - Fail with retry semantics
#[async_trait]
pub trait Action: Send + Sync {
    /// Get the action name (must be unique)
    fn name(&self) -> &str;

    /// Get the action description (for LLM planning)
    fn description(&self) -> &str;

    /// Get action metadata (typed input/output schema hints for planning/runtime checks)
    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
    }

    /// Execute the action
    async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult;
}

/// Action metadata for planner
#[derive(Debug, Clone)]
pub struct ActionMeta {
    /// Action name
    pub name: String,
    /// Action description
    pub description: String,
    /// JSON schema for fully resolved input payload.
    pub input_schema: serde_json::Value,
    /// JSON schema for action output payload.
    pub output_schema: serde_json::Value,
}

impl ActionMeta {
    /// Create new action metadata
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            input_schema: serde_json::Value::Null,
            output_schema: serde_json::Value::Null,
        }
    }

    /// Set input schema.
    pub fn with_input_schema(mut self, schema: serde_json::Value) -> Self {
        self.input_schema = schema;
        self
    }

    /// Set output schema.
    pub fn with_output_schema(mut self, schema: serde_json::Value) -> Self {
        self.output_schema = schema;
        self
    }
}

/// Extract metadata from an Action implementation
pub fn extract_meta<A: Action + ?Sized>(action: &A) -> ActionMeta {
    action.metadata()
}
