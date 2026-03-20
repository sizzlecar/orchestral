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
    /// Optional action grouping/category for planner UX and reporting.
    pub category: Option<String>,
    /// JSON schema for fully resolved input payload.
    pub input_schema: serde_json::Value,
    /// JSON schema for action output payload.
    pub output_schema: serde_json::Value,
    /// Semantic capability tags used by planner/runtime guardrails.
    pub capabilities: Vec<String>,
    /// Semantic workflow roles used by recipe lowering.
    pub roles: Vec<String>,
    /// Abstract artifact/input kinds this action can consume.
    pub input_kinds: Vec<String>,
    /// Abstract artifact/output kinds this action can produce.
    pub output_kinds: Vec<String>,
}

impl ActionMeta {
    /// Create new action metadata
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            category: None,
            input_schema: serde_json::Value::Null,
            output_schema: serde_json::Value::Null,
            capabilities: Vec::new(),
            roles: Vec::new(),
            input_kinds: Vec::new(),
            output_kinds: Vec::new(),
        }
    }

    /// Set action category/grouping metadata.
    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        let category = category.into();
        if category.trim().is_empty() {
            self.category = None;
        } else {
            self.category = Some(category);
        }
        self
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

    /// Add one capability tag.
    pub fn with_capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self.capabilities.sort();
        self.capabilities.dedup();
        self
    }

    /// Add multiple capability tags.
    pub fn with_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.capabilities
            .extend(capabilities.into_iter().map(Into::into));
        self.capabilities.sort();
        self.capabilities.dedup();
        self
    }

    /// Add one workflow role.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self.roles.sort();
        self.roles.dedup();
        self
    }

    /// Add multiple workflow roles.
    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles.extend(roles.into_iter().map(Into::into));
        self.roles.sort();
        self.roles.dedup();
        self
    }

    /// Add multiple supported input kinds.
    pub fn with_input_kinds<I, S>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.input_kinds.extend(kinds.into_iter().map(Into::into));
        self.input_kinds.sort();
        self.input_kinds.dedup();
        self
    }

    /// Add multiple supported output kinds.
    pub fn with_output_kinds<I, S>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.output_kinds.extend(kinds.into_iter().map(Into::into));
        self.output_kinds.sort();
        self.output_kinds.dedup();
        self
    }

    /// Check whether this action advertises a capability.
    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|item| item == capability)
    }

    /// Check whether this action advertises a workflow role.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|item| item == role)
    }

    /// Check whether this action can consume the given abstract input kind.
    pub fn supports_input_kind(&self, kind: &str) -> bool {
        self.input_kinds.iter().any(|item| item == kind)
    }

    /// Check whether this action can produce the given abstract output kind.
    pub fn supports_output_kind(&self, kind: &str) -> bool {
        self.output_kinds.iter().any(|item| item == kind)
    }
}

/// Extract metadata from an Action implementation
pub fn extract_meta<A: Action + ?Sized>(action: &A) -> ActionMeta {
    action.metadata()
}
