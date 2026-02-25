//! Step type definitions
//!
//! Step represents an atomic execution unit in a Plan.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Strongly-typed Step ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct StepId(pub String);

impl StepId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for StepId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for StepId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&StepId> for StepId {
    fn from(value: &StepId) -> Self {
        value.clone()
    }
}

impl From<StepId> for String {
    fn from(value: StepId) -> Self {
        value.0
    }
}

impl fmt::Display for StepId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for StepId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<&str> for StepId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

/// Step type - distinguishes control semantics
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// Normal Action execution
    #[default]
    Action,
    /// Wait for user input
    WaitUser,
    /// Wait for external event
    WaitEvent,
    /// System built-in step (e.g., resolve_reference)
    System,
}

/// Data binding from an upstream task key to this step's input key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepIoBinding {
    /// Source task key, e.g. "s1.result" or "result"
    pub from: String,
    /// Target key for this step input
    pub to: String,
    /// Whether missing source value should fail this step
    #[serde(default = "default_true")]
    pub required: bool,
}

impl StepIoBinding {
    /// Create a required binding from source key to target key
    pub fn required(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            required: true,
        }
    }

    /// Create an optional binding from source key to target key
    pub fn optional(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            required: false,
        }
    }
}

fn default_true() -> bool {
    true
}

/// A single step in the execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    /// Unique identifier for this step (logical ID)
    pub id: StepId,
    /// Name of the action to execute
    pub action: String,
    /// Step type for control flow semantics
    #[serde(default)]
    pub kind: StepKind,
    /// IDs of steps this step depends on
    #[serde(default)]
    pub depends_on: Vec<StepId>,
    /// Keys to export to WorkingSet (explicit data contract)
    #[serde(default)]
    pub exports: Vec<String>,
    /// Explicit upstream -> current step input bindings
    #[serde(default)]
    pub io_bindings: Vec<StepIoBinding>,
    /// Parameters for the action
    #[serde(default)]
    pub params: Value,
}

impl Step {
    /// Create a new action step
    pub fn action(id: impl Into<StepId>, action: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            action: action.into(),
            kind: StepKind::Action,
            depends_on: Vec::new(),
            exports: Vec::new(),
            io_bindings: Vec::new(),
            params: Value::Null,
        }
    }

    /// Create a new system step
    pub fn system(id: impl Into<StepId>, action: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            action: action.into(),
            kind: StepKind::System,
            depends_on: Vec::new(),
            exports: Vec::new(),
            io_bindings: Vec::new(),
            params: Value::Null,
        }
    }

    /// Create a wait-user step
    pub fn wait_user(id: impl Into<StepId>) -> Self {
        Self {
            id: id.into(),
            action: "wait_user".to_string(),
            kind: StepKind::WaitUser,
            depends_on: Vec::new(),
            exports: Vec::new(),
            io_bindings: Vec::new(),
            params: Value::Null,
        }
    }

    /// Add dependencies
    pub fn with_depends_on(mut self, deps: Vec<StepId>) -> Self {
        self.depends_on = deps;
        self
    }

    /// Add exports
    pub fn with_exports(mut self, exports: Vec<String>) -> Self {
        self.exports = exports;
        self
    }

    /// Add explicit IO bindings
    pub fn with_io_bindings(mut self, io_bindings: Vec<StepIoBinding>) -> Self {
        self.io_bindings = io_bindings;
        self
    }

    /// Add parameters
    pub fn with_params(mut self, params: Value) -> Self {
        self.params = params;
        self
    }
}
