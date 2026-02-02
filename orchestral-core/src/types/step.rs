//! Step type definitions
//!
//! Step represents an atomic execution unit in a Plan.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Step type - distinguishes control semantics
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    /// Normal Action execution
    Action,
    /// Wait for user input
    WaitUser,
    /// Wait for external event
    WaitEvent,
    /// System built-in step (e.g., resolve_reference)
    System,
}

impl Default for StepKind {
    fn default() -> Self {
        Self::Action
    }
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
    pub id: String,
    /// Name of the action to execute
    pub action: String,
    /// Step type for control flow semantics
    #[serde(default)]
    pub kind: StepKind,
    /// IDs of steps this step depends on
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Keys to import from WorkingSet (explicit data contract)
    #[serde(default)]
    pub imports: Vec<String>,
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
    pub fn action(id: impl Into<String>, action: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            action: action.into(),
            kind: StepKind::Action,
            depends_on: Vec::new(),
            imports: Vec::new(),
            exports: Vec::new(),
            io_bindings: Vec::new(),
            params: Value::Null,
        }
    }

    /// Create a new system step
    pub fn system(id: impl Into<String>, action: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            action: action.into(),
            kind: StepKind::System,
            depends_on: Vec::new(),
            imports: Vec::new(),
            exports: Vec::new(),
            io_bindings: Vec::new(),
            params: Value::Null,
        }
    }

    /// Create a wait-user step
    pub fn wait_user(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            action: "wait_user".to_string(),
            kind: StepKind::WaitUser,
            depends_on: Vec::new(),
            imports: Vec::new(),
            exports: Vec::new(),
            io_bindings: Vec::new(),
            params: Value::Null,
        }
    }

    /// Add dependencies
    pub fn with_depends_on(mut self, deps: Vec<String>) -> Self {
        self.depends_on = deps;
        self
    }

    /// Add imports
    pub fn with_imports(mut self, imports: Vec<String>) -> Self {
        self.imports = imports;
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
