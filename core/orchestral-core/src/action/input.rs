//! ActionInput type definition

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Input data for action execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionInput {
    /// Parameters for the action (from Step.params)
    #[serde(default)]
    pub params: Value,
}

impl ActionInput {
    /// Create a new empty input
    pub fn new() -> Self {
        Self::default()
    }

    /// Create input with params only
    pub fn with_params(params: Value) -> Self {
        Self { params }
    }

    /// Get a parameter by JSON pointer (e.g., "/foo/bar")
    pub fn get_param(&self, pointer: &str) -> Option<&Value> {
        self.params.pointer(pointer)
    }
}
