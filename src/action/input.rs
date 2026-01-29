//! ActionInput type definition

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Input data for action execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionInput {
    /// Parameters for the action (from Step.params)
    #[serde(default)]
    pub params: Value,
    /// Imported values from WorkingSet
    #[serde(default)]
    pub imports: HashMap<String, Value>,
}

impl ActionInput {
    /// Create a new empty input
    pub fn new() -> Self {
        Self::default()
    }

    /// Create input with params only
    pub fn with_params(params: Value) -> Self {
        Self {
            params,
            imports: HashMap::new(),
        }
    }

    /// Create input with imports only
    pub fn with_imports(imports: HashMap<String, Value>) -> Self {
        Self {
            params: Value::Null,
            imports,
        }
    }

    /// Get a parameter by JSON pointer (e.g., "/foo/bar")
    pub fn get_param(&self, pointer: &str) -> Option<&Value> {
        self.params.pointer(pointer)
    }

    /// Get an imported value
    pub fn get_import(&self, key: &str) -> Option<&Value> {
        self.imports.get(key)
    }

    /// Check if a required import exists
    pub fn has_import(&self, key: &str) -> bool {
        self.imports.contains_key(key)
    }
}
