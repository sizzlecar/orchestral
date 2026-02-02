//! Action configuration types.
//!
//! These types define how actions are configured in YAML.

use serde::Deserialize;
use serde_json::Value;

/// Root configuration for actions.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ActionsConfig {
    /// Whether action definitions should be hot-reloaded.
    #[serde(default)]
    pub hot_reload: bool,
    /// Optional legacy source file (kept for compatibility).
    #[serde(default)]
    pub source: Option<String>,
    /// List of action specifications.
    #[serde(default)]
    pub actions: Vec<ActionSpec>,
}

impl ActionsConfig {
    /// Get action spec by name.
    pub fn get(&self, name: &str) -> Option<&ActionSpec> {
        self.actions.iter().find(|a| a.name == name)
    }

    /// List all action names.
    pub fn names(&self) -> Vec<&str> {
        self.actions.iter().map(|a| a.name.as_str()).collect()
    }
}

/// Single action definition from config.
#[derive(Debug, Clone, Deserialize)]
pub struct ActionSpec {
    /// Unique name for this action.
    pub name: String,
    /// Action type/kind (e.g., "echo", "http", "shell").
    pub kind: String,
    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,
    /// Action-specific configuration.
    #[serde(default)]
    pub config: Value,
    /// Optional declared action interface (typed input/output contract).
    #[serde(default)]
    pub interface: Option<ActionInterfaceSpec>,
}

impl ActionSpec {
    /// Get description or fallback.
    pub fn description_or(&self, fallback: &str) -> String {
        self.description
            .clone()
            .unwrap_or_else(|| fallback.to_string())
    }

    /// Get a config value as a specific type.
    pub fn get_config<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.config
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// Action interface declaration used for planning and IO contracts.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ActionInterfaceSpec {
    /// JSON Schema describing resolved action input.
    /// Backward-compatible alias: params_schema.
    #[serde(default, alias = "params_schema")]
    pub input_schema: Value,
    /// JSON Schema describing action output (exports map).
    #[serde(default)]
    pub output_schema: Value,
}
