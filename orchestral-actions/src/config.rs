use serde::Deserialize;
use serde_json::Value;

/// Actions config root
#[derive(Debug, Deserialize)]
pub struct ActionsConfig {
    pub actions: Vec<ActionSpec>,
}

/// Single action definition from config
#[derive(Debug, Deserialize)]
pub struct ActionSpec {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub config: Value,
}

impl ActionSpec {
    pub fn description_or(&self, fallback: &str) -> String {
        self.description
            .clone()
            .unwrap_or_else(|| fallback.to_string())
    }
}
