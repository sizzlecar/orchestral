use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Shared derive/assess envelope for bounded patch candidate outputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PatchCandidatesEnvelope {
    #[serde(default)]
    pub candidates: Value,
    #[serde(default)]
    pub unknowns: Vec<String>,
    #[serde(default)]
    pub assumptions: Vec<String>,
}
