use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Final verification verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyStatus {
    Passed,
    Failed,
}

/// Structured verify result. This is the only terminal done gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyDecision {
    pub status: VerifyStatus,
    pub reason: String,
    #[serde(default)]
    pub evidence: Value,
}
