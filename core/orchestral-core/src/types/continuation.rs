use serde::{Deserialize, Serialize};

/// Explicit continuation decision emitted by a completed stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationStatus {
    CommitReady,
    WaitUser,
    Done,
    Failed,
}

/// Structured stage exit state consumed by runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuationState {
    pub status: ContinuationStatus,
    pub reason: String,
    #[serde(default)]
    pub unknowns: Vec<String>,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub user_message: Option<String>,
}
