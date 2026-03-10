use serde::{Deserialize, Serialize};

/// Coarse-grained reactor stage kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StageKind {
    Locate,
    #[default]
    Probe,
    Derive,
    Assess,
    Commit,
    Verify,
    Export,
    Prepare,
    Run,
    Collect,
    Compare,
    WaitUser,
    Done,
    Failed,
}

/// Structured policy controlling bounded derivation behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DerivationPolicy {
    #[default]
    Strict,
    Permissive,
}

/// Stable artifact family selector for typed adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactFamily {
    Spreadsheet,
    Document,
    Structured,
    Codebase,
}
