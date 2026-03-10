use serde::{Deserialize, Serialize};

use super::{ArtifactFamily, DerivationPolicy, Plan, SkeletonKind, StageKind};

/// Planner output for the reactor path. Planner selects only the current stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageChoice {
    pub skeleton: SkeletonKind,
    pub artifact_family: ArtifactFamily,
    pub current_stage: StageKind,
    pub stage_goal: String,
    pub derivation_policy: DerivationPolicy,
    #[serde(default)]
    pub next_stage_hint: Option<StageKind>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Lowered stage-local plan paired with its stage choice metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagePlan {
    pub choice: StageChoice,
    pub plan: Plan,
}
