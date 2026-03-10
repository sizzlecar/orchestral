use serde::{Deserialize, Serialize};

use super::{ArtifactFamily, StageKind};

/// Stable task-shape selector defined by RFC0310.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkeletonKind {
    LocateAndPatch,
    InspectAndExtract,
    InspectAndTransform,
    CompareAndSync,
    RunAndVerify,
}

/// Planner output used at task entry before stage-local execution begins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkeletonChoice {
    pub skeleton: SkeletonKind,
    #[serde(default)]
    pub artifact_family: Option<ArtifactFamily>,
    pub initial_stage: StageKind,
    pub confidence: f32,
    #[serde(default)]
    pub reason: Option<String>,
}
