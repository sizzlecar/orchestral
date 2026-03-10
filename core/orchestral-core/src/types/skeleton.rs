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

impl SkeletonKind {
    pub fn allowed_stages(self) -> &'static [StageKind] {
        match self {
            Self::LocateAndPatch => &[
                StageKind::Locate,
                StageKind::Probe,
                StageKind::Derive,
                StageKind::Assess,
                StageKind::Commit,
                StageKind::Verify,
                StageKind::WaitUser,
                StageKind::Done,
                StageKind::Failed,
            ],
            Self::InspectAndExtract => &[
                StageKind::Probe,
                StageKind::Derive,
                StageKind::Export,
                StageKind::Verify,
                StageKind::WaitUser,
                StageKind::Done,
                StageKind::Failed,
            ],
            Self::InspectAndTransform => &[
                StageKind::Probe,
                StageKind::Transform,
                StageKind::Verify,
                StageKind::Export,
                StageKind::WaitUser,
                StageKind::Done,
                StageKind::Failed,
            ],
            Self::CompareAndSync => &[
                StageKind::Locate,
                StageKind::ProbeLeft,
                StageKind::ProbeRight,
                StageKind::Compare,
                StageKind::Derive,
                StageKind::Assess,
                StageKind::Commit,
                StageKind::Verify,
                StageKind::WaitUser,
                StageKind::Done,
                StageKind::Failed,
            ],
            Self::RunAndVerify => &[
                StageKind::Prepare,
                StageKind::Run,
                StageKind::Collect,
                StageKind::Verify,
                StageKind::Export,
                StageKind::WaitUser,
                StageKind::Done,
                StageKind::Failed,
            ],
        }
    }

    pub fn allows_stage(self, stage: StageKind) -> bool {
        self.allowed_stages().contains(&stage)
    }

    pub fn default_initial_stage(self) -> StageKind {
        match self {
            Self::LocateAndPatch => StageKind::Locate,
            Self::InspectAndExtract => StageKind::Probe,
            Self::InspectAndTransform => StageKind::Probe,
            Self::CompareAndSync => StageKind::Locate,
            Self::RunAndVerify => StageKind::Prepare,
        }
    }

    pub fn next_stage_on_success(self, current: StageKind) -> Option<StageKind> {
        match self {
            Self::LocateAndPatch => match current {
                StageKind::Locate => Some(StageKind::Probe),
                StageKind::Probe => Some(StageKind::Derive),
                StageKind::Derive => Some(StageKind::Assess),
                StageKind::Assess => Some(StageKind::Commit),
                StageKind::Commit => Some(StageKind::Verify),
                StageKind::Verify => Some(StageKind::Done),
                _ => None,
            },
            Self::InspectAndExtract => match current {
                StageKind::Probe => Some(StageKind::Derive),
                StageKind::Derive => Some(StageKind::Export),
                StageKind::Export => Some(StageKind::Verify),
                StageKind::Verify => Some(StageKind::Done),
                _ => None,
            },
            Self::InspectAndTransform => match current {
                StageKind::Probe => Some(StageKind::Transform),
                StageKind::Transform => Some(StageKind::Verify),
                StageKind::Verify => Some(StageKind::Export),
                StageKind::Export => Some(StageKind::Done),
                _ => None,
            },
            Self::CompareAndSync => match current {
                StageKind::Locate => Some(StageKind::ProbeLeft),
                StageKind::ProbeLeft => Some(StageKind::ProbeRight),
                StageKind::ProbeRight => Some(StageKind::Compare),
                StageKind::Compare => Some(StageKind::Derive),
                StageKind::Derive => Some(StageKind::Assess),
                StageKind::Assess => Some(StageKind::Commit),
                StageKind::Commit => Some(StageKind::Verify),
                StageKind::Verify => Some(StageKind::Done),
                _ => None,
            },
            Self::RunAndVerify => match current {
                StageKind::Prepare => Some(StageKind::Run),
                StageKind::Run => Some(StageKind::Collect),
                StageKind::Collect => Some(StageKind::Verify),
                StageKind::Verify => Some(StageKind::Export),
                StageKind::Export => Some(StageKind::Done),
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skeleton_allowed_stages_cover_rfc_shapes() {
        assert!(SkeletonKind::LocateAndPatch.allows_stage(StageKind::Probe));
        assert!(SkeletonKind::InspectAndExtract.allows_stage(StageKind::Export));
        assert!(SkeletonKind::InspectAndTransform.allows_stage(StageKind::Transform));
        assert!(SkeletonKind::CompareAndSync.allows_stage(StageKind::ProbeLeft));
        assert!(SkeletonKind::CompareAndSync.allows_stage(StageKind::ProbeRight));
        assert!(SkeletonKind::RunAndVerify.allows_stage(StageKind::Prepare));
        assert!(!SkeletonKind::RunAndVerify.allows_stage(StageKind::Commit));
    }

    #[test]
    fn test_skeleton_default_initial_stages_are_stable() {
        assert_eq!(
            SkeletonKind::LocateAndPatch.default_initial_stage(),
            StageKind::Locate
        );
        assert_eq!(
            SkeletonKind::CompareAndSync.default_initial_stage(),
            StageKind::Locate
        );
        assert_eq!(
            SkeletonKind::RunAndVerify.default_initial_stage(),
            StageKind::Prepare
        );
    }
}
