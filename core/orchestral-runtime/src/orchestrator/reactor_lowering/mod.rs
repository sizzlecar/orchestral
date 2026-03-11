mod locate_and_patch;

use orchestral_core::planner::PlanError;
use orchestral_core::types::{Plan, SkeletonKind, StageChoice, StageKind, Task};

use super::OrchestratorError;

pub(super) fn lower_reactor_stage_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<Plan, OrchestratorError> {
    if choice.skeleton == SkeletonKind::LocateAndPatch {
        return locate_and_patch::lower_locate_and_patch_plan(task, choice);
    }

    Err(OrchestratorError::Planner(PlanError::Generation(format!(
        "reactor lowering not implemented for skeleton={:?} artifact_family={:?} current_stage={:?}",
        choice.skeleton, choice.artifact_family, choice.current_stage
    ))))
}

pub(super) fn unsupported_stage_error(
    choice: &StageChoice,
    stage: StageKind,
) -> Result<Plan, OrchestratorError> {
    Err(OrchestratorError::Planner(PlanError::Generation(format!(
        "locate_and_patch does not support stage {:?} for artifact_family={:?}",
        stage, choice.artifact_family
    ))))
}
