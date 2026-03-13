mod inspect_and_extract;
mod locate_and_patch;
mod run_and_verify;

use orchestral_core::planner::PlanError;
use orchestral_core::types::{Plan, SkeletonKind, StageChoice, StageKind, Task};

use super::OrchestratorError;

pub(super) fn lower_reactor_stage_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<Plan, OrchestratorError> {
    match choice.skeleton {
        SkeletonKind::LocateAndPatch => locate_and_patch::lower_locate_and_patch_plan(task, choice),
        SkeletonKind::InspectAndExtract => {
            inspect_and_extract::lower_inspect_and_extract_plan(task, choice)
        }
        SkeletonKind::RunAndVerify => run_and_verify::lower_run_and_verify_plan(task, choice),
        _ => Err(OrchestratorError::Planner(PlanError::Generation(format!(
            "reactor lowering not implemented for skeleton={:?} artifact_family={:?} current_stage={:?}",
            choice.skeleton, choice.artifact_family, choice.current_stage
        )))),
    }
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
