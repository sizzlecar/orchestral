use orchestral_core::planner::PlanError;
use orchestral_core::types::{
    ArtifactFamily, DerivationPolicy, SkeletonChoice, SkeletonKind, StageChoice, StageKind,
};

use super::OrchestratorError;

fn reactor_stage_goal(
    skeleton: SkeletonKind,
    artifact_family: ArtifactFamily,
    stage: StageKind,
) -> String {
    match skeleton {
        SkeletonKind::LocateAndPatch => match (artifact_family, stage) {
            (_, StageKind::Locate) => "locate the target artifact".to_string(),
            (ArtifactFamily::Spreadsheet, StageKind::Probe) => {
                "inspect workbook structure".to_string()
            }
            (ArtifactFamily::Document, StageKind::Probe) => {
                "inspect document structure".to_string()
            }
            (ArtifactFamily::Structured, StageKind::Probe) => {
                "inspect structured artifact contents".to_string()
            }
            (_, StageKind::Probe) => "inspect the target artifact".to_string(),
            (_, StageKind::Derive) => "derive a structured patch candidate set".to_string(),
            (_, StageKind::Assess) => "assess whether the task is ready to commit".to_string(),
            (ArtifactFamily::Spreadsheet, StageKind::Commit) => {
                "derive a typed spreadsheet patch and apply it".to_string()
            }
            (ArtifactFamily::Document, StageKind::Commit) => {
                "derive a typed document patch and apply it".to_string()
            }
            (ArtifactFamily::Structured, StageKind::Commit) => {
                "derive a typed structured patch and apply it".to_string()
            }
            (_, StageKind::Commit) => "derive a typed patch and apply it".to_string(),
            (ArtifactFamily::Spreadsheet, StageKind::Verify) => {
                "verify the applied spreadsheet patch".to_string()
            }
            (ArtifactFamily::Document, StageKind::Verify) => {
                "verify the applied document patch".to_string()
            }
            (ArtifactFamily::Structured, StageKind::Verify) => {
                "verify the applied structured patch".to_string()
            }
            (_, StageKind::Verify) => "verify the applied patch".to_string(),
            (_, StageKind::WaitUser) => "wait for additional user input".to_string(),
            (_, StageKind::Done) => "finish the patch task".to_string(),
            (_, StageKind::Failed) => "mark the patch task as failed".to_string(),
            (_, unsupported) => {
                format!("stage {:?} is not valid for locate_and_patch", unsupported)
            }
        },
        SkeletonKind::InspectAndExtract => match stage {
            StageKind::Probe => "inspect the artifact and collect structured findings".to_string(),
            StageKind::Derive => "derive the structured extraction result".to_string(),
            StageKind::Export => "emit the extracted result".to_string(),
            StageKind::Verify => "verify the extracted result".to_string(),
            StageKind::WaitUser => "wait for additional user input".to_string(),
            StageKind::Done => "finish the extraction task".to_string(),
            StageKind::Failed => "mark the extraction task as failed".to_string(),
            unsupported => format!(
                "stage {:?} is not valid for inspect_and_extract",
                unsupported
            ),
        },
        SkeletonKind::InspectAndTransform => match stage {
            StageKind::Probe => {
                "inspect the source artifact and determine transform inputs".to_string()
            }
            StageKind::Transform => "apply the bounded transformation".to_string(),
            StageKind::Verify => "verify the transformed result".to_string(),
            StageKind::Export => "emit the transformed artifact".to_string(),
            StageKind::WaitUser => "wait for additional user input".to_string(),
            StageKind::Done => "finish the transform task".to_string(),
            StageKind::Failed => "mark the transform task as failed".to_string(),
            unsupported => format!(
                "stage {:?} is not valid for inspect_and_transform",
                unsupported
            ),
        },
        SkeletonKind::CompareAndSync => match stage {
            StageKind::Locate => "locate the left and right comparison targets".to_string(),
            StageKind::ProbeLeft => "inspect the left-side artifact".to_string(),
            StageKind::ProbeRight => "inspect the right-side artifact".to_string(),
            StageKind::Compare => "compare both artifacts and identify differences".to_string(),
            StageKind::Derive => "derive a structured sync patch".to_string(),
            StageKind::Assess => "assess whether the sync patch is safe to commit".to_string(),
            StageKind::Commit => "apply the sync patch".to_string(),
            StageKind::Verify => "verify the synchronized result".to_string(),
            StageKind::WaitUser => "wait for additional user input".to_string(),
            StageKind::Done => "finish the compare-and-sync task".to_string(),
            StageKind::Failed => "mark the compare-and-sync task as failed".to_string(),
            unsupported => format!("stage {:?} is not valid for compare_and_sync", unsupported),
        },
        SkeletonKind::RunAndVerify => match stage {
            StageKind::Prepare => "prepare inputs for controlled execution".to_string(),
            StageKind::Run => "run the controlled program".to_string(),
            StageKind::Collect => "collect outputs from the controlled run".to_string(),
            StageKind::Verify => "verify the program result".to_string(),
            StageKind::Export => "emit the verified program outputs".to_string(),
            StageKind::WaitUser => "wait for additional user input".to_string(),
            StageKind::Done => "finish the run-and-verify task".to_string(),
            StageKind::Failed => "mark the run-and-verify task as failed".to_string(),
            unsupported => format!("stage {:?} is not valid for run_and_verify", unsupported),
        },
    }
}

pub(super) fn validate_reactor_stage_choice(choice: &StageChoice) -> Result<(), OrchestratorError> {
    if !choice.skeleton.allows_stage(choice.current_stage) {
        return Err(OrchestratorError::Planner(PlanError::Generation(format!(
            "reactor stage_choice invalid: skeleton={:?} does not allow current_stage={:?}",
            choice.skeleton, choice.current_stage
        ))));
    }
    if let Some(next_stage_hint) = choice.next_stage_hint {
        if !choice.skeleton.allows_stage(next_stage_hint) {
            return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "reactor stage_choice invalid: skeleton={:?} does not allow next_stage_hint={:?}",
                choice.skeleton, next_stage_hint
            ))));
        }
    }
    Ok(())
}

pub(super) fn build_reactor_stage_choice(
    skeleton: SkeletonKind,
    artifact_family: ArtifactFamily,
    current_stage: StageKind,
    derivation_policy: DerivationPolicy,
    next_stage_hint: Option<StageKind>,
    reason: Option<String>,
) -> Result<StageChoice, OrchestratorError> {
    let choice = StageChoice {
        skeleton,
        artifact_family,
        current_stage,
        stage_goal: reactor_stage_goal(skeleton, artifact_family, current_stage),
        derivation_policy,
        next_stage_hint,
        reason,
    };
    validate_reactor_stage_choice(&choice)?;
    Ok(choice)
}

pub(super) fn normalize_reactor_stage_choice(
    choice: StageChoice,
) -> Result<StageChoice, OrchestratorError> {
    build_reactor_stage_choice(
        choice.skeleton,
        choice.artifact_family,
        choice.current_stage,
        choice.derivation_policy,
        choice.next_stage_hint,
        choice.reason,
    )
}

pub(super) fn resolve_stage_choice_from_skeleton_choice(
    choice: SkeletonChoice,
    fallback_artifact_family: Option<ArtifactFamily>,
    derivation_policy: DerivationPolicy,
    force_default_initial_stage: bool,
) -> Result<StageChoice, OrchestratorError> {
    let artifact_family = choice
        .artifact_family
        .or(fallback_artifact_family)
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "skeleton_choice requires explicit artifact_family hint".to_string(),
            ))
        })?;
    let initial_stage = if force_default_initial_stage {
        choice.skeleton.default_initial_stage()
    } else {
        choice.initial_stage
    };
    build_reactor_stage_choice(
        choice.skeleton,
        artifact_family,
        initial_stage,
        derivation_policy,
        None,
        choice.reason,
    )
}
