use serde_json::Value;

use orchestral_core::planner::PlanError;
use orchestral_core::types::{
    ArtifactFamily, Plan, StageChoice, StageKind, Step, StepId, StepIoBinding, Task,
};

use super::super::{
    require_working_set_string, require_working_set_string_array, OrchestratorError,
};

const REACTOR_CODEBASE_COLLECT_TARGETS_ACTION: &str = "reactor_codebase_collect_targets";
const REACTOR_CODEBASE_COLLECT_RESULTS_ACTION: &str = "reactor_codebase_collect_results";
const REACTOR_CODEBASE_AGGREGATE_VERIFY_ACTION: &str = "reactor_codebase_aggregate_verify";
const REACTOR_CODEBASE_EXPORT_SUMMARY_ACTION: &str = "reactor_codebase_export_summary";
const REACTOR_DOCUMENT_INSPECT_ACTION: &str = "reactor_document_inspect";
const REACTOR_SPREADSHEET_INSPECT_ACTION: &str = "reactor_spreadsheet_inspect";
const REACTOR_SPREADSHEET_APPLY_ACTION: &str = "reactor_spreadsheet_apply_patch";
const REACTOR_SPREADSHEET_VERIFY_ACTION: &str = "reactor_spreadsheet_verify_patch";
const REACTOR_STRUCTURED_INSPECT_ACTION: &str = "reactor_structured_inspect";
const REACTOR_STRUCTURED_APPLY_ACTION: &str = "reactor_structured_apply_patch";
const REACTOR_STRUCTURED_VERIFY_ACTION: &str = "reactor_structured_verify_patch";

fn build_leaf_output_rules(keys: &[&str], result_slot: &str) -> Value {
    let mut map = serde_json::Map::new();
    for key in keys {
        map.insert(
            (*key).to_string(),
            serde_json::json!({
                "candidates": [
                    { "slot": result_slot, "path": key }
                ]
            }),
        );
    }
    Value::Object(map)
}

fn mixed_run_goal() -> &'static str {
    "Run stage for cross-family workspace execution. Using user_request, instruction_inspection, spreadsheet_path, spreadsheet_inspection, structured_path, structured_inspection, and derivation_policy, return JSON with keys spreadsheet_patch_spec, structured_patch_spec, and execution_summary. spreadsheet_patch_spec must be an object with path and fills, where fills is an array of {cell, value}. structured_patch_spec must be an object with files, where files is an array of {path, operations}. For structured_patch_spec.operations, use only op=set or op=remove; never emit replace, add, copy, move, or test. Use the explicit target paths provided in spreadsheet_path and structured_path. Treat the instruction document inspection as authoritative guidance for what to change. Preserve unrelated workbook cells and unrelated structured fields. execution_summary must concisely describe the intended spreadsheet and structured changes."
}

pub(super) fn lower_run_and_verify_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<Plan, OrchestratorError> {
    if choice.artifact_family != ArtifactFamily::Codebase {
        return Err(OrchestratorError::Planner(PlanError::Generation(format!(
            "run_and_verify lowering not implemented for artifact_family={:?}",
            choice.artifact_family
        ))));
    }

    match choice.current_stage {
        StageKind::Prepare => build_prepare_plan(task, choice),
        StageKind::Run => build_run_plan(task, choice),
        StageKind::Collect => build_collect_plan(task, choice),
        StageKind::Verify => build_verify_plan(task, choice),
        StageKind::Export => build_export_plan(task, choice),
        unsupported => Err(OrchestratorError::Planner(PlanError::Generation(format!(
            "run_and_verify does not support stage {:?} for artifact_family={:?}",
            unsupported, choice.artifact_family
        )))),
    }
}

fn build_prepare_plan(task: &Task, choice: &StageChoice) -> Result<Plan, OrchestratorError> {
    let mut plan = Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::action(
                "reactor_prepare_collect_targets",
                REACTOR_CODEBASE_COLLECT_TARGETS_ACTION,
            )
            .with_exports(vec![
                "instruction_path".to_string(),
                "instruction_source_paths".to_string(),
                "spreadsheet_path".to_string(),
                "structured_path".to_string(),
                "structured_source_paths".to_string(),
            ])
            .with_params(serde_json::json!({
                "user_request": task.intent.content,
            })),
            Step::action(
                "reactor_prepare_instruction_inspect",
                REACTOR_DOCUMENT_INSPECT_ACTION,
            )
            .with_depends_on(vec![StepId::from("reactor_prepare_collect_targets")])
            .with_exports(vec!["inspection".to_string()])
            .with_io_bindings(vec![StepIoBinding::required(
                "reactor_prepare_collect_targets.instruction_source_paths",
                "source_paths",
            )]),
            Step::action(
                "reactor_prepare_spreadsheet_inspect",
                REACTOR_SPREADSHEET_INSPECT_ACTION,
            )
            .with_depends_on(vec![StepId::from("reactor_prepare_collect_targets")])
            .with_exports(vec!["inspection".to_string()])
            .with_io_bindings(vec![StepIoBinding::required(
                "reactor_prepare_collect_targets.spreadsheet_path",
                "path",
            )]),
            Step::action(
                "reactor_prepare_structured_inspect",
                REACTOR_STRUCTURED_INSPECT_ACTION,
            )
            .with_depends_on(vec![StepId::from("reactor_prepare_collect_targets")])
            .with_exports(vec!["inspection".to_string()])
            .with_io_bindings(vec![StepIoBinding::required(
                "reactor_prepare_collect_targets.structured_source_paths",
                "source_paths",
            )]),
        ],
    );
    plan.on_failure = Some("Cross-family prepare failed: {{error}}".to_string());
    Ok(plan)
}

fn build_run_plan(task: &Task, choice: &StageChoice) -> Result<Plan, OrchestratorError> {
    let spreadsheet_path = require_working_set_string(
        &task.working_set_snapshot,
        "reactor_prepare_collect_targets.spreadsheet_path",
    )?;
    let structured_path = require_working_set_string(
        &task.working_set_snapshot,
        "reactor_prepare_collect_targets.structured_path",
    )?;

    let mut params = serde_json::Map::new();
    params.insert("mode".to_string(), serde_json::json!("leaf"));
    params.insert(
        "goal".to_string(),
        Value::String(mixed_run_goal().to_string()),
    );
    params.insert(
        "allowed_actions".to_string(),
        serde_json::json!(["json_stdout"]),
    );
    params.insert("max_iterations".to_string(), serde_json::json!(1));
    params.insert(
        "result_slot".to_string(),
        Value::String("run_result".to_string()),
    );
    params.insert(
        "output_keys".to_string(),
        serde_json::json!([
            "spreadsheet_patch_spec",
            "structured_patch_spec",
            "execution_summary"
        ]),
    );
    params.insert(
        "output_rules".to_string(),
        build_leaf_output_rules(
            &[
                "spreadsheet_patch_spec",
                "structured_patch_spec",
                "execution_summary",
            ],
            "run_result",
        ),
    );
    params.insert(
        "user_request".to_string(),
        Value::String(task.intent.content.clone()),
    );
    params.insert(
        "instruction_inspection".to_string(),
        task.working_set_snapshot
            .get("reactor_prepare_instruction_inspect.inspection")
            .cloned()
            .unwrap_or(Value::Null),
    );
    params.insert(
        "spreadsheet_path".to_string(),
        Value::String(spreadsheet_path.clone()),
    );
    params.insert(
        "spreadsheet_inspection".to_string(),
        task.working_set_snapshot
            .get("reactor_prepare_spreadsheet_inspect.inspection")
            .cloned()
            .unwrap_or(Value::Null),
    );
    params.insert(
        "structured_path".to_string(),
        Value::String(structured_path.clone()),
    );
    params.insert(
        "structured_inspection".to_string(),
        task.working_set_snapshot
            .get("reactor_prepare_structured_inspect.inspection")
            .cloned()
            .unwrap_or(Value::Null),
    );
    params.insert(
        "derivation_policy".to_string(),
        serde_json::to_value(choice.derivation_policy).map_err(|err| {
            OrchestratorError::Planner(PlanError::Generation(format!(
                "failed to serialize derivation_policy: {}",
                err
            )))
        })?,
    );

    let mut plan = Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::leaf_agent("reactor_run_derive")
                .with_exports(vec![
                    "spreadsheet_patch_spec".to_string(),
                    "structured_patch_spec".to_string(),
                    "execution_summary".to_string(),
                ])
                .with_params(Value::Object(params)),
            Step::action(
                "reactor_run_apply_spreadsheet",
                REACTOR_SPREADSHEET_APPLY_ACTION,
            )
            .with_depends_on(vec![StepId::from("reactor_run_derive")])
            .with_exports(vec![
                "updated_file_path".to_string(),
                "patch_count".to_string(),
                "summary".to_string(),
            ])
            .with_io_bindings(vec![StepIoBinding::required(
                "reactor_run_derive.spreadsheet_patch_spec",
                "patch_spec",
            )])
            .with_params(serde_json::json!({
                "path": spreadsheet_path,
            })),
            Step::action(
                "reactor_run_apply_structured",
                REACTOR_STRUCTURED_APPLY_ACTION,
            )
            .with_depends_on(vec![StepId::from("reactor_run_derive")])
            .with_exports(vec![
                "updated_paths".to_string(),
                "patch_count".to_string(),
                "summary".to_string(),
            ])
            .with_io_bindings(vec![StepIoBinding::required(
                "reactor_run_derive.structured_patch_spec",
                "patch_spec",
            )])
            .with_params(serde_json::json!({})),
        ],
    );
    plan.on_failure = Some("Cross-family run failed: {{error}}".to_string());
    Ok(plan)
}

fn build_collect_plan(task: &Task, choice: &StageChoice) -> Result<Plan, OrchestratorError> {
    let spreadsheet_path = require_working_set_string(
        &task.working_set_snapshot,
        "reactor_run_apply_spreadsheet.updated_file_path",
    )
    .or_else(|_| {
        require_working_set_string(
            &task.working_set_snapshot,
            "reactor_prepare_collect_targets.spreadsheet_path",
        )
    })?;
    let structured_paths = require_working_set_string_array(
        &task.working_set_snapshot,
        "reactor_run_apply_structured.updated_paths",
    )?;
    let execution_summary = require_working_set_string(
        &task.working_set_snapshot,
        "reactor_run_derive.execution_summary",
    )?;

    let mut plan = Plan::new(
        choice.stage_goal.clone(),
        vec![Step::action(
            "reactor_collect_results",
            REACTOR_CODEBASE_COLLECT_RESULTS_ACTION,
        )
        .with_exports(vec!["collected_result".to_string(), "summary".to_string()])
        .with_params(serde_json::json!({
            "spreadsheet_path": spreadsheet_path,
            "structured_paths": structured_paths,
            "execution_summary": execution_summary,
        }))],
    );
    plan.on_failure = Some("Cross-family collect failed: {{error}}".to_string());
    Ok(plan)
}

fn build_verify_plan(task: &Task, choice: &StageChoice) -> Result<Plan, OrchestratorError> {
    let spreadsheet_path = require_working_set_string(
        &task.working_set_snapshot,
        "reactor_run_apply_spreadsheet.updated_file_path",
    )
    .or_else(|_| {
        require_working_set_string(
            &task.working_set_snapshot,
            "reactor_prepare_collect_targets.spreadsheet_path",
        )
    })?;
    let spreadsheet_patch_spec = task
        .working_set_snapshot
        .get("reactor_run_derive.spreadsheet_patch_spec")
        .cloned()
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "reactor expected working_set key 'reactor_run_derive.spreadsheet_patch_spec'"
                    .to_string(),
            ))
        })?;
    let structured_patch_spec = task
        .working_set_snapshot
        .get("reactor_run_derive.structured_patch_spec")
        .cloned()
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "reactor expected working_set key 'reactor_run_derive.structured_patch_spec'"
                    .to_string(),
            ))
        })?;
    let structured_inspection = task
        .working_set_snapshot
        .get("reactor_prepare_structured_inspect.inspection")
        .cloned()
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "reactor expected working_set key 'reactor_prepare_structured_inspect.inspection'"
                    .to_string(),
            ))
        })?;
    let collected_result = task
        .working_set_snapshot
        .get("collected_result")
        .cloned()
        .unwrap_or(Value::Null);

    let mut plan = Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::action(
                "reactor_verify_spreadsheet",
                REACTOR_SPREADSHEET_VERIFY_ACTION,
            )
            .with_exports(vec!["verify_decision".to_string(), "summary".to_string()])
            .with_params(serde_json::json!({
                "path": spreadsheet_path,
                "patch_spec": spreadsheet_patch_spec,
            })),
            Step::action(
                "reactor_verify_structured",
                REACTOR_STRUCTURED_VERIFY_ACTION,
            )
            .with_exports(vec!["verify_decision".to_string(), "summary".to_string()])
            .with_params(serde_json::json!({
                "patch_spec": structured_patch_spec,
                "inspection": structured_inspection,
            })),
            Step::action(
                "reactor_verify_aggregate",
                REACTOR_CODEBASE_AGGREGATE_VERIFY_ACTION,
            )
            .with_depends_on(vec![
                StepId::from("reactor_verify_spreadsheet"),
                StepId::from("reactor_verify_structured"),
            ])
            .with_exports(vec!["verify_decision".to_string(), "summary".to_string()])
            .with_io_bindings(vec![
                StepIoBinding::required(
                    "reactor_verify_spreadsheet.verify_decision",
                    "spreadsheet_verify_decision",
                ),
                StepIoBinding::required(
                    "reactor_verify_structured.verify_decision",
                    "structured_verify_decision",
                ),
            ])
            .with_params(serde_json::json!({
                "collected_result": collected_result,
            })),
        ],
    );
    plan.on_failure = Some("Cross-family verify failed: {{error}}".to_string());
    Ok(plan)
}

fn build_export_plan(task: &Task, choice: &StageChoice) -> Result<Plan, OrchestratorError> {
    let collected_result = task
        .working_set_snapshot
        .get("collected_result")
        .cloned()
        .unwrap_or(Value::Null);
    let verify_decision = task
        .working_set_snapshot
        .get("verify_decision")
        .cloned()
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "reactor expected working_set key 'verify_decision'".to_string(),
            ))
        })?;

    let mut plan = Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::action("reactor_export", REACTOR_CODEBASE_EXPORT_SUMMARY_ACTION)
                .with_exports(vec!["summary".to_string()])
                .with_params(serde_json::json!({
                    "collected_result": collected_result,
                    "verify_decision": verify_decision,
                })),
        ],
    );
    plan.on_complete = Some("{{summary}}".to_string());
    plan.on_failure = Some("Cross-family export failed: {{error}}".to_string());
    Ok(plan)
}
