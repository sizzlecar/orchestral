use std::collections::HashMap;

use serde_json::Value;

use orchestral_core::planner::PlanError;
use orchestral_core::types::{
    ArtifactFamily, Plan, StageChoice, StageKind, Step, StepId, StepIoBinding, Task,
};

use super::super::{
    require_working_set_string, require_working_set_string_array, OrchestratorError,
};
use super::unsupported_stage_error;

const REACTOR_SPREADSHEET_LOCATE_ACTION: &str = "reactor_spreadsheet_locate";
const REACTOR_SPREADSHEET_INSPECT_ACTION: &str = "reactor_spreadsheet_inspect";
const REACTOR_SPREADSHEET_ASSESS_ACTION: &str = "reactor_spreadsheet_assess_readiness";
const REACTOR_SPREADSHEET_APPLY_ACTION: &str = "reactor_spreadsheet_apply_patch";
const REACTOR_SPREADSHEET_VERIFY_ACTION: &str = "reactor_spreadsheet_verify_patch";
const REACTOR_DOCUMENT_LOCATE_ACTION: &str = "reactor_document_locate";
const REACTOR_DOCUMENT_INSPECT_ACTION: &str = "reactor_document_inspect";
const REACTOR_DOCUMENT_ASSESS_ACTION: &str = "reactor_document_assess_readiness";
const REACTOR_DOCUMENT_APPLY_ACTION: &str = "reactor_document_apply_patch";
const REACTOR_DOCUMENT_VERIFY_ACTION: &str = "reactor_document_verify_patch";
const REACTOR_STRUCTURED_LOCATE_ACTION: &str = "reactor_structured_locate";
const REACTOR_STRUCTURED_INSPECT_ACTION: &str = "reactor_structured_inspect";
const REACTOR_STRUCTURED_DERIVE_ACTION: &str = "reactor_structured_derive_candidates";
const REACTOR_STRUCTURED_ASSESS_ACTION: &str = "reactor_structured_assess_readiness";
const REACTOR_STRUCTURED_BUILD_PATCH_ACTION: &str = "reactor_structured_build_patch_spec";
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

#[derive(Debug, Clone, Copy)]
struct LocateAndPatchFamilyAdapter {
    artifact_family: ArtifactFamily,
    locate_action: &'static str,
    inspect_action: &'static str,
    assess_action: &'static str,
    apply_action: &'static str,
    verify_action: &'static str,
    locate_export_keys: &'static [&'static str],
    verify_complete_message: &'static str,
}

impl LocateAndPatchFamilyAdapter {
    fn for_artifact_family(artifact_family: ArtifactFamily) -> Option<Self> {
        match artifact_family {
            ArtifactFamily::Spreadsheet => Some(Self {
                artifact_family,
                locate_action: REACTOR_SPREADSHEET_LOCATE_ACTION,
                inspect_action: REACTOR_SPREADSHEET_INSPECT_ACTION,
                assess_action: REACTOR_SPREADSHEET_ASSESS_ACTION,
                apply_action: REACTOR_SPREADSHEET_APPLY_ACTION,
                verify_action: REACTOR_SPREADSHEET_VERIFY_ACTION,
                locate_export_keys: &["source_path", "artifact_candidates", "artifact_count"],
                verify_complete_message: "Workbook updated and verified.",
            }),
            ArtifactFamily::Document => Some(Self {
                artifact_family,
                locate_action: REACTOR_DOCUMENT_LOCATE_ACTION,
                inspect_action: REACTOR_DOCUMENT_INSPECT_ACTION,
                assess_action: REACTOR_DOCUMENT_ASSESS_ACTION,
                apply_action: REACTOR_DOCUMENT_APPLY_ACTION,
                verify_action: REACTOR_DOCUMENT_VERIFY_ACTION,
                locate_export_keys: &[
                    "source_paths",
                    "artifact_candidates",
                    "artifact_count",
                    "report_path",
                ],
                verify_complete_message: "Documents updated and verified.",
            }),
            ArtifactFamily::Structured => Some(Self {
                artifact_family,
                locate_action: REACTOR_STRUCTURED_LOCATE_ACTION,
                inspect_action: REACTOR_STRUCTURED_INSPECT_ACTION,
                assess_action: REACTOR_STRUCTURED_ASSESS_ACTION,
                apply_action: REACTOR_STRUCTURED_APPLY_ACTION,
                verify_action: REACTOR_STRUCTURED_VERIFY_ACTION,
                locate_export_keys: &["source_paths", "artifact_candidates", "artifact_count"],
                verify_complete_message: "Structured files updated and verified.",
            }),
            _ => None,
        }
    }

    fn source_snapshot_key(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => "source_path",
            ArtifactFamily::Document | ArtifactFamily::Structured => "source_paths",
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn inspect_input_key(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => "path",
            ArtifactFamily::Document | ArtifactFamily::Structured => "source_paths",
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn family_label(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => "Spreadsheet",
            ArtifactFamily::Document => "Document",
            ArtifactFamily::Structured => "Structured",
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn derive_goal(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => {
                "Derive locate_and_patch spreadsheet candidates. Using user_request, source_path, inspection, and derivation_policy, return JSON with keys patch_candidates and summary. patch_candidates must be an object with keys candidates, unknowns, and assumptions. patch_candidates.candidates.cells must describe candidate fills for patchable cells without modifying the file. permissive policy may propose generic but coherent spreadsheet fill content when structure is clear. strict policy should surface unresolved unknowns through patch_candidates.unknowns. Do not decide continuation here."
            }
            ArtifactFamily::Document => {
                "Derive locate_and_patch document candidates. Using user_request, source_paths, inspection, and derivation_policy, return JSON with keys patch_candidates and summary. patch_candidates must be an object with keys candidates, unknowns, and assumptions. patch_candidates.candidates.files must be an array. Each file entry should contain path, planned_changes, needs_user_input, and unknowns. If the requested document change depends on concrete facts not present in user_request or inspection (for example dates, names, amounts, IDs, addresses, contract numbers, or other business facts), you must record them in unknowns, set needs_user_input=true for that file, and avoid inventing values. summary should be a concise change plan. Do not modify files. Do not decide continuation here."
            }
            ArtifactFamily::Structured => {
                "Derive locate_and_patch structured patch candidates. Using user_request, source_paths, inspection, and derivation_policy, return JSON with keys patch_candidates and summary. patch_candidates must be an object with keys candidates, unknowns, and assumptions. patch_candidates.candidates.files must be an array directly, not wrapped in another object. Each file entry must contain path, operations, needs_user_input, and unknowns. operations must be an array of JSON Pointer edits shaped as {op, path, value?}. Use op=set when assigning a concrete JSON-compatible value and op=remove when deleting a field. inspection.files[*].field_inventory lists canonical field references with selector, pointer, value_type, summary, and value. Use that inventory as the authoritative path model. When user_request names a dotted selector and it uniquely matches a field_inventory.selector, you must use the corresponding field_inventory.pointer in operations and treat the request as explicit enough. When user_request already states an explicit target path and target value or explicit remove instruction, you must mirror that instruction into operations, set needs_user_input=false for that file, and leave unknowns empty unless the referenced path is ambiguous or missing. strict policy should surface unresolved unknowns instead of guessing target values. Do not modify files. Do not decide continuation here."
            }
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn commit_goal(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => {
                "Commit stage for locate_and_patch spreadsheet patching. Using user_request, source_path, inspection, patch_candidates, and derivation_policy, return JSON with keys patch_spec and summary. patch_candidates follows the derive envelope with candidates, unknowns, and assumptions. patch_spec must be an object with path and fills. patch_spec.fills must be an array of {cell, value}. Cover every patchable cell listed in inspection.selected_region.patchable_cells unless the cell is already handled by a formula or existing non-empty value. If derivation_policy is permissive, fill any remaining uncovered patchable cells with coherent generic content inferred from row labels and column headers instead of leaving gaps. Do not include unchanged cells. Preserve formulas and workbook structure. When a proposed fill is numeric, emit a numeric JSON value instead of prose."
            }
            ArtifactFamily::Document => {
                "Commit stage for locate_and_patch document patching. Using user_request, resume_user_input, source_paths, inspection, patch_candidates, and derivation_policy, return JSON with keys patch_spec and summary. patch_candidates follows the derive envelope with candidates, unknowns, and assumptions. patch_spec must be an object with updates. updates must be an array of {path, content}. Generate complete final markdown/text content for each file that needs changes. Replace TODO placeholders with concrete coherent content, add a top-level title when missing, preserve unaffected content, and if resume_user_input requests a summary/report markdown path include an update for that file too. Do not emit unchanged files."
            }
            ArtifactFamily::Structured => {
                "Commit stage for locate_and_patch structured patching. Using user_request, source_paths, inspection, and derivation_policy, return JSON with keys patch_spec and summary. Treat the explicit user_request as authoritative for requested field updates and removals. patch_spec must be an object with files and optional summary. files must be an array of {path, operations}. operations must be an array of JSON Pointer edits shaped as {op, path, value?}. Use op=set to assign a concrete JSON-compatible value and op=remove to delete a field. inspection.files[*].field_inventory lists canonical field references with selector and pointer; use those pointers as the source of truth when constructing operations. Every explicit user-requested update or removal must appear in patch_spec unless the referenced path is missing or ambiguous in inspection. Preserve unrelated keys and do not emit unchanged files."
            }
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn source_value(self, snapshot: &HashMap<String, Value>) -> Result<Value, OrchestratorError> {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => Ok(Value::String(require_working_set_string(
                snapshot,
                "source_path",
            )?)),
            ArtifactFamily::Document | ArtifactFamily::Structured => Ok(Value::Array(
                require_working_set_string_array(snapshot, "source_paths")?
                    .into_iter()
                    .map(Value::String)
                    .collect(),
            )),
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn build_locate_plan(self, task: &Task, choice: &StageChoice) -> Plan {
        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![Step::action("reactor_locate", self.locate_action)
                .with_exports(
                    self.locate_export_keys
                        .iter()
                        .map(|key| (*key).to_string())
                        .collect(),
                )
                .with_params(serde_json::json!({
                    "source_root": ".",
                    "user_request": task.intent.content,
                }))],
        );
        plan.on_failure = Some(format!("{} locate failed: {{error}}", self.family_label()));
        plan
    }

    fn build_probe_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
        let source_value = self.source_value(&task.working_set_snapshot)?;
        let mut params = serde_json::Map::new();
        params.insert(self.inspect_input_key().to_string(), source_value);
        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![Step::action("reactor_probe_inspect", self.inspect_action)
                .with_exports(vec!["inspection".to_string()])
                .with_params(Value::Object(params))],
        );
        plan.on_failure = Some(format!("{} probe failed: {{error}}", self.family_label()));
        Ok(plan)
    }

    fn build_derive_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
        if matches!(self.artifact_family, ArtifactFamily::Structured) {
            let mut plan = Plan::new(
                choice.stage_goal.clone(),
                vec![Step::action("reactor_derive", REACTOR_STRUCTURED_DERIVE_ACTION)
                    .with_exports(vec!["patch_candidates".to_string(), "summary".to_string()])
                    .with_params(serde_json::json!({
                        "user_request": task.intent.content,
                        "inspection": task
                            .working_set_snapshot
                            .get("inspection")
                            .cloned()
                            .unwrap_or(Value::Null),
                        "derivation_policy": serde_json::to_value(choice.derivation_policy).map_err(|err| {
                            OrchestratorError::Planner(PlanError::Generation(format!(
                                "failed to serialize derivation_policy: {}",
                                err
                            )))
                        })?,
                    }))],
            );
            plan.on_failure = Some(format!("{} derive failed: {{error}}", self.family_label()));
            return Ok(plan);
        }

        let mut params = serde_json::Map::new();
        params.insert("mode".to_string(), serde_json::json!("leaf"));
        params.insert(
            "goal".to_string(),
            Value::String(self.derive_goal().to_string()),
        );
        params.insert(
            "allowed_actions".to_string(),
            serde_json::json!(["json_stdout"]),
        );
        params.insert("max_iterations".to_string(), serde_json::json!(1));
        params.insert(
            "result_slot".to_string(),
            Value::String("derive_result".to_string()),
        );
        params.insert(
            "output_keys".to_string(),
            serde_json::json!(["patch_candidates", "summary"]),
        );
        params.insert(
            "output_rules".to_string(),
            build_leaf_output_rules(&["patch_candidates", "summary"], "derive_result"),
        );
        params.insert(
            "user_request".to_string(),
            Value::String(task.intent.content.clone()),
        );
        params.insert(
            self.source_snapshot_key().to_string(),
            self.source_value(&task.working_set_snapshot)?,
        );
        params.insert(
            "inspection".to_string(),
            task.working_set_snapshot
                .get("inspection")
                .cloned()
                .unwrap_or(Value::Null),
        );
        params.insert(
            "stage_goal".to_string(),
            Value::String(choice.stage_goal.clone()),
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
            vec![Step::leaf_agent("reactor_derive")
                .with_exports(vec!["patch_candidates".to_string(), "summary".to_string()])
                .with_params(Value::Object(params))],
        );
        plan.on_failure = Some(format!("{} derive failed: {{error}}", self.family_label()));
        Ok(plan)
    }

    fn build_assess_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
        let mut params = serde_json::Map::new();
        params.insert(
            "inspection".to_string(),
            task.working_set_snapshot
                .get("inspection")
                .cloned()
                .unwrap_or(Value::Null),
        );
        params.insert(
            "patch_candidates".to_string(),
            task.working_set_snapshot
                .get("patch_candidates")
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
        if matches!(self.artifact_family, ArtifactFamily::Document) {
            params.insert(
                "user_request".to_string(),
                Value::String(task.intent.content.clone()),
            );
        }
        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![Step::action("reactor_assess", self.assess_action)
                .with_exports(vec!["continuation".to_string(), "summary".to_string()])
                .with_params(Value::Object(params))],
        );
        plan.on_failure = Some(format!("{} assess failed: {{error}}", self.family_label()));
        Ok(plan)
    }

    fn build_commit_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
        if matches!(self.artifact_family, ArtifactFamily::Structured) {
            let mut plan = Plan::new(
                choice.stage_goal.clone(),
                vec![
                    Step::action(
                        "reactor_commit_build_patch_spec",
                        REACTOR_STRUCTURED_BUILD_PATCH_ACTION,
                    )
                    .with_exports(vec!["patch_spec".to_string(), "summary".to_string()])
                    .with_params(serde_json::json!({
                        "patch_candidates": task
                            .working_set_snapshot
                            .get("patch_candidates")
                            .cloned()
                            .unwrap_or(Value::Null),
                    })),
                    Step::action("reactor_commit_apply", self.apply_action)
                        .with_depends_on(vec![StepId::from("reactor_commit_build_patch_spec")])
                        .with_exports(vec![
                            "updated_paths".to_string(),
                            "patch_count".to_string(),
                            "summary".to_string(),
                        ])
                        .with_io_bindings(vec![StepIoBinding::required(
                            "reactor_commit_build_patch_spec.patch_spec",
                            "patch_spec",
                        )]),
                ],
            );
            plan.on_failure = Some(format!("{} commit failed: {{error}}", self.family_label()));
            return Ok(plan);
        }

        let mut params = serde_json::Map::new();
        params.insert("mode".to_string(), serde_json::json!("leaf"));
        params.insert(
            "goal".to_string(),
            Value::String(self.commit_goal().to_string()),
        );
        params.insert(
            "allowed_actions".to_string(),
            serde_json::json!(["json_stdout"]),
        );
        params.insert("max_iterations".to_string(), serde_json::json!(1));
        params.insert(
            "result_slot".to_string(),
            Value::String("commit_result".to_string()),
        );
        params.insert(
            "output_keys".to_string(),
            serde_json::json!(["patch_spec", "summary"]),
        );
        params.insert(
            "output_rules".to_string(),
            build_leaf_output_rules(&["patch_spec", "summary"], "commit_result"),
        );
        params.insert(
            "user_request".to_string(),
            Value::String(task.intent.content.clone()),
        );
        params.insert(
            self.source_snapshot_key().to_string(),
            self.source_value(&task.working_set_snapshot)?,
        );
        params.insert(
            "inspection".to_string(),
            task.working_set_snapshot
                .get("inspection")
                .cloned()
                .unwrap_or(Value::Null),
        );
        if !matches!(self.artifact_family, ArtifactFamily::Structured) {
            params.insert(
                "patch_candidates".to_string(),
                task.working_set_snapshot
                    .get("patch_candidates")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }
        params.insert(
            "derivation_policy".to_string(),
            serde_json::to_value(choice.derivation_policy).map_err(|err| {
                OrchestratorError::Planner(PlanError::Generation(format!(
                    "failed to serialize derivation_policy: {}",
                    err
                )))
            })?,
        );
        if matches!(self.artifact_family, ArtifactFamily::Document) {
            params.insert(
                "resume_user_input".to_string(),
                task.working_set_snapshot
                    .get("resume_user_input")
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        }

        let apply_step = match self.artifact_family {
            ArtifactFamily::Spreadsheet => Step::action("reactor_commit_apply", self.apply_action)
                .with_depends_on(vec![StepId::from("reactor_commit_derive")])
                .with_exports(vec![
                    "updated_file_path".to_string(),
                    "patch_count".to_string(),
                    "summary".to_string(),
                ])
                .with_io_bindings(vec![StepIoBinding::required(
                    "reactor_commit_derive.patch_spec",
                    "patch_spec",
                )])
                .with_params(serde_json::json!({
                    "path": require_working_set_string(&task.working_set_snapshot, "source_path")?,
                })),
            ArtifactFamily::Document => Step::action("reactor_commit_apply", self.apply_action)
                .with_depends_on(vec![StepId::from("reactor_commit_derive")])
                .with_exports(vec![
                    "updated_paths".to_string(),
                    "patch_count".to_string(),
                    "summary".to_string(),
                ])
                .with_io_bindings(vec![StepIoBinding::required(
                    "reactor_commit_derive.patch_spec",
                    "patch_spec",
                )])
                .with_params(serde_json::json!({
                    "report_path": task
                        .working_set_snapshot
                        .get("report_path")
                        .cloned()
                        .unwrap_or(Value::Null),
                })),
            ArtifactFamily::Structured => Step::action("reactor_commit_apply", self.apply_action)
                .with_depends_on(vec![StepId::from("reactor_commit_derive")])
                .with_exports(vec![
                    "updated_paths".to_string(),
                    "patch_count".to_string(),
                    "summary".to_string(),
                ])
                .with_io_bindings(vec![StepIoBinding::required(
                    "reactor_commit_derive.patch_spec",
                    "patch_spec",
                )]),
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        };

        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![
                Step::leaf_agent("reactor_commit_derive")
                    .with_exports(vec!["patch_spec".to_string(), "summary".to_string()])
                    .with_params(Value::Object(params)),
                apply_step,
            ],
        );
        plan.on_failure = Some(format!("{} commit failed: {{error}}", self.family_label()));
        Ok(plan)
    }

    fn build_verify_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
        let mut params = serde_json::Map::new();
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => {
                let path =
                    require_working_set_string(&task.working_set_snapshot, "updated_file_path")
                        .or_else(|_| {
                            require_working_set_string(&task.working_set_snapshot, "source_path")
                        })?;
                params.insert("path".to_string(), Value::String(path));
                if let Some(patch_spec) = task.working_set_snapshot.get("patch_spec").cloned() {
                    params.insert("patch_spec".to_string(), patch_spec);
                }
            }
            ArtifactFamily::Document => {
                params.insert(
                    "patch_spec".to_string(),
                    task.working_set_snapshot
                        .get("patch_spec")
                        .cloned()
                        .ok_or_else(|| {
                            OrchestratorError::Planner(PlanError::Generation(
                                "reactor expected working_set key 'patch_spec'".to_string(),
                            ))
                        })?,
                );
                params.insert(
                    "inspection".to_string(),
                    task.working_set_snapshot
                        .get("inspection")
                        .cloned()
                        .ok_or_else(|| {
                            OrchestratorError::Planner(PlanError::Generation(
                                "reactor expected working_set key 'inspection'".to_string(),
                            ))
                        })?,
                );
                params.insert(
                    "user_request".to_string(),
                    Value::String(task.intent.content.clone()),
                );
                params.insert(
                    "resume_user_input".to_string(),
                    task.working_set_snapshot
                        .get("resume_user_input")
                        .cloned()
                        .unwrap_or(Value::Null),
                );
            }
            ArtifactFamily::Structured => {
                params.insert(
                    "patch_spec".to_string(),
                    task.working_set_snapshot
                        .get("patch_spec")
                        .cloned()
                        .ok_or_else(|| {
                            OrchestratorError::Planner(PlanError::Generation(
                                "reactor expected working_set key 'patch_spec'".to_string(),
                            ))
                        })?,
                );
                if let Some(inspection) = task.working_set_snapshot.get("inspection").cloned() {
                    params.insert("inspection".to_string(), inspection);
                }
            }
            unsupported => unreachable!(
                "locate_and_patch adapter does not support artifact family {:?}",
                unsupported
            ),
        }

        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![Step::action("reactor_verify", self.verify_action)
                .with_exports(vec!["verify_decision".to_string(), "summary".to_string()])
                .with_params(Value::Object(params))],
        );
        plan.on_complete = Some(self.verify_complete_message.to_string());
        plan.on_failure = Some(format!("{} verify failed: {{error}}", self.family_label()));
        Ok(plan)
    }
}

pub(super) fn lower_locate_and_patch_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<Plan, OrchestratorError> {
    if let Some(adapter) = LocateAndPatchFamilyAdapter::for_artifact_family(choice.artifact_family)
    {
        return match choice.current_stage {
            StageKind::Locate => Ok(adapter.build_locate_plan(task, choice)),
            StageKind::Probe => adapter.build_probe_plan(task, choice),
            StageKind::Derive => adapter.build_derive_plan(task, choice),
            StageKind::Assess => adapter.build_assess_plan(task, choice),
            StageKind::Commit => adapter.build_commit_plan(task, choice),
            StageKind::Verify => adapter.build_verify_plan(task, choice),
            unsupported => unsupported_stage_error(choice, unsupported),
        };
    }

    Err(OrchestratorError::Planner(PlanError::Generation(format!(
        "reactor lowering not implemented for skeleton={:?} artifact_family={:?} current_stage={:?}",
        choice.skeleton, choice.artifact_family, choice.current_stage
    ))))
}
