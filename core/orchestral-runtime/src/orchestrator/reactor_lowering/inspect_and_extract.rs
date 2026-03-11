use serde_json::Value;

use orchestral_core::planner::PlanError;
use orchestral_core::types::{
    ArtifactFamily, Plan, StageChoice, StageKind, Step, StepId, StepIoBinding, Task,
};

use super::super::{
    require_working_set_string, require_working_set_string_array, OrchestratorError,
};

const REACTOR_SPREADSHEET_LOCATE_ACTION: &str = "reactor_spreadsheet_locate";
const REACTOR_SPREADSHEET_INSPECT_ACTION: &str = "reactor_spreadsheet_inspect";
const REACTOR_DOCUMENT_LOCATE_ACTION: &str = "reactor_document_locate";
const REACTOR_DOCUMENT_INSPECT_ACTION: &str = "reactor_document_inspect";
const REACTOR_STRUCTURED_LOCATE_ACTION: &str = "reactor_structured_locate";
const REACTOR_STRUCTURED_INSPECT_ACTION: &str = "reactor_structured_inspect";

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
struct InspectAndExtractFamilyAdapter {
    artifact_family: ArtifactFamily,
    locate_action: &'static str,
    inspect_action: &'static str,
    locate_export_keys: &'static [&'static str],
}

impl InspectAndExtractFamilyAdapter {
    fn for_artifact_family(artifact_family: ArtifactFamily) -> Option<Self> {
        match artifact_family {
            ArtifactFamily::Spreadsheet => Some(Self {
                artifact_family,
                locate_action: REACTOR_SPREADSHEET_LOCATE_ACTION,
                inspect_action: REACTOR_SPREADSHEET_INSPECT_ACTION,
                locate_export_keys: &["source_path", "artifact_candidates", "artifact_count"],
            }),
            ArtifactFamily::Document => Some(Self {
                artifact_family,
                locate_action: REACTOR_DOCUMENT_LOCATE_ACTION,
                inspect_action: REACTOR_DOCUMENT_INSPECT_ACTION,
                locate_export_keys: &["source_paths", "artifact_candidates", "artifact_count"],
            }),
            ArtifactFamily::Structured => Some(Self {
                artifact_family,
                locate_action: REACTOR_STRUCTURED_LOCATE_ACTION,
                inspect_action: REACTOR_STRUCTURED_INSPECT_ACTION,
                locate_export_keys: &["source_paths", "artifact_candidates", "artifact_count"],
            }),
            _ => None,
        }
    }

    fn source_snapshot_key(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => "source_path",
            ArtifactFamily::Document | ArtifactFamily::Structured => "source_paths",
            unsupported => unreachable!(
                "inspect_and_extract adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn inspect_input_key(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => "path",
            ArtifactFamily::Document | ArtifactFamily::Structured => "source_paths",
            unsupported => unreachable!(
                "inspect_and_extract adapter does not support artifact family {:?}",
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
                "inspect_and_extract adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn source_value(self, task: &Task) -> Option<Value> {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => task
                .working_set_snapshot
                .get("source_path")
                .cloned()
                .or_else(|| {
                    require_working_set_string(&task.working_set_snapshot, "source_path")
                        .ok()
                        .map(Value::String)
                }),
            ArtifactFamily::Document | ArtifactFamily::Structured => task
                .working_set_snapshot
                .get("source_paths")
                .cloned()
                .or_else(|| {
                    require_working_set_string_array(&task.working_set_snapshot, "source_paths")
                        .ok()
                        .map(|items| Value::Array(items.into_iter().map(Value::String).collect()))
                }),
            unsupported => unreachable!(
                "inspect_and_extract adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn derive_goal(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => {
                "Derive inspect_and_extract spreadsheet output. Using user_request, source_path, and inspection, return JSON with keys extracted_result and summary. extracted_result must be a structured factual description of workbook contents grounded only in inspection. summary must directly answer the user's request in concise natural language. If the user asks generally what the workbook says, summarize visible sheet names, table headings, filled regions, and notable non-empty content without inventing unseen values."
            }
            ArtifactFamily::Document => {
                "Derive inspect_and_extract document output. Using user_request, source_paths, and inspection, return JSON with keys extracted_result and summary. extracted_result must be a structured factual description grounded only in inspection. summary must directly answer the user's request in concise natural language and may summarize titles, headings, TODO markers, and other explicit content found in the inspected files."
            }
            ArtifactFamily::Structured => {
                "Derive inspect_and_extract structured output. Using user_request, source_paths, and inspection, return JSON with keys extracted_result and summary. extracted_result must be a structured factual description grounded only in inspection. summary must directly answer the user's request in concise natural language and may summarize keys, values, and explicit configuration state present in the inspected files."
            }
            unsupported => unreachable!(
                "inspect_and_extract adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn export_goal(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => {
                "Export stage for inspect_and_extract spreadsheet work. Using user_request, source_path, inspection, extracted_result, and summary, return JSON with key summary. summary must be the final user-facing answer, concise but complete, and must not add facts not present in extracted_result or inspection."
            }
            ArtifactFamily::Document => {
                "Export stage for inspect_and_extract document work. Using user_request, source_paths, inspection, extracted_result, and summary, return JSON with key summary. summary must be the final user-facing answer, concise but complete, and must not add facts not present in extracted_result or inspection."
            }
            ArtifactFamily::Structured => {
                "Export stage for inspect_and_extract structured work. Using user_request, source_paths, inspection, extracted_result, and summary, return JSON with key summary. summary must be the final user-facing answer, concise but complete, and must not add facts not present in extracted_result or inspection."
            }
            unsupported => unreachable!(
                "inspect_and_extract adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn verify_goal(self) -> &'static str {
        match self.artifact_family {
            ArtifactFamily::Spreadsheet => {
                "Verify stage for inspect_and_extract spreadsheet work. Using user_request, inspection, extracted_result, and summary, return JSON with keys verify_decision and summary. verify_decision must be an object with status and reason. Use status=passed only when summary is grounded in extracted_result and extracted_result is non-empty. Use status=failed when the extracted result is empty, unsupported, or the summary overreaches. Keep summary unchanged when verification passes."
            }
            ArtifactFamily::Document => {
                "Verify stage for inspect_and_extract document work. Using user_request, inspection, extracted_result, and summary, return JSON with keys verify_decision and summary. verify_decision must be an object with status and reason. Use status=passed only when summary is grounded in extracted_result and extracted_result is non-empty. Use status=failed when the extracted result is empty, unsupported, or the summary overreaches. Keep summary unchanged when verification passes."
            }
            ArtifactFamily::Structured => {
                "Verify stage for inspect_and_extract structured work. Using user_request, inspection, extracted_result, and summary, return JSON with keys verify_decision and summary. verify_decision must be an object with status and reason. Use status=passed only when summary is grounded in extracted_result and extracted_result is non-empty. Use status=failed when the extracted result is empty, unsupported, or the summary overreaches. Keep summary unchanged when verification passes."
            }
            unsupported => unreachable!(
                "inspect_and_extract adapter does not support artifact family {:?}",
                unsupported
            ),
        }
    }

    fn build_probe_plan(self, task: &Task, choice: &StageChoice) -> Plan {
        let inspect_step = if let Some(source_value) = self.source_value(task) {
            let mut params = serde_json::Map::new();
            params.insert(self.inspect_input_key().to_string(), source_value);
            Step::action("reactor_probe_inspect", self.inspect_action)
                .with_exports(vec!["inspection".to_string()])
                .with_params(Value::Object(params))
        } else {
            let inspect_step = Step::action("reactor_probe_inspect", self.inspect_action)
                .with_depends_on(vec![StepId::from("reactor_probe_locate")])
                .with_exports(vec!["inspection".to_string()])
                .with_io_bindings(vec![StepIoBinding::required(
                    &format!("reactor_probe_locate.{}", self.source_snapshot_key()),
                    self.inspect_input_key(),
                )]);
            inspect_step
        };

        let steps = if self.source_value(task).is_some() {
            vec![inspect_step]
        } else {
            vec![
                Step::action("reactor_probe_locate", self.locate_action)
                    .with_exports(
                        self.locate_export_keys
                            .iter()
                            .map(|key| (*key).to_string())
                            .collect(),
                    )
                    .with_params(serde_json::json!({
                        "source_root": ".",
                        "user_request": task.intent.content,
                    })),
                inspect_step,
            ]
        };

        let mut plan = Plan::new(choice.stage_goal.clone(), steps);
        plan.on_failure = Some(format!("{} probe failed: {{error}}", self.family_label()));
        plan
    }

    fn build_derive_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
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
            serde_json::json!(["extracted_result", "summary"]),
        );
        params.insert(
            "output_rules".to_string(),
            build_leaf_output_rules(&["extracted_result", "summary"], "derive_result"),
        );
        params.insert(
            "user_request".to_string(),
            Value::String(task.intent.content.clone()),
        );
        if let Some(source) = self.source_value(task) {
            params.insert(self.source_snapshot_key().to_string(), source);
        }
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

        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![Step::leaf_agent("reactor_extract_derive")
                .with_exports(vec!["extracted_result".to_string(), "summary".to_string()])
                .with_params(Value::Object(params))],
        );
        plan.on_failure = Some(format!("{} derive failed: {{error}}", self.family_label()));
        Ok(plan)
    }

    fn build_export_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
        let mut params = serde_json::Map::new();
        params.insert("mode".to_string(), serde_json::json!("leaf"));
        params.insert(
            "goal".to_string(),
            Value::String(self.export_goal().to_string()),
        );
        params.insert(
            "allowed_actions".to_string(),
            serde_json::json!(["json_stdout"]),
        );
        params.insert("max_iterations".to_string(), serde_json::json!(1));
        params.insert(
            "result_slot".to_string(),
            Value::String("export_result".to_string()),
        );
        params.insert("output_keys".to_string(), serde_json::json!(["summary"]));
        params.insert(
            "output_rules".to_string(),
            build_leaf_output_rules(&["summary"], "export_result"),
        );
        params.insert(
            "user_request".to_string(),
            Value::String(task.intent.content.clone()),
        );
        if let Some(source) = self.source_value(task) {
            params.insert(self.source_snapshot_key().to_string(), source);
        }
        params.insert(
            "inspection".to_string(),
            task.working_set_snapshot
                .get("inspection")
                .cloned()
                .unwrap_or(Value::Null),
        );
        params.insert(
            "extracted_result".to_string(),
            task.working_set_snapshot
                .get("extracted_result")
                .cloned()
                .unwrap_or(Value::Null),
        );
        params.insert(
            "summary".to_string(),
            task.working_set_snapshot
                .get("summary")
                .cloned()
                .unwrap_or(Value::Null),
        );

        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![Step::leaf_agent("reactor_extract_export")
                .with_exports(vec!["summary".to_string()])
                .with_params(Value::Object(params))],
        );
        plan.on_failure = Some(format!("{} export failed: {{error}}", self.family_label()));
        Ok(plan)
    }

    fn build_verify_plan(
        self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<Plan, OrchestratorError> {
        let mut params = serde_json::Map::new();
        params.insert("mode".to_string(), serde_json::json!("leaf"));
        params.insert(
            "goal".to_string(),
            Value::String(self.verify_goal().to_string()),
        );
        params.insert(
            "allowed_actions".to_string(),
            serde_json::json!(["json_stdout"]),
        );
        params.insert("max_iterations".to_string(), serde_json::json!(1));
        params.insert(
            "result_slot".to_string(),
            Value::String("verify_result".to_string()),
        );
        params.insert(
            "output_keys".to_string(),
            serde_json::json!(["verify_decision", "summary"]),
        );
        params.insert(
            "output_rules".to_string(),
            build_leaf_output_rules(&["verify_decision", "summary"], "verify_result"),
        );
        params.insert(
            "user_request".to_string(),
            Value::String(task.intent.content.clone()),
        );
        params.insert(
            "inspection".to_string(),
            task.working_set_snapshot
                .get("inspection")
                .cloned()
                .unwrap_or(Value::Null),
        );
        params.insert(
            "extracted_result".to_string(),
            task.working_set_snapshot
                .get("extracted_result")
                .cloned()
                .unwrap_or(Value::Null),
        );
        params.insert(
            "summary".to_string(),
            task.working_set_snapshot
                .get("summary")
                .cloned()
                .unwrap_or(Value::Null),
        );

        let mut plan = Plan::new(
            choice.stage_goal.clone(),
            vec![Step::leaf_agent("reactor_extract_verify")
                .with_exports(vec!["verify_decision".to_string(), "summary".to_string()])
                .with_params(Value::Object(params))],
        );
        plan.on_complete = Some("{{summary}}".to_string());
        plan.on_failure = Some(format!("{} verify failed: {{error}}", self.family_label()));
        Ok(plan)
    }
}

pub(super) fn lower_inspect_and_extract_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<Plan, OrchestratorError> {
    if let Some(adapter) =
        InspectAndExtractFamilyAdapter::for_artifact_family(choice.artifact_family)
    {
        return match choice.current_stage {
            StageKind::Probe => Ok(adapter.build_probe_plan(task, choice)),
            StageKind::Derive => adapter.build_derive_plan(task, choice),
            StageKind::Export => adapter.build_export_plan(task, choice),
            StageKind::Verify => adapter.build_verify_plan(task, choice),
            unsupported => Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "inspect_and_extract does not support stage {:?} for artifact_family={:?}",
                unsupported, choice.artifact_family
            )))),
        };
    }

    Err(OrchestratorError::Planner(PlanError::Generation(format!(
        "reactor lowering not implemented for skeleton={:?} artifact_family={:?} current_stage={:?}",
        choice.skeleton, choice.artifact_family, choice.current_stage
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::types::{DerivationPolicy, Intent, SkeletonKind};

    fn build_choice(artifact_family: ArtifactFamily, current_stage: StageKind) -> StageChoice {
        StageChoice {
            skeleton: SkeletonKind::InspectAndExtract,
            artifact_family,
            current_stage,
            stage_goal: "test".to_string(),
            derivation_policy: DerivationPolicy::Permissive,
            next_stage_hint: None,
            reason: None,
        }
    }

    #[test]
    fn test_spreadsheet_probe_plan_locates_when_source_missing() {
        let task = Task::new(Intent::new("docs 目录下有一个excel 里面内容说了什么"));
        let choice = build_choice(ArtifactFamily::Spreadsheet, StageKind::Probe);
        let plan = lower_inspect_and_extract_plan(&task, &choice).expect("plan");
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].action, REACTOR_SPREADSHEET_LOCATE_ACTION);
        assert_eq!(plan.steps[1].action, REACTOR_SPREADSHEET_INSPECT_ACTION);
    }

    #[test]
    fn test_spreadsheet_verify_plan_uses_summary_template() {
        let mut task = Task::new(Intent::new("docs 目录下有一个excel 里面内容说了什么"));
        task.working_set_snapshot.insert(
            "inspection".to_string(),
            serde_json::json!({"sheets":[{"name":"Sheet1"}]}),
        );
        task.working_set_snapshot.insert(
            "extracted_result".to_string(),
            serde_json::json!({"overview":"Sheet1 has a header row"}),
        );
        task.working_set_snapshot.insert(
            "summary".to_string(),
            Value::String("Sheet1 有一行表头。".to_string()),
        );
        let choice = build_choice(ArtifactFamily::Spreadsheet, StageKind::Verify);
        let plan = lower_inspect_and_extract_plan(&task, &choice).expect("plan");
        assert_eq!(plan.on_complete.as_deref(), Some("{{summary}}"));
    }
}
