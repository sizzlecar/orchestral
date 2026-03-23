use std::path::Path;

use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::super::factory::ActionBuildError;
use super::apply::apply_patch;
use super::assess::assess_readiness;
use super::derive::derive_spreadsheet_patch_candidates;
use super::inspect::inspect_workbook;
use super::locate::locate_workbook;
use super::verify::verify_patch;
use crate::action::test_hooks::forced_verify_failure;

pub fn build_spreadsheet_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    let action: Box<dyn Action> = match spec.kind.as_str() {
        "spreadsheet_locate" => Box::new(SpreadsheetLocateAction::from_spec(spec)),
        "spreadsheet_inspect" => Box::new(SpreadsheetInspectAction::from_spec(spec)),
        "spreadsheet_derive_candidates" => {
            Box::new(SpreadsheetDeriveCandidatesAction::from_spec(spec))
        }
        "spreadsheet_assess_readiness" => {
            Box::new(SpreadsheetAssessReadinessAction::from_spec(spec))
        }
        "spreadsheet_apply_patch" => Box::new(SpreadsheetApplyPatchAction::from_spec(spec)),
        "spreadsheet_verify_patch" => Box::new(SpreadsheetVerifyPatchAction::from_spec(spec)),
        _ => return Ok(None),
    };
    Ok(Some(action))
}

#[derive(Debug)]
struct SpreadsheetLocateAction {
    name: String,
    description: String,
}

impl SpreadsheetLocateAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Locate a spreadsheet artifact"),
        }
    }
}

#[async_trait]
impl Action for SpreadsheetLocateAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("spreadsheet")
            .with_capabilities(["filesystem_read"])
            .with_input_kinds(["workspace.path", "intent.request"])
            .with_output_kinds(["spreadsheet.location"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_root": { "type": "string" },
                    "user_request": { "type": "string" }
                }
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": { "type": "string" },
                    "artifact_candidates": { "type": "array", "items": { "type": "string" } },
                    "artifact_count": { "type": "integer" }
                },
                "required": ["source_path", "artifact_candidates", "artifact_count"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let source_root = input
            .params
            .get("source_root")
            .and_then(Value::as_str)
            .unwrap_or(".");
        let user_request = input
            .params
            .get("user_request")
            .and_then(Value::as_str)
            .unwrap_or("");
        match locate_workbook(Path::new(source_root), user_request) {
            Ok(exports) => ActionResult::success_with(exports),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct SpreadsheetInspectAction {
    name: String,
    description: String,
}

impl SpreadsheetInspectAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Inspect spreadsheet structure"),
        }
    }
}

#[async_trait]
impl Action for SpreadsheetInspectAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("spreadsheet")
            .with_capabilities(["filesystem_read"])
            .with_input_kinds(["spreadsheet.location"])
            .with_output_kinds(["spreadsheet.inspection"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" }
                },
                "required": ["inspection"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(path) = input.params.get("path").and_then(Value::as_str) else {
            return ActionResult::error("Missing path for spreadsheet_inspect");
        };
        match inspect_workbook(Path::new(path)) {
            Ok(inspection) => ActionResult::success_with_one("inspection", inspection),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct SpreadsheetDeriveCandidatesAction {
    name: String,
    description: String,
}

impl SpreadsheetDeriveCandidatesAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Derive spreadsheet patch candidates"),
        }
    }
}

#[async_trait]
impl Action for SpreadsheetDeriveCandidatesAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("spreadsheet")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["spreadsheet.inspection", "intent.request"])
            .with_output_kinds(["spreadsheet.patch_candidates"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "user_request": { "type": "string" },
                    "inspection": { "type": "object" },
                    "derivation_policy": {
                        "type": "string",
                        "enum": ["strict", "permissive"],
                        "description": "Strict keeps placeholder-sensitive review. Permissive allows generic workbook autofill heuristics."
                    }
                },
                "required": ["user_request", "inspection", "derivation_policy"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "patch_candidates": {
                        "type": "object",
                        "properties": {
                            "candidates": {
                                "type": "object",
                                "properties": {
                                    "cells": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "cell": { "type": "string" },
                                                "header": { "type": "string" },
                                                "proposed_action": { "type": "string" },
                                                "proposed_value": {}
                                            },
                                            "required": ["cell", "proposed_action", "proposed_value"]
                                        }
                                    }
                                }
                            },
                            "unknowns": { "type": "array", "items": { "type": "string" } },
                            "assumptions": { "type": "array", "items": { "type": "string" } }
                        }
                    },
                    "summary": { "type": "string" }
                },
                "required": ["patch_candidates", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(user_request) = input.params.get("user_request").and_then(Value::as_str) else {
            return ActionResult::error("Missing user_request for spreadsheet_derive_candidates");
        };
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for spreadsheet_derive_candidates");
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error(
                "Missing derivation_policy for spreadsheet_derive_candidates",
            );
        };
        match derive_spreadsheet_patch_candidates(user_request, inspection, raw_policy) {
            Ok((patch_candidates, summary)) => ActionResult::success_with(
                [
                    ("patch_candidates".to_string(), patch_candidates),
                    ("summary".to_string(), Value::String(summary)),
                ]
                .into_iter()
                .collect(),
            ),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct SpreadsheetAssessReadinessAction {
    name: String,
    description: String,
}

impl SpreadsheetAssessReadinessAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Assess spreadsheet probe readiness"),
        }
    }
}

#[async_trait]
impl Action for SpreadsheetAssessReadinessAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("spreadsheet")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["spreadsheet.inspection", "spreadsheet.patch_candidates"])
            .with_output_kinds(["spreadsheet.continuation", "spreadsheet.patch_spec"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "patch_candidates": {},
                    "derivation_policy": {
                        "type": "string",
                        "enum": ["strict", "permissive"],
                        "description": "Strict requires complete non-placeholder coverage. Permissive allows commit-ready continuation when concrete fills exist."
                    }
                },
                "required": ["inspection", "patch_candidates", "derivation_policy"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": {
                        "type": "object",
                        "properties": {
                            "fills": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "cell": { "type": "string" },
                                        "value": {}
                                    },
                                    "required": ["cell", "value"]
                                }
                            }
                        }
                    },
                    "continuation": {
                        "type": "object",
                        "description": "Continuation decision. When status is commit_ready, this object also carries patch_spec-compatible fills[] and patch_spec.fills[].",
                        "properties": {
                            "status": { "type": "string" },
                            "reason": { "type": "string" },
                            "unknowns": { "type": "array", "items": { "type": "string" } },
                            "assumptions": { "type": "array", "items": { "type": "string" } },
                            "user_message": { "type": ["string", "null"] },
                            "fills": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "cell": { "type": "string" },
                                        "value": {}
                                    },
                                    "required": ["cell", "value"]
                                }
                            },
                            "patch_spec": {
                                "type": "object",
                                "properties": {
                                    "fills": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "cell": { "type": "string" },
                                                "value": {}
                                            },
                                            "required": ["cell", "value"]
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "summary": { "type": "string" }
                },
                "required": ["continuation", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for spreadsheet_assess_readiness");
        };
        let Some(patch_candidates) = input.params.get("patch_candidates") else {
            return ActionResult::error(
                "Missing patch_candidates for spreadsheet_assess_readiness",
            );
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error(
                "Missing derivation_policy for spreadsheet_assess_readiness",
            );
        };
        match assess_readiness(inspection, patch_candidates, raw_policy) {
            Ok((continuation, summary)) => {
                let patch_spec = continuation.get("patch_spec").cloned();
                let mut exports = [
                    ("continuation".to_string(), continuation),
                    ("summary".to_string(), Value::String(summary)),
                ]
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>();
                if let Some(patch_spec) = patch_spec {
                    exports.insert("patch_spec".to_string(), patch_spec);
                }
                ActionResult::success_with(exports)
            }
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct SpreadsheetApplyPatchAction {
    name: String,
    description: String,
}

impl SpreadsheetApplyPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Apply a spreadsheet patch"),
        }
    }
}

#[async_trait]
impl Action for SpreadsheetApplyPatchAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("spreadsheet")
            .with_capabilities(["filesystem_read", "filesystem_write", "side_effect"])
            .with_input_kinds(["spreadsheet.location", "spreadsheet.patch_spec"])
            .with_output_kinds(["spreadsheet.apply_result", "workspace.path"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "patch_spec": {
                        "type": "object",
                        "description": "Spreadsheet patch spec. Use fills[] with cell/value pairs.",
                        "properties": {
                            "fills": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "cell": { "type": "string" },
                                        "value": {}
                                    },
                                    "required": ["cell", "value"]
                                }
                            }
                        },
                        "required": ["fills"]
                    }
                },
                "required": ["path", "patch_spec"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "updated_file_path": { "type": "string" },
                    "patch_count": { "type": "integer" },
                    "cells_filled": { "type": "integer" },
                    "result": { "type": "string" },
                    "summary": { "type": "string" }
                },
                "required": ["updated_file_path", "patch_count", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(path) = input.params.get("path").and_then(Value::as_str) else {
            return ActionResult::error("Missing path for spreadsheet_apply_patch");
        };
        let Some(patch_spec) = input.params.get("patch_spec") else {
            return ActionResult::error("Missing patch_spec for spreadsheet_apply_patch");
        };
        match apply_patch(Path::new(path), patch_spec) {
            Ok((updated_file_path, patch_count)) => {
                let result = json!({
                    "updated_file_path": updated_file_path,
                    "patch_count": patch_count,
                    "cells_filled": patch_count,
                });
                ActionResult::success_with(
                    [
                        (
                            "updated_file_path".to_string(),
                            result
                                .get("updated_file_path")
                                .cloned()
                                .unwrap_or(Value::Null),
                        ),
                        ("patch_count".to_string(), json!(patch_count)),
                        ("cells_filled".to_string(), json!(patch_count)),
                        ("result".to_string(), Value::String(result.to_string())),
                        (
                            "summary".to_string(),
                            Value::String(format!("Applied {} spreadsheet fills.", patch_count)),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                )
            }
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct SpreadsheetVerifyPatchAction {
    name: String,
    description: String,
    default_validator_path: Option<String>,
}

impl SpreadsheetVerifyPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Verify a spreadsheet patch"),
            default_validator_path: spec.get_config::<String>("default_validator_path"),
        }
    }
}

#[async_trait]
impl Action for SpreadsheetVerifyPatchAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("spreadsheet")
            .with_capabilities(["filesystem_read"])
            .with_input_kinds(["spreadsheet.location", "spreadsheet.patch_spec"])
            .with_output_kinds(["spreadsheet.verify_decision"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "patch_spec": {
                        "type": "object",
                        "description": "Spreadsheet patch spec. Use fills[] with cell/value pairs.",
                        "properties": {
                            "fills": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "cell": { "type": "string" },
                                        "value": {}
                                    },
                                    "required": ["cell", "value"]
                                }
                            }
                        },
                        "required": ["fills"]
                    },
                    "validator_path": { "type": "string" }
                },
                "required": ["path"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "verify_decision": { "type": "object" },
                    "summary": { "type": "string" }
                },
                "required": ["verify_decision", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        if let Some(verify_decision) = forced_verify_failure(self.name()) {
            let summary = format!("Workbook verification failed: {}", verify_decision.reason);
            return ActionResult::success_with(
                [
                    (
                        "verify_decision".to_string(),
                        serde_json::to_value(verify_decision).unwrap_or(Value::Null),
                    ),
                    ("summary".to_string(), Value::String(summary)),
                ]
                .into_iter()
                .collect(),
            );
        }
        let Some(path) = input.params.get("path").and_then(Value::as_str) else {
            return ActionResult::error("Missing path for spreadsheet_verify_patch");
        };
        let patch_spec = input.params.get("patch_spec");
        let validator_path = input
            .params
            .get("validator_path")
            .and_then(Value::as_str)
            .or(self.default_validator_path.as_deref());
        match verify_patch(Path::new(path), patch_spec, validator_path) {
            Ok(decision) => {
                let status = decision
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("failed");
                let summary = if status == "passed" {
                    "Workbook verified.".to_string()
                } else {
                    format!(
                        "Workbook verification failed: {}",
                        decision
                            .get("reason")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown reason")
                    )
                };
                ActionResult::success_with(
                    [
                        ("verify_decision".to_string(), decision),
                        ("summary".to_string(), Value::String(summary)),
                    ]
                    .into_iter()
                    .collect(),
                )
            }
            Err(error) => ActionResult::error(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use orchestral_core::store::WorkingSet;
    use tokio::sync::RwLock;

    use super::*;

    fn test_ctx() -> ActionContext {
        ActionContext::new(
            "task-1",
            "s1",
            "exec-1",
            Arc::new(RwLock::new(WorkingSet::new())),
        )
    }

    #[tokio::test]
    async fn test_spreadsheet_assess_action_exports_top_level_patch_spec() {
        let action = SpreadsheetAssessReadinessAction {
            name: "spreadsheet_assess_readiness".to_string(),
            description: "Assess spreadsheet probe readiness".to_string(),
        };
        let result = action
            .run(
                ActionInput::with_params(json!({
                    "derivation_policy": "permissive",
                    "inspection": {
                        "selected_region": {
                            "patchable_cells": [
                                { "cell": "F5" },
                                { "cell": "H5" }
                            ]
                        }
                    },
                    "patch_candidates": {
                        "candidates": {
                            "cells": [
                                {
                                    "cell": "F5",
                                    "proposed_action": "fill",
                                    "proposed_value": "按计划推进工作业绩相关工作，达成既定目标。"
                                },
                                {
                                    "cell": "H5",
                                    "proposed_action": "fill",
                                    "proposed_value": 90
                                }
                            ]
                        }
                    }
                })),
                test_ctx(),
            )
            .await;

        match result {
            ActionResult::Success { exports } => {
                let patch_spec = exports.get("patch_spec").expect("patch_spec export");
                assert_eq!(
                    patch_spec["fills"].as_array().map(|items| items.len()),
                    Some(2)
                );
                assert_eq!(
                    exports["continuation"]["patch_spec"]["fills"]
                        .as_array()
                        .map(|items| items.len()),
                    Some(2)
                );
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
