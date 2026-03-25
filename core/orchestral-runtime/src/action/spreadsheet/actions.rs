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
        "spreadsheet_inspect" => Box::new(SpreadsheetInspectAction::from_spec(spec)),
        "spreadsheet_patch" => Box::new(SpreadsheetPatchAction::from_spec(spec)),
        "spreadsheet_verify_patch" => Box::new(SpreadsheetVerifyPatchAction::from_spec(spec)),
        _ => return Ok(None),
    };
    Ok(Some(action))
}

// ---------------------------------------------------------------------------
// spreadsheet_inspect  (locate + inspect)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SpreadsheetInspectAction {
    name: String,
    description: String,
}

impl SpreadsheetInspectAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Locate and inspect spreadsheet structure"),
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
            .with_input_kinds(["workspace.path", "intent.request"])
            .with_output_kinds(["spreadsheet.inspection", "spreadsheet.location"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_root": { "type": "string", "description": "Directory to scan for spreadsheets" },
                    "user_request": { "type": "string" },
                    "path": { "type": "string", "description": "Direct path to a spreadsheet file" }
                }
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "source_path": { "type": "string" },
                    "path": { "type": "string" },
                    "artifact_candidates": { "type": "array", "items": { "type": "string" } },
                    "artifact_count": { "type": "integer" }
                },
                "required": ["inspection", "source_path"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        // Resolve path: direct path takes priority, otherwise locate from source_root.
        let (path, locate_exports) =
            if let Some(direct) = input.params.get("path").and_then(Value::as_str) {
                (direct.to_string(), None)
            } else {
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
                    Ok(exports) => {
                        let p = exports
                            .get("source_path")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        (p, Some(exports))
                    }
                    Err(error) => return ActionResult::error(error),
                }
            };

        if path.is_empty() {
            return ActionResult::error("No spreadsheet path found. Provide path or source_root.");
        }

        let inspection = match inspect_workbook(Path::new(&path)) {
            Ok(v) => v,
            Err(error) => return ActionResult::error(error),
        };

        let mut exports = locate_exports.unwrap_or_default();
        if !exports.contains_key("source_path") {
            exports.insert("source_path".to_string(), Value::String(path.clone()));
            exports.insert("path".to_string(), Value::String(path.clone()));
            exports.insert("artifact_candidates".to_string(), json!([path]));
            exports.insert("artifact_count".to_string(), json!(1));
        }
        exports.insert("inspection".to_string(), inspection);

        ActionResult::success_with(exports)
    }
}

// ---------------------------------------------------------------------------
// spreadsheet_patch  (derive + assess + apply)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SpreadsheetPatchAction {
    name: String,
    description: String,
}

impl SpreadsheetPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Derive, assess, and apply a spreadsheet patch"),
        }
    }
}

#[async_trait]
impl Action for SpreadsheetPatchAction {
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
            .with_input_kinds([
                "spreadsheet.inspection",
                "spreadsheet.location",
                "intent.request",
            ])
            .with_output_kinds([
                "spreadsheet.apply_result",
                "spreadsheet.patch_spec",
                "workspace.path",
            ])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "inspection": { "type": "object" },
                    "user_request": { "type": "string" },
                    "derivation_policy": {
                        "type": "string",
                        "enum": ["strict", "permissive"]
                    }
                },
                "required": ["path", "inspection", "user_request", "derivation_policy"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "updated_file_path": { "type": "string" },
                    "patch_count": { "type": "integer" },
                    "cells_filled": { "type": "integer" },
                    "patch_spec": { "type": "object" },
                    "summary": { "type": "string" }
                },
                "required": ["updated_file_path", "patch_count", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(path) = input.params.get("path").and_then(Value::as_str) else {
            return ActionResult::error("Missing path for spreadsheet_patch");
        };
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for spreadsheet_patch");
        };
        let Some(user_request) = input.params.get("user_request").and_then(Value::as_str) else {
            return ActionResult::error("Missing user_request for spreadsheet_patch");
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error("Missing derivation_policy for spreadsheet_patch");
        };

        // Step 1: derive candidates
        let (patch_candidates, _derive_summary) =
            match derive_spreadsheet_patch_candidates(user_request, inspection, raw_policy) {
                Ok(v) => v,
                Err(error) => return ActionResult::error(error),
            };

        // Step 2: assess readiness
        let (continuation, _assess_summary) =
            match assess_readiness(inspection, &patch_candidates, raw_policy) {
                Ok(v) => v,
                Err(error) => return ActionResult::error(error),
            };

        let status = continuation
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("failed");

        match status {
            "wait_user" => {
                let message = continuation
                    .get("user_message")
                    .and_then(Value::as_str)
                    .or_else(|| continuation.get("reason").and_then(Value::as_str))
                    .unwrap_or("Need user input before proceeding")
                    .to_string();
                return ActionResult::need_clarification(message);
            }
            "done" => {
                let reason = continuation
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("No changes needed");
                return ActionResult::success_with(
                    [
                        (
                            "updated_file_path".to_string(),
                            Value::String(path.to_string()),
                        ),
                        ("patch_count".to_string(), json!(0)),
                        ("cells_filled".to_string(), json!(0)),
                        ("summary".to_string(), Value::String(reason.to_string())),
                    ]
                    .into_iter()
                    .collect(),
                );
            }
            "commit_ready" => { /* continue to apply */ }
            other => {
                return ActionResult::error(format!(
                    "Unexpected continuation status from assess: {}",
                    other
                ));
            }
        }

        // Step 3: extract patch_spec from assess continuation (already built by assess)
        let patch_spec = continuation.get("patch_spec").cloned().unwrap_or_else(|| {
            // Fallback: build from fills in continuation
            let fills = continuation.get("fills").cloned().unwrap_or(json!([]));
            json!({ "fills": fills })
        });

        // Step 4: apply patch
        match apply_patch(Path::new(path), &patch_spec) {
            Ok((updated_file_path, patch_count)) => ActionResult::success_with(
                [
                    (
                        "updated_file_path".to_string(),
                        Value::String(updated_file_path),
                    ),
                    ("patch_count".to_string(), json!(patch_count)),
                    ("cells_filled".to_string(), json!(patch_count)),
                    ("patch_spec".to_string(), patch_spec),
                    (
                        "summary".to_string(),
                        Value::String(format!("Applied {} spreadsheet fills.", patch_count)),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
            Err(error) => ActionResult::error(error),
        }
    }
}

// ---------------------------------------------------------------------------
// spreadsheet_verify_patch  (unchanged)
// ---------------------------------------------------------------------------

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
                    "patch_spec": { "type": "object" },
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
