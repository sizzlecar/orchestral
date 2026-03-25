use std::path::Path;

use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::super::factory::ActionBuildError;
use super::apply::apply_structured_patch;
use super::assess::assess_structured_readiness;
use super::commit::build_structured_patch_spec;
use super::derive::derive_structured_patch_candidates;
use super::inspect::inspect_structured_files;
use super::locate::locate_structured_files;
use super::verify::verify_structured_patch;
use crate::action::test_hooks::forced_verify_failure;

pub fn build_structured_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    let action: Box<dyn Action> = match spec.kind.as_str() {
        "structured_inspect" => Box::new(StructuredInspectAction::from_spec(spec)),
        "structured_patch" => Box::new(StructuredPatchAction::from_spec(spec)),
        "structured_verify_patch" => Box::new(StructuredVerifyPatchAction::from_spec(spec)),
        _ => return Ok(None),
    };
    Ok(Some(action))
}

// ---------------------------------------------------------------------------
// structured_inspect  (locate + inspect)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StructuredInspectAction {
    name: String,
    description: String,
}

impl StructuredInspectAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Locate and inspect structured artifact files"),
        }
    }
}

#[async_trait]
impl Action for StructuredInspectAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("structured")
            .with_capabilities(["filesystem_read"])
            .with_input_kinds(["workspace.path", "intent.request"])
            .with_output_kinds(["structured.inspection", "structured.source_paths"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_root": { "type": "string", "description": "Directory to scan for structured files" },
                    "user_request": { "type": "string" },
                    "source_paths": { "type": "array", "items": { "type": "string" } }
                }
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "source_paths": { "type": "array", "items": { "type": "string" } },
                    "artifact_candidates": { "type": "array", "items": { "type": "string" } },
                    "artifact_count": { "type": "integer" }
                },
                "required": ["inspection", "source_paths"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        // If explicit source_paths given, use them. Otherwise locate first.
        let (source_paths, locate_exports) =
            if let Some(paths) = input.params.get("source_paths").and_then(Value::as_array) {
                let paths: Vec<String> = paths
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
                (paths, None)
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
                match locate_structured_files(Path::new(source_root), user_request) {
                    Ok(exports) => {
                        let paths = exports
                            .get("source_paths")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(String::from))
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        (paths, Some(exports))
                    }
                    Err(error) => return ActionResult::error(error),
                }
            };

        if source_paths.is_empty() {
            return ActionResult::error(
                "No structured source paths found. Provide source_root or source_paths.",
            );
        }

        let inspection = match inspect_structured_files(&source_paths) {
            Ok(v) => v,
            Err(error) => return ActionResult::error(error),
        };

        let mut exports = locate_exports.unwrap_or_default();
        if !exports.contains_key("source_paths") {
            let paths_val: Vec<Value> = source_paths
                .iter()
                .map(|s| Value::String(s.clone()))
                .collect();
            exports.insert("source_paths".to_string(), Value::Array(paths_val.clone()));
            exports.insert(
                "artifact_candidates".to_string(),
                Value::Array(paths_val.clone()),
            );
            exports.insert(
                "artifact_count".to_string(),
                Value::Number(paths_val.len().into()),
            );
        }
        exports.insert("inspection".to_string(), inspection);

        ActionResult::success_with(exports)
    }
}

// ---------------------------------------------------------------------------
// structured_patch  (derive + assess + build_patch_spec + apply)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StructuredPatchAction {
    name: String,
    description: String,
}

impl StructuredPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Derive, assess, and apply a structured patch"),
        }
    }
}

#[async_trait]
impl Action for StructuredPatchAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("structured")
            .with_capabilities(["filesystem_read", "filesystem_write", "side_effect"])
            .with_input_kinds(["structured.inspection", "intent.request"])
            .with_output_kinds([
                "structured.apply_result",
                "structured.patch_spec",
                "workspace.path",
            ])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "user_request": { "type": "string" },
                    "derivation_policy": {
                        "type": "string",
                        "enum": ["strict", "permissive"]
                    }
                },
                "required": ["inspection", "user_request", "derivation_policy"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "updated_paths": { "type": "array", "items": { "type": "string" } },
                    "patch_count": { "type": "integer" },
                    "patch_spec": { "type": "object" },
                    "summary": { "type": "string" }
                },
                "required": ["updated_paths", "patch_count", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for structured_patch");
        };
        let Some(user_request) = input.params.get("user_request").and_then(Value::as_str) else {
            return ActionResult::error("Missing user_request for structured_patch");
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error("Missing derivation_policy for structured_patch");
        };

        // Step 1: derive candidates
        let (patch_candidates, _derive_summary) =
            match derive_structured_patch_candidates(user_request, inspection, raw_policy) {
                Ok(v) => v,
                Err(error) => return ActionResult::error(error),
            };

        // Step 2: assess readiness
        let (continuation, _assess_summary) =
            match assess_structured_readiness(inspection, &patch_candidates, raw_policy) {
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
                        ("updated_paths".to_string(), Value::Array(Vec::new())),
                        ("patch_count".to_string(), json!(0)),
                        ("summary".to_string(), Value::String(reason.to_string())),
                    ]
                    .into_iter()
                    .collect(),
                );
            }
            "commit_ready" => { /* continue to build + apply */ }
            other => {
                return ActionResult::error(format!(
                    "Unexpected continuation status from assess: {}",
                    other
                ));
            }
        }

        // Step 3: build patch spec
        let (patch_spec, _build_summary) = match build_structured_patch_spec(&patch_candidates) {
            Ok(v) => v,
            Err(error) => return ActionResult::error(error),
        };

        // Step 4: apply patch
        let mut exports = match apply_structured_patch(&patch_spec) {
            Ok(v) => v,
            Err(error) => return ActionResult::error(error),
        };

        exports.insert("patch_spec".to_string(), patch_spec);

        ActionResult::success_with(exports)
    }
}

// ---------------------------------------------------------------------------
// structured_verify_patch  (unchanged)
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StructuredVerifyPatchAction {
    name: String,
    description: String,
}

impl StructuredVerifyPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Verify a structured patch"),
        }
    }
}

#[async_trait]
impl Action for StructuredVerifyPatchAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("structured")
            .with_capabilities(["filesystem_read"])
            .with_input_kinds(["structured.inspection", "structured.patch_spec"])
            .with_output_kinds(["structured.verify_decision"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": { "type": "object" },
                    "inspection": { "type": "object" }
                },
                "required": ["patch_spec"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "summary": { "type": "string" },
                    "verify_decision": { "type": "object" }
                },
                "required": ["summary", "verify_decision"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        if let Some(verify_decision) = forced_verify_failure(self.name()) {
            return ActionResult::success_with(
                [
                    (
                        "verify_decision".to_string(),
                        serde_json::to_value(verify_decision).unwrap_or(Value::Null),
                    ),
                    (
                        "summary".to_string(),
                        Value::String("Structured verification failed.".to_string()),
                    ),
                ]
                .into_iter()
                .collect(),
            );
        }
        let Some(patch_spec) = input.params.get("patch_spec") else {
            return ActionResult::error("Missing patch_spec for structured_verify_patch");
        };
        let inspection = input.params.get("inspection");
        match verify_structured_patch(patch_spec, inspection) {
            Ok((verify_decision, summary)) => ActionResult::success_with(
                [
                    (
                        "verify_decision".to_string(),
                        serde_json::to_value(verify_decision).unwrap_or(Value::Null),
                    ),
                    ("summary".to_string(), Value::String(summary)),
                ]
                .into_iter()
                .collect(),
            ),
            Err(error) => ActionResult::error(error),
        }
    }
}
