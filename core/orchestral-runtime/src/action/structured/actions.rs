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
        "structured_locate" => Box::new(StructuredLocateAction::from_spec(spec)),
        "structured_inspect" => Box::new(StructuredInspectAction::from_spec(spec)),
        "structured_derive_candidates" => {
            Box::new(StructuredDeriveCandidatesAction::from_spec(spec))
        }
        "structured_assess_readiness" => Box::new(StructuredAssessReadinessAction::from_spec(spec)),
        "structured_build_patch_spec" => Box::new(StructuredBuildPatchSpecAction::from_spec(spec)),
        "structured_apply_patch" => Box::new(StructuredApplyPatchAction::from_spec(spec)),
        "structured_verify_patch" => Box::new(StructuredVerifyPatchAction::from_spec(spec)),
        _ => return Ok(None),
    };
    Ok(Some(action))
}

#[derive(Debug)]
struct StructuredLocateAction {
    name: String,
    description: String,
}

impl StructuredLocateAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Locate structured artifact files"),
        }
    }
}

#[async_trait]
impl Action for StructuredLocateAction {
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
            .with_output_kinds(["structured.location", "structured.source_paths"])
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
                    "source_paths": { "type": "array", "items": { "type": "string" } },
                    "artifact_candidates": { "type": "array", "items": { "type": "string" } },
                    "artifact_count": { "type": "integer" }
                },
                "required": ["source_paths", "artifact_candidates", "artifact_count"]
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
        match locate_structured_files(std::path::Path::new(source_root), user_request) {
            Ok(exports) => ActionResult::success_with(exports),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct StructuredInspectAction {
    name: String,
    description: String,
}

impl StructuredInspectAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Inspect structured artifact files"),
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
            .with_input_kinds(["structured.location", "structured.source_paths"])
            .with_output_kinds(["structured.inspection"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_paths": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["source_paths"]
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
        let Some(paths) = input.params.get("source_paths").and_then(Value::as_array) else {
            return ActionResult::error("Missing source_paths for structured_inspect");
        };
        let source_paths = paths
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        match inspect_structured_files(&source_paths) {
            Ok(inspection) => ActionResult::success_with_one("inspection", inspection),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct StructuredDeriveCandidatesAction {
    name: String,
    description: String,
}

impl StructuredDeriveCandidatesAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Derive structured patch candidates"),
        }
    }
}

#[async_trait]
impl Action for StructuredDeriveCandidatesAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("structured")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["structured.inspection", "intent.request"])
            .with_output_kinds(["structured.patch_candidates"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "user_request": { "type": "string" },
                    "inspection": { "type": "object" },
                    "derivation_policy": {
                        "type": "string",
                        "enum": ["strict", "permissive"],
                        "description": "Strict waits when unknowns remain. Permissive allows partial concrete operations."
                    }
                },
                "required": ["user_request", "inspection", "derivation_policy"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "patch_candidates": { "type": "object" },
                    "summary": { "type": "string" }
                },
                "required": ["patch_candidates", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(user_request) = input.params.get("user_request").and_then(Value::as_str) else {
            return ActionResult::error("Missing user_request for structured_derive_candidates");
        };
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for structured_derive_candidates");
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error(
                "Missing derivation_policy for structured_derive_candidates",
            );
        };
        match derive_structured_patch_candidates(user_request, inspection, raw_policy) {
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
struct StructuredAssessReadinessAction {
    name: String,
    description: String,
}

impl StructuredAssessReadinessAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Assess structured probe readiness"),
        }
    }
}

#[async_trait]
impl Action for StructuredAssessReadinessAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("structured")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["structured.inspection", "structured.patch_candidates"])
            .with_output_kinds(["structured.continuation"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "patch_candidates": { "type": "object" },
                    "derivation_policy": {
                        "type": "string",
                        "enum": ["strict", "permissive"],
                        "description": "Strict waits when unknowns remain. Permissive allows commit-ready continuation when concrete operations exist."
                    }
                },
                "required": ["inspection", "patch_candidates", "derivation_policy"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "continuation": {
                        "type": "object",
                        "properties": {
                            "status": { "type": "string" },
                            "reason": { "type": "string" },
                            "unknowns": { "type": "array", "items": { "type": "string" } },
                            "assumptions": { "type": "array", "items": { "type": "string" } },
                            "user_message": { "type": ["string", "null"] }
                        }
                    },
                    "summary": { "type": "string" }
                },
                "required": ["continuation", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for structured_assess_readiness");
        };
        let Some(patch_candidates) = input.params.get("patch_candidates") else {
            return ActionResult::error("Missing patch_candidates for structured_assess_readiness");
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error(
                "Missing derivation_policy for structured_assess_readiness",
            );
        };
        match assess_structured_readiness(inspection, patch_candidates, raw_policy) {
            Ok((continuation, summary)) => ActionResult::success_with(
                [
                    ("continuation".to_string(), continuation),
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
struct StructuredBuildPatchSpecAction {
    name: String,
    description: String,
}

impl StructuredBuildPatchSpecAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Build a structured patch spec"),
        }
    }
}

#[async_trait]
impl Action for StructuredBuildPatchSpecAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("structured")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["structured.patch_candidates"])
            .with_output_kinds(["structured.patch_spec"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_candidates": { "type": "object" }
                },
                "required": ["patch_candidates"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": {
                        "type": "object",
                        "properties": {
                            "files": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "path": { "type": "string" },
                                        "operations": {
                                            "type": "array",
                                            "items": { "type": "object" }
                                        }
                                    },
                                    "required": ["path", "operations"]
                                }
                            },
                            "summary": { "type": "string" }
                        },
                        "required": ["files"]
                    },
                    "summary": { "type": "string" }
                },
                "required": ["patch_spec", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(patch_candidates) = input.params.get("patch_candidates") else {
            return ActionResult::error("Missing patch_candidates for structured_build_patch_spec");
        };
        match build_structured_patch_spec(patch_candidates) {
            Ok((patch_spec, summary)) => ActionResult::success_with(
                [
                    ("patch_spec".to_string(), patch_spec),
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
struct StructuredApplyPatchAction {
    name: String,
    description: String,
}

impl StructuredApplyPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Apply a structured patch"),
        }
    }
}

#[async_trait]
impl Action for StructuredApplyPatchAction {
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
            .with_input_kinds(["structured.patch_spec"])
            .with_output_kinds(["structured.apply_result", "workspace.path"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": {
                        "type": "object",
                        "description": "Structured patch spec. Use files[].operations from structured_build_patch_spec output.",
                        "properties": {
                            "files": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "path": { "type": "string" },
                                        "operations": {
                                            "type": "array",
                                            "items": { "type": "object" }
                                        }
                                    },
                                    "required": ["path", "operations"]
                                }
                            },
                            "summary": { "type": "string" }
                        },
                        "required": ["files"]
                    }
                },
                "required": ["patch_spec"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "updated_paths": { "type": "array", "items": { "type": "string" } },
                    "patch_count": { "type": "integer" },
                    "summary": { "type": "string" }
                },
                "required": ["updated_paths", "patch_count", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(patch_spec) = input.params.get("patch_spec") else {
            return ActionResult::error("Missing patch_spec for structured_apply_patch");
        };
        match apply_structured_patch(patch_spec) {
            Ok(exports) => ActionResult::success_with(exports),
            Err(error) => ActionResult::error(error),
        }
    }
}

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
                    "patch_spec": {
                        "type": "object",
                        "description": "Structured patch spec. Use files[].operations from structured_build_patch_spec output.",
                        "properties": {
                            "files": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "path": { "type": "string" },
                                        "operations": {
                                            "type": "array",
                                            "items": { "type": "object" }
                                        }
                                    },
                                    "required": ["path", "operations"]
                                }
                            },
                            "summary": { "type": "string" }
                        },
                        "required": ["files"]
                    },
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
