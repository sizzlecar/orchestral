use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::super::factory::ActionBuildError;
use super::apply::apply_structured_patch;
use super::assess::assess_structured_readiness;
use super::inspect::inspect_structured_files;
use super::locate::locate_structured_files;
use super::verify::verify_structured_patch;

pub fn build_structured_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    let action: Box<dyn Action> = match spec.kind.as_str() {
        "structured_locate" => Box::new(StructuredLocateAction::from_spec(spec)),
        "structured_inspect" => Box::new(StructuredInspectAction::from_spec(spec)),
        "structured_assess_readiness" => Box::new(StructuredAssessReadinessAction::from_spec(spec)),
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
            .with_capabilities(["filesystem_read"])
            .with_roles(["collect", "inspect"])
            .with_input_kinds(["path", "text"])
            .with_output_kinds(["path", "structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_root": { "type": "string" },
                    "user_request": { "type": "string" }
                },
                "required": ["source_root", "user_request"]
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
            .with_capabilities(["filesystem_read"])
            .with_roles(["inspect", "verify"])
            .with_input_kinds(["path"])
            .with_output_kinds(["structured"])
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
            .with_capabilities(["pure", "structured_output"])
            .with_roles(["verify", "control"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "patch_candidates": { "type": "object" },
                    "derivation_policy": { "type": "string" }
                },
                "required": ["inspection", "patch_candidates", "derivation_policy"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "continuation": { "type": "object" },
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
            .with_capabilities(["filesystem_read", "filesystem_write", "side_effect"])
            .with_roles(["apply", "execute"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["path", "structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": { "type": "object" }
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
            .with_capabilities(["filesystem_read"])
            .with_roles(["verify"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
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
