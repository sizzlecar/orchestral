use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::super::factory::ActionBuildError;
use super::apply::apply_document_patch;
use super::assess::assess_document_readiness;
use super::commit::build_document_patch_spec;
use super::derive::derive_document_patch_candidates;
use super::inspect::inspect_documents;
use super::locate::locate_documents;
use super::support::{collect_markdown_files, dedup_and_sort_paths, display_path, normalize_path};
use super::verify::verify_document_patch;
use crate::action::test_hooks::forced_verify_failure;

pub fn build_document_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    let action: Box<dyn Action> = match spec.kind.as_str() {
        "document_locate" => Box::new(DocumentLocateAction::from_spec(spec)),
        "document_inspect" => Box::new(DocumentInspectAction::from_spec(spec)),
        "document_derive_candidates" => Box::new(DocumentDeriveCandidatesAction::from_spec(spec)),
        "document_build_patch_spec" => Box::new(DocumentBuildPatchSpecAction::from_spec(spec)),
        "document_assess_readiness" => Box::new(DocumentAssessReadinessAction::from_spec(spec)),
        "document_apply_patch" => Box::new(DocumentApplyPatchAction::from_spec(spec)),
        "document_verify_patch" => Box::new(DocumentVerifyPatchAction::from_spec(spec)),
        _ => return Ok(None),
    };
    Ok(Some(action))
}

#[derive(Debug)]
struct DocumentLocateAction {
    name: String,
    description: String,
}

impl DocumentLocateAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Locate document artifacts"),
        }
    }
}

#[async_trait]
impl Action for DocumentLocateAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("document")
            .with_capabilities(["filesystem_read"])
            .with_roles(["collect", "inspect"])
            .with_input_kinds(["path", "text"])
            .with_output_kinds(["path", "structured"])
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
                    "source_paths": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "documents": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "artifact_candidates": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "artifact_count": { "type": "integer" },
                    "report_path": { "type": "string" }
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
        match locate_documents(Path::new(source_root), user_request) {
            Ok(exports) => ActionResult::success_with(exports),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct DocumentInspectAction {
    name: String,
    description: String,
}

impl DocumentInspectAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Inspect document structure"),
        }
    }
}

#[async_trait]
impl Action for DocumentInspectAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("document")
            .with_capabilities(["filesystem_read"])
            .with_roles(["inspect", "verify"])
            .with_input_kinds(["path", "structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_paths": {
                        "oneOf": [
                            { "type": "array", "items": { "type": "string" } },
                            { "type": "string" }
                        ]
                    },
                    "documents": {
                        "oneOf": [
                            { "type": "array", "items": { "type": "string" } },
                            { "type": "string" }
                        ]
                    },
                    "source_path": {
                        "oneOf": [
                            { "type": "string" },
                            { "type": "array", "items": { "type": "string" } }
                        ]
                    },
                    "path": {
                        "oneOf": [
                            { "type": "string" },
                            { "type": "array", "items": { "type": "string" } }
                        ]
                    },
                    "document": {
                        "oneOf": [
                            { "type": "string" },
                            { "type": "array", "items": { "type": "string" } }
                        ]
                    }
                }
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
        let (source_paths, source_field) = match resolve_document_source_paths(&input.params) {
            Ok(result) => result,
            Err(error) => return ActionResult::error(error),
        };
        tracing::debug!(
            action = %self.name,
            source_field,
            source_count = source_paths.len(),
            "document_inspect resolved source paths"
        );
        match inspect_documents(&source_paths) {
            Ok(inspection) => ActionResult::success_with_one("inspection", inspection),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct DocumentAssessReadinessAction {
    name: String,
    description: String,
}

impl DocumentAssessReadinessAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Assess document probe readiness"),
        }
    }
}

const DOCUMENT_SOURCE_FIELDS: [&str; 5] = [
    "source_paths",
    "documents",
    "source_path",
    "path",
    "document",
];

fn resolve_document_source_paths(params: &Value) -> Result<(Vec<String>, &'static str), String> {
    let Some(object) = params.as_object() else {
        return Err("document_inspect params must be an object".to_string());
    };

    for field in DOCUMENT_SOURCE_FIELDS {
        let Some(value) = object.get(field) else {
            continue;
        };
        let paths = coerce_document_path_inputs(value)
            .map_err(|error| format!("Invalid {} for document_inspect: {}", field, error))?;
        let source_paths = expand_document_source_paths(paths)?;
        if !source_paths.is_empty() {
            return Ok((source_paths, field));
        }
    }

    Err(format!(
        "Missing document source paths for document_inspect. Provide one of: {}",
        DOCUMENT_SOURCE_FIELDS.join(", ")
    ))
}

fn coerce_document_path_inputs(value: &Value) -> Result<Vec<String>, String> {
    match value {
        Value::Array(items) => {
            let mut paths = Vec::new();
            for item in items {
                let Some(path) = item.as_str() else {
                    return Err("array items must be strings".to_string());
                };
                let trimmed = path.trim();
                if !trimmed.is_empty() {
                    paths.push(trimmed.to_string());
                }
            }
            Ok(paths)
        }
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return Ok(Vec::new());
            }
            if trimmed.starts_with('[') {
                if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
                    return coerce_document_path_inputs(&parsed);
                }
            }
            Ok(vec![trimmed.to_string()])
        }
        Value::Null => Ok(Vec::new()),
        _ => Err("value must be a string or array of strings".to_string()),
    }
}

fn expand_document_source_paths(paths: Vec<String>) -> Result<Vec<String>, String> {
    let mut expanded = Vec::<PathBuf>::new();

    for raw_path in paths {
        let normalized = normalize_path(Path::new(&raw_path))?;
        if normalized.is_dir() {
            collect_markdown_files(&normalized, &mut expanded).map_err(|err| err.to_string())?;
        } else {
            expanded.push(normalized);
        }
    }

    dedup_and_sort_paths(&mut expanded);
    Ok(expanded
        .iter()
        .map(|path| display_path(path))
        .collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::*;
    use orchestral_core::store::WorkingSet;

    fn test_action_context() -> ActionContext {
        ActionContext::new(
            "task-1",
            "step-1",
            "exec-1",
            Arc::new(RwLock::new(WorkingSet::new())),
        )
    }

    fn temp_test_dir(prefix: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("orchestral-{}-{}", prefix, uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn test_document_inspect_resolves_directory_alias() {
        let dir = temp_test_dir("document-inspect-dir");
        let docs = dir.join("docs");
        fs::create_dir_all(&docs).expect("create docs dir");
        fs::write(docs.join("guide.md"), "# Guide\n\nbody\n").expect("write guide");
        fs::write(docs.join("notes.txt"), "notes\n").expect("write notes");

        let resolved = resolve_document_source_paths(
            &json!({ "document": docs.to_string_lossy().to_string() }),
        )
        .expect("resolve source paths");
        assert_eq!(resolved.1, "document");
        assert_eq!(resolved.0.len(), 2);
        assert!(resolved.0.iter().any(|path| path.ends_with("guide.md")));
        assert!(resolved.0.iter().any(|path| path.ends_with("notes.txt")));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_document_inspect_accepts_json_array_string() {
        let resolved = resolve_document_source_paths(&json!({
            "source_paths": "[\"docs/a.md\",\"docs/b.md\"]"
        }))
        .expect("resolve source paths");

        assert_eq!(resolved.1, "source_paths");
        assert_eq!(resolved.0, vec!["docs/a.md", "docs/b.md"]);
    }

    #[test]
    fn test_document_locate_exports_documents_alias() {
        let dir = temp_test_dir("document-locate");
        fs::write(dir.join("guide.md"), "# Guide\n").expect("write markdown");

        let exports = locate_documents(&dir, "").expect("locate documents");
        assert_eq!(exports.get("documents"), exports.get("source_paths"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_document_inspect_run_accepts_documents_alias() {
        tokio_test::block_on(async {
            let dir = temp_test_dir("document-inspect-run");
            fs::write(dir.join("guide.md"), "# Guide\n\nbody\n").expect("write markdown");

            let action = DocumentInspectAction::from_spec(&ActionSpec {
                name: "document_inspect".to_string(),
                kind: "document_inspect".to_string(),
                description: None,
                category: None,
                config: Value::Null,
                interface: None,
            });
            let result = action
                .run(
                    ActionInput::with_params(json!({
                        "documents": [dir.join("guide.md").to_string_lossy().to_string()]
                    })),
                    test_action_context(),
                )
                .await;

            match result {
                ActionResult::Success { exports } => {
                    assert!(exports.contains_key("inspection"));
                }
                other => panic!("expected success, got {:?}", other),
            }

            let _ = fs::remove_dir_all(dir);
        });
    }
}

#[async_trait]
impl Action for DocumentAssessReadinessAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("document")
            .with_capabilities(["pure", "structured_output"])
            .with_roles(["verify", "control"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "patch_candidates": { "type": "object" },
                    "derivation_policy": { "type": "string" },
                    "user_request": { "type": "string" }
                },
                "required": ["inspection", "patch_candidates", "derivation_policy", "user_request"]
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
            return ActionResult::error("Missing inspection for document_assess_readiness");
        };
        let Some(patch_candidates) = input.params.get("patch_candidates") else {
            return ActionResult::error("Missing patch_candidates for document_assess_readiness");
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error("Missing derivation_policy for document_assess_readiness");
        };
        let user_request = input
            .params
            .get("user_request")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match assess_document_readiness(user_request, inspection, patch_candidates, raw_policy) {
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
struct DocumentDeriveCandidatesAction {
    name: String,
    description: String,
}

impl DocumentDeriveCandidatesAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Derive document patch candidates"),
        }
    }
}

#[async_trait]
impl Action for DocumentDeriveCandidatesAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("document")
            .with_capabilities(["pure", "structured_output"])
            .with_roles(["derive", "emit"])
            .with_input_kinds(["structured", "text"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "user_request": { "type": "string" },
                    "inspection": { "type": "object" },
                    "derivation_policy": { "type": "string" }
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
            return ActionResult::error("Missing user_request for document_derive_candidates");
        };
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for document_derive_candidates");
        };
        let Some(raw_policy) = input
            .params
            .get("derivation_policy")
            .and_then(Value::as_str)
        else {
            return ActionResult::error("Missing derivation_policy for document_derive_candidates");
        };
        match derive_document_patch_candidates(user_request, inspection, raw_policy) {
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
struct DocumentBuildPatchSpecAction {
    name: String,
    description: String,
}

impl DocumentBuildPatchSpecAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Build a typed document patch spec"),
        }
    }
}

#[async_trait]
impl Action for DocumentBuildPatchSpecAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("document")
            .with_capabilities(["pure", "structured_output"])
            .with_roles(["derive", "emit"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_candidates": { "type": "object" },
                    "inspection": { "type": "object" }
                },
                "required": ["patch_candidates", "inspection"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": { "type": "object" },
                    "summary": { "type": "string" }
                },
                "required": ["patch_spec", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(patch_candidates) = input.params.get("patch_candidates") else {
            return ActionResult::error("Missing patch_candidates for document_build_patch_spec");
        };
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for document_build_patch_spec");
        };
        match build_document_patch_spec(patch_candidates, inspection) {
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
struct DocumentApplyPatchAction {
    name: String,
    description: String,
}

impl DocumentApplyPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Apply a document patch"),
        }
    }
}

#[async_trait]
impl Action for DocumentApplyPatchAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("document")
            .with_capabilities(["filesystem_write", "side_effect"])
            .with_roles(["apply", "execute"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["path", "structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": { "type": "object" },
                    "report_path": { "type": "string" },
                    "inspection": { "type": "object" },
                    "patch_candidates": { "type": "object" }
                },
                "required": ["patch_spec"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "updated_paths": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "patch_count": { "type": "integer" },
                    "summary": { "type": "string" }
                },
                "required": ["updated_paths", "patch_count", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(patch_spec) = input.params.get("patch_spec") else {
            return ActionResult::error("Missing patch_spec for document_apply_patch");
        };
        let report_path = input.params.get("report_path").and_then(Value::as_str);
        let inspection = input.params.get("inspection");
        let patch_candidates = input.params.get("patch_candidates");
        match apply_document_patch(patch_spec, report_path, inspection, patch_candidates) {
            Ok(exports) => ActionResult::success_with(exports),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct DocumentVerifyPatchAction {
    name: String,
    description: String,
}

impl DocumentVerifyPatchAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Verify a document patch"),
        }
    }
}

#[async_trait]
impl Action for DocumentVerifyPatchAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("document")
            .with_capabilities(["filesystem_read"])
            .with_roles(["verify"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": { "type": "object" },
                    "inspection": { "type": "object" },
                    "patch_candidates": { "type": "object" },
                    "user_request": { "type": "string" },
                    "resume_user_input": {}
                },
                "required": ["patch_spec", "inspection", "user_request"]
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
                        Value::String("Document verification failed.".to_string()),
                    ),
                ]
                .into_iter()
                .collect(),
            );
        }
        let Some(patch_spec) = input.params.get("patch_spec") else {
            return ActionResult::error("Missing patch_spec for document_verify_patch");
        };
        let Some(inspection) = input.params.get("inspection") else {
            return ActionResult::error("Missing inspection for document_verify_patch");
        };
        let user_request = input
            .params
            .get("user_request")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let patch_candidates = input.params.get("patch_candidates");
        let resume_user_input = input.params.get("resume_user_input");
        match verify_document_patch(
            patch_spec,
            inspection,
            patch_candidates,
            user_request,
            resume_user_input,
        ) {
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
