use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;
use orchestral_core::types::{
    ContinuationState, ContinuationStatus, DerivationPolicy, PatchCandidatesEnvelope, StageKind,
    VerifyDecision, VerifyStatus,
};

use super::factory::ActionBuildError;

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
        match locate_structured_files(Path::new(source_root), user_request) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StructuredFormat {
    Json,
    Yaml,
    Toml,
}

impl StructuredFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
            Self::Toml => "toml",
        }
    }

    fn from_path(path: &Path) -> Option<Self> {
        match path
            .extension()
            .and_then(|value| value.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref()
        {
            Some("json") => Some(Self::Json),
            Some("yaml" | "yml") => Some(Self::Yaml),
            Some("toml") => Some(Self::Toml),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StructuredPatchSpec {
    files: Vec<StructuredPatchFile>,
    #[serde(default)]
    summary: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StructuredPatchFile {
    path: String,
    operations: Vec<StructuredPatchOperation>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StructuredPatchOperation {
    op: String,
    path: String,
    #[serde(default)]
    value: Option<Value>,
}

fn locate_structured_files(
    source_root: &Path,
    user_request: &str,
) -> Result<HashMap<String, Value>, String> {
    let root = normalize_path(source_root)?;
    let lowered_request = user_request.to_ascii_lowercase();
    let path_hints = extract_path_hints(user_request);
    let mut candidates = Vec::new();
    collect_structured_files(&root, &mut candidates).map_err(|err| err.to_string())?;
    if candidates.is_empty() {
        return Err(format!(
            "no structured artifact candidates found under {}",
            root.display()
        ));
    }

    let mut scored = candidates
        .into_iter()
        .map(|path| {
            let mut score = 0i32;
            let display = display_path(&path).to_ascii_lowercase();
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();

            for hint in &path_hints {
                if display == *hint || display.ends_with(hint) {
                    score += 24;
                } else if name == *hint || display.contains(hint) {
                    score += 12;
                }
            }
            if lowered_request.contains("config") && display.contains("config") {
                score += 3;
            }
            if lowered_request.contains("json") && display.ends_with(".json") {
                score += 4;
            }
            if (lowered_request.contains("yaml") || lowered_request.contains("yml"))
                && (display.ends_with(".yaml") || display.ends_with(".yml"))
            {
                score += 4;
            }
            if lowered_request.contains("toml") && display.ends_with(".toml") {
                score += 4;
            }
            if lowered_request.contains("fixtures") && display.starts_with("fixtures/") {
                score += 4;
            }
            let depth_penalty = display.matches('/').count() as i32;
            score -= depth_penalty.min(6);
            (score, path)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.as_os_str().cmp(right.1.as_os_str()))
    });

    let selected = scored
        .iter()
        .take(10)
        .map(|(_, path)| Value::String(display_path(path)))
        .collect::<Vec<_>>();

    Ok([
        ("source_paths".to_string(), Value::Array(selected.clone())),
        ("artifact_candidates".to_string(), Value::Array(selected)),
        ("artifact_count".to_string(), json!(scored.len())),
    ]
    .into_iter()
    .collect())
}

fn inspect_structured_files(source_paths: &[String]) -> Result<Value, String> {
    let mut files = Vec::new();
    for path in source_paths {
        let path_buf = normalize_path(Path::new(path))?;
        let format = StructuredFormat::from_path(&path_buf)
            .ok_or_else(|| format!("unsupported structured file '{}'", path_buf.display()))?;
        let parsed = parse_structured_file(&path_buf)?;
        let top_level_keys = parsed
            .as_object()
            .map(|object| {
                object
                    .keys()
                    .map(|key| Value::String(key.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        files.push(json!({
            "path": display_path(&path_buf),
            "format": format.as_str(),
            "top_level_keys": top_level_keys,
            "summary": summarize_structured_value(&parsed),
            "value": parsed,
        }));
    }

    Ok(json!({
        "target_count": files.len(),
        "files": files,
    }))
}

fn assess_structured_readiness(
    inspection: &Value,
    patch_candidates: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let policy = parse_derivation_policy(raw_policy)?;
    let files = inspection
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| "structured inspection is missing files".to_string())?;
    if files.is_empty() {
        return Err("structured inspection produced no files".to_string());
    }

    let envelope = parse_patch_candidates_envelope(patch_candidates);
    let candidate_files = structured_candidate_files(patch_candidates);
    let mut unknowns = envelope
        .as_ref()
        .map(|value| value.unknowns.clone())
        .unwrap_or_else(|| extract_unknowns(patch_candidates));
    let assumptions = envelope
        .as_ref()
        .map(|value| value.assumptions.clone())
        .unwrap_or_else(|| extract_assumptions(patch_candidates));
    let mut operation_count = 0usize;
    let mut missing_input_files = Vec::new();

    for file in &candidate_files {
        let file_operation_count = file
            .get("operations")
            .and_then(Value::as_array)
            .map(|items| items.len())
            .unwrap_or(0);
        if file
            .get("needs_user_input")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && file_operation_count == 0
        {
            if let Some(path) = file.get("path").and_then(Value::as_str) {
                missing_input_files.push(format!("{} requires additional user input", path));
            }
        }
        operation_count += file_operation_count;
    }

    unknowns.extend(missing_input_files);

    unknowns.sort();
    unknowns.dedup();

    if operation_count == 0 && unknowns.is_empty() {
        let continuation = ContinuationState {
            status: ContinuationStatus::Done,
            reason: "structured inspection found no required changes".to_string(),
            unknowns: Vec::new(),
            assumptions,
            next_stage_hint: Some(StageKind::Done),
            user_message: None,
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            "Structured probe found no required changes.".to_string(),
        ));
    }

    let should_wait = match policy {
        DerivationPolicy::Strict => !unknowns.is_empty(),
        DerivationPolicy::Permissive => operation_count == 0 && !unknowns.is_empty(),
    };

    if should_wait {
        let user_message = format!("Need more input before commit: {}.", unknowns.join("; "));
        let continuation = ContinuationState {
            status: ContinuationStatus::WaitUser,
            reason: user_message.clone(),
            unknowns,
            assumptions,
            next_stage_hint: Some(StageKind::WaitUser),
            user_message: Some(user_message.clone()),
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            "Structured probe needs user input before commit.".to_string(),
        ));
    }

    let continuation = ContinuationState {
        status: ContinuationStatus::CommitReady,
        reason: format!(
            "structured probe derived {} patch operation(s) across {} file(s)",
            operation_count,
            candidate_files.len()
        ),
        unknowns: Vec::new(),
        assumptions,
        next_stage_hint: Some(StageKind::Commit),
        user_message: None,
    };
    Ok((
        serde_json::to_value(continuation).map_err(|err| err.to_string())?,
        format!(
            "Structured probe ready for commit with {} patch operation(s).",
            operation_count
        ),
    ))
}

fn apply_structured_patch(patch_spec: &Value) -> Result<HashMap<String, Value>, String> {
    let patch_spec = parse_structured_patch_spec(patch_spec)?;
    if patch_spec.files.is_empty() {
        return Err("structured patch_spec.files is empty".to_string());
    }

    let mut updated_paths = Vec::new();
    let mut patch_count = 0usize;
    for file in &patch_spec.files {
        let path = normalize_path(Path::new(&file.path))?;
        let format = StructuredFormat::from_path(&path)
            .ok_or_else(|| format!("unsupported structured file '{}'", path.display()))?;
        let mut current = parse_structured_file(&path)?;
        for operation in &file.operations {
            apply_structured_operation(&mut current, operation)?;
            patch_count += 1;
        }
        write_structured_file(&path, format, &current)?;
        updated_paths.push(Value::String(display_path(&path)));
    }

    Ok([
        ("updated_paths".to_string(), Value::Array(updated_paths)),
        ("patch_count".to_string(), json!(patch_count)),
        (
            "summary".to_string(),
            Value::String(
                patch_spec
                    .summary
                    .unwrap_or_else(|| "Structured patch applied.".to_string()),
            ),
        ),
    ]
    .into_iter()
    .collect())
}

fn verify_structured_patch(
    patch_spec: &Value,
    inspection: Option<&Value>,
) -> Result<(VerifyDecision, String), String> {
    let patch_spec = parse_structured_patch_spec(patch_spec)?;
    if patch_spec.files.is_empty() {
        return Err("structured verify received empty patch_spec.files".to_string());
    }

    let mut checked_paths = Vec::new();
    let mut checked_ops = 0usize;
    for file in &patch_spec.files {
        let path = normalize_path(Path::new(&file.path))?;
        let parsed = parse_structured_file(&path)?;
        checked_paths.push(display_path(&path));

        for operation in &file.operations {
            checked_ops += 1;
            match operation.op.as_str() {
                "set" => {
                    let Some(expected) = &operation.value else {
                        return Err(format!(
                            "structured set operation '{}' is missing value",
                            operation.path
                        ));
                    };
                    let Some(actual) = parsed.pointer(&operation.path) else {
                        return Ok((
                            VerifyDecision {
                                status: VerifyStatus::Failed,
                                reason: format!(
                                    "structured path '{}' is missing after patch",
                                    operation.path
                                ),
                                evidence: json!({
                                    "path": display_path(&path),
                                    "operation": operation,
                                }),
                            },
                            "Structured verify failed.".to_string(),
                        ));
                    };
                    if actual != expected {
                        return Ok((
                            VerifyDecision {
                                status: VerifyStatus::Failed,
                                reason: format!(
                                    "structured path '{}' does not match expected value",
                                    operation.path
                                ),
                                evidence: json!({
                                    "path": display_path(&path),
                                    "operation": operation,
                                    "actual": actual,
                                }),
                            },
                            "Structured verify failed.".to_string(),
                        ));
                    }
                }
                "remove" => {
                    if parsed.pointer(&operation.path).is_some() {
                        return Ok((
                            VerifyDecision {
                                status: VerifyStatus::Failed,
                                reason: format!(
                                    "structured path '{}' still exists after remove",
                                    operation.path
                                ),
                                evidence: json!({
                                    "path": display_path(&path),
                                    "operation": operation,
                                }),
                            },
                            "Structured verify failed.".to_string(),
                        ));
                    }
                }
                other => {
                    return Err(format!("unsupported structured operation '{}'", other));
                }
            }
        }
    }

    Ok((
        VerifyDecision {
            status: VerifyStatus::Passed,
            reason: "structured patch verified".to_string(),
            evidence: json!({
                "checked_paths": checked_paths,
                "checked_operations": checked_ops,
                "inspection_target_count": inspection
                    .and_then(|value| value.get("target_count"))
                    .cloned()
                    .unwrap_or(Value::Null),
            }),
        },
        "Structured patch verified.".to_string(),
    ))
}

fn parse_structured_patch_spec(patch_spec: &Value) -> Result<StructuredPatchSpec, String> {
    serde_json::from_value::<StructuredPatchSpec>(patch_spec.clone())
        .map_err(|err| format!("parse structured patch spec failed: {}", err))
}

fn structured_candidate_files(patch_candidates: &Value) -> Vec<Value> {
    let candidate_root = parse_patch_candidates_envelope(patch_candidates)
        .map(|value| value.candidates)
        .unwrap_or_else(|| patch_candidates.clone());
    candidate_root
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            candidate_root.as_array().map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        item.get("file").cloned().or_else(|| {
                            item.get("path")
                                .and_then(Value::as_str)
                                .map(|_| item.clone())
                        })
                    })
                    .collect::<Vec<_>>()
            })
        })
        .or_else(|| {
            patch_candidates
                .get("files")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            item.get("file").cloned().or_else(|| {
                                item.get("path")
                                    .and_then(Value::as_str)
                                    .map(|_| item.clone())
                            })
                        })
                        .collect::<Vec<_>>()
                })
        })
        .unwrap_or_default()
}

fn parse_patch_candidates_envelope(value: &Value) -> Option<PatchCandidatesEnvelope> {
    serde_json::from_value::<PatchCandidatesEnvelope>(value.clone()).ok()
}

fn extract_unknowns(patch_candidates: &Value) -> Vec<String> {
    let mut unknowns = patch_candidates
        .get("unknowns")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for file in structured_candidate_files(patch_candidates) {
        if let Some(items) = file.get("unknowns").and_then(Value::as_array) {
            unknowns.extend(items.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    unknowns
}

fn extract_assumptions(patch_candidates: &Value) -> Vec<String> {
    patch_candidates
        .get("assumptions")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn collect_structured_files(root: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            if matches!(name, ".git" | ".orchestral" | "target" | "node_modules") {
                continue;
            }
            collect_structured_files(&path, out)?;
            continue;
        }
        if StructuredFormat::from_path(&path).is_some() {
            out.push(path);
        }
    }
    Ok(())
}

fn extract_path_hints(user_request: &str) -> Vec<String> {
    let mut hints = user_request
        .split_whitespace()
        .map(trim_path_punctuation)
        .filter(|value| !value.is_empty())
        .filter(|value| {
            value.contains('/')
                || value.contains('\\')
                || value.ends_with(".json")
                || value.ends_with(".yaml")
                || value.ends_with(".yml")
                || value.ends_with(".toml")
        })
        .map(|value| value.replace('\\', "/").to_ascii_lowercase())
        .collect::<Vec<_>>();
    hints.sort();
    hints.dedup();
    hints
}

fn trim_path_punctuation(raw: &str) -> &str {
    raw.trim_matches(|ch: char| {
        ch.is_whitespace()
            || matches!(
                ch,
                '"' | '\''
                    | ','
                    | '，'
                    | '。'
                    | ':'
                    | '：'
                    | ';'
                    | '('
                    | ')'
                    | '['
                    | ']'
                    | '{'
                    | '}'
            )
    })
}

fn parse_structured_file(path: &Path) -> Result<Value, String> {
    let format = StructuredFormat::from_path(path)
        .ok_or_else(|| format!("unsupported structured file '{}'", path.display()))?;
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("read structured file '{}' failed: {}", path.display(), err))?;
    match format {
        StructuredFormat::Json => serde_json::from_str(&raw)
            .map_err(|err| format!("parse json '{}' failed: {}", path.display(), err)),
        StructuredFormat::Yaml => serde_yaml::from_str(&raw)
            .map_err(|err| format!("parse yaml '{}' failed: {}", path.display(), err)),
        StructuredFormat::Toml => {
            let value = toml::from_str::<toml::Value>(&raw)
                .map_err(|err| format!("parse toml '{}' failed: {}", path.display(), err))?;
            serde_json::to_value(value)
                .map_err(|err| format!("convert toml '{}' failed: {}", path.display(), err))
        }
    }
}

fn write_structured_file(
    path: &Path,
    format: StructuredFormat,
    value: &Value,
) -> Result<(), String> {
    let content = match format {
        StructuredFormat::Json => {
            let mut rendered = serde_json::to_string_pretty(value)
                .map_err(|err| format!("serialize json '{}' failed: {}", path.display(), err))?;
            rendered.push('\n');
            rendered
        }
        StructuredFormat::Yaml => serde_yaml::to_string(value)
            .map_err(|err| format!("serialize yaml '{}' failed: {}", path.display(), err))?,
        StructuredFormat::Toml => {
            let toml_value = json_to_toml_value(value)?;
            let mut rendered = toml::to_string_pretty(&toml_value)
                .map_err(|err| format!("serialize toml '{}' failed: {}", path.display(), err))?;
            rendered.push('\n');
            rendered
        }
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| {
            format!(
                "create structured dir '{}' failed: {}",
                parent.display(),
                err
            )
        })?;
    }
    fs::write(path, content.as_bytes())
        .map_err(|err| format!("write structured file '{}' failed: {}", path.display(), err))
}

fn json_to_toml_value(value: &Value) -> Result<toml::Value, String> {
    match value {
        Value::Null => Err("toml does not support null values".to_string()),
        Value::Bool(boolean) => Ok(toml::Value::Boolean(*boolean)),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                Ok(toml::Value::Integer(integer))
            } else if let Some(float) = number.as_f64() {
                Ok(toml::Value::Float(float))
            } else {
                Err(format!("unsupported numeric value '{}'", number))
            }
        }
        Value::String(text) => Ok(toml::Value::String(text.clone())),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(json_to_toml_value(item)?);
            }
            Ok(toml::Value::Array(out))
        }
        Value::Object(object) => {
            let mut out = toml::map::Map::new();
            for (key, value) in object {
                out.insert(key.clone(), json_to_toml_value(value)?);
            }
            Ok(toml::Value::Table(out))
        }
    }
}

fn summarize_structured_value(value: &Value) -> String {
    match value {
        Value::Object(object) => {
            let keys = object.keys().take(8).cloned().collect::<Vec<_>>();
            format!(
                "object with {} top-level key(s): {}",
                object.len(),
                keys.join(", ")
            )
        }
        Value::Array(items) => format!("array with {} item(s)", items.len()),
        other => format!("scalar {}", other),
    }
}

fn apply_structured_operation(
    target: &mut Value,
    operation: &StructuredPatchOperation,
) -> Result<(), String> {
    match operation.op.as_str() {
        "set" => {
            let value = operation.value.clone().ok_or_else(|| {
                format!(
                    "structured set operation '{}' is missing value",
                    operation.path
                )
            })?;
            set_json_pointer_value(target, &operation.path, value)
        }
        "remove" => remove_json_pointer_value(target, &operation.path),
        other => Err(format!("unsupported structured operation '{}'", other)),
    }
}

fn set_json_pointer_value(target: &mut Value, pointer: &str, value: Value) -> Result<(), String> {
    let segments = parse_json_pointer(pointer)?;
    set_by_segments(target, &segments, value)
}

fn set_by_segments(target: &mut Value, segments: &[String], value: Value) -> Result<(), String> {
    if segments.is_empty() {
        *target = value;
        return Ok(());
    }

    let head = &segments[0];
    if segments.len() == 1 {
        match target {
            Value::Object(object) => {
                object.insert(head.clone(), value);
                Ok(())
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                if index == items.len() {
                    items.push(value);
                    Ok(())
                } else if index < items.len() {
                    items[index] = value;
                    Ok(())
                } else {
                    Err(format!("array index '{}' is out of bounds", head))
                }
            }
            other => Err(format!(
                "cannot set pointer segment '{}' on {}",
                head,
                type_name(other)
            )),
        }
    } else {
        match target {
            Value::Object(object) => {
                let child = object
                    .entry(head.clone())
                    .or_insert_with(|| empty_container_for(&segments[1]));
                set_by_segments(child, &segments[1..], value)
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                if index > items.len() {
                    return Err(format!("array index '{}' is out of bounds", head));
                }
                if index == items.len() {
                    items.push(empty_container_for(&segments[1]));
                }
                set_by_segments(
                    items
                        .get_mut(index)
                        .ok_or_else(|| format!("array index '{}' is out of bounds", head))?,
                    &segments[1..],
                    value,
                )
            }
            other => Err(format!(
                "cannot descend into {} for '{}'",
                type_name(other),
                head
            )),
        }
    }
}

fn remove_json_pointer_value(target: &mut Value, pointer: &str) -> Result<(), String> {
    let segments = parse_json_pointer(pointer)?;
    remove_by_segments(target, &segments)
}

fn remove_by_segments(target: &mut Value, segments: &[String]) -> Result<(), String> {
    if segments.is_empty() {
        return Err("cannot remove the root structured value".to_string());
    }

    let head = &segments[0];
    if segments.len() == 1 {
        match target {
            Value::Object(object) => {
                object.remove(head);
                Ok(())
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                if index < items.len() {
                    items.remove(index);
                    Ok(())
                } else {
                    Err(format!("array index '{}' is out of bounds", head))
                }
            }
            other => Err(format!(
                "cannot remove pointer segment '{}' from {}",
                head,
                type_name(other)
            )),
        }
    } else {
        match target {
            Value::Object(object) => {
                let Some(child) = object.get_mut(head) else {
                    return Ok(());
                };
                remove_by_segments(child, &segments[1..])
            }
            Value::Array(items) => {
                let index = parse_array_index(head)?;
                let Some(child) = items.get_mut(index) else {
                    return Ok(());
                };
                remove_by_segments(child, &segments[1..])
            }
            other => Err(format!(
                "cannot descend into {} for '{}'",
                type_name(other),
                head
            )),
        }
    }
}

fn parse_json_pointer(pointer: &str) -> Result<Vec<String>, String> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(format!(
            "structured path '{}' must use JSON Pointer syntax like /service/enabled",
            pointer
        ));
    }
    Ok(pointer
        .split('/')
        .skip(1)
        .map(|segment| segment.replace("~1", "/").replace("~0", "~"))
        .collect())
}

fn parse_array_index(segment: &str) -> Result<usize, String> {
    segment
        .parse::<usize>()
        .map_err(|_| format!("array segment '{}' is not a valid index", segment))
}

fn empty_container_for(next_segment: &str) -> Value {
    if next_segment.parse::<usize>().is_ok() {
        Value::Array(Vec::new())
    } else {
        Value::Object(Map::new())
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn parse_derivation_policy(raw_policy: &str) -> Result<DerivationPolicy, String> {
    match raw_policy {
        "strict" => Ok(DerivationPolicy::Strict),
        "permissive" => Ok(DerivationPolicy::Permissive),
        other => Err(format!("unsupported derivation_policy '{}'", other)),
    }
}

fn normalize_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|err| format!("resolve current dir failed: {}", err))
    }
}

fn display_path(path: &Path) -> String {
    let cwd = std::env::current_dir().ok();
    if let Some(cwd) = cwd {
        if let Ok(relative) = path.strip_prefix(&cwd) {
            return relative.to_string_lossy().replace('\\', "/");
        }
    }
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assess_structured_readiness_accepts_enveloped_candidates() {
        let inspection = json!({
            "files": [
                { "path": "configs/app.json", "format": "json" }
            ]
        });
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "configs/app.json",
                        "operations": [
                            { "op": "set", "path": "/service/enabled", "value": true }
                        ],
                        "needs_user_input": false,
                        "unknowns": []
                    }
                ]
            },
            "unknowns": [],
            "assumptions": ["existing config structure is authoritative"]
        });

        let (continuation, summary) =
            assess_structured_readiness(&inspection, &patch_candidates, "strict").expect("assess");
        assert_eq!(
            continuation["status"],
            Value::String("commit_ready".to_string())
        );
        assert!(summary.contains("ready for commit"));
    }

    #[test]
    fn test_assess_structured_readiness_permissive_commits_with_concrete_ops() {
        let inspection = json!({
            "files": [
                { "path": "configs/app.json", "format": "json" }
            ]
        });
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "configs/app.json",
                        "operations": [
                            { "op": "set", "path": "/service/enabled", "value": true },
                            { "op": "remove", "path": "/obsolete" }
                        ],
                        "needs_user_input": true,
                        "unknowns": ["model claimed more user input was needed"]
                    }
                ]
            },
            "unknowns": ["narrative uncertainty from derive"],
            "assumptions": []
        });

        let (continuation, summary) =
            assess_structured_readiness(&inspection, &patch_candidates, "permissive")
                .expect("assess");
        assert_eq!(
            continuation["status"],
            Value::String("commit_ready".to_string())
        );
        assert!(summary.contains("ready for commit"));
    }

    #[test]
    fn test_set_and_remove_json_pointer_value() {
        let mut value = json!({
            "service": {
                "enabled": false,
                "port": 8080
            }
        });

        set_json_pointer_value(&mut value, "/service/enabled", json!(true)).expect("set");
        set_json_pointer_value(&mut value, "/owner", json!("platform-team")).expect("set owner");
        remove_json_pointer_value(&mut value, "/service/port").expect("remove");

        assert_eq!(value.pointer("/service/enabled"), Some(&json!(true)));
        assert_eq!(value.pointer("/owner"), Some(&json!("platform-team")));
        assert!(value.pointer("/service/port").is_none());
    }

    #[test]
    fn test_json_to_toml_value_supports_nested_objects() {
        let value = json!({
            "service": {
                "enabled": true,
                "port": 9090
            },
            "owner": "platform-team"
        });

        let toml_value = json_to_toml_value(&value).expect("convert");
        let rendered = toml::to_string_pretty(&toml_value).expect("render");
        assert!(rendered.contains("owner = \"platform-team\""));
        assert!(rendered.contains("[service]"));
        assert!(rendered.contains("enabled = true"));
    }
}
