use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;
use orchestral_core::types::{
    ContinuationState, ContinuationStatus, DerivationPolicy, PatchCandidatesEnvelope, StageKind,
    VerifyDecision, VerifyStatus,
};

use super::factory::ActionBuildError;

pub fn build_document_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    let action: Box<dyn Action> = match spec.kind.as_str() {
        "document_locate" => Box::new(DocumentLocateAction::from_spec(spec)),
        "document_inspect" => Box::new(DocumentInspectAction::from_spec(spec)),
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
                    "source_paths": {
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
            .with_capabilities(["filesystem_read"])
            .with_roles(["inspect", "verify"])
            .with_input_kinds(["path"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_paths": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
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
            return ActionResult::error("Missing source_paths for document_inspect");
        };
        let source_paths = paths
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
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
            .with_capabilities(["filesystem_write", "side_effect"])
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
        match apply_document_patch(patch_spec) {
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
            .with_capabilities(["filesystem_read"])
            .with_roles(["verify"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "patch_spec": { "type": "object" },
                    "inspection": { "type": "object" },
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
        let resume_user_input = input.params.get("resume_user_input");
        match verify_document_patch(patch_spec, inspection, user_request, resume_user_input) {
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

#[derive(Debug, Clone, Deserialize)]
struct DocumentUpdate {
    path: String,
    content: String,
}

fn locate_documents(
    source_root: &Path,
    user_request: &str,
) -> Result<HashMap<String, Value>, String> {
    let root = normalize_path(source_root)?;
    let mut explicit_files = Vec::new();
    let mut explicit_dirs = Vec::new();
    let mut report_path = None;
    let mut seen = HashSet::new();

    for candidate in extract_existing_paths(&root, user_request) {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        if candidate.is_dir() {
            explicit_dirs.push(candidate);
            continue;
        }
        if is_markdown_path(&candidate) {
            if looks_like_report_path(&candidate) && report_path.is_none() {
                report_path = Some(candidate);
            } else {
                explicit_files.push(candidate);
            }
        }
    }

    let mut sources = Vec::new();
    for dir in explicit_dirs {
        collect_markdown_files(&dir, &mut sources).map_err(|err| err.to_string())?;
    }
    sources.extend(explicit_files);
    if sources.is_empty() {
        collect_markdown_files(&root, &mut sources).map_err(|err| err.to_string())?;
    }
    dedup_and_sort_paths(&mut sources);

    if let Some(path) = &report_path {
        sources.retain(|candidate| candidate != path);
    }

    if sources.is_empty() {
        return Err(format!(
            "no markdown/document candidates found under {}",
            root.display()
        ));
    }

    let mut exports = HashMap::new();
    exports.insert(
        "source_paths".to_string(),
        Value::Array(
            sources
                .iter()
                .map(|path| Value::String(display_path(path)))
                .collect(),
        ),
    );
    exports.insert(
        "artifact_candidates".to_string(),
        Value::Array(
            sources
                .iter()
                .map(|path| Value::String(display_path(path)))
                .collect(),
        ),
    );
    exports.insert("artifact_count".to_string(), json!(sources.len()));
    if let Some(path) = report_path {
        exports.insert(
            "report_path".to_string(),
            Value::String(display_path(&path)),
        );
    }
    Ok(exports)
}

fn inspect_documents(source_paths: &[String]) -> Result<Value, String> {
    let mut files = Vec::new();
    let mut total_todos = 0usize;
    let mut missing_titles = 0usize;

    for path in source_paths {
        let content = fs::read_to_string(path)
            .map_err(|err| format!("read document '{}' failed: {}", path, err))?;
        let file_info = inspect_document_content(Path::new(path), &content);
        total_todos += file_info.todo_count;
        if file_info.missing_title {
            missing_titles += 1;
        }
        files.push(json!({
            "path": path,
            "file_name": file_info.file_name,
            "stem": file_info.stem,
            "title": file_info.title,
            "missing_title": file_info.missing_title,
            "todo_count": file_info.todo_count,
            "todo_lines": file_info.todo_lines,
            "heading_count": file_info.heading_count,
            "headings": file_info.headings,
            "line_count": file_info.line_count,
            "content": content,
        }));
    }

    Ok(json!({
        "target_count": files.len(),
        "missing_title_count": missing_titles,
        "todo_count": total_todos,
        "files": files,
    }))
}

fn assess_document_readiness(
    user_request: &str,
    inspection: &Value,
    patch_candidates: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let policy = parse_derivation_policy(raw_policy)?;
    let files = inspection
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| "document inspection is missing files".to_string())?;
    if files.is_empty() {
        return Err("document inspection produced no files".to_string());
    }

    let summary = synthesize_document_plan_summary(inspection, patch_candidates);
    let unknowns = collect_candidate_unknowns(patch_candidates);
    let needs_confirmation = request_requires_confirmation(user_request);
    let has_any_change = files.iter().any(|file| {
        file.get("missing_title")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || file
                .get("todo_count")
                .and_then(Value::as_u64)
                .unwrap_or_default()
                > 0
    });

    let continuation = if !has_any_change {
        ContinuationState {
            status: ContinuationStatus::Done,
            reason: "document inspection found no missing titles or TODO placeholders".to_string(),
            unknowns: Vec::new(),
            assumptions: Vec::new(),
            next_stage_hint: Some(StageKind::Verify),
            user_message: Some("未发现需要写回的文档改动。".to_string()),
        }
    } else if needs_confirmation {
        let next_stage_hint = if unknowns.is_empty() || policy == DerivationPolicy::Permissive {
            StageKind::Commit
        } else {
            StageKind::Probe
        };
        let mut user_message = format!("{}\n\n回复“确认”后我会按这个计划写回。", summary);
        if !unknowns.is_empty() {
            user_message.push_str(&format!("\n仍有待确认信息：{}。", unknowns.join("；")));
        }
        ContinuationState {
            status: ContinuationStatus::WaitUser,
            reason: if unknowns.is_empty() || policy == DerivationPolicy::Permissive {
                "user requested review/confirmation before document writes".to_string()
            } else {
                "bounded derivation reported unresolved document unknowns".to_string()
            },
            unknowns: unknowns.clone(),
            assumptions: Vec::new(),
            next_stage_hint: Some(next_stage_hint),
            user_message: Some(user_message),
        }
    } else if !unknowns.is_empty() {
        ContinuationState {
            status: ContinuationStatus::WaitUser,
            reason: "bounded derivation reported unresolved document unknowns".to_string(),
            unknowns: unknowns.clone(),
            assumptions: Vec::new(),
            next_stage_hint: Some(StageKind::Probe),
            user_message: Some(format!(
                "{}\n\n仍有待确认信息：{}。",
                summary,
                unknowns.join("；")
            )),
        }
    } else {
        ContinuationState {
            status: ContinuationStatus::CommitReady,
            reason: "document probe gathered enough structure to derive a typed patch".to_string(),
            unknowns: Vec::new(),
            assumptions: Vec::new(),
            next_stage_hint: Some(StageKind::Commit),
            user_message: None,
        }
    };

    Ok((
        serde_json::to_value(continuation).unwrap_or(Value::Null),
        summary,
    ))
}

fn apply_document_patch(patch_spec: &Value) -> Result<HashMap<String, Value>, String> {
    let updates = parse_document_updates(patch_spec)?;
    if updates.is_empty() {
        return Err("document patch_spec.updates is empty".to_string());
    }

    for update in &updates {
        let path = Path::new(&update.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "create document parent '{}' failed: {}",
                    parent.display(),
                    err
                )
            })?;
        }
        fs::write(path, update.content.as_bytes())
            .map_err(|err| format!("write document '{}' failed: {}", update.path, err))?;
    }

    Ok([
        (
            "updated_paths".to_string(),
            Value::Array(
                updates
                    .iter()
                    .map(|update| Value::String(update.path.clone()))
                    .collect(),
            ),
        ),
        ("patch_count".to_string(), json!(updates.len())),
        (
            "summary".to_string(),
            Value::String(
                patch_spec
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or("Document patch applied.")
                    .to_string(),
            ),
        ),
    ]
    .into_iter()
    .collect())
}

fn verify_document_patch(
    patch_spec: &Value,
    inspection: &Value,
    user_request: &str,
    resume_user_input: Option<&Value>,
) -> Result<(VerifyDecision, String), String> {
    let updates = parse_document_updates(patch_spec)?;
    if updates.is_empty() {
        return Err("document verify received empty patch_spec.updates".to_string());
    }

    let mut actual_content = BTreeMap::new();
    for update in &updates {
        let content = fs::read_to_string(&update.path)
            .map_err(|err| format!("read patched document '{}' failed: {}", update.path, err))?;
        if content != update.content {
            return Ok((
                VerifyDecision {
                    status: VerifyStatus::Failed,
                    reason: format!(
                        "patched document '{}' does not match expected content",
                        update.path
                    ),
                    evidence: json!({ "path": update.path }),
                },
                "Document verify failed.".to_string(),
            ));
        }
        actual_content.insert(update.path.clone(), content);
    }

    let files = inspection
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut title_fixed = Vec::new();
    let mut todo_cleared = Vec::new();

    for file in files {
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        let Some(content) = actual_content.get(path) else {
            continue;
        };
        let after = inspect_document_content(Path::new(path), content);
        let missing_title_before = file
            .get("missing_title")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if missing_title_before {
            if after.missing_title {
                return Ok((
                    VerifyDecision {
                        status: VerifyStatus::Failed,
                        reason: format!("document '{}' still has no top-level title", path),
                        evidence: json!({ "path": path }),
                    },
                    "Document verify failed.".to_string(),
                ));
            }
            title_fixed.push(path.to_string());
        }
        let todo_count_before = file
            .get("todo_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if todo_count_before > 0 {
            if after.todo_count > 0 {
                return Ok((
                    VerifyDecision {
                        status: VerifyStatus::Failed,
                        reason: format!("document '{}' still contains TODO markers", path),
                        evidence: json!({
                            "path": path,
                            "todo_lines": after.todo_lines,
                        }),
                    },
                    "Document verify failed.".to_string(),
                ));
            }
            todo_cleared.push(path.to_string());
        }
    }

    if let Some(report_path) = requested_report_path(user_request, resume_user_input) {
        let content = fs::read_to_string(&report_path)
            .map_err(|err| format!("read report '{}' failed: {}", report_path, err))?;
        if content.trim().is_empty() || content.contains("Pending run.") {
            return Ok((
                VerifyDecision {
                    status: VerifyStatus::Failed,
                    reason: format!(
                        "report '{}' was not updated with a real summary",
                        report_path
                    ),
                    evidence: json!({ "path": report_path }),
                },
                "Document verify failed.".to_string(),
            ));
        }
    }

    Ok((
        VerifyDecision {
            status: VerifyStatus::Passed,
            reason: "document patch verified".to_string(),
            evidence: json!({
                "checked_paths": actual_content.keys().cloned().collect::<Vec<_>>(),
                "title_fixed": title_fixed,
                "todo_cleared": todo_cleared,
            }),
        },
        "Document patch verified.".to_string(),
    ))
}

fn parse_document_updates(patch_spec: &Value) -> Result<Vec<DocumentUpdate>, String> {
    let updates = patch_spec
        .get("updates")
        .cloned()
        .ok_or_else(|| "patch_spec missing updates".to_string())?;
    serde_json::from_value::<Vec<DocumentUpdate>>(updates)
        .map_err(|err| format!("parse document patch updates failed: {}", err))
}

fn synthesize_document_plan_summary(inspection: &Value, patch_candidates: &Value) -> String {
    let mut lines = vec!["计划修改以下文档：".to_string()];
    let candidate_files = document_candidate_files(patch_candidates);
    let mut candidate_map = BTreeMap::new();
    for file in candidate_files {
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        candidate_map.insert(path.to_string(), file);
    }

    if let Some(files) = inspection.get("files").and_then(Value::as_array) {
        for file in files {
            let Some(path) = file.get("path").and_then(Value::as_str) else {
                continue;
            };
            let missing_title = file
                .get("missing_title")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let todo_count = file
                .get("todo_count")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let mut changes = Vec::new();
            if missing_title {
                changes.push("补全一级标题".to_string());
            }
            if todo_count > 0 {
                changes.push(format!("替换 {} 处 TODO 占位符", todo_count));
            }
            if let Some(candidate) = candidate_map.get(path) {
                if let Some(extra) = candidate
                    .get("planned_changes")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                {
                    for item in extra {
                        if !item.trim().is_empty() {
                            changes.push(item);
                        }
                    }
                }
            }
            if changes.is_empty() {
                changes.push("检查并保持现有内容结构".to_string());
            }
            lines.push(format!("- {}：{}", path, changes.join("；")));
        }
    }

    if let Some(summary) = patch_candidates.get("summary").and_then(Value::as_str) {
        let trimmed = summary.trim();
        if !trimmed.is_empty() {
            lines.push(String::new());
            lines.push(trimmed.to_string());
        }
    }
    lines.join("\n")
}

fn collect_candidate_unknowns(patch_candidates: &Value) -> Vec<String> {
    let mut unknowns = parse_patch_candidates_envelope(patch_candidates)
        .map(|value| value.unknowns)
        .unwrap_or_else(|| {
            patch_candidates
                .get("unknowns")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        });
    for file in document_candidate_files(patch_candidates) {
        if file
            .get("needs_user_input")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            if let Some(path) = file.get("path").and_then(Value::as_str) {
                unknowns.push(format!("{} requires additional user input", path));
            }
        }
        if let Some(items) = file.get("unknowns").and_then(Value::as_array) {
            unknowns.extend(items.iter().filter_map(Value::as_str).map(str::to_string));
        }
    }
    unknowns.sort();
    unknowns.dedup();
    unknowns
}

fn document_candidate_files(patch_candidates: &Value) -> Vec<Value> {
    let candidate_root = parse_patch_candidates_envelope(patch_candidates)
        .map(|value| value.candidates)
        .unwrap_or_else(|| patch_candidates.clone());
    candidate_root
        .get("files")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            patch_candidates
                .get("files")
                .and_then(Value::as_array)
                .cloned()
        })
        .unwrap_or_default()
}

fn parse_patch_candidates_envelope(value: &Value) -> Option<PatchCandidatesEnvelope> {
    serde_json::from_value::<PatchCandidatesEnvelope>(value.clone()).ok()
}

fn request_requires_confirmation(user_request: &str) -> bool {
    let lowered = user_request.to_ascii_lowercase();
    [
        "先不要写",
        "先不要写回",
        "先给出修改计划",
        "等待我确认",
        "等我确认",
        "wait for my confirmation",
        "do not write yet",
        "review first",
        "propose plan first",
    ]
    .iter()
    .any(|marker| lowered.contains(marker))
}

fn requested_report_path(user_request: &str, resume_user_input: Option<&Value>) -> Option<String> {
    let mut texts = vec![user_request.to_string()];
    if let Some(value) = resume_user_input {
        if let Some(text) = value.as_str() {
            texts.push(text.to_string());
        } else if let Some(text) = value.get("message").and_then(Value::as_str) {
            texts.push(text.to_string());
        } else if let Some(text) = value.get("text").and_then(Value::as_str) {
            texts.push(text.to_string());
        }
    }
    for text in texts {
        for path in extract_path_like_tokens(&text) {
            if looks_like_report_path(Path::new(&path)) {
                return Some(path);
            }
        }
    }
    None
}

fn parse_derivation_policy(raw_policy: &str) -> Result<DerivationPolicy, String> {
    match raw_policy {
        "strict" => Ok(DerivationPolicy::Strict),
        "permissive" => Ok(DerivationPolicy::Permissive),
        other => Err(format!("unsupported derivation_policy '{}'", other)),
    }
}

#[derive(Debug)]
struct DocumentInspectionSummary {
    file_name: String,
    stem: String,
    title: Option<String>,
    missing_title: bool,
    todo_count: usize,
    todo_lines: Vec<Value>,
    heading_count: usize,
    headings: Vec<Value>,
    line_count: usize,
}

fn inspect_document_content(path: &Path, content: &str) -> DocumentInspectionSummary {
    let mut todo_lines = Vec::new();
    let mut headings = Vec::new();
    let mut title = None;
    let mut first_nonempty = None;

    for (index, line) in content.lines().enumerate() {
        let trimmed = line.trim();
        if first_nonempty.is_none() && !trimmed.is_empty() {
            first_nonempty = Some(trimmed.to_string());
        }
        if trimmed.contains("TODO") {
            todo_lines.push(json!({
                "line": index + 1,
                "text": trimmed,
            }));
        }
        if trimmed.starts_with('#') {
            let hashes = trimmed.chars().take_while(|ch| *ch == '#').count();
            let text = trimmed[hashes..].trim().to_string();
            headings.push(json!({
                "line": index + 1,
                "level": hashes,
                "text": text,
            }));
            if title.is_none() && hashes == 1 && !text.is_empty() {
                title = Some(text);
            }
        }
    }

    let missing_title = first_nonempty
        .as_deref()
        .map(|line| !line.starts_with("# "))
        .unwrap_or(true);

    DocumentInspectionSummary {
        file_name: path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string(),
        stem: path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string(),
        title,
        missing_title,
        todo_count: todo_lines.len(),
        todo_lines,
        heading_count: headings.len(),
        headings,
        line_count: content.lines().count(),
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

fn dedup_and_sort_paths(paths: &mut Vec<PathBuf>) {
    paths.sort();
    paths.dedup();
}

fn collect_markdown_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
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
            collect_markdown_files(&path, out)?;
        } else if is_markdown_path(&path) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_markdown_path(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|ext| {
            matches!(
                ext.to_ascii_lowercase().as_str(),
                "md" | "markdown" | "mdx" | "txt"
            )
        })
        .unwrap_or(false)
}

fn looks_like_report_path(path: &Path) -> bool {
    let lower = display_path(path).to_ascii_lowercase();
    lower.contains("/report")
        || lower.contains("/reports/")
        || lower.ends_with("summary.md")
        || lower.ends_with("summary.markdown")
}

fn extract_existing_paths(root: &Path, input: &str) -> Vec<PathBuf> {
    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for raw in extract_path_like_tokens(input) {
        let candidate = PathBuf::from(&raw);
        let resolved = if candidate.is_absolute() {
            candidate
        } else {
            root.join(&candidate)
        };
        if resolved.exists() && seen.insert(resolved.clone()) {
            results.push(resolved);
        }
    }
    results
}

fn extract_path_like_tokens(input: &str) -> Vec<String> {
    input
        .split_whitespace()
        .map(trim_path_token)
        .filter(|token| !token.is_empty())
        .filter(|token| {
            token.contains('/') || token.ends_with(".md") || token.ends_with(".markdown")
        })
        .collect()
}

fn trim_path_token(token: &str) -> String {
    token
        .trim_matches(|ch: char| {
            ch.is_whitespace()
                || matches!(
                    ch,
                    '"' | '\''
                        | '`'
                        | ','
                        | '，'
                        | '。'
                        | ':'
                        | '：'
                        | ';'
                        | '；'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                )
        })
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_requires_confirmation_for_plan_first_prompt() {
        assert!(request_requires_confirmation(
            "先不要写回，先给出修改计划并等待我确认"
        ));
        assert!(!request_requires_confirmation("直接写回所有文档"));
    }

    #[test]
    fn test_assess_document_readiness_waits_for_confirmation() {
        let inspection = json!({
            "files": [
                {
                    "path": "docs/a.md",
                    "missing_title": true,
                    "todo_count": 2
                }
            ]
        });
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "docs/a.md",
                        "planned_changes": ["补全标题", "替换 TODO"],
                        "needs_user_input": false,
                        "unknowns": []
                    }
                ]
            },
            "unknowns": [],
            "assumptions": []
        });

        let (continuation, summary) = assess_document_readiness(
            "扫描 markdown，先给出修改计划并等待我确认",
            &inspection,
            &patch_candidates,
            "permissive",
        )
        .expect("assess");
        assert!(summary.contains("计划修改以下文档"));
        assert_eq!(
            continuation["status"],
            Value::String("wait_user".to_string())
        );
        assert_eq!(
            continuation["next_stage_hint"],
            Value::String("commit".to_string())
        );
    }

    #[test]
    fn test_collect_candidate_unknowns_supports_enveloped_files() {
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "docs/a.md",
                        "needs_user_input": true,
                        "unknowns": ["missing owner"]
                    }
                ]
            },
            "unknowns": ["missing due date"]
        });

        let unknowns = collect_candidate_unknowns(&patch_candidates);
        assert!(unknowns.contains(&"docs/a.md requires additional user input".to_string()));
        assert!(unknowns.contains(&"missing owner".to_string()));
        assert!(unknowns.contains(&"missing due date".to_string()));
    }
}
