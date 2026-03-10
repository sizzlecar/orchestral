use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use async_trait::async_trait;
use quick_xml::events::Event;
use quick_xml::Reader;
use serde_json::{json, Value};
use zip::write::SimpleFileOptions;
use zip::{ZipArchive, ZipWriter};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;
use orchestral_core::types::{
    ContinuationState, ContinuationStatus, DerivationPolicy, PatchCandidatesEnvelope, StageKind,
};

use super::factory::ActionBuildError;

pub fn build_spreadsheet_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    let action: Box<dyn Action> = match spec.kind.as_str() {
        "spreadsheet_locate" => Box::new(SpreadsheetLocateAction::from_spec(spec)),
        "spreadsheet_inspect" => Box::new(SpreadsheetInspectAction::from_spec(spec)),
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
            .with_capabilities(["filesystem_read"])
            .with_roles(["inspect", "verify"])
            .with_input_kinds(["path"])
            .with_output_kinds(["structured"])
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
            .with_capabilities(["pure", "structured_output"])
            .with_roles(["verify", "control"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "inspection": { "type": "object" },
                    "patch_candidates": {},
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
            .with_capabilities(["filesystem_read", "filesystem_write", "side_effect"])
            .with_roles(["apply", "execute"])
            .with_input_kinds(["path", "structured"])
            .with_output_kinds(["path", "structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "patch_spec": { "type": "object" }
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
            .with_capabilities(["filesystem_read"])
            .with_roles(["verify"])
            .with_input_kinds(["path", "structured"])
            .with_output_kinds(["structured"])
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

fn locate_workbook(
    source_root: &Path,
    user_request: &str,
) -> Result<HashMap<String, Value>, String> {
    let root = normalize_path(source_root)?;
    let lowered_request = user_request.to_ascii_lowercase();
    let path_hints = extract_path_hints(user_request);
    let mut candidates = Vec::new();
    collect_workbooks(&root, &mut candidates).map_err(|e| e.to_string())?;
    if candidates.is_empty() {
        return Err(format!(
            "no spreadsheet candidates found under {}",
            root.display()
        ));
    }
    if candidates.len() == 1 {
        let selected = candidates.pop().expect("single candidate must exist");
        return Ok([
            (
                "source_path".to_string(),
                Value::String(display_path(&selected)),
            ),
            (
                "artifact_candidates".to_string(),
                Value::Array(vec![Value::String(display_path(&selected))]),
            ),
            ("artifact_count".to_string(), json!(1u64)),
        ]
        .into_iter()
        .collect());
    }

    let mut scored = candidates
        .into_iter()
        .map(|path| {
            let mut score = 0i32;
            let display = display_path(&path).to_ascii_lowercase();
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();

            if request_mentions_path_scope(&lowered_request, &display) {
                score += 6;
            }
            for hint in &path_hints {
                if display == *hint || display.ends_with(hint) {
                    score += 24;
                } else if name == *hint || display.contains(hint) {
                    score += 12;
                } else if let Some(filename) = hint.rsplit('/').next() {
                    if !filename.is_empty() && name == filename {
                        score += 10;
                    }
                }
            }
            if lowered_request.contains("excel")
                || lowered_request.contains("xlsx")
                || lowered_request.contains("spreadsheet")
                || lowered_request.contains("workbook")
                || lowered_request.contains("表格")
                || lowered_request.contains("工作簿")
            {
                score += 1;
            }
            let depth_penalty = display.matches('/').count() as i32;
            score -= depth_penalty.min(4);
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
        .first()
        .map(|(_, path)| path)
        .ok_or_else(|| "spreadsheet candidate selection failed".to_string())?;

    Ok([
        (
            "source_path".to_string(),
            Value::String(display_path(selected)),
        ),
        (
            "artifact_candidates".to_string(),
            Value::Array(
                scored
                    .iter()
                    .take(10)
                    .map(|(_, path)| Value::String(display_path(path)))
                    .collect(),
            ),
        ),
        (
            "artifact_count".to_string(),
            json!(u64::try_from(scored.len()).unwrap_or(0)),
        ),
    ]
    .into_iter()
    .collect())
}

fn extract_path_hints(user_request: &str) -> Vec<String> {
    let mut hints = user_request
        .split_whitespace()
        .map(trim_path_punctuation)
        .filter(|value| !value.is_empty())
        .filter(|value| {
            value.contains('/')
                || value.contains('\\')
                || value.ends_with(".xlsx")
                || value.ends_with(".xlsm")
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

fn request_mentions_path_scope(lowered_request: &str, display_path: &str) -> bool {
    if lowered_request.contains("docs") && display_path.starts_with("docs/") {
        return true;
    }
    if lowered_request.contains("config") && display_path.starts_with("configs/") {
        return true;
    }
    false
}

fn collect_workbooks(root: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default();
            if name.starts_with('.') || name == "__pycache__" || name == ".orchestral" {
                continue;
            }
            collect_workbooks(&path, out)?;
            continue;
        }
        let ext = path
            .extension()
            .and_then(|v| v.to_str())
            .map(|v| v.to_ascii_lowercase())
            .unwrap_or_default();
        if ext == "xlsx" || ext == "xlsm" {
            out.push(path);
        }
    }
    Ok(())
}

fn inspect_workbook(path: &Path) -> Result<Value, String> {
    let workbook = load_xlsx_model(path)?;
    let regions = detect_candidate_regions(&workbook);
    let selected = regions.first().cloned().unwrap_or_else(empty_region);
    Ok(json!({
        "path": display_path(&workbook.path),
        "sheet_name": workbook.sheet_name,
        "sheet_path": workbook.sheet_path,
        "max_row": workbook.max_row,
        "max_column": workbook.max_col,
        "candidate_regions": regions,
        "selected_region": selected,
        "patchable_cell_count": selected.get("patchable_cells").and_then(Value::as_array).map(|v| v.len()).unwrap_or(0),
    }))
}

fn verify_patch(
    path: &Path,
    patch_spec: Option<&Value>,
    validator_path: Option<&str>,
) -> Result<Value, String> {
    let workbook_path = normalize_path(path)?;
    let spec_evidence = match patch_spec {
        Some(spec) => Some(verify_patch_against_spec(&workbook_path, spec)?),
        None => None,
    };
    if let Some(evidence) = &spec_evidence {
        let mismatch_count = evidence
            .get("mismatch_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if mismatch_count > 0 {
            return Ok(json!({
                "status": "failed",
                "reason": format!("{} spreadsheet fills do not match patch_spec", mismatch_count),
                "evidence": {
                    "spec_verification": evidence,
                    "path": workbook_path,
                },
            }));
        }
    }

    let inspection = inspect_workbook(&workbook_path)?;
    let patchable = inspection
        .get("selected_region")
        .and_then(|v| v.get("patchable_cells"))
        .and_then(Value::as_array)
        .map(|v| v.len())
        .unwrap_or(0);
    if let Some(validator_path) = validator_path.filter(|value| !value.trim().is_empty()) {
        let validator_decision = run_external_validator(&workbook_path, validator_path)?;
        return Ok(attach_verify_evidence(
            validator_decision,
            spec_evidence,
            Some(inspection),
        ));
    }
    if patchable == 0 {
        Ok(attach_verify_evidence(
            json!({
                "status": "passed",
                "reason": "selected spreadsheet region no longer has patchable cells",
                "evidence": inspection,
            }),
            spec_evidence,
            None,
        ))
    } else {
        Ok(attach_verify_evidence(
            json!({
                "status": "failed",
                "reason": format!("selected spreadsheet region still has {} patchable cells", patchable),
                "evidence": inspection,
            }),
            spec_evidence,
            None,
        ))
    }
}

fn run_external_validator(workbook_path: &Path, validator_path: &str) -> Result<Value, String> {
    let validator_script = resolve_project_path(Path::new(validator_path))?;
    if !validator_script.exists() {
        return Err(format!(
            "validator script not found: {}",
            validator_script.display()
        ));
    }

    let python = resolve_validator_python()?;
    let output = Command::new(&python)
        .arg(&validator_script)
        .arg(workbook_path)
        .output()
        .map_err(|err| format!("run validator failed: {}", err))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stdout.is_empty() {
        return Err(format!(
            "validator produced empty stdout (status={} stderr={})",
            output
                .status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            stderr
        ));
    }

    let parsed: Value = serde_json::from_str(&stdout)
        .map_err(|err| format!("validator returned invalid json: {}", err))?;
    let ok = parsed
        .get("ok")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| output.status.success());
    let failures = parsed
        .get("failures")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let reason = if ok {
        "external spreadsheet validator passed".to_string()
    } else if !failures.is_empty() {
        failures.join("; ")
    } else if !stderr.is_empty() {
        stderr.clone()
    } else {
        format!(
            "validator exited with status {}",
            output
                .status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string())
        )
    };

    Ok(json!({
        "status": if ok { "passed" } else { "failed" },
        "reason": reason,
        "evidence": {
            "validator_result": parsed,
            "validator_stderr": stderr,
            "validator_status": output.status.code(),
            "validator_script": validator_script,
            "python": python,
            "path": workbook_path,
        }
    }))
}

fn assess_readiness(
    inspection: &Value,
    patch_candidates: &Value,
    raw_policy: &str,
) -> Result<(Value, String), String> {
    let derivation_policy = match raw_policy {
        "strict" => DerivationPolicy::Strict,
        "permissive" => DerivationPolicy::Permissive,
        other => return Err(format!("unsupported derivation_policy '{}'", other)),
    };

    let patchable_cells = inspection
        .get("selected_region")
        .and_then(|value| value.get("patchable_cells"))
        .and_then(Value::as_array)
        .ok_or_else(|| "inspection.selected_region.patchable_cells must be an array".to_string())?;
    let patchable_refs = patchable_cells
        .iter()
        .filter_map(|cell| cell.get("cell").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<HashSet<_>>();
    let candidate_cells = extract_spreadsheet_candidate_cells(patch_candidates);

    if patchable_refs.is_empty() {
        let continuation = ContinuationState {
            status: ContinuationStatus::Done,
            reason: "selected spreadsheet region has no patchable cells".to_string(),
            unknowns: Vec::new(),
            assumptions: Vec::new(),
            next_stage_hint: Some(StageKind::Done),
            user_message: None,
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            "Spreadsheet probe found no editable gaps.".to_string(),
        ));
    }

    let mut covered_refs = HashSet::new();
    let mut placeholder_refs = Vec::new();
    for candidate in candidate_cells {
        let cell_ref = candidate
            .get("cell")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let proposed_action = candidate
            .get("proposed_action")
            .and_then(Value::as_str)
            .unwrap_or("fill");
        let proposed_value = candidate_value_text(&candidate).unwrap_or_default();
        if proposed_action != "fill" || proposed_value.trim().is_empty() {
            continue;
        }
        if patchable_refs.contains(&cell_ref) {
            covered_refs.insert(cell_ref.clone());
            if looks_like_placeholder_value(&proposed_value) {
                placeholder_refs.push(cell_ref);
            }
        }
    }

    let envelope = parse_patch_candidates_envelope(patch_candidates);
    let mut unknowns = envelope
        .as_ref()
        .map(|value| value.unknowns.clone())
        .unwrap_or_else(|| extract_string_array_field(patch_candidates, "unknowns"));
    let assumptions = envelope
        .as_ref()
        .map(|value| value.assumptions.clone())
        .unwrap_or_else(|| extract_string_array_field(patch_candidates, "assumptions"));

    if matches!(derivation_policy, DerivationPolicy::Permissive) && !covered_refs.is_empty() {
        let continuation = ContinuationState {
            status: ContinuationStatus::CommitReady,
            reason: format!(
                "probe identified the target spreadsheet region and produced {} candidate fills for {} patchable cells under permissive policy",
                covered_refs.len(),
                patchable_refs.len()
            ),
            unknowns,
            assumptions,
            next_stage_hint: Some(StageKind::Commit),
            user_message: None,
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            format!(
                "Spreadsheet probe ready for permissive commit with {} candidate fills and {} patchable cells.",
                covered_refs.len(),
                patchable_refs.len()
            ),
        ));
    }

    if covered_refs.len() == patchable_refs.len()
        && placeholder_refs.is_empty()
        && matches!(derivation_policy, DerivationPolicy::Strict)
    {
        let continuation = ContinuationState {
            status: ContinuationStatus::CommitReady,
            reason: format!(
                "probe produced complete non-placeholder candidate fills for all {} patchable cells",
                patchable_refs.len()
            ),
            unknowns,
            assumptions,
            next_stage_hint: Some(StageKind::Commit),
            user_message: None,
        };
        return Ok((
            serde_json::to_value(continuation).map_err(|err| err.to_string())?,
            format!(
                "Spreadsheet probe ready for strict commit with {} candidate fills.",
                covered_refs.len()
            ),
        ));
    }

    if covered_refs.len() < patchable_refs.len() {
        unknowns.push(format!(
            "{} patchable cells still lack candidate values",
            patchable_refs.len().saturating_sub(covered_refs.len())
        ));
    }
    if !placeholder_refs.is_empty() && matches!(derivation_policy, DerivationPolicy::Strict) {
        unknowns.push(format!(
            "{} candidate fills still look like placeholders under strict policy",
            placeholder_refs.len()
        ));
    }
    if unknowns.is_empty() {
        unknowns.push("employee-specific spreadsheet facts are still unresolved".to_string());
    }

    let user_message = format!("Need more input before commit: {}.", unknowns.join("; "));
    let continuation = ContinuationState {
        status: ContinuationStatus::WaitUser,
        reason: user_message.clone(),
        unknowns,
        assumptions,
        next_stage_hint: Some(StageKind::WaitUser),
        user_message: Some(user_message.clone()),
    };
    Ok((
        serde_json::to_value(continuation).map_err(|err| err.to_string())?,
        "Spreadsheet probe needs user input before commit.".to_string(),
    ))
}

fn apply_patch(path: &Path, patch_spec: &Value) -> Result<(String, usize), String> {
    let mut workbook = load_xlsx_model(path)?;
    let fills = patch_spec
        .get("fills")
        .and_then(Value::as_array)
        .ok_or_else(|| "patch_spec.fills must be an array".to_string())?;
    let mut applied = 0usize;
    for fill in fills {
        let Some(cell_ref) = fill.get("cell").and_then(Value::as_str) else {
            continue;
        };
        let (row, col) = parse_cell_ref(cell_ref)?;
        let value = fill
            .get("value")
            .ok_or_else(|| format!("fill {} missing value", cell_ref))?;
        workbook.set_cell_value(cell_ref, row, col, value.clone());
        applied += 1;
    }
    save_xlsx_model(&workbook)?;
    Ok((display_path(&workbook.path), applied))
}

fn extract_spreadsheet_candidate_cells(patch_candidates: &Value) -> Vec<Value> {
    match patch_candidates {
        Value::Array(items) => items.clone(),
        Value::Object(_) => {
            let candidate_root = parse_patch_candidates_envelope(patch_candidates)
                .map(|value| value.candidates)
                .unwrap_or_else(|| patch_candidates.clone());
            candidate_root
                .get("cells")
                .and_then(Value::as_array)
                .cloned()
                .or_else(|| {
                    patch_candidates
                        .get("cells")
                        .and_then(Value::as_array)
                        .cloned()
                })
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

fn parse_patch_candidates_envelope(value: &Value) -> Option<PatchCandidatesEnvelope> {
    serde_json::from_value::<PatchCandidatesEnvelope>(value.clone()).ok()
}

fn extract_string_array_field(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
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

fn candidate_value_text(candidate: &Value) -> Option<String> {
    candidate
        .get("proposed_value")
        .or_else(|| candidate.get("suggested_value"))
        .or_else(|| candidate.get("value"))
        .and_then(value_to_text)
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        other => Some(other.to_string()),
    }
}

fn verify_patch_against_spec(path: &Path, patch_spec: &Value) -> Result<Value, String> {
    let workbook = load_xlsx_model(path)?;
    let fills = patch_spec
        .get("fills")
        .and_then(Value::as_array)
        .ok_or_else(|| "patch_spec.fills must be an array".to_string())?;
    let mut mismatches = Vec::new();

    for fill in fills {
        let Some(cell_ref) = fill.get("cell").and_then(Value::as_str) else {
            continue;
        };
        let expected = fill
            .get("value")
            .ok_or_else(|| format!("fill {} missing value", cell_ref))?;
        let (row, col) = parse_cell_ref(cell_ref)?;
        let actual_text = workbook
            .rows
            .get(&row)
            .and_then(|row_data| row_data.cells.get(&col))
            .map(cell_display_text)
            .unwrap_or_default();
        if !cell_value_matches_expected(expected, &actual_text) {
            mismatches.push(json!({
                "cell": cell_ref,
                "expected": expected,
                "actual": actual_text,
            }));
        }
    }

    Ok(json!({
        "checked_fill_count": fills.len(),
        "mismatch_count": mismatches.len(),
        "mismatches": mismatches,
    }))
}

fn cell_value_matches_expected(expected: &Value, actual_text: &str) -> bool {
    let actual = actual_text.trim();
    match expected {
        Value::Null => actual.is_empty(),
        Value::Bool(boolean) => {
            let normalized = actual.to_ascii_lowercase();
            if *boolean {
                normalized == "1" || normalized == "true"
            } else {
                normalized == "0" || normalized == "false"
            }
        }
        Value::Number(number) => number
            .as_f64()
            .and_then(|expected_number| {
                actual
                    .parse::<f64>()
                    .ok()
                    .map(|actual_number| (actual_number - expected_number).abs() < 1e-9)
            })
            .unwrap_or(false),
        Value::String(text) => actual == text.trim(),
        other => actual == other.to_string().trim_matches('"'),
    }
}

fn attach_verify_evidence(
    mut decision: Value,
    spec_evidence: Option<Value>,
    inspection: Option<Value>,
) -> Value {
    let Some(decision_object) = decision.as_object_mut() else {
        return decision;
    };
    let evidence = decision_object
        .entry("evidence".to_string())
        .or_insert_with(|| json!({}));
    if !evidence.is_object() {
        *evidence = json!({});
    }
    let evidence_object = evidence
        .as_object_mut()
        .expect("evidence should be an object after initialization");
    if let Some(spec_evidence) = spec_evidence {
        evidence_object.insert("spec_verification".to_string(), spec_evidence);
    }
    if let Some(inspection) = inspection {
        evidence_object.insert("inspection".to_string(), inspection);
    }
    decision
}

fn looks_like_placeholder_value(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.contains("待填写")
        || trimmed.contains("待补充")
        || trimmed.contains("placeholder")
        || trimmed.contains("XXX")
        || trimmed.contains("XX%")
        || trimmed.contains("X个")
        || trimmed.contains("X次")
}

#[derive(Debug, Clone)]
struct WorkbookModel {
    path: PathBuf,
    sheet_path: String,
    sheet_name: String,
    original_sheet_xml: String,
    rows: BTreeMap<u32, RowData>,
    merged_followers: HashSet<String>,
    max_row: u32,
    max_col: u32,
}

impl WorkbookModel {
    fn set_cell_value(&mut self, cell_ref: &str, row: u32, col: u32, value: Value) {
        let inferred_style = infer_style(&self.rows, row, col);
        let row_entry = self.rows.entry(row).or_insert_with(|| RowData {
            row_index: row,
            attrs: vec![("r".to_string(), row.to_string())],
            cells: BTreeMap::new(),
        });
        let mut cell = row_entry.cells.remove(&col).unwrap_or_else(|| CellData {
            coord: cell_ref.to_string(),
            row,
            col,
            style: inferred_style,
            extra_attrs: Vec::new(),
            content: CellContent::Empty,
            formula: None,
        });
        cell.coord = cell_ref.to_string();
        cell.row = row;
        cell.col = col;
        cell.formula = None;
        cell.content = match value {
            Value::Number(number) => CellContent::Number(number.to_string()),
            Value::Bool(boolean) => CellContent::Bool(if boolean { "1" } else { "0" }.to_string()),
            Value::String(text) => CellContent::InlineString(text),
            Value::Null => CellContent::InlineString(String::new()),
            other => CellContent::InlineString(other.to_string()),
        };
        row_entry.cells.insert(col, cell);
        self.max_row = self.max_row.max(row);
        self.max_col = self.max_col.max(col);
    }
}

#[derive(Debug, Clone)]
struct RowData {
    row_index: u32,
    attrs: Vec<(String, String)>,
    cells: BTreeMap<u32, CellData>,
}

#[derive(Debug, Clone)]
struct CellData {
    coord: String,
    row: u32,
    col: u32,
    style: Option<String>,
    extra_attrs: Vec<(String, String)>,
    content: CellContent,
    formula: Option<String>,
}

#[derive(Debug, Clone)]
enum CellContent {
    Empty,
    Number(String),
    Bool(String),
    SharedString(String, String),
    InlineString(String),
    PlainString(String),
}

fn load_xlsx_model(path: &Path) -> Result<WorkbookModel, String> {
    let path = normalize_path(path)?;
    let file = File::open(&path).map_err(|e| format!("open workbook failed: {}", e))?;
    let mut archive = ZipArchive::new(file).map_err(|e| format!("open zip failed: {}", e))?;
    let workbook_xml = read_zip_entry(&mut archive, "xl/workbook.xml")?;
    let rels_xml = read_zip_entry(&mut archive, "xl/_rels/workbook.xml.rels")?;
    let (sheet_name, sheet_path) = resolve_first_sheet(&workbook_xml, &rels_xml)?;
    let shared_strings = match read_optional_zip_entry(&mut archive, "xl/sharedStrings.xml") {
        Some(result) => parse_shared_strings(result?)?,
        None => Vec::new(),
    };
    let sheet_xml = read_zip_entry(&mut archive, &sheet_path)?;
    let parsed_sheet = parse_sheet(&sheet_xml, &shared_strings)?;
    Ok(WorkbookModel {
        path,
        sheet_path,
        sheet_name,
        original_sheet_xml: sheet_xml,
        rows: parsed_sheet.rows,
        merged_followers: parsed_sheet.merged_followers,
        max_row: parsed_sheet.max_row,
        max_col: parsed_sheet.max_col,
    })
}

fn save_xlsx_model(workbook: &WorkbookModel) -> Result<(), String> {
    let source = File::open(&workbook.path).map_err(|e| format!("open workbook failed: {}", e))?;
    let mut archive = ZipArchive::new(source).map_err(|e| format!("open zip failed: {}", e))?;
    let replacement_sheet_xml = replace_sheet_data(
        &workbook.original_sheet_xml,
        &build_sheet_data_xml(&workbook.rows),
    )?;
    let temp_path = workbook.path.with_extension("xlsx.orchestral.tmp");
    let temp_file =
        File::create(&temp_path).map_err(|e| format!("create temp workbook failed: {}", e))?;
    let mut writer = ZipWriter::new(temp_file);

    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|e| format!("read zip entry failed: {}", e))?;
        let name = entry.name().to_string();
        if entry.is_dir() {
            writer
                .add_directory(name, SimpleFileOptions::default())
                .map_err(|e| format!("write zip dir failed: {}", e))?;
            continue;
        }
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read zip file failed: {}", e))?;
        let options = SimpleFileOptions::default().compression_method(entry.compression());
        writer
            .start_file(name.clone(), options)
            .map_err(|e| format!("write zip file header failed: {}", e))?;
        if name == workbook.sheet_path {
            writer
                .write_all(replacement_sheet_xml.as_bytes())
                .map_err(|e| format!("write replacement sheet failed: {}", e))?;
        } else {
            writer
                .write_all(&bytes)
                .map_err(|e| format!("copy zip entry failed: {}", e))?;
        }
    }

    writer
        .finish()
        .map_err(|e| format!("finish workbook write failed: {}", e))?;
    std::fs::rename(&temp_path, &workbook.path)
        .map_err(|e| format!("replace workbook failed: {}", e))?;
    Ok(())
}

fn resolve_first_sheet(workbook_xml: &str, rels_xml: &str) -> Result<(String, String), String> {
    let mut reader = Reader::from_str(workbook_xml);
    reader.config_mut().trim_text(true);
    let mut first_sheet_name: Option<String> = None;
    let mut first_sheet_rid: Option<String> = None;
    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) | Ok(Event::Empty(ref event))
                if event.name().as_ref() == b"sheet" =>
            {
                first_sheet_name = attr_value(event, b"name");
                first_sheet_rid = attr_value(event, b"r:id").or_else(|| attr_value(event, b"id"));
                break;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(format!("parse workbook.xml failed: {}", error));
            }
        }
    }
    let rid = first_sheet_rid.ok_or_else(|| "workbook missing first sheet r:id".to_string())?;
    let sheet_name = first_sheet_name.unwrap_or_else(|| "Sheet1".to_string());

    let mut reader = Reader::from_str(rels_xml);
    reader.config_mut().trim_text(true);
    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) | Ok(Event::Empty(ref event))
                if event.name().as_ref() == b"Relationship" =>
            {
                let id = attr_value(event, b"Id");
                if id.as_deref() != Some(rid.as_str()) {
                    continue;
                }
                let target = attr_value(event, b"Target")
                    .ok_or_else(|| "workbook relationship missing Target".to_string())?;
                let sheet_path = if target.starts_with("xl/") {
                    target
                } else {
                    format!("xl/{}", target.trim_start_matches('/'))
                };
                return Ok((sheet_name, sheet_path));
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => {
                return Err(format!("parse workbook rels failed: {}", error));
            }
        }
    }
    Err("failed to resolve first worksheet target".to_string())
}

fn parse_shared_strings(xml: String) -> Result<Vec<String>, String> {
    let mut reader = Reader::from_str(&xml);
    reader.config_mut().trim_text(false);
    let mut strings = Vec::new();
    let mut in_si = false;
    let mut in_t = false;
    let mut current = String::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"si" => {
                in_si = true;
                current.clear();
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"si" => {
                if in_si {
                    strings.push(current.clone());
                }
                in_si = false;
                in_t = false;
            }
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"t" && in_si => {
                in_t = true;
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"t" && in_si => {
                in_t = false;
            }
            Ok(Event::Text(text)) if in_t => {
                current.push_str(&decode_text(text.as_ref()));
            }
            Ok(Event::CData(text)) if in_t => {
                current.push_str(&String::from_utf8_lossy(text.as_ref()));
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("parse shared strings failed: {}", error)),
        }
    }
    Ok(strings)
}

struct ParsedSheet {
    rows: BTreeMap<u32, RowData>,
    merged_followers: HashSet<String>,
    max_row: u32,
    max_col: u32,
}

fn parse_sheet(xml: &str, shared_strings: &[String]) -> Result<ParsedSheet, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut rows = BTreeMap::new();
    let mut merged_followers = HashSet::new();
    let mut current_row: Option<RowData> = None;
    let mut current_cell: Option<CellParseState> = None;
    let mut in_value = false;
    let mut in_formula = false;
    let mut in_inline_text = false;
    let mut max_row = 0u32;
    let mut max_col = 0u32;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"row" => {
                let row_index = attr_value(event, b"r")
                    .and_then(|v| v.parse::<u32>().ok())
                    .unwrap_or(0);
                max_row = max_row.max(row_index);
                current_row = Some(RowData {
                    row_index,
                    attrs: collect_attrs(event),
                    cells: BTreeMap::new(),
                });
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"row" => {
                if let Some(row) = current_row.take() {
                    rows.insert(row.row_index, row);
                }
            }
            Ok(Event::Start(ref event)) if event.name().as_ref() == b"c" => {
                let coord =
                    attr_value(event, b"r").ok_or_else(|| "cell missing coordinate".to_string())?;
                let (row, col) = parse_cell_ref(&coord)?;
                max_row = max_row.max(row);
                max_col = max_col.max(col);
                current_cell = Some(CellParseState {
                    coord,
                    row,
                    col,
                    cell_type: attr_value(event, b"t"),
                    style: attr_value(event, b"s"),
                    extra_attrs: collect_extra_cell_attrs(event),
                    value: String::new(),
                    formula: None,
                    inline_text: String::new(),
                });
            }
            Ok(Event::End(ref event)) if event.name().as_ref() == b"c" => {
                if let Some(cell_state) = current_cell.take() {
                    let cell = finalize_cell(cell_state, shared_strings);
                    if let Some(row) = current_row.as_mut() {
                        row.cells.insert(cell.col, cell);
                    }
                }
                in_value = false;
                in_formula = false;
                in_inline_text = false;
            }
            Ok(Event::Start(ref event))
                if event.name().as_ref() == b"v" && current_cell.is_some() =>
            {
                in_value = true;
            }
            Ok(Event::End(ref event))
                if event.name().as_ref() == b"v" && current_cell.is_some() =>
            {
                in_value = false;
            }
            Ok(Event::Start(ref event))
                if event.name().as_ref() == b"f" && current_cell.is_some() =>
            {
                in_formula = true;
            }
            Ok(Event::End(ref event))
                if event.name().as_ref() == b"f" && current_cell.is_some() =>
            {
                in_formula = false;
            }
            Ok(Event::Start(ref event))
                if event.name().as_ref() == b"t" && current_cell.is_some() =>
            {
                in_inline_text = true;
            }
            Ok(Event::End(ref event))
                if event.name().as_ref() == b"t" && current_cell.is_some() =>
            {
                in_inline_text = false;
            }
            Ok(Event::Text(text)) => {
                let decoded = decode_text(text.as_ref());
                if let Some(cell) = current_cell.as_mut() {
                    if in_value {
                        cell.value.push_str(&decoded);
                    } else if in_formula {
                        cell.formula
                            .get_or_insert_with(String::new)
                            .push_str(&decoded);
                    } else if in_inline_text {
                        cell.inline_text.push_str(&decoded);
                    }
                }
            }
            Ok(Event::CData(text)) => {
                let decoded = String::from_utf8_lossy(text.as_ref()).to_string();
                if let Some(cell) = current_cell.as_mut() {
                    if in_value {
                        cell.value.push_str(&decoded);
                    } else if in_formula {
                        cell.formula
                            .get_or_insert_with(String::new)
                            .push_str(&decoded);
                    } else if in_inline_text {
                        cell.inline_text.push_str(&decoded);
                    }
                }
            }
            Ok(Event::Empty(ref event)) if event.name().as_ref() == b"mergeCell" => {
                if let Some(range_ref) = attr_value(event, b"ref") {
                    extend_merged_followers(&mut merged_followers, &range_ref);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(format!("parse sheet xml failed: {}", error)),
        }
    }

    Ok(ParsedSheet {
        rows,
        merged_followers,
        max_row,
        max_col,
    })
}

struct CellParseState {
    coord: String,
    row: u32,
    col: u32,
    cell_type: Option<String>,
    style: Option<String>,
    extra_attrs: Vec<(String, String)>,
    value: String,
    formula: Option<String>,
    inline_text: String,
}

fn finalize_cell(state: CellParseState, shared_strings: &[String]) -> CellData {
    let content = match state.cell_type.as_deref() {
        Some("s") => {
            let text = state
                .value
                .parse::<usize>()
                .ok()
                .and_then(|index| shared_strings.get(index).cloned())
                .unwrap_or_default();
            CellContent::SharedString(state.value.clone(), text)
        }
        Some("inlineStr") => CellContent::InlineString(state.inline_text.clone()),
        Some("str") => CellContent::PlainString(state.value.clone()),
        Some("b") => CellContent::Bool(state.value.clone()),
        _ => {
            if state.value.is_empty() && state.inline_text.is_empty() {
                CellContent::Empty
            } else if !state.inline_text.is_empty() {
                CellContent::InlineString(state.inline_text.clone())
            } else {
                CellContent::Number(state.value.clone())
            }
        }
    };
    CellData {
        coord: state.coord,
        row: state.row,
        col: state.col,
        style: state.style,
        extra_attrs: state.extra_attrs,
        content,
        formula: state.formula,
    }
}

fn detect_candidate_regions(workbook: &WorkbookModel) -> Vec<Value> {
    let mut header_candidates = Vec::new();
    let scan_limit = workbook.max_row.min(20);
    for row in 1..=scan_limit {
        let cells = row_non_empty_cells(workbook, row, 1, None);
        if cells.len() < 4 {
            continue;
        }
        let start_col = cells
            .iter()
            .filter_map(|cell| cell.get("column_index").and_then(Value::as_u64))
            .min()
            .unwrap_or(1) as u32;
        let end_col = cells
            .iter()
            .filter_map(|cell| cell.get("column_index").and_then(Value::as_u64))
            .max()
            .unwrap_or(start_col as u64) as u32;
        let mut support_rows = 0;
        for probe_row in (row + 1)..=(workbook.max_row.min(row + 3)) {
            if row_non_empty_cells(workbook, probe_row, start_col, Some(end_col)).len() >= 2 {
                support_rows += 1;
            }
        }
        if support_rows == 0 {
            continue;
        }
        header_candidates.push((
            cells.len() as i32 * 2 + support_rows,
            row,
            start_col,
            end_col,
        ));
    }
    header_candidates.sort_by(|left, right| right.cmp(left));
    let mut regions = Vec::new();
    for (_, header_row, start_col, end_col) in header_candidates {
        if regions.iter().any(|region: &Value| {
            region
                .get("header_row")
                .and_then(Value::as_u64)
                .map(|existing| header_row.abs_diff(existing as u32) <= 1)
                .unwrap_or(false)
        }) {
            continue;
        }
        let region = build_candidate_region(
            workbook,
            regions.len() as u32 + 1,
            header_row,
            start_col,
            end_col,
        );
        if region.get("row_count").and_then(Value::as_u64).unwrap_or(0) == 0 {
            continue;
        }
        regions.push(region);
        if regions.len() >= 3 {
            break;
        }
    }
    regions
}

fn build_candidate_region(
    workbook: &WorkbookModel,
    region_index: u32,
    header_row: u32,
    start_col: u32,
    end_col: u32,
) -> Value {
    let mut columns = Vec::new();
    let mut active_columns = Vec::new();
    for col in start_col..=end_col {
        let text = workbook
            .rows
            .get(&header_row)
            .and_then(|row| row.cells.get(&col))
            .map(cell_display_text)
            .unwrap_or_default();
        if text.is_empty() {
            continue;
        }
        active_columns.push(col);
        columns.push(json!({
            "column": column_letter(col),
            "header": preview(&text, 160),
        }));
    }

    let mut rows = Vec::new();
    let mut patchable_cells = Vec::new();
    let mut blank_streak = 0u32;
    for row_index in (header_row + 1)..=(workbook.max_row.min(header_row + 80)) {
        let mut leading_texts = Vec::new();
        for col in active_columns.iter().take(3) {
            let text = workbook
                .rows
                .get(&row_index)
                .and_then(|row| row.cells.get(col))
                .map(cell_display_text)
                .unwrap_or_default();
            if !text.is_empty() {
                leading_texts.push(text);
            }
        }
        if leading_texts
            .iter()
            .any(|text| TOTAL_ROW_MARKERS.iter().any(|marker| text.contains(marker)))
        {
            break;
        }

        let mut row_cells = Vec::new();
        let mut missing_cells = Vec::new();
        let mut non_empty_count = 0u32;
        for col in &active_columns {
            let coord = format!("{}{}", column_letter(*col), row_index);
            if workbook.merged_followers.contains(&coord) {
                continue;
            }
            let header = columns
                .iter()
                .find(|entry| {
                    entry.get("column").and_then(Value::as_str)
                        == Some(column_letter(*col).as_str())
                })
                .and_then(|entry| entry.get("header"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let cell = workbook
                .rows
                .get(&row_index)
                .and_then(|row| row.cells.get(col));
            let value = cell.map(cell_display_text).unwrap_or_default();
            let is_formula = cell.and_then(|c| c.formula.as_ref()).is_some();
            row_cells.push(json!({
                "cell": coord,
                "column": column_letter(*col),
                "header": header,
                "value": preview(&value, 200),
                "is_formula": is_formula,
            }));
            if !value.is_empty() || is_formula {
                non_empty_count += 1;
            }
            if (cell.is_none() || value.trim().is_empty()) && !is_formula {
                let missing = json!({
                    "cell": coord,
                    "column": column_letter(*col),
                    "header": header,
                });
                missing_cells.push(missing.clone());
                patchable_cells.push(missing);
            }
        }
        if non_empty_count == 0 && missing_cells.is_empty() {
            blank_streak += 1;
            if blank_streak >= 2 {
                break;
            }
            continue;
        }
        blank_streak = 0;
        let label = row_cells
            .iter()
            .filter_map(|cell| cell.get("value").and_then(Value::as_str))
            .find(|value| !value.trim().is_empty())
            .unwrap_or("")
            .to_string();
        rows.push(json!({
            "row_index": row_index,
            "label": if label.is_empty() { format!("row_{}", row_index) } else { label },
            "cells": row_cells,
            "missing_cells": missing_cells,
        }));
    }

    json!({
        "region_id": format!("region_{}", region_index),
        "header_row": header_row,
        "row_count": rows.len(),
        "columns": columns,
        "rows": rows,
        "patchable_cells": patchable_cells,
    })
}

fn row_non_empty_cells(
    workbook: &WorkbookModel,
    row_index: u32,
    start_col: u32,
    end_col: Option<u32>,
) -> Vec<Value> {
    let end = end_col.unwrap_or(workbook.max_col.max(start_col));
    let mut cells = Vec::new();
    for col in start_col..=end {
        let Some(cell) = workbook
            .rows
            .get(&row_index)
            .and_then(|row| row.cells.get(&col))
        else {
            continue;
        };
        let text = cell_display_text(cell);
        if text.trim().is_empty() {
            continue;
        }
        cells.push(json!({
            "cell": cell.coord,
            "column": column_letter(col),
            "column_index": col,
            "value": preview(&text, 240),
        }));
    }
    cells
}

fn cell_display_text(cell: &CellData) -> String {
    match &cell.content {
        CellContent::Empty => String::new(),
        CellContent::Number(value)
        | CellContent::Bool(value)
        | CellContent::PlainString(value)
        | CellContent::InlineString(value) => value.clone(),
        CellContent::SharedString(_, text) => text.clone(),
    }
}

fn infer_style(rows: &BTreeMap<u32, RowData>, row: u32, col: u32) -> Option<String> {
    if let Some(style) = rows.get(&row).and_then(|data| {
        data.cells.values().find_map(|cell| {
            if cell.col == col || cell.col.abs_diff(col) <= 1 {
                cell.style.clone()
            } else {
                None
            }
        })
    }) {
        return Some(style);
    }

    rows.values().find_map(|data| {
        data.cells
            .get(&col)
            .and_then(|cell| cell.style.clone())
            .or_else(|| {
                data.cells.values().find_map(|cell| {
                    if cell.col.abs_diff(col) <= 1 {
                        cell.style.clone()
                    } else {
                        None
                    }
                })
            })
    })
}

fn build_sheet_data_xml(rows: &BTreeMap<u32, RowData>) -> String {
    let mut out = String::new();
    out.push_str("<sheetData>");
    for row in rows.values() {
        out.push_str("<row");
        let mut has_r = false;
        for (key, value) in &row.attrs {
            if key == "r" {
                has_r = true;
            }
            out.push(' ');
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&xml_escape_attr(value));
            out.push('"');
        }
        if !has_r {
            out.push_str(&format!(" r=\"{}\"", row.row_index));
        }
        out.push('>');
        for cell in row.cells.values() {
            out.push_str(&build_cell_xml(cell));
        }
        out.push_str("</row>");
    }
    out.push_str("</sheetData>");
    out
}

fn build_cell_xml(cell: &CellData) -> String {
    let mut out = String::new();
    out.push_str("<c");
    out.push_str(&format!(" r=\"{}\"", cell.coord));
    if let Some(style) = &cell.style {
        out.push_str(&format!(" s=\"{}\"", xml_escape_attr(style)));
    }
    for (key, value) in &cell.extra_attrs {
        if key == "r" || key == "s" || key == "t" {
            continue;
        }
        out.push(' ');
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(&xml_escape_attr(value));
        out.push('"');
    }

    match &cell.content {
        CellContent::InlineString(text) => {
            out.push_str(" t=\"inlineStr\">");
            out.push_str("<is><t xml:space=\"preserve\">");
            out.push_str(&xml_escape_text(text));
            out.push_str("</t></is>");
        }
        CellContent::SharedString(index, _) => {
            out.push_str(" t=\"s\">");
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            out.push_str("<v>");
            out.push_str(&xml_escape_text(index));
            out.push_str("</v>");
        }
        CellContent::PlainString(text) => {
            out.push_str(" t=\"str\">");
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            out.push_str("<v>");
            out.push_str(&xml_escape_text(text));
            out.push_str("</v>");
        }
        CellContent::Bool(value) => {
            out.push_str(" t=\"b\">");
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            out.push_str("<v>");
            out.push_str(&xml_escape_text(value));
            out.push_str("</v>");
        }
        CellContent::Number(value) => {
            out.push('>');
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
            if !value.is_empty() {
                out.push_str("<v>");
                out.push_str(&xml_escape_text(value));
                out.push_str("</v>");
            }
        }
        CellContent::Empty => {
            out.push('>');
            if let Some(formula) = &cell.formula {
                out.push_str("<f>");
                out.push_str(&xml_escape_text(formula));
                out.push_str("</f>");
            }
        }
    }

    out.push_str("</c>");
    out
}

fn replace_sheet_data(xml: &str, replacement: &str) -> Result<String, String> {
    let start = xml
        .find("<sheetData>")
        .ok_or_else(|| "sheetData start not found".to_string())?;
    let end = xml
        .find("</sheetData>")
        .ok_or_else(|| "sheetData end not found".to_string())?;
    let end_index = end + "</sheetData>".len();
    let mut out = String::with_capacity(xml.len() + replacement.len());
    out.push_str(&xml[..start]);
    out.push_str(replacement);
    out.push_str(&xml[end_index..]);
    Ok(out)
}

fn empty_region() -> Value {
    json!({
        "region_id": "region_1",
        "header_row": 1,
        "row_count": 0,
        "columns": [],
        "rows": [],
        "patchable_cells": [],
    })
}

const TOTAL_ROW_MARKERS: &[&str] = &["总计", "合计", "TOTAL", "SUM"];

fn read_zip_entry(archive: &mut ZipArchive<File>, name: &str) -> Result<String, String> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| format!("read zip entry '{}' failed: {}", name, e))?;
    let mut text = String::new();
    entry
        .read_to_string(&mut text)
        .map_err(|e| format!("read zip entry '{}' content failed: {}", name, e))?;
    Ok(text)
}

fn read_optional_zip_entry(
    archive: &mut ZipArchive<File>,
    name: &str,
) -> Option<Result<String, String>> {
    match archive.by_name(name) {
        Ok(mut entry) => {
            let mut text = String::new();
            Some(
                entry
                    .read_to_string(&mut text)
                    .map(|_| text)
                    .map_err(|e| format!("read zip entry '{}' content failed: {}", name, e)),
            )
        }
        Err(_) => None,
    }
}

fn attr_value(event: &quick_xml::events::BytesStart<'_>, key: &[u8]) -> Option<String> {
    event
        .attributes()
        .flatten()
        .find(|attr| attr.key.as_ref() == key)
        .map(|attr| String::from_utf8_lossy(attr.value.as_ref()).to_string())
}

fn collect_attrs(event: &quick_xml::events::BytesStart<'_>) -> Vec<(String, String)> {
    event
        .attributes()
        .flatten()
        .map(|attr| {
            (
                String::from_utf8_lossy(attr.key.as_ref()).to_string(),
                String::from_utf8_lossy(attr.value.as_ref()).to_string(),
            )
        })
        .collect()
}

fn collect_extra_cell_attrs(event: &quick_xml::events::BytesStart<'_>) -> Vec<(String, String)> {
    event
        .attributes()
        .flatten()
        .filter_map(|attr| {
            let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
            if key == "r" || key == "s" || key == "t" {
                None
            } else {
                Some((
                    key,
                    String::from_utf8_lossy(attr.value.as_ref()).to_string(),
                ))
            }
        })
        .collect()
}

fn extend_merged_followers(followers: &mut HashSet<String>, range_ref: &str) {
    let Some((start, end)) = range_ref.split_once(':') else {
        return;
    };
    let Ok((start_row, start_col)) = parse_cell_ref(start) else {
        return;
    };
    let Ok((end_row, end_col)) = parse_cell_ref(end) else {
        return;
    };
    for row in start_row..=end_row {
        for col in start_col..=end_col {
            let coord = format!("{}{}", column_letter(col), row);
            if coord != start {
                followers.insert(coord);
            }
        }
    }
}

fn parse_cell_ref(cell_ref: &str) -> Result<(u32, u32), String> {
    let mut letters = String::new();
    let mut digits = String::new();
    for ch in cell_ref.chars() {
        if ch.is_ascii_alphabetic() {
            letters.push(ch.to_ascii_uppercase());
        } else if ch.is_ascii_digit() {
            digits.push(ch);
        }
    }
    if letters.is_empty() || digits.is_empty() {
        return Err(format!("invalid cell reference '{}'", cell_ref));
    }
    let row = digits
        .parse::<u32>()
        .map_err(|e| format!("invalid row in cell ref '{}': {}", cell_ref, e))?;
    let mut col = 0u32;
    for ch in letters.bytes() {
        col = col * 26 + u32::from(ch - b'A' + 1);
    }
    Ok((row, col))
}

fn column_letter(mut col: u32) -> String {
    let mut out = String::new();
    while col > 0 {
        let rem = ((col - 1) % 26) as u8;
        out.insert(0, (b'A' + rem) as char);
        col = (col - 1) / 26;
    }
    out
}

fn normalize_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|e| format!("resolve current dir failed: {}", e))
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

fn preview(text: &str, max_chars: usize) -> String {
    let count = text.chars().count();
    if count <= max_chars {
        return text.trim().to_string();
    }
    let mut preview: String = text.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated {} chars]", count));
    preview
}

fn decode_text(bytes: &[u8]) -> String {
    let raw = String::from_utf8_lossy(bytes);
    raw.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn resolve_project_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir
        .join("../..")
        .canonicalize()
        .map_err(|err| format!("resolve repository root failed: {}", err))?;
    Ok(root.join(path))
}

fn resolve_validator_python() -> Result<PathBuf, String> {
    let repo_root = resolve_project_path(Path::new("."))?;
    let candidates = [
        repo_root.join(".claude/skills/xlsx/.venv/bin/python3"),
        repo_root.join(".venv/bin/python3"),
    ];
    for candidate in candidates {
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    Err("validator python interpreter not found under project virtual environments".to_string())
}

fn xml_escape_text(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn xml_escape_attr(input: &str) -> String {
    xml_escape_text(input)
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assess_readiness_accepts_enveloped_candidate_cells() {
        let inspection = json!({
            "selected_region": {
                "patchable_cells": [
                    { "cell": "E5" },
                    { "cell": "F5" }
                ]
            }
        });
        let patch_candidates = json!({
            "candidates": {
                "cells": [
                    {
                        "cell": "E5",
                        "proposed_action": "fill",
                        "proposed_value": "按计划完成核心需求开发"
                    },
                    {
                        "cell": "F5",
                        "proposed_action": "fill",
                        "proposed_value": "95"
                    }
                ]
            },
            "unknowns": [],
            "assumptions": ["generic spreadsheet wording is acceptable"]
        });

        let (continuation, summary) =
            assess_readiness(&inspection, &patch_candidates, "strict").expect("assess");
        assert_eq!(
            continuation["status"],
            Value::String("commit_ready".to_string())
        );
        assert!(summary.contains("ready for strict commit"));
    }

    #[test]
    fn test_cell_value_matches_expected_handles_numeric_and_string_values() {
        assert!(cell_value_matches_expected(&json!(95), "95"));
        assert!(cell_value_matches_expected(&json!("完成"), "完成"));
        assert!(cell_value_matches_expected(&Value::Null, ""));
        assert!(!cell_value_matches_expected(&json!(96), "95"));
        assert!(!cell_value_matches_expected(&json!("完成"), "未完成"));
    }
}
