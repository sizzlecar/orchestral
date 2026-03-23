use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;
use orchestral_core::types::{VerifyDecision, VerifyStatus};

use super::factory::ActionBuildError;

pub fn build_codebase_action(
    spec: &ActionSpec,
) -> Result<Option<Box<dyn Action>>, ActionBuildError> {
    let action: Box<dyn Action> = match spec.kind.as_str() {
        "codebase_collect_targets" => Box::new(CodebaseCollectTargetsAction::from_spec(spec)),
        "codebase_collect_results" => Box::new(CodebaseCollectResultsAction::from_spec(spec)),
        "codebase_aggregate_verify" => Box::new(CodebaseAggregateVerifyAction::from_spec(spec)),
        "codebase_export_summary" => Box::new(CodebaseExportSummaryAction::from_spec(spec)),
        _ => return Ok(None),
    };
    Ok(Some(action))
}

#[derive(Debug)]
struct CodebaseCollectTargetsAction {
    name: String,
    description: String,
}

impl CodebaseCollectTargetsAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Collect explicit mixed-artifact targets"),
        }
    }
}

#[async_trait]
impl Action for CodebaseCollectTargetsAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("codebase")
            .with_capabilities(["filesystem_read", "pure"])
            .with_input_kinds(["text"])
            .with_output_kinds(["path", "structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "user_request": { "type": "string" }
                },
                "required": ["user_request"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "instruction_path": { "type": "string" },
                    "instruction_source_paths": { "type": "array", "items": { "type": "string" } },
                    "spreadsheet_path": { "type": "string" },
                    "structured_path": { "type": "string" },
                    "structured_source_paths": { "type": "array", "items": { "type": "string" } }
                },
                "required": [
                    "instruction_path",
                    "instruction_source_paths",
                    "spreadsheet_path",
                    "structured_path",
                    "structured_source_paths"
                ]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(user_request) = input.params.get("user_request").and_then(Value::as_str) else {
            return ActionResult::error("Missing user_request for codebase_collect_targets");
        };
        match collect_targets(user_request) {
            Ok(exports) => ActionResult::success_with(exports),
            Err(error) => ActionResult::error(error),
        }
    }
}

#[derive(Debug)]
struct CodebaseCollectResultsAction {
    name: String,
    description: String,
}

impl CodebaseCollectResultsAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Collect mixed-artifact run results"),
        }
    }
}

#[async_trait]
impl Action for CodebaseCollectResultsAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("codebase")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "spreadsheet_path": { "type": "string" },
                    "structured_paths": { "type": "array", "items": { "type": "string" } },
                    "execution_summary": { "type": "string" }
                },
                "required": ["spreadsheet_path", "structured_paths", "execution_summary"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "collected_result": { "type": "object" },
                    "summary": { "type": "string" }
                },
                "required": ["collected_result", "summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(spreadsheet_path) = input.params.get("spreadsheet_path").and_then(Value::as_str)
        else {
            return ActionResult::error("Missing spreadsheet_path for codebase_collect_results");
        };
        let Some(structured_paths) = input
            .params
            .get("structured_paths")
            .and_then(Value::as_array)
        else {
            return ActionResult::error("Missing structured_paths for codebase_collect_results");
        };
        let Some(execution_summary) = input
            .params
            .get("execution_summary")
            .and_then(Value::as_str)
        else {
            return ActionResult::error("Missing execution_summary for codebase_collect_results");
        };

        let structured_paths = structured_paths
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>();
        let collected_result = json!({
            "spreadsheet_path": spreadsheet_path,
            "structured_paths": structured_paths,
            "execution_summary": execution_summary,
        });
        ActionResult::success_with(
            [
                ("collected_result".to_string(), collected_result),
                (
                    "summary".to_string(),
                    Value::String(execution_summary.to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }
}

#[derive(Debug)]
struct CodebaseAggregateVerifyAction {
    name: String,
    description: String,
}

impl CodebaseAggregateVerifyAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Aggregate mixed-artifact verify decisions"),
        }
    }
}

#[async_trait]
impl Action for CodebaseAggregateVerifyAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("codebase")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "spreadsheet_verify_decision": { "type": "object" },
                    "structured_verify_decision": { "type": "object" },
                    "collected_result": { "type": "object" }
                },
                "required": [
                    "spreadsheet_verify_decision",
                    "structured_verify_decision",
                    "collected_result"
                ]
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
        let Some(spreadsheet_verify_decision) = input.params.get("spreadsheet_verify_decision")
        else {
            return ActionResult::error(
                "Missing spreadsheet_verify_decision for codebase_aggregate_verify",
            );
        };
        let Some(structured_verify_decision) = input.params.get("structured_verify_decision")
        else {
            return ActionResult::error(
                "Missing structured_verify_decision for codebase_aggregate_verify",
            );
        };
        let Some(collected_result) = input.params.get("collected_result") else {
            return ActionResult::error("Missing collected_result for codebase_aggregate_verify");
        };

        let spreadsheet = match parse_verify_decision(spreadsheet_verify_decision) {
            Ok(value) => value,
            Err(error) => return ActionResult::error(error),
        };
        let structured = match parse_verify_decision(structured_verify_decision) {
            Ok(value) => value,
            Err(error) => return ActionResult::error(error),
        };
        let execution_summary = collected_result
            .get("execution_summary")
            .and_then(Value::as_str)
            .unwrap_or_default();

        let (status, reason) = if spreadsheet.status == VerifyStatus::Passed
            && structured.status == VerifyStatus::Passed
        {
            (
                VerifyStatus::Passed,
                "multi-artifact verification passed".to_string(),
            )
        } else if spreadsheet.status != VerifyStatus::Passed {
            (VerifyStatus::Failed, spreadsheet.reason)
        } else {
            (VerifyStatus::Failed, structured.reason)
        };

        let verify_decision = VerifyDecision {
            status,
            reason: reason.clone(),
            evidence: json!({
                "spreadsheet_verify": spreadsheet_verify_decision,
                "structured_verify": structured_verify_decision,
                "collected_result": collected_result,
            }),
        };
        let summary = if verify_decision.status == VerifyStatus::Passed {
            if execution_summary.is_empty() {
                "Multi-artifact run verified.".to_string()
            } else {
                format!("Multi-artifact run verified. {}", execution_summary)
            }
        } else {
            format!("Multi-artifact verify failed: {}", reason)
        };

        ActionResult::success_with(
            [
                (
                    "verify_decision".to_string(),
                    serde_json::to_value(verify_decision).unwrap_or(Value::Null),
                ),
                ("summary".to_string(), Value::String(summary)),
            ]
            .into_iter()
            .collect(),
        )
    }
}

#[derive(Debug)]
struct CodebaseExportSummaryAction {
    name: String,
    description: String,
}

impl CodebaseExportSummaryAction {
    fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or("Export a unified mixed-artifact summary"),
        }
    }
}

#[async_trait]
impl Action for CodebaseExportSummaryAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_category("codebase")
            .with_capabilities(["pure", "structured_output"])
            .with_input_kinds(["structured"])
            .with_output_kinds(["structured", "text"])
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "collected_result": { "type": "object" },
                    "verify_decision": { "type": "object" }
                },
                "required": ["collected_result", "verify_decision"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "summary": { "type": "string" }
                },
                "required": ["summary"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let Some(collected_result) = input.params.get("collected_result") else {
            return ActionResult::error("Missing collected_result for codebase_export_summary");
        };
        let Some(verify_decision) = input.params.get("verify_decision") else {
            return ActionResult::error("Missing verify_decision for codebase_export_summary");
        };
        let verify_decision = match parse_verify_decision(verify_decision) {
            Ok(value) => value,
            Err(error) => return ActionResult::error(error),
        };
        let execution_summary = collected_result
            .get("execution_summary")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let spreadsheet_path = collected_result
            .get("spreadsheet_path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let structured_paths = collected_result
            .get("structured_paths")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect::<Vec<_>>();

        let mut summary = String::new();
        if verify_decision.status == VerifyStatus::Passed {
            summary.push_str("Multi-artifact run completed and verified.");
        } else {
            summary.push_str("Multi-artifact run failed verification.");
        }
        if !execution_summary.is_empty() {
            summary.push_str("\n\n");
            summary.push_str(execution_summary);
        }
        if !spreadsheet_path.is_empty() || !structured_paths.is_empty() {
            summary.push_str("\n\nUpdated artifacts:");
            if !spreadsheet_path.is_empty() {
                summary.push_str("\n- spreadsheet: ");
                summary.push_str(spreadsheet_path);
            }
            if !structured_paths.is_empty() {
                summary.push_str("\n- structured: ");
                summary.push_str(&structured_paths.join(", "));
            }
        }

        ActionResult::success_with_one("summary", Value::String(summary))
    }
}

fn collect_targets(user_request: &str) -> Result<HashMap<String, Value>, String> {
    let mut instruction_path = None;
    let mut spreadsheet_path = None;
    let mut structured_path = None;

    for token in extract_existing_path_tokens(user_request)? {
        if instruction_path.is_none() && is_instruction_path(&token) {
            instruction_path = Some(token.clone());
            continue;
        }
        if spreadsheet_path.is_none() && is_spreadsheet_path(&token) {
            spreadsheet_path = Some(token.clone());
            continue;
        }
        if structured_path.is_none() && is_structured_path(&token) {
            structured_path = Some(token.clone());
        }
    }

    let instruction_path =
        instruction_path.ok_or_else(|| "Missing explicit instruction document path".to_string())?;
    let spreadsheet_path =
        spreadsheet_path.ok_or_else(|| "Missing explicit spreadsheet path".to_string())?;
    let structured_path =
        structured_path.ok_or_else(|| "Missing explicit structured file path".to_string())?;

    Ok([
        (
            "instruction_path".to_string(),
            Value::String(instruction_path.clone()),
        ),
        (
            "instruction_source_paths".to_string(),
            Value::Array(vec![Value::String(instruction_path)]),
        ),
        (
            "spreadsheet_path".to_string(),
            Value::String(spreadsheet_path),
        ),
        (
            "structured_path".to_string(),
            Value::String(structured_path.clone()),
        ),
        (
            "structured_source_paths".to_string(),
            Value::Array(vec![Value::String(structured_path)]),
        ),
    ]
    .into_iter()
    .collect())
}

fn extract_existing_path_tokens(input: &str) -> Result<Vec<String>, String> {
    let cwd =
        std::env::current_dir().map_err(|err| format!("resolve current dir failed: {err}"))?;
    let mut results = Vec::new();
    for token in input.split_whitespace().map(trim_path_token) {
        if token.is_empty() || !looks_like_path_token(&token) {
            continue;
        }
        let candidate = PathBuf::from(&token);
        let resolved = if candidate.is_absolute() {
            candidate
        } else {
            cwd.join(&candidate)
        };
        if resolved.exists() {
            results.push(display_path(&cwd, &resolved));
        }
    }
    results.sort();
    results.dedup();
    Ok(results)
}

fn trim_path_token(token: &str) -> String {
    token
        .trim()
        .trim_matches(|ch: char| {
            matches!(
                ch,
                '`' | '"'
                    | '\''
                    | ','
                    | '，'
                    | '。'
                    | ';'
                    | '；'
                    | '('
                    | ')'
                    | '['
                    | ']'
                    | '{'
                    | '}'
                    | '<'
                    | '>'
            )
        })
        .to_string()
}

fn looks_like_path_token(token: &str) -> bool {
    token.contains('/')
        || token.contains('.')
        || token.starts_with("./")
        || token.starts_with("../")
}

fn display_path(cwd: &Path, path: &Path) -> String {
    path.strip_prefix(cwd)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn is_instruction_path(path: &str) -> bool {
    matches!(
        Path::new(path)
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref(),
        Some("md" | "markdown" | "txt")
    )
}

fn is_spreadsheet_path(path: &str) -> bool {
    matches!(
        Path::new(path)
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref(),
        Some("xlsx" | "xlsm" | "xls")
    )
}

fn is_structured_path(path: &str) -> bool {
    matches!(
        Path::new(path)
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref(),
        Some("yaml" | "yml" | "json" | "toml")
    )
}

fn parse_verify_decision(value: &Value) -> Result<VerifyDecision, String> {
    serde_json::from_value(value.clone())
        .map_err(|err| format!("invalid verify_decision payload: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_path_token_removes_wrappers() {
        assert_eq!(trim_path_token("`./docs/a.md`, "), "./docs/a.md");
    }
}
