use std::collections::HashMap;

use async_trait::async_trait;
use orchestral_runtime::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult, ActionSpec};
use serde_json::{json, Value};

use super::support::{
    config_bool, parse_document_with_kreuzberg, parse_headings, params_get_string, word_count,
};

pub(super) struct DocParseAction {
    name: String,
    description: String,
    default_extract_pages: bool,
}

impl DocParseAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Parse documents with Kreuzberg Rust SDK and return normalized markdown",
            ),
            default_extract_pages: config_bool(&spec.config, "extract_pages").unwrap_or(false),
        }
    }
}

#[async_trait]
impl Action for DocParseAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new(self.name(), self.description())
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": {"type": "string"},
                    "extract_pages": {"type": "boolean"}
                },
                "required": ["source_path"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "markdown": {"type": "string"},
                    "headings": {"type": "array"},
                    "word_count": {"type": "integer"},
                    "char_count": {"type": "integer"},
                    "table_count": {"type": "integer"},
                    "page_count": {"type": ["integer", "null"]},
                    "parser": {"type": "string"}
                },
                "required": ["source_path", "markdown", "parser"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let source_path = match params_get_string(params, "source_path") {
            Some(path) if !path.is_empty() => path,
            _ => return ActionResult::error("doc_parse requires params.source_path"),
        };

        let extract_pages =
            super::support::params_get_bool(params, "extract_pages").unwrap_or(self.default_extract_pages);
        let parsed = match parse_document_with_kreuzberg(&source_path, extract_pages).await {
            Ok(parsed) => parsed,
            Err(err) => return ActionResult::error(err),
        };

        let headings = parse_headings(&parsed.markdown);
        let mut exports = HashMap::new();
        exports.insert("source_path".to_string(), json!(parsed.source_path));
        exports.insert("mime_type".to_string(), json!(parsed.mime_type));
        exports.insert("markdown".to_string(), json!(parsed.markdown));
        exports.insert("headings".to_string(), Value::Array(headings));
        exports.insert(
            "word_count".to_string(),
            json!(word_count(&parsed.markdown)),
        );
        exports.insert(
            "char_count".to_string(),
            json!(parsed.markdown.chars().count()),
        );
        exports.insert(
            "table_count".to_string(),
            json!(parsed.tables_markdown.len()),
        );
        exports.insert("tables_markdown".to_string(), json!(parsed.tables_markdown));
        exports.insert("page_count".to_string(), json!(parsed.page_count));
        exports.insert("metadata".to_string(), parsed.metadata);
        exports.insert("parser".to_string(), json!("kreuzberg_rust_sdk"));
        ActionResult::success_with(exports)
    }
}
