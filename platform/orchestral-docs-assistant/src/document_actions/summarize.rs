use std::collections::HashMap;

use async_trait::async_trait;
use orchestral_runtime::action::{Action, ActionContext, ActionInput, ActionResult, ActionSpec};
use serde_json::json;

use super::support::{
    build_outline, config_string, config_u64, doc_input_markdown_or_parse, params_get_string,
    params_get_u64, parse_headings, summarize_text, word_count, write_text,
};

pub(super) struct DocSummarizeAction {
    name: String,
    description: String,
    default_sentences: usize,
    default_mode: String,
}

impl DocSummarizeAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Generate summary/outline/overview markdown from normalized document text",
            ),
            default_sentences: config_u64(&spec.config, "default_sentences")
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(5),
            default_mode: config_string(&spec.config, "default_mode")
                .unwrap_or_else(|| "summary".to_string()),
        }
    }
}

#[async_trait]
impl Action for DocSummarizeAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let (source_path, markdown) = match doc_input_markdown_or_parse(params, false).await {
            Ok(v) => v,
            Err(err) => return ActionResult::error(format!("doc_summarize {}", err)),
        };

        let mode = params_get_string(params, "mode")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.default_mode.clone())
            .to_ascii_lowercase();
        let summary_sentences = params_get_u64(params, "summary_sentences")
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(self.default_sentences)
            .max(1);

        let headings = parse_headings(&markdown);
        let summary = summarize_text(&markdown, summary_sentences);
        let outline = build_outline(&headings);

        let generated_markdown = if mode == "outline" {
            format!("# Document Outline\n\n{}\n", outline)
        } else if mode == "overview" {
            format!(
                "# Document Overview\n\n- Word count: {}\n- Heading count: {}\n\n## Summary\n\n{}\n",
                word_count(&markdown),
                headings.len(),
                summary
            )
        } else {
            format!(
                "# Document Summary\n\n{}\n\n## Key Sections\n\n{}\n",
                summary, outline
            )
        };

        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let (wrote_file, bytes) = if let Some(path) = target_path.as_deref() {
            match write_text(path, &generated_markdown).await {
                Ok(bytes) => (true, Some(bytes)),
                Err(err) => return ActionResult::error(err),
            }
        } else {
            (false, None)
        };

        let mut exports = HashMap::new();
        exports.insert("source_path".to_string(), json!(source_path));
        exports.insert("mode".to_string(), json!(mode));
        exports.insert("generated_markdown".to_string(), json!(generated_markdown));
        exports.insert("target_path".to_string(), json!(target_path));
        exports.insert("wrote_file".to_string(), json!(wrote_file));
        exports.insert("bytes".to_string(), json!(bytes));
        exports.insert("word_count".to_string(), json!(word_count(&markdown)));
        exports.insert("heading_count".to_string(), json!(headings.len()));
        exports.insert("parser".to_string(), json!("kreuzberg_rust_sdk"));
        ActionResult::success_with(exports)
    }
}
