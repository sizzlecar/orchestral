use std::collections::HashMap;

use async_trait::async_trait;
use orchestral_runtime::action::{Action, ActionContext, ActionInput, ActionResult, ActionSpec};
use serde_json::{json, Value};

use super::support::{
    config_bool, config_string, config_u64, is_markdown_format, params_get_bool,
    params_get_string, params_get_string_array, parse_document_with_kreuzberg,
    run_pandoc_from_markdown, write_text,
};

pub(super) struct DocMergeAction {
    name: String,
    description: String,
    pandoc_command: String,
    timeout_ms: Option<u64>,
    add_source_headers: bool,
}

impl DocMergeAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec
                .description_or("Merge multiple documents and optionally convert via pandoc"),
            pandoc_command: config_string(&spec.config, "pandoc_command")
                .unwrap_or_else(|| "pandoc".to_string()),
            timeout_ms: config_u64(&spec.config, "timeout_ms"),
            add_source_headers: config_bool(&spec.config, "add_source_headers").unwrap_or(true),
        }
    }
}

#[async_trait]
impl Action for DocMergeAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let source_paths = match params_get_string_array(params, "source_paths") {
            Ok(v) => v,
            Err(err) => return ActionResult::error(err),
        };
        let inline_markdowns = match params_get_string_array(params, "markdowns") {
            Ok(v) => v,
            Err(err) => return ActionResult::error(err),
        };

        if source_paths.is_empty() && inline_markdowns.is_empty() {
            return ActionResult::error(
                "doc_merge requires params.source_paths or params.markdowns",
            );
        }

        let separator =
            params_get_string(params, "separator").unwrap_or_else(|| "\n\n---\n\n".to_string());
        let add_source_headers =
            params_get_bool(params, "add_source_headers").unwrap_or(self.add_source_headers);

        let mut chunks = Vec::new();
        for path in &source_paths {
            let parsed = match parse_document_with_kreuzberg(path, false).await {
                Ok(parsed) => parsed,
                Err(err) => return ActionResult::error(err),
            };
            if add_source_headers {
                chunks.push(format!("## Source: {}\n\n{}", path, parsed.markdown));
            } else {
                chunks.push(parsed.markdown);
            }
        }
        chunks.extend(inline_markdowns);

        let merged_markdown = chunks.join(&separator);
        let to_format = params_get_string(params, "to_format")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "markdown".to_string());
        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let extra_args = match params_get_string_array(params, "extra_args") {
            Ok(args) => args,
            Err(err) => return ActionResult::error(err),
        };

        let mut exports = HashMap::new();
        exports.insert("mode".to_string(), json!("merge"));
        exports.insert("source_count".to_string(), json!(source_paths.len()));
        exports.insert("target_path".to_string(), json!(target_path));
        exports.insert("to_format".to_string(), json!(to_format));
        exports.insert("merged_markdown".to_string(), json!(merged_markdown));

        if is_markdown_format(&to_format) {
            if let Some(path) = target_path.as_deref() {
                let bytes = match write_text(path, &merged_markdown).await {
                    Ok(bytes) => bytes,
                    Err(err) => return ActionResult::error(err),
                };
                exports.insert("wrote_file".to_string(), json!(true));
                exports.insert("bytes".to_string(), json!(bytes));
                exports.insert("content".to_string(), json!(""));
            } else {
                exports.insert("wrote_file".to_string(), json!(false));
                exports.insert("bytes".to_string(), Value::Null);
                exports.insert("content".to_string(), json!(merged_markdown));
            }
            return ActionResult::success_with(exports);
        }

        let converted = match run_pandoc_from_markdown(
            &self.pandoc_command,
            &merged_markdown,
            &to_format,
            target_path.as_deref(),
            &extra_args,
            self.timeout_ms,
        )
        .await
        {
            Ok(result) => result,
            Err(err) => return ActionResult::error(err),
        };

        exports.insert("wrote_file".to_string(), json!(converted.wrote_file));
        exports.insert("bytes".to_string(), json!(converted.bytes));
        exports.insert("content".to_string(), json!(converted.content));
        exports.insert("pandoc_command".to_string(), json!(converted.command));
        ActionResult::success_with(exports)
    }
}
