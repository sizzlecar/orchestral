use std::collections::HashMap;

use async_trait::async_trait;
use orchestral_runtime::action::{Action, ActionContext, ActionInput, ActionResult, ActionSpec};
use serde_json::{json, Value};

use super::support::{
    config_string, config_u64, is_markdown_format, params_get_string, params_get_string_array,
    render_sections, run_pandoc_from_markdown, write_text,
};

pub(super) struct DocGenerateAction {
    name: String,
    description: String,
    pandoc_command: String,
    timeout_ms: Option<u64>,
}

impl DocGenerateAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Generate structured documents and optionally convert format via pandoc",
            ),
            pandoc_command: config_string(&spec.config, "pandoc_command")
                .unwrap_or_else(|| "pandoc".to_string()),
            timeout_ms: config_u64(&spec.config, "timeout_ms"),
        }
    }
}

#[async_trait]
impl Action for DocGenerateAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let title = params_get_string(params, "title")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "Generated Document".to_string());
        let body = params_get_string(params, "body").unwrap_or_default();
        let source_markdowns = match params_get_string_array(params, "source_markdowns") {
            Ok(v) => v,
            Err(err) => return ActionResult::error(err),
        };
        let sections = params
            .get("sections")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        if body.trim().is_empty() && sections.is_empty() && source_markdowns.is_empty() {
            return ActionResult::error(
                "doc_generate requires at least one of params.body / params.sections / params.source_markdowns",
            );
        }

        let mut markdown = format!("# {}\n\n", title.trim());
        if !body.trim().is_empty() {
            markdown.push_str(body.trim());
            markdown.push_str("\n\n");
        }
        match render_sections(&sections) {
            Ok(rendered) => markdown.push_str(&rendered),
            Err(err) => return ActionResult::error(err),
        }

        if !source_markdowns.is_empty() {
            markdown.push_str("## Source Notes\n\n");
            for (idx, src) in source_markdowns.iter().enumerate() {
                markdown.push_str(&format!("### Source {}\n\n{}\n\n", idx + 1, src));
            }
        }

        let to_format = params_get_string(params, "to_format")
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "markdown".to_string());
        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let extra_args = match params_get_string_array(params, "extra_args") {
            Ok(args) => args,
            Err(err) => return ActionResult::error(err),
        };

        let mut exports = HashMap::new();
        exports.insert("mode".to_string(), json!("generate"));
        exports.insert("to_format".to_string(), json!(to_format));
        exports.insert("target_path".to_string(), json!(target_path));
        exports.insert("generated_markdown".to_string(), json!(markdown));

        if is_markdown_format(&to_format) {
            if let Some(path) = target_path.as_deref() {
                let bytes = match write_text(path, &markdown).await {
                    Ok(bytes) => bytes,
                    Err(err) => return ActionResult::error(err),
                };
                exports.insert("wrote_file".to_string(), json!(true));
                exports.insert("bytes".to_string(), json!(bytes));
                exports.insert("content".to_string(), json!(""));
            } else {
                exports.insert("wrote_file".to_string(), json!(false));
                exports.insert("bytes".to_string(), Value::Null);
                exports.insert("content".to_string(), json!(markdown));
            }
            return ActionResult::success_with(exports);
        }

        let converted = match run_pandoc_from_markdown(
            &self.pandoc_command,
            &markdown,
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
