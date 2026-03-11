use std::collections::HashMap;

use async_trait::async_trait;
use orchestral_runtime::action::{
    Action, ActionContext, ActionInput, ActionMeta, ActionResult, ActionSpec,
};
use serde_json::json;

use super::support::{
    config_string, config_u64, normalize_to_format, params_get_bool, params_get_string,
    params_get_string_array, parse_document_with_kreuzberg, run_pandoc_from_markdown,
};

pub(super) struct DocConvertAction {
    name: String,
    description: String,
    pandoc_command: String,
    default_to_format: String,
    timeout_ms: Option<u64>,
}

impl DocConvertAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec.description_or(
                "Convert documents using pandoc with markdown normalized by Kreuzberg parser",
            ),
            pandoc_command: config_string(&spec.config, "pandoc_command")
                .unwrap_or_else(|| "pandoc".to_string()),
            default_to_format: config_string(&spec.config, "default_to_format")
                .unwrap_or_else(|| "gfm-raw_html".to_string()),
            timeout_ms: config_u64(&spec.config, "timeout_ms"),
        }
    }
}

#[async_trait]
impl Action for DocConvertAction {
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
                    "markdown": {"type": "string"},
                    "to_format": {"type": "string"},
                    "target_path": {"type": "string"},
                    "extra_args": {"type": "array", "items": {"type": "string"}},
                    "return_markdown": {"type": "boolean"}
                }
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "source_path": {"type": ["string", "null"]},
                    "to_format": {"type": "string"},
                    "target_path": {"type": ["string", "null"]},
                    "wrote_file": {"type": "boolean"},
                    "bytes": {"type": ["integer", "null"]},
                    "content": {"type": "string"},
                    "pandoc_command": {"type": "string"},
                    "parser": {"type": "string"}
                },
                "required": ["to_format", "wrote_file", "pandoc_command", "parser"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let source_path = params_get_string(params, "source_path").filter(|v| !v.is_empty());
        let markdown_input = params_get_string(params, "markdown").filter(|v| !v.is_empty());

        let markdown = if let Some(markdown) = markdown_input {
            markdown
        } else if let Some(path) = source_path.as_ref() {
            match parse_document_with_kreuzberg(path, false).await {
                Ok(parsed) => parsed.markdown,
                Err(err) => return ActionResult::error(err),
            }
        } else {
            return ActionResult::error(
                "doc_convert requires params.source_path or params.markdown",
            );
        };

        let to_format = normalize_to_format(
            params_get_string(params, "to_format"),
            &self.default_to_format,
        );
        let target_path = params_get_string(params, "target_path").filter(|v| !v.is_empty());
        let extra_args = match params_get_string_array(params, "extra_args") {
            Ok(args) => args,
            Err(err) => return ActionResult::error(err),
        };

        let return_markdown = params_get_bool(params, "return_markdown").unwrap_or(false);

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

        let mut exports = HashMap::new();
        exports.insert("source_path".to_string(), json!(source_path));
        exports.insert("to_format".to_string(), json!(to_format));
        exports.insert("target_path".to_string(), json!(converted.target_path));
        exports.insert("wrote_file".to_string(), json!(converted.wrote_file));
        exports.insert("bytes".to_string(), json!(converted.bytes));
        exports.insert("content".to_string(), json!(converted.content));
        exports.insert("pandoc_command".to_string(), json!(converted.command));
        exports.insert("parser".to_string(), json!("kreuzberg_rust_sdk"));
        if return_markdown {
            exports.insert("markdown".to_string(), json!(markdown));
        }
        ActionResult::success_with(exports)
    }
}
