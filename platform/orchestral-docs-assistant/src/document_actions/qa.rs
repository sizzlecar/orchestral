use std::collections::HashMap;

use async_trait::async_trait;
use orchestral_runtime::action::{Action, ActionContext, ActionInput, ActionResult, ActionSpec};
use serde_json::json;

use super::support::{
    build_qa_answer, chunk_markdown, config_u64, doc_input_markdown_or_parse, params_get_string,
    params_get_u64, score_chunks,
};

pub(super) struct DocQaAction {
    name: String,
    description: String,
    default_top_k: usize,
}

impl DocQaAction {
    pub(super) fn from_spec(spec: &ActionSpec) -> Self {
        Self {
            name: spec.name.clone(),
            description: spec
                .description_or("Answer questions from document content with evidence snippets"),
            default_top_k: config_u64(&spec.config, "default_top_k")
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(3),
        }
    }
}

#[async_trait]
impl Action for DocQaAction {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let params = &input.params;
        let question = match params_get_string(params, "question") {
            Some(q) if !q.trim().is_empty() => q,
            _ => return ActionResult::error("doc_qa requires params.question"),
        };

        let top_k = params_get_u64(params, "top_k")
            .and_then(|v| usize::try_from(v).ok())
            .unwrap_or(self.default_top_k)
            .max(1);

        let (source_path, markdown) = match doc_input_markdown_or_parse(params, false).await {
            Ok(v) => v,
            Err(err) => return ActionResult::error(format!("doc_qa {}", err)),
        };

        let chunks = chunk_markdown(&markdown);
        let scored = score_chunks(&question, &chunks, top_k);
        let answer = build_qa_answer(&question, &scored);

        let evidence = scored
            .iter()
            .enumerate()
            .map(|(idx, (score, overlap, excerpt))| {
                json!({
                    "rank": idx + 1,
                    "score": score,
                    "token_overlap": overlap,
                    "excerpt": excerpt,
                })
            })
            .collect::<Vec<_>>();

        let mut exports = HashMap::new();
        exports.insert("question".to_string(), json!(question));
        exports.insert("answer".to_string(), json!(answer));
        exports.insert("source_path".to_string(), json!(source_path));
        exports.insert("evidence".to_string(), json!(evidence));
        exports.insert("top_k".to_string(), json!(top_k));
        ActionResult::success_with(exports)
    }
}
