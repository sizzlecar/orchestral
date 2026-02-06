use std::sync::Arc;
use std::{collections::HashSet, fmt::Write};

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info};

use orchestral_core::planner::{HistoryItem, PlanError, Planner, PlannerContext, PlannerOutput};
use orchestral_core::types::{Intent, Plan, Step};

const MAX_PROMPT_LOG_CHARS: usize = 4_000;
const MAX_LLM_OUTPUT_LOG_CHARS: usize = 8_000;

/// LLM request payload
#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub system: String,
    pub user: String,
    pub model: String,
    pub temperature: f32,
}

pub type StreamChunkCallback = Arc<dyn Fn(String) + Send + Sync>;

/// LLM client trait
#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError>;

    async fn complete_stream(
        &self,
        request: LlmRequest,
        on_chunk: StreamChunkCallback,
    ) -> Result<String, LlmError> {
        let full = self.complete(request).await?;
        for token in full.split_inclusive(char::is_whitespace) {
            if !token.is_empty() {
                on_chunk(token.to_string());
            }
        }
        Ok(full)
    }
}

#[async_trait]
impl LlmClient for Arc<dyn LlmClient> {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError> {
        (**self).complete(request).await
    }

    async fn complete_stream(
        &self,
        request: LlmRequest,
        on_chunk: StreamChunkCallback,
    ) -> Result<String, LlmError> {
        (**self).complete_stream(request, on_chunk).await
    }
}

/// LLM errors
#[derive(Debug, Error)]
pub enum LlmError {
    #[error("http error: {0}")]
    Http(String),
    #[error("response error: {0}")]
    Response(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Planner config for LLM
#[derive(Debug, Clone)]
pub struct LlmPlannerConfig {
    pub model: String,
    pub temperature: f32,
    pub max_history: usize,
    pub system_prompt: String,
}

impl Default for LlmPlannerConfig {
    fn default() -> Self {
        Self {
            model: "gpt-4o-mini".to_string(),
            temperature: 0.2,
            max_history: 20,
            system_prompt: "You are a task planner. Return ONLY valid JSON for the Plan."
                .to_string(),
        }
    }
}

/// LLM-based planner
pub struct LlmPlanner<C: LlmClient> {
    pub client: C,
    pub config: LlmPlannerConfig,
}

impl<C: LlmClient> LlmPlanner<C> {
    pub fn new(client: C, config: LlmPlannerConfig) -> Self {
        Self { client, config }
    }

    fn build_prompt(&self, intent: &Intent, context: &PlannerContext) -> (String, String) {
        let system = build_system_prompt(&self.config.system_prompt, context);
        let mut user = String::new();
        user.push_str(&format!("Intent:\n{}\n\n", intent.content));

        if !context.history.is_empty() {
            user.push_str("History:\n");
            for item in select_history_for_prompt(&context.history, self.config.max_history) {
                user.push_str(&format!("- {}: {}\n", item.role, item.content));
            }
            user.push('\n');
        }

        user.push_str("Return ONE JSON object in one of these shapes:\n");
        user.push_str(
            r#"{"type":"WORKFLOW","goal":"...","steps":[{"id":"s1","action":"action_name","params":{},"io_bindings":[{"from":"s1.output_key","to":"input_key","required":true}]}],"confidence":0.0}"#,
        );
        user.push_str("\n");
        user.push_str(r#"{"type":"DIRECT_RESPONSE","message":"..."}"#);
        user.push_str("\n");
        user.push_str(r#"{"type":"CLARIFICATION","question":"..."}"#);
        user.push_str(
            "\nUse only action names listed in Action Catalog when type is WORKFLOW. Return JSON only.\n",
        );
        user.push('\n');

        (system, user)
    }
}

fn select_history_for_prompt<'a>(
    history: &'a [HistoryItem],
    max_history: usize,
) -> Vec<&'a HistoryItem> {
    if max_history == 0 {
        return Vec::new();
    }

    let dialog: Vec<&HistoryItem> = history
        .iter()
        .filter(|h| h.role == "user" || h.role == "assistant")
        .collect();

    if dialog.len() <= max_history {
        return dialog;
    }

    let head_keep = if max_history >= 8 { 2 } else { 1 };
    let tail_keep = max_history.saturating_sub(head_keep);
    let split = dialog.len().saturating_sub(tail_keep);

    let mut selected = Vec::with_capacity(max_history);
    selected.extend(dialog.iter().take(head_keep).copied());
    selected.extend(dialog.iter().skip(split).copied());
    selected
}

fn build_system_prompt(base: &str, context: &PlannerContext) -> String {
    let mut system = String::new();
    system.push_str(
        "You are Orchestral Planner, the planning component of the Orchestral runtime.\n",
    );
    system.push_str(
        "Project context:\n- Workspace: Rust multi-crate project.\n- Core crates: orchestral-core, orchestral-runtime, orchestral-actions, orchestral-stores, orchestral-planners, orchestral-cli.\n- Execution model: Intent -> Plan -> Normalize -> Execute.\n",
    );
    system.push('\n');
    system.push_str(base.trim());
    if !context.runtime_info.os.is_empty() {
        system.push_str("\n\nExecution Environment:\n");
        system.push_str(&format!(
            "- os: {}\n- os_family: {}\n- arch: {}\n",
            context.runtime_info.os, context.runtime_info.os_family, context.runtime_info.arch
        ));
        if let Some(shell) = &context.runtime_info.shell {
            system.push_str(&format!("- shell: {}\n", shell));
        }
    }
    system.push_str("\n\nPlanning Rules:\n");
    system.push_str("1) Return ONLY one valid JSON object matching one allowed output shape.\n");
    system.push_str("2) If no tool/action execution is needed, return DIRECT_RESPONSE.\n");
    system
        .push_str("3) If information is missing, return CLARIFICATION with a concrete question.\n");
    system.push_str("4) Only return WORKFLOW when execution is required.\n");
    system.push_str("5) For WORKFLOW: step.id must be unique and stable.\n");
    system.push_str("6) For WORKFLOW: step.params must satisfy selected action input_schema.\n");
    system.push_str("7) For WORKFLOW: prefer minimal steps; omit optional fields unless needed.\n");
    system.push_str("8) For WORKFLOW: if a step consumes upstream output, declare io_bindings.\n");
    system.push_str("9) Do not invent action names not listed in Action Catalog.\n");
    system.push_str(
        "10) If clarifying via WORKFLOW, use wait_user; otherwise return CLARIFICATION.\n",
    );
    system.push_str(
        "11) For wait_user/wait_event/system steps, set kind explicitly; normal action steps can omit kind.\n",
    );
    system.push_str(
        "12) When generating step.params, strictly follow Action Catalog input fields.\n",
    );
    system.push_str(
        "13) Any shell/file operation must be compatible with current host platform and shell.\n",
    );
    system.push_str(
        "14) Only use `echo` when user explicitly asks to echo/repeat text; otherwise avoid `echo`.\n",
    );
    system.push_str(
        "15) For HTTP headers requests, use `http` method `HEAD` and return headers directly.\n",
    );
    system.push_str("\nAction Catalog:\n");
    for action in &context.available_actions {
        append_action_catalog_entry(
            &mut system,
            &action.name,
            &action.description,
            &action.input_schema,
            &action.output_schema,
        );
    }
    system
}

fn append_action_catalog_entry(
    buf: &mut String,
    name: &str,
    description: &str,
    input_schema: &serde_json::Value,
    output_schema: &serde_json::Value,
) {
    let _ = writeln!(buf, "- name: {}", name);
    let _ = writeln!(buf, "  description: {}", description);
    append_schema_fields(buf, "input_fields", input_schema);
    append_schema_fields(buf, "output_fields", output_schema);
    let _ = writeln!(buf, "  input_schema: {}", input_schema);
    let _ = writeln!(buf, "  output_schema: {}", output_schema);
}

fn append_schema_fields(buf: &mut String, label: &str, schema: &serde_json::Value) {
    if schema.is_null() {
        let _ = writeln!(buf, "  {}: []", label);
        return;
    }

    let Some(properties) = schema.get("properties").and_then(|v| v.as_object()) else {
        let _ = writeln!(buf, "  {}: []", label);
        return;
    };

    let required = schema_required_fields(schema);
    let mut keys: Vec<&str> = properties.keys().map(String::as_str).collect();
    keys.sort_unstable();

    let _ = writeln!(buf, "  {}:", label);
    for key in keys {
        let Some(field_schema) = properties.get(key) else {
            continue;
        };
        let type_hint = schema_type_hint(field_schema);
        let required_label = if required.contains(key) {
            "required"
        } else {
            "optional"
        };
        let mut extras = Vec::new();
        if let Some(desc) = field_schema.get("description").and_then(|v| v.as_str()) {
            extras.push(format!("desc={}", desc));
        }
        if let Some(default) = field_schema.get("default") {
            extras.push(format!(
                "default={}",
                truncate_for_log(&default.to_string(), 120)
            ));
        }
        if let Some(example) = field_schema.get("example").or_else(|| {
            field_schema
                .get("examples")
                .and_then(|v| v.as_array()?.first())
        }) {
            extras.push(format!(
                "example={}",
                truncate_for_log(&example.to_string(), 120)
            ));
        }

        if extras.is_empty() {
            let _ = writeln!(buf, "    - {} ({}, {})", key, type_hint, required_label);
        } else {
            let _ = writeln!(
                buf,
                "    - {} ({}, {}): {}",
                key,
                type_hint,
                required_label,
                extras.join("; ")
            );
        }
    }
}

fn schema_required_fields(schema: &serde_json::Value) -> HashSet<String> {
    schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

fn schema_type_hint(schema: &serde_json::Value) -> String {
    if let Some(t) = schema.get("type") {
        if let Some(text) = t.as_str() {
            return text.to_string();
        }
        if let Some(list) = t.as_array() {
            let mut items = list
                .iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<Vec<_>>();
            if !items.is_empty() {
                items.sort_unstable();
                return items.join("|");
            }
        }
    }
    if schema.get("enum").is_some() {
        return "enum".to_string();
    }
    if schema.get("oneOf").is_some() {
        return "oneOf".to_string();
    }
    if schema.get("anyOf").is_some() {
        return "anyOf".to_string();
    }
    if schema.is_object() {
        return "object".to_string();
    }
    "any".to_string()
}

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

#[async_trait]
impl<C: LlmClient> Planner for LlmPlanner<C> {
    async fn plan(
        &self,
        intent: &Intent,
        context: &PlannerContext,
    ) -> Result<PlannerOutput, PlanError> {
        let (system, user) = self.build_prompt(intent, context);
        info!(
            model = %self.config.model,
            temperature = self.config.temperature,
            intent_len = intent.content.len(),
            action_count = context.available_actions.len(),
            history_count = context.history.len(),
            "planner request prepared"
        );
        if tracing::enabled!(tracing::Level::DEBUG) {
            let system_preview = truncate_for_log(&system, MAX_PROMPT_LOG_CHARS);
            let user_preview = truncate_for_log(&user, MAX_PROMPT_LOG_CHARS);
            debug!(
                system_prompt = %system_preview,
                user_prompt = %user_preview,
                "planner prompts"
            );
        }
        let request = LlmRequest {
            system,
            user,
            model: self.config.model.clone(),
            temperature: self.config.temperature,
        };
        let output = self
            .client
            .complete(request)
            .await
            .map_err(|e| PlanError::LlmError(e.to_string()))?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                llm_output = %truncate_for_log(&output, MAX_LLM_OUTPUT_LOG_CHARS),
                "planner raw llm output"
            );
        }

        let json_str = extract_json(&output)
            .ok_or_else(|| PlanError::Generation("LLM output did not contain JSON".to_string()))?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                plan_json = %truncate_for_log(&json_str, MAX_LLM_OUTPUT_LOG_CHARS),
                "planner extracted json"
            );
        }

        let output = parse_planner_output(&json_str)?;
        match &output {
            PlannerOutput::Workflow(plan) => {
                info!(
                    output_type = "workflow",
                    step_count = plan.steps.len(),
                    confidence = plan.confidence.unwrap_or_default(),
                    "planner parsed output"
                );
                if tracing::enabled!(tracing::Level::DEBUG) {
                    debug!(
                        goal = %truncate_for_log(&plan.goal, MAX_PROMPT_LOG_CHARS),
                        plan = %truncate_for_log(&format!("{:?}", plan.steps), MAX_LLM_OUTPUT_LOG_CHARS),
                        "planner workflow detail"
                    );
                }
            }
            PlannerOutput::DirectResponse(message) => {
                info!(
                    output_type = "direct_response",
                    message = %truncate_for_log(message, MAX_PROMPT_LOG_CHARS),
                    "planner parsed output"
                );
            }
            PlannerOutput::Clarification(question) => {
                info!(
                    output_type = "clarification",
                    question = %truncate_for_log(question, MAX_PROMPT_LOG_CHARS),
                    "planner parsed output"
                );
            }
        }
        Ok(output)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
enum PlannerJsonOutput {
    Workflow {
        goal: String,
        #[serde(default)]
        steps: Vec<Step>,
        #[serde(default)]
        confidence: Option<f32>,
    },
    DirectResponse {
        message: String,
    },
    Clarification {
        question: String,
    },
}

fn parse_planner_output(json: &str) -> Result<PlannerOutput, PlanError> {
    let parsed = serde_json::from_str::<PlannerJsonOutput>(json)
        .map_err(|e| PlanError::Generation(format!("Invalid planner output JSON: {}", e)))?;

    match parsed {
        PlannerJsonOutput::Workflow {
            goal,
            steps,
            confidence,
        } => {
            let mut plan = Plan::new(goal, steps);
            plan.confidence = confidence.map(|c| c.clamp(0.0, 1.0));
            Ok(PlannerOutput::Workflow(plan))
        }
        PlannerJsonOutput::DirectResponse { message } => Ok(PlannerOutput::DirectResponse(message)),
        PlannerJsonOutput::Clarification { question } => Ok(PlannerOutput::Clarification(question)),
    }
}

/// Mock LLM client for tests/examples
pub struct MockLlmClient {
    pub response: String,
}

#[async_trait]
impl LlmClient for MockLlmClient {
    async fn complete(&self, _request: LlmRequest) -> Result<String, LlmError> {
        Ok(self.response.clone())
    }
}

/// HTTP client config (OpenAI-compatible)
#[derive(Debug, Clone)]
pub struct HttpLlmClientConfig {
    pub endpoint: String,
    pub api_key: Option<String>,
    pub model: String,
    pub temperature: f32,
    pub timeout_secs: u64,
    pub extra_headers: HeaderMap,
}

impl Default for HttpLlmClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://api.openai.com/v1/chat/completions".to_string(),
            api_key: None,
            model: "gpt-4o-mini".to_string(),
            temperature: 0.2,
            timeout_secs: 30,
            extra_headers: HeaderMap::new(),
        }
    }
}

/// HTTP LLM client using an OpenAI-compatible API
pub struct HttpLlmClient {
    client: reqwest::Client,
    config: HttpLlmClientConfig,
}

impl HttpLlmClient {
    pub fn new(config: HttpLlmClientConfig) -> Result<Self, LlmError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| LlmError::Http(e.to_string()))?;
        Ok(Self { client, config })
    }
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f32,
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessageResponse,
}

#[derive(Debug, Deserialize)]
struct ChatMessageResponse {
    content: String,
}

#[async_trait]
impl LlmClient for HttpLlmClient {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError> {
        let mut headers = self.config.extra_headers.clone();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(key) = &self.config.api_key {
            let value = format!("Bearer {}", key);
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&value).map_err(|e| LlmError::Http(e.to_string()))?,
            );
        }

        let body = ChatRequest {
            model: request.model,
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: request.system,
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: request.user,
                },
            ],
            temperature: request.temperature,
        };

        let response = self
            .client
            .post(&self.config.endpoint)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(LlmError::Response(format!("HTTP {}: {}", status, text)));
        }

        let text = response
            .text()
            .await
            .map_err(|e| LlmError::Http(e.to_string()))?;
        let parsed: ChatResponse =
            serde_json::from_str(&text).map_err(|e| LlmError::Serialization(e.to_string()))?;

        let content = parsed
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| LlmError::Response("Missing choices".to_string()))?;

        Ok(content)
    }
}

fn extract_json(text: &str) -> Option<String> {
    for (start, ch) in text.char_indices() {
        if ch != '{' {
            continue;
        }
        if let Some(end) = find_json_object_end(text, start) {
            let candidate = &text[start..=end];
            if serde_json::from_str::<serde_json::Value>(candidate)
                .map(|v| v.is_object())
                .unwrap_or(false)
            {
                return Some(candidate.to_string());
            }
        }
    }
    None
}

fn find_json_object_end(text: &str, start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in text[start..].char_indices() {
        let abs = start + idx;
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(abs);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use orchestral_core::action::ActionMeta;
    use orchestral_core::planner::PlannerContext;
    use orchestral_core::store::{Reference, ReferenceStore, ReferenceType, StoreError};
    use orchestral_core::types::Intent;
    use serde_json::json;
    use std::sync::Arc;

    struct NoopReferenceStore;

    #[async_trait]
    impl ReferenceStore for NoopReferenceStore {
        async fn add(&self, _reference: Reference) -> Result<(), StoreError> {
            Ok(())
        }

        async fn get(&self, _id: &str) -> Result<Option<Reference>, StoreError> {
            Ok(None)
        }

        async fn query_by_type(
            &self,
            _ref_type: &ReferenceType,
        ) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn query_recent(&self, _limit: usize) -> Result<Vec<Reference>, StoreError> {
            Ok(Vec::new())
        }

        async fn delete(&self, _id: &str) -> Result<bool, StoreError> {
            Ok(false)
        }
    }

    #[test]
    fn test_system_prompt_contains_action_catalog_with_schema_and_field_hints() {
        let planner = LlmPlanner::new(
            MockLlmClient {
                response: "{}".to_string(),
            },
            LlmPlannerConfig {
                system_prompt: "Base prompt.".to_string(),
                ..LlmPlannerConfig::default()
            },
        );

        let actions = vec![ActionMeta::new("write_doc", "Write markdown to file")
            .with_input_schema(json!({
                "type":"object",
                "properties":{
                    "path":{"type":"string","description":"Target markdown path","example":"guide.md"},
                    "content":{"type":"string","description":"Markdown content"}
                },
                "required":["path","content"]
            }))
            .with_output_schema(
                json!({
                    "type":"object",
                    "properties":{
                        "path":{"type":"string","description":"Resolved path"},
                        "bytes":{"type":"integer","description":"Written bytes"}
                    }
                }),
            )];
        let context = PlannerContext::new(actions, Arc::new(NoopReferenceStore));
        let intent = Intent::new("generate a guide");
        let (system, _user) = planner.build_prompt(&intent, &context);

        assert!(system.contains("Action Catalog"));
        assert!(system.contains("Orchestral Planner"));
        assert!(system.contains("Intent -> Plan -> Normalize -> Execute"));
        assert!(system.contains("write_doc"));
        assert!(system.contains("input_fields"));
        assert!(system.contains("path (string, required)"));
        assert!(system.contains("desc=Target markdown path"));
        assert!(system.contains("example=\"guide.md\""));
        assert!(system.contains("input_schema"));
        assert!(system.contains("output_schema"));
    }

    #[test]
    fn test_parse_workflow_output() {
        let raw = r#"{
            "type":"WORKFLOW",
            "goal":"echo",
            "steps":[{"id":"s1","action":"echo","params":{"message":"hi"}}],
            "confidence": 1.2
        }"#;
        let parsed = parse_planner_output(raw).expect("parse workflow");
        match parsed {
            PlannerOutput::Workflow(plan) => {
                assert_eq!(plan.goal, "echo");
                assert_eq!(plan.steps.len(), 1);
                assert_eq!(plan.confidence, Some(1.0));
            }
            _ => panic!("expected workflow output"),
        }
    }

    #[test]
    fn test_parse_direct_response_output() {
        let raw = r#"{"type":"DIRECT_RESPONSE","message":"你好"}"#;
        let parsed = parse_planner_output(raw).expect("parse direct response");
        match parsed {
            PlannerOutput::DirectResponse(message) => {
                assert_eq!(message, "你好");
            }
            _ => panic!("expected direct_response output"),
        }
    }

    #[test]
    fn test_parse_clarification_output() {
        let raw = r#"{"type":"CLARIFICATION","question":"请提供文件路径"}"#;
        let parsed = parse_planner_output(raw).expect("parse clarification");
        match parsed {
            PlannerOutput::Clarification(question) => {
                assert_eq!(question, "请提供文件路径");
            }
            _ => panic!("expected clarification output"),
        }
    }

    #[test]
    fn test_extract_json_ignores_non_json_braces() {
        let raw = r#"Preface {not json} -> {"type":"DIRECT_RESPONSE","message":"ok"} trailing"#;
        let json = extract_json(raw).expect("json");
        assert_eq!(json, r#"{"type":"DIRECT_RESPONSE","message":"ok"}"#);
    }

    #[test]
    fn test_extract_json_handles_braces_inside_strings() {
        let raw = r#"noise {"type":"DIRECT_RESPONSE","message":"value with } brace"} end"#;
        let json = extract_json(raw).expect("json");
        assert_eq!(
            json,
            r#"{"type":"DIRECT_RESPONSE","message":"value with } brace"}"#
        );
    }
}
