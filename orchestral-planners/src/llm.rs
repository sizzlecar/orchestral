use std::sync::Arc;
use std::{collections::HashSet, fmt::Write};

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info};

use orchestral_core::planner::{PlanError, Planner, PlannerContext};
use orchestral_core::types::{Intent, Plan};

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

/// LLM client trait
#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError>;
}

#[async_trait]
impl LlmClient for Arc<dyn LlmClient> {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError> {
        (**self).complete(request).await
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
            for item in context
                .history
                .iter()
                .rev()
                .take(self.config.max_history)
                .rev()
            {
                user.push_str(&format!("- {}: {}\n", item.role, item.content));
            }
            user.push('\n');
        }

        user.push_str("Return a JSON object with shape:\n");
        user.push_str(
            r#"{"goal":"...","steps":[{"id":"s1","action":"action_name","params":{},"io_bindings":[{"from":"s1.output_key","to":"input_key","required":true}]}],"confidence":0.0}"#,
        );
        user.push_str(
            "\nUse only action names listed in the system prompt Action Catalog. Return JSON only.\n",
        );
        user.push('\n');

        (system, user)
    }
}

fn build_system_prompt(base: &str, context: &PlannerContext) -> String {
    let mut system = String::new();
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
    system.push_str("1) Return ONLY one valid JSON object matching the required Plan schema.\n");
    system.push_str("2) step.id must be unique and stable.\n");
    system.push_str("3) step.params must satisfy the selected action input_schema.\n");
    system.push_str(
        "4) Prefer minimal steps: output action + params + io_bindings; omit optional fields unless needed.\n",
    );
    system.push_str("5) If a step consumes upstream output, declare io_bindings.\n");
    system.push_str("6) Do not invent action names not listed in Action Catalog.\n");
    system
        .push_str("7) If requirements are unclear, prefer wait_user step to ask clarification.\n");
    system.push_str(
        "8) For wait_user/wait_event/system steps, set kind explicitly; normal action steps can omit kind.\n",
    );
    system
        .push_str("9) When generating step.params, strictly follow Action Catalog input fields.\n");
    system.push_str(
        "10) Any shell/file operation must be compatible with the current host platform and shell.\n",
    );
    system.push_str(
        "11) Only use `echo` when the user explicitly asks to echo/repeat text; otherwise avoid `echo` in plans.\n",
    );
    system.push_str(
        "12) For requests about HTTP response headers, use `http` with method `HEAD` and return headers directly.\n",
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
    async fn plan(&self, intent: &Intent, context: &PlannerContext) -> Result<Plan, PlanError> {
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

        let plan = serde_json::from_str::<Plan>(&json_str)
            .map_err(|e| PlanError::Generation(format!("Invalid plan JSON: {}", e)))?;
        info!(
            step_count = plan.steps.len(),
            confidence = plan.confidence.unwrap_or_default(),
            "planner parsed plan"
        );
        if tracing::enabled!(tracing::Level::DEBUG) {
            debug!(
                goal = %truncate_for_log(&plan.goal, MAX_PROMPT_LOG_CHARS),
                plan = %truncate_for_log(&format!("{:?}", plan.steps), MAX_LLM_OUTPUT_LOG_CHARS),
                "planner plan detail"
            );
        }
        Ok(plan)
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
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end <= start {
        return None;
    }
    Some(text[start..=end].to_string())
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
        assert!(system.contains("write_doc"));
        assert!(system.contains("input_fields"));
        assert!(system.contains("path (string, required)"));
        assert!(system.contains("desc=Target markdown path"));
        assert!(system.contains("example=\"guide.md\""));
        assert!(system.contains("input_schema"));
        assert!(system.contains("output_schema"));
    }
}
