use std::sync::Arc;

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use orchestral_core::planner::{PlanError, Planner, PlannerContext};
use orchestral_core::types::{Intent, Plan};

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

        user.push_str("Available actions:\n");
        for action in &context.available_actions {
            user.push_str(&format!(
                "- {}: {} (imports: {:?}, exports: {:?})\n",
                action.name, action.description, action.imports, action.exports
            ));
        }

        user.push_str("\nReturn a JSON object with shape:\n");
        user.push_str(
            r#"{"goal":"...","steps":[{"id":"s1","action":"action_name","kind":"action","depends_on":[],"imports":[],"exports":[],"io_bindings":[{"from":"s1.output_key","to":"input_key","required":true}],"params":{}}],"confidence":0.0}"#,
        );
        user.push_str("\nRules:\n");
        user.push_str("- Declare each step output keys in step.exports.\n");
        user.push_str(
            "- When a step consumes previous outputs, use io_bindings and explicit depends_on.\n",
        );
        user.push('\n');

        (self.config.system_prompt.clone(), user)
    }
}

#[async_trait]
impl<C: LlmClient> Planner for LlmPlanner<C> {
    async fn plan(&self, intent: &Intent, context: &PlannerContext) -> Result<Plan, PlanError> {
        let (system, user) = self.build_prompt(intent, context);
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

        let json_str = extract_json(&output)
            .ok_or_else(|| PlanError::Generation("LLM output did not contain JSON".to_string()))?;

        serde_json::from_str::<Plan>(&json_str)
            .map_err(|e| PlanError::Generation(format!("Invalid plan JSON: {}", e)))
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
