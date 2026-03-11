use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use super::{LlmClient, LlmError, LlmRequest};

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
