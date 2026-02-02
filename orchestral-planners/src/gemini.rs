//! Gemini LLM client implementation.
//!
//! This module provides a client for Google's Gemini API.

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::llm::{LlmClient, LlmError, LlmRequest};

/// Gemini client configuration.
#[derive(Debug, Clone)]
pub struct GeminiClientConfig {
    /// API key for authentication.
    pub api_key: String,
    /// Model name (e.g., "gemini-1.5-pro", "gemini-1.5-flash").
    pub model: String,
    /// Base endpoint URL.
    pub endpoint: String,
    /// Temperature for generation (0.0 - 2.0).
    pub temperature: f32,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

impl Default for GeminiClientConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            model: "gemini-3-flash-preview".to_string(),
            endpoint: "https://generativelanguage.googleapis.com/v1beta".to_string(),
            temperature: 0.2,
            timeout_secs: 30,
        }
    }
}

/// Gemini LLM client.
pub struct GeminiClient {
    client: reqwest::Client,
    config: GeminiClientConfig,
}

impl GeminiClient {
    /// Create a new Gemini client.
    pub fn new(config: GeminiClientConfig) -> Result<Self, LlmError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| LlmError::Http(e.to_string()))?;
        Ok(Self { client, config })
    }

    fn build_url(&self, model: &str) -> String {
        format!(
            "{}/models/{}:generateContent?key={}",
            self.config.endpoint, model, self.config.api_key
        )
    }
}

// Gemini API request/response structures

#[derive(Debug, Serialize)]
struct GeminiRequest {
    contents: Vec<GeminiContent>,
    #[serde(rename = "systemInstruction", skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiSystemInstruction>,
    #[serde(rename = "generationConfig")]
    generation_config: GeminiGenerationConfig,
}

#[derive(Debug, Serialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiSystemInstruction {
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiPart {
    text: String,
}

#[derive(Debug, Serialize)]
struct GeminiGenerationConfig {
    temperature: f32,
}

#[derive(Debug, Deserialize)]
struct GeminiResponse {
    candidates: Option<Vec<GeminiCandidate>>,
    error: Option<GeminiErrorDetail>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    content: GeminiContentResponse,
}

#[derive(Debug, Deserialize)]
struct GeminiContentResponse {
    parts: Vec<GeminiPartResponse>,
}

#[derive(Debug, Deserialize)]
struct GeminiPartResponse {
    text: String,
}

#[derive(Debug, Deserialize)]
struct GeminiErrorDetail {
    message: String,
    #[allow(dead_code)]
    code: Option<i32>,
}

#[async_trait]
impl LlmClient for GeminiClient {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError> {
        let url = self.build_url(&request.model);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // Build Gemini request
        let body = GeminiRequest {
            contents: vec![GeminiContent {
                role: "user".to_string(),
                parts: vec![GeminiPart { text: request.user }],
            }],
            system_instruction: if request.system.is_empty() {
                None
            } else {
                Some(GeminiSystemInstruction {
                    parts: vec![GeminiPart {
                        text: request.system,
                    }],
                })
            },
            generation_config: GeminiGenerationConfig {
                temperature: request.temperature,
            },
        };

        let response = self
            .client
            .post(&url)
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

        let parsed: GeminiResponse =
            serde_json::from_str(&text).map_err(|e| LlmError::Serialization(e.to_string()))?;

        // Check for API error
        if let Some(error) = parsed.error {
            return Err(LlmError::Response(format!(
                "Gemini API error: {}",
                error.message
            )));
        }

        // Extract content from response
        let content = parsed
            .candidates
            .and_then(|c| c.into_iter().next())
            .and_then(|c| c.content.parts.into_iter().next())
            .map(|p| p.text)
            .ok_or_else(|| LlmError::Response("No content in response".to_string()))?;

        Ok(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::LlmRequest;

    #[test]
    fn test_default_config() {
        let config = GeminiClientConfig::default();
        assert_eq!(config.model, "gemini-3-flash-preview");
        assert!(config
            .endpoint
            .contains("generativelanguage.googleapis.com"));
    }

    #[test]
    fn test_build_url() {
        let config = GeminiClientConfig {
            api_key: "test-key".to_string(),
            model: "gemini-1.5-pro".to_string(),
            ..Default::default()
        };
        let client = GeminiClient::new(config).unwrap();
        let url = client.build_url("gemini-1.5-pro");
        assert!(url.contains("gemini-1.5-pro:generateContent"));
        assert!(url.contains("key=test-key"));
    }

    #[tokio::test]
    #[ignore = "requires live GEMINI_API_KEY and network"]
    async fn test_live_gemini_completion_when_env_set() {
        let api_key = match std::env::var("GEMINI_API_KEY") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                eprintln!("skipped: GEMINI_API_KEY is not set");
                return;
            }
        };

        let config = GeminiClientConfig {
            api_key,
            model: "gemini-3-flash-preview".to_string(),
            ..Default::default()
        };
        let client = GeminiClient::new(config).expect("client should initialize");
        let request = LlmRequest {
            system: "You are a concise assistant.".to_string(),
            user: "Reply with exactly: OK".to_string(),
            model: "gemini-3-flash-preview".to_string(),
            temperature: 0.0,
        };

        let response = client
            .complete(request)
            .await
            .expect("live Gemini completion should succeed");
        assert!(!response.trim().is_empty());
    }
}
