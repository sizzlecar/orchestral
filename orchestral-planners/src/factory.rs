//! LLM client factory for building clients from backend configuration.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use llm_sdk::builder::{LLMBackend, LLMBuilder};
use llm_sdk::chat::ChatMessage;
use thiserror::Error;

use orchestral_config::BackendSpec;

use crate::llm::{LlmClient, LlmError, LlmRequest};

/// Runtime invocation config for an LLM call chain.
#[derive(Debug, Clone)]
pub struct LlmInvocationConfig {
    pub model: String,
    pub temperature: f32,
    pub normalize_response: bool,
}

impl Default for LlmInvocationConfig {
    fn default() -> Self {
        Self {
            model: "gpt-4o-mini".to_string(),
            temperature: 0.2,
            normalize_response: true,
        }
    }
}

/// Errors that can occur when building an LLM client.
#[derive(Debug, Error)]
pub enum LlmBuildError {
    #[error("unknown backend kind: {0}")]
    UnknownKind(String),
    #[error("missing API key for backend")]
    MissingApiKey,
    #[error("environment variable '{0}' not found")]
    EnvNotFound(String),
}

/// Factory trait for building LLM clients.
pub trait LlmClientFactory: Send + Sync {
    /// Build an LLM client from backend + invocation config.
    fn build(
        &self,
        backend: &BackendSpec,
        invocation: &LlmInvocationConfig,
    ) -> Result<Arc<dyn LlmClient>, LlmBuildError>;
}

/// Default factory implementation using `graniet/llm`.
pub struct DefaultLlmClientFactory;

impl DefaultLlmClientFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultLlmClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl LlmClientFactory for DefaultLlmClientFactory {
    fn build(
        &self,
        backend: &BackendSpec,
        invocation: &LlmInvocationConfig,
    ) -> Result<Arc<dyn LlmClient>, LlmBuildError> {
        build_client_from_backend(backend, invocation)
    }
}

/// Build an LLM client from backend spec and invocation defaults.
pub fn build_client_from_backend(
    backend: &BackendSpec,
    invocation: &LlmInvocationConfig,
) -> Result<Arc<dyn LlmClient>, LlmBuildError> {
    let backend_kind = parse_backend(&backend.kind)?;
    let api_key = resolve_api_key(backend)?;
    let client = GranietLlmClient {
        backend: backend_kind,
        api_key: Some(api_key),
        model: invocation.model.clone(),
        temperature: invocation.temperature,
        normalize_response: invocation.normalize_response,
        timeout_secs: backend.get_config::<u64>("timeout_secs").unwrap_or(60),
    };
    Ok(Arc::new(client))
}

fn resolve_api_key(spec: &BackendSpec) -> Result<String, LlmBuildError> {
    let env_name = spec
        .api_key_env
        .as_ref()
        .ok_or(LlmBuildError::MissingApiKey)?;
    std::env::var(env_name).map_err(|_| LlmBuildError::EnvNotFound(env_name.clone()))
}

fn parse_backend(kind: &str) -> Result<LLMBackend, LlmBuildError> {
    let normalized = kind.to_lowercase();
    let mapped = match normalized.as_str() {
        "gemini" => "google",
        other => other,
    };
    let allowed = matches!(
        mapped,
        "openai"
            | "google"
            | "anthropic"
            | "deepseek"
            | "groq"
            | "xai"
            | "mistral"
            | "cohere"
            | "openrouter"
            | "ollama"
    );
    if !allowed {
        return Err(LlmBuildError::UnknownKind(kind.to_string()));
    }
    LLMBackend::from_str(mapped).map_err(|_| LlmBuildError::UnknownKind(kind.to_string()))
}

struct GranietLlmClient {
    backend: LLMBackend,
    api_key: Option<String>,
    model: String,
    temperature: f32,
    normalize_response: bool,
    timeout_secs: u64,
}

#[async_trait]
impl LlmClient for GranietLlmClient {
    async fn complete(&self, request: LlmRequest) -> Result<String, LlmError> {
        let prompt = if request.system.trim().is_empty() {
            request.user
        } else {
            format!("System:\n{}\n\nUser:\n{}", request.system, request.user)
        };

        let model = if request.model.trim().is_empty() {
            self.model.clone()
        } else {
            request.model
        };
        let temperature = if request.temperature <= 0.0 {
            self.temperature
        } else {
            request.temperature
        };

        let mut builder = LLMBuilder::new()
            .backend(self.backend.clone())
            .model(model)
            .temperature(temperature)
            .normalize_response(self.normalize_response);
        if let Some(api_key) = &self.api_key {
            builder = builder.api_key(api_key.clone());
        }

        let llm = builder
            .build()
            .map_err(|e| LlmError::Http(format!("llm builder error: {}", e)))?;

        let messages = vec![ChatMessage::user().content(prompt).build()];
        let response =
            tokio::time::timeout(Duration::from_secs(self.timeout_secs), llm.chat(&messages))
                .await
                .map_err(|_| {
                    LlmError::Http(format!("llm chat timeout after {}s", self.timeout_secs))
                })?
                .map_err(|e| LlmError::Http(format!("llm chat error: {}", e)))?;

        response
            .text()
            .map(|s| s.to_string())
            .ok_or_else(|| LlmError::Response("llm response had no text".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_backend(kind: &str) -> BackendSpec {
        BackendSpec {
            name: "test".to_string(),
            kind: kind.to_string(),
            endpoint: None,
            api_key_env: Some("TEST_API_KEY".to_string()),
            config: json!({}),
        }
    }

    #[test]
    fn test_unknown_kind() {
        let backend = make_backend("not-a-real-backend-kind");
        let invocation = LlmInvocationConfig::default();
        std::env::set_var("TEST_API_KEY", "dummy");
        let result = build_client_from_backend(&backend, &invocation);
        std::env::remove_var("TEST_API_KEY");
        assert!(matches!(result, Err(LlmBuildError::UnknownKind(_))));
    }

    #[test]
    fn test_missing_env_var() {
        let backend = make_backend("openai");
        let invocation = LlmInvocationConfig::default();
        std::env::remove_var("TEST_API_KEY");
        let result = build_client_from_backend(&backend, &invocation);
        assert!(matches!(result, Err(LlmBuildError::EnvNotFound(_))));
    }
}
