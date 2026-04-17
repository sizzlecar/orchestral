//! LLM client factory for building clients from backend configuration.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use llm_sdk::builder::{FunctionBuilder, LLMBackend, LLMBuilder};
use llm_sdk::chat::{ChatMessage, StructuredOutputFormat, ToolChoice};
use thiserror::Error;

use orchestral_core::config::BackendSpec;

use super::llm::{
    LlmClient, LlmError, LlmRequest, LlmResponse, StreamChunkCallback, ToolDefinition,
};

/// Runtime invocation config for an LLM call chain.
#[derive(Debug, Clone)]
pub struct LlmInvocationConfig {
    pub model: String,
    pub temperature: f32,
    pub max_tokens: u32,
    pub normalize_response: bool,
}

impl Default for LlmInvocationConfig {
    fn default() -> Self {
        Self {
            model: "anthropic/claude-sonnet-4.5".to_string(),
            temperature: 0.2,
            max_tokens: 8192,
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
    #[error("client config error: {0}")]
    Config(String),
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
    let api_key = resolve_api_key(backend)?;
    let max_tokens = backend
        .get_config::<u32>("max_tokens")
        .unwrap_or(invocation.max_tokens);
    let timeout_secs = backend.get_config::<u64>("timeout_secs").unwrap_or(60);

    // Use native Gemini client for Google backend — proper system_instruction
    // and JSON response mode support.
    if backend.kind == "google" {
        // TODO: remove hardcode after config override issue is resolved
        let model_override = std::env::var("ORCHESTRAL_PLANNER_MODEL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| invocation.model.clone());
        let config = super::gemini::GeminiClientConfig {
            api_key,
            model: model_override,
            endpoint: backend
                .endpoint
                .clone()
                .unwrap_or_else(|| "https://generativelanguage.googleapis.com/v1beta".to_string()),
            temperature: invocation.temperature,
            max_tokens,
            timeout_secs,
        };
        let client = super::gemini::GeminiClient::new(config)
            .map_err(|e| LlmBuildError::Config(e.to_string()))?;
        tracing::info!("planner using native Gemini client with system_instruction + JSON mode");
        return Ok(Arc::new(client));
    }

    let backend_kind = parse_backend(&backend.kind)?;
    let client = GranietLlmClient {
        backend: backend_kind,
        api_key: Some(api_key),
        base_url: backend.endpoint.clone(),
        model: invocation.model.clone(),
        temperature: invocation.temperature,
        max_tokens,
        normalize_response: invocation.normalize_response,
        timeout_secs,
    };
    Ok(Arc::new(client))
}

fn resolve_api_key(spec: &BackendSpec) -> Result<String, LlmBuildError> {
    let candidates = api_key_env_candidates(spec);
    let first = candidates
        .first()
        .cloned()
        .ok_or(LlmBuildError::MissingApiKey)?;
    for env_name in candidates {
        if let Ok(value) = std::env::var(&env_name) {
            if !value.trim().is_empty() {
                return Ok(value);
            }
        }
    }
    Err(LlmBuildError::EnvNotFound(first))
}

fn api_key_env_candidates(spec: &BackendSpec) -> Vec<String> {
    let mut candidates = Vec::new();
    if let Some(explicit) = spec.api_key_env.as_ref() {
        candidates.push(explicit.clone());
    }
    for fallback in default_api_key_envs_for_kind(&spec.kind) {
        if !candidates.iter().any(|existing| existing == fallback) {
            candidates.push(fallback.to_string());
        }
    }
    candidates
}

fn default_api_key_envs_for_kind(kind: &str) -> &'static [&'static str] {
    match kind.trim().to_ascii_lowercase().as_str() {
        "openai" => &["OPENAI_API_KEY"],
        "google" | "gemini" => &["GOOGLE_API_KEY", "GEMINI_API_KEY"],
        "anthropic" | "claude" => &["ANTHROPIC_API_KEY", "CLAUDE_API_KEY"],
        "deepseek" => &["DEEPSEEK_API_KEY"],
        "groq" => &["GROQ_API_KEY"],
        "xai" => &["XAI_API_KEY"],
        "mistral" => &["MISTRAL_API_KEY"],
        "cohere" => &["COHERE_API_KEY"],
        "openrouter" => &["OPENROUTER_API_KEY"],
        _ => &[],
    }
}

fn parse_backend(kind: &str) -> Result<LLMBackend, LlmBuildError> {
    let normalized = kind.to_lowercase();
    let mapped = match normalized.as_str() {
        "gemini" => "google",
        "claude" => "anthropic",
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
    base_url: Option<String>,
    model: String,
    temperature: f32,
    max_tokens: u32,
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

        tracing::info!(
            model = %model,
            max_tokens = self.max_tokens,
            "planner llm call"
        );
        let mut builder = LLMBuilder::new()
            .backend(self.backend.clone())
            .model(model)
            .temperature(temperature)
            .max_tokens(self.max_tokens)
            .normalize_response(self.normalize_response)
            .schema(StructuredOutputFormat {
                name: "planner_output".to_string(),
                description: Some(
                    "Planner decision: one of SINGLE_ACTION, MINI_PLAN, DONE, or NEED_INPUT"
                        .to_string(),
                ),
                schema: None,
                strict: None,
            });
        if let Some(endpoint) = &self.base_url {
            builder = builder.base_url(endpoint.clone());
        }
        if let Some(api_key) = &self.api_key {
            builder = builder.api_key(api_key.clone());
        }

        let llm = builder
            .build()
            .map_err(|e| LlmError::Http(format!("llm builder error: {}", e)))?;

        let messages = vec![ChatMessage::user().content(prompt).build()];

        // Retry on parse failure: LLM may occasionally produce malformed output.
        const MAX_RETRIES: usize = 2;
        let mut last_error = None;
        for attempt in 0..=MAX_RETRIES {
            let response =
                tokio::time::timeout(Duration::from_secs(self.timeout_secs), llm.chat(&messages))
                    .await
                    .map_err(|_| {
                        LlmError::Http(format!("llm chat timeout after {}s", self.timeout_secs))
                    })?
                    .map_err(|e| LlmError::Http(format!("llm chat error: {}", e)))?;

            let text = match response.text().map(|s| s.to_string()) {
                Some(t) => t,
                None => {
                    last_error = Some("llm response had no text".to_string());
                    if attempt < MAX_RETRIES {
                        tracing::warn!(attempt, "planner llm returned empty, retrying");
                        continue;
                    }
                    return Err(LlmError::Response(last_error.unwrap()));
                }
            };

            if tracing::enabled!(tracing::Level::DEBUG) {
                let escaped = text.replace('\n', "\\n").replace('\r', "\\r");
                let preview: String = escaped.chars().take(800).collect();
                tracing::debug!(
                    response_text_len = text.len(),
                    attempt,
                    "planner llm response: {}",
                    preview
                );
            }

            return Ok(text);
        }
        Err(LlmError::Response(last_error.unwrap_or_else(|| {
            "planner llm exhausted retries".to_string()
        })))
    }

    async fn complete_with_tools(
        &self,
        request: LlmRequest,
        tools: &[ToolDefinition],
    ) -> Result<LlmResponse, LlmError> {
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
            .max_tokens(self.max_tokens)
            .normalize_response(self.normalize_response);
        for t in tools {
            builder = builder.function(
                FunctionBuilder::new(&t.name)
                    .description(&t.description)
                    .json_schema(t.parameters.clone()),
            );
        }
        builder = builder.tool_choice(ToolChoice::Any);
        if let Some(endpoint) = &self.base_url {
            builder = builder.base_url(endpoint.clone());
        }
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
                .map_err(|e| LlmError::Http(format!("llm chat_with_tools error: {}", e)))?;

        if let Some(tool_calls) = response.tool_calls() {
            if let Some(tc) = tool_calls.into_iter().next() {
                let arguments: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                    .map_err(|e| {
                        LlmError::Serialization(format!(
                            "failed to parse tool call arguments: {}",
                            e
                        ))
                    })?;
                return Ok(LlmResponse::ToolCall {
                    id: tc.id.clone(),
                    name: tc.function.name.clone(),
                    arguments,
                });
            }
        }

        let text = response.text().unwrap_or_default();
        Ok(LlmResponse::Text(text))
    }

    async fn complete_stream(
        &self,
        request: LlmRequest,
        on_chunk: StreamChunkCallback,
    ) -> Result<String, LlmError> {
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
            .max_tokens(self.max_tokens)
            .normalize_response(self.normalize_response);
        if let Some(endpoint) = &self.base_url {
            builder = builder.base_url(endpoint.clone());
        }
        if let Some(api_key) = &self.api_key {
            builder = builder.api_key(api_key.clone());
        }
        let llm = builder
            .build()
            .map_err(|e| LlmError::Http(format!("llm builder error: {}", e)))?;
        let messages = vec![ChatMessage::user().content(prompt).build()];

        let mut stream = tokio::time::timeout(
            Duration::from_secs(self.timeout_secs),
            llm.chat_stream(&messages),
        )
        .await
        .map_err(|_| {
            LlmError::Http(format!(
                "llm stream setup timeout after {}s",
                self.timeout_secs
            ))
        })?
        .map_err(|e| LlmError::Http(format!("llm chat_stream error: {}", e)))?;

        let mut full = String::new();
        while let Some(item) =
            tokio::time::timeout(Duration::from_secs(self.timeout_secs), stream.next())
                .await
                .map_err(|_| {
                    LlmError::Http(format!(
                        "llm stream timeout after {}s while reading chunk",
                        self.timeout_secs
                    ))
                })?
        {
            let chunk =
                item.map_err(|e| LlmError::Http(format!("llm stream chunk error: {}", e)))?;
            if chunk.is_empty() {
                continue;
            }
            full.push_str(&chunk);
            on_chunk(chunk);
        }
        if full.is_empty() {
            return Err(LlmError::Response(
                "llm stream produced no text chunks".to_string(),
            ));
        }
        Ok(full)
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
        std::env::remove_var("OPENAI_API_KEY");
        let result = build_client_from_backend(&backend, &invocation);
        assert!(matches!(result, Err(LlmBuildError::EnvNotFound(_))));
    }

    #[test]
    fn test_google_backend_accepts_google_api_key_alias() {
        let backend = BackendSpec {
            name: "google".to_string(),
            kind: "google".to_string(),
            endpoint: None,
            api_key_env: None,
            config: json!({}),
        };
        std::env::remove_var("GEMINI_API_KEY");
        std::env::set_var("GOOGLE_API_KEY", "google-key");
        let resolved = resolve_api_key(&backend).expect("google api key");
        std::env::remove_var("GOOGLE_API_KEY");
        assert_eq!(resolved, "google-key");
    }

    #[test]
    fn test_claude_backend_accepts_claude_api_key_alias() {
        let backend = BackendSpec {
            name: "claude".to_string(),
            kind: "claude".to_string(),
            endpoint: None,
            api_key_env: None,
            config: json!({}),
        };
        std::env::remove_var("ANTHROPIC_API_KEY");
        std::env::set_var("CLAUDE_API_KEY", "claude-key");
        let resolved = resolve_api_key(&backend).expect("claude api key");
        std::env::remove_var("CLAUDE_API_KEY");
        assert_eq!(resolved, "claude-key");
        assert!(parse_backend("claude").is_ok());
    }
}
