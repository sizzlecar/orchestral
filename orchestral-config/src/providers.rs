//! LLM provider and model configuration types.

use serde::Deserialize;
use serde_json::Value;

/// Root configuration for LLM providers.
///
/// Preferred schema:
/// - `backends`: vendor/endpoint/auth config
/// - `models`: model presets + optional guardrails
///
/// Legacy compatibility:
/// - `providers` + `default_provider` still supported and normalized internally.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProvidersConfig {
    /// Default backend name.
    #[serde(default)]
    pub default_backend: Option<String>,
    /// Default model profile name.
    #[serde(default)]
    pub default_model: Option<String>,
    /// Backend definitions (recommended).
    #[serde(default)]
    pub backends: Vec<BackendSpec>,
    /// Model profiles (recommended).
    #[serde(default)]
    pub models: Vec<ModelProfile>,
    /// Legacy provider list (still accepted).
    #[serde(default)]
    pub providers: Vec<LegacyProviderSpec>,
    /// Legacy default provider.
    #[serde(default)]
    pub default_provider: Option<String>,
}

impl ProvidersConfig {
    /// Get backend by name, including normalized legacy providers.
    pub fn get_backend(&self, name: &str) -> Option<BackendSpec> {
        self.normalized_backends()
            .into_iter()
            .find(|p| p.name == name)
    }

    /// Get model profile by name, including normalized legacy providers.
    pub fn get_model(&self, name: &str) -> Option<ModelProfile> {
        self.normalized_models()
            .into_iter()
            .find(|m| m.name == name)
    }

    /// Get default backend.
    pub fn get_default_backend(&self) -> Option<BackendSpec> {
        if let Some(name) = &self.default_backend {
            return self.get_backend(name);
        }
        if let Some(name) = &self.default_provider {
            return self.get_backend(name);
        }
        self.normalized_backends().into_iter().next()
    }

    /// Get default model profile.
    pub fn get_default_model(&self) -> Option<ModelProfile> {
        if let Some(name) = &self.default_model {
            return self.get_model(name);
        }
        if let Some(name) = &self.default_provider {
            return self.get_model(name);
        }
        self.normalized_models().into_iter().next()
    }

    /// List all backend names.
    pub fn backend_names(&self) -> Vec<String> {
        self.normalized_backends()
            .into_iter()
            .map(|p| p.name)
            .collect()
    }

    /// List all model profile names.
    pub fn model_names(&self) -> Vec<String> {
        self.normalized_models().into_iter().map(|m| m.name).collect()
    }

    /// Compatibility alias: returns backend names.
    pub fn names(&self) -> Vec<String> {
        self.backend_names()
    }

    /// Compatibility alias: resolves default model first, then default backend.
    pub fn get_default(&self) -> Option<LegacyProviderSpec> {
        if let Some(model) = self.get_default_model() {
            let backend = model
                .backend
                .as_ref()
                .and_then(|name| self.get_backend(name))
                .or_else(|| self.get_default_backend());
            if let Some(backend) = backend {
                return Some(LegacyProviderSpec {
                    name: model.name,
                    kind: backend.kind,
                    model: model.model,
                    endpoint: backend.endpoint,
                    api_key_env: backend.api_key_env,
                    config: model.config,
                });
            }
        }
        self.providers.first().cloned()
    }

    /// Merge new schema + legacy schema into backend list.
    pub fn normalized_backends(&self) -> Vec<BackendSpec> {
        let mut result = self.backends.clone();
        for legacy in &self.providers {
            if !result.iter().any(|b| b.name == legacy.name) {
                result.push(BackendSpec {
                    name: legacy.name.clone(),
                    kind: legacy.kind.clone(),
                    endpoint: legacy.endpoint.clone(),
                    api_key_env: legacy.api_key_env.clone(),
                    config: Value::Null,
                });
            }
        }
        result
    }

    /// Merge new schema + legacy schema into model profile list.
    pub fn normalized_models(&self) -> Vec<ModelProfile> {
        let mut result = self.models.clone();
        for legacy in &self.providers {
            if !result.iter().any(|m| m.name == legacy.name) {
                result.push(ModelProfile {
                    name: legacy.name.clone(),
                    backend: Some(legacy.name.clone()),
                    model: legacy.model.clone(),
                    temperature: legacy.get_config("temperature"),
                    max_tokens: legacy.get_config("max_tokens"),
                    system_prompt: legacy.get_config("system_prompt"),
                    policy: ModelPolicy::default(),
                    config: legacy.config.clone(),
                });
            }
        }
        result
    }
}

/// Backend configuration (auth, endpoint, vendor).
#[derive(Debug, Clone, Deserialize)]
pub struct BackendSpec {
    /// Backend identifier (e.g. "openai", "google").
    pub name: String,
    /// Backend kind understood by the SDK.
    pub kind: String,
    /// Optional custom endpoint URL.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Environment variable name containing the API key.
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// Backend-specific settings.
    #[serde(default)]
    pub config: Value,
}

impl BackendSpec {
    /// Resolve the API key from environment variable.
    pub fn resolve_api_key(&self) -> Result<String, ApiKeyError> {
        let env_name = self.api_key_env.as_ref().ok_or(ApiKeyError::NotConfigured)?;
        std::env::var(env_name).map_err(|_| ApiKeyError::EnvNotFound(env_name.clone()))
    }

    /// Read backend config value as typed object.
    pub fn get_config<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.config
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// Model profile used by planner/runtime.
#[derive(Debug, Clone, Deserialize)]
pub struct ModelProfile {
    /// Profile name (e.g. "fast", "deep_reasoning", "cheap").
    pub name: String,
    /// Backend reference.
    #[serde(default)]
    pub backend: Option<String>,
    /// Actual model name.
    pub model: String,
    /// Optional default temperature.
    #[serde(default)]
    pub temperature: Option<f32>,
    /// Optional max tokens.
    #[serde(default)]
    pub max_tokens: Option<u32>,
    /// Optional default system prompt.
    #[serde(default)]
    pub system_prompt: Option<String>,
    /// Optional guardrails.
    #[serde(default)]
    pub policy: ModelPolicy,
    /// Extra arbitrary config.
    #[serde(default)]
    pub config: Value,
}

impl ModelProfile {
    /// Read model config value as typed object.
    pub fn get_config<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.config
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Clamp input temperature with configured guardrails.
    pub fn clamp_temperature(&self, candidate: f32) -> f32 {
        let mut value = candidate;
        if let Some(min) = self.policy.temperature_min {
            value = value.max(min);
        }
        if let Some(max) = self.policy.temperature_max {
            value = value.min(max);
        }
        value
    }
}

/// Optional model guardrails.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ModelPolicy {
    #[serde(default)]
    pub temperature_min: Option<f32>,
    #[serde(default)]
    pub temperature_max: Option<f32>,
}

/// Legacy provider specification (pre-backend/model split).
#[derive(Debug, Clone, Deserialize)]
pub struct LegacyProviderSpec {
    /// Unique identifier for this provider (e.g., "openai-gpt4").
    pub name: String,
    /// Provider type: "openai" | "gemini" | etc.
    pub kind: String,
    /// Model identifier (e.g., "gpt-4o-mini", "gemini-1.5-pro").
    pub model: String,
    /// Optional custom endpoint URL.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Environment variable name containing the API key.
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// Provider-specific configuration (temperature, timeout, etc.).
    #[serde(default)]
    pub config: Value,
}

impl LegacyProviderSpec {
    /// Get a config value as a specific type.
    pub fn get_config<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.config
            .get(key)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }
}

/// Errors related to API key resolution.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiKeyError {
    #[error("API key environment variable not configured")]
    NotConfigured,
    #[error("Environment variable '{0}' not found")]
    EnvNotFound(String),
}
