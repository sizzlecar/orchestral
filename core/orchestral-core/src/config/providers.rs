//! Provider-neutral model backend and model-profile configuration.

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvidersConfig {
    #[serde(default)]
    pub default_backend: Option<String>,
    #[serde(default)]
    pub default_model: Option<String>,
    #[serde(default)]
    pub backends: Vec<BackendSpec>,
    #[serde(default)]
    pub models: Vec<ModelProfile>,
}

impl ProvidersConfig {
    pub fn get_backend(&self, name: &str) -> Option<BackendSpec> {
        self.backends.iter().find(|item| item.name == name).cloned()
    }

    pub fn get_model(&self, name: &str) -> Option<ModelProfile> {
        self.models.iter().find(|item| item.name == name).cloned()
    }

    pub fn get_default_backend(&self) -> Option<BackendSpec> {
        self.default_backend
            .as_deref()
            .and_then(|name| self.get_backend(name))
    }

    pub fn get_default_model(&self) -> Option<ModelProfile> {
        self.default_model
            .as_deref()
            .and_then(|name| self.get_model(name))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendSpec {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub config: Value,
}

impl BackendSpec {
    pub fn resolve_api_key(&self) -> Result<String, ApiKeyError> {
        let candidates = self.api_key_env_candidates();
        let first = candidates
            .first()
            .cloned()
            .ok_or(ApiKeyError::NotConfigured)?;
        for env_name in candidates {
            if let Ok(value) = std::env::var(&env_name) {
                if !value.trim().is_empty() {
                    return Ok(value);
                }
            }
        }
        Err(ApiKeyError::EnvNotFound(first))
    }

    pub fn get_config<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.config
            .get(key)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    fn api_key_env_candidates(&self) -> Vec<String> {
        let mut candidates = Vec::new();
        if let Some(explicit) = &self.api_key_env {
            candidates.push(explicit.clone());
        }
        for fallback in default_api_key_envs_for_kind(&self.kind) {
            if !candidates.iter().any(|candidate| candidate == fallback) {
                candidates.push((*fallback).to_owned());
            }
        }
        candidates
    }
}

fn default_api_key_envs_for_kind(kind: &str) -> &'static [&'static str] {
    match kind.trim().to_ascii_lowercase().as_str() {
        "openai" => &["OPENAI_API_KEY"],
        "google" | "gemini" => &["GOOGLE_API_KEY", "GEMINI_API_KEY"],
        "openrouter" => &["OPENROUTER_API_KEY"],
        "deepseek" => &["DEEPSEEK_API_KEY"],
        "groq" => &["GROQ_API_KEY"],
        "xai" => &["XAI_API_KEY"],
        "mistral" => &["MISTRAL_API_KEY"],
        _ => &[],
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProfile {
    pub name: String,
    pub backend: String,
    pub model: String,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub policy: ModelPolicy,
    #[serde(default)]
    pub config: Value,
}

impl ModelProfile {
    pub fn get_config<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.config
            .get(key)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }

    pub fn clamp_temperature(&self, candidate: f32) -> f32 {
        let mut value = candidate;
        if let Some(minimum) = self.policy.temperature_min {
            value = value.max(minimum);
        }
        if let Some(maximum) = self.policy.temperature_max {
            value = value.min(maximum);
        }
        value
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelPolicy {
    #[serde(default)]
    pub temperature_min: Option<f32>,
    #[serde(default)]
    pub temperature_max: Option<f32>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiKeyError {
    #[error("API key environment variable not configured")]
    NotConfigured,
    #[error("environment variable '{0}' was not found")]
    EnvNotFound(String),
}
