use std::sync::Arc;

use serde_json::json;

use crate::agent::{LlmAgentExecutor, LlmAgentExecutorConfig};
use crate::planner::{
    DefaultLlmClientFactory, LlmClient, LlmClientFactory, LlmInvocationConfig, LlmPlanner,
    LlmPlannerConfig,
};
use crate::{
    ConcurrencyPolicy, DefaultConcurrencyPolicy, ParallelConcurrencyPolicy,
    RejectWhenBusyConcurrencyPolicy,
};
use orchestral_core::config::{OrchestralConfig, StoreSpec};
use orchestral_core::planner::{PlanError, Planner, PlannerContext, PlannerOutput};
use orchestral_core::types::Intent;

use super::BootstrapError;

const MAX_PROMPT_LOG_CHARS: usize = 4_000;

#[derive(Clone)]
struct PlannerBackendSelection {
    backend: orchestral_core::config::BackendSpec,
    profile: Option<orchestral_core::config::ModelProfile>,
    fell_back_from: Option<String>,
}

pub(super) fn concurrency_policy_from_name(
    policy: &str,
) -> Result<Arc<dyn ConcurrencyPolicy>, BootstrapError> {
    match policy {
        "interrupt_and_start_new" | "interrupt" => Ok(Arc::new(DefaultConcurrencyPolicy)),
        "queue" => Err(BootstrapError::UnsupportedConcurrencyPolicy(
            "queue (not implemented; use interrupt/parallel/reject)".to_string(),
        )),
        "parallel" => Ok(Arc::new(ParallelConcurrencyPolicy::default())),
        "reject" | "reject_when_busy" => Ok(Arc::new(RejectWhenBusyConcurrencyPolicy)),
        other => Err(BootstrapError::UnsupportedConcurrencyPolicy(
            other.to_string(),
        )),
    }
}

pub(super) fn build_planner(config: &OrchestralConfig) -> Result<Arc<dyn Planner>, BootstrapError> {
    match config.planner.mode.as_str() {
        "llm" => {
            let selection = resolve_planner_backend_selection(config)?;
            let backend = selection.backend;
            let profile = selection.profile;

            let model = config
                .planner
                .model
                .clone()
                .or_else(|| profile.as_ref().map(|p| p.model.clone()))
                .unwrap_or_else(|| LlmPlannerConfig::default().model);
            let temperature_candidate = config
                .planner
                .temperature
                .or_else(|| profile.as_ref().and_then(|p| p.temperature))
                .unwrap_or_else(|| LlmPlannerConfig::default().temperature);
            let temperature = profile
                .as_ref()
                .map(|p| p.clamp_temperature(temperature_candidate))
                .unwrap_or(temperature_candidate);

            let invocation = LlmInvocationConfig {
                model: model.clone(),
                temperature,
                max_tokens: LlmInvocationConfig::default().max_tokens,
                normalize_response: true,
            };

            let client = DefaultLlmClientFactory::new().build(&backend, &invocation)?;
            let system_prompt = String::new();
            let prompt_source = "template";

            if let Some(original_backend) = selection.fell_back_from.as_ref() {
                tracing::warn!(
                    original_backend = %original_backend,
                    resolved_backend = %backend.name,
                    resolved_model = %model,
                    "planner backend API key missing; fell back to alternate configured backend"
                );
            }
            tracing::info!(
                backend_name = %backend.name,
                backend_kind = %backend.kind,
                model = %model,
                temperature = temperature,
                prompt_source = %prompt_source,
                planner_mode = "llm",
                "planner llm config selected"
            );
            if tracing::enabled!(tracing::Level::DEBUG) {
                tracing::debug!(
                    system_prompt = %truncate_for_log(&system_prompt, MAX_PROMPT_LOG_CHARS),
                    "planner system prompt"
                );
            }

            let planner_cfg = LlmPlannerConfig {
                model,
                temperature,
                max_history: config.planner.max_history,
                system_prompt,
                log_full_prompts: config.planner.log_full_prompts,
                reactor_enabled: config.runtime.reactor.enabled,
                reactor_default_derivation_policy: config.runtime.reactor.default_derivation_policy,
            };

            let planner: LlmPlanner<Arc<dyn LlmClient>> = LlmPlanner::new(client, planner_cfg);
            Ok(Arc::new(planner))
        }
        "deterministic" => Ok(Arc::new(DeterministicPlanner)),
        other => Err(BootstrapError::UnsupportedPlannerMode(other.to_string())),
    }
}

pub(super) fn build_agent_step_executor(
    config: &OrchestralConfig,
) -> Result<Option<Arc<dyn orchestral_core::executor::AgentStepExecutor>>, BootstrapError> {
    match config.planner.mode.as_str() {
        "llm" => {
            let selection = resolve_planner_backend_selection(config)?;
            let profile = selection.profile;
            let backend = selection.backend;

            let model = config
                .planner
                .model
                .clone()
                .or_else(|| profile.as_ref().map(|p| p.model.clone()))
                .unwrap_or_else(|| LlmPlannerConfig::default().model);
            let temperature_candidate = config
                .planner
                .temperature
                .or_else(|| profile.as_ref().and_then(|p| p.temperature))
                .unwrap_or_else(|| LlmPlannerConfig::default().temperature);
            let temperature = profile
                .as_ref()
                .map(|p| p.clamp_temperature(temperature_candidate))
                .unwrap_or(temperature_candidate);

            let invocation = LlmInvocationConfig {
                model: model.clone(),
                temperature,
                max_tokens: LlmInvocationConfig::default().max_tokens,
                normalize_response: true,
            };

            let client = DefaultLlmClientFactory::new().build(&backend, &invocation)?;
            let executor =
                LlmAgentExecutor::new(client, LlmAgentExecutorConfig { model, temperature });
            Ok(Some(Arc::new(executor)))
        }
        "deterministic" => Ok(None),
        other => Err(BootstrapError::UnsupportedPlannerMode(other.to_string())),
    }
}

pub(super) fn build_runtime_component_options(
    config: &OrchestralConfig,
) -> serde_json::Map<String, serde_json::Value> {
    let mut options = serde_json::Map::new();
    options.insert(
        "stores".to_string(),
        json!({
            "event": store_spec_to_json(&config.stores.event),
            "task": store_spec_to_json(&config.stores.task),
            "reference": store_spec_to_json(&config.stores.reference),
        }),
    );
    options.insert(
        "blobs".to_string(),
        json!({
            "mode": config.blobs.mode.clone(),
            "catalog": {
                "backend": config.blobs.catalog.backend.clone(),
                "connection_url": config.blobs.catalog.connection_url.clone(),
                "table_prefix": config.blobs.catalog.table_prefix.clone(),
            },
            "local": {
                "root_dir": config.blobs.local.root_dir.clone(),
            },
            "hybrid": {
                "write_to": config.blobs.hybrid.write_to.clone(),
            }
        }),
    );
    options
}

fn store_spec_to_json(spec: &StoreSpec) -> serde_json::Value {
    json!({
        "backend": spec.backend.clone(),
        "connection_url": spec.connection_url.clone(),
        "key_prefix": spec.key_prefix.clone(),
    })
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

fn resolve_planner_backend_selection(
    config: &OrchestralConfig,
) -> Result<PlannerBackendSelection, BootstrapError> {
    let requested_profile = if let Some(profile_name) = &config.planner.model_profile {
        Some(
            config
                .providers
                .get_model(profile_name)
                .ok_or_else(|| BootstrapError::ModelProfileNotFound(profile_name.clone()))?,
        )
    } else {
        config.providers.get_default_model()
    };

    let requested_backend = if let Some(name) = &config.planner.backend {
        config
            .providers
            .get_backend(name)
            .ok_or_else(|| BootstrapError::BackendNotFound(name.clone()))?
    } else if let Some(profile) = &requested_profile {
        let backend_name = profile
            .backend
            .clone()
            .ok_or(BootstrapError::MissingProviderConfig)?;
        config
            .providers
            .get_backend(&backend_name)
            .ok_or(BootstrapError::BackendNotFound(backend_name))?
    } else {
        config
            .providers
            .get_default_backend()
            .ok_or(BootstrapError::MissingProviderConfig)?
    };

    if backend_has_available_api_key(&requested_backend) {
        return Ok(PlannerBackendSelection {
            backend: requested_backend,
            profile: requested_profile,
            fell_back_from: None,
        });
    }

    if config.planner.model.is_some() {
        return Ok(PlannerBackendSelection {
            backend: requested_backend,
            profile: requested_profile,
            fell_back_from: None,
        });
    }

    if let Some((backend, profile)) =
        select_fallback_backend_and_profile(config, &requested_backend.name)
    {
        return Ok(PlannerBackendSelection {
            backend,
            profile,
            fell_back_from: Some(requested_backend.name),
        });
    }

    Ok(PlannerBackendSelection {
        backend: requested_backend,
        profile: requested_profile,
        fell_back_from: None,
    })
}

fn backend_has_available_api_key(backend: &orchestral_core::config::BackendSpec) -> bool {
    if backend.kind.eq_ignore_ascii_case("ollama") {
        return true;
    }
    backend.resolve_api_key().is_ok()
}

fn select_fallback_backend_and_profile(
    config: &OrchestralConfig,
    excluded_backend: &str,
) -> Option<(
    orchestral_core::config::BackendSpec,
    Option<orchestral_core::config::ModelProfile>,
)> {
    let mut backends = config.providers.normalized_backends();
    backends.sort_by_key(|backend| backend_priority(&backend.kind));
    for backend in backends {
        if backend.name == excluded_backend || !backend_has_available_api_key(&backend) {
            continue;
        }
        let profile = preferred_model_for_backend(config, &backend.name);
        if profile.is_some() {
            return Some((backend, profile));
        }
    }
    None
}

fn preferred_model_for_backend(
    config: &OrchestralConfig,
    backend_name: &str,
) -> Option<orchestral_core::config::ModelProfile> {
    if let Some(profile_name) = &config.planner.model_profile {
        if let Some(profile) = config.providers.get_model(profile_name) {
            if profile.backend.as_deref() == Some(backend_name) {
                return Some(profile);
            }
        }
    }
    if let Some(default_model) = config.providers.get_default_model() {
        if default_model.backend.as_deref() == Some(backend_name) {
            return Some(default_model);
        }
    }
    config
        .providers
        .normalized_models()
        .into_iter()
        .find(|profile| profile.backend.as_deref() == Some(backend_name))
}

fn backend_priority(kind: &str) -> usize {
    match kind.trim().to_ascii_lowercase().as_str() {
        "openai" => 0,
        "google" | "gemini" => 1,
        "anthropic" | "claude" => 2,
        "openrouter" => 3,
        _ => 10,
    }
}

struct DeterministicPlanner;

#[async_trait::async_trait]
impl Planner for DeterministicPlanner {
    async fn plan(
        &self,
        intent: &Intent,
        context: &PlannerContext,
    ) -> Result<PlannerOutput, PlanError> {
        let _ = context;
        Ok(PlannerOutput::DirectResponse(format!(
            "Deterministic planner cannot execute tasks after RFC0310 transition: {}",
            intent.content
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::config::{BackendSpec, ModelPolicy, ModelProfile, ProvidersConfig};
    use serde_json::json;
    use std::sync::{Mutex, OnceLock};

    const KEY_ENV_NAMES: &[&str] = &[
        "OPENAI_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "ANTHROPIC_API_KEY",
        "CLAUDE_API_KEY",
        "OPENROUTER_API_KEY",
    ];

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn clear_key_envs() {
        for name in KEY_ENV_NAMES {
            std::env::remove_var(name);
        }
    }

    fn backend(name: &str, kind: &str, api_key_env: &str) -> BackendSpec {
        BackendSpec {
            name: name.to_string(),
            kind: kind.to_string(),
            endpoint: None,
            api_key_env: Some(api_key_env.to_string()),
            config: json!({}),
        }
    }

    fn model(name: &str, backend: &str, model: &str) -> ModelProfile {
        ModelProfile {
            name: name.to_string(),
            backend: Some(backend.to_string()),
            model: model.to_string(),
            temperature: Some(0.2),
            max_tokens: None,
            system_prompt: None,
            policy: ModelPolicy::default(),
            config: json!({}),
        }
    }

    #[test]
    fn test_resolve_planner_backend_selection_falls_back_to_available_openai() {
        let _guard = env_lock().lock().expect("env lock");
        clear_key_envs();
        std::env::set_var("OPENAI_API_KEY", "openai-key");

        let mut config = OrchestralConfig::default();
        config.planner.mode = "llm".to_string();
        config.planner.backend = Some("openrouter".to_string());
        config.planner.model_profile = None;
        config.providers = ProvidersConfig {
            default_backend: Some("openrouter".to_string()),
            default_model: Some("gpt-4o-mini".to_string()),
            backends: vec![
                backend("openrouter", "openrouter", "OPENROUTER_API_KEY"),
                backend("openai", "openai", "OPENAI_API_KEY"),
                backend("google", "google", "GOOGLE_API_KEY"),
            ],
            models: vec![
                model("gpt-4o-mini", "openai", "gpt-4o-mini"),
                model("claude-sonnet-4-5-openrouter", "openrouter", "anthropic/claude-sonnet-4.5"),
            ],
            ..ProvidersConfig::default()
        };

        let selection = resolve_planner_backend_selection(&config).expect("selection");
        assert_eq!(selection.backend.name, "openai");
        assert_eq!(selection.profile.as_ref().map(|p| p.name.as_str()), Some("gpt-4o-mini"));
        assert_eq!(selection.fell_back_from.as_deref(), Some("openrouter"));

        clear_key_envs();
    }

    #[test]
    fn test_resolve_planner_backend_selection_prefers_requested_backend_when_key_exists() {
        let _guard = env_lock().lock().expect("env lock");
        clear_key_envs();
        std::env::set_var("OPENROUTER_API_KEY", "or-key");
        std::env::set_var("OPENAI_API_KEY", "openai-key");

        let mut config = OrchestralConfig::default();
        config.planner.mode = "llm".to_string();
        config.planner.backend = Some("openrouter".to_string());
        config.providers = ProvidersConfig {
            default_backend: Some("openrouter".to_string()),
            default_model: Some("claude-sonnet-4-5-openrouter".to_string()),
            backends: vec![
                backend("openrouter", "openrouter", "OPENROUTER_API_KEY"),
                backend("openai", "openai", "OPENAI_API_KEY"),
            ],
            models: vec![
                model("gpt-4o-mini", "openai", "gpt-4o-mini"),
                model("claude-sonnet-4-5-openrouter", "openrouter", "anthropic/claude-sonnet-4.5"),
            ],
            ..ProvidersConfig::default()
        };

        let selection = resolve_planner_backend_selection(&config).expect("selection");
        assert_eq!(selection.backend.name, "openrouter");
        assert!(selection.fell_back_from.is_none());

        clear_key_envs();
    }
}
