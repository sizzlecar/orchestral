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
            let backend = if let Some(name) = &config.planner.backend {
                config
                    .providers
                    .get_backend(name)
                    .ok_or_else(|| BootstrapError::BackendNotFound(name.clone()))?
            } else if let Some(profile_name) = &config.planner.model_profile {
                let profile = config
                    .providers
                    .get_model(profile_name)
                    .ok_or_else(|| BootstrapError::ModelProfileNotFound(profile_name.clone()))?;
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

            let profile =
                if let Some(profile_name) = &config.planner.model_profile {
                    Some(config.providers.get_model(profile_name).ok_or_else(|| {
                        BootstrapError::ModelProfileNotFound(profile_name.clone())
                    })?)
                } else {
                    config.providers.get_default_model()
                };

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
            let profile = if let Some(profile_name) = &config.planner.model_profile {
                Some(config.providers.get_model(profile_name).ok_or_else(|| {
                    BootstrapError::ModelProfileNotFound(profile_name.to_string())
                })?)
            } else {
                config.providers.get_default_model()
            };

            let backend = if let Some(profile) = &profile {
                if let Some(backend_name) = &profile.backend {
                    config
                        .providers
                        .get_backend(backend_name)
                        .ok_or_else(|| BootstrapError::BackendNotFound(backend_name.to_string()))?
                } else {
                    config
                        .providers
                        .get_default_backend()
                        .ok_or(BootstrapError::MissingProviderConfig)?
                }
            } else if let Some(backend_name) = &config.planner.backend {
                config
                    .providers
                    .get_backend(backend_name)
                    .ok_or_else(|| BootstrapError::BackendNotFound(backend_name.to_string()))?
            } else if let Some(default_provider) = config.providers.get_default() {
                config
                    .providers
                    .get_backend(&default_provider.name)
                    .ok_or_else(|| BootstrapError::BackendNotFound(default_provider.name.clone()))?
            } else {
                return Err(BootstrapError::MissingProviderConfig);
            };

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
