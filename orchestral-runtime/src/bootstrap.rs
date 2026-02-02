//! Bootstrap helpers for starting Orchestral from a single YAML config.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use thiserror::Error;

use orchestral_actions::{
    ActionConfigError, ActionRegistryManager, ActionWatcher, DefaultActionFactory,
};
use orchestral_config::{ConfigError, ConfigManager, OrchestralConfig, StoreSpec};
use orchestral_context::{BasicContextBuilder, TokenBudget};
use orchestral_core::action::extract_meta;
use orchestral_core::executor::Executor;
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::planner::{PlanError, Planner, PlannerContext};
use orchestral_core::store::{ReferenceStore, StoreError, TaskStore};
use orchestral_core::types::{Intent, Plan, Step};
use orchestral_planners::{
    DefaultLlmClientFactory, LlmBuildError, LlmClient, LlmClientFactory, LlmInvocationConfig,
    LlmPlanner, LlmPlannerConfig,
};
use orchestral_stores::{
    EventStore, InMemoryEventStore, InMemoryReferenceStore, InMemoryTaskStore, RedisEventStore,
    RedisReferenceStore, RedisTaskStore,
};

use crate::orchestrator::OrchestratorConfig;
use crate::{
    ConcurrencyPolicy, DefaultConcurrencyPolicy, Orchestrator, ParallelConcurrencyPolicy,
    QueueConcurrencyPolicy, RejectWhenBusyConcurrencyPolicy, Thread, ThreadRuntime,
    ThreadRuntimeConfig,
};

/// Runtime bootstrap errors.
#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("action config error: {0}")]
    ActionConfig(#[from] ActionConfigError),
    #[error("planner build error: {0}")]
    PlannerBuild(#[from] LlmBuildError),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("unsupported planner mode: {0}")]
    UnsupportedPlannerMode(String),
    #[error("unsupported concurrency policy: {0}")]
    UnsupportedConcurrencyPolicy(String),
    #[error("missing provider config for planner mode llm")]
    MissingProviderConfig,
    #[error("backend '{0}' not found")]
    BackendNotFound(String),
    #[error("model profile '{0}' not found")]
    ModelProfileNotFound(String),
    #[error("unsupported store backend for {store}: {backend}")]
    UnsupportedStoreBackend { store: String, backend: String },
    #[error("missing connection_url for {store} store backend")]
    MissingStoreConnectionUrl { store: String },
}

/// Running app bundle created from unified config.
pub struct RuntimeApp {
    pub orchestrator: Orchestrator,
    pub config_manager: Arc<ConfigManager>,
    pub action_registry_manager: Arc<ActionRegistryManager>,
    _action_watcher: Option<ActionWatcher>,
}

/// Factory abstraction for pluggable store backends.
pub trait StoreBackendFactory: Send + Sync {
    fn build_event_store(&self, spec: &StoreSpec) -> Result<Arc<dyn EventStore>, BootstrapError>;
    fn build_task_store(&self, spec: &StoreSpec) -> Result<Arc<dyn TaskStore>, BootstrapError>;
    fn build_reference_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn ReferenceStore>, BootstrapError>;
}

/// Default store factory supporting in-memory and Redis backends.
pub struct DefaultStoreBackendFactory;

impl DefaultStoreBackendFactory {
    fn backend_name(spec: &StoreSpec) -> String {
        spec.backend.trim().to_ascii_lowercase()
    }

    fn redis_url(spec: &StoreSpec, store: &str) -> Result<String, BootstrapError> {
        spec.connection_url
            .clone()
            .ok_or_else(|| BootstrapError::MissingStoreConnectionUrl {
                store: store.to_string(),
            })
    }

    fn key_prefix(spec: &StoreSpec, default_prefix: &str) -> String {
        spec.key_prefix
            .clone()
            .unwrap_or_else(|| default_prefix.to_string())
    }
}

impl StoreBackendFactory for DefaultStoreBackendFactory {
    fn build_event_store(&self, spec: &StoreSpec) -> Result<Arc<dyn EventStore>, BootstrapError> {
        match Self::backend_name(spec).as_str() {
            "in_memory" | "memory" => Ok(Arc::new(InMemoryEventStore::new())),
            "redis" => {
                let url = Self::redis_url(spec, "event")?;
                let prefix = Self::key_prefix(spec, "orchestral:event");
                let store = RedisEventStore::new(&url, prefix).map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "event".to_string(),
                backend: backend.to_string(),
            }),
        }
    }

    fn build_task_store(&self, spec: &StoreSpec) -> Result<Arc<dyn TaskStore>, BootstrapError> {
        match Self::backend_name(spec).as_str() {
            "in_memory" | "memory" => Ok(Arc::new(InMemoryTaskStore::new())),
            "redis" => {
                let url = Self::redis_url(spec, "task")?;
                let prefix = Self::key_prefix(spec, "orchestral:task");
                let store = RedisTaskStore::new(&url, prefix).map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "task".to_string(),
                backend: backend.to_string(),
            }),
        }
    }

    fn build_reference_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn ReferenceStore>, BootstrapError> {
        match Self::backend_name(spec).as_str() {
            "in_memory" | "memory" => Ok(Arc::new(InMemoryReferenceStore::new())),
            "redis" => {
                let url = Self::redis_url(spec, "reference")?;
                let prefix = Self::key_prefix(spec, "orchestral:reference");
                let store =
                    RedisReferenceStore::new(&url, prefix).map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "reference".to_string(),
                backend: backend.to_string(),
            }),
        }
    }
}

impl RuntimeApp {
    /// Create a runnable app from a single `orchestral.yaml`.
    pub async fn from_config_path(path: impl Into<PathBuf>) -> Result<Self, BootstrapError> {
        Self::from_config_path_with_store_factory(path, Arc::new(DefaultStoreBackendFactory)).await
    }

    /// Create a runnable app and inject custom store backend factory.
    pub async fn from_config_path_with_store_factory(
        path: impl Into<PathBuf>,
        store_factory: Arc<dyn StoreBackendFactory>,
    ) -> Result<Self, BootstrapError> {
        let path = path.into();
        let config_manager = Arc::new(ConfigManager::new(path.clone()));
        config_manager.load().await?;
        let config = config_manager.config().read().await.clone();

        let event_store = store_factory.build_event_store(&config.stores.event)?;
        let task_store = store_factory.build_task_store(&config.stores.task)?;
        let reference_store = store_factory.build_reference_store(&config.stores.reference)?;

        let policy = concurrency_policy_from_name(&config.runtime.concurrency_policy)?;
        let runtime_cfg = ThreadRuntimeConfig {
            max_interactions_per_thread: config.runtime.max_interactions_per_thread,
            auto_cleanup: config.runtime.auto_cleanup,
        };

        let thread_runtime = ThreadRuntime::with_policy_and_config(
            Thread::new(),
            event_store.clone(),
            policy,
            runtime_cfg,
        );

        let action_factory = Arc::new(DefaultActionFactory::new());
        let action_registry_manager = Arc::new(ActionRegistryManager::new(path, action_factory));
        action_registry_manager.load().await?;
        let action_watcher = if config.actions.hot_reload {
            Some(action_registry_manager.start_watching()?)
        } else {
            None
        };

        let executor = Executor::with_registry(action_registry_manager.registry())
            .with_io_contract(config.runtime.strict_imports, config.runtime.strict_exports);
        let planner = build_planner(&config)?;

        let mut normalizer = PlanNormalizer::new();
        {
            let registry = executor.action_registry.read().await;
            for name in registry.names() {
                if let Some(action) = registry.get(&name) {
                    normalizer.register_action_meta(&extract_meta(action.as_ref()));
                } else {
                    normalizer.register_action(name);
                }
            }
        }

        let context_builder = Arc::new(BasicContextBuilder::new(
            event_store.clone(),
            reference_store.clone(),
        ));

        let orchestrator_cfg = OrchestratorConfig {
            history_limit: config.context.history_limit,
            context_budget: TokenBudget::new(config.context.max_tokens),
            include_history: config.context.include_history,
            include_references: config.context.include_references,
        };

        let orchestrator = Orchestrator::with_config(
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            reference_store,
            orchestrator_cfg,
        )
        .with_context_builder(context_builder);

        Ok(Self {
            orchestrator,
            config_manager,
            action_registry_manager,
            _action_watcher: action_watcher,
        })
    }
}

fn concurrency_policy_from_name(
    policy: &str,
) -> Result<Arc<dyn ConcurrencyPolicy>, BootstrapError> {
    match policy {
        "interrupt_and_start_new" | "interrupt" => Ok(Arc::new(DefaultConcurrencyPolicy)),
        "queue" => Ok(Arc::new(QueueConcurrencyPolicy)),
        "parallel" => Ok(Arc::new(ParallelConcurrencyPolicy::default())),
        "reject" | "reject_when_busy" => Ok(Arc::new(RejectWhenBusyConcurrencyPolicy)),
        other => Err(BootstrapError::UnsupportedConcurrencyPolicy(
            other.to_string(),
        )),
    }
}

fn build_planner(config: &OrchestralConfig) -> Result<Arc<dyn Planner>, BootstrapError> {
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
                normalize_response: true,
            };

            let client = DefaultLlmClientFactory::new().build(&backend, &invocation)?;
            let default_cfg = LlmPlannerConfig::default();
            let system_prompt = config
                .planner
                .model_profile
                .as_ref()
                .and_then(|name| config.providers.get_model(name))
                .and_then(|p| p.system_prompt.clone())
                .or_else(|| backend.get_config::<String>("system_prompt"))
                .unwrap_or(default_cfg.system_prompt);

            let planner_cfg = LlmPlannerConfig {
                model,
                temperature,
                max_history: config.planner.max_history,
                system_prompt,
            };

            let planner: LlmPlanner<Arc<dyn LlmClient>> = LlmPlanner::new(client, planner_cfg);
            Ok(Arc::new(planner))
        }
        "deterministic" => Ok(Arc::new(DeterministicPlanner)),
        other => Err(BootstrapError::UnsupportedPlannerMode(other.to_string())),
    }
}

struct DeterministicPlanner;

#[async_trait]
impl Planner for DeterministicPlanner {
    async fn plan(&self, intent: &Intent, context: &PlannerContext) -> Result<Plan, PlanError> {
        let action_name = context
            .available_actions
            .iter()
            .find(|a| a.name == "echo")
            .map(|a| a.name.clone())
            .or_else(|| context.available_actions.first().map(|a| a.name.clone()))
            .ok_or(PlanError::NoSuitableActions)?;

        Ok(Plan::new(
            format!("Deterministic plan for intent: {}", intent.content),
            vec![Step::action("s1", action_name).with_params(json!({
                "message": intent.content
            }))],
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_store_factory_rejects_redis_without_connection_url() {
        let factory = DefaultStoreBackendFactory;
        let spec = StoreSpec {
            backend: "redis".to_string(),
            connection_url: None,
            key_prefix: None,
        };

        let result = factory.build_event_store(&spec);
        assert!(matches!(
            result,
            Err(BootstrapError::MissingStoreConnectionUrl { store }) if store == "event"
        ));
    }

    #[test]
    fn test_default_store_factory_accepts_redis_spec() {
        let factory = DefaultStoreBackendFactory;
        let spec = StoreSpec {
            backend: "redis".to_string(),
            connection_url: Some("redis://127.0.0.1/".to_string()),
            key_prefix: Some("orchestral:test".to_string()),
        };

        assert!(factory.build_event_store(&spec).is_ok());
        assert!(factory.build_task_store(&spec).is_ok());
        assert!(factory.build_reference_store(&spec).is_ok());
    }
}
