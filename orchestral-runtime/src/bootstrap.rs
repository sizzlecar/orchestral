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
use orchestral_core::executor::Executor;
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::planner::{PlanError, Planner, PlannerContext};
use orchestral_core::store::{ReferenceStore, TaskStore};
use orchestral_core::types::{Intent, Plan, Step};
use orchestral_planners::{
    DefaultLlmClientFactory, LlmBuildError, LlmClient, LlmClientFactory, LlmInvocationConfig,
    LlmPlanner, LlmPlannerConfig,
};
use orchestral_stores::{
    EventStore, InMemoryEventStore, InMemoryReferenceStore, InMemoryTaskStore,
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
}

/// Running app bundle created from unified config.
pub struct RuntimeApp {
    pub orchestrator: Orchestrator,
    pub config_manager: Arc<ConfigManager>,
    pub action_registry_manager: Arc<ActionRegistryManager>,
    _action_watcher: Option<ActionWatcher>,
}

impl RuntimeApp {
    /// Create a runnable app from a single `orchestral.yaml`.
    pub async fn from_config_path(path: impl Into<PathBuf>) -> Result<Self, BootstrapError> {
        let path = path.into();
        let config_manager = Arc::new(ConfigManager::new(path.clone()));
        config_manager.load().await?;
        let config = config_manager.config().read().await.clone();

        let event_store = build_event_store(&config.stores.event)?;
        let task_store = build_task_store(&config.stores.task)?;
        let reference_store = build_reference_store(&config.stores.reference)?;

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

        let executor = Executor::with_registry(action_registry_manager.registry());
        let planner = build_planner(&config)?;

        let mut normalizer = PlanNormalizer::new();
        {
            let registry = executor.action_registry.read().await;
            for name in registry.names() {
                normalizer.register_action(name);
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

fn build_event_store(spec: &StoreSpec) -> Result<Arc<dyn EventStore>, BootstrapError> {
    ensure_in_memory(spec, "event")?;
    Ok(Arc::new(InMemoryEventStore::new()))
}

fn build_task_store(spec: &StoreSpec) -> Result<Arc<dyn TaskStore>, BootstrapError> {
    ensure_in_memory(spec, "task")?;
    Ok(Arc::new(InMemoryTaskStore::new()))
}

fn build_reference_store(spec: &StoreSpec) -> Result<Arc<dyn ReferenceStore>, BootstrapError> {
    ensure_in_memory(spec, "reference")?;
    Ok(Arc::new(InMemoryReferenceStore::new()))
}

fn ensure_in_memory(spec: &StoreSpec, store_name: &str) -> Result<(), BootstrapError> {
    if spec.backend.eq_ignore_ascii_case("in_memory") {
        return Ok(());
    }
    Err(BootstrapError::UnsupportedStoreBackend {
        store: store_name.to_string(),
        backend: spec.backend.clone(),
    })
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
