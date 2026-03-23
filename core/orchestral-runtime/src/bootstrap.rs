//! Bootstrap helpers for starting Orchestral from a single YAML config.

mod blob_store;
mod components;
mod observability;
mod runtime_builder;

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use thiserror::Error;

use crate::action::{
    ActionConfigError, ActionFactory, ActionRegistryManager, DefaultActionFactory,
};
use crate::agent::default_action_preflight_hook;
use crate::context::{BasicContextBuilder, TokenBudget};
use crate::planner::LlmBuildError;
use crate::skill::discovery::discover_skills;
use crate::skill::SkillCatalog;
use orchestral_core::action::extract_meta;
use orchestral_core::config::{load_config, ConfigError};
use orchestral_core::executor::Executor;
use orchestral_core::io::{BlobIoError, BlobStore};
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::spi::{
    ComponentRegistry, HookRegistry, RuntimeBuildRequest, RuntimeComponentFactory, SpiError,
    SpiMeta, StoreBundle,
};
use orchestral_core::store::{InMemoryEventStore, InMemoryTaskStore, StoreError};

use crate::orchestrator::OrchestratorConfig;
use crate::{Orchestrator, Thread, ThreadRuntime, ThreadRuntimeConfig};

pub use self::blob_store::InMemoryBlobStore;
use self::observability::init_tracing_if_needed;
use self::runtime_builder::{
    build_agent_step_executor, build_planner, build_runtime_component_options,
    concurrency_policy_from_name,
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
    #[error("unsupported interpreter mode: {0}")]
    UnsupportedInterpreterMode(String),
    #[error("unsupported concurrency policy: {0}")]
    UnsupportedConcurrencyPolicy(String),
    #[error("missing provider config for planner mode llm")]
    MissingProviderConfig,
    #[error("backend '{0}' not found")]
    BackendNotFound(String),
    #[error("model profile '{0}' not found")]
    ModelProfileNotFound(String),
    #[error("blob io error: {0}")]
    Blob(#[from] BlobIoError),
    #[error("spi error: {0}")]
    Spi(#[from] SpiError),
}

/// Running app bundle created from unified config.
pub struct RuntimeApp {
    pub orchestrator: Orchestrator,
    pub blob_store: Arc<dyn BlobStore>,
    pub hook_registry: Arc<HookRegistry>,
    pub action_registry_manager: Arc<ActionRegistryManager>,
}

static TRACING_INIT: OnceLock<()> = OnceLock::new();

/// Default component factory providing in-memory stores and blob storage.
pub struct DefaultRuntimeComponentFactory;

#[async_trait]
impl RuntimeComponentFactory for DefaultRuntimeComponentFactory {
    async fn build(&self, _request: &RuntimeBuildRequest) -> Result<ComponentRegistry, SpiError> {
        Ok(ComponentRegistry::new()
            .with_stores(StoreBundle {
                event_store: Arc::new(InMemoryEventStore::new()),
                task_store: Arc::new(InMemoryTaskStore::new()),
            })
            .with_blob_store(Arc::new(InMemoryBlobStore::default())))
    }
}

impl RuntimeApp {
    /// Create a runnable app from a single `orchestral.yaml`.
    pub async fn from_config_path(path: impl Into<PathBuf>) -> Result<Self, BootstrapError> {
        let action_factory: Arc<dyn ActionFactory> = Arc::new(DefaultActionFactory::new());
        Self::from_config_path_with_spi(
            path,
            Arc::new(DefaultRuntimeComponentFactory),
            Arc::new(HookRegistry::new()),
            action_factory,
        )
        .await
    }

    /// Create a runnable app and inject custom runtime component factory.
    pub async fn from_config_path_with_component_factory(
        path: impl Into<PathBuf>,
        component_factory: Arc<dyn RuntimeComponentFactory>,
    ) -> Result<Self, BootstrapError> {
        let action_factory: Arc<dyn ActionFactory> = Arc::new(DefaultActionFactory::new());
        Self::from_config_path_with_spi(
            path,
            component_factory,
            Arc::new(HookRegistry::new()),
            action_factory,
        )
        .await
    }

    /// Create a runnable app and inject component factory + hook registry.
    pub async fn from_config_path_with_spi(
        path: impl Into<PathBuf>,
        component_factory: Arc<dyn RuntimeComponentFactory>,
        hook_registry: Arc<HookRegistry>,
        action_factory: Arc<dyn ActionFactory>,
    ) -> Result<Self, BootstrapError> {
        let path = path.into();
        let config = load_config(&path)?;
        init_tracing_if_needed(&TRACING_INIT, &config.observability);
        let build_request = RuntimeBuildRequest {
            meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
            config_path: path.to_string_lossy().to_string(),
            profile: None,
            options: build_runtime_component_options(&config),
        };
        let components = component_factory.build(&build_request).await?;
        let stores = components.stores.unwrap_or_else(|| {
            tracing::warn!("component factory missing stores; fallback to in-memory stores");
            StoreBundle {
                event_store: Arc::new(InMemoryEventStore::new()),
                task_store: Arc::new(InMemoryTaskStore::new()),
            }
        });
        let blob_store: Arc<dyn BlobStore> = components.blob_store.unwrap_or_else(|| {
            tracing::warn!(
                "component factory missing blob_store; fallback to in-memory blob store"
            );
            Arc::new(InMemoryBlobStore::default())
        });

        let policy = concurrency_policy_from_name(&config.runtime.concurrency_policy)?;
        let runtime_cfg = ThreadRuntimeConfig {
            max_interactions_per_thread: config.runtime.max_interactions_per_thread,
            auto_cleanup: config.runtime.auto_cleanup,
        };

        let thread_runtime = ThreadRuntime::with_policy_and_config(
            Thread::new(),
            stores.event_store.clone(),
            policy,
            runtime_cfg,
        );

        let action_registry_manager =
            Arc::new(ActionRegistryManager::new(path.clone(), action_factory));
        action_registry_manager.load().await?;

        let executor = Executor::with_registry(action_registry_manager.registry())
            .with_action_preflight_hook(default_action_preflight_hook())
            .with_export_contract(config.runtime.strict_exports);
        let executor = if let Some(agent_executor) = build_agent_step_executor(&config)? {
            executor.with_agent_step_executor(agent_executor)
        } else {
            executor
        };
        let planner = build_planner(&config)?;
        let skill_entries = discover_skills(&config, &path)?;
        let skill_catalog = Arc::new(SkillCatalog::new(
            skill_entries,
            config.extensions.skill.max_active_skills,
        ));

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
        let context_builder = Arc::new(BasicContextBuilder::new(stores.event_store.clone()));

        let orchestrator_cfg = OrchestratorConfig {
            history_limit: config.context.history_limit,
            context_budget: TokenBudget::new(config.context.max_tokens),
            include_history: config.context.include_history,
            auto_replan_once: true,
            auto_repair_plan_once: true,
            max_planner_iterations: config.runtime.max_planner_iterations,
        };

        let orchestrator = Orchestrator::with_config(
            thread_runtime,
            planner,
            normalizer,
            executor,
            stores.task_store,
            orchestrator_cfg,
        )
        .with_context_builder(context_builder)
        .with_hook_registry(hook_registry.clone())
        .with_skill_catalog(skill_catalog);

        Ok(Self {
            orchestrator,
            blob_store,
            hook_registry,
            action_registry_manager,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use orchestral_core::config::OrchestralConfig;
    use orchestral_core::io::BlobWriteRequest;

    #[tokio::test]
    async fn test_default_component_factory_builds_min_runtime_components() {
        let factory = DefaultRuntimeComponentFactory;
        let request = RuntimeBuildRequest {
            meta: SpiMeta::runtime_defaults("0.1.0"),
            config_path: "/tmp/orchestral.yaml".to_string(),
            profile: None,
            options: serde_json::Map::new(),
        };
        let components = factory.build(&request).await.expect("components");

        assert!(components.stores.is_some());
        assert!(components.blob_store.is_some());
    }

    #[tokio::test]
    async fn test_default_blob_store_writes_and_reads() {
        let store: Arc<dyn BlobStore> = Arc::new(InMemoryBlobStore::default());
        let payload = vec![1_u8, 2, 3];
        let request = BlobWriteRequest::new(Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(payload))
        })));
        let written = store.write(request).await.expect("write");
        assert_eq!(written.byte_size, 3);
        let read = store.read(&written.id).await.expect("read");
        assert_eq!(read.meta.id, written.id);
    }

    #[test]
    fn test_runtime_component_options_include_store_and_blob_hints() {
        let config = OrchestralConfig::default();
        let options = build_runtime_component_options(&config);

        assert!(options.get("stores").is_some());
        assert!(options.get("blobs").is_some());
    }

    #[test]
    fn test_queue_concurrency_policy_is_rejected_in_bootstrap() {
        let err = match concurrency_policy_from_name("queue") {
            Ok(_) => panic!("queue should be unsupported"),
            Err(err) => err,
        };
        match err {
            BootstrapError::UnsupportedConcurrencyPolicy(message) => {
                assert!(message.contains("not implemented"));
            }
            other => panic!("expected UnsupportedConcurrencyPolicy, got {}", other),
        }
    }
}
