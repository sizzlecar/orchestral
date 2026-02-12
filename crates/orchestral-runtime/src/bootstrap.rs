//! Bootstrap helpers for starting Orchestral from a single YAML config.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::json;
use thiserror::Error;
use tokio::sync::RwLock;

use orchestral_actions::{
    ActionConfigError, ActionRegistryManager, ActionWatcher, DefaultActionFactory,
};
use orchestral_config::{
    ConfigError, ConfigManager, ObservabilityConfig, OrchestralConfig, StoreSpec,
};
use orchestral_context::{BasicContextBuilder, TokenBudget};
use orchestral_core::action::extract_meta;
use orchestral_core::executor::Executor;
use orchestral_core::interpreter::{NoopResultInterpreter, ResultInterpreter};
use orchestral_core::io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobWriteRequest,
};
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::planner::{PlanError, Planner, PlannerContext, PlannerOutput};
use orchestral_core::store::{EventStore, ReferenceStore, StoreError, TaskStore};
use orchestral_core::types::{Intent, Plan, Step};
use orchestral_planners::{
    DefaultLlmClientFactory, LlmBuildError, LlmClient, LlmClientFactory, LlmInvocationConfig,
    LlmPlanner, LlmPlannerConfig,
};
use orchestral_stores::{InMemoryEventStore, InMemoryReferenceStore, InMemoryTaskStore};

use crate::interpreter::{LlmResultInterpreter, LlmResultInterpreterConfig};
use crate::orchestrator::OrchestratorConfig;
use crate::{
    ConcurrencyPolicy, DefaultConcurrencyPolicy, HookRegistry, Orchestrator,
    ParallelConcurrencyPolicy, QueueConcurrencyPolicy, RejectWhenBusyConcurrencyPolicy, Thread,
    ThreadRuntime, ThreadRuntimeConfig,
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
    #[error("unsupported store backend for {store}: {backend}")]
    UnsupportedStoreBackend { store: String, backend: String },
    #[error("missing connection_url for {store} store backend")]
    MissingStoreConnectionUrl { store: String },
    #[error("blob io error: {0}")]
    Blob(#[from] BlobIoError),
}

/// Running app bundle created from unified config.
pub struct RuntimeApp {
    pub orchestrator: Orchestrator,
    pub blob_store: Arc<dyn BlobStore>,
    pub hook_registry: Arc<HookRegistry>,
    pub config_manager: Arc<ConfigManager>,
    pub action_registry_manager: Arc<ActionRegistryManager>,
    _action_watcher: Option<ActionWatcher>,
}

static TRACING_INIT: OnceLock<()> = OnceLock::new();
const MAX_PROMPT_LOG_CHARS: usize = 4_000;

/// Factory abstraction for pluggable store backends.
#[async_trait]
pub trait StoreBackendFactory: Send + Sync {
    async fn build_event_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn EventStore>, BootstrapError>;
    async fn build_task_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn TaskStore>, BootstrapError>;
    async fn build_reference_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn ReferenceStore>, BootstrapError>;
}

/// Factory abstraction for pluggable blob store implementations.
#[async_trait]
pub trait BlobStoreFactory: Send + Sync {
    async fn build_blob_store(
        &self,
        config: &OrchestralConfig,
    ) -> Result<Arc<dyn BlobStore>, BootstrapError>;
}

/// Default store factory supporting in-memory backends only.
pub struct DefaultStoreBackendFactory;

impl DefaultStoreBackendFactory {
    fn backend_name(spec: &StoreSpec) -> String {
        spec.backend.trim().to_ascii_lowercase()
    }
}

#[async_trait]
impl StoreBackendFactory for DefaultStoreBackendFactory {
    async fn build_event_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn EventStore>, BootstrapError> {
        match Self::backend_name(spec).as_str() {
            "in_memory" | "memory" => Ok(Arc::new(InMemoryEventStore::new())),
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "event".to_string(),
                backend: backend.to_string(),
            }),
        }
    }

    async fn build_task_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn TaskStore>, BootstrapError> {
        match Self::backend_name(spec).as_str() {
            "in_memory" | "memory" => Ok(Arc::new(InMemoryTaskStore::new())),
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "task".to_string(),
                backend: backend.to_string(),
            }),
        }
    }

    async fn build_reference_store(
        &self,
        spec: &StoreSpec,
    ) -> Result<Arc<dyn ReferenceStore>, BootstrapError> {
        match Self::backend_name(spec).as_str() {
            "in_memory" | "memory" => Ok(Arc::new(InMemoryReferenceStore::new())),
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "reference".to_string(),
                backend: backend.to_string(),
            }),
        }
    }
}

/// Default blob factory that uses an in-memory blob store.
pub struct DefaultBlobStoreFactory;

#[async_trait]
impl BlobStoreFactory for DefaultBlobStoreFactory {
    async fn build_blob_store(
        &self,
        _config: &OrchestralConfig,
    ) -> Result<Arc<dyn BlobStore>, BootstrapError> {
        Ok(Arc::new(InMemoryBlobStore::default()))
    }
}

impl RuntimeApp {
    /// Create a runnable app from a single `orchestral.yaml`.
    pub async fn from_config_path(path: impl Into<PathBuf>) -> Result<Self, BootstrapError> {
        Self::from_config_path_with_factories_and_hooks(
            path,
            Arc::new(DefaultStoreBackendFactory),
            Arc::new(DefaultBlobStoreFactory),
            Arc::new(HookRegistry::new()),
        )
        .await
    }

    /// Create a runnable app and inject custom store backend factory.
    pub async fn from_config_path_with_store_factory(
        path: impl Into<PathBuf>,
        store_factory: Arc<dyn StoreBackendFactory>,
    ) -> Result<Self, BootstrapError> {
        Self::from_config_path_with_factories_and_hooks(
            path,
            store_factory,
            Arc::new(DefaultBlobStoreFactory),
            Arc::new(HookRegistry::new()),
        )
        .await
    }

    /// Create a runnable app and inject custom store/blob factories.
    pub async fn from_config_path_with_factories(
        path: impl Into<PathBuf>,
        store_factory: Arc<dyn StoreBackendFactory>,
        blob_store_factory: Arc<dyn BlobStoreFactory>,
    ) -> Result<Self, BootstrapError> {
        Self::from_config_path_with_factories_and_hooks(
            path,
            store_factory,
            blob_store_factory,
            Arc::new(HookRegistry::new()),
        )
        .await
    }

    /// Create a runnable app and inject custom store/blob factories + hook registry.
    pub async fn from_config_path_with_factories_and_hooks(
        path: impl Into<PathBuf>,
        store_factory: Arc<dyn StoreBackendFactory>,
        blob_store_factory: Arc<dyn BlobStoreFactory>,
        hook_registry: Arc<HookRegistry>,
    ) -> Result<Self, BootstrapError> {
        let path = path.into();
        let config_manager = Arc::new(ConfigManager::new(path.clone()));
        config_manager.load().await?;
        let config = config_manager.config().read().await.clone();
        init_tracing_if_needed(&config.observability);
        let blob_store = blob_store_factory.build_blob_store(&config).await?;

        let event_store = store_factory
            .build_event_store(&config.stores.event)
            .await?;
        let task_store = store_factory.build_task_store(&config.stores.task).await?;
        let reference_store = store_factory
            .build_reference_store(&config.stores.reference)
            .await?;

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
            .with_export_contract(config.runtime.strict_exports);
        let planner = build_planner(&config)?;
        let result_interpreter = build_result_interpreter(&config)?;

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
            auto_replan_once: true,
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
        .with_context_builder(context_builder)
        .with_result_interpreter(result_interpreter)
        .with_hook_registry(hook_registry.clone());

        Ok(Self {
            orchestrator,
            blob_store,
            hook_registry,
            config_manager,
            action_registry_manager,
            _action_watcher: action_watcher,
        })
    }
}

#[derive(Clone)]
struct InMemoryBlobObject {
    meta: BlobMeta,
    bytes: Vec<u8>,
}

#[derive(Default)]
pub struct InMemoryBlobStore {
    objects: RwLock<HashMap<String, InMemoryBlobObject>>,
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn write(&self, mut request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
        let blob_id = BlobId::from(uuid::Uuid::new_v4().to_string());
        let mut data: Vec<u8> = Vec::new();
        while let Some(chunk) = request.body.next().await {
            let chunk = chunk?;
            data.extend_from_slice(&chunk);
        }
        if data.is_empty() {
            return Err(BlobIoError::Invalid("empty blob payload".to_string()));
        }
        let now = chrono::Utc::now();
        let meta = BlobMeta {
            id: blob_id,
            file_name: request.file_name.take(),
            mime_type: request.mime_type.take(),
            byte_size: data.len() as u64,
            checksum_sha256: None,
            metadata: if request.metadata.is_null() {
                serde_json::json!({})
            } else {
                request.metadata
            },
            created_at: now,
            updated_at: now,
        };
        self.objects.write().await.insert(
            meta.id.to_string(),
            InMemoryBlobObject {
                meta: meta.clone(),
                bytes: data,
            },
        );
        Ok(meta)
    }

    async fn read(&self, blob_id: &BlobId) -> Result<BlobRead, BlobIoError> {
        let obj = self
            .objects
            .read()
            .await
            .get(blob_id.as_str())
            .cloned()
            .ok_or_else(|| BlobIoError::NotFound(blob_id.to_string()))?;
        let body = Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(obj.bytes))
        }));
        Ok(BlobRead {
            meta: obj.meta,
            body,
        })
    }

    async fn head(&self, blob_id: &BlobId) -> Result<BlobHead, BlobIoError> {
        let obj = self
            .objects
            .read()
            .await
            .get(blob_id.as_str())
            .cloned()
            .ok_or_else(|| BlobIoError::NotFound(blob_id.to_string()))?;
        Ok(BlobHead {
            byte_size: obj.meta.byte_size,
            etag: None,
            last_modified: Some(obj.meta.updated_at),
        })
    }

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError> {
        Ok(self
            .objects
            .write()
            .await
            .remove(blob_id.as_str())
            .is_some())
    }
}

fn init_tracing_if_needed(observability: &ObservabilityConfig) {
    TRACING_INIT.get_or_init(|| {
        let silent_tui_logs = std::env::var("ORCHESTRAL_TUI_SILENT_LOGS")
            .map(|v| v == "1")
            .unwrap_or(false);
        let log_file_path = std::env::var("ORCHESTRAL_LOG_FILE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .or_else(|| observability.log_file.clone());
        let file_writer = log_file_path.as_deref().and_then(create_log_writer);
        let fallback_level = match observability.log_level.trim().to_ascii_lowercase().as_str() {
            "trace" => "trace",
            "debug" => "debug",
            "info" => "info",
            "warn" => "warn",
            "error" => "error",
            _ => "info",
        };

        let make_filter = || {
            tracing_subscriber::EnvFilter::try_from_default_env()
                .or_else(|_| tracing_subscriber::EnvFilter::try_new(fallback_level))
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        };

        match (observability.traces_enabled, silent_tui_logs, file_writer) {
            (true, _, Some(writer)) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_ansi(false)
                    .with_writer(writer)
                    .with_span_events(
                        tracing_subscriber::fmt::format::FmtSpan::NEW
                            | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                    )
                    .try_init();
            }
            (true, true, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_writer(std::io::sink)
                    .with_span_events(
                        tracing_subscriber::fmt::format::FmtSpan::NEW
                            | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                    )
                    .try_init();
            }
            (true, false, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_span_events(
                        tracing_subscriber::fmt::format::FmtSpan::NEW
                            | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
                    )
                    .try_init();
            }
            (false, _, Some(writer)) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_ansi(false)
                    .with_writer(writer)
                    .try_init();
            }
            (false, true, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .with_writer(std::io::sink)
                    .try_init();
            }
            (false, false, None) => {
                let _ = tracing_subscriber::fmt()
                    .with_env_filter(make_filter())
                    .with_target(true)
                    .try_init();
            }
        }

        tracing::info!(
            log_level = %observability.log_level,
            traces_enabled = observability.traces_enabled,
            log_file = log_file_path.as_deref().unwrap_or("(stdout)"),
            "tracing initialized"
        );
    });
}

fn create_log_writer(path: &str) -> Option<SharedFileMakeWriter> {
    use std::fs::{create_dir_all, OpenOptions};
    use std::path::Path;

    let file_path = Path::new(path);
    if let Some(parent) = file_path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(err) = create_dir_all(parent) {
                eprintln!(
                    "failed to create log directory '{}': {}",
                    parent.display(),
                    err
                );
                return None;
            }
        }
    }
    let file = match OpenOptions::new().create(true).append(true).open(file_path) {
        Ok(f) => f,
        Err(err) => {
            eprintln!("failed to open log file '{}': {}", file_path.display(), err);
            return None;
        }
    };
    Some(SharedFileMakeWriter::new(file))
}

#[derive(Clone)]
struct SharedFileMakeWriter {
    file: Arc<std::sync::Mutex<std::fs::File>>,
}

impl SharedFileMakeWriter {
    fn new(file: std::fs::File) -> Self {
        Self {
            file: Arc::new(std::sync::Mutex::new(file)),
        }
    }
}

struct SharedFileWriter {
    file: Arc<std::sync::Mutex<std::fs::File>>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedFileMakeWriter {
    type Writer = SharedFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SharedFileWriter {
            file: self.file.clone(),
        }
    }
}

impl std::io::Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("log file mutex poisoned"))?;
        std::io::Write::write(&mut *file, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("log file mutex poisoned"))?;
        std::io::Write::flush(&mut *file)
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

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
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
            let prompt_from_profile = config
                .planner
                .model_profile
                .as_ref()
                .and_then(|name| config.providers.get_model(name))
                .and_then(|p| p.system_prompt.clone());
            let prompt_from_backend = backend.get_config::<String>("system_prompt");
            let (system_prompt, prompt_source) = if let Some(prompt) = prompt_from_profile {
                (prompt, "model_profile")
            } else if let Some(prompt) = prompt_from_backend {
                (prompt, "backend")
            } else {
                (default_cfg.system_prompt, "default")
            };

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
            };

            let planner: LlmPlanner<Arc<dyn LlmClient>> = LlmPlanner::new(client, planner_cfg);
            Ok(Arc::new(planner))
        }
        "deterministic" => Ok(Arc::new(DeterministicPlanner)),
        other => Err(BootstrapError::UnsupportedPlannerMode(other.to_string())),
    }
}

fn build_result_interpreter(
    config: &OrchestralConfig,
) -> Result<Arc<dyn ResultInterpreter>, BootstrapError> {
    let mode = config.interpreter.mode.trim().to_ascii_lowercase();
    let use_llm = match mode.as_str() {
        "auto" | "" => config.planner.mode.trim().eq_ignore_ascii_case("llm"),
        "llm" => true,
        "noop" | "rule" | "rules" => false,
        other => {
            return Err(BootstrapError::UnsupportedInterpreterMode(
                other.to_string(),
            ))
        }
    };
    if !use_llm {
        return Ok(Arc::new(NoopResultInterpreter));
    }

    let backend = resolve_interpreter_backend(config)?;
    let profile = resolve_interpreter_model_profile(config);
    let default_cfg = LlmResultInterpreterConfig::default();
    let model = config
        .interpreter
        .model
        .clone()
        .or_else(|| profile.as_ref().map(|p| p.model.clone()))
        .or_else(|| config.planner.model.clone())
        .unwrap_or(default_cfg.model.clone());
    let temperature_candidate = config
        .interpreter
        .temperature
        .or_else(|| profile.as_ref().and_then(|p| p.temperature))
        .or(config.planner.temperature)
        .unwrap_or(default_cfg.temperature);
    let temperature = profile
        .as_ref()
        .map(|p| p.clamp_temperature(temperature_candidate))
        .unwrap_or(temperature_candidate);
    let system_prompt = config
        .interpreter
        .system_prompt
        .clone()
        .or_else(|| profile.as_ref().and_then(|p| p.system_prompt.clone()))
        .or_else(|| backend.get_config::<String>("interpreter_system_prompt"))
        .unwrap_or(default_cfg.system_prompt);

    let invocation = LlmInvocationConfig {
        model: model.clone(),
        temperature,
        normalize_response: true,
    };
    let client = DefaultLlmClientFactory::new().build(&backend, &invocation)?;
    let cfg = LlmResultInterpreterConfig {
        model,
        temperature,
        system_prompt,
        timeout_secs: backend
            .get_config::<u64>("interpreter_timeout_secs")
            .unwrap_or(12),
    };
    tracing::info!(
        mode = %mode,
        backend_name = %backend.name,
        backend_kind = %backend.kind,
        model = %cfg.model,
        temperature = cfg.temperature,
        timeout_secs = cfg.timeout_secs,
        "result interpreter configured"
    );
    Ok(Arc::new(LlmResultInterpreter::new(client, cfg)))
}

fn resolve_interpreter_backend(
    config: &OrchestralConfig,
) -> Result<orchestral_config::BackendSpec, BootstrapError> {
    if let Some(name) = &config.interpreter.backend {
        return config
            .providers
            .get_backend(name)
            .ok_or_else(|| BootstrapError::BackendNotFound(name.clone()));
    }
    if let Some(profile_name) = &config.interpreter.model_profile {
        let profile = config
            .providers
            .get_model(profile_name)
            .ok_or_else(|| BootstrapError::ModelProfileNotFound(profile_name.clone()))?;
        let backend_name = profile
            .backend
            .clone()
            .ok_or(BootstrapError::MissingProviderConfig)?;
        return config
            .providers
            .get_backend(&backend_name)
            .ok_or(BootstrapError::BackendNotFound(backend_name));
    }
    if let Some(name) = &config.planner.backend {
        return config
            .providers
            .get_backend(name)
            .ok_or_else(|| BootstrapError::BackendNotFound(name.clone()));
    }
    if let Some(profile_name) = &config.planner.model_profile {
        let profile = config
            .providers
            .get_model(profile_name)
            .ok_or_else(|| BootstrapError::ModelProfileNotFound(profile_name.clone()))?;
        let backend_name = profile
            .backend
            .clone()
            .ok_or(BootstrapError::MissingProviderConfig)?;
        return config
            .providers
            .get_backend(&backend_name)
            .ok_or(BootstrapError::BackendNotFound(backend_name));
    }
    config
        .providers
        .get_default_backend()
        .ok_or(BootstrapError::MissingProviderConfig)
}

fn resolve_interpreter_model_profile(
    config: &OrchestralConfig,
) -> Option<orchestral_config::ModelProfile> {
    if let Some(name) = &config.interpreter.model_profile {
        return config.providers.get_model(name);
    }
    if let Some(name) = &config.planner.model_profile {
        return config.providers.get_model(name);
    }
    config.providers.get_default_model()
}

struct DeterministicPlanner;

#[async_trait]
impl Planner for DeterministicPlanner {
    async fn plan(
        &self,
        intent: &Intent,
        context: &PlannerContext,
    ) -> Result<PlannerOutput, PlanError> {
        let action_name = context
            .available_actions
            .iter()
            .find(|a| a.name == "echo")
            .map(|a| a.name.clone())
            .or_else(|| context.available_actions.first().map(|a| a.name.clone()))
            .ok_or(PlanError::NoSuitableActions)?;

        Ok(PlannerOutput::Workflow(Plan::new(
            format!("Deterministic plan for intent: {}", intent.content),
            vec![Step::action("s1", action_name).with_params(json!({
                "message": intent.content
            }))],
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_default_store_factory_accepts_in_memory_spec() {
        let factory = DefaultStoreBackendFactory;
        let spec = StoreSpec {
            backend: "in_memory".to_string(),
            connection_url: None,
            key_prefix: None,
        };

        assert!(factory.build_event_store(&spec).await.is_ok());
        assert!(factory.build_task_store(&spec).await.is_ok());
        assert!(factory.build_reference_store(&spec).await.is_ok());
    }

    #[tokio::test]
    async fn test_default_store_factory_rejects_redis_spec() {
        let factory = DefaultStoreBackendFactory;
        let spec = StoreSpec {
            backend: "redis".to_string(),
            connection_url: Some("redis://127.0.0.1/".to_string()),
            key_prefix: Some("orchestral:test".to_string()),
        };

        assert!(matches!(
            factory.build_event_store(&spec).await,
            Err(BootstrapError::UnsupportedStoreBackend { .. })
        ));
        assert!(matches!(
            factory.build_task_store(&spec).await,
            Err(BootstrapError::UnsupportedStoreBackend { .. })
        ));
        assert!(matches!(
            factory.build_reference_store(&spec).await,
            Err(BootstrapError::UnsupportedStoreBackend { .. })
        ));
    }

    #[tokio::test]
    async fn test_default_blob_store_factory_writes_and_reads() {
        let factory = DefaultBlobStoreFactory;
        let config = OrchestralConfig::default();
        let store = factory.build_blob_store(&config).await.expect("blob store");
        let payload = vec![1_u8, 2, 3];
        let request = BlobWriteRequest::new(Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(payload))
        })));
        let written = store.write(request).await.expect("write");
        assert_eq!(written.byte_size, 3);
        let read = store.read(&written.id).await.expect("read");
        assert_eq!(read.meta.id, written.id);
    }
}
