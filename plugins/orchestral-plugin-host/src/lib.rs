use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_api::{ApiError, RuntimeAppBuilder};
use orchestral_config::{load_config, OrchestralConfig, RuntimePluginSpec, StoreSpec};
use orchestral_core::io::BlobStore;
use orchestral_files::{BlobMode, BlobServiceConfig, FileService, LocalBlobStoreConfig};
use orchestral_runtime::RuntimeApp;
use orchestral_spi::{
    ComponentRegistry, HookRegistry, RuntimeBuildRequest, RuntimeComponentFactory, RuntimeHook,
    SpiError, SpiMeta, StoreBundle,
};
use orchestral_stores::{InMemoryEventStore, InMemoryReferenceStore, InMemoryTaskStore};
use orchestral_stores_backends::{
    PostgresEventStore, PostgresReferenceStore, PostgresTaskStore, RedisEventStore,
    RedisReferenceStore, RedisTaskStore,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RuntimeTarget {
    Cli,
    Server,
}

impl RuntimeTarget {
    fn as_config_value(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Server => "server",
        }
    }
}

#[async_trait]
pub trait RuntimePlugin: Send + Sync {
    fn name(&self) -> &'static str;

    async fn build_components(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        _spec: &RuntimePluginSpec,
    ) -> Result<ComponentRegistry, SpiError> {
        Ok(ComponentRegistry::new())
    }

    async fn build_hooks(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        _spec: &RuntimePluginSpec,
    ) -> Result<Vec<Arc<dyn RuntimeHook>>, SpiError> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct LoadedPlugin {
    spec: RuntimePluginSpec,
    plugin: Arc<dyn RuntimePlugin>,
}

struct CompositeRuntimeComponentFactory {
    config: Arc<OrchestralConfig>,
    plugins: Vec<LoadedPlugin>,
}

#[async_trait]
impl RuntimeComponentFactory for CompositeRuntimeComponentFactory {
    async fn build(&self, request: &RuntimeBuildRequest) -> Result<ComponentRegistry, SpiError> {
        let mut merged = ComponentRegistry::new();
        for loaded in &self.plugins {
            let contribution = loaded
                .plugin
                .build_components(request, &self.config, &loaded.spec)
                .await?;
            merge_component_registry(&mut merged, contribution, loaded.plugin.name())?;
        }
        Ok(merged)
    }
}

pub struct PluginRuntimeAppBuilder {
    target: RuntimeTarget,
}

impl PluginRuntimeAppBuilder {
    pub fn new(target: RuntimeTarget) -> Self {
        Self { target }
    }
}

#[async_trait]
impl RuntimeAppBuilder for PluginRuntimeAppBuilder {
    async fn build(&self, config_path: PathBuf) -> Result<RuntimeApp, ApiError> {
        let config = load_config(&config_path).map_err(|err| {
            ApiError::Internal(format!("load config for plugin host failed: {}", err))
        })?;
        let config = Arc::new(config);
        let plugins = resolve_loaded_plugins(&config, self.target)?;
        let hook_registry =
            build_hook_registry(&config_path, self.target, &config, &plugins).await?;
        let component_factory = Arc::new(CompositeRuntimeComponentFactory { config, plugins });
        RuntimeApp::from_config_path_with_spi(config_path, component_factory, hook_registry)
            .await
            .map_err(|err| ApiError::Internal(format!("build runtime app failed: {}", err)))
    }
}

fn resolve_loaded_plugins(
    config: &OrchestralConfig,
    target: RuntimeTarget,
) -> Result<Vec<LoadedPlugin>, ApiError> {
    resolve_plugin_specs(config, target)
        .into_iter()
        .map(|spec| {
            let plugin = resolve_plugin(&spec.name).ok_or_else(|| {
                ApiError::InvalidArgument(format!(
                    "runtime plugin '{}' is not registered",
                    spec.name
                ))
            })?;
            Ok(LoadedPlugin { spec, plugin })
        })
        .collect()
}

fn resolve_plugin_specs(
    config: &OrchestralConfig,
    target: RuntimeTarget,
) -> Vec<RuntimePluginSpec> {
    let mut specs: Vec<RuntimePluginSpec> = config
        .plugins
        .runtime
        .iter()
        .filter(|spec| spec.enabled && plugin_target_matches(spec, target))
        .cloned()
        .collect();
    if config.plugins.runtime.is_empty() {
        specs.push(RuntimePluginSpec {
            name: "builtin.storage_bundle".to_string(),
            enabled: true,
            targets: Vec::new(),
            options: serde_json::Value::Null,
        });
    }
    specs
}

fn plugin_target_matches(spec: &RuntimePluginSpec, target: RuntimeTarget) -> bool {
    if spec.targets.is_empty() {
        return true;
    }
    let target_value = target.as_config_value();
    spec.targets.iter().any(|candidate| {
        let normalized = candidate.trim().to_ascii_lowercase();
        normalized == target_value
            || normalized == "all"
            || normalized == "*"
            || normalized == "both"
    })
}

fn resolve_plugin(name: &str) -> Option<Arc<dyn RuntimePlugin>> {
    let requested = normalize_plugin_name(name);
    match requested.as_str() {
        "builtin.storage_bundle" | "storage_bundle" => Some(Arc::new(StorageBundlePlugin)),
        _ => None,
    }
}

fn normalize_plugin_name(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('-', "_")
}

fn merge_component_registry(
    target: &mut ComponentRegistry,
    mut source: ComponentRegistry,
    plugin_name: &str,
) -> Result<(), SpiError> {
    if let Some(stores) = source.stores.take() {
        if target.stores.is_some() {
            return Err(SpiError::Internal(format!(
                "plugin '{}' tried to override stores component",
                plugin_name
            )));
        }
        target.stores = Some(stores);
    }

    if let Some(blob_store) = source.blob_store.take() {
        if target.blob_store.is_some() {
            return Err(SpiError::Internal(format!(
                "plugin '{}' tried to override blob_store component",
                plugin_name
            )));
        }
        target.blob_store = Some(blob_store);
    }

    for (key, component) in source.into_named_components() {
        if target.get_named_component(&key).is_some() {
            return Err(SpiError::Internal(format!(
                "plugin '{}' tried to override named component '{}'",
                plugin_name, key
            )));
        }
        target.insert_named_component(key, component);
    }

    Ok(())
}

async fn build_hook_registry(
    config_path: &Path,
    target: RuntimeTarget,
    config: &OrchestralConfig,
    plugins: &[LoadedPlugin],
) -> Result<Arc<HookRegistry>, ApiError> {
    let request = RuntimeBuildRequest {
        meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
        config_path: config_path.to_string_lossy().to_string(),
        profile: Some(target.as_config_value().to_string()),
        options: serde_json::Map::new(),
    };

    let registry = Arc::new(HookRegistry::new());
    for loaded in plugins {
        let hooks = loaded
            .plugin
            .build_hooks(&request, config, &loaded.spec)
            .await
            .map_err(|err| {
                ApiError::Internal(format!(
                    "build hooks from plugin '{}' failed: {}",
                    loaded.spec.name, err
                ))
            })?;
        registry.register_many(hooks).await;
    }
    Ok(registry)
}

struct StorageBundlePlugin;

#[async_trait]
impl RuntimePlugin for StorageBundlePlugin {
    fn name(&self) -> &'static str {
        "builtin.storage_bundle"
    }

    async fn build_components(
        &self,
        _request: &RuntimeBuildRequest,
        config: &OrchestralConfig,
        _spec: &RuntimePluginSpec,
    ) -> Result<ComponentRegistry, SpiError> {
        let event_store = build_event_store(&config.stores.event).await?;
        let task_store = build_task_store(&config.stores.task).await?;
        let reference_store = build_reference_store(&config.stores.reference).await?;
        let blob_store = build_blob_store(config).await?;

        Ok(ComponentRegistry::new()
            .with_stores(StoreBundle {
                event_store,
                task_store,
                reference_store,
            })
            .with_blob_store(blob_store))
    }
}

async fn build_event_store(
    spec: &StoreSpec,
) -> Result<Arc<dyn orchestral_core::store::EventStore>, SpiError> {
    match spec.backend.trim().to_ascii_lowercase().as_str() {
        "in_memory" | "memory" => Ok(Arc::new(InMemoryEventStore::new())),
        "redis" => {
            let url = require_setting(
                "event_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral:event".to_string());
            let store = RedisEventStore::new(url, prefix)
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "postgres" | "postgresql" | "pgsql" => {
            let url = require_setting(
                "event_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = PostgresEventStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        backend => Err(SpiError::UnsupportedBackend {
            component: "event_store".to_string(),
            backend: backend.to_string(),
        }),
    }
}

async fn build_task_store(
    spec: &StoreSpec,
) -> Result<Arc<dyn orchestral_core::store::TaskStore>, SpiError> {
    match spec.backend.trim().to_ascii_lowercase().as_str() {
        "in_memory" | "memory" => Ok(Arc::new(InMemoryTaskStore::new())),
        "redis" => {
            let url =
                require_setting("task_store", "connection_url", spec.connection_url.as_ref())?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral:task".to_string());
            let store = RedisTaskStore::new(url, prefix)
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "postgres" | "postgresql" | "pgsql" => {
            let url =
                require_setting("task_store", "connection_url", spec.connection_url.as_ref())?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = PostgresTaskStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        backend => Err(SpiError::UnsupportedBackend {
            component: "task_store".to_string(),
            backend: backend.to_string(),
        }),
    }
}

async fn build_reference_store(
    spec: &StoreSpec,
) -> Result<Arc<dyn orchestral_core::store::ReferenceStore>, SpiError> {
    match spec.backend.trim().to_ascii_lowercase().as_str() {
        "in_memory" | "memory" => Ok(Arc::new(InMemoryReferenceStore::new())),
        "redis" => {
            let url = require_setting(
                "reference_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral:reference".to_string());
            let store = RedisReferenceStore::new(url, prefix)
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        "postgres" | "postgresql" | "pgsql" => {
            let url = require_setting(
                "reference_store",
                "connection_url",
                spec.connection_url.as_ref(),
            )?;
            let prefix = spec
                .key_prefix
                .clone()
                .unwrap_or_else(|| "orchestral".to_string());
            let store = PostgresReferenceStore::new(url, prefix)
                .await
                .map_err(|err| SpiError::Internal(err.to_string()))?;
            Ok(Arc::new(store))
        }
        backend => Err(SpiError::UnsupportedBackend {
            component: "reference_store".to_string(),
            backend: backend.to_string(),
        }),
    }
}

async fn build_blob_store(config: &OrchestralConfig) -> Result<Arc<dyn BlobStore>, SpiError> {
    let mode = match config.blobs.mode.trim().to_ascii_lowercase().as_str() {
        "local" => BlobMode::Local,
        "s3" => BlobMode::S3,
        "hybrid" => BlobMode::Hybrid,
        backend => {
            return Err(SpiError::UnsupportedBackend {
                component: "blob_store".to_string(),
                backend: backend.to_string(),
            });
        }
    };

    let service_config = BlobServiceConfig {
        mode: mode.clone(),
        hybrid_write_to: if config.blobs.hybrid.write_to.trim().is_empty() {
            None
        } else {
            Some(config.blobs.hybrid.write_to.clone())
        },
    };

    let mut service = match config
        .blobs
        .catalog
        .backend
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "in_memory" | "memory" => FileService::with_in_memory_catalog(service_config),
        "postgres" | "postgresql" | "pgsql" => {
            let url = require_setting(
                "blob_catalog",
                "connection_url",
                config.blobs.catalog.connection_url.as_ref(),
            )?;
            FileService::with_postgres_catalog(
                service_config,
                url,
                config.blobs.catalog.table_prefix.clone(),
            )
            .await
            .map_err(|err| SpiError::Io(err.to_string()))?
        }
        backend => {
            return Err(SpiError::UnsupportedBackend {
                component: "blob_catalog".to_string(),
                backend: backend.to_string(),
            });
        }
    };

    service = service.with_local_defaults(LocalBlobStoreConfig {
        root_dir: config.blobs.local.root_dir.clone(),
    });

    if matches!(mode, BlobMode::S3)
        || (matches!(mode, BlobMode::Hybrid)
            && config
                .blobs
                .hybrid
                .write_to
                .trim()
                .eq_ignore_ascii_case("s3"))
    {
        return Err(SpiError::UnsupportedBackend {
            component: "blob_store".to_string(),
            backend: "s3_requires_custom_client".to_string(),
        });
    }

    Ok(Arc::new(service))
}

fn require_setting<'a>(
    component: &str,
    setting: &str,
    value: Option<&'a String>,
) -> Result<&'a str, SpiError> {
    value
        .map(String::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| SpiError::MissingSetting {
            component: component.to_string(),
            setting: setting.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_plugin_specs_defaults_to_builtin_storage_bundle() {
        let config = OrchestralConfig::default();
        let specs = resolve_plugin_specs(&config, RuntimeTarget::Cli);
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "builtin.storage_bundle");
    }

    #[test]
    fn test_resolve_plugin_specs_respects_targets_and_enabled() {
        let mut config = OrchestralConfig::default();
        config.plugins.runtime = vec![
            RuntimePluginSpec {
                name: "storage_bundle".to_string(),
                enabled: true,
                targets: vec!["server".to_string()],
                options: serde_json::Value::Null,
            },
            RuntimePluginSpec {
                name: "storage_bundle".to_string(),
                enabled: false,
                targets: vec![],
                options: serde_json::Value::Null,
            },
        ];

        let cli_specs = resolve_plugin_specs(&config, RuntimeTarget::Cli);
        assert!(cli_specs.is_empty());

        let server_specs = resolve_plugin_specs(&config, RuntimeTarget::Server);
        assert_eq!(server_specs.len(), 1);
    }

    #[test]
    fn test_builtin_plugin_descriptor_is_discoverable() {
        assert!(resolve_plugin("builtin.storage_bundle").is_some());
        assert!(resolve_plugin("storage_bundle").is_some());
    }
}
