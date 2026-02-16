use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use orchestral_actions::{ActionBuildError, ActionFactory, ActionSpec, DefaultActionFactory};
use orchestral_config::{ConfigError, OrchestralConfig, RuntimeExtensionSpec};
use orchestral_docs_assistant::{DocsAssistantExtension, EXTENSION_NAME as DOCS_EXTENSION_NAME};
use orchestral_spi::{
    ComponentRegistry, HookRegistry, RuntimeBuildRequest, RuntimeComponentFactory, RuntimeHook,
    SpiError, SpiMeta,
};

#[derive(Debug, thiserror::Error)]
pub enum ExtensionHostError {
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("runtime extension '{0}' is not registered")]
    ExtensionNotRegistered(String),
    #[error("runtime extension error: {0}")]
    Spi(#[from] SpiError),
}

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
pub trait RuntimeExtension: Send + Sync {
    fn name(&self) -> &'static str;

    async fn build_components(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        _spec: &RuntimeExtensionSpec,
    ) -> Result<ComponentRegistry, SpiError> {
        Ok(ComponentRegistry::new())
    }

    async fn build_hooks(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        _spec: &RuntimeExtensionSpec,
    ) -> Result<Vec<Arc<dyn RuntimeHook>>, SpiError> {
        Ok(Vec::new())
    }

    async fn build_action_factories(
        &self,
        _request: &RuntimeBuildRequest,
        _config: &OrchestralConfig,
        _spec: &RuntimeExtensionSpec,
    ) -> Result<Vec<Arc<dyn ActionFactory>>, SpiError> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct LoadedExtension {
    spec: RuntimeExtensionSpec,
    extension: Arc<dyn RuntimeExtension>,
}

struct ExtensionComponentFactory {
    config: Arc<OrchestralConfig>,
    extensions: Vec<LoadedExtension>,
}

#[async_trait]
impl RuntimeComponentFactory for ExtensionComponentFactory {
    async fn build(&self, request: &RuntimeBuildRequest) -> Result<ComponentRegistry, SpiError> {
        let mut merged = ComponentRegistry::new();
        for loaded in &self.extensions {
            let contribution = loaded
                .extension
                .build_components(request, &self.config, &loaded.spec)
                .await?;
            merge_extension_components(&mut merged, contribution, loaded.extension.name())?;
        }
        Ok(merged)
    }
}

#[derive(Clone, Default)]
pub struct RuntimeExtensionCatalog {
    extensions: HashMap<String, Arc<dyn RuntimeExtension>>,
}

impl RuntimeExtensionCatalog {
    pub fn with_builtin_extensions() -> Self {
        let mut catalog = Self::default();
        catalog.register(
            Arc::new(DocsAssistantRuntimeExtension::new()),
            &["builtin.docs_assistant", "docs_assistant"],
        );
        catalog
    }

    pub fn register(&mut self, extension: Arc<dyn RuntimeExtension>, aliases: &[&str]) {
        self.extensions.insert(
            normalize_extension_name(extension.name()),
            extension.clone(),
        );
        for alias in aliases {
            self.extensions
                .insert(normalize_extension_name(alias), extension.clone());
        }
    }

    pub fn with_extension(
        mut self,
        extension: Arc<dyn RuntimeExtension>,
        aliases: &[&str],
    ) -> Self {
        self.register(extension, aliases);
        self
    }

    pub fn resolve(&self, name: &str) -> Option<Arc<dyn RuntimeExtension>> {
        self.extensions
            .get(&normalize_extension_name(name))
            .cloned()
    }
}

fn normalize_extension_name(name: &str) -> String {
    name.trim().to_ascii_lowercase().replace('-', "_")
}

fn merge_extension_components(
    target: &mut ComponentRegistry,
    mut source: ComponentRegistry,
    extension_name: &str,
) -> Result<(), SpiError> {
    if source.stores.take().is_some() || source.blob_store.take().is_some() {
        return Err(SpiError::Internal(format!(
            "runtime extension '{}' cannot override infrastructure components (stores/blob_store)",
            extension_name
        )));
    }

    for (key, component) in source.into_named_components() {
        if target.get_named_component(&key).is_some() {
            return Err(SpiError::Internal(format!(
                "runtime extension '{}' tried to override named component '{}'",
                extension_name, key
            )));
        }
        target.insert_named_component(key, component);
    }

    Ok(())
}

fn extension_target_matches(spec: &RuntimeExtensionSpec, target: RuntimeTarget) -> bool {
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

fn resolve_extension_specs(
    config: &OrchestralConfig,
    target: RuntimeTarget,
) -> Vec<RuntimeExtensionSpec> {
    config
        .extensions
        .runtime
        .iter()
        .filter(|spec| spec.enabled && extension_target_matches(spec, target))
        .cloned()
        .collect()
}

fn resolve_loaded_extensions(
    config: &OrchestralConfig,
    target: RuntimeTarget,
    catalog: &RuntimeExtensionCatalog,
) -> Result<Vec<LoadedExtension>, ExtensionHostError> {
    resolve_extension_specs(config, target)
        .into_iter()
        .map(|spec| {
            let extension = catalog
                .resolve(&spec.name)
                .ok_or_else(|| ExtensionHostError::ExtensionNotRegistered(spec.name.clone()))?;
            Ok(LoadedExtension { spec, extension })
        })
        .collect()
}

async fn build_hook_registry(
    config_path: &Path,
    target: RuntimeTarget,
    config: &OrchestralConfig,
    extensions: &[LoadedExtension],
) -> Result<Arc<HookRegistry>, ExtensionHostError> {
    let request = RuntimeBuildRequest {
        meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
        config_path: config_path.to_string_lossy().to_string(),
        profile: Some(target.as_config_value().to_string()),
        options: serde_json::Map::new(),
    };

    let registry = Arc::new(HookRegistry::new());
    for loaded in extensions {
        let hooks = loaded
            .extension
            .build_hooks(&request, config, &loaded.spec)
            .await?;
        registry.register_many(hooks).await;
    }
    Ok(registry)
}

async fn build_action_factory(
    config_path: &Path,
    target: RuntimeTarget,
    config: &OrchestralConfig,
    extensions: &[LoadedExtension],
) -> Result<Arc<dyn ActionFactory>, ExtensionHostError> {
    let request = RuntimeBuildRequest {
        meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
        config_path: config_path.to_string_lossy().to_string(),
        profile: Some(target.as_config_value().to_string()),
        options: serde_json::Map::new(),
    };

    let mut factories: Vec<Arc<dyn ActionFactory>> = Vec::new();
    for loaded in extensions {
        let mut extension_factories = loaded
            .extension
            .build_action_factories(&request, config, &loaded.spec)
            .await?;
        factories.append(&mut extension_factories);
    }
    factories.push(Arc::new(DefaultActionFactory::new()));

    Ok(Arc::new(CompositeActionFactory { factories }))
}

pub struct ExtensionRuntimeBundle {
    pub component_factory: Arc<dyn RuntimeComponentFactory>,
    pub hook_registry: Arc<HookRegistry>,
    pub action_factory: Arc<dyn ActionFactory>,
    pub loaded_extensions: Vec<String>,
}

pub async fn build_extension_runtime_bundle(
    config: Arc<OrchestralConfig>,
    config_path: &Path,
    target: RuntimeTarget,
    catalog: &RuntimeExtensionCatalog,
) -> Result<ExtensionRuntimeBundle, ExtensionHostError> {
    let extensions = resolve_loaded_extensions(&config, target, catalog)?;
    let hook_registry = build_hook_registry(config_path, target, &config, &extensions).await?;
    let action_factory = build_action_factory(config_path, target, &config, &extensions).await?;
    let loaded_extensions = extensions
        .iter()
        .map(|loaded| loaded.spec.name.clone())
        .collect();
    Ok(ExtensionRuntimeBundle {
        component_factory: Arc::new(ExtensionComponentFactory { config, extensions }),
        hook_registry,
        action_factory,
        loaded_extensions,
    })
}

struct CompositeActionFactory {
    factories: Vec<Arc<dyn ActionFactory>>,
}

impl ActionFactory for CompositeActionFactory {
    fn build(
        &self,
        spec: &ActionSpec,
    ) -> Result<Arc<dyn orchestral_actions::Action>, ActionBuildError> {
        for factory in &self.factories {
            match factory.build(spec) {
                Ok(action) => return Ok(action),
                Err(ActionBuildError::UnknownKind(_)) => continue,
                Err(err) => return Err(err),
            }
        }
        Err(ActionBuildError::UnknownKind(spec.kind.clone()))
    }
}

struct DocsAssistantRuntimeExtension {
    inner: DocsAssistantExtension,
}

impl DocsAssistantRuntimeExtension {
    fn new() -> Self {
        Self {
            inner: DocsAssistantExtension::new(),
        }
    }
}

#[async_trait]
impl RuntimeExtension for DocsAssistantRuntimeExtension {
    fn name(&self) -> &'static str {
        DOCS_EXTENSION_NAME
    }

    async fn build_components(
        &self,
        request: &RuntimeBuildRequest,
        config: &OrchestralConfig,
        spec: &RuntimeExtensionSpec,
    ) -> Result<ComponentRegistry, SpiError> {
        self.inner.build_components(request, config, spec).await
    }

    async fn build_hooks(
        &self,
        request: &RuntimeBuildRequest,
        config: &OrchestralConfig,
        spec: &RuntimeExtensionSpec,
    ) -> Result<Vec<Arc<dyn RuntimeHook>>, SpiError> {
        self.inner.build_hooks(request, config, spec).await
    }

    async fn build_action_factories(
        &self,
        request: &RuntimeBuildRequest,
        config: &OrchestralConfig,
        spec: &RuntimeExtensionSpec,
    ) -> Result<Vec<Arc<dyn ActionFactory>>, SpiError> {
        self.inner
            .build_action_factories(request, config, spec)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    struct DummyExtension;

    #[async_trait]
    impl RuntimeExtension for DummyExtension {
        fn name(&self) -> &'static str {
            "custom.dummy"
        }
    }

    #[test]
    fn test_resolve_extension_specs_filters_targets() {
        let mut config = OrchestralConfig::default();
        config.extensions.runtime = vec![
            RuntimeExtensionSpec {
                name: "docs_assistant".to_string(),
                enabled: true,
                targets: vec!["server".to_string()],
                options: serde_json::Value::Null,
            },
            RuntimeExtensionSpec {
                name: "disabled".to_string(),
                enabled: false,
                targets: vec![],
                options: serde_json::Value::Null,
            },
        ];

        let cli_specs = resolve_extension_specs(&config, RuntimeTarget::Cli);
        assert!(cli_specs.is_empty());

        let server_specs = resolve_extension_specs(&config, RuntimeTarget::Server);
        assert_eq!(server_specs.len(), 1);
        assert_eq!(server_specs[0].name, "docs_assistant");
    }

    #[test]
    fn test_catalog_can_register_custom_extension() {
        let catalog = RuntimeExtensionCatalog::with_builtin_extensions()
            .with_extension(Arc::new(DummyExtension), &["custom_dummy"]);
        assert!(catalog.resolve("custom.dummy").is_some());
        assert!(catalog.resolve("custom_dummy").is_some());
    }

    #[test]
    fn test_resolve_loaded_extensions_fails_for_unregistered() {
        let mut config = OrchestralConfig::default();
        config.extensions.runtime = vec![RuntimeExtensionSpec {
            name: "custom_missing".to_string(),
            enabled: true,
            targets: Vec::new(),
            options: serde_json::Value::Null,
        }];
        let catalog = RuntimeExtensionCatalog::with_builtin_extensions();
        let result = resolve_loaded_extensions(&config, RuntimeTarget::Cli, &catalog);
        match result {
            Err(ExtensionHostError::ExtensionNotRegistered(name)) => {
                assert_eq!(name, "custom_missing");
            }
            Err(other) => panic!("unexpected error: {}", other),
            Ok(_) => panic!("expected ExtensionNotRegistered error"),
        }
    }

    #[tokio::test]
    async fn test_bundle_action_factory_combines_extension_and_default() {
        let mut config = OrchestralConfig::default();
        config.extensions.runtime = vec![RuntimeExtensionSpec {
            name: "builtin.docs_assistant".to_string(),
            enabled: true,
            targets: vec!["cli".to_string()],
            options: serde_json::Value::Null,
        }];

        let bundle = build_extension_runtime_bundle(
            Arc::new(config),
            Path::new("configs/orchestral.cli.yaml"),
            RuntimeTarget::Cli,
            &RuntimeExtensionCatalog::with_builtin_extensions(),
        )
        .await
        .expect("build extension runtime bundle");

        let doc_spec = ActionSpec {
            name: "doc_parse".to_string(),
            kind: "doc_parse".to_string(),
            description: None,
            config: serde_json::Value::Null,
            interface: None,
        };
        assert!(bundle.action_factory.build(&doc_spec).is_ok());

        let echo_spec = ActionSpec {
            name: "echo".to_string(),
            kind: "echo".to_string(),
            description: None,
            config: serde_json::Value::Null,
            interface: None,
        };
        assert!(bundle.action_factory.build(&echo_spec).is_ok());

        let unknown_spec = ActionSpec {
            name: "unknown".to_string(),
            kind: "unknown_kind".to_string(),
            description: None,
            config: serde_json::Value::Null,
            interface: None,
        };
        match bundle.action_factory.build(&unknown_spec) {
            Err(ActionBuildError::UnknownKind(kind)) => assert_eq!(kind, "unknown_kind"),
            _ => panic!("expected unknown kind error"),
        }
    }
}
