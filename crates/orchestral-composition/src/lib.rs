use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use orchestral_api::{ApiError, RuntimeAppBuilder};
use orchestral_config::load_config;
use orchestral_extension_host::build_extension_runtime_bundle;
use orchestral_infra::{InfraComponentFactory, InfraFactoryOptions};
use orchestral_runtime::RuntimeApp;
use orchestral_spi::{ComponentRegistry, RuntimeBuildRequest, RuntimeComponentFactory, SpiError};

pub use orchestral_extension_host::{RuntimeExtensionCatalog, RuntimeTarget};

pub struct ComposedRuntimeAppBuilder {
    target: RuntimeTarget,
    extension_catalog: RuntimeExtensionCatalog,
    infra_options: InfraFactoryOptions,
}

impl ComposedRuntimeAppBuilder {
    pub fn new(target: RuntimeTarget) -> Self {
        Self {
            target,
            extension_catalog: RuntimeExtensionCatalog::with_builtin_extensions(),
            infra_options: InfraFactoryOptions::default(),
        }
    }

    pub fn with_extension_catalog(
        target: RuntimeTarget,
        extension_catalog: RuntimeExtensionCatalog,
    ) -> Self {
        Self {
            target,
            extension_catalog,
            infra_options: InfraFactoryOptions::default(),
        }
    }

    pub fn with_infra_options(mut self, infra_options: InfraFactoryOptions) -> Self {
        self.infra_options = infra_options;
        self
    }
}

#[async_trait]
impl RuntimeAppBuilder for ComposedRuntimeAppBuilder {
    async fn build(&self, config_path: PathBuf) -> Result<RuntimeApp, ApiError> {
        let config = load_config(&config_path)
            .map_err(|err| ApiError::Internal(format!("load config failed: {}", err)))?;
        let config = Arc::new(config);

        let infra_factory = Arc::new(InfraComponentFactory::with_options(
            config.clone(),
            self.infra_options.clone(),
        ));
        let extension_bundle = build_extension_runtime_bundle(
            config.clone(),
            &config_path,
            self.target,
            &self.extension_catalog,
        )
        .await
        .map_err(|err| ApiError::Internal(format!("build runtime extensions failed: {}", err)))?;

        let component_factory = Arc::new(CompositeComponentFactory::new(
            infra_factory,
            extension_bundle.component_factory,
        ));

        RuntimeApp::from_config_path_with_spi(
            config_path,
            component_factory,
            extension_bundle.hook_registry,
            extension_bundle.action_factory,
        )
        .await
        .map_err(|err| ApiError::Internal(format!("build runtime app failed: {}", err)))
    }
}

struct CompositeComponentFactory {
    infra_factory: Arc<dyn RuntimeComponentFactory>,
    extension_factory: Arc<dyn RuntimeComponentFactory>,
}

impl CompositeComponentFactory {
    fn new(
        infra_factory: Arc<dyn RuntimeComponentFactory>,
        extension_factory: Arc<dyn RuntimeComponentFactory>,
    ) -> Self {
        Self {
            infra_factory,
            extension_factory,
        }
    }
}

#[async_trait]
impl RuntimeComponentFactory for CompositeComponentFactory {
    async fn build(&self, request: &RuntimeBuildRequest) -> Result<ComponentRegistry, SpiError> {
        let mut merged = ComponentRegistry::new();

        let infra = self.infra_factory.build(request).await?;
        merge_component_registry(&mut merged, infra, "infra_factory")?;

        let extensions = self.extension_factory.build(request).await?;
        merge_component_registry(&mut merged, extensions, "extension_factory")?;

        Ok(merged)
    }
}

fn merge_component_registry(
    target: &mut ComponentRegistry,
    mut source: ComponentRegistry,
    source_name: &str,
) -> Result<(), SpiError> {
    if let Some(stores) = source.stores.take() {
        if target.stores.is_some() {
            return Err(SpiError::Internal(format!(
                "{} tried to override stores component",
                source_name
            )));
        }
        target.stores = Some(stores);
    }

    if let Some(blob_store) = source.blob_store.take() {
        if target.blob_store.is_some() {
            return Err(SpiError::Internal(format!(
                "{} tried to override blob_store component",
                source_name
            )));
        }
        target.blob_store = Some(blob_store);
    }

    for (key, component) in source.into_named_components() {
        if target.get_named_component(&key).is_some() {
            return Err(SpiError::Internal(format!(
                "{} tried to override named component '{}'",
                source_name, key
            )));
        }
        target.insert_named_component(key, component);
    }

    Ok(())
}
