use std::path::PathBuf;
use std::sync::Arc;

use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use thiserror::Error;
use tokio::sync::RwLock;

use orchestral_config::{load_actions_config, ActionsConfig, ConfigError};
use orchestral_core::executor::ActionRegistry;

use crate::factory::{ActionBuildError, ActionFactory};

/// Action config errors
#[derive(Debug, Error)]
pub enum ActionConfigError {
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("build error: {0}")]
    Build(#[from] ActionBuildError),
    #[error("notify error: {0}")]
    Notify(#[from] notify::Error),
}

/// Loads action specs and maintains a live registry
pub struct ActionRegistryManager {
    path: PathBuf,
    registry: Arc<RwLock<ActionRegistry>>,
    factory: Arc<dyn ActionFactory>,
}

impl ActionRegistryManager {
    pub fn new(path: impl Into<PathBuf>, factory: Arc<dyn ActionFactory>) -> Self {
        Self {
            path: path.into(),
            registry: Arc::new(RwLock::new(ActionRegistry::new())),
            factory,
        }
    }

    pub fn registry(&self) -> Arc<RwLock<ActionRegistry>> {
        self.registry.clone()
    }

    pub async fn load(&self) -> Result<usize, ActionConfigError> {
        let config = load_actions_config(&self.path)?;
        self.load_from_config(&config).await
    }

    /// Load from an already-parsed config.
    pub async fn load_from_config(
        &self,
        config: &ActionsConfig,
    ) -> Result<usize, ActionConfigError> {
        let mut registry = ActionRegistry::new();
        for spec in &config.actions {
            let action = self.factory.build(spec)?;
            registry.register(action);
        }

        let mut current = self.registry.write().await;
        *current = registry;
        Ok(current.names().len())
    }

    pub fn start_watching(self: &Arc<Self>) -> Result<ActionWatcher, ActionConfigError> {
        let manager = Arc::clone(self);
        let handle = tokio::runtime::Handle::current();

        let mut watcher: RecommendedWatcher =
            notify::recommended_watcher(move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                    if matches!(
                        event.kind,
                        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
                    ) {
                        let manager = Arc::clone(&manager);
                        handle.spawn(async move {
                            if let Err(e) = manager.load().await {
                                tracing::error!("Action reload failed: {}", e);
                            } else {
                                tracing::info!("Actions reloaded from config");
                            }
                        });
                    }
                }
            })?;

        watcher.watch(&self.path, RecursiveMode::NonRecursive)?;
        Ok(ActionWatcher { _watcher: watcher })
    }
}

/// Keeps file watcher alive
pub struct ActionWatcher {
    _watcher: RecommendedWatcher,
}
