use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use thiserror::Error;
use tokio::sync::RwLock;

use orchestral_core::executor::ActionRegistry;

use crate::config::{ActionSpec, ActionsConfig};
use crate::factory::{ActionBuildError, ActionFactory};

/// Action config errors
#[derive(Debug, Error)]
pub enum ActionConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse error: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("build error: {0}")]
    Build(#[from] ActionBuildError),
    #[error("notify error: {0}")]
    Notify(#[from] notify::Error),
    #[error("invalid config: {0}")]
    InvalidConfig(String),
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
        let mut registry = ActionRegistry::new();
        for spec in config.actions {
            let action = self.factory.build(&spec)?;
            registry.register(action);
        }

        let mut current = self.registry.write().await;
        *current = registry;
        Ok(current.names().len())
    }

    pub fn start_watching(self: &Arc<Self>) -> Result<ActionWatcher, ActionConfigError> {
        let manager = Arc::clone(self);
        let handle = tokio::runtime::Handle::current();

        let mut watcher: RecommendedWatcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| {
                if let Ok(event) = res {
                if matches!(
                    event.kind,
                    EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
                ) {
                    let manager = Arc::clone(&manager);
                    handle.spawn(async move {
                        let _ = manager.load().await;
                    });
                }
            }
        },
        )?;

        watcher.watch(&self.path, RecursiveMode::NonRecursive)?;
        Ok(ActionWatcher { _watcher: watcher })
    }
}

/// Keeps file watcher alive
pub struct ActionWatcher {
    _watcher: RecommendedWatcher,
}

fn load_actions_config(path: &Path) -> Result<ActionsConfig, ActionConfigError> {
    let content = fs::read_to_string(path)?;
    let config: ActionsConfig = serde_yaml::from_str(&content)?;
    if config.actions.is_empty() {
        return Err(ActionConfigError::InvalidConfig(
            "actions list is empty".to_string(),
        ));
    }
    validate_specs(&config.actions)?;
    Ok(config)
}

fn validate_specs(specs: &[ActionSpec]) -> Result<(), ActionConfigError> {
    for spec in specs {
        if spec.name.trim().is_empty() {
            return Err(ActionConfigError::InvalidConfig(
                "action name must not be empty".to_string(),
            ));
        }
        if spec.kind.trim().is_empty() {
            return Err(ActionConfigError::InvalidConfig(
                "action kind must not be empty".to_string(),
            ));
        }
    }
    Ok(())
}
