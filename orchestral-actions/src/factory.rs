use std::sync::Arc;

use thiserror::Error;

use orchestral_config::ActionSpec;
use orchestral_core::action::Action;

use crate::builtin::build_builtin_action;

/// Action factory errors
#[derive(Debug, Error)]
pub enum ActionBuildError {
    #[error("unknown action kind: {0}")]
    UnknownKind(String),
}

/// Action factory trait
pub trait ActionFactory: Send + Sync {
    fn build(&self, spec: &ActionSpec) -> Result<Arc<dyn Action>, ActionBuildError>;
}

/// Default factory for built-in actions
pub struct DefaultActionFactory;

impl DefaultActionFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultActionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ActionFactory for DefaultActionFactory {
    fn build(&self, spec: &ActionSpec) -> Result<Arc<dyn Action>, ActionBuildError> {
        match build_builtin_action(spec) {
            Some(action) => Ok(Arc::from(action)),
            None => Err(ActionBuildError::UnknownKind(spec.kind.clone())),
        }
    }
}
