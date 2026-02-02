use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use orchestral_config::{ActionInterfaceSpec, ActionSpec};
use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};

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
            Some(action) => {
                let action: Arc<dyn Action> = Arc::from(action);
                Ok(Arc::new(ConfiguredAction::new(action, spec)))
            }
            None => Err(ActionBuildError::UnknownKind(spec.kind.clone())),
        }
    }
}

struct ConfiguredAction {
    inner: Arc<dyn Action>,
    metadata: ActionMeta,
}

impl ConfiguredAction {
    fn new(inner: Arc<dyn Action>, spec: &ActionSpec) -> Self {
        let metadata = merge_action_metadata(inner.metadata(), spec.interface.as_ref());
        Self { inner, metadata }
    }
}

#[async_trait]
impl Action for ConfiguredAction {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn description(&self) -> &str {
        self.inner.description()
    }

    fn metadata(&self) -> ActionMeta {
        self.metadata.clone()
    }

    async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult {
        self.inner.run(input, ctx).await
    }
}

fn merge_action_metadata(base: ActionMeta, interface: Option<&ActionInterfaceSpec>) -> ActionMeta {
    let Some(interface) = interface else {
        return base;
    };

    let mut merged = base;
    if !interface.input_schema.is_null() {
        merged.input_schema = interface.input_schema.clone();
    }
    if !interface.output_schema.is_null() {
        merged.output_schema = interface.output_schema.clone();
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_action_metadata_overrides_declared_interface() {
        let base = ActionMeta::new("echo", "echo")
            .with_input_schema(json!({"type":"object","properties":{"old":{"type":"string"}}}))
            .with_output_schema(json!({"type":"object","properties":{"old":{"type":"string"}}}));
        let interface = ActionInterfaceSpec {
            input_schema: json!({"type":"object","properties":{"message":{"type":"string"}}}),
            output_schema: json!({"type":"object","properties":{"result":{"type":"string"}}}),
        };

        let merged = merge_action_metadata(base, Some(&interface));
        assert_eq!(merged.input_schema, interface.input_schema);
        assert_eq!(merged.output_schema, interface.output_schema);
    }
}
