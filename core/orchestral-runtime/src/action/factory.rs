use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::config::ActionSpec;

use super::builtin::build_builtin_action;
use super::document::build_document_action;

use super::mcp::build_mcp_action;
use super::spreadsheet::build_spreadsheet_action;
use super::structured::build_structured_action;

/// Action factory errors
#[derive(Debug, Error)]
pub enum ActionBuildError {
    #[error("unknown action kind: {0}")]
    UnknownKind(String),
    #[error("invalid action config: {0}")]
    InvalidConfig(String),
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
        let wrap = |action: Arc<dyn Action>| -> Result<Arc<dyn Action>, ActionBuildError> {
            Ok(Arc::new(ConfiguredAction::new(action, spec)))
        };

        if let Some(a) = build_builtin_action(spec) {
            return wrap(Arc::from(a));
        }
        if let Some(a) = build_spreadsheet_action(spec)? {
            return wrap(Arc::from(a));
        }
        if let Some(a) = build_document_action(spec)? {
            return wrap(Arc::from(a));
        }
        if let Some(a) = build_structured_action(spec)? {
            return wrap(Arc::from(a));
        }
        if let Some(a) = build_mcp_action(spec)? {
            return wrap(Arc::from(a));
        }
        Err(ActionBuildError::UnknownKind(spec.kind.clone()))
    }
}

struct ConfiguredAction {
    inner: Arc<dyn Action>,
    metadata: ActionMeta,
}

impl ConfiguredAction {
    fn new(inner: Arc<dyn Action>, spec: &ActionSpec) -> Self {
        let metadata = merge_action_metadata(inner.metadata(), spec);
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

fn merge_action_metadata(base: ActionMeta, spec: &ActionSpec) -> ActionMeta {
    let mut merged = base;
    if let Some(category) = &spec.category {
        if !category.trim().is_empty() {
            merged.category = Some(category.clone());
        }
    }
    if let Some(interface) = spec.interface.as_ref() {
        if !interface.input_schema.is_null() {
            merged.input_schema = interface.input_schema.clone();
        }
        if !interface.output_schema.is_null() {
            merged.output_schema = interface.output_schema.clone();
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::config::ActionInterfaceSpec;
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
        let spec = ActionSpec {
            name: "echo".to_string(),
            kind: "echo".to_string(),
            description: None,
            category: Some("direct".to_string()),
            config: json!({}),
            interface: Some(interface.clone()),
        };

        let merged = merge_action_metadata(base, &spec);
        assert_eq!(merged.input_schema, interface.input_schema);
        assert_eq!(merged.output_schema, interface.output_schema);
        assert_eq!(merged.category.as_deref(), Some("direct"));
    }
}
