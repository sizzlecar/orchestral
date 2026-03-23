use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use thiserror::Error;
use tokio::sync::RwLock;

use orchestral_core::action::ActionMeta;
use orchestral_core::config::{load_config, ConfigError, OrchestralConfig};
use orchestral_core::executor::ActionRegistry;

use super::builtin::JsonStdoutAction;
use super::factory::{ActionBuildError, ActionFactory};
use super::providers::{collect_action_registration_specs, ActionRegistrationSpec};

/// Action config errors
#[derive(Debug, Error)]
pub enum ActionConfigError {
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("build error: {0}")]
    Build(#[from] ActionBuildError),
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
        let config = load_config(&self.path)?;
        self.load_from_orchestral_config(&config).await
    }

    /// Load from an already-parsed full orchestral config.
    pub async fn load_from_orchestral_config(
        &self,
        config: &OrchestralConfig,
    ) -> Result<usize, ActionConfigError> {
        let specs = collect_action_registration_specs(config, &self.path)?;
        self.load_from_registration_specs(specs).await
    }

    async fn load_from_registration_specs(
        &self,
        specs: Vec<ActionRegistrationSpec>,
    ) -> Result<usize, ActionConfigError> {
        let mut registry = ActionRegistry::new();
        let mut effective: HashMap<String, (ActionRegistrationSpec, ActionMeta)> = HashMap::new();
        for registration in specs {
            let name = registration.spec.name.clone();
            match self.factory.build(&registration.spec) {
                Ok(action) => {
                    let metadata = action.metadata();
                    registry.register(action);
                    effective.insert(name, (registration, metadata));
                }
                Err(super::factory::ActionBuildError::UnknownKind(ref kind)) => {
                    tracing::warn!(kind = %kind, "skipping unknown action kind (may require an extension)");
                }
                Err(e) => return Err(e.into()),
            }
        }
        registry.register(Arc::new(JsonStdoutAction::internal()));

        let mut current = self.registry.write().await;
        *current = registry;
        let count = current.names().len();
        drop(current);

        log_registered_actions(&effective);
        Ok(count)
    }
}

fn log_registered_actions(actions: &HashMap<String, (ActionRegistrationSpec, ActionMeta)>) {
    let mut names = actions.keys().cloned().collect::<Vec<_>>();
    names.sort();
    tracing::info!(count = names.len(), "action registry loaded");

    for name in names {
        let Some((registration, metadata)) = actions.get(&name) else {
            continue;
        };
        let spec = &registration.spec;
        let description = summarize_description(Some(&metadata.description));
        let input = summarize_schema(&metadata.input_schema);
        let output = summarize_schema(&metadata.output_schema);
        let capabilities = if metadata.capabilities.is_empty() {
            "-".to_string()
        } else {
            metadata.capabilities.join(",")
        };
        let category = metadata.category.as_deref().unwrap_or("-");

        tracing::info!(
            source = registration.source.as_str(),
            name = %spec.name,
            kind = %spec.kind,
            category = %category,
            desc = %description,
            capabilities = %capabilities,
            input = %input,
            output = %output,
            "registered action",
        );

        if !metadata.input_schema.is_null() || !metadata.output_schema.is_null() {
            tracing::debug!(
                name = %spec.name,
                input_schema = %metadata.input_schema,
                output_schema = %metadata.output_schema,
                "registered action schemas",
            );
        }
    }
}

fn summarize_description(description: Option<&str>) -> String {
    const MAX_CHARS: usize = 200;
    let compact = description
        .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "-".to_string());
    trim_chars(compact, MAX_CHARS)
}

fn summarize_schema(schema: &Value) -> String {
    if let Some(obj) = schema.as_object() {
        let mut fields = obj
            .get("properties")
            .and_then(Value::as_object)
            .map(|props| props.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        fields.sort();

        let mut required = obj
            .get("required")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(ToString::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        required.sort();

        if fields.is_empty() && required.is_empty() {
            return "object".to_string();
        }
        return format!(
            "fields=[{}] required=[{}]",
            fields.join(","),
            required.join(",")
        );
    }

    let serialized =
        serde_json::to_string(schema).unwrap_or_else(|_| "<invalid_schema>".to_string());
    trim_chars(serialized, 180)
}

fn trim_chars(value: String, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value;
    }

    let mut out = value.chars().take(max_chars).collect::<String>();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn summarize_schema_lists_fields_and_required() {
        let schema = json!({
            "type": "object",
            "properties": {
                "tool": {"type": "string"},
                "arguments": {"type": "object"}
            },
            "required": ["tool"]
        });

        let summary = summarize_schema(&schema);
        assert_eq!(summary, "fields=[arguments,tool] required=[tool]");
    }

    #[test]
    fn summarize_description_compacts_whitespace() {
        let summary = summarize_description(Some("  hello\n\nworld\tfrom action  "));
        assert_eq!(summary, "hello world from action");
    }
}
