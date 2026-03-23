//! Configuration loading and hot-reload support.

use std::fs;
use std::path::Path;

use thiserror::Error;

use super::{ActionsConfig, OrchestralConfig, ProvidersConfig};

/// Configuration loading errors.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parse error: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("Invalid config: {0}")]
    Invalid(String),
}

/// Load full Orchestral configuration from YAML file.
pub fn load_config(path: &Path) -> Result<OrchestralConfig, ConfigError> {
    let content = fs::read_to_string(path)?;
    let config: OrchestralConfig = serde_yaml::from_str(&content)?;
    validate_config(&config)?;
    Ok(config)
}

/// Load only providers section from unified config file.
pub fn load_providers_config(path: &Path) -> Result<ProvidersConfig, ConfigError> {
    let config = load_config(path)?;
    Ok(config.providers)
}

/// Load only actions section from unified config file.
pub fn load_actions_config(path: &Path) -> Result<ActionsConfig, ConfigError> {
    let config = load_config(path)?;
    Ok(config.actions)
}

fn validate_config(config: &OrchestralConfig) -> Result<(), ConfigError> {
    if config.version == 0 {
        return Err(ConfigError::Invalid(
            "version must be greater than 0".to_string(),
        ));
    }

    if config.app.name.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "app.name must not be empty".to_string(),
        ));
    }

    if config.runtime.max_interactions_per_thread == 0 {
        return Err(ConfigError::Invalid(
            "runtime.max_interactions_per_thread must be > 0".to_string(),
        ));
    }

    if config.runtime.max_planner_iterations == 0 {
        return Err(ConfigError::Invalid(
            "runtime.max_planner_iterations must be > 0".to_string(),
        ));
    }

    if config.context.max_tokens == 0 {
        return Err(ConfigError::Invalid(
            "context.max_tokens must be > 0".to_string(),
        ));
    }

    validate_providers(&config.providers)?;
    validate_actions(&config.actions)?;
    validate_extensions(config)?;
    validate_blobs(config)?;

    Ok(())
}

fn validate_providers(config: &ProvidersConfig) -> Result<(), ConfigError> {
    for backend in &config.backends {
        if backend.name.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "providers.backends[].name must not be empty".to_string(),
            ));
        }
        if backend.kind.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "providers.backends[].kind must not be empty".to_string(),
            ));
        }
    }

    for model in &config.models {
        if model.name.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "providers.models[].name must not be empty".to_string(),
            ));
        }
        if model.model.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "providers.models[{}].model must not be empty",
                model.name
            )));
        }
        if let Some(backend) = &model.backend {
            if config.get_backend(backend).is_none() {
                return Err(ConfigError::Invalid(format!(
                    "providers.models[{}].backend '{}' not found",
                    model.name, backend
                )));
            }
        }
    }

    // Legacy providers are still supported.
    for provider in &config.providers {
        if provider.name.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "providers.providers[].name must not be empty".to_string(),
            ));
        }
        if provider.kind.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "providers.providers[].kind must not be empty".to_string(),
            ));
        }
        if provider.model.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "providers.providers[].model must not be empty".to_string(),
            ));
        }
    }

    if let Some(default_backend) = &config.default_backend {
        if config.get_backend(default_backend).is_none() {
            return Err(ConfigError::Invalid(format!(
                "providers.default_backend '{}' not found",
                default_backend
            )));
        }
    }

    if let Some(default_model) = &config.default_model {
        if config.get_model(default_model).is_none() {
            return Err(ConfigError::Invalid(format!(
                "providers.default_model '{}' not found",
                default_model
            )));
        }
    }

    if let Some(default_provider) = &config.default_provider {
        if config.get_backend(default_provider).is_none()
            && config.get_model(default_provider).is_none()
        {
            return Err(ConfigError::Invalid(format!(
                "providers.default_provider '{}' not found",
                default_provider
            )));
        }
    }

    Ok(())
}

fn validate_actions(config: &ActionsConfig) -> Result<(), ConfigError> {
    for spec in &config.actions {
        if spec.name.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "action name must not be empty".to_string(),
            ));
        }
        if spec.kind.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "action kind must not be empty".to_string(),
            ));
        }
        if let Some(interface) = &spec.interface {
            if !interface.input_schema.is_null() && !interface.input_schema.is_object() {
                return Err(ConfigError::Invalid(format!(
                    "action '{}' interface.input_schema must be an object",
                    spec.name
                )));
            }
            if !interface.output_schema.is_null() && !interface.output_schema.is_object() {
                return Err(ConfigError::Invalid(format!(
                    "action '{}' interface.output_schema must be an object",
                    spec.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_extensions(config: &OrchestralConfig) -> Result<(), ConfigError> {
    for spec in &config.extensions.runtime {
        if spec.name.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "extensions.runtime[].name must not be empty".to_string(),
            ));
        }
    }
    for spec in &config.extensions.mcp.servers {
        if spec.name.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "extensions.mcp.servers[].name must not be empty".to_string(),
            ));
        }
        let has_stdio = spec
            .command
            .as_ref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false);
        let has_http = spec
            .url
            .as_ref()
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false);
        if !has_stdio && !has_http {
            return Err(ConfigError::Invalid(format!(
                "extensions.mcp.servers[{}] must set command or url",
                spec.name
            )));
        }
    }
    Ok(())
}

fn validate_blobs(config: &OrchestralConfig) -> Result<(), ConfigError> {
    let mode = config.blobs.mode.trim().to_ascii_lowercase();
    let write_to_s3 = config
        .blobs
        .hybrid
        .write_to
        .trim()
        .eq_ignore_ascii_case("s3");
    let needs_s3 = mode == "s3" || (mode == "hybrid" && write_to_s3);

    if needs_s3
        && config
            .blobs
            .s3
            .bucket
            .as_ref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
    {
        return Err(ConfigError::Invalid(
            "blobs.s3.bucket must be set when blobs.mode is s3 or hybrid(write_to=s3)".to_string(),
        ));
    }

    let has_access_env = config
        .blobs
        .s3
        .access_key_env
        .as_ref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);
    let has_secret_env = config
        .blobs
        .s3
        .secret_key_env
        .as_ref()
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);
    if has_access_env != has_secret_env {
        return Err(ConfigError::Invalid(
            "blobs.s3.access_key_env and blobs.s3.secret_key_env must be set together".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{McpServerSpec, RuntimeExtensionSpec};
    use serde_yaml::from_str;

    #[test]
    fn test_validate_config_accepts_default_extensions() {
        let config = OrchestralConfig::default();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_rejects_empty_runtime_extension_name() {
        let mut config = OrchestralConfig::default();
        config.extensions.runtime = vec![RuntimeExtensionSpec {
            name: "".to_string(),
            enabled: true,
            targets: Vec::new(),
            options: serde_json::Value::Null,
        }];

        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn test_extensions_alias_plugins_maps_to_runtime() {
        let yaml = r#"
version: 1
app:
  name: orchestral
plugins:
  runtime:
    - name: custom_dummy
      enabled: true
"#;
        let config: OrchestralConfig = from_str(yaml).expect("parse yaml");
        assert_eq!(config.extensions.runtime.len(), 1);
        assert_eq!(config.extensions.runtime[0].name, "custom_dummy");
    }

    #[test]
    fn test_validate_config_rejects_s3_mode_without_bucket() {
        let mut config = OrchestralConfig::default();
        config.blobs.mode = "s3".to_string();
        config.blobs.s3.bucket = None;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn test_validate_config_rejects_mcp_server_without_transport() {
        let mut config = OrchestralConfig::default();
        config.extensions.mcp.servers = vec![McpServerSpec {
            name: "alpha".to_string(),
            enabled: true,
            required: false,
            command: None,
            args: Vec::new(),
            env: std::collections::HashMap::new(),
            url: None,
            headers: std::collections::HashMap::new(),
            bearer_token_env_var: None,
            startup_timeout_ms: None,
            tool_timeout_ms: None,
            enabled_tools: Vec::new(),
            disabled_tools: Vec::new(),
        }];

        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::Invalid(_))
        ));
    }

    #[test]
    fn test_validate_config_rejects_zero_max_planner_iterations() {
        let mut config = OrchestralConfig::default();
        config.runtime.max_planner_iterations = 0;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::Invalid(message))
            if message.contains("runtime.max_planner_iterations must be > 0")
        ));
    }

    #[test]
    fn test_validate_config_accepts_mcp_server_with_command() {
        let mut config = OrchestralConfig::default();
        config.extensions.mcp.servers = vec![McpServerSpec {
            name: "alpha".to_string(),
            enabled: true,
            required: false,
            command: Some("npx".to_string()),
            args: vec!["-y".to_string(), "demo-mcp".to_string()],
            env: std::collections::HashMap::new(),
            url: None,
            headers: std::collections::HashMap::new(),
            bearer_token_env_var: None,
            startup_timeout_ms: Some(1000),
            tool_timeout_ms: Some(2000),
            enabled_tools: Vec::new(),
            disabled_tools: Vec::new(),
        }];

        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_accepts_recipe_templates() {
        let yaml = r#"
version: 1
app:
  name: orchestral
recipes:
  templates:
    - name: fill_sheet
      stages:
        - id: inspect
          kind: action
          selector:
            all_of: ["filesystem_read"]
        - id: derive
          kind: agent
          params:
            mode: leaf
            goal: derive patch
            output_keys: ["change_spec"]
"#;
        let config: OrchestralConfig = from_str(yaml).expect("parse yaml");
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_ignores_legacy_duplicate_recipe_templates() {
        let yaml = r#"
version: 1
app:
  name: orchestral
recipes:
  templates:
    - name: fill_sheet
      stages:
        - id: inspect
          kind: action
          action: file_read
    - name: fill_sheet
      stages:
        - id: inspect
          kind: action
          action: file_read
"#;
        let config: OrchestralConfig = from_str(yaml).expect("parse yaml");
        assert!(validate_config(&config).is_ok());
    }
}
