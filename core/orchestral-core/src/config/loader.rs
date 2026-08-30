use std::collections::HashSet;
use std::fs;
use std::path::Path;

use thiserror::Error;

use super::{McpTransportSpec, OrchestralConfig, ProvidersConfig};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML parse error: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

pub fn load_config(path: &Path) -> Result<OrchestralConfig, ConfigError> {
    let content = fs::read_to_string(path)?;
    let config: OrchestralConfig = serde_yaml::from_str(&content)?;
    validate_config(&config)?;
    Ok(config)
}

pub fn load_providers_config(path: &Path) -> Result<ProvidersConfig, ConfigError> {
    Ok(load_config(path)?.providers)
}

fn validate_config(config: &OrchestralConfig) -> Result<(), ConfigError> {
    if config.version != 1 {
        return invalid("version must be 1");
    }
    if config.app.name.trim().is_empty() {
        return invalid("app.name must not be empty");
    }
    if config.agent.stream_buffer == 0
        || config.agent.max_model_steps == Some(0)
        || config.agent.max_tool_calls == Some(0)
        || config.agent.history_limit == 0
        || config.agent.max_context_tokens == 0
        || config.agent.reserved_output_tokens == 0
    {
        return invalid("Agent limits must be positive when present");
    }
    if config.agent.reserved_output_tokens >= config.agent.max_context_tokens {
        return invalid("agent.reserved_output_tokens must be below max_context_tokens");
    }
    if config.agent.compaction.enabled
        && (config.agent.compaction.minimum_source_records == 0
            || config.agent.compaction.keep_recent_records == 0
            || config.agent.compaction.summary_max_chars < 256
            || config
                .agent
                .compaction
                .minimum_source_records
                .checked_add(config.agent.compaction.keep_recent_records)
                .is_none())
    {
        return invalid("enabled Agent compaction limits are invalid");
    }
    if config.tools.max_timeout_ms == 0 || config.tools.max_output_bytes == 0 {
        return invalid("Tool limits must be positive");
    }
    if config
        .tools
        .exec
        .network_targets
        .iter()
        .any(|target| !valid_network_target(target))
    {
        return invalid("tools.exec.network_targets must contain exact host:port values");
    }
    validate_storage_backend("journal", &config.journal.backend, &config.journal.root_dir)?;
    validate_storage_backend(
        "artifacts",
        &config.artifacts.backend,
        &config.artifacts.root_dir,
    )?;
    if config.artifacts.max_bytes == 0 || config.artifacts.summary_max_chars == 0 {
        return invalid("Artifact limits must be positive");
    }
    validate_providers(config)?;
    validate_mcp(config)?;
    Ok(())
}

fn valid_network_target(target: &str) -> bool {
    let Some((host, port)) = target.trim().rsplit_once(':') else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);
    !host.is_empty()
        && host
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || ".-_:".contains(character))
        && port.parse::<u16>().is_ok_and(|port| port > 0)
}

fn validate_storage_backend(
    section: &str,
    backend: &str,
    root_dir: &str,
) -> Result<(), ConfigError> {
    if !matches!(backend.trim(), "memory" | "filesystem" | "fs") {
        return invalid(format!("unsupported {section}.backend '{backend}'"));
    }
    if backend.trim() != "memory" && root_dir.trim().is_empty() {
        return invalid(format!("{section}.root_dir must not be empty"));
    }
    Ok(())
}

fn validate_providers(config: &OrchestralConfig) -> Result<(), ConfigError> {
    let mut backend_names = HashSet::new();
    for backend in &config.providers.backends {
        if backend.name.trim().is_empty() || backend.kind.trim().is_empty() {
            return invalid("Provider backend name and kind must not be empty");
        }
        if !backend_names.insert(backend.name.as_str()) {
            return invalid(format!("duplicate Provider backend '{}'", backend.name));
        }
    }
    let mut model_names = HashSet::new();
    for model in &config.providers.models {
        if model.name.trim().is_empty() || model.model.trim().is_empty() {
            return invalid("Model profile name and model must not be empty");
        }
        if !model_names.insert(model.name.as_str()) {
            return invalid(format!("duplicate Model profile '{}'", model.name));
        }
        if !backend_names.contains(model.backend.as_str()) {
            return invalid(format!(
                "Model profile '{}' references unknown backend '{}'",
                model.name, model.backend
            ));
        }
    }
    for selected in [
        config.providers.default_backend.as_deref(),
        config.agent.backend.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !backend_names.contains(selected) {
            return invalid(format!("selected backend '{selected}' was not found"));
        }
    }
    for selected in [
        config.providers.default_model.as_deref(),
        config.agent.model_profile.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if !model_names.contains(selected) {
            return invalid(format!("selected Model profile '{selected}' was not found"));
        }
    }
    Ok(())
}

fn validate_mcp(config: &OrchestralConfig) -> Result<(), ConfigError> {
    if config
        .mcp
        .import_files
        .iter()
        .any(|path| path.trim().is_empty() || path.chars().any(char::is_control))
    {
        return invalid("MCP import file paths must not be empty");
    }
    let mut names = HashSet::new();
    for server in &config.mcp.servers {
        if server.name.trim().is_empty() {
            return invalid("MCP server name must not be empty");
        }
        if !names.insert(server.name.as_str()) {
            return invalid(format!("duplicate MCP server '{}'", server.name));
        }
        match &server.transport {
            McpTransportSpec::Stdio {
                command,
                cwd,
                readable_roots,
                writable_roots,
                network_targets,
                ..
            } => {
                if command.trim().is_empty()
                    || cwd.as_deref().is_some_and(|path| path.trim().is_empty())
                    || readable_roots
                        .iter()
                        .chain(writable_roots.iter())
                        .any(|path| path.trim().is_empty() || path.chars().any(char::is_control))
                    || network_targets
                        .iter()
                        .any(|target| !valid_network_target(target))
                {
                    return invalid("MCP stdio declaration is invalid");
                }
            }
            McpTransportSpec::StreamableHttp {
                endpoint,
                credential_headers,
                max_frame_bytes,
            } => {
                if endpoint.trim().is_empty()
                    || *max_frame_bytes == Some(0)
                    || credential_headers.iter().any(|(header, credential)| {
                        header.trim().is_empty() || credential.env.trim().is_empty()
                    })
                {
                    return invalid("MCP Streamable HTTP declaration is invalid");
                }
            }
        }
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError::Invalid(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = OrchestralConfig::default();
        assert_eq!(config.agent.max_model_steps, None);
        assert_eq!(config.agent.max_tool_calls, None);
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn continuation_ceilings_are_optional_and_positive_when_configured() {
        let config = serde_yaml::from_str::<OrchestralConfig>(
            r#"
agent:
  max_model_steps: 48
  max_tool_calls: 96
"#,
        )
        .expect("explicit continuation ceilings deserialize");
        assert_eq!(config.agent.max_model_steps, Some(48));
        assert_eq!(config.agent.max_tool_calls, Some(96));
        assert!(validate_config(&config).is_ok());

        let mut invalid = OrchestralConfig::default();
        invalid.agent.max_model_steps = Some(0);
        assert!(matches!(
            validate_config(&invalid),
            Err(ConfigError::Invalid(message)) if message.contains("positive when present")
        ));
    }

    #[test]
    fn legacy_hidden_model_round_limit_is_rejected() {
        let error = serde_yaml::from_str::<OrchestralConfig>(
            r#"
agent:
  max_model_rounds: 16
"#,
        )
        .expect_err("the removed hidden round-limit surface must not deserialize");
        assert!(error.to_string().contains("max_model_rounds"));
    }

    #[test]
    fn old_planner_surface_is_rejected() {
        let error = serde_yaml::from_str::<OrchestralConfig>("version: 1\nplanner:\n  mode: llm\n")
            .expect_err("old Planner config must not deserialize");
        assert!(error.to_string().contains("unknown field `planner`"));
    }

    #[test]
    fn unified_exec_needs_no_per_program_allowlist() {
        let mut config = OrchestralConfig::default();
        config.tools.exec.enabled = true;
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn exec_network_targets_are_exact_host_and_port_pairs() {
        let mut config = OrchestralConfig::default();
        config.tools.exec.network_targets = vec![
            "localhost:43128".to_owned(),
            "127.0.0.1:443".to_owned(),
            "[::1]:8080".to_owned(),
            "api.example.com:443".to_owned(),
        ];
        assert!(validate_config(&config).is_ok());

        for invalid_target in [
            "",
            "api.example.com",
            "api.example.com:*",
            "api.example.com:0",
            "api.example.com:65536",
            "api.example.com:443\") (allow network-outbound)",
        ] {
            config.tools.exec.network_targets = vec![invalid_target.to_owned()];
            assert!(matches!(
                validate_config(&config),
                Err(ConfigError::Invalid(message))
                    if message.contains("exact host:port")
            ));
        }
    }

    #[test]
    fn enabled_session_compaction_requires_bounded_positive_limits() {
        let mut config = OrchestralConfig::default();
        config.agent.compaction.summary_max_chars = 255;
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::Invalid(message)) if message.contains("compaction")
        ));
        config.agent.compaction.enabled = false;
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn tagged_stdio_and_streamable_http_mcp_transports_are_valid() {
        let config = serde_yaml::from_str::<OrchestralConfig>(
            r#"
mcp:
  servers:
    - name: local
      transport:
        type: stdio
        command: /bin/echo
        args: [--stdio]
        cwd: .
        readable_roots: [/opt/example-mcp]
        writable_roots: [.orchestral/mcp/local]
        network_targets: [localhost:4317]
    - name: remote
      transport:
        type: streamable_http
        endpoint: https://example.com/mcp
        credential_headers:
          Authorization:
            env: MCP_TOKEN
"#,
        )
        .unwrap();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn stdio_mcp_rejects_invalid_resource_authority() {
        let config = serde_yaml::from_str::<OrchestralConfig>(
            r#"
mcp:
  servers:
    - name: local
      transport:
        type: stdio
        command: /bin/echo
        readable_roots: [""]
        network_targets: [missing-port]
"#,
        )
        .unwrap();
        assert!(matches!(
            validate_config(&config),
            Err(ConfigError::Invalid(message)) if message.contains("stdio declaration")
        ));
    }

    #[test]
    fn flat_legacy_mcp_command_surface_is_rejected() {
        let error = serde_yaml::from_str::<OrchestralConfig>(
            r#"
mcp:
  servers:
    - name: legacy
      command: /bin/echo
"#,
        )
        .expect_err("MCP transport must use the tagged transport contract");
        assert!(error.to_string().contains("transport"));
    }
}
