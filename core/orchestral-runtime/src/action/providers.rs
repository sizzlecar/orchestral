use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::json;

use orchestral_core::config::{
    ActionInterfaceSpec, ActionSpec, ConfigError, McpServerSpec, OrchestralConfig,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionSpecSource {
    McpDiscovery,
    McpConfig,
    SkillDiscovery,
    ActionConfig,
}

impl ActionSpecSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::McpDiscovery => "mcp_discovery",
            Self::McpConfig => "mcp_config",
            Self::SkillDiscovery => "skill_discovery",
            Self::ActionConfig => "action_config",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActionRegistrationSpec {
    pub source: ActionSpecSource,
    pub spec: ActionSpec,
}

pub fn collect_action_registration_specs(
    config: &OrchestralConfig,
    config_path: &Path,
) -> Result<Vec<ActionRegistrationSpec>, ConfigError> {
    let mut all = Vec::new();

    // Registration order defines override priority because ActionRegistry::register replaces
    // existing actions with the same name.
    // priority: mcp < skill < explicit user actions
    all.extend(collect_mcp_action_registration_specs(config, config_path)?);
    all.extend(collect_skill_action_registration_specs(
        config,
        config_path,
    )?);
    all.extend(
        config
            .actions
            .actions
            .iter()
            .cloned()
            .map(|spec| ActionRegistrationSpec {
                source: ActionSpecSource::ActionConfig,
                spec,
            }),
    );

    Ok(all)
}

#[cfg(test)]
fn collect_mcp_action_specs(
    config: &OrchestralConfig,
    config_path: &Path,
) -> Result<Vec<ActionSpec>, ConfigError> {
    Ok(collect_mcp_action_registration_specs(config, config_path)?
        .into_iter()
        .map(|registration| registration.spec)
        .collect())
}

fn collect_mcp_action_registration_specs(
    config: &OrchestralConfig,
    config_path: &Path,
) -> Result<Vec<ActionRegistrationSpec>, ConfigError> {
    if !config.extensions.mcp.enabled || env_disable_flag("ORCHESTRAL_DISABLE_MCP") {
        return Ok(Vec::new());
    }

    let mut servers: HashMap<String, SourcedMcpServer> = HashMap::new();

    if config.extensions.mcp.auto_discover {
        for path in candidate_mcp_paths(config, config_path) {
            if !path.exists() || !path.is_file() {
                continue;
            }
            let content = fs::read_to_string(&path).map_err(ConfigError::Io)?;
            let discovered = parse_mcp_discovery_file(&content).map_err(|err| {
                ConfigError::Invalid(format!(
                    "parse MCP discovery file '{}' failed: {}",
                    path.display(),
                    err
                ))
            })?;
            for (name, server) in discovered {
                servers.insert(
                    name,
                    SourcedMcpServer {
                        source: ActionSpecSource::McpDiscovery,
                        server,
                    },
                );
            }
        }
    }

    for spec in &config.extensions.mcp.servers {
        servers.insert(
            spec.name.clone(),
            SourcedMcpServer {
                source: ActionSpecSource::McpConfig,
                server: ResolvedMcpServer::from_config_spec(spec),
            },
        );
    }

    let mut names = servers.keys().cloned().collect::<Vec<_>>();
    names.sort();

    let mut actions = Vec::new();
    for name in names {
        let Some(entry) = servers.get(&name).cloned() else {
            continue;
        };
        let source = entry.source;
        let server = entry.server;
        if !server.enabled {
            continue;
        }
        if server.command.is_none() && server.url.is_none() {
            if server.required {
                return Err(ConfigError::Invalid(format!(
                    "required MCP server '{}' must provide command or url",
                    name
                )));
            }
            continue;
        }

        let action_name = format!("mcp__{}", sanitize_identifier(&name));
        let description = format!(
            "MCP server '{}' bridge action. Use operation=list_tools or operation=call.",
            name
        );

        let interface = ActionInterfaceSpec {
            input_schema: json!({
                "type": "object",
                "properties": {
                    "operation": {
                        "type": "string",
                        "enum": ["list_tools", "call"],
                        "default": "call"
                    },
                    "tool": {"type": "string"},
                    "arguments": {"type": "object", "default": {}}
                },
                "required": ["operation"]
            }),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "server": {"type": "string"},
                    "operation": {"type": "string"},
                    "tool": {"type": "string"},
                    "tools": {"type": "array", "items": {"type": "string"}},
                    "result": {}
                },
                "required": ["server", "operation"]
            }),
        };

        actions.push(ActionRegistrationSpec {
            source,
            spec: ActionSpec {
                name: action_name,
                kind: "mcp_server".to_string(),
                description: Some(description),
                config: json!({
                    "server_name": name,
                    "command": server.command,
                    "args": server.args,
                    "env": server.env,
                    "url": server.url,
                    "headers": server.headers,
                    "bearer_token_env_var": server.bearer_token_env_var,
                    "required": server.required,
                    "startup_timeout_ms": server.startup_timeout_ms,
                    "tool_timeout_ms": server.tool_timeout_ms,
                    "enabled_tools": server.enabled_tools,
                    "disabled_tools": server.disabled_tools,
                }),
                interface: Some(interface),
            },
        });
    }

    Ok(actions)
}

#[cfg(test)]
fn collect_skill_action_specs(
    config: &OrchestralConfig,
    config_path: &Path,
) -> Result<Vec<ActionSpec>, ConfigError> {
    Ok(
        collect_skill_action_registration_specs(config, config_path)?
            .into_iter()
            .map(|registration| registration.spec)
            .collect(),
    )
}

fn collect_skill_action_registration_specs(
    config: &OrchestralConfig,
    config_path: &Path,
) -> Result<Vec<ActionRegistrationSpec>, ConfigError> {
    if !config.extensions.skill.enabled || env_disable_flag("ORCHESTRAL_DISABLE_SKILLS") {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    if config.extensions.skill.auto_discover {
        for dir in candidate_skill_directories(config, config_path) {
            collect_skill_files(&dir, 3, &mut files);
        }
    }

    let mut actions_by_name: HashMap<String, ActionRegistrationSpec> = HashMap::new();

    for file in files {
        let content = match fs::read_to_string(&file) {
            Ok(value) => value,
            Err(err) => {
                tracing::warn!(path = %file.display(), error = %err, "skip unreadable SKILL.md");
                continue;
            }
        };

        let meta = parse_skill_frontmatter(&content);
        let skill_name = meta
            .name
            .clone()
            .or_else(|| {
                file.parent()
                    .and_then(|p| p.file_name())
                    .and_then(|name| name.to_str())
                    .map(ToString::to_string)
            })
            .unwrap_or_else(|| "skill".to_string());

        let action_name = format!("skill__{}", sanitize_identifier(&skill_name));
        let description = append_skill_instruction_only_note(
            meta.description
                .unwrap_or_else(|| format!("Expose skill '{}' instructions", skill_name)),
        );
        let interface = ActionInterfaceSpec {
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"}
                }
            }),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "skill": {"type": "string"},
                    "source_path": {"type": "string"},
                    "instructions": {"type": "string"},
                    "query": {"type": "string"}
                },
                "required": ["skill", "source_path", "instructions"]
            }),
        };

        actions_by_name.insert(
            action_name.clone(),
            ActionRegistrationSpec {
                source: ActionSpecSource::SkillDiscovery,
                spec: ActionSpec {
                    name: action_name,
                    kind: "skill_prompt".to_string(),
                    description: Some(description),
                    config: json!({
                        "skill_name": skill_name,
                        "source_path": file.to_string_lossy().to_string(),
                        "content": content,
                    }),
                    interface: Some(interface),
                },
            },
        );
    }

    let mut names = actions_by_name.keys().cloned().collect::<Vec<_>>();
    names.sort();

    Ok(names
        .into_iter()
        .filter_map(|name| actions_by_name.remove(&name))
        .collect())
}

fn candidate_mcp_paths(config: &OrchestralConfig, config_path: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    let roots = discovery_roots(config_path);
    for root in &roots {
        candidates.push(root.join(".mcp.json"));
        candidates.push(root.join("mcp.json"));
        candidates.push(root.join(".claude").join("mcp.json"));
    }

    if let Ok(home) = std::env::var("HOME") {
        let home_path = PathBuf::from(home);
        candidates.push(home_path.join(".mcp.json"));
        candidates.push(home_path.join(".config").join("mcp.json"));
    }

    for custom in &config.extensions.mcp.discover_paths {
        let path = PathBuf::from(custom);
        if path.is_absolute() {
            candidates.push(path);
        } else {
            for root in &roots {
                candidates.push(root.join(&path));
            }
        }
    }

    dedupe_paths(candidates)
}

fn candidate_skill_directories(config: &OrchestralConfig, config_path: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let roots = discovery_roots(config_path);
    for root in &roots {
        dirs.push(root.join(".claude").join("skills"));
        dirs.push(root.join(".codex").join("skills"));
        dirs.push(root.join("skills"));
    }

    if let Ok(home) = std::env::var("HOME") {
        let home_path = PathBuf::from(home);
        dirs.push(home_path.join(".claude").join("skills"));
        dirs.push(home_path.join(".codex").join("skills"));
    }

    for custom in &config.extensions.skill.directories {
        let path = PathBuf::from(custom);
        if path.is_absolute() {
            dirs.push(path);
        } else {
            for root in &roots {
                dirs.push(root.join(&path));
            }
        }
    }

    dedupe_paths(dirs)
}

fn discovery_roots(config_path: &Path) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }

    if let Some(base) = config_path.parent() {
        roots.push(base.to_path_buf());
        if let Some(parent) = base.parent() {
            roots.push(parent.to_path_buf());
        }
    }

    dedupe_paths(roots)
}

fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for path in paths {
        let key = path.to_string_lossy().to_string();
        if seen.insert(key) {
            out.push(path);
        }
    }
    out
}

fn collect_skill_files(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    if depth == 0 {
        return;
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_skill_files(&path, depth - 1, out);
            continue;
        }

        let is_skill = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.eq_ignore_ascii_case("SKILL.md"))
            .unwrap_or(false);
        if is_skill {
            out.push(path);
        }
    }
}

#[derive(Debug, Default, Clone)]
struct SkillFrontmatter {
    name: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SkillFrontmatterRaw {
    name: Option<String>,
    description: Option<String>,
}

fn parse_skill_frontmatter(content: &str) -> SkillFrontmatter {
    let Some(rest) = content.strip_prefix("---\n") else {
        return SkillFrontmatter::default();
    };

    let Some((frontmatter, _tail)) = rest.split_once("\n---\n") else {
        return SkillFrontmatter::default();
    };

    match serde_yaml::from_str::<SkillFrontmatterRaw>(frontmatter) {
        Ok(raw) => SkillFrontmatter {
            name: raw.name,
            description: raw.description,
        },
        Err(_) => SkillFrontmatter::default(),
    }
}

#[derive(Debug, Clone)]
struct ResolvedMcpServer {
    enabled: bool,
    required: bool,
    command: Option<String>,
    args: Vec<String>,
    env: HashMap<String, String>,
    url: Option<String>,
    headers: HashMap<String, String>,
    bearer_token_env_var: Option<String>,
    startup_timeout_ms: Option<u64>,
    tool_timeout_ms: Option<u64>,
    enabled_tools: Vec<String>,
    disabled_tools: Vec<String>,
}

#[derive(Debug, Clone)]
struct SourcedMcpServer {
    source: ActionSpecSource,
    server: ResolvedMcpServer,
}

impl ResolvedMcpServer {
    fn from_config_spec(spec: &McpServerSpec) -> Self {
        Self {
            enabled: spec.enabled,
            required: spec.required,
            command: normalize_opt_string(spec.command.clone()),
            args: spec.args.clone(),
            env: spec.env.clone(),
            url: normalize_opt_string(spec.url.clone()),
            headers: spec.headers.clone(),
            bearer_token_env_var: normalize_opt_string(spec.bearer_token_env_var.clone()),
            startup_timeout_ms: spec.startup_timeout_ms,
            tool_timeout_ms: spec.tool_timeout_ms,
            enabled_tools: spec.enabled_tools.clone(),
            disabled_tools: spec.disabled_tools.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct McpDiscoveryFile {
    #[serde(
        default,
        rename = "mcpServers",
        alias = "mcp_servers",
        alias = "servers"
    )]
    mcp_servers: HashMap<String, McpDiscoveryServer>,
}

#[derive(Debug, Default, Deserialize)]
struct McpDiscoveryServer {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    required: Option<bool>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Option<Vec<String>>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default, alias = "httpHeaders", alias = "http_headers")]
    headers: Option<HashMap<String, String>>,
    #[serde(default, alias = "bearer_token_env", alias = "bearerTokenEnvVar")]
    bearer_token_env_var: Option<String>,
    #[serde(default)]
    startup_timeout_ms: Option<u64>,
    #[serde(default)]
    startup_timeout_sec: Option<f64>,
    #[serde(default)]
    tool_timeout_ms: Option<u64>,
    #[serde(default)]
    tool_timeout_sec: Option<f64>,
    #[serde(default)]
    enabled_tools: Option<Vec<String>>,
    #[serde(default)]
    disabled_tools: Option<Vec<String>>,
}

fn parse_mcp_discovery_file(content: &str) -> Result<HashMap<String, ResolvedMcpServer>, String> {
    let parsed: McpDiscoveryFile =
        serde_json::from_str(content).map_err(|err| format!("invalid JSON: {}", err))?;

    let mut result = HashMap::new();
    for (name, server) in parsed.mcp_servers {
        let startup_timeout_ms = server.startup_timeout_ms.or_else(|| {
            server
                .startup_timeout_sec
                .map(|secs| (secs.max(0.0) * 1000.0) as u64)
        });
        let tool_timeout_ms = server.tool_timeout_ms.or_else(|| {
            server
                .tool_timeout_sec
                .map(|secs| (secs.max(0.0) * 1000.0) as u64)
        });

        result.insert(
            name,
            ResolvedMcpServer {
                enabled: server.enabled.unwrap_or(true),
                required: server.required.unwrap_or(false),
                command: normalize_opt_string(server.command),
                args: server.args.unwrap_or_default(),
                env: server.env.unwrap_or_default(),
                url: normalize_opt_string(server.url),
                headers: server.headers.unwrap_or_default(),
                bearer_token_env_var: normalize_opt_string(server.bearer_token_env_var),
                startup_timeout_ms,
                tool_timeout_ms,
                enabled_tools: server.enabled_tools.unwrap_or_default(),
                disabled_tools: server.disabled_tools.unwrap_or_default(),
            },
        );
    }
    Ok(result)
}

fn normalize_opt_string(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn sanitize_identifier(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.trim_matches('_').is_empty() {
        "item".to_string()
    } else {
        out
    }
}

fn env_disable_flag(var_name: &str) -> bool {
    std::env::var(var_name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn append_skill_instruction_only_note(description: String) -> String {
    const NOTE: &str =
        "Instruction-only: returns skill guidance text and does not directly modify files or call tools.";
    if description.contains(NOTE) {
        return description;
    }
    format!("{} {}", description.trim(), NOTE)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|v| v.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "orchestral-{}-{}-{}",
            prefix,
            std::process::id(),
            nanos
        ));
        let _ = fs::create_dir_all(&path);
        path
    }

    #[test]
    fn parse_mcp_discovery_file_parses_aliases_and_timeouts() {
        let raw = r#"{
  "mcpServers": {
    "alpha": {
      "command": "node",
      "args": ["server.js"],
      "startup_timeout_sec": 2.5,
      "tool_timeout_sec": 3.0,
      "enabled_tools": ["a"],
      "disabled_tools": ["b"]
    }
  }
}"#;

        let parsed = parse_mcp_discovery_file(raw).expect("parse should succeed");
        let server = parsed.get("alpha").expect("alpha should exist");
        assert_eq!(server.command.as_deref(), Some("node"));
        assert_eq!(server.args, vec!["server.js".to_string()]);
        assert_eq!(server.startup_timeout_ms, Some(2500));
        assert_eq!(server.tool_timeout_ms, Some(3000));
        assert_eq!(server.enabled_tools, vec!["a".to_string()]);
        assert_eq!(server.disabled_tools, vec!["b".to_string()]);
    }

    #[test]
    fn collect_mcp_action_specs_merges_discovery_and_explicit_overrides() {
        let dir = test_temp_dir("providers-mcp");
        let config_path = dir.join("orchestral.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");
        fs::write(
            dir.join(".mcp.json"),
            r#"{
  "mcpServers": {
    "alpha": {"url": "http://127.0.0.1:8787/mcp", "enabled": true}
  }
}"#,
        )
        .expect("write discovery file");

        let mut config = OrchestralConfig::default();
        config.extensions.mcp.enabled = true;
        config.extensions.mcp.auto_discover = true;
        config.extensions.mcp.servers = vec![McpServerSpec {
            name: "alpha".to_string(),
            enabled: true,
            required: true,
            command: Some("npx".to_string()),
            args: vec!["-y".to_string(), "alpha-mcp".to_string()],
            env: HashMap::new(),
            url: None,
            headers: HashMap::new(),
            bearer_token_env_var: None,
            startup_timeout_ms: Some(1500),
            tool_timeout_ms: Some(2200),
            enabled_tools: vec!["ping".to_string()],
            disabled_tools: vec!["danger".to_string()],
        }];

        let actions = collect_mcp_action_specs(&config, &config_path).expect("collect should work");
        assert_eq!(actions.len(), 1);
        let action = &actions[0];
        assert_eq!(action.name, "mcp__alpha");
        assert_eq!(action.kind, "mcp_server");
        assert_eq!(action.config["server_name"], "alpha");
        // explicit config should override discovered url-only config
        assert_eq!(action.config["command"], "npx");
        assert_eq!(action.config["required"], true);
        assert_eq!(action.config["enabled_tools"], json!(["ping"]));
        assert_eq!(action.config["disabled_tools"], json!(["danger"]));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn collect_skill_action_specs_discovers_skill_markdown() {
        let dir = test_temp_dir("providers-skill");
        let config_path = dir.join("orchestral.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");

        let skill_dir = dir.join(".claude/skills/demo");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: demo-skill\ndescription: demo description\n---\nbody content\n",
        )
        .expect("write skill");

        let mut config = OrchestralConfig::default();
        config.extensions.skill.enabled = true;
        config.extensions.skill.auto_discover = true;

        let actions =
            collect_skill_action_specs(&config, &config_path).expect("collect skills should work");
        let action = actions
            .iter()
            .find(|action| action.name == "skill__demo-skill")
            .expect("discovered skill action should include demo-skill");
        assert_eq!(action.name, "skill__demo-skill");
        assert_eq!(action.kind, "skill_prompt");
        assert_eq!(action.config["skill_name"], "demo-skill");
        assert!(action
            .description
            .as_deref()
            .unwrap_or_default()
            .contains("Instruction-only: returns skill guidance text"));
        assert!(action.config["source_path"].as_str().is_some());
        assert!(action
            .config
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .contains("body content"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn collect_skill_action_specs_discovers_project_root_when_config_under_configs_dir() {
        let root = test_temp_dir("providers-skill-root");
        let config_dir = root.join("configs");
        fs::create_dir_all(&config_dir).expect("create configs dir");
        let config_path = config_dir.join("orchestral.cli.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");

        let skill_dir = root.join(".claude/skills/root-demo");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: root-demo-skill\ndescription: root demo description\n---\nbody content\n",
        )
        .expect("write skill");

        let mut config = OrchestralConfig::default();
        config.extensions.skill.enabled = true;
        config.extensions.skill.auto_discover = true;

        let actions =
            collect_skill_action_specs(&config, &config_path).expect("collect skills should work");
        assert!(
            actions
                .iter()
                .any(|action| action.name == "skill__root-demo-skill"),
            "expected project root skill to be discovered when config lives under configs/"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn collect_action_specs_keeps_override_order_for_same_name() {
        let dir = test_temp_dir("providers-order");
        let config_path = dir.join("orchestral.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");
        fs::write(
            dir.join(".mcp.json"),
            r#"{
  "mcpServers": {
    "alpha": {"url": "http://127.0.0.1:8787/mcp", "enabled": true}
  }
}"#,
        )
        .expect("write discovery file");

        let mut config = OrchestralConfig::default();
        config.extensions.mcp.enabled = true;
        config.extensions.mcp.auto_discover = true;
        config.actions.actions = vec![ActionSpec {
            name: "mcp__alpha".to_string(),
            kind: "echo".to_string(),
            description: Some("explicit override".to_string()),
            config: json!({"prefix":"Echo: "}),
            interface: None,
        }];

        let specs = collect_action_registration_specs(&config, &config_path)
            .expect("collect should work")
            .into_iter()
            .map(|registration| registration.spec)
            .collect::<Vec<_>>();
        let mcp_pos = specs
            .iter()
            .position(|s| s.kind == "mcp_server" && s.name == "mcp__alpha")
            .expect("mcp action present");
        let explicit_pos = specs
            .iter()
            .position(|s| s.kind == "echo" && s.name == "mcp__alpha")
            .expect("explicit action present");
        assert!(explicit_pos > mcp_pos);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn collect_action_registration_specs_records_sources() {
        let dir = test_temp_dir("providers-sources");
        let config_path = dir.join("orchestral.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");
        fs::write(
            dir.join(".mcp.json"),
            r#"{
  "mcpServers": {
    "alpha": {"url": "http://127.0.0.1:8787/mcp", "enabled": true}
  }
}"#,
        )
        .expect("write discovery file");

        let skill_dir = dir.join(".claude/skills/demo");
        fs::create_dir_all(&skill_dir).expect("create skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: demo-skill\ndescription: demo description\n---\nbody content\n",
        )
        .expect("write skill");

        let mut config = OrchestralConfig::default();
        config.extensions.mcp.enabled = true;
        config.extensions.mcp.auto_discover = true;
        config.extensions.skill.enabled = true;
        config.extensions.skill.auto_discover = true;
        config.actions.actions = vec![ActionSpec {
            name: "echo_demo".to_string(),
            kind: "echo".to_string(),
            description: Some("explicit action".to_string()),
            config: json!({"prefix":"Echo: "}),
            interface: None,
        }];

        let registrations = collect_action_registration_specs(&config, &config_path)
            .expect("collect registrations should work");
        assert!(registrations.iter().any(|registration| {
            registration.spec.name == "mcp__alpha"
                && registration.source == ActionSpecSource::McpDiscovery
        }));
        assert!(registrations.iter().any(|registration| {
            registration.spec.name == "skill__demo-skill"
                && registration.source == ActionSpecSource::SkillDiscovery
        }));
        assert!(registrations.iter().any(|registration| {
            registration.spec.name == "echo_demo"
                && registration.source == ActionSpecSource::ActionConfig
        }));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn collect_mcp_action_specs_discovers_project_root_when_config_under_configs_dir() {
        let root = test_temp_dir("providers-mcp-root");
        let config_dir = root.join("configs");
        fs::create_dir_all(&config_dir).expect("create configs dir");
        let config_path = config_dir.join("orchestral.cli.yaml");
        fs::write(&config_path, "version: 1\n").expect("write config placeholder");

        fs::write(
            root.join(".mcp.json"),
            r#"{
  "mcpServers": {
    "root-alpha": {"url": "http://127.0.0.1:8787/mcp", "enabled": true}
  }
}"#,
        )
        .expect("write root mcp file");

        let mut config = OrchestralConfig::default();
        config.extensions.mcp.enabled = true;
        config.extensions.mcp.auto_discover = true;

        let actions = collect_mcp_action_specs(&config, &config_path).expect("collect should work");
        assert!(
            actions
                .iter()
                .any(|action| action.name == "mcp__root-alpha"),
            "expected project root mcp server to be discovered when config lives under configs/"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parse_skill_frontmatter_handles_missing_delimiters() {
        let no_frontmatter = "name: a\ndescription: b";
        let parsed = parse_skill_frontmatter(no_frontmatter);
        assert!(parsed.name.is_none());
        assert!(parsed.description.is_none());
    }

    #[test]
    fn env_disable_flag_interprets_boolean_values() {
        let key = format!("ORCHESTRAL_TEST_DISABLE_{}", std::process::id());
        std::env::set_var(&key, "true");
        assert!(env_disable_flag(&key));
        std::env::set_var(&key, "0");
        assert!(!env_disable_flag(&key));
        std::env::remove_var(&key);
    }

    #[test]
    fn append_skill_instruction_only_note_is_idempotent() {
        let desc = "demo skill".to_string();
        let once = append_skill_instruction_only_note(desc);
        let twice = append_skill_instruction_only_note(once.clone());
        assert_eq!(once, twice);
    }
}
