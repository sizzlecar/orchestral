use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use orchestral_core::config::{load_config, OrchestralConfig};
use serde_yaml::{Mapping, Value as YamlValue};

use super::{
    ModelOverrides, GENERATED_CONFIG_DIR, GENERATED_CONFIG_FILE, GENERATED_OVERRIDE_CONFIG_SUFFIX,
};

pub(crate) fn prepare_runtime_config_path(
    explicit: Option<PathBuf>,
    model_overrides: &ModelOverrides,
) -> anyhow::Result<PathBuf> {
    let base_path = resolve_runtime_config_path(explicit)?;
    let automatic = auto_override_model_if_needed(&base_path).unwrap_or_default();
    let effective = merge_model_overrides(model_overrides, &automatic);
    if effective.is_empty() {
        return Ok(base_path);
    }
    write_overridden_runtime_config(&base_path, &effective)
}

fn auto_override_model_if_needed(config_path: &Path) -> Option<ModelOverrides> {
    let config = load_config(config_path).ok()?;
    let backend_name = config
        .agent
        .backend
        .as_deref()
        .or(config.providers.default_backend.as_deref())?;
    let backend = config.providers.get_backend(backend_name)?;
    if backend.resolve_api_key().is_ok() {
        return None;
    }
    let (detected_backend, detected_profile) = detect_default_model_profile();
    (detected_backend != backend_name).then(|| ModelOverrides {
        backend: Some(detected_backend.to_owned()),
        model_profile: Some(detected_profile.to_owned()),
        model: None,
        temperature: None,
    })
}

fn merge_model_overrides(requested: &ModelOverrides, automatic: &ModelOverrides) -> ModelOverrides {
    ModelOverrides {
        backend: requested
            .backend
            .clone()
            .or_else(|| automatic.backend.clone()),
        model_profile: requested
            .model_profile
            .clone()
            .or_else(|| automatic.model_profile.clone()),
        model: requested.model.clone().or_else(|| automatic.model.clone()),
        temperature: requested.temperature.or(automatic.temperature),
    }
}

fn resolve_runtime_config_path(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        if !path.exists() {
            bail!("config file not found: {}", path.display());
        }
        return Ok(path);
    }
    discover_config_path().map_or_else(generate_default_config, Ok)
}

fn discover_config_path() -> Option<PathBuf> {
    [
        PathBuf::from(".orchestral/config.yaml"),
        PathBuf::from(".orchestral/config.yml"),
        PathBuf::from("configs/orchestral.cli.yaml"),
        PathBuf::from("orchestral.yaml"),
    ]
    .into_iter()
    .find(|path| path.exists())
}

fn generate_default_config() -> anyhow::Result<PathBuf> {
    let root = std::env::current_dir().context("resolve current directory")?;
    let directory = root.join(GENERATED_CONFIG_DIR);
    fs::create_dir_all(&directory)
        .with_context(|| format!("create config directory '{}'", directory.display()))?;
    let path = directory.join(GENERATED_CONFIG_FILE);
    let desired = embedded_default_config();
    if fs::read_to_string(&path).ok().as_deref() != Some(desired.as_str()) {
        fs::write(&path, desired)
            .with_context(|| format!("write generated config '{}'", path.display()))?;
    }
    Ok(path)
}

fn write_overridden_runtime_config(
    base_path: &Path,
    overrides: &ModelOverrides,
) -> anyhow::Result<PathBuf> {
    let config =
        load_config(base_path).with_context(|| format!("load config '{}'", base_path.display()))?;
    let raw = fs::read_to_string(base_path)
        .with_context(|| format!("read config '{}'", base_path.display()))?;
    let mut yaml: YamlValue = serde_yaml::from_str(&raw)
        .with_context(|| format!("parse config '{}'", base_path.display()))?;
    apply_model_overrides_to_yaml(&mut yaml, &config, overrides)?;
    let output = serde_yaml::to_string(&yaml).context("serialize model overrides")?;
    let path = runtime_override_config_path(base_path);
    fs::write(&path, output)
        .with_context(|| format!("write override config '{}'", path.display()))?;
    Ok(path)
}

fn runtime_override_config_path(base_path: &Path) -> PathBuf {
    let parent = base_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = base_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("orchestral");
    parent.join(format!("{stem}{GENERATED_OVERRIDE_CONFIG_SUFFIX}"))
}

fn apply_model_overrides_to_yaml(
    yaml: &mut YamlValue,
    config: &OrchestralConfig,
    overrides: &ModelOverrides,
) -> anyhow::Result<()> {
    let root = yaml
        .as_mapping_mut()
        .context("config root must be a YAML mapping")?;
    let agent = ensure_mapping_entry(root, "agent");

    if let Some(profile_name) = &overrides.model_profile {
        let profile = config
            .providers
            .get_model(profile_name)
            .with_context(|| format!("Model profile not found: {profile_name}"))?;
        set_yaml_key(
            agent,
            "model_profile",
            YamlValue::String(profile_name.clone()),
        );
        set_yaml_key(agent, "model", YamlValue::Null);
        if overrides.backend.is_none() {
            set_yaml_key(agent, "backend", YamlValue::String(profile.backend));
        }
        if overrides.temperature.is_none() {
            set_yaml_key(agent, "temperature", YamlValue::Null);
        }
    }
    if let Some(backend) = &overrides.backend {
        if config.providers.get_backend(backend).is_none() {
            bail!("Model backend not found: {backend}");
        }
        set_yaml_key(agent, "backend", YamlValue::String(backend.clone()));
    }
    if let Some(model) = &overrides.model {
        set_yaml_key(agent, "model", YamlValue::String(model.clone()));
    }
    if let Some(temperature) = overrides.temperature {
        set_yaml_key(
            agent,
            "temperature",
            serde_yaml::to_value(temperature).context("serialize temperature")?,
        );
    }
    Ok(())
}

fn ensure_mapping_entry<'a>(mapping: &'a mut Mapping, key: &str) -> &'a mut Mapping {
    let entry = mapping
        .entry(YamlValue::String(key.to_owned()))
        .or_insert_with(|| YamlValue::Mapping(Mapping::new()));
    if !entry.is_mapping() {
        *entry = YamlValue::Mapping(Mapping::new());
    }
    entry.as_mapping_mut().expect("entry was normalized")
}

fn set_yaml_key(mapping: &mut Mapping, key: &str, value: YamlValue) {
    mapping.insert(YamlValue::String(key.to_owned()), value);
}

fn embedded_default_config() -> String {
    let (backend, profile) = detect_default_model_profile();
    format!(
        r#"version: 1

app:
  name: orchestral-cli
  environment: development

agent:
  backend: {backend}
  model_profile: {profile}
  stream_buffer: 128
  max_model_rounds: 8
  max_tool_calls: 32
  max_context_tokens: 131072
  reserved_output_tokens: 4096

providers:
  default_backend: {backend}
  default_model: {profile}
  backends:
    - name: openai
      kind: openai
      api_key_env: OPENAI_API_KEY
      config: {{ timeout_secs: 60 }}
    - name: google
      kind: gemini
      api_key_env: GOOGLE_API_KEY
      config: {{ timeout_secs: 60 }}
    - name: openrouter
      kind: openrouter
      endpoint: https://openrouter.ai/api/v1
      api_key_env: OPENROUTER_API_KEY
      config: {{ timeout_secs: 60 }}
    - name: deepseek
      kind: deepseek
      endpoint: https://api.deepseek.com
      api_key_env: DEEPSEEK_API_KEY
      config: {{ timeout_secs: 60 }}
  models:
    - name: gpt-4o-mini
      backend: openai
      model: gpt-4o-mini
      temperature: 0.2
      max_tokens: 8192
    - name: gemini-2.5-flash
      backend: google
      model: gemini-2.5-flash
      temperature: 0.2
      max_tokens: 8192
    - name: openrouter-auto
      backend: openrouter
      model: openrouter/auto
      temperature: 0.2
      max_tokens: 8192
    - name: deepseek-chat
      backend: deepseek
      model: deepseek-chat
      temperature: 0.2
      max_tokens: 8192

tools:
  max_timeout_ms: 30000
  max_output_bytes: 1048576
  shell:
    enabled: true
    allowed_programs: [git, rg, cargo, rustc, ls, find, sed, head, tail, wc, pwd, mkdir, cp, mv]

mcp:
  enabled: true
  servers: []

skills:
  enabled: true
  auto_discover: true
  max_active_skills: 3
  directories: []
  trust_workspace: false
  allow_incompatible: false

journal:
  backend: filesystem
  root_dir: .orchestral/agent-journal

artifacts:
  backend: filesystem
  root_dir: .orchestral/artifacts
  max_bytes: 67108864
  summary_max_chars: 512

observability:
  log_level: info
  traces_enabled: false
"#
    )
}

fn detect_default_model_profile() -> (&'static str, &'static str) {
    if has_any_env(&["GOOGLE_API_KEY", "GEMINI_API_KEY"]) {
        ("google", "gemini-2.5-flash")
    } else if has_env("OPENAI_API_KEY") {
        ("openai", "gpt-4o-mini")
    } else if has_env("DEEPSEEK_API_KEY") {
        ("deepseek", "deepseek-chat")
    } else if has_env("OPENROUTER_API_KEY") {
        ("openrouter", "openrouter-auto")
    } else {
        ("openai", "gpt-4o-mini")
    }
}

fn has_any_env(names: &[&str]) -> bool {
    names.iter().any(|name| has_env(name))
}

fn has_env(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_config_is_strict_agent_config() {
        let raw = embedded_default_config();
        let parsed: OrchestralConfig = serde_yaml::from_str(&raw).expect("strict config");
        assert!(parsed.tools.shell.enabled);
        assert!(!raw.contains("planner:"));
        assert!(!raw.contains("actions:"));
        assert!(!raw.contains("task:"));
    }

    #[test]
    fn model_overrides_write_to_agent_section() {
        let raw = embedded_default_config();
        let config: OrchestralConfig = serde_yaml::from_str(&raw).expect("config");
        let mut yaml: YamlValue = serde_yaml::from_str(&raw).expect("yaml");
        apply_model_overrides_to_yaml(
            &mut yaml,
            &config,
            &ModelOverrides {
                backend: Some("google".to_owned()),
                model_profile: Some("gemini-2.5-flash".to_owned()),
                model: None,
                temperature: Some(0.1),
            },
        )
        .expect("override");
        assert_eq!(yaml["agent"]["backend"].as_str(), Some("google"));
        assert_eq!(
            yaml["agent"]["model_profile"].as_str(),
            Some("gemini-2.5-flash")
        );
    }
}
