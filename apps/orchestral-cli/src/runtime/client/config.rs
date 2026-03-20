use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use orchestral_core::config::{load_config, OrchestralConfig};
use serde_yaml::{Mapping, Value as YamlValue};

use super::{
    PlannerOverrides, GENERATED_CONFIG_DIR, GENERATED_CONFIG_FILE, GENERATED_OVERRIDE_CONFIG_SUFFIX,
};

pub(super) fn prepare_runtime_config_path(
    explicit: Option<PathBuf>,
    planner_overrides: &PlannerOverrides,
) -> anyhow::Result<PathBuf> {
    let resolved = resolve_runtime_config_path(explicit)?;
    if planner_overrides.is_empty() {
        return Ok(resolved);
    }
    write_overridden_runtime_config(&resolved, planner_overrides)
}

fn resolve_runtime_config_path(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        if !path.exists() {
            anyhow::bail!("config file not found: {}", path.display());
        }
        return Ok(path);
    }

    if let Some(found) = discover_config_path() {
        return Ok(found);
    }

    generate_default_config()
}

fn discover_config_path() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from(".orchestral/config.yaml"),
        PathBuf::from(".orchestral/config.yml"),
        PathBuf::from("configs/orchestral.cli.yaml"),
        PathBuf::from("configs/orchestral.yaml"),
        PathBuf::from("orchestral.yaml"),
    ];
    candidates.into_iter().find(|path| path.exists())
}

fn generate_default_config() -> anyhow::Result<PathBuf> {
    let cwd = std::env::current_dir().context("resolve current directory failed")?;
    let dir = cwd.join(GENERATED_CONFIG_DIR);
    fs::create_dir_all(&dir)
        .with_context(|| format!("create default config dir '{}' failed", dir.display()))?;
    let path = dir.join(GENERATED_CONFIG_FILE);

    let desired = embedded_default_config();
    let needs_write = match fs::read_to_string(&path) {
        Ok(existing) => existing != desired,
        Err(_) => true,
    };

    if needs_write {
        fs::write(&path, desired)
            .with_context(|| format!("write generated config '{}' failed", path.display()))?;
    }

    Ok(path)
}

fn write_overridden_runtime_config(
    base_path: &Path,
    planner_overrides: &PlannerOverrides,
) -> anyhow::Result<PathBuf> {
    let config = load_config(base_path)
        .with_context(|| format!("load config '{}' for overrides failed", base_path.display()))?;
    let raw = fs::read_to_string(base_path)
        .with_context(|| format!("read config '{}' failed", base_path.display()))?;
    let mut yaml: YamlValue = serde_yaml::from_str(&raw)
        .with_context(|| format!("parse config '{}' as yaml failed", base_path.display()))?;
    apply_planner_overrides_to_yaml(&mut yaml, &config, planner_overrides)?;

    let serialized = serde_yaml::to_string(&yaml).context("serialize overridden config failed")?;
    let override_path = runtime_override_config_path(base_path);
    fs::write(&override_path, serialized).with_context(|| {
        format!(
            "write overridden config '{}' failed",
            override_path.display()
        )
    })?;
    Ok(override_path)
}

pub(super) fn runtime_override_config_path(base_path: &Path) -> PathBuf {
    let parent = base_path.parent().unwrap_or_else(|| Path::new("."));
    let stem = base_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("orchestral");
    parent.join(format!("{}{}", stem, GENERATED_OVERRIDE_CONFIG_SUFFIX))
}

pub(super) fn apply_planner_overrides_to_yaml(
    yaml: &mut YamlValue,
    config: &OrchestralConfig,
    planner_overrides: &PlannerOverrides,
) -> anyhow::Result<()> {
    let root = yaml
        .as_mapping_mut()
        .ok_or_else(|| anyhow::anyhow!("config root must be a YAML mapping"))?;
    let planner = ensure_mapping_entry(root, "planner");

    if let Some(profile_name) = planner_overrides.model_profile.as_ref() {
        let profile = config
            .providers
            .get_model(profile_name)
            .ok_or_else(|| anyhow::anyhow!("planner model profile not found: {}", profile_name))?;
        set_yaml_key(
            planner,
            "model_profile",
            YamlValue::String(profile_name.clone()),
        );
        set_yaml_key(planner, "model", YamlValue::Null);
        if planner_overrides.temperature.is_none() {
            set_yaml_key(planner, "temperature", YamlValue::Null);
        }
        if planner_overrides.backend.is_none() {
            let backend_value = profile
                .backend
                .as_ref()
                .map(|backend| YamlValue::String(backend.clone()))
                .unwrap_or(YamlValue::Null);
            set_yaml_key(planner, "backend", backend_value);
        }
    }

    if let Some(model) = planner_overrides.model.as_ref() {
        set_yaml_key(planner, "model", YamlValue::String(model.clone()));
    }

    if let Some(backend) = planner_overrides.backend.as_ref() {
        if config.providers.get_backend(backend).is_none() {
            bail!("planner backend not found: {}", backend);
        }
        set_yaml_key(planner, "backend", YamlValue::String(backend.clone()));
    }

    if let Some(temperature) = planner_overrides.temperature {
        let value =
            serde_yaml::to_value(temperature).context("serialize planner temperature failed")?;
        set_yaml_key(planner, "temperature", value);
    }

    Ok(())
}

fn ensure_mapping_entry<'a>(map: &'a mut Mapping, key: &str) -> &'a mut Mapping {
    let key_value = YamlValue::String(key.to_string());
    let entry = map
        .entry(key_value)
        .or_insert_with(|| YamlValue::Mapping(Mapping::new()));
    if !entry.is_mapping() {
        *entry = YamlValue::Mapping(Mapping::new());
    }
    entry
        .as_mapping_mut()
        .expect("mapping entry should exist after normalization")
}

fn set_yaml_key(map: &mut Mapping, key: &str, value: YamlValue) {
    map.insert(YamlValue::String(key.to_string()), value);
}

pub(super) fn embedded_default_config() -> String {
    let (planner_backend, planner_model_profile) = detect_default_llm_profile();
    format!(
        r#"version: 1

app:
  name: orchestral-cli
  environment: development

runtime:
  max_interactions_per_thread: 10
  auto_cleanup: true
  concurrency_policy: interrupt_and_start_new
  strict_exports: true
  max_planner_iterations: 6

planner:
  mode: llm
  backend: {planner_backend}
  model_profile: {planner_model_profile}
  max_history: 20
  dynamic_model_selection: true

interpreter:
  mode: auto

context:
  history_limit: 50
  max_tokens: 4096
  include_history: true
  include_references: true

extensions:
  mcp:
    enabled: true
    auto_discover: true
  skill:
    enabled: true
    auto_discover: true

providers:
  default_backend: {planner_backend}
  default_model: {planner_model_profile}
  backends:
    - name: openai
      kind: openai
      api_key_env: OPENAI_API_KEY
      config:
        timeout_secs: 60
    - name: google
      kind: google
      api_key_env: GOOGLE_API_KEY
      config:
        timeout_secs: 60
    - name: anthropic
      kind: anthropic
      api_key_env: ANTHROPIC_API_KEY
      config:
        timeout_secs: 60
    - name: openrouter
      kind: openrouter
      api_key_env: OPENROUTER_API_KEY
      endpoint: https://openrouter.ai/api/v1/
      config:
        timeout_secs: 60
  models:
    - name: gpt-4o-mini
      backend: openai
      model: gpt-4o-mini
      temperature: 0.2
    - name: gemini-2.5-flash
      backend: google
      model: gemini-2.5-flash
      temperature: 0.2
    - name: claude-sonnet-4-5
      backend: anthropic
      model: claude-sonnet-4-5
      temperature: 0.2
    - name: claude-sonnet-4-5-openrouter
      backend: openrouter
      model: anthropic/claude-sonnet-4.5
      temperature: 0.2

actions:
  hot_reload: false
  actions:
    - name: echo
      kind: echo
      description: Echo text back.
      interface:
        input_schema:
          type: object
          properties:
            message:
              type: string
          required: [message]
        output_schema:
          type: object
          properties:
            result:
              type: string
          required: [result]
      config:
        prefix: "Echo: "
    - name: shell
      kind: shell
      description: Run a shell command.
      interface:
        input_schema:
          type: object
          properties:
            command:
              type: string
            args:
              type: array
              items:
                type: string
          required: [command]
        output_schema:
          type: object
          properties:
            stdout:
              type: string
            stderr:
              type: string
            status:
              type: integer
          required: [stdout, stderr, status]
      config:
        timeout_ms: 10000
    - name: file_read
      kind: file_read
      description: Read a file from workspace.
      interface:
        input_schema:
          type: object
          properties:
            path:
              type: string
          required: [path]
        output_schema:
          type: object
          properties:
            content:
              type: string
            path:
              type: string
          required: [content, path]
      config:
        root_dir: "."
"#
    )
}

fn detect_default_llm_profile() -> (&'static str, &'static str) {
    if has_env("OPENAI_API_KEY") {
        ("openai", "gpt-4o-mini")
    } else if has_any_env(&["GOOGLE_API_KEY", "GEMINI_API_KEY"]) {
        ("google", "gemini-2.5-flash")
    } else if has_any_env(&["ANTHROPIC_API_KEY", "CLAUDE_API_KEY"]) {
        ("anthropic", "claude-sonnet-4-5")
    } else if has_env("OPENROUTER_API_KEY") {
        ("openrouter", "claude-sonnet-4-5-openrouter")
    } else {
        ("openai", "gpt-4o-mini")
    }
}

fn has_any_env(names: &[&str]) -> bool {
    names.iter().any(|name| has_env(name))
}

fn has_env(name: &str) -> bool {
    std::env::var(name)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    const KEY_ENV_NAMES: &[&str] = &[
        "OPENAI_API_KEY",
        "GOOGLE_API_KEY",
        "GEMINI_API_KEY",
        "ANTHROPIC_API_KEY",
        "CLAUDE_API_KEY",
        "OPENROUTER_API_KEY",
    ];

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn clear_key_envs() {
        for name in KEY_ENV_NAMES {
            std::env::remove_var(name);
        }
    }

    #[test]
    fn test_detect_default_llm_profile_prefers_openai_over_other_keys() {
        let _guard = env_lock().lock().expect("env lock");
        clear_key_envs();
        std::env::set_var("OPENAI_API_KEY", "openai");
        std::env::set_var("GOOGLE_API_KEY", "google");
        std::env::set_var("ANTHROPIC_API_KEY", "anthropic");
        std::env::set_var("OPENROUTER_API_KEY", "openrouter");

        assert_eq!(detect_default_llm_profile(), ("openai", "gpt-4o-mini"));

        clear_key_envs();
    }

    #[test]
    fn test_detect_default_llm_profile_accepts_google_and_claude_aliases() {
        let _guard = env_lock().lock().expect("env lock");
        clear_key_envs();
        std::env::set_var("GEMINI_API_KEY", "gemini");
        assert_eq!(detect_default_llm_profile(), ("google", "gemini-2.5-flash"));

        clear_key_envs();
        std::env::set_var("CLAUDE_API_KEY", "claude");
        assert_eq!(
            detect_default_llm_profile(),
            ("anthropic", "claude-sonnet-4-5")
        );

        clear_key_envs();
    }

    #[test]
    fn test_detect_default_llm_profile_falls_back_to_openrouter_then_openai_default() {
        let _guard = env_lock().lock().expect("env lock");
        clear_key_envs();
        std::env::set_var("OPENROUTER_API_KEY", "openrouter");
        assert_eq!(
            detect_default_llm_profile(),
            ("openrouter", "claude-sonnet-4-5-openrouter")
        );

        clear_key_envs();
        assert_eq!(detect_default_llm_profile(), ("openai", "gpt-4o-mini"));
    }
}
