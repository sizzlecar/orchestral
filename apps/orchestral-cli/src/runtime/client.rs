mod config;
mod event_support;
mod submit;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use tokio::sync::Mutex;
use tokio::time::Duration;

use orchestral_composition::{ComposedRuntimeAppBuilder, RuntimeTarget};
use orchestral_runtime::api::{RuntimeApi, RuntimeAppBuilder};

use crate::channel::CliRuntime;

use self::config::prepare_runtime_config_path;

const TURN_SETTLE_TIMEOUT: Duration = Duration::from_secs(30);
const TURN_SETTLE_GRACE_TIMEOUT: Duration = Duration::from_secs(60);
const FORWARD_DRAIN_IDLE_TIMEOUT: Duration = Duration::from_millis(120);

#[derive(Clone)]
pub struct RuntimeClient {
    runtime: Arc<CliRuntime>,
    thread_id: String,
    submit_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Default)]
pub struct PlannerOverrides {
    pub backend: Option<String>,
    pub model_profile: Option<String>,
    pub model: Option<String>,
    pub temperature: Option<f32>,
}

impl PlannerOverrides {
    pub fn is_empty(&self) -> bool {
        self.backend.is_none()
            && self.model_profile.is_none()
            && self.model.is_none()
            && self.temperature.is_none()
    }
}

impl RuntimeClient {
    pub async fn from_config(
        config: Option<PathBuf>,
        thread_id_override: Option<String>,
        planner_overrides: PlannerOverrides,
    ) -> anyhow::Result<Self> {
        let config = prepare_runtime_config_path(config, &planner_overrides)?;
        let app_builder: Arc<dyn RuntimeAppBuilder> =
            Arc::new(ComposedRuntimeAppBuilder::new(RuntimeTarget::Cli));
        let api = Arc::new(
            RuntimeApi::from_config_path_with_builder(config, app_builder)
                .await
                .context("failed to build runtime api")?,
        );
        let runtime = CliRuntime::from_api(api, thread_id_override)
            .await
            .context("failed to build cli runtime from config")?;
        Ok(Self {
            thread_id: runtime.thread_id().to_string(),
            runtime: Arc::new(runtime),
            submit_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }
}

const GENERATED_CONFIG_DIR: &str = ".orchestral/generated";
const GENERATED_CONFIG_FILE: &str = "default.cli.yaml";
const GENERATED_OVERRIDE_CONFIG_SUFFIX: &str = ".runtime.override.yaml";

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::config::{BackendSpec, ModelPolicy, ModelProfile, OrchestralConfig};
    use serde_json::json;
    use serde_yaml::Value as YamlValue;

    #[test]
    fn test_event_interaction_id_from_assistant_output() {
        let event = ChannelEvent::assistant_output("thread-1", "int-1", json!({"message":"ok"}));
        assert_eq!(event_interaction_id(&event), Some("int-1"));
    }

    #[test]
    fn test_event_interaction_id_from_system_trace_payload() {
        let event = ChannelEvent::trace("thread-1", "info", json!({"interaction_id":"int-2"}));
        assert_eq!(event_interaction_id(&event), Some("int-2"));
    }

    #[test]
    fn test_normalize_step_action_label_falls_back_to_step_id_for_empty_action() {
        assert_eq!(
            normalize_step_action_label("process_xlsx", Some("".to_string())),
            "process_xlsx"
        );
        assert_eq!(
            normalize_step_action_label("process_xlsx", Some("   ".to_string())),
            "process_xlsx"
        );
        assert_eq!(
            normalize_step_action_label("process_xlsx", None),
            "process_xlsx"
        );
        assert_eq!(
            normalize_step_action_label("process_xlsx", Some("file_read".to_string())),
            "file_read"
        );
    }

    #[test]
    fn test_format_running_label_avoids_duplicate_parentheses() {
        assert_eq!(
            format_running_label("process_xlsx", "process_xlsx"),
            "Running process_xlsx"
        );
        assert_eq!(
            format_running_label("read_skill_docs", "file_read"),
            "Running read_skill_docs (file_read)"
        );
    }

    #[test]
    fn test_apply_planner_overrides_aligns_backend_with_model_profile() {
        let mut yaml: YamlValue = serde_yaml::from_str(
            r#"
planner:
  backend: openrouter
  model_profile: claude-sonnet-4-5
  model: anthropic/claude-sonnet-4.5
"#,
        )
        .unwrap();
        let mut config = OrchestralConfig::default();
        config.providers.backends = vec![
            BackendSpec {
                name: "openrouter".to_string(),
                kind: "openrouter".to_string(),
                endpoint: None,
                api_key_env: None,
                config: json!(null),
            },
            BackendSpec {
                name: "google".to_string(),
                kind: "google".to_string(),
                endpoint: None,
                api_key_env: None,
                config: json!(null),
            },
        ];
        config.providers.models = vec![ModelProfile {
            name: "gemini-2.5-flash".to_string(),
            backend: Some("google".to_string()),
            model: "gemini-2.5-flash".to_string(),
            temperature: Some(0.2),
            max_tokens: None,
            system_prompt: None,
            policy: ModelPolicy::default(),
            config: json!(null),
        }];

        apply_planner_overrides_to_yaml(
            &mut yaml,
            &config,
            &PlannerOverrides {
                backend: None,
                model_profile: Some("gemini-2.5-flash".to_string()),
                model: None,
                temperature: None,
            },
        )
        .unwrap();

        let planner = yaml
            .get("planner")
            .and_then(|value| value.as_mapping())
            .unwrap();
        let backend_key = YamlValue::String("backend".to_string());
        let profile_key = YamlValue::String("model_profile".to_string());
        let model_key = YamlValue::String("model".to_string());
        assert_eq!(
            planner.get(&backend_key).and_then(|value| value.as_str()),
            Some("google")
        );
        assert_eq!(
            planner.get(&profile_key).and_then(|value| value.as_str()),
            Some("gemini-2.5-flash")
        );
        assert!(planner.get(&model_key).is_some_and(YamlValue::is_null));
    }

    #[test]
    fn test_runtime_override_config_path_is_stable_sibling_file() {
        let base = Path::new("configs/orchestral.cli.yaml");
        let override_path = runtime_override_config_path(base);
        assert_eq!(
            override_path,
            PathBuf::from("configs/orchestral.cli.runtime.override.yaml")
        );
    }
}
