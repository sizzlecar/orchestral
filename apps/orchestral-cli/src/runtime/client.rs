//! CLI configuration resolution shared by the Agent entry point.

mod config;

pub(crate) use config::inspect_runtime_config;
pub(crate) use config::prepare_runtime_config_path;
pub(crate) use config::resolve_runtime_config_path;

const GENERATED_CONFIG_DIR: &str = ".orchestral/generated";
const GENERATED_CONFIG_FILE: &str = "default.agent.yaml";
const GENERATED_OVERRIDE_CONFIG_SUFFIX: &str = ".agent.override.yaml";

#[derive(Debug, Clone, Default)]
pub struct ModelOverrides {
    pub backend: Option<String>,
    pub model_profile: Option<String>,
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub base_url: Option<String>,
    pub api_key_env: Option<String>,
    pub no_auth: bool,
}

impl ModelOverrides {
    pub fn is_empty(&self) -> bool {
        self.backend.is_none()
            && self.model_profile.is_none()
            && self.model.is_none()
            && self.temperature.is_none()
            && self.base_url.is_none()
            && self.api_key_env.is_none()
            && !self.no_auth
    }
}
