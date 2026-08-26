//! CLI configuration resolution shared by the Agent entry point.

mod config;

pub(crate) use config::prepare_runtime_config_path;

const GENERATED_CONFIG_DIR: &str = ".orchestral/generated";
const GENERATED_CONFIG_FILE: &str = "default.agent.yaml";
const GENERATED_OVERRIDE_CONFIG_SUFFIX: &str = ".agent.override.yaml";

#[derive(Debug, Clone, Default)]
pub struct ModelOverrides {
    pub backend: Option<String>,
    pub model_profile: Option<String>,
    pub model: Option<String>,
    pub temperature: Option<f32>,
}

impl ModelOverrides {
    pub fn is_empty(&self) -> bool {
        self.backend.is_none()
            && self.model_profile.is_none()
            && self.model.is_none()
            && self.temperature.is_none()
    }
}
