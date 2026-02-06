//! # Orchestral Config
//!
//! Unified single-file configuration management for Orchestral.
//! A single `orchestral.yaml` can configure runtime, planner, LLM providers,
//! actions, stores, context behavior, and observability settings.

mod actions;
mod loader;
mod providers;

pub use actions::{ActionInterfaceSpec, ActionSpec, ActionsConfig};
pub use loader::{
    load_actions_config, load_config, load_providers_config, ConfigError, ConfigManager,
    ConfigWatcher,
};
pub use providers::{
    ApiKeyError, BackendSpec, LegacyProviderSpec, ModelPolicy, ModelProfile, ProvidersConfig,
};

use serde::Deserialize;

/// Top-level configuration schema for Orchestral.
#[derive(Debug, Clone, Deserialize)]
pub struct OrchestralConfig {
    /// Config schema version.
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub app: AppConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub planner: PlannerConfig,
    #[serde(default)]
    pub interpreter: InterpreterConfig,
    #[serde(default)]
    pub context: ContextConfig,
    #[serde(default)]
    pub stores: StoresConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub providers: ProvidersConfig,
    #[serde(default)]
    pub actions: ActionsConfig,
}

fn default_version() -> u32 {
    1
}

impl Default for OrchestralConfig {
    fn default() -> Self {
        Self {
            version: default_version(),
            app: AppConfig::default(),
            runtime: RuntimeConfig::default(),
            planner: PlannerConfig::default(),
            interpreter: InterpreterConfig::default(),
            context: ContextConfig::default(),
            stores: StoresConfig::default(),
            observability: ObservabilityConfig::default(),
            providers: ProvidersConfig::default(),
            actions: ActionsConfig::default(),
        }
    }
}

impl OrchestralConfig {
    pub fn providers(&self) -> &ProvidersConfig {
        &self.providers
    }

    pub fn actions(&self) -> &ActionsConfig {
        &self.actions
    }
}

/// Backward-compatible alias.
pub type OrchestraConfig = OrchestralConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_app_name")]
    pub name: String,
    #[serde(default = "default_env")]
    pub environment: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: default_app_name(),
            environment: default_env(),
        }
    }
}

fn default_app_name() -> String {
    "orchestral".to_string()
}

fn default_env() -> String {
    "development".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_max_interactions")]
    pub max_interactions_per_thread: usize,
    #[serde(default = "default_true")]
    pub auto_cleanup: bool,
    #[serde(default = "default_concurrency_policy")]
    pub concurrency_policy: String,
    #[serde(default = "default_true")]
    pub strict_imports: bool,
    #[serde(default = "default_true")]
    pub strict_exports: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_interactions_per_thread: default_max_interactions(),
            auto_cleanup: true,
            concurrency_policy: default_concurrency_policy(),
            strict_imports: true,
            strict_exports: true,
        }
    }
}

fn default_max_interactions() -> usize {
    10
}

fn default_true() -> bool {
    true
}

fn default_concurrency_policy() -> String {
    "interrupt_and_start_new".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlannerConfig {
    #[serde(default = "default_planner_mode")]
    pub mode: String,
    /// Backend name for planner LLM calls.
    #[serde(default, alias = "provider")]
    pub backend: Option<String>,
    /// Optional model profile name.
    #[serde(default)]
    pub model_profile: Option<String>,
    /// Optional direct model override.
    #[serde(default)]
    pub model: Option<String>,
    /// Optional direct temperature override.
    #[serde(default)]
    pub temperature: Option<f32>,
    /// Whether planner can dynamically choose backend/model from request metadata.
    #[serde(default = "default_dynamic_model_selection")]
    pub dynamic_model_selection: bool,
    #[serde(default = "default_max_history")]
    pub max_history: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            mode: default_planner_mode(),
            backend: None,
            model_profile: None,
            model: None,
            temperature: None,
            dynamic_model_selection: default_dynamic_model_selection(),
            max_history: default_max_history(),
        }
    }
}

fn default_planner_mode() -> String {
    "llm".to_string()
}

fn default_max_history() -> usize {
    20
}

fn default_dynamic_model_selection() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct InterpreterConfig {
    #[serde(default = "default_interpreter_mode")]
    pub mode: String,
    /// Backend name for interpreter LLM calls.
    #[serde(default)]
    pub backend: Option<String>,
    /// Optional model profile name.
    #[serde(default)]
    pub model_profile: Option<String>,
    /// Optional direct model override.
    #[serde(default)]
    pub model: Option<String>,
    /// Optional direct temperature override.
    #[serde(default)]
    pub temperature: Option<f32>,
    /// Optional system prompt override.
    #[serde(default)]
    pub system_prompt: Option<String>,
}

impl Default for InterpreterConfig {
    fn default() -> Self {
        Self {
            mode: default_interpreter_mode(),
            backend: None,
            model_profile: None,
            model: None,
            temperature: None,
            system_prompt: None,
        }
    }
}

fn default_interpreter_mode() -> String {
    "auto".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContextConfig {
    #[serde(default = "default_history_limit")]
    pub history_limit: usize,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: usize,
    #[serde(default = "default_true")]
    pub include_history: bool,
    #[serde(default = "default_true")]
    pub include_references: bool,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            history_limit: default_history_limit(),
            max_tokens: default_max_tokens(),
            include_history: true,
            include_references: true,
        }
    }
}

fn default_history_limit() -> usize {
    50
}

fn default_max_tokens() -> usize {
    4096
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StoresConfig {
    #[serde(default)]
    pub event: StoreSpec,
    #[serde(default)]
    pub task: StoreSpec,
    #[serde(default)]
    pub reference: StoreSpec,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreSpec {
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default)]
    pub connection_url: Option<String>,
    /// Optional key prefix/namespace used by backend implementations.
    #[serde(default)]
    pub key_prefix: Option<String>,
}

impl Default for StoreSpec {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            connection_url: None,
            key_prefix: None,
        }
    }
}

fn default_backend() -> String {
    "in_memory".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub traces_enabled: bool,
    #[serde(default)]
    pub log_file: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            traces_enabled: false,
            log_file: None,
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}
