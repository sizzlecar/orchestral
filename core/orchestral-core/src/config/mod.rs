//! Strict configuration for the Agent Foundation runtime.

mod loader;
mod providers;

pub use loader::{load_config, load_providers_config, ConfigError};
pub use providers::{ApiKeyError, BackendSpec, ModelPolicy, ModelProfile, ProvidersConfig};

use std::collections::HashMap;

use serde::Deserialize;

/// One configuration surface for the Generic Agent composition root.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestralConfig {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub app: AppConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub providers: ProvidersConfig,
    #[serde(default)]
    pub tools: ToolsConfig,
    #[serde(default)]
    pub mcp: McpConfig,
    #[serde(default)]
    pub skills: SkillsConfig,
    #[serde(default)]
    pub journal: JournalConfig,
    #[serde(default)]
    pub artifacts: ArtifactConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

impl Default for OrchestralConfig {
    fn default() -> Self {
        Self {
            version: default_version(),
            app: AppConfig::default(),
            agent: AgentConfig::default(),
            providers: ProvidersConfig::default(),
            tools: ToolsConfig::default(),
            mcp: McpConfig::default(),
            skills: SkillsConfig::default(),
            journal: JournalConfig::default(),
            artifacts: ArtifactConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default = "default_app_name")]
    pub name: String,
    #[serde(default = "default_environment")]
    pub environment: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: default_app_name(),
            environment: default_environment(),
        }
    }
}

fn default_app_name() -> String {
    "orchestral".to_owned()
}

fn default_environment() -> String {
    "development".to_owned()
}

/// Generic Agent loop and model selection policy.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub model_profile: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default = "default_stream_buffer")]
    pub stream_buffer: usize,
    #[serde(default = "default_max_model_rounds")]
    pub max_model_rounds: u64,
    #[serde(default = "default_max_tool_calls")]
    pub max_tool_calls: u64,
    #[serde(default = "default_history_limit")]
    pub history_limit: usize,
    #[serde(default = "default_max_context_tokens")]
    pub max_context_tokens: u64,
    #[serde(default = "default_reserved_output_tokens")]
    pub reserved_output_tokens: u64,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            backend: None,
            model_profile: None,
            model: None,
            temperature: None,
            system_prompt: None,
            stream_buffer: default_stream_buffer(),
            max_model_rounds: default_max_model_rounds(),
            max_tool_calls: default_max_tool_calls(),
            history_limit: default_history_limit(),
            max_context_tokens: default_max_context_tokens(),
            reserved_output_tokens: default_reserved_output_tokens(),
        }
    }
}

fn default_stream_buffer() -> usize {
    128
}

fn default_max_model_rounds() -> u64 {
    8
}

fn default_max_tool_calls() -> u64 {
    32
}

fn default_history_limit() -> usize {
    128
}

fn default_max_context_tokens() -> u64 {
    128 * 1024
}

fn default_reserved_output_tokens() -> u64 {
    4 * 1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolsConfig {
    #[serde(default)]
    pub shell: ShellToolConfig,
    #[serde(default = "default_tool_timeout_ms")]
    pub max_timeout_ms: u64,
    #[serde(default = "default_tool_output_bytes")]
    pub max_output_bytes: u64,
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            shell: ShellToolConfig::default(),
            max_timeout_ms: default_tool_timeout_ms(),
            max_output_bytes: default_tool_output_bytes(),
        }
    }
}

fn default_tool_timeout_ms() -> u64 {
    30_000
}

fn default_tool_output_bytes() -> u64 {
    1024 * 1024
}

/// Shell and PTY are disabled until the Host grants explicit executable names.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellToolConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub allowed_programs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub servers: Vec<McpServerSpec>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            servers: Vec::new(),
        }
    }
}

/// Explicit Host MCP server declaration. MCP tools enter through Tool Protocol.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerSpec {
    pub name: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub required: bool,
    pub transport: McpTransportSpec,
    #[serde(default)]
    pub startup_timeout_ms: Option<u64>,
    #[serde(default)]
    pub tool_timeout_ms: Option<u64>,
    #[serde(default)]
    pub enabled_tools: Vec<String>,
    #[serde(default)]
    pub disabled_tools: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpTransportSpec {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    StreamableHttp {
        endpoint: String,
        #[serde(default)]
        credential_headers: HashMap<String, McpCredentialHeaderSpec>,
        #[serde(default)]
        max_frame_bytes: Option<usize>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpCredentialHeaderSpec {
    /// Environment variable resolved only by the application composition root.
    pub env: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub auto_discover: bool,
    #[serde(default = "default_max_active_skills")]
    pub max_active_skills: usize,
    #[serde(default)]
    pub directories: Vec<String>,
    #[serde(default)]
    pub trust_workspace: bool,
    #[serde(default)]
    pub allow_incompatible: bool,
}

impl Default for SkillsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_discover: true,
            max_active_skills: default_max_active_skills(),
            directories: Vec::new(),
            trust_workspace: false,
            allow_incompatible: false,
        }
    }
}

fn default_max_active_skills() -> usize {
    3
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalConfig {
    #[serde(default = "default_filesystem_backend")]
    pub backend: String,
    #[serde(default = "default_journal_root")]
    pub root_dir: String,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            backend: default_filesystem_backend(),
            root_dir: default_journal_root(),
        }
    }
}

fn default_journal_root() -> String {
    ".orchestral/agent-journal".to_owned()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactConfig {
    #[serde(default = "default_filesystem_backend")]
    pub backend: String,
    #[serde(default = "default_artifact_root")]
    pub root_dir: String,
    #[serde(default = "default_max_artifact_bytes")]
    pub max_bytes: u64,
    #[serde(default = "default_artifact_summary_chars")]
    pub summary_max_chars: usize,
}

impl Default for ArtifactConfig {
    fn default() -> Self {
        Self {
            backend: default_filesystem_backend(),
            root_dir: default_artifact_root(),
            max_bytes: default_max_artifact_bytes(),
            summary_max_chars: default_artifact_summary_chars(),
        }
    }
}

fn default_filesystem_backend() -> String {
    "filesystem".to_owned()
}

fn default_artifact_root() -> String {
    ".orchestral/artifacts".to_owned()
}

fn default_max_artifact_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_artifact_summary_chars() -> usize {
    512
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
    "info".to_owned()
}

fn default_true() -> bool {
    true
}
