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
    /// Optional Host ceiling. Omit it to let a progressing turn continue until
    /// it settles or reaches an explicit Run resource boundary.
    #[serde(default)]
    pub max_model_steps: Option<u64>,
    #[serde(default)]
    pub max_tool_calls: Option<u64>,
    #[serde(default = "default_history_limit")]
    pub history_limit: usize,
    #[serde(default = "default_max_context_tokens")]
    pub max_context_tokens: u64,
    #[serde(default = "default_reserved_output_tokens")]
    pub reserved_output_tokens: u64,
    #[serde(default)]
    pub compaction: AgentCompactionConfig,
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
            max_model_steps: None,
            max_tool_calls: None,
            history_limit: default_history_limit(),
            max_context_tokens: default_max_context_tokens(),
            reserved_output_tokens: default_reserved_output_tokens(),
            compaction: AgentCompactionConfig::default(),
        }
    }
}

/// Host-owned Session compaction policy for the Generic Agent composition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCompactionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_compaction_minimum_source_records")]
    pub minimum_source_records: usize,
    #[serde(default = "default_compaction_keep_recent_records")]
    pub keep_recent_records: usize,
    #[serde(default = "default_compaction_summary_chars")]
    pub summary_max_chars: usize,
}

impl Default for AgentCompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            minimum_source_records: default_compaction_minimum_source_records(),
            keep_recent_records: default_compaction_keep_recent_records(),
            summary_max_chars: default_compaction_summary_chars(),
        }
    }
}

fn default_compaction_minimum_source_records() -> usize {
    32
}

fn default_compaction_keep_recent_records() -> usize {
    16
}

fn default_compaction_summary_chars() -> usize {
    16 * 1024
}

fn default_stream_buffer() -> usize {
    128
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
    pub exec: ExecToolConfig,
    #[serde(default = "default_tool_timeout_ms")]
    pub max_timeout_ms: u64,
    #[serde(default = "default_tool_output_bytes")]
    pub max_output_bytes: u64,
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            exec: ExecToolConfig::default(),
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

/// Unified command execution. The Host resolves one command shell; child
/// process safety is enforced by the effect sandbox rather than a model-facing
/// executable allowlist.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecToolConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub shell: Option<String>,
    /// Allows an invocation to request exact user approval for execution
    /// outside the default OS sandbox. This is a hard Host ceiling and is
    /// disabled by default in the library configuration.
    #[serde(default)]
    pub allow_host_execution: bool,
    /// Exact `host:port` destinations available inside the default sandbox.
    /// Empty denies sandboxed network access. Approved Host execution, when
    /// explicitly enabled above, carries its own unrestricted network scope.
    #[serde(default)]
    pub network_targets: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Explicit MCP manifest files composed by the application Host. Files
    /// are never auto-executed merely because they exist in a repository.
    #[serde(default)]
    pub import_files: Vec<String>,
    #[serde(default)]
    pub servers: Vec<McpServerSpec>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            import_files: Vec::new(),
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
        /// Permit the registered launcher to form a process tree inside this
        /// MCP server's filesystem/network sandbox (needed by npx/uvx/sh).
        #[serde(default = "default_true")]
        allow_child_processes: bool,
        /// Permit the registered local MCP process to ask the operating
        /// system to open URLs or application UI. User-level registrations
        /// enable this by default; project manifests remain denied.
        #[serde(default)]
        allow_host_ui: bool,
        /// Working directory exposed to the local MCP process. Relative paths
        /// are resolved from the Agent workspace by the application Host.
        #[serde(default)]
        cwd: Option<String>,
        /// Additional read-only filesystem roots required by the MCP server.
        /// The resolved cwd remains readable even when this list is empty.
        #[serde(default)]
        readable_roots: Vec<String>,
        /// Additional writable filesystem roots. The application Host always
        /// supplies a private MCP runtime directory separately.
        #[serde(default)]
        writable_roots: Vec<String>,
        /// Exact `host:port` destinations available to this local process.
        /// Mutually exclusive with unrestricted Host network access.
        #[serde(default)]
        network_targets: Vec<String>,
        /// Permit the explicitly registered local MCP process to use the Host
        /// network. User-level `mcp add` enables this by default, matching the
        /// trust boundary established by registering the process itself.
        #[serde(default)]
        allow_unrestricted_network: bool,
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
    #[serde(default)]
    pub directories: Vec<String>,
}

impl Default for SkillsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            auto_discover: true,
            directories: Vec::new(),
        }
    }
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
