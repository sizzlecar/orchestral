use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

use orchestral_core::agent_protocol::wire::{Digest, ToolActivityEvidence};
use orchestral_core::mcp_protocol::{
    McpProtocolEra, McpServerId, McpServerSnapshot, McpToolAnnotations, McpToolSnapshot,
    McpTransportAuthority, McpTransportCancellation, McpTransportConnection, McpTransportError,
    McpTransportFactory, McpTransportKind, McpTransportRequest, MCP_LATEST_LEGACY_PROTOCOL,
    MCP_STATELESS_PROTOCOL_2026_07_28,
};
use orchestral_core::tool_protocol::{
    ApprovalCapabilityStore, CapabilityRequest, CapabilitySelector, EffectScope, ModelToolSchema,
    ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency, ToolInvocation, ToolOperationPlan,
    ToolOperationRisk, ToolOutcome, ToolRestriction,
};
use tokio_util::sync::CancellationToken;

use crate::tool_runtime::{
    GuardedToolExecution, GuardedToolExecutor, GuardedToolRuntime, ToolRuntimeError,
};
use crate::tools::shell_sandbox::{
    normalize_network_targets, sandbox_command, SandboxNetworkAccess, ShellSandboxPolicy,
};

const DEFAULT_MCP_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_SAFE_MCP_HEADER_INTEGER: u64 = 9_007_199_254_740_991;
const MCP_TRANSPORT_CLOSE_TIMEOUT: Duration = Duration::from_millis(750);
const MCP_PROCESS_REAP_TIMEOUT: Duration = Duration::from_millis(500);
const MCP_STDERR_CAPTURE_BYTES: usize = 16 * 1024;
pub const MCP_STDIO_SANDBOX_PROFILE: &str = "orchestral.mcp.stdio.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpHeaderValueKind {
    String,
    Integer,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct McpHeaderBinding {
    property_path: Vec<String>,
    header_suffix: String,
    value_kind: McpHeaderValueKind,
}

fn compile_mcp_header_bindings(schema: &Value) -> Result<Vec<McpHeaderBinding>, String> {
    if !schema.is_object() {
        return Err("MCP Tool inputSchema must be an object".to_owned());
    }
    let mut bindings = Vec::new();
    let mut header_names = BTreeSet::new();
    scan_mcp_header_annotations(schema, &[], true, &mut header_names, &mut bindings)?;
    bindings.sort_by(|left, right| {
        left.header_suffix
            .to_ascii_lowercase()
            .cmp(&right.header_suffix.to_ascii_lowercase())
            .then_with(|| left.property_path.cmp(&right.property_path))
    });
    Ok(bindings)
}

fn scan_mcp_header_annotations(
    value: &Value,
    property_path: &[String],
    statically_reachable: bool,
    header_names: &mut BTreeSet<String>,
    bindings: &mut Vec<McpHeaderBinding>,
) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if let Some(annotation) = object.get("x-mcp-header") {
                if !statically_reachable || property_path.is_empty() {
                    return Err(
                        "x-mcp-header is not on a property statically reachable from the schema root"
                            .to_owned(),
                    );
                }
                let header_suffix = annotation.as_str().ok_or_else(|| {
                    "x-mcp-header must be a non-empty HTTP field-name token".to_owned()
                })?;
                if !is_mcp_http_token(header_suffix) {
                    return Err(format!(
                        "x-mcp-header '{header_suffix}' is not an HTTP field-name token"
                    ));
                }
                if !header_names.insert(header_suffix.to_ascii_lowercase()) {
                    return Err(format!(
                        "x-mcp-header '{header_suffix}' is not case-insensitively unique"
                    ));
                }
                let value_kind = match object.get("type").and_then(Value::as_str) {
                    Some("string") => McpHeaderValueKind::String,
                    Some("integer") => McpHeaderValueKind::Integer,
                    Some("boolean") => McpHeaderValueKind::Boolean,
                    _ => {
                        return Err(format!(
                        "x-mcp-header '{header_suffix}' must annotate string, integer, or boolean"
                    ))
                    }
                };
                bindings.push(McpHeaderBinding {
                    property_path: property_path.to_vec(),
                    header_suffix: header_suffix.to_owned(),
                    value_kind,
                });
            }

            for (keyword, child) in object {
                if keyword == "properties" && statically_reachable {
                    let properties = child.as_object().ok_or_else(|| {
                        "JSON Schema properties containing x-mcp-header must be an object"
                            .to_owned()
                    })?;
                    for (name, property_schema) in properties {
                        let mut child_path = property_path.to_vec();
                        child_path.push(name.clone());
                        scan_mcp_header_annotations(
                            property_schema,
                            &child_path,
                            true,
                            header_names,
                            bindings,
                        )?;
                    }
                } else if keyword != "x-mcp-header" {
                    scan_mcp_header_annotations(
                        child,
                        property_path,
                        false,
                        header_names,
                        bindings,
                    )?;
                }
            }
        }
        Value::Array(values) => {
            for child in values {
                scan_mcp_header_annotations(child, property_path, false, header_names, bindings)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_mcp_http_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn build_mcp_parameter_headers(
    bindings: &[McpHeaderBinding],
    arguments: &Value,
) -> Result<BTreeMap<String, String>, String> {
    let mut headers = BTreeMap::new();
    for binding in bindings {
        let mut value = arguments;
        let mut missing = false;
        for segment in &binding.property_path {
            let Some(next) = value.as_object().and_then(|object| object.get(segment)) else {
                missing = true;
                break;
            };
            value = next;
        }
        if missing || value.is_null() {
            continue;
        }
        let value = match binding.value_kind {
            McpHeaderValueKind::String => value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| invalid_mcp_header_value(binding))?,
            McpHeaderValueKind::Boolean => value
                .as_bool()
                .map(|value| value.to_string())
                .ok_or_else(|| invalid_mcp_header_value(binding))?,
            McpHeaderValueKind::Integer => canonical_safe_mcp_integer(value)
                .ok_or_else(|| invalid_mcp_header_value(binding))?,
        };
        headers.insert(binding.header_suffix.clone(), value);
    }
    Ok(headers)
}

fn canonical_safe_mcp_integer(value: &Value) -> Option<String> {
    if let Some(value) = value.as_i64() {
        if value.unsigned_abs() <= MAX_SAFE_MCP_HEADER_INTEGER {
            return Some(value.to_string());
        }
    } else if let Some(value) = value.as_u64() {
        if value <= MAX_SAFE_MCP_HEADER_INTEGER {
            return Some(value.to_string());
        }
    }
    None
}

fn invalid_mcp_header_value(binding: &McpHeaderBinding) -> String {
    format!(
        "MCP Tool argument at '{}' does not match x-mcp-header '{}' primitive type or safe integer range",
        binding.property_path.join("."),
        binding.header_suffix
    )
}

/// Immutable Host filesystem boundary for one stdio MCP server process.
#[derive(Debug, Clone)]
pub struct StdioMcpSandboxPolicy {
    cwd: PathBuf,
    readable_roots: BTreeSet<PathBuf>,
    writable_roots: BTreeSet<PathBuf>,
    network_targets: BTreeSet<String>,
    allow_unrestricted_network: bool,
    private_runtime_home: Option<PathBuf>,
    allow_child_processes: bool,
    allow_host_ui: bool,
}

impl StdioMcpSandboxPolicy {
    pub fn workspace(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            cwd: root.clone(),
            readable_roots: BTreeSet::from([root.clone()]),
            writable_roots: BTreeSet::from([root]),
            network_targets: BTreeSet::new(),
            allow_unrestricted_network: false,
            private_runtime_home: None,
            allow_child_processes: false,
            allow_host_ui: false,
        }
    }

    /// Builds one explicit local-MCP process boundary. The executable itself
    /// is bound separately by [`StdioMcpTransportFactory`]; these roots cover
    /// only the server's data access and never widen generic command Tools.
    pub fn scoped(
        cwd: impl Into<PathBuf>,
        readable_roots: BTreeSet<PathBuf>,
        writable_roots: BTreeSet<PathBuf>,
        network_targets: BTreeSet<String>,
    ) -> Self {
        Self {
            cwd: cwd.into(),
            readable_roots,
            writable_roots,
            network_targets,
            allow_unrestricted_network: false,
            private_runtime_home: None,
            allow_child_processes: false,
            allow_host_ui: false,
        }
    }

    /// Allows launchers such as npx/uvx/sh to form a process tree. Every
    /// descendant remains confined by this server's filesystem/network policy.
    pub fn with_child_processes(mut self, allow: bool) -> Self {
        self.allow_child_processes = allow;
        self
    }

    /// Allows this registered MCP transport to invoke the operating system's
    /// URL/application opener. The MCP still owns the protocol (for example,
    /// OAuth); the Host only materializes the declared OS capability.
    pub fn with_host_ui(mut self, allow: bool) -> Self {
        self.allow_host_ui = allow;
        self
    }

    /// Uses the Host network for an explicitly registered local MCP process.
    /// This is transport trust established by configuration, not authority
    /// supplied by a model Tool call.
    pub fn with_unrestricted_network(mut self, allow: bool) -> Self {
        self.allow_unrestricted_network = allow;
        self
    }

    /// Supplies HOME/TMP-style process state without inheriting the user's
    /// ambient home directory. The directory must remain inside one declared
    /// writable root after canonicalization.
    pub fn with_private_runtime_home(mut self, root: impl Into<PathBuf>) -> Self {
        self.private_runtime_home = Some(root.into());
        self
    }

    fn normalize(self) -> Result<Self, McpToolsAdapterError> {
        let cwd = canonical_mcp_directory(&self.cwd, "cwd")?;
        let mut readable_roots = self
            .readable_roots
            .iter()
            .map(|root| canonical_mcp_directory(root, "readable root"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let writable_roots = self
            .writable_roots
            .iter()
            .map(|root| canonical_mcp_directory(root, "writable root"))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let network_targets = normalize_network_targets(&self.network_targets)
            .map_err(McpToolsAdapterError::InvalidConfig)?;
        if self.allow_unrestricted_network && !network_targets.is_empty() {
            return Err(McpToolsAdapterError::InvalidConfig(
                "MCP stdio network authority must be exact targets or unrestricted, not both"
                    .to_owned(),
            ));
        }
        if self.allow_host_ui && !self.allow_child_processes {
            return Err(McpToolsAdapterError::InvalidConfig(
                "MCP stdio Host UI access requires child-process authority".to_owned(),
            ));
        }
        let private_runtime_home = self
            .private_runtime_home
            .as_deref()
            .map(|root| canonical_mcp_directory(root, "private runtime home"))
            .transpose()?;
        if let Some(home) = &private_runtime_home {
            readable_roots.insert(home.clone());
        }
        if readable_roots.is_empty()
            || writable_roots.is_empty()
            || !readable_roots.iter().any(|root| cwd.starts_with(root))
            || private_runtime_home.as_ref().is_some_and(|home| {
                !writable_roots
                    .iter()
                    .any(|writable| home == writable || home.starts_with(writable))
            })
        {
            return Err(McpToolsAdapterError::InvalidConfig(
                "MCP stdio sandbox requires readable/writable roots and a readable cwd".to_owned(),
            ));
        }
        Ok(Self {
            cwd,
            readable_roots,
            writable_roots,
            network_targets,
            allow_unrestricted_network: self.allow_unrestricted_network,
            private_runtime_home,
            allow_child_processes: self.allow_child_processes,
            allow_host_ui: self.allow_host_ui,
        })
    }
}

fn canonical_mcp_directory(
    path: &std::path::Path,
    label: &str,
) -> Result<PathBuf, McpToolsAdapterError> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        McpToolsAdapterError::InvalidConfig(format!(
            "canonicalize MCP stdio sandbox {label} '{}' failed: {error}",
            path.display()
        ))
    })?;
    if !canonical.is_dir() {
        return Err(McpToolsAdapterError::InvalidConfig(format!(
            "MCP stdio sandbox {label} '{}' is not a directory",
            path.display()
        )));
    }
    Ok(canonical)
}

/// Built-in stdio transport factory. External transports implement the core
/// [`McpTransportFactory`] SPI from a plugin and are injected by the Host.
#[derive(Debug, Clone)]
pub struct StdioMcpTransportFactory {
    program: PathBuf,
    args: Vec<String>,
    environment: BTreeMap<String, String>,
    sandbox: StdioMcpSandboxPolicy,
    authority: McpTransportAuthority,
}

impl StdioMcpTransportFactory {
    pub fn new(
        program: PathBuf,
        args: Vec<String>,
        environment: BTreeMap<String, String>,
        sandbox: StdioMcpSandboxPolicy,
    ) -> Result<Self, McpToolsAdapterError> {
        let canonical_program = program.canonicalize().ok();
        if !program.is_absolute()
            || !program.is_file()
            || canonical_program.as_ref() != Some(&program)
            || environment.keys().any(|name| {
                name.trim().is_empty() || name.contains('=') || name.chars().any(char::is_control)
            })
        {
            return Err(McpToolsAdapterError::InvalidConfig(
                "invalid guarded MCP stdio transport".to_owned(),
            ));
        }
        let sandbox = sandbox.normalize()?;
        let mut effect_scopes = BTreeSet::from([
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::ExternalSideEffect,
        ]);
        if !environment.is_empty() {
            effect_scopes.insert(EffectScope::SecretRead);
        }
        if sandbox.allow_unrestricted_network || !sandbox.network_targets.is_empty() {
            effect_scopes.insert(EffectScope::Network);
        }
        let binding = json!({
            "transport": "stdio",
            "program": program.to_string_lossy(),
            "args": args,
            "environmentNames": environment.keys().collect::<Vec<_>>(),
            "cwd": sandbox.cwd.to_string_lossy(),
            "readableRoots": sandbox.readable_roots.iter().map(|root| root.to_string_lossy()).collect::<Vec<_>>(),
            "writableRoots": sandbox.writable_roots.iter().map(|root| root.to_string_lossy()).collect::<Vec<_>>(),
            "networkTargets": &sandbox.network_targets,
            "allowUnrestrictedNetwork": sandbox.allow_unrestricted_network,
            "privateRuntimeHome": sandbox.private_runtime_home.as_ref().map(|path| path.to_string_lossy()),
            "allowChildProcesses": sandbox.allow_child_processes,
            "allowHostUi": sandbox.allow_host_ui,
            "sandboxProfile": MCP_STDIO_SANDBOX_PROFILE,
            "maxFrameBytes": DEFAULT_MCP_MAX_FRAME_BYTES,
        });
        let binding_digest = Digest::sha256(
            serde_jcs::to_vec(&binding)
                .map_err(|error| McpToolsAdapterError::InvalidConfig(error.to_string()))?,
        );
        let authority = McpTransportAuthority {
            kind: McpTransportKind::Stdio,
            binding_digest,
            effect_scopes,
            process_programs: BTreeSet::from([program.to_string_lossy().to_string()]),
            allow_child_processes: sandbox.allow_child_processes,
            allow_host_ui: sandbox.allow_host_ui,
            filesystem_read_roots: sandbox
                .readable_roots
                .iter()
                .map(|root| root.to_string_lossy().into_owned())
                .collect(),
            filesystem_write_roots: sandbox
                .writable_roots
                .iter()
                .map(|root| root.to_string_lossy().into_owned())
                .collect(),
            sandbox_profiles: BTreeSet::from([MCP_STDIO_SANDBOX_PROFILE.to_owned()]),
            network_targets: sandbox.network_targets.clone(),
            allow_unrestricted_network: sandbox.allow_unrestricted_network,
            environment_variables: environment.keys().cloned().collect(),
            credential_references: BTreeSet::new(),
        };
        authority
            .validate()
            .map_err(|error| McpToolsAdapterError::InvalidConfig(error.to_string()))?;
        Ok(Self {
            program,
            args,
            environment,
            sandbox,
            authority,
        })
    }
}

#[async_trait]
impl McpTransportFactory for StdioMcpTransportFactory {
    fn authority(&self) -> &McpTransportAuthority {
        &self.authority
    }

    async fn connect(&self) -> Result<Box<dyn McpTransportConnection>, McpTransportError> {
        let command = sandbox_command(
            self.program.to_string_lossy().into_owned(),
            self.args.clone(),
            &self.sandbox.cwd,
            &ShellSandboxPolicy {
                readable_roots: self.sandbox.readable_roots.iter().cloned().collect(),
                readable_files: Vec::new(),
                writable_roots: self.sandbox.writable_roots.iter().cloned().collect(),
                allow_child_processes: self.sandbox.allow_child_processes,
                allow_host_ui: self.sandbox.allow_host_ui,
                launcher_programs: vec![self.program.clone()],
                network: if self.sandbox.allow_unrestricted_network {
                    SandboxNetworkAccess::Unrestricted
                } else if self.sandbox.network_targets.is_empty() {
                    SandboxNetworkAccess::Disabled
                } else {
                    SandboxNetworkAccess::ExactTargets(self.sandbox.network_targets.clone())
                },
                linux_bwrap_path: None,
            },
        )
        .map_err(McpTransportError::Transport)?;
        let mut environment = self
            .environment
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(home) = &self.sandbox.private_runtime_home {
            let home = home.to_string_lossy().into_owned();
            environment
                .entry("HOME".to_owned())
                .or_insert_with(|| home.clone());
            environment
                .entry("USERPROFILE".to_owned())
                .or_insert_with(|| home.clone());
            environment.entry("TMPDIR".to_owned()).or_insert(home);
        }
        environment.extend(command.env);
        StdioMcpTransport::connect(
            &command.program,
            &command.args,
            &environment,
            &self.sandbox.cwd,
            command.backend_starts_new_session,
        )
        .await
        .map(|transport| Box::new(transport) as Box<dyn McpTransportConnection>)
        .map_err(McpTransportError::Transport)
    }
}

/// One explicitly configured MCP Tool provider. Discovery and invocation use
/// the same immutable transport authority for the lifetime of this registry.
#[derive(Clone)]
pub struct GuardedMcpServerConfig {
    pub server_id: McpServerId,
    pub required: bool,
    pub transport: Arc<dyn McpTransportFactory>,
    pub startup_timeout: Duration,
    pub tool_timeout: Duration,
    pub enabled_tools: BTreeSet<String>,
    pub disabled_tools: BTreeSet<String>,
}

impl std::fmt::Debug for GuardedMcpServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GuardedMcpServerConfig")
            .field("server_id", &self.server_id)
            .field("required", &self.required)
            .field("transport_authority", self.transport.authority())
            .field("startup_timeout", &self.startup_timeout)
            .field("tool_timeout", &self.tool_timeout)
            .field("enabled_tools", &self.enabled_tools)
            .field("disabled_tools", &self.disabled_tools)
            .finish()
    }
}

impl GuardedMcpServerConfig {
    pub fn validate(&self) -> Result<(), McpToolsAdapterError> {
        if self.server_id.is_empty()
            || self.startup_timeout.is_zero()
            || self.tool_timeout.is_zero()
            || self
                .enabled_tools
                .iter()
                .chain(self.disabled_tools.iter())
                .any(|name| name.trim().is_empty())
        {
            return Err(McpToolsAdapterError::InvalidConfig(format!(
                "invalid guarded MCP configuration for '{}'",
                self.server_id
            )));
        }
        self.transport.authority().validate().map_err(|error| {
            McpToolsAdapterError::InvalidConfig(format!(
                "invalid MCP transport authority for '{}': {error}",
                self.server_id
            ))
        })?;
        Ok(())
    }

    fn allows_tool(&self, name: &str) -> bool {
        !name.trim().is_empty()
            && (self.enabled_tools.is_empty() || self.enabled_tools.contains(name))
            && !self.disabled_tools.contains(name)
    }

    pub fn effect_scopes(&self) -> BTreeSet<EffectScope> {
        self.transport.authority().effect_scopes.clone()
    }

    pub fn allowed_programs(&self) -> BTreeSet<String> {
        self.transport.authority().process_programs.clone()
    }

    pub fn allows_child_processes(&self) -> bool {
        self.transport.authority().allow_child_processes
    }

    pub fn allowed_network_targets(&self) -> BTreeSet<String> {
        self.transport.authority().network_targets.clone()
    }

    pub fn allows_unrestricted_network(&self) -> bool {
        self.transport.authority().allow_unrestricted_network
    }

    pub fn filesystem_read_roots(&self) -> BTreeSet<String> {
        self.transport.authority().filesystem_read_roots.clone()
    }

    pub fn filesystem_write_roots(&self) -> BTreeSet<String> {
        self.transport.authority().filesystem_write_roots.clone()
    }

    pub fn sandbox_profiles(&self) -> BTreeSet<String> {
        self.transport.authority().sandbox_profiles.clone()
    }

    pub fn environment_names(&self) -> BTreeSet<String> {
        self.transport.authority().environment_variables.clone()
    }

    pub fn credential_references(&self) -> BTreeSet<String> {
        self.transport.authority().credential_references.clone()
    }

    fn transport_kind(&self) -> McpTransportKind {
        self.transport.authority().kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpServerHealth {
    Connecting,
    Ready,
    Degraded,
    Closed,
}

impl McpServerHealth {
    fn encode(self) -> u64 {
        match self {
            Self::Connecting => 0,
            Self::Ready => 1,
            Self::Degraded => 2,
            Self::Closed => 3,
        }
    }

    fn decode(value: u64) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Degraded,
            3 => Self::Closed,
            _ => Self::Connecting,
        }
    }
}

/// Owns exactly one negotiated transport handle for all Tools on one MCP server.
pub struct McpServerConnectionManager {
    config: GuardedMcpServerConfig,
    snapshot: McpServerSnapshot,
    parameter_headers: BTreeMap<String, Vec<McpHeaderBinding>>,
    session: tokio::sync::Mutex<Option<McpTransportSession>>,
    connection_generation: AtomicU64,
    health: AtomicU64,
}

impl McpServerConnectionManager {
    async fn connect(
        config: GuardedMcpServerConfig,
        cancellation: CancellationToken,
    ) -> Result<Arc<Self>, McpToolsAdapterError> {
        config.validate()?;
        let (session, snapshot, parameter_headers) =
            discover_guarded_mcp_session(&config, &cancellation).await?;
        Ok(Arc::new(Self {
            config,
            snapshot,
            parameter_headers,
            session: tokio::sync::Mutex::new(Some(session)),
            connection_generation: AtomicU64::new(1),
            health: AtomicU64::new(McpServerHealth::Ready.encode()),
        }))
    }

    pub fn config(&self) -> &GuardedMcpServerConfig {
        &self.config
    }

    pub fn snapshot(&self) -> &McpServerSnapshot {
        &self.snapshot
    }

    pub fn connection_generation(&self) -> u64 {
        self.connection_generation.load(Ordering::Acquire)
    }

    pub fn health(&self) -> McpServerHealth {
        McpServerHealth::decode(self.health.load(Ordering::Acquire))
    }

    async fn invoke(
        &self,
        tool_name: &str,
        arguments: Value,
        cancellation: CancellationToken,
    ) -> Result<Value, GuardedMcpCallError> {
        if !self.config.allows_tool(tool_name)
            || !self
                .snapshot
                .tools
                .iter()
                .any(|tool| tool.name == tool_name)
        {
            return Err(GuardedMcpCallError::Rejected(format!(
                "MCP Tool '{tool_name}' is outside the pinned Host snapshot/filter"
            )));
        }
        let parameter_headers = build_mcp_parameter_headers(
            self.parameter_headers
                .get(tool_name)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            &arguments,
        )
        .map_err(GuardedMcpCallError::Rejected)?;
        let mut guard = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(GuardedMcpCallError::Cancelled),
            guard = self.session.lock() => guard,
        };
        if guard.is_none() {
            self.health
                .store(McpServerHealth::Connecting.encode(), Ordering::Release);
            let (mut session, current_snapshot, current_parameter_headers) =
                discover_guarded_mcp_session(&self.config, &cancellation)
                    .await
                    .map_err(|error| GuardedMcpCallError::Failed(error.to_string()))?;
            if current_snapshot.revision != self.snapshot.revision
                || current_parameter_headers != self.parameter_headers
            {
                let _ = session.shutdown().await;
                self.health
                    .store(McpServerHealth::Degraded.encode(), Ordering::Release);
                return Err(GuardedMcpCallError::Failed(
                    "MCP Tool catalog changed after reconnect; the pinned Host snapshot is stale"
                        .to_owned(),
                ));
            }
            self.connection_generation.fetch_add(1, Ordering::AcqRel);
            *guard = Some(session);
        }
        let request = {
            let session = guard.as_mut().expect("MCP session was initialized");
            let request_id = session.next_request_id();
            let exchange_cancellation = cancellation.child_token();
            enum Wait {
                Response(Result<Value, String>),
                Cancelled,
                TimedOut,
            }
            let wait = tokio::select! {
                biased;
                _ = cancellation.cancelled() => Wait::Cancelled,
                result = timeout(
                    self.config.tool_timeout,
                    session.request(
                        "tools/call",
                        json!({"name": tool_name, "arguments": arguments}),
                        parameter_headers,
                        exchange_cancellation.clone(),
                    ),
                ) => match result {
                    Ok(response) => Wait::Response(response),
                    Err(_) => Wait::TimedOut,
                }
            };
            match wait {
                Wait::Response(Ok(value)) => validate_mcp_call_result(
                    session
                        .negotiated_protocol()
                        .map_err(|error| GuardedMcpCallError::Failed(error.to_string()))?,
                    value,
                ),
                Wait::Response(Err(error)) => Err(GuardedMcpCallError::UnknownEffect(error)),
                Wait::Cancelled => {
                    exchange_cancellation.cancel();
                    session
                        .cancel_request(request_id, "Agent Run cancelled")
                        .await;
                    Err(GuardedMcpCallError::UnknownEffect(
                        "MCP call was cancelled after dispatch; remote effect is unknown"
                            .to_owned(),
                    ))
                }
                Wait::TimedOut => {
                    exchange_cancellation.cancel();
                    session
                        .cancel_request(request_id, "Host deadline exceeded")
                        .await;
                    Err(GuardedMcpCallError::UnknownEffect(
                        format!(
                            "MCP server '{}' Tool '{}' timed out after {} ms after dispatch; remote effect is unknown",
                            self.config.server_id,
                            tool_name,
                            self.config.tool_timeout.as_millis()
                        ),
                    ))
                }
            }
        };
        if request
            .as_ref()
            .is_err_and(GuardedMcpCallError::invalidates_session)
        {
            self.health
                .store(McpServerHealth::Degraded.encode(), Ordering::Release);
            if let Some(mut session) = guard.take() {
                let _ = session.shutdown().await;
            }
        } else {
            self.health
                .store(McpServerHealth::Ready.encode(), Ordering::Release);
        }
        request
    }

    pub async fn shutdown(&self) {
        let mut guard = self.session.lock().await;
        if let Some(mut session) = guard.take() {
            let _ = session.shutdown().await;
        }
        self.health
            .store(McpServerHealth::Closed.encode(), Ordering::Release);
    }
}

async fn connect_guarded_mcp_session(
    config: &GuardedMcpServerConfig,
    cancellation: &CancellationToken,
) -> Result<McpTransportSession, McpToolsAdapterError> {
    let mut session = spawn_guarded_mcp_session(config, cancellation).await?;
    let probe = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            let _ = session.shutdown().await;
            return Err(McpToolsAdapterError::Cancelled);
        }
        result = timeout(
            config.startup_timeout,
            session.probe_stateless(cancellation.child_token()),
        ) => result,
    };
    match probe {
        Ok(Ok(true)) => return Ok(session),
        Ok(Ok(false)) if config.transport_kind() == McpTransportKind::Stdio => {}
        Ok(Ok(false)) => {
            let _ = session.shutdown().await;
            return Err(McpToolsAdapterError::Protocol(
                "Streamable HTTP endpoint does not support MCP 2026-07-28".to_owned(),
            ));
        }
        Ok(Err(error)) if error.is_transport() => {
            let _ = session.shutdown().await;
            if config.transport_kind() != McpTransportKind::Stdio {
                return Err(mcp_request_adapter_error(error));
            }
            session = spawn_guarded_mcp_session(config, cancellation).await?;
        }
        Ok(Err(error)) => {
            let _ = session.shutdown().await;
            return Err(mcp_request_adapter_error(error));
        }
        Err(_) => {
            let _ = session.shutdown().await;
            if config.transport_kind() != McpTransportKind::Stdio {
                return Err(McpToolsAdapterError::Transport(
                    "MCP server/discover timed out".to_owned(),
                ));
            }
            // A legacy stdio server may wait forever for initialize instead of
            // rejecting server/discover. Restart it before legacy fallback.
            session = spawn_guarded_mcp_session(config, cancellation).await?;
        }
    }
    let initialized = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            let _ = session.shutdown().await;
            return Err(McpToolsAdapterError::Cancelled);
        }
        result = timeout(
            config.startup_timeout,
            session.initialize_guarded_legacy(cancellation.child_token()),
        ) => result,
    };
    match initialized {
        Ok(Ok(())) => Ok(session),
        Ok(Err(error)) => {
            let _ = session.shutdown().await;
            Err(mcp_request_adapter_error(error))
        }
        Err(_) => {
            let _ = session.shutdown().await;
            Err(McpToolsAdapterError::Transport(
                "legacy MCP initialize timed out".to_owned(),
            ))
        }
    }
}

async fn spawn_guarded_mcp_session(
    config: &GuardedMcpServerConfig,
    cancellation: &CancellationToken,
) -> Result<McpTransportSession, McpToolsAdapterError> {
    let connection = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(McpToolsAdapterError::Cancelled),
        result = timeout(config.startup_timeout, config.transport.connect()) => match result {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => return Err(mcp_request_adapter_error(error)),
            Err(_) => {
                return Err(McpToolsAdapterError::Transport(
                    "MCP transport connect timed out".to_owned(),
                ));
            }
        },
    };
    if connection.kind() != config.transport_kind() {
        let _ = timeout(MCP_TRANSPORT_CLOSE_TIMEOUT, connection.close()).await;
        return Err(McpToolsAdapterError::InvalidConfig(format!(
            "MCP transport factory for '{}' returned a connection of the wrong kind",
            config.server_id
        )));
    }
    Ok(McpTransportSession::new(connection))
}

async fn discover_guarded_mcp_session(
    config: &GuardedMcpServerConfig,
    cancellation: &CancellationToken,
) -> Result<
    (
        McpTransportSession,
        McpServerSnapshot,
        BTreeMap<String, Vec<McpHeaderBinding>>,
    ),
    McpToolsAdapterError,
> {
    let mut session = connect_guarded_mcp_session(config, cancellation).await?;
    let listed = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            let _ = session.shutdown().await;
            return Err(McpToolsAdapterError::Cancelled);
        }
        result = timeout(
            config.startup_timeout,
            list_all_mcp_tools(&mut session, cancellation),
        ) => result,
    };
    let listed = match listed {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            let _ = session.shutdown().await;
            return Err(McpToolsAdapterError::Transport(error));
        }
        Err(_) => {
            let _ = session.shutdown().await;
            return Err(McpToolsAdapterError::Transport(
                "MCP tools/list timed out".to_owned(),
            ));
        }
    };
    let negotiated = session
        .negotiated_protocol()
        .map_err(mcp_request_adapter_error)?
        .clone();
    match parse_guarded_tool_snapshot(config, &negotiated, &listed) {
        Ok((snapshot, parameter_headers)) => Ok((session, snapshot, parameter_headers)),
        Err(error) => {
            let _ = session.shutdown().await;
            Err(error)
        }
    }
}

async fn list_all_mcp_tools(
    session: &mut McpTransportSession,
    cancellation: &CancellationToken,
) -> Result<Value, String> {
    const MAX_PAGES: usize = 256;
    let stateless = session
        .negotiated_protocol()
        .map_err(|error| error.to_string())?
        .era
        == McpProtocolEra::Stateless;
    let mut tools = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    for _ in 0..MAX_PAGES {
        let params = cursor
            .as_ref()
            .map(|cursor| json!({"cursor": cursor}))
            .unwrap_or_else(|| json!({}));
        let result = session
            .request(
                "tools/list",
                params,
                BTreeMap::new(),
                cancellation.child_token(),
            )
            .await?;
        match result.get("resultType").and_then(Value::as_str) {
            Some("input_required") => {
                return Err(
                    "MCP tools/list requested unsupported multi-round-trip input".to_owned(),
                )
            }
            Some("complete") => {}
            None if !stateless => {}
            Some(other) => {
                return Err(format!(
                    "MCP tools/list returned unknown resultType '{other}'"
                ))
            }
            None => return Err("stateless MCP tools/list omitted resultType".to_owned()),
        }
        if stateless {
            validate_stateless_cache_hint(&result)?;
        }
        let page = result
            .get("tools")
            .and_then(Value::as_array)
            .ok_or_else(|| "MCP tools/list omitted a tools array".to_owned())?;
        tools.extend(page.iter().cloned());
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .filter(|cursor| !cursor.is_empty())
            .map(str::to_owned);
        let Some(next) = cursor.as_ref() else {
            return Ok(json!({"tools": tools}));
        };
        if !seen_cursors.insert(next.clone()) {
            return Err("MCP tools/list repeated a pagination cursor".to_owned());
        }
    }
    Err(format!(
        "MCP tools/list exceeded the {MAX_PAGES}-page safety limit"
    ))
}

fn validate_stateless_cache_hint(result: &Value) -> Result<(), String> {
    if result.get("ttlMs").and_then(Value::as_u64).is_none()
        || !matches!(
            result.get("cacheScope").and_then(Value::as_str),
            Some("public" | "private")
        )
    {
        return Err(
            "stateless MCP tools/list requires non-negative ttlMs and public/private cacheScope"
                .to_owned(),
        );
    }
    Ok(())
}

fn parse_guarded_tool_snapshot(
    config: &GuardedMcpServerConfig,
    negotiated: &NegotiatedMcpProtocol,
    result: &Value,
) -> Result<(McpServerSnapshot, BTreeMap<String, Vec<McpHeaderBinding>>), McpToolsAdapterError> {
    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            McpToolsAdapterError::Protocol("MCP tools/list omitted a tools array".to_owned())
        })?;
    let mut names = BTreeSet::new();
    let mut snapshots = Vec::new();
    let mut parameter_headers = BTreeMap::new();
    for raw in tools {
        let name = raw
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                McpToolsAdapterError::Protocol(
                    "MCP tools/list returned an invalid Tool name".to_owned(),
                )
            })?;
        if !config.allows_tool(name) {
            continue;
        }
        if !names.insert(name.to_owned()) {
            return Err(McpToolsAdapterError::Protocol(format!(
                "MCP server '{}' returned duplicate Tool '{name}'",
                config.server_id
            )));
        }
        let input_schema = raw
            .get("inputSchema")
            .cloned()
            .unwrap_or_else(|| json!({"type": "object"}));
        if config.transport_kind() == McpTransportKind::StreamableHttp {
            match compile_mcp_header_bindings(&input_schema) {
                Ok(bindings) => {
                    parameter_headers.insert(name.to_owned(), bindings);
                }
                Err(error) => {
                    tracing::warn!(
                        server = %config.server_id,
                        tool = name,
                        %error,
                        "excluding MCP Tool with invalid x-mcp-header annotation"
                    );
                    continue;
                }
            }
        }
        let annotations = raw
            .get("annotations")
            .cloned()
            .map(serde_json::from_value::<McpToolAnnotations>)
            .transpose()
            .map_err(|error| {
                McpToolsAdapterError::Protocol(format!(
                    "MCP Tool '{name}' returned invalid annotations: {error}"
                ))
            })?
            .unwrap_or_default();
        snapshots.push(
            McpToolSnapshot::seal_with_annotations(
                config.server_id.clone(),
                name,
                raw.get("description").and_then(Value::as_str).unwrap_or(""),
                input_schema,
                raw.get("outputSchema").cloned(),
                annotations,
            )
            .map_err(|error| McpToolsAdapterError::Protocol(error.to_string()))?,
        );
    }
    let snapshot = McpServerSnapshot::seal(
        config.server_id.clone(),
        config.transport_kind(),
        config.transport.authority().binding_digest.clone(),
        negotiated.version.clone(),
        negotiated.era,
        snapshots,
    )
    .map_err(|error| McpToolsAdapterError::Protocol(error.to_string()))?;
    Ok((snapshot, parameter_headers))
}

fn mcp_request_adapter_error(error: McpRequestError) -> McpToolsAdapterError {
    match error {
        McpRequestError::Transport(message) => McpToolsAdapterError::Transport(message),
        error => McpToolsAdapterError::Protocol(error.to_string()),
    }
}

/// Keeps server managers alive after their thin Tool executors are registered.
pub struct McpToolsAdapterRegistry {
    managers: BTreeMap<McpServerId, Arc<McpServerConnectionManager>>,
    skipped_optional_servers: BTreeMap<McpServerId, String>,
    tool_count: usize,
}

impl McpToolsAdapterRegistry {
    pub async fn register<S: ApprovalCapabilityStore>(
        runtime: &GuardedToolRuntime<S>,
        mut configs: Vec<GuardedMcpServerConfig>,
        restriction: ToolRestriction,
        cancellation: CancellationToken,
    ) -> Result<Self, McpToolsAdapterError> {
        configs.sort_by(|left, right| left.server_id.cmp(&right.server_id));
        restriction
            .bounds
            .validate()
            .map_err(|error| McpToolsAdapterError::InvalidConfig(error.to_string()))?;
        let mut configured_ids = BTreeSet::new();
        for config in &configs {
            config.validate()?;
            if !configured_ids.insert(config.server_id.clone()) {
                return Err(McpToolsAdapterError::Conflict(format!(
                    "duplicate MCP server id: {}",
                    config.server_id
                )));
            }
            let programs = config.allowed_programs();
            let allow_child_processes = config.allows_child_processes();
            let read_roots = config.filesystem_read_roots();
            let write_roots = config.filesystem_write_roots();
            let sandbox_profiles = config.sandbox_profiles();
            let network_targets = config.allowed_network_targets();
            let allow_unrestricted_network = config.allows_unrestricted_network();
            let environment = config.environment_names();
            let credentials = config.credential_references();
            if !config
                .effect_scopes()
                .is_subset(&restriction.bounds.allowed_effects)
                || !programs.is_subset(&restriction.bounds.process.transport.allowed_programs)
                || (allow_child_processes
                    && !restriction.bounds.process.transport.allow_child_processes)
                || !read_roots.is_subset(&restriction.bounds.filesystem.readable_roots)
                || !write_roots.is_subset(&restriction.bounds.filesystem.writable_roots)
                || !sandbox_profiles.is_subset(&restriction.bounds.sandbox.allowed_profiles)
                || (!sandbox_profiles.is_empty() && !restriction.bounds.sandbox.required)
                || !network_targets.is_subset(&restriction.bounds.network.allowed_targets)
                || (allow_unrestricted_network && !restriction.bounds.network.allow_unrestricted)
                || !environment.is_subset(&restriction.bounds.environment.allowed_variables)
                || !credentials.is_subset(&restriction.bounds.allowed_credentials)
            {
                return Err(McpToolsAdapterError::InvalidConfig(format!(
                    "MCP server '{}' exceeds its Host Tool restriction",
                    config.server_id
                )));
            }
        }
        let mut managers = BTreeMap::new();
        let mut skipped_optional_servers = BTreeMap::new();
        for config in configs {
            match McpServerConnectionManager::connect(config.clone(), cancellation.child_token())
                .await
            {
                Ok(manager) => {
                    managers.insert(config.server_id.clone(), manager);
                }
                Err(error) if !config.required => {
                    skipped_optional_servers.insert(config.server_id.clone(), error.to_string());
                }
                Err(error) => {
                    shutdown_mcp_managers(&managers).await;
                    return Err(error);
                }
            }
        }

        let existing_names = match runtime.model_tool_schemas() {
            Ok(schemas) => schemas
                .into_iter()
                .map(|schema| schema.name)
                .collect::<BTreeSet<_>>(),
            Err(error) => {
                shutdown_mcp_managers(&managers).await;
                return Err(McpToolsAdapterError::ToolRuntime(error));
            }
        };
        let mut registrations = Vec::new();
        let mut model_names = BTreeSet::new();
        for manager in managers.values() {
            let server_restriction = mcp_server_restriction(&restriction, &manager.config);
            let mut sanitized_names = BTreeSet::new();
            for tool in &manager.snapshot.tools {
                let server = sanitize_mcp_identifier(manager.config.server_id.as_str());
                let tool_name = sanitize_mcp_identifier(&tool.name);
                if !sanitized_names.insert(tool_name.clone()) {
                    let error = McpToolsAdapterError::Conflict(format!(
                        "MCP server '{}' has Tool names that collide after namespacing",
                        manager.config.server_id
                    ));
                    shutdown_mcp_managers(&managers).await;
                    return Err(error);
                }
                let model_name = format!("mcp__{server}__{tool_name}");
                if existing_names.contains(&model_name) || !model_names.insert(model_name.clone()) {
                    let error = McpToolsAdapterError::Conflict(format!(
                        "MCP model Tool name collides: {model_name}"
                    ));
                    shutdown_mcp_managers(&managers).await;
                    return Err(error);
                }
                let descriptor = ToolDescriptor {
                    tool_id: ToolId::new(format!("mcp/{server}/{tool_name}/v1")),
                    model_schema: ModelToolSchema {
                        name: model_name,
                        description: if tool.description.trim().is_empty() {
                            format!(
                                "MCP Tool '{}' on server '{}'",
                                tool.name, manager.config.server_id
                            )
                        } else {
                            tool.description.clone()
                        },
                        input_schema: tool.input_schema.clone(),
                    },
                    output_schema: json!({
                        "type": "object",
                        "required": ["server", "tool", "result"],
                        "properties": {
                            "server": {"type": "string"},
                            "tool": {"type": "string"},
                            "result": {}
                        },
                        "additionalProperties": false
                    }),
                    effect_scopes: manager.config.effect_scopes(),
                    restriction: server_restriction.clone(),
                    idempotency: mcp_tool_idempotency(&tool.annotations),
                    concurrency: ToolConcurrency::GlobalSerial,
                };
                if let Err(error) = descriptor.validate() {
                    shutdown_mcp_managers(&managers).await;
                    return Err(McpToolsAdapterError::Protocol(error.to_string()));
                }
                registrations.push((
                    descriptor,
                    Arc::new(GuardedMcpToolExecutor {
                        manager: manager.clone(),
                        tool_name: tool.name.clone(),
                        annotations: tool.annotations.clone(),
                        session_approval_scope: tool.schema_digest.clone(),
                    }) as Arc<dyn GuardedToolExecutor>,
                ));
            }
        }
        let tool_count = registrations.len();
        for (descriptor, executor) in registrations {
            if let Err(error) = runtime.register(descriptor, executor) {
                shutdown_mcp_managers(&managers).await;
                return Err(McpToolsAdapterError::ToolRuntime(error));
            }
        }
        Ok(Self {
            managers,
            skipped_optional_servers,
            tool_count,
        })
    }

    pub fn tool_count(&self) -> usize {
        self.tool_count
    }

    pub fn server_names(&self) -> BTreeSet<String> {
        self.managers
            .keys()
            .map(|server| server.as_str().to_owned())
            .collect()
    }

    pub fn skipped_optional_servers(&self) -> &BTreeMap<McpServerId, String> {
        &self.skipped_optional_servers
    }

    pub fn manager(&self, server: &McpServerId) -> Option<Arc<McpServerConnectionManager>> {
        self.managers.get(server).cloned()
    }

    pub async fn shutdown(&self) {
        for manager in self.managers.values() {
            manager.shutdown().await;
        }
    }
}

/// Narrows the aggregate Host MCP ceiling to one immutable server binding.
/// A remote transport therefore cannot inherit a local server's filesystem or
/// process authority, and local servers cannot inherit each other's roots.
fn mcp_server_restriction(
    ceiling: &ToolRestriction,
    config: &GuardedMcpServerConfig,
) -> ToolRestriction {
    let mut bounds = ceiling.bounds.clone();
    bounds.allowed_effects = config.effect_scopes();
    bounds.sandbox.allowed_profiles = config.sandbox_profiles();
    bounds.sandbox.required = !bounds.sandbox.allowed_profiles.is_empty();
    bounds.process.interactive = Default::default();
    bounds.process.transport.allowed_programs = config.allowed_programs();
    bounds.process.transport.allow_child_processes = config.allows_child_processes();
    bounds.filesystem.readable_roots = config.filesystem_read_roots();
    bounds.filesystem.writable_roots = config.filesystem_write_roots();
    bounds.network.allowed_targets = config.allowed_network_targets();
    bounds.network.allow_unrestricted = config.allows_unrestricted_network();
    bounds.environment.allowed_variables = config.environment_names();
    bounds.environment.inherit_host_environment = false;
    bounds.allowed_credentials = config.credential_references();
    ToolRestriction { bounds }
}

async fn shutdown_mcp_managers(managers: &BTreeMap<McpServerId, Arc<McpServerConnectionManager>>) {
    for manager in managers.values() {
        manager.shutdown().await;
    }
}

struct GuardedMcpToolExecutor {
    manager: Arc<McpServerConnectionManager>,
    tool_name: String,
    annotations: McpToolAnnotations,
    session_approval_scope: Digest,
}

fn mcp_tool_idempotency(annotations: &McpToolAnnotations) -> ToolIdempotency {
    if annotations.read_only_hint == Some(true) || annotations.idempotent_hint == Some(true) {
        ToolIdempotency::Idempotent
    } else {
        ToolIdempotency::NonIdempotent
    }
}

fn mcp_unknown_effect_outcome(annotations: &McpToolAnnotations, message: String) -> ToolOutcome {
    if matches!(
        mcp_tool_idempotency(annotations),
        ToolIdempotency::Idempotent
    ) {
        ToolOutcome::Failed {
            code: "mcp_call_interrupted".to_owned(),
            message: format!(
                "{message}; the MCP Tool is annotated read-only or idempotent and may be retried"
            ),
            retryable: true,
        }
    } else {
        ToolOutcome::UnknownEffect { message }
    }
}

/// Describes the business operation initiated through an already-authorized
/// MCP transport. Process launch, transport cache roots, environment, and
/// credentials belong to the immutable server binding and must not be copied
/// into every user-facing Tool approval request.
fn mcp_operation_capabilities(
    config: &GuardedMcpServerConfig,
    tool_name: &str,
    annotations: &McpToolAnnotations,
) -> CapabilityRequest {
    let transport_effects = config.effect_scopes();
    let mut required = CapabilityRequest::default();

    if transport_effects.contains(&EffectScope::Network) {
        if config.allows_unrestricted_network() {
            required.insert_resource(EffectScope::Network, CapabilitySelector::Unrestricted);
        } else {
            for target in config.allowed_network_targets() {
                required.insert_resource(EffectScope::Network, CapabilitySelector::Exact(target));
            }
        }
    }

    let can_change_external_state =
        annotations.read_only_hint != Some(true) || annotations.destructive_hint == Some(true);
    if can_change_external_state && transport_effects.contains(&EffectScope::ExternalSideEffect) {
        required.insert_resource(
            EffectScope::ExternalSideEffect,
            CapabilitySelector::Exact(format!("mcp:{}:{tool_name}", config.server_id)),
        );
    }

    required
}

#[async_trait]
impl GuardedToolExecutor for GuardedMcpToolExecutor {
    fn planning_contract(&self) -> Value {
        json!({
            "contract": "orchestral.mcp-operation-planner/v1",
            "transportBinding": self.manager.config.transport.authority().binding_digest,
            "tool": self.tool_name,
        })
    }

    fn activity_evidence(
        &self,
        _invocation: &ToolInvocation,
        _outcome: Option<&ToolOutcome>,
    ) -> Vec<ToolActivityEvidence> {
        vec![ToolActivityEvidence::Note {
            text: format!("{}/{}", self.manager.config.server_id, self.tool_name),
        }]
    }

    fn plan_operation(
        &self,
        invocation: &ToolInvocation,
        descriptor: &ToolDescriptor,
        _effective_policy: &orchestral_core::tool_protocol::EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        let required_capabilities =
            mcp_operation_capabilities(&self.manager.config, &self.tool_name, &self.annotations);
        let operation = ToolOperationPlan {
            required_capabilities,
            risk: if self.annotations.destructive_hint == Some(true) {
                ToolOperationRisk::Destructive
            } else if self.annotations.requires_approval() {
                ToolOperationRisk::Elevated
            } else {
                ToolOperationRisk::Routine
            },
            // Like Codex's server + Tool session key, but sealed to the full
            // discovered schema and annotations so a changed Tool contract
            // cannot inherit an earlier decision.
            session_approval_scope: Some(self.session_approval_scope.clone()),
            summary: self.approval_summary(invocation),
        };
        operation
            .validate_envelope(&descriptor.effect_scopes)
            .map_err(|error| ToolOutcome::Rejected {
                code: "mcp_operation_invalid".to_owned(),
                message: error.message,
            })?;
        Ok(operation)
    }

    fn approval_summary(
        &self,
        invocation: &orchestral_core::tool_protocol::ToolInvocation,
    ) -> String {
        let digest = invocation
            .args_digest()
            .map(|digest| digest.to_string())
            .unwrap_or_else(|_| "invalid-arguments".to_owned());
        format!(
            "Call MCP server '{}' Tool '{}' with arguments {}",
            self.manager.config.server_id, self.tool_name, digest
        )
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        let bounds = execution.effective_policy.bounds();
        let programs = self.manager.config.allowed_programs();
        let allow_child_processes = self.manager.config.allows_child_processes();
        let read_roots = self.manager.config.filesystem_read_roots();
        let write_roots = self.manager.config.filesystem_write_roots();
        let sandbox_profiles = self.manager.config.sandbox_profiles();
        let network_targets = self.manager.config.allowed_network_targets();
        let allow_unrestricted_network = self.manager.config.allows_unrestricted_network();
        let environment = self.manager.config.environment_names();
        let credentials = self.manager.config.credential_references();
        if !programs.is_subset(&bounds.process.transport.allowed_programs)
            || (allow_child_processes && !bounds.process.transport.allow_child_processes)
            || !read_roots.is_subset(&bounds.filesystem.readable_roots)
            || !write_roots.is_subset(&bounds.filesystem.writable_roots)
            || !sandbox_profiles.is_subset(&bounds.sandbox.allowed_profiles)
            || (!sandbox_profiles.is_empty() && !bounds.sandbox.required)
            || !network_targets.is_subset(&bounds.network.allowed_targets)
            || (allow_unrestricted_network && !bounds.network.allow_unrestricted)
            || !environment.is_subset(&bounds.environment.allowed_variables)
            || !credentials.is_subset(&bounds.allowed_credentials)
        {
            return ToolOutcome::Rejected {
                code: "mcp_policy_rejected".to_owned(),
                message: "MCP transport exceeds the effective Host policy".to_owned(),
            };
        }
        // This watcher outlives a dropped executor future. GuardedToolRuntime
        // may finish the Run cancellation branch first, but the server process
        // must still be terminated and reaped.
        let cleanup_manager = self.manager.clone();
        let cleanup_cancellation = execution.cancellation.clone();
        let cleanup = tokio::spawn(async move {
            cleanup_cancellation.cancelled().await;
            cleanup_manager.shutdown().await;
        });
        let result = self
            .manager
            .invoke(
                &self.tool_name,
                execution.invocation.arguments,
                execution.cancellation,
            )
            .await;
        cleanup.abort();
        match result {
            Ok(result) => ToolOutcome::Completed {
                output: json!({
                    "server": self.manager.config.server_id,
                    "tool": self.tool_name,
                    "result": result,
                })
                .into(),
            },
            Err(GuardedMcpCallError::Rejected(message)) => ToolOutcome::Rejected {
                code: "mcp_tool_rejected".to_owned(),
                message,
            },
            Err(GuardedMcpCallError::Failed(message)) => ToolOutcome::Failed {
                code: "mcp_call_failed".to_owned(),
                message,
                retryable: true,
            },
            Err(GuardedMcpCallError::ToolError(message)) => ToolOutcome::Failed {
                code: "mcp_tool_error".to_owned(),
                message,
                retryable: false,
            },
            Err(GuardedMcpCallError::Unsupported(message)) => ToolOutcome::Failed {
                code: "mcp_feature_unsupported".to_owned(),
                message,
                retryable: false,
            },
            Err(GuardedMcpCallError::UnknownEffect(message)) => {
                mcp_unknown_effect_outcome(&self.annotations, message)
            }
            Err(GuardedMcpCallError::Cancelled) => ToolOutcome::Cancelled,
        }
    }
}

enum GuardedMcpCallError {
    Rejected(String),
    Failed(String),
    ToolError(String),
    Unsupported(String),
    UnknownEffect(String),
    Cancelled,
}

impl GuardedMcpCallError {
    fn invalidates_session(&self) -> bool {
        matches!(self, Self::Failed(_) | Self::UnknownEffect(_))
    }
}

fn validate_mcp_call_result(
    negotiated: &NegotiatedMcpProtocol,
    result: Value,
) -> Result<Value, GuardedMcpCallError> {
    if negotiated.era == McpProtocolEra::Stateless {
        match result.get("resultType").and_then(Value::as_str) {
            Some("complete") => {}
            Some("input_required") => {
                return Err(GuardedMcpCallError::Unsupported(
                    "MCP multi-round-trip input is not implemented by Tools Adapter v1".to_owned(),
                ))
            }
            Some(other) => {
                return Err(GuardedMcpCallError::Failed(format!(
                    "MCP tools/call returned unknown resultType '{other}'"
                )))
            }
            None => {
                return Err(GuardedMcpCallError::Failed(
                    "stateless MCP tools/call omitted resultType".to_owned(),
                ))
            }
        }
    }
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        return Err(GuardedMcpCallError::ToolError(mcp_tool_error_message(
            &result,
        )));
    }
    Ok(result)
}

/// MCP Tool failures commonly carry actionable JSON inside text content. Keep
/// that payload readable for both the model and TUI instead of stringifying
/// the whole `CallToolResult`, which double-escapes the actual error.
fn mcp_tool_error_message(result: &Value) -> String {
    if let Some(structured) = result
        .get("structuredContent")
        .filter(|value| !value.is_null())
    {
        return actionable_mcp_error(structured);
    }

    let text = result
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            (item.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| item.get("text").and_then(Value::as_str))
                .flatten()
        })
        .map(|text| {
            embedded_json(text)
                .as_ref()
                .map(actionable_mcp_error)
                .unwrap_or_else(|| text.to_owned())
        })
        .collect::<Vec<_>>()
        .join("\n");
    if !text.trim().is_empty() {
        text
    } else {
        result.to_string()
    }
}

fn embedded_json(text: &str) -> Option<Value> {
    let mut value = serde_json::from_str::<Value>(text.trim()).ok()?;
    for _ in 0..2 {
        let Value::String(nested) = &value else {
            break;
        };
        let Ok(parsed) = serde_json::from_str::<Value>(nested.trim()) else {
            break;
        };
        value = parsed;
    }
    Some(value)
}

fn actionable_mcp_error(value: &Value) -> String {
    const KEYS: &[&str] = &["code", "message", "reason", "how_to_get", "hint"];
    const MAX_ITEMS: usize = 8;
    const MAX_CHARS: usize = 4_096;

    fn collect(value: &Value, output: &mut Vec<(String, String)>) {
        if output.len() >= MAX_ITEMS {
            return;
        }
        match value {
            Value::Object(fields) => {
                for key in KEYS {
                    let Some(value) = fields.get(*key) else {
                        continue;
                    };
                    let rendered = match value {
                        Value::String(value) => value.trim().to_owned(),
                        Value::Null => continue,
                        value => value.to_string(),
                    };
                    if !rendered.is_empty()
                        && !output.iter().any(|(existing_key, existing)| {
                            existing_key == key && existing == &rendered
                        })
                    {
                        output.push(((*key).to_owned(), rendered));
                        if output.len() >= MAX_ITEMS {
                            return;
                        }
                    }
                }
                for value in fields.values() {
                    collect(value, output);
                    if output.len() >= MAX_ITEMS {
                        return;
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    collect(value, output);
                    if output.len() >= MAX_ITEMS {
                        return;
                    }
                }
            }
            _ => {}
        }
    }

    let mut fields = Vec::new();
    collect(value, &mut fields);
    let message = if fields.is_empty() {
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    } else {
        fields
            .into_iter()
            .map(|(key, value)| format!("{key}: {value}"))
            .collect::<Vec<_>>()
            .join("; ")
    };
    let mut chars = message.chars();
    let mut bounded = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        bounded.push('…');
    }
    bounded
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum McpToolsAdapterError {
    #[error("invalid MCP Tools Adapter configuration: {0}")]
    InvalidConfig(String),
    #[error("MCP Tools transport failed: {0}")]
    Transport(String),
    #[error("MCP Tools protocol failed: {0}")]
    Protocol(String),
    #[error("MCP Tools registry conflict: {0}")]
    Conflict(String),
    #[error("MCP Tools startup was cancelled")]
    Cancelled,
    #[error(transparent)]
    ToolRuntime(#[from] ToolRuntimeError),
}

fn sanitize_mcp_identifier(value: &str) -> String {
    let normalized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let normalized = normalized.trim_matches('_');
    if normalized.is_empty() {
        "unnamed".to_owned()
    } else {
        normalized.to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NegotiatedMcpProtocol {
    version: String,
    era: McpProtocolEra,
}

type McpRequestError = McpTransportError;

struct McpTransportSession {
    connection: Box<dyn McpTransportConnection>,
    next_id: u64,
    negotiated: Option<NegotiatedMcpProtocol>,
}

impl McpTransportSession {
    fn new(connection: Box<dyn McpTransportConnection>) -> Self {
        Self {
            connection,
            next_id: 1,
            negotiated: None,
        }
    }

    fn next_request_id(&self) -> u64 {
        self.next_id
    }

    async fn probe_stateless(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<bool, McpRequestError> {
        let params = attach_stateless_request_metadata(json!({}))?;
        let result = match self
            .request_raw("server/discover", params, BTreeMap::new(), cancellation)
            .await
        {
            Ok(result) => result,
            Err(error) if error.permits_legacy_fallback() => return Ok(false),
            Err(error) => return Err(error),
        };
        let versions = result
            .get("supportedVersions")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                McpRequestError::Protocol("server/discover omitted supportedVersions".to_owned())
            })?;
        if !versions
            .iter()
            .any(|version| version.as_str() == Some(MCP_STATELESS_PROTOCOL_2026_07_28))
        {
            return Ok(false);
        }
        if result
            .get("capabilities")
            .and_then(|value| value.get("tools"))
            .and_then(Value::as_object)
            .is_none()
        {
            return Err(McpRequestError::Protocol(
                "MCP server does not advertise the tools capability".to_owned(),
            ));
        }
        self.negotiated = Some(NegotiatedMcpProtocol {
            version: MCP_STATELESS_PROTOCOL_2026_07_28.to_owned(),
            era: McpProtocolEra::Stateless,
        });
        Ok(true)
    }

    async fn initialize_guarded_legacy(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<(), McpRequestError> {
        let params = json!({
            "protocolVersion": MCP_LATEST_LEGACY_PROTOCOL,
            "capabilities": {},
            "clientInfo": {
                "name": "orchestral",
                "version": env!("CARGO_PKG_VERSION")
            }
        });
        let result = self
            .request_raw(
                "initialize",
                params,
                BTreeMap::new(),
                cancellation.child_token(),
            )
            .await?;
        let version = result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                McpRequestError::Protocol(
                    "legacy initialize response omitted protocolVersion".to_owned(),
                )
            })?;
        const SUPPORTED_LEGACY: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
        if !SUPPORTED_LEGACY.contains(&version) {
            return Err(McpRequestError::Protocol(format!(
                "server selected unsupported legacy MCP version '{version}'"
            )));
        }
        if result
            .get("capabilities")
            .and_then(|value| value.get("tools"))
            .and_then(Value::as_object)
            .is_none()
        {
            return Err(McpRequestError::Protocol(
                "legacy MCP server does not advertise the tools capability".to_owned(),
            ));
        }
        self.negotiated = Some(NegotiatedMcpProtocol {
            version: version.to_owned(),
            era: McpProtocolEra::LegacyHandshake,
        });
        self.connection
            .notification(
                "notifications/initialized",
                json!({}),
                cancellation.child_token(),
            )
            .await
    }

    fn negotiated_protocol(&self) -> Result<&NegotiatedMcpProtocol, McpRequestError> {
        self.negotiated
            .as_ref()
            .ok_or_else(|| McpRequestError::Protocol("MCP transport was not negotiated".to_owned()))
    }

    async fn shutdown(&mut self) -> Result<(), String> {
        match timeout(MCP_TRANSPORT_CLOSE_TIMEOUT, self.connection.close()).await {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err("MCP transport close timed out".to_owned()),
        }
    }

    async fn cancel_request(&self, request_id: u64, reason: &str) {
        if self.connection.cancellation() == McpTransportCancellation::ProtocolNotification {
            let _ = timeout(
                Duration::from_millis(100),
                self.connection.notification(
                    "notifications/cancelled",
                    json!({"requestId": request_id, "reason": reason}),
                    CancellationToken::new(),
                ),
            )
            .await;
        }
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        parameter_headers: BTreeMap<String, String>,
        cancellation: CancellationToken,
    ) -> Result<Value, String> {
        let params = match self.negotiated.as_ref().map(|value| value.era) {
            Some(McpProtocolEra::Stateless) => {
                attach_stateless_request_metadata(params).map_err(|error| error.to_string())?
            }
            _ => params,
        };
        self.request_raw(method, params, parameter_headers, cancellation)
            .await
            .map_err(|error| error.to_string())
    }

    async fn request_raw(
        &mut self,
        method: &str,
        params: Value,
        parameter_headers: BTreeMap<String, String>,
        cancellation: CancellationToken,
    ) -> Result<Value, McpRequestError> {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.connection
            .request(
                McpTransportRequest {
                    id,
                    method: method.to_owned(),
                    params,
                    parameter_headers,
                },
                cancellation,
            )
            .await
    }
}

struct StdioMcpTransport {
    state: tokio::sync::Mutex<StdioMcpTransportState>,
}

struct StdioMcpTransportState {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr_tail: Arc<StdMutex<VecDeque<u8>>>,
    stderr_task: tokio::task::JoinHandle<()>,
    process_group_id: Option<u32>,
    max_frame_bytes: usize,
}

impl StdioMcpTransport {
    async fn connect(
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
        cwd: &std::path::Path,
        backend_starts_new_session: bool,
    ) -> Result<Self, String> {
        let mut cmd = Command::new(command);
        cmd.args(args);
        // MCP stdio servers receive only the Host-configured environment.
        cmd.env_clear();
        cmd.envs(env);
        cmd.current_dir(cwd);
        isolate_mcp_process_group(&mut cmd, backend_starts_new_session);
        cmd.kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|err| format!("spawn mcp process failed: {}", err))?;

        let process_group_id = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "mcp stdio missing stdin pipe".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "mcp stdio missing stdout pipe".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "mcp stdio missing stderr pipe".to_string())?;
        let (stderr_tail, stderr_task) = capture_mcp_stderr(stderr);

        Ok(Self {
            state: tokio::sync::Mutex::new(StdioMcpTransportState {
                child,
                stdin,
                stdout: BufReader::new(stdout),
                stderr_tail,
                stderr_task,
                process_group_id,
                max_frame_bytes: DEFAULT_MCP_MAX_FRAME_BYTES,
            }),
        })
    }
}

impl StdioMcpTransportState {
    async fn request(&mut self, request: McpTransportRequest) -> Result<Value, McpTransportError> {
        let id = request.id;
        let payload = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": request.method,
            "params": request.params,
        });
        self.write_frame(&payload)
            .await
            .map_err(McpTransportError::Transport)?;

        loop {
            let msg = self
                .read_frame()
                .await
                .map_err(McpTransportError::Transport)?;
            let matched = msg
                .get("id")
                .and_then(Value::as_u64)
                .map(|value| value == id)
                .unwrap_or(false);
            if !matched {
                if msg.get("id").is_some() && msg.get("method").is_some() {
                    return Err(McpTransportError::Protocol(
                        "MCP server initiated a request without a negotiated client capability"
                            .to_owned(),
                    ));
                }
                if msg.get("id").is_some() {
                    return Err(McpTransportError::Protocol(format!(
                        "MCP server returned an unexpected response id while waiting for {id}"
                    )));
                }
                continue;
            }

            if let Some(error) = msg.get("error") {
                return Err(McpTransportError::Rpc {
                    code: error.get("code").and_then(Value::as_i64).unwrap_or(-32000),
                    message: error
                        .get("message")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| error.to_string()),
                });
            }
            return Ok(msg.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    async fn write_frame(&mut self, payload: &Value) -> Result<(), String> {
        // Use NDJSON (newline-delimited JSON) — compatible with all MCP servers.
        let body = serde_json::to_vec(payload)
            .map_err(|err| format!("serialize mcp payload failed: {}", err))?;
        if body.len() > self.max_frame_bytes {
            return Err(format!(
                "MCP request frame exceeds the {} byte limit",
                self.max_frame_bytes
            ));
        }
        self.stdin
            .write_all(&body)
            .await
            .map_err(|err| format!("write mcp payload failed: {}", err))?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(|err| format!("write mcp newline failed: {}", err))?;
        self.stdin
            .flush()
            .await
            .map_err(|err| format!("flush mcp payload failed: {}", err))
    }

    async fn read_frame(&mut self) -> Result<Value, String> {
        // Auto-detect: NDJSON (line = JSON) or LSP (Content-Length header).
        loop {
            let line = self.read_bounded_line().await?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            // If line starts with '{', it's NDJSON
            if trimmed.starts_with('{') {
                return serde_json::from_str::<Value>(trimmed)
                    .map_err(|err| format!("parse mcp NDJSON failed: {}", err));
            }
            // Otherwise treat as Content-Length header (LSP style)
            if let Some((key, value)) = trimmed.split_once(':') {
                if key.trim().eq_ignore_ascii_case("content-length") {
                    if let Ok(len) = value.trim().parse::<usize>() {
                        if len > self.max_frame_bytes {
                            return Err(format!(
                                "MCP response frame exceeds the {} byte limit",
                                self.max_frame_bytes
                            ));
                        }
                        // Read blank line after headers
                        let _ = self.read_bounded_line().await?;
                        // Read exact body
                        let mut body = vec![0_u8; len];
                        self.stdout
                            .read_exact(&mut body)
                            .await
                            .map_err(|err| format!("read mcp payload failed: {}", err))?;
                        return serde_json::from_slice::<Value>(&body)
                            .map_err(|err| format!("parse mcp payload failed: {}", err));
                    }
                }
            }
        }
    }

    async fn read_bounded_line(&mut self) -> Result<String, String> {
        let mut bytes = Vec::new();
        loop {
            let (chunk, consumed, complete) = {
                let available = self
                    .stdout
                    .fill_buf()
                    .await
                    .map_err(|error| format!("read MCP frame failed: {error}"))?;
                if available.is_empty() {
                    if bytes.is_empty() {
                        return Err(self.closed_process_error().await);
                    }
                    (Vec::new(), 0, true)
                } else if let Some(index) = available.iter().position(|byte| *byte == b'\n') {
                    (available[..=index].to_vec(), index + 1, true)
                } else {
                    (available.to_vec(), available.len(), false)
                }
            };
            if bytes.len().saturating_add(chunk.len()) > self.max_frame_bytes {
                return Err(format!(
                    "MCP response line exceeds the {} byte limit",
                    self.max_frame_bytes
                ));
            }
            bytes.extend_from_slice(&chunk);
            self.stdout.consume(consumed);
            if complete {
                return String::from_utf8(bytes)
                    .map_err(|error| format!("MCP response is not UTF-8: {error}"));
            }
        }
    }

    async fn closed_process_error(&mut self) -> String {
        let status = self
            .child
            .try_wait()
            .ok()
            .flatten()
            .map(|status| status.to_string())
            .unwrap_or_else(|| "unknown status".to_owned());
        tokio::task::yield_now().await;
        let stderr = self
            .stderr_tail
            .lock()
            .map(|tail| tail.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let stderr = String::from_utf8_lossy(&stderr)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if stderr.is_empty() {
            format!("mcp process closed stdout ({status})")
        } else {
            format!("mcp process closed stdout ({status}): {stderr}")
        }
    }
}

#[async_trait]
impl McpTransportConnection for StdioMcpTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::Stdio
    }

    fn cancellation(&self) -> McpTransportCancellation {
        McpTransportCancellation::ProtocolNotification
    }

    async fn request(
        &self,
        request: McpTransportRequest,
        cancellation: CancellationToken,
    ) -> Result<Value, McpTransportError> {
        let mut state = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(McpTransportError::Transport(
                    "stdio MCP request was cancelled before dispatch".to_owned(),
                ));
            }
            state = self.state.lock() => state,
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(McpTransportError::Transport(
                "stdio MCP request was cancelled after dispatch".to_owned(),
            )),
            result = state.request(request) => result,
        }
    }

    async fn notification(
        &self,
        method: &str,
        params: Value,
        cancellation: CancellationToken,
    ) -> Result<(), McpTransportError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        let mut state = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(McpTransportError::Transport(
                    "stdio MCP notification was cancelled".to_owned(),
                ));
            }
            state = self.state.lock() => state,
        };
        state
            .write_frame(&payload)
            .await
            .map_err(McpTransportError::Transport)
    }

    async fn close(&self) -> Result<(), McpTransportError> {
        let mut state = self.state.lock().await;
        let process_group_id = state.process_group_id;
        terminate_mcp_process_tree(&mut state.child, process_group_id).await;
        state.stderr_task.abort();
        Ok(())
    }
}

fn capture_mcp_stderr(
    stderr: ChildStderr,
) -> (Arc<StdMutex<VecDeque<u8>>>, tokio::task::JoinHandle<()>) {
    let tail = Arc::new(StdMutex::new(VecDeque::with_capacity(
        MCP_STDERR_CAPTURE_BYTES,
    )));
    let captured = tail.clone();
    let task = tokio::spawn(async move {
        let mut stderr = BufReader::new(stderr);
        let mut buffer = [0_u8; 4096];
        loop {
            let Ok(count) = stderr.read(&mut buffer).await else {
                break;
            };
            if count == 0 {
                break;
            }
            let Ok(mut tail) = captured.lock() else {
                break;
            };
            tail.extend(&buffer[..count]);
            while tail.len() > MCP_STDERR_CAPTURE_BYTES {
                tail.pop_front();
            }
        }
    });
    (tail, task)
}

fn attach_stateless_request_metadata(mut params: Value) -> Result<Value, McpRequestError> {
    let object = params.as_object_mut().ok_or_else(|| {
        McpRequestError::Protocol("MCP request params must be an object".to_owned())
    })?;
    if object.contains_key("_meta") {
        return Err(McpRequestError::Protocol(
            "MCP caller cannot override Host request metadata".to_owned(),
        ));
    }
    object.insert(
        "_meta".to_owned(),
        json!({
            "io.modelcontextprotocol/protocolVersion": MCP_STATELESS_PROTOCOL_2026_07_28,
            "io.modelcontextprotocol/clientInfo": {
                "name": "orchestral",
                "version": env!("CARGO_PKG_VERSION")
            },
            "io.modelcontextprotocol/clientCapabilities": {}
        }),
    );
    Ok(params)
}

#[cfg(unix)]
fn isolate_mcp_process_group(command: &mut Command, backend_starts_new_session: bool) {
    if !backend_starts_new_session {
        command.process_group(0);
    }
}

#[cfg(not(unix))]
fn isolate_mcp_process_group(_command: &mut Command, _backend_starts_new_session: bool) {}

async fn terminate_mcp_process_tree(child: &mut Child, process_group_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(process_group_id) = process_group_id.filter(|id| *id <= i32::MAX as u32) {
        // SAFETY: this child was spawned as leader of a fresh process group.
        unsafe {
            libc::kill(-(process_group_id as i32), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = process_group_id;
    let _ = child.kill().await;
    let _ = timeout(MCP_PROCESS_REAP_TIMEOUT, child.wait()).await;
}

#[cfg(test)]
mod mcp_header_tests {
    use super::*;

    #[test]
    fn nested_static_property_headers_compile_and_extract_canonically() {
        let bindings = compile_mcp_header_bindings(&json!({
            "type": "object",
            "properties": {
                "region": {"type": "string", "x-mcp-header": "Region"},
                "routing": {
                    "type": "object",
                    "properties": {
                        "priority": {"type": "integer", "x-mcp-header": "Priority"},
                        "dryRun": {"type": "boolean", "x-mcp-header": "Dry-Run"}
                    }
                }
            }
        }))
        .unwrap();
        let headers = build_mcp_parameter_headers(
            &bindings,
            &json!({
                "region": "us-west1",
                "routing": {"priority": -7, "dryRun": true}
            }),
        )
        .unwrap();
        assert_eq!(headers.get("Region").map(String::as_str), Some("us-west1"));
        assert_eq!(headers.get("Priority").map(String::as_str), Some("-7"));
        assert_eq!(headers.get("Dry-Run").map(String::as_str), Some("true"));
    }

    #[test]
    fn non_static_duplicate_and_non_primitive_annotations_are_rejected() {
        let invalid = [
            json!({
                "type": "object",
                "properties": {"values": {"type": "array", "items": {
                    "type": "string", "x-mcp-header": "Item"
                }}}
            }),
            json!({
                "type": "object",
                "allOf": [{"properties": {"value": {
                    "type": "string", "x-mcp-header": "Value"
                }}}]
            }),
            json!({
                "type": "object",
                "properties": {
                    "left": {"type": "string", "x-mcp-header": "Route"},
                    "right": {"type": "string", "x-mcp-header": "route"}
                }
            }),
            json!({
                "type": "object",
                "properties": {"ratio": {"type": "number", "x-mcp-header": "Ratio"}}
            }),
            json!({
                "type": "object",
                "properties": {"value": {"type": "string", "x-mcp-header": "bad header"}}
            }),
        ];
        for schema in invalid {
            assert!(compile_mcp_header_bindings(&schema).is_err());
        }
    }

    #[test]
    fn header_integer_extraction_enforces_the_javascript_safe_range() {
        let bindings = compile_mcp_header_bindings(&json!({
            "type": "object",
            "properties": {"value": {"type": "integer", "x-mcp-header": "Value"}}
        }))
        .unwrap();
        assert!(build_mcp_parameter_headers(
            &bindings,
            &json!({"value": MAX_SAFE_MCP_HEADER_INTEGER})
        )
        .is_ok());
        assert!(build_mcp_parameter_headers(
            &bindings,
            &json!({"value": MAX_SAFE_MCP_HEADER_INTEGER + 1})
        )
        .is_err());
    }
}

#[cfg(test)]
mod mcp_lifecycle_gate_tests {
    use super::*;
    use crate::tool_runtime::{GuardedToolResult, ToolArtifactStore};
    use crate::InMemoryBlobStore;
    use orchestral_core::agent_protocol::wire::RunId;
    use orchestral_core::tool_effect::{
        replay_tool_effect, InMemoryToolEffectJournalStore, ToolEffectJournalStore, ToolEffectKey,
        ToolEffectPhase,
    };
    use orchestral_core::tool_protocol::ApprovalPolicy;
    use orchestral_core::tool_protocol::{
        HostApprovalIssuer, HostApprovalVerifier, HostToolPolicy, InMemoryApprovalCapabilityStore,
        RunToolGrant, ToolCallId, ToolInvocation, ToolOutput, ToolPolicyBounds,
    };
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::time::Instant;
    use tokio::sync::Notify;

    const GATE_CASES: usize = 1_000;
    const GATE_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FaultStage {
        Healthy,
        Connect,
        Discover,
        Initialize,
        List,
        Call,
        BodyDecode,
        ReconnectStable,
        SchemaChanged,
        NameConflict,
        LargeResult,
    }

    struct FaultState {
        stage: FaultStage,
        connects: AtomicUsize,
        active_connects: AtomicUsize,
        active_connections: AtomicUsize,
        active_requests: AtomicUsize,
        closes: AtomicUsize,
        explicit_closes: AtomicUsize,
        tool_call_dispatches: AtomicUsize,
        completed_call_responses: AtomicUsize,
        entered: Notify,
    }

    impl FaultState {
        fn new(stage: FaultStage) -> Arc<Self> {
            Arc::new(Self {
                stage,
                connects: AtomicUsize::new(0),
                active_connects: AtomicUsize::new(0),
                active_connections: AtomicUsize::new(0),
                active_requests: AtomicUsize::new(0),
                closes: AtomicUsize::new(0),
                explicit_closes: AtomicUsize::new(0),
                tool_call_dispatches: AtomicUsize::new(0),
                completed_call_responses: AtomicUsize::new(0),
                entered: Notify::new(),
            })
        }

        fn assert_released(&self) {
            assert_eq!(self.active_connects.load(Ordering::SeqCst), 0);
            assert_eq!(self.active_connections.load(Ordering::SeqCst), 0);
            assert_eq!(self.active_requests.load(Ordering::SeqCst), 0);
        }
    }

    enum ActiveCounter {
        Connect(Arc<FaultState>),
        Request(Arc<FaultState>),
    }

    impl Drop for ActiveCounter {
        fn drop(&mut self) {
            match self {
                Self::Connect(state) => {
                    state.active_connects.fetch_sub(1, Ordering::SeqCst);
                }
                Self::Request(state) => {
                    state.active_requests.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }
    }

    struct FaultFactory {
        authority: McpTransportAuthority,
        state: Arc<FaultState>,
    }

    impl FaultFactory {
        fn new(stage: FaultStage) -> Arc<Self> {
            let kind = match stage {
                FaultStage::Discover | FaultStage::BodyDecode => McpTransportKind::StreamableHttp,
                FaultStage::Healthy
                | FaultStage::Connect
                | FaultStage::Initialize
                | FaultStage::List
                | FaultStage::Call
                | FaultStage::ReconnectStable
                | FaultStage::SchemaChanged
                | FaultStage::NameConflict
                | FaultStage::LargeResult => McpTransportKind::Stdio,
            };
            let (effect_scopes, process_programs, network_targets) = match kind {
                McpTransportKind::Stdio => (
                    BTreeSet::from([
                        EffectScope::Process,
                        EffectScope::FilesystemRead,
                        EffectScope::FilesystemWrite,
                        EffectScope::ExternalSideEffect,
                    ]),
                    BTreeSet::from(["/fault/mcp".to_owned()]),
                    BTreeSet::new(),
                ),
                McpTransportKind::StreamableHttp => (
                    BTreeSet::from([EffectScope::Network, EffectScope::ExternalSideEffect]),
                    BTreeSet::new(),
                    BTreeSet::from(["http://127.0.0.1/fault-mcp".to_owned()]),
                ),
                _ => unreachable!("fault factory only selects v1 transport kinds"),
            };
            let (filesystem_read_roots, filesystem_write_roots, sandbox_profiles) = match kind {
                McpTransportKind::Stdio => (
                    BTreeSet::from(["/fault/read".to_owned()]),
                    BTreeSet::from(["/fault/write".to_owned()]),
                    BTreeSet::from([MCP_STDIO_SANDBOX_PROFILE.to_owned()]),
                ),
                McpTransportKind::StreamableHttp => {
                    (BTreeSet::new(), BTreeSet::new(), BTreeSet::new())
                }
                _ => unreachable!("fault factory only selects v1 transport kinds"),
            };
            Arc::new(Self {
                authority: McpTransportAuthority {
                    kind,
                    binding_digest: Digest::sha256(format!("fault-stage-{stage:?}")),
                    effect_scopes,
                    process_programs,
                    allow_child_processes: false,
                    allow_host_ui: false,
                    filesystem_read_roots,
                    filesystem_write_roots,
                    sandbox_profiles,
                    network_targets,
                    allow_unrestricted_network: false,
                    environment_variables: BTreeSet::new(),
                    credential_references: BTreeSet::new(),
                },
                state: FaultState::new(stage),
            })
        }

        fn with_authority(authority: McpTransportAuthority) -> Arc<Self> {
            Arc::new(Self {
                authority,
                state: FaultState::new(FaultStage::Healthy),
            })
        }
    }

    #[async_trait]
    impl McpTransportFactory for FaultFactory {
        fn authority(&self) -> &McpTransportAuthority {
            &self.authority
        }

        async fn connect(&self) -> Result<Box<dyn McpTransportConnection>, McpTransportError> {
            let generation = self.state.connects.fetch_add(1, Ordering::SeqCst) + 1;
            if self.state.stage == FaultStage::Connect {
                self.state.active_connects.fetch_add(1, Ordering::SeqCst);
                let _guard = ActiveCounter::Connect(self.state.clone());
                self.state.entered.notify_one();
                return pending().await;
            }
            self.state.active_connections.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FaultConnection {
                kind: self.authority.kind,
                state: self.state.clone(),
                closed: AtomicBool::new(false),
                generation,
            }))
        }
    }

    struct FaultConnection {
        kind: McpTransportKind,
        state: Arc<FaultState>,
        closed: AtomicBool,
        generation: usize,
    }

    impl FaultConnection {
        async fn block_request(&self) -> Result<Value, McpTransportError> {
            self.state.active_requests.fetch_add(1, Ordering::SeqCst);
            let _guard = ActiveCounter::Request(self.state.clone());
            self.state.entered.notify_one();
            pending().await
        }

        fn release(&self, explicit: bool) {
            if !self.closed.swap(true, Ordering::SeqCst) {
                self.state.active_connections.fetch_sub(1, Ordering::SeqCst);
                self.state.closes.fetch_add(1, Ordering::SeqCst);
                if explicit {
                    self.state.explicit_closes.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    impl Drop for FaultConnection {
        fn drop(&mut self) {
            self.release(false);
        }
    }

    #[async_trait]
    impl McpTransportConnection for FaultConnection {
        fn kind(&self) -> McpTransportKind {
            self.kind
        }

        fn cancellation(&self) -> McpTransportCancellation {
            McpTransportCancellation::DropExchange
        }

        async fn request(
            &self,
            request: McpTransportRequest,
            _cancellation: CancellationToken,
        ) -> Result<Value, McpTransportError> {
            let is_tool_call = request.method == "tools/call";
            let result = match request.method.as_str() {
                "server/discover" if self.state.stage == FaultStage::Discover => {
                    self.block_request().await
                }
                "server/discover" if self.state.stage == FaultStage::Initialize => {
                    Err(McpTransportError::Rpc {
                        code: -32601,
                        message: "legacy server".to_owned(),
                    })
                }
                "server/discover" => Ok(json!({
                    "supportedVersions": [MCP_STATELESS_PROTOCOL_2026_07_28],
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fault", "version": "1"}
                })),
                "initialize" if self.state.stage == FaultStage::Initialize => {
                    self.block_request().await
                }
                "initialize" => Ok(json!({
                    "protocolVersion": MCP_LATEST_LEGACY_PROTOCOL,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fault", "version": "1"}
                })),
                "tools/list" if self.state.stage == FaultStage::List => self.block_request().await,
                "tools/list" if self.state.stage == FaultStage::NameConflict => Ok(json!({
                    "resultType": "complete",
                    "ttlMs": 1_000,
                    "cacheScope": "private",
                    "tools": [
                        {"name": "a.b", "inputSchema": {"type": "object"}},
                        {"name": "a b", "inputSchema": {"type": "object"}}
                    ]
                })),
                "tools/list" => {
                    let echo_schema =
                        if self.state.stage == FaultStage::SchemaChanged && self.generation > 1 {
                            json!({
                                "type": "object",
                                "properties": {"changed": {"type": "boolean"}},
                                "additionalProperties": false
                            })
                        } else {
                            json!({"type": "object", "additionalProperties": false})
                        };
                    Ok(json!({
                        "resultType": "complete",
                        "ttlMs": 1_000,
                        "cacheScope": "private",
                        "tools": [
                            {"name": "echo", "description": "echo", "inputSchema": echo_schema},
                            {"name": "beta", "description": "beta", "inputSchema": {
                                "type": "object", "additionalProperties": false
                            }},
                            {"name": "hidden", "description": "hidden", "inputSchema": {
                                "type": "object", "additionalProperties": false
                            }}
                        ]
                    }))
                }
                "tools/call"
                    if matches!(self.state.stage, FaultStage::Call | FaultStage::BodyDecode) =>
                {
                    self.block_request().await
                }
                "tools/call" => {
                    self.state
                        .tool_call_dispatches
                        .fetch_add(1, Ordering::SeqCst);
                    if matches!(
                        self.state.stage,
                        FaultStage::ReconnectStable | FaultStage::SchemaChanged
                    ) && self.generation == 1
                    {
                        Err(McpTransportError::Transport(
                            "injected disconnect".to_owned(),
                        ))
                    } else {
                        let text = if self.state.stage == FaultStage::LargeResult {
                            "large-mcp-result/".repeat(256)
                        } else {
                            "ok".to_owned()
                        };
                        Ok(json!({
                            "resultType": "complete",
                            "content": [{"type": "text", "text": text}],
                            "isError": false
                        }))
                    }
                }
                other => Err(McpTransportError::Protocol(format!(
                    "unexpected fault transport method: {other}"
                ))),
            };
            if result.is_ok() && is_tool_call {
                self.state
                    .completed_call_responses
                    .fetch_add(1, Ordering::SeqCst);
            }
            result
        }

        async fn notification(
            &self,
            _method: &str,
            _params: Value,
            _cancellation: CancellationToken,
        ) -> Result<(), McpTransportError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), McpTransportError> {
            self.release(true);
            Ok(())
        }
    }

    fn fault_config(factory: Arc<FaultFactory>, deadline: Duration) -> GuardedMcpServerConfig {
        GuardedMcpServerConfig {
            server_id: McpServerId::new(format!("fault-{:?}", factory.state.stage)),
            required: true,
            transport: factory,
            startup_timeout: deadline,
            tool_timeout: deadline,
            enabled_tools: BTreeSet::new(),
            disabled_tools: BTreeSet::new(),
        }
    }

    async fn connect_result(
        config: GuardedMcpServerConfig,
        cancellation: CancellationToken,
    ) -> Result<Arc<McpServerConnectionManager>, McpToolsAdapterError> {
        McpServerConnectionManager::connect(config, cancellation).await
    }

    fn assert_unknown_effect(result: Result<Value, GuardedMcpCallError>) {
        assert!(matches!(result, Err(GuardedMcpCallError::UnknownEffect(_))));
    }

    fn authority_bounds(authority: &McpTransportAuthority) -> ToolPolicyBounds {
        let mut bounds = ToolPolicyBounds {
            allowed_effects: authority.effect_scopes.clone(),
            approval: ApprovalPolicy::Required,
            max_timeout_ms: Some(30_000),
            max_output_bytes: Some(64 * 1024),
            ..ToolPolicyBounds::default()
        };
        bounds.process.transport.allowed_programs = authority.process_programs.clone();
        bounds.process.transport.allow_child_processes = authority.allow_child_processes;
        bounds.filesystem.readable_roots = authority.filesystem_read_roots.clone();
        bounds.filesystem.writable_roots = authority.filesystem_write_roots.clone();
        bounds.sandbox.allowed_profiles = authority.sandbox_profiles.clone();
        bounds.sandbox.required = !authority.sandbox_profiles.is_empty();
        bounds.network.allowed_targets = authority.network_targets.clone();
        bounds.network.allow_unrestricted = authority.allow_unrestricted_network;
        bounds.environment.allowed_variables = authority.environment_variables.clone();
        bounds.allowed_credentials = authority.credential_references.clone();
        bounds
    }

    #[test]
    fn mcp_operation_does_not_reapprove_transport_authority() {
        let factory = FaultFactory::new(FaultStage::Healthy);
        let config = fault_config(factory, Duration::from_secs(1));

        let unknown = mcp_operation_capabilities(&config, "run", &McpToolAnnotations::default());
        assert_eq!(
            unknown.effects,
            BTreeSet::from([EffectScope::ExternalSideEffect])
        );
        assert!(!unknown.effects.contains(&EffectScope::Process));
        assert!(!unknown.effects.contains(&EffectScope::FilesystemRead));
        assert!(!unknown.effects.contains(&EffectScope::FilesystemWrite));
        assert!(!unknown.effects.contains(&EffectScope::SecretRead));

        let read_only = mcp_operation_capabilities(
            &config,
            "lookup",
            &McpToolAnnotations {
                read_only_hint: Some(true),
                ..McpToolAnnotations::default()
            },
        );
        assert!(read_only.effects.is_empty());
        assert!(read_only.resources.is_empty());
    }

    #[test]
    fn mcp_operation_keeps_logical_network_effect_without_transport_secrets() {
        let factory = FaultFactory::new(FaultStage::Discover);
        let config = fault_config(factory, Duration::from_secs(1));
        let request = mcp_operation_capabilities(
            &config,
            "lookup",
            &McpToolAnnotations {
                read_only_hint: Some(true),
                ..McpToolAnnotations::default()
            },
        );

        assert_eq!(request.effects, BTreeSet::from([EffectScope::Network]));
        assert_eq!(
            request
                .resources_for(EffectScope::Network)
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([CapabilitySelector::Exact(
                "http://127.0.0.1/fault-mcp".to_owned()
            )])
        );
    }

    #[test]
    fn mcp_tool_error_prefers_structured_content_then_plain_text() {
        assert_eq!(
            mcp_tool_error_message(&json!({
                "structuredContent": {"error": {"code": "bad_args"}},
                "content": [{"type": "text", "text": "fallback"}],
                "isError": true
            })),
            "code: bad_args"
        );
        assert_eq!(
            mcp_tool_error_message(&json!({
                "content": [
                    {"type": "text", "text": "first"},
                    {"type": "image", "data": "ignored"},
                    {"type": "text", "text": "second"}
                ],
                "isError": true
            })),
            "first\nsecond"
        );
        assert_eq!(
            mcp_tool_error_message(&json!({
                "content": [{
                    "type": "text",
                    "text": serde_json::to_string(&json!({
                        "capability_schema": {
                            "how_to_get": "Call search_capabilities first"
                        },
                        "reason": "schema omitted"
                    })).unwrap()
                }],
                "isError": true
            })),
            "reason: schema omitted; how_to_get: Call search_capabilities first"
        );
    }

    #[test]
    fn mcp_idempotency_follows_standard_annotations() {
        assert_eq!(
            mcp_tool_idempotency(&McpToolAnnotations {
                read_only_hint: Some(true),
                ..McpToolAnnotations::default()
            }),
            ToolIdempotency::Idempotent
        );
        assert_eq!(
            mcp_tool_idempotency(&McpToolAnnotations {
                idempotent_hint: Some(true),
                ..McpToolAnnotations::default()
            }),
            ToolIdempotency::Idempotent
        );
        assert_eq!(
            mcp_tool_idempotency(&McpToolAnnotations::default()),
            ToolIdempotency::NonIdempotent
        );
        assert!(matches!(
            mcp_unknown_effect_outcome(
                &McpToolAnnotations {
                    read_only_hint: Some(true),
                    ..McpToolAnnotations::default()
                },
                "timed out".to_owned()
            ),
            ToolOutcome::Failed {
                ref code,
                retryable: true,
                ..
            } if code == "mcp_call_interrupted"
        ));
        assert!(matches!(
            mcp_unknown_effect_outcome(&McpToolAnnotations::default(), "timed out".to_owned()),
            ToolOutcome::UnknownEffect { .. }
        ));
    }

    #[tokio::test]
    async fn workspace_auto_policy_runs_unannotated_registered_mcp_without_prompt() {
        let factory = FaultFactory::new(FaultStage::Healthy);
        let config = fault_config(factory.clone(), Duration::from_secs(1));
        let mut bounds = authority_bounds(factory.authority());
        bounds.approval = ApprovalPolicy::NotRequired;
        let journal = Arc::new(InMemoryToolEffectJournalStore::default());
        let verifier =
            HostApprovalVerifier::new(GATE_SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
                .unwrap();
        let runtime = Arc::new(
            GuardedToolRuntime::new_with_effect_journal(
                HostToolPolicy {
                    bounds: bounds.clone(),
                },
                verifier,
                journal.clone(),
            )
            .unwrap()
            .with_permission_policy(Arc::new(crate::tool_runtime::WorkspacePermissionPolicy)),
        );
        let registry = McpToolsAdapterRegistry::register(
            runtime.as_ref(),
            vec![config],
            ToolRestriction {
                bounds: bounds.clone(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let run_id = RunId::new("auto-mcp");
        let call_id = ToolCallId::new("call-1");
        let result = runtime
            .invoke(
                ToolInvocation {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    tool_id: ToolId::new("mcp/fault-healthy/echo/v1"),
                    arguments: json!({}),
                },
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await;

        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Completed { .. },
                cached: false
            }
        ));
        let key = ToolEffectKey::new(run_id, call_id);
        let records = journal.load_effect(&key).await.unwrap();
        let projection = replay_tool_effect(&key, &records).unwrap().unwrap();
        assert_eq!(
            projection.prepared.effect_scopes,
            BTreeSet::from([EffectScope::ExternalSideEffect])
        );
        registry.shutdown().await;
        factory.state.assert_released();
    }

    fn gate_runtime(
        bounds: ToolPolicyBounds,
        journal: Arc<InMemoryToolEffectJournalStore>,
    ) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
        let verifier =
            HostApprovalVerifier::new(GATE_SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
                .unwrap();
        Arc::new(
            GuardedToolRuntime::new_with_effect_journal(
                HostToolPolicy { bounds },
                verifier,
                journal,
            )
            .unwrap(),
        )
    }

    fn gate_runtime_with_artifacts(
        bounds: ToolPolicyBounds,
        journal: Arc<InMemoryToolEffectJournalStore>,
        artifacts: ToolArtifactStore,
    ) -> Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>> {
        let verifier =
            HostApprovalVerifier::new(GATE_SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
                .unwrap();
        Arc::new(
            GuardedToolRuntime::new_with_effect_journal_and_artifacts(
                HostToolPolicy { bounds },
                verifier,
                journal,
                artifacts,
            )
            .unwrap(),
        )
    }

    async fn invoke_with_approval(
        runtime: &GuardedToolRuntime<InMemoryApprovalCapabilityStore>,
        invocation: ToolInvocation,
        grant: RunToolGrant,
    ) -> GuardedToolResult {
        let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
            .invoke(
                invocation.clone(),
                grant.clone(),
                None,
                CancellationToken::new(),
            )
            .await
        else {
            panic!("MCP invocation bypassed exact Host approval");
        };
        let capability = HostApprovalIssuer::new(GATE_SIGNING_KEY)
            .unwrap()
            .issue(binding, i64::MAX)
            .unwrap();
        runtime
            .invoke(
                invocation,
                grant,
                Some(capability),
                CancellationToken::new(),
            )
            .await
    }

    #[tokio::test]
    async fn one_thousand_mcp_calls_are_journaled_filtered_and_share_one_connection() {
        let factory = FaultFactory::new(FaultStage::Healthy);
        let mut config = fault_config(factory.clone(), Duration::from_secs(2));
        config.server_id = McpServerId::new("gate");
        config.enabled_tools =
            BTreeSet::from(["echo".to_owned(), "beta".to_owned(), "hidden".to_owned()]);
        config.disabled_tools = BTreeSet::from(["hidden".to_owned()]);
        let bounds = authority_bounds(factory.authority());
        let journal = Arc::new(InMemoryToolEffectJournalStore::default());
        let runtime = gate_runtime(bounds.clone(), journal.clone());
        let registry = McpToolsAdapterRegistry::register(
            runtime.as_ref(),
            vec![config],
            ToolRestriction {
                bounds: bounds.clone(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let manager = registry.manager(&McpServerId::new("gate")).unwrap();

        assert_eq!(registry.tool_count(), 2);
        assert_eq!(
            manager
                .snapshot()
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["beta", "echo"])
        );
        assert!(runtime
            .resolve_tool_id("mcp__gate__hidden")
            .unwrap()
            .is_none());

        let mut committed = 0;
        for index in 0..GATE_CASES {
            let dispatches_before = factory.state.tool_call_dispatches.load(Ordering::SeqCst);
            for rejected_name in ["hidden", "stale-cached-tool"] {
                assert!(matches!(
                    manager
                        .invoke(rejected_name, json!({}), CancellationToken::new())
                        .await,
                    Err(GuardedMcpCallError::Rejected(_))
                ));
            }
            assert_eq!(
                factory.state.tool_call_dispatches.load(Ordering::SeqCst),
                dispatches_before
            );

            let tool = if index % 2 == 0 { "echo" } else { "beta" };
            let run_id = RunId::new("m4-mcp-gate");
            let call_id = ToolCallId::new(format!("call-{index}"));
            let invocation = ToolInvocation {
                run_id: run_id.clone(),
                call_id: call_id.clone(),
                tool_id: ToolId::new(format!("mcp/gate/{tool}/v1")),
                arguments: json!({}),
            };
            let result = invoke_with_approval(
                runtime.as_ref(),
                invocation,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
            )
            .await;
            assert!(matches!(
                result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Completed {
                        output: ToolOutput::Inline(_)
                    },
                    cached: false,
                }
            ));

            let key = ToolEffectKey::new(run_id, call_id);
            let records = journal.load_effect(&key).await.unwrap();
            let projection = replay_tool_effect(&key, &records).unwrap().unwrap();
            assert!(matches!(
                projection.phase,
                ToolEffectPhase::Committed { .. }
            ));
            committed += 1;
        }

        assert_eq!(
            factory.state.tool_call_dispatches.load(Ordering::SeqCst),
            committed
        );
        assert_eq!(factory.state.connects.load(Ordering::SeqCst), 1);
        assert_eq!(manager.connection_generation(), 1);
        assert_eq!(factory.state.active_connections.load(Ordering::SeqCst), 1);
        registry.shutdown().await;
        factory.state.assert_released();
        assert_eq!(factory.state.explicit_closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn one_thousand_oversized_mcp_results_keep_only_verified_artifact_summaries() {
        let factory = FaultFactory::new(FaultStage::LargeResult);
        let mut config = fault_config(factory.clone(), Duration::from_secs(2));
        config.server_id = McpServerId::new("spill-gate");
        config.enabled_tools = BTreeSet::from(["echo".to_owned()]);
        let mut bounds = authority_bounds(factory.authority());
        bounds.max_output_bytes = Some(128);
        let journal = Arc::new(InMemoryToolEffectJournalStore::default());
        let artifacts =
            ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 128 * 1024, 96).unwrap();
        let runtime =
            gate_runtime_with_artifacts(bounds.clone(), journal.clone(), artifacts.clone());
        let registry = McpToolsAdapterRegistry::register(
            runtime.as_ref(),
            vec![config],
            ToolRestriction {
                bounds: bounds.clone(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();

        for index in 0..GATE_CASES {
            let run_id = RunId::new("m4-mcp-spill-gate");
            let call_id = ToolCallId::new(format!("spill-{index}"));
            let result = invoke_with_approval(
                runtime.as_ref(),
                ToolInvocation {
                    run_id: run_id.clone(),
                    call_id: call_id.clone(),
                    tool_id: ToolId::new("mcp/spill-gate/echo/v1"),
                    arguments: json!({}),
                },
                RunToolGrant {
                    bounds: bounds.clone(),
                },
            )
            .await;
            let GuardedToolResult::Outcome {
                outcome:
                    ToolOutcome::Completed {
                        output: ToolOutput::Artifact(artifact),
                    },
                cached: false,
            } = result
            else {
                panic!("oversized MCP output escaped artifact spill")
            };
            artifact.validate().unwrap();
            assert!(!artifact.summary.trim().is_empty());
            assert!(artifact.byte_size > 128);
            let bytes = artifacts.resolve(&artifact).await.unwrap();
            let resolved: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(resolved["result"]["content"][0]["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("large-mcp-result/")));

            let key = ToolEffectKey::new(run_id, call_id);
            let projection = replay_tool_effect(&key, &journal.load_effect(&key).await.unwrap())
                .unwrap()
                .unwrap();
            assert!(matches!(
                projection.phase,
                ToolEffectPhase::Committed {
                    outcome: ToolOutcome::Completed {
                        output: ToolOutput::Artifact(_)
                    },
                    ..
                }
            ));
        }
        assert_eq!(
            factory.state.tool_call_dispatches.load(Ordering::SeqCst),
            GATE_CASES
        );
        registry.shutdown().await;
        factory.state.assert_released();
    }

    #[tokio::test]
    async fn one_thousand_unauthorized_environment_and_credential_bindings_connect_zero_times() {
        let mut baseline_error = None;
        for index in 0..GATE_CASES {
            let authority = if index % 2 == 0 {
                McpTransportAuthority {
                    kind: McpTransportKind::Stdio,
                    binding_digest: Digest::sha256(format!("unauthorized-env-{index}")),
                    effect_scopes: BTreeSet::from([
                        EffectScope::Process,
                        EffectScope::FilesystemRead,
                        EffectScope::FilesystemWrite,
                        EffectScope::ExternalSideEffect,
                        EffectScope::SecretRead,
                    ]),
                    process_programs: BTreeSet::from(["/fault/mcp".to_owned()]),
                    allow_child_processes: false,
                    allow_host_ui: false,
                    filesystem_read_roots: BTreeSet::from(["/fault/read".to_owned()]),
                    filesystem_write_roots: BTreeSet::from(["/fault/write".to_owned()]),
                    sandbox_profiles: BTreeSet::from([MCP_STDIO_SANDBOX_PROFILE.to_owned()]),
                    network_targets: BTreeSet::new(),
                    allow_unrestricted_network: false,
                    environment_variables: BTreeSet::from([format!("SENTINEL_ENV_{index}")]),
                    credential_references: BTreeSet::new(),
                }
            } else {
                McpTransportAuthority {
                    kind: McpTransportKind::StreamableHttp,
                    binding_digest: Digest::sha256(format!("unauthorized-credential-{index}")),
                    effect_scopes: BTreeSet::from([
                        EffectScope::Network,
                        EffectScope::ExternalSideEffect,
                        EffectScope::SecretRead,
                    ]),
                    process_programs: BTreeSet::new(),
                    allow_child_processes: false,
                    allow_host_ui: false,
                    filesystem_read_roots: BTreeSet::new(),
                    filesystem_write_roots: BTreeSet::new(),
                    sandbox_profiles: BTreeSet::new(),
                    network_targets: BTreeSet::from(["http://127.0.0.1/fault-mcp".to_owned()]),
                    allow_unrestricted_network: false,
                    environment_variables: BTreeSet::new(),
                    credential_references: BTreeSet::from([format!("env:SENTINEL_TOKEN_{index}")]),
                }
            };
            authority.validate().unwrap();
            let factory = FaultFactory::with_authority(authority);
            let mut config = fault_config(factory.clone(), Duration::from_secs(1));
            config.server_id = McpServerId::new("authority-gate");
            let mut restriction_bounds = authority_bounds(factory.authority());
            restriction_bounds.environment.allowed_variables.clear();
            restriction_bounds.allowed_credentials.clear();
            let runtime = gate_runtime(
                restriction_bounds.clone(),
                Arc::new(InMemoryToolEffectJournalStore::default()),
            );
            let result = McpToolsAdapterRegistry::register(
                runtime.as_ref(),
                vec![config],
                ToolRestriction {
                    bounds: restriction_bounds,
                },
                CancellationToken::new(),
            )
            .await;
            let error = match result {
                Err(McpToolsAdapterError::InvalidConfig(message)) => message,
                Err(other) => panic!("unexpected authority rejection: {other:?}"),
                Ok(registry) => {
                    registry.shutdown().await;
                    panic!("unauthorized MCP authority reached connect")
                }
            };
            if let Some(baseline) = &baseline_error {
                assert_eq!(&error, baseline);
            } else {
                baseline_error = Some(error);
            }
            assert_eq!(factory.state.connects.load(Ordering::SeqCst), 0);
            factory.state.assert_released();
        }
    }

    #[tokio::test]
    async fn one_thousand_disconnects_reconnect_once_and_schema_drift_never_calls_stale_tools() {
        for stage in [FaultStage::ReconnectStable, FaultStage::SchemaChanged] {
            let mut baseline_schema_error = None;
            for _ in 0..GATE_CASES {
                let factory = FaultFactory::new(stage);
                let manager = connect_result(
                    fault_config(factory.clone(), Duration::from_secs(1)),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
                assert_unknown_effect(
                    manager
                        .invoke("echo", json!({}), CancellationToken::new())
                        .await,
                );
                assert_eq!(manager.health(), McpServerHealth::Degraded);
                factory.state.assert_released();
                assert_eq!(factory.state.explicit_closes.load(Ordering::SeqCst), 1);

                let second = manager
                    .invoke("echo", json!({}), CancellationToken::new())
                    .await;
                match stage {
                    FaultStage::ReconnectStable => {
                        assert!(second.is_ok());
                        assert_eq!(manager.connection_generation(), 2);
                        assert_eq!(factory.state.active_connections.load(Ordering::SeqCst), 1);
                        assert_eq!(factory.state.tool_call_dispatches.load(Ordering::SeqCst), 2);
                        manager.shutdown().await;
                        assert_eq!(factory.state.explicit_closes.load(Ordering::SeqCst), 2);
                    }
                    FaultStage::SchemaChanged => {
                        let message = match second {
                            Err(GuardedMcpCallError::Failed(message)) => message,
                            _ => panic!("schema drift did not fail closed"),
                        };
                        if let Some(baseline) = &baseline_schema_error {
                            assert_eq!(&message, baseline);
                        } else {
                            baseline_schema_error = Some(message);
                        }
                        assert_eq!(manager.connection_generation(), 1);
                        assert_eq!(factory.state.tool_call_dispatches.load(Ordering::SeqCst), 1);
                        assert_eq!(factory.state.explicit_closes.load(Ordering::SeqCst), 2);
                    }
                    _ => unreachable!(),
                }
                assert_eq!(factory.state.connects.load(Ordering::SeqCst), 2);
                factory.state.assert_released();
            }
        }
    }

    #[tokio::test]
    async fn one_thousand_required_start_and_name_conflicts_are_deterministic_and_clean() {
        let start_factory = FaultFactory::new(FaultStage::Connect);
        let start_config = fault_config(start_factory.clone(), Duration::from_millis(1));
        let start_bounds = authority_bounds(start_factory.authority());
        let start_runtime = gate_runtime(
            start_bounds.clone(),
            Arc::new(InMemoryToolEffectJournalStore::default()),
        );
        let mut baseline_start_error = None;
        for _ in 0..GATE_CASES {
            let result = McpToolsAdapterRegistry::register(
                start_runtime.as_ref(),
                vec![start_config.clone()],
                ToolRestriction {
                    bounds: start_bounds.clone(),
                },
                CancellationToken::new(),
            )
            .await;
            let error = match result {
                Err(error) => error.to_string(),
                Ok(registry) => {
                    registry.shutdown().await;
                    panic!("required MCP startup failure was silently skipped")
                }
            };
            if let Some(baseline) = &baseline_start_error {
                assert_eq!(&error, baseline);
            } else {
                baseline_start_error = Some(error);
            }
            start_factory.state.assert_released();
        }
        assert_eq!(
            start_factory.state.connects.load(Ordering::SeqCst),
            GATE_CASES
        );

        let conflict_factory = FaultFactory::new(FaultStage::NameConflict);
        let conflict_config = fault_config(conflict_factory.clone(), Duration::from_secs(1));
        let conflict_bounds = authority_bounds(conflict_factory.authority());
        let conflict_runtime = gate_runtime(
            conflict_bounds.clone(),
            Arc::new(InMemoryToolEffectJournalStore::default()),
        );
        let mut baseline_conflict_error = None;
        for _ in 0..GATE_CASES {
            let result = McpToolsAdapterRegistry::register(
                conflict_runtime.as_ref(),
                vec![conflict_config.clone()],
                ToolRestriction {
                    bounds: conflict_bounds.clone(),
                },
                CancellationToken::new(),
            )
            .await;
            let error = match result {
                Err(McpToolsAdapterError::Conflict(message)) => message,
                Err(other) => panic!("unexpected name conflict result: {other:?}"),
                Ok(registry) => {
                    registry.shutdown().await;
                    panic!("MCP name collision was silently registered")
                }
            };
            if let Some(baseline) = &baseline_conflict_error {
                assert_eq!(&error, baseline);
            } else {
                baseline_conflict_error = Some(error);
            }
            conflict_factory.state.assert_released();
        }
        assert_eq!(
            conflict_factory.state.connects.load(Ordering::SeqCst),
            GATE_CASES
        );
        assert_eq!(
            conflict_factory
                .state
                .explicit_closes
                .load(Ordering::SeqCst),
            GATE_CASES
        );
    }

    #[tokio::test]
    async fn one_thousand_deadlines_per_mcp_stage_never_escape_or_leak_a_session() {
        let startup_stages = [
            FaultStage::Connect,
            FaultStage::Discover,
            FaultStage::Initialize,
            FaultStage::List,
        ];
        for stage in startup_stages {
            let factory = FaultFactory::new(stage);
            let config = fault_config(factory.clone(), Duration::from_millis(1));
            let mut baseline_error = None;
            for _ in 0..GATE_CASES {
                let error = match connect_result(config.clone(), CancellationToken::new()).await {
                    Ok(manager) => {
                        manager.shutdown().await;
                        panic!("fault stage {stage:?} escaped its deadline")
                    }
                    Err(error) => error.to_string(),
                };
                if let Some(baseline) = &baseline_error {
                    assert_eq!(&error, baseline);
                } else {
                    baseline_error = Some(error);
                }
                factory.state.assert_released();
            }
            assert_eq!(factory.state.connects.load(Ordering::SeqCst), GATE_CASES);
        }

        for stage in [FaultStage::Call, FaultStage::BodyDecode] {
            let factory = FaultFactory::new(stage);
            let config = fault_config(factory.clone(), Duration::from_millis(1));
            let mut baseline_error = None;
            for _ in 0..GATE_CASES {
                let manager = connect_result(config.clone(), CancellationToken::new())
                    .await
                    .unwrap();
                let result = manager
                    .invoke("echo", json!({}), CancellationToken::new())
                    .await;
                let message = match &result {
                    Err(GuardedMcpCallError::UnknownEffect(message)) => message.clone(),
                    _ => panic!("fault stage {stage:?} did not become UnknownEffect"),
                };
                if let Some(baseline) = &baseline_error {
                    assert_eq!(&message, baseline);
                } else {
                    baseline_error = Some(message);
                }
                assert_unknown_effect(result);
                assert_eq!(manager.health(), McpServerHealth::Degraded);
                assert_eq!(
                    factory
                        .state
                        .completed_call_responses
                        .load(Ordering::SeqCst),
                    0
                );
                factory.state.assert_released();
            }
            assert_eq!(factory.state.connects.load(Ordering::SeqCst), GATE_CASES);
        }
    }

    #[tokio::test]
    async fn one_thousand_connect_read_and_call_cancellations_release_within_one_second() {
        let mut connect_latencies = Vec::with_capacity(GATE_CASES);
        let mut read_latencies = Vec::with_capacity(GATE_CASES);
        let mut call_latencies = Vec::with_capacity(GATE_CASES);

        for _ in 0..GATE_CASES {
            let factory = FaultFactory::new(FaultStage::Connect);
            let config = fault_config(factory.clone(), Duration::from_secs(30));
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let task = tokio::spawn(async move { connect_result(config, task_cancellation).await });
            factory.state.entered.notified().await;
            let started = Instant::now();
            cancellation.cancel();
            let result = timeout(Duration::from_secs(1), task)
                .await
                .expect("MCP connect cancellation exceeded one second")
                .unwrap();
            assert!(matches!(result, Err(McpToolsAdapterError::Cancelled)));
            connect_latencies.push(started.elapsed());
            factory.state.assert_released();
        }

        for _ in 0..GATE_CASES {
            let factory = FaultFactory::new(FaultStage::List);
            let config = fault_config(factory.clone(), Duration::from_secs(30));
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let task = tokio::spawn(async move { connect_result(config, task_cancellation).await });
            factory.state.entered.notified().await;
            let started = Instant::now();
            cancellation.cancel();
            let result = timeout(Duration::from_secs(1), task)
                .await
                .expect("MCP read cancellation exceeded one second")
                .unwrap();
            assert!(matches!(result, Err(McpToolsAdapterError::Cancelled)));
            read_latencies.push(started.elapsed());
            factory.state.assert_released();
        }

        for _ in 0..GATE_CASES {
            let factory = FaultFactory::new(FaultStage::Call);
            let manager = connect_result(
                fault_config(factory.clone(), Duration::from_secs(30)),
                CancellationToken::new(),
            )
            .await
            .unwrap();
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let task =
                tokio::spawn(
                    async move { manager.invoke("echo", json!({}), task_cancellation).await },
                );
            factory.state.entered.notified().await;
            let started = Instant::now();
            cancellation.cancel();
            let result = timeout(Duration::from_secs(1), task)
                .await
                .expect("MCP call cancellation exceeded one second")
                .unwrap();
            assert_unknown_effect(result);
            call_latencies.push(started.elapsed());
            assert_eq!(
                factory
                    .state
                    .completed_call_responses
                    .load(Ordering::SeqCst),
                0
            );
            factory.state.assert_released();
        }

        assert!(p99(&mut connect_latencies) <= Duration::from_secs(1));
        assert!(p99(&mut read_latencies) <= Duration::from_secs(1));
        assert!(p99(&mut call_latencies) <= Duration::from_secs(1));
    }

    fn p99(latencies: &mut [Duration]) -> Duration {
        latencies.sort_unstable();
        latencies[(latencies.len() * 99).div_ceil(100) - 1]
    }
}
