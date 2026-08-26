use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::timeout;

use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::mcp_protocol::{
    McpProtocolEra, McpServerId, McpServerSnapshot, McpToolSnapshot, McpTransportAuthority,
    McpTransportCancellation, McpTransportConnection, McpTransportError, McpTransportFactory,
    McpTransportKind, McpTransportRequest, MCP_LATEST_LEGACY_PROTOCOL,
    MCP_STATELESS_PROTOCOL_2026_07_28,
};
use orchestral_core::tool_protocol::{
    ApprovalCapabilityStore, ApprovalPolicy, EffectScope, ModelToolSchema, ToolConcurrency,
    ToolDescriptor, ToolId, ToolIdempotency, ToolOutcome, ToolRestriction,
};
use tokio_util::sync::CancellationToken;

use crate::tool_runtime::{
    GuardedToolExecution, GuardedToolExecutor, GuardedToolRuntime, ToolRuntimeError,
};

const DEFAULT_MCP_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Built-in stdio transport factory. External transports implement the core
/// [`McpTransportFactory`] SPI from a plugin and are injected by the Host.
#[derive(Debug, Clone)]
pub struct StdioMcpTransportFactory {
    program: PathBuf,
    args: Vec<String>,
    environment: BTreeMap<String, String>,
    authority: McpTransportAuthority,
}

impl StdioMcpTransportFactory {
    pub fn new(
        program: PathBuf,
        args: Vec<String>,
        environment: BTreeMap<String, String>,
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
        let mut effect_scopes = BTreeSet::from([
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::ExternalSideEffect,
        ]);
        if !environment.is_empty() {
            effect_scopes.insert(EffectScope::SecretRead);
        }
        let binding = json!({
            "transport": "stdio",
            "program": program.to_string_lossy(),
            "args": args,
            "environmentNames": environment.keys().collect::<Vec<_>>(),
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
            network_targets: BTreeSet::new(),
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
        let environment = self
            .environment
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        StdioMcpTransport::connect(
            self.program.to_string_lossy().as_ref(),
            &self.args,
            &environment,
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

    pub fn allowed_network_targets(&self) -> BTreeSet<String> {
        self.transport.authority().network_targets.clone()
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
        let (session, snapshot) = discover_guarded_mcp_session(&config, &cancellation).await?;
        Ok(Arc::new(Self {
            config,
            snapshot,
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
        let mut guard = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(GuardedMcpCallError::Cancelled),
            guard = self.session.lock() => guard,
        };
        if guard.is_none() {
            self.health
                .store(McpServerHealth::Connecting.encode(), Ordering::Release);
            let (mut session, current_snapshot) =
                discover_guarded_mcp_session(&self.config, &cancellation)
                    .await
                    .map_err(|error| GuardedMcpCallError::Failed(error.to_string()))?;
            if current_snapshot.revision != self.snapshot.revision {
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
                        BTreeMap::new(),
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
                        "MCP call timed out after dispatch; remote effect is unknown".to_owned(),
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
    let mut session = spawn_guarded_mcp_session(config).await?;
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
            session = spawn_guarded_mcp_session(config).await?;
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
            session = spawn_guarded_mcp_session(config).await?;
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
) -> Result<McpTransportSession, McpToolsAdapterError> {
    let connection = config
        .transport
        .connect()
        .await
        .map_err(mcp_request_adapter_error)?;
    if connection.kind() != config.transport_kind() {
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
) -> Result<(McpTransportSession, McpServerSnapshot), McpToolsAdapterError> {
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
        Ok(snapshot) => Ok((session, snapshot)),
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

fn parse_guarded_tool_snapshot(
    config: &GuardedMcpServerConfig,
    negotiated: &NegotiatedMcpProtocol,
    result: &Value,
) -> Result<McpServerSnapshot, McpToolsAdapterError> {
    let tools = result
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            McpToolsAdapterError::Protocol("MCP tools/list omitted a tools array".to_owned())
        })?;
    let mut names = BTreeSet::new();
    let mut snapshots = Vec::new();
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
        snapshots.push(
            McpToolSnapshot::seal(
                config.server_id.clone(),
                name,
                raw.get("description").and_then(Value::as_str).unwrap_or(""),
                raw.get("inputSchema")
                    .cloned()
                    .unwrap_or_else(|| json!({"type": "object"})),
                raw.get("outputSchema").cloned(),
            )
            .map_err(|error| McpToolsAdapterError::Protocol(error.to_string()))?,
        );
    }
    McpServerSnapshot::seal(
        config.server_id.clone(),
        config.transport_kind(),
        config.transport.authority().binding_digest.clone(),
        negotiated.version.clone(),
        negotiated.era,
        snapshots,
    )
    .map_err(|error| McpToolsAdapterError::Protocol(error.to_string()))
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
        if restriction.bounds.approval != ApprovalPolicy::Required {
            return Err(McpToolsAdapterError::InvalidConfig(
                "MCP Tool restriction must require exact Host approval".to_owned(),
            ));
        }
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
            let network_targets = config.allowed_network_targets();
            let environment = config.environment_names();
            let credentials = config.credential_references();
            if !config
                .effect_scopes()
                .is_subset(&restriction.bounds.allowed_effects)
                || !programs.is_subset(&restriction.bounds.process.allowed_programs)
                || !network_targets.is_subset(&restriction.bounds.network.allowed_targets)
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

        let existing_names = runtime
            .model_tool_schemas()
            .map_err(McpToolsAdapterError::ToolRuntime)?
            .into_iter()
            .map(|schema| schema.name)
            .collect::<BTreeSet<_>>();
        let mut registrations = Vec::new();
        let mut model_names = BTreeSet::new();
        for manager in managers.values() {
            let mut sanitized_names = BTreeSet::new();
            for tool in &manager.snapshot.tools {
                let server = sanitize_mcp_identifier(manager.config.server_id.as_str());
                let tool_name = sanitize_mcp_identifier(&tool.name);
                if !sanitized_names.insert(tool_name.clone()) {
                    return Err(McpToolsAdapterError::Conflict(format!(
                        "MCP server '{}' has Tool names that collide after namespacing",
                        manager.config.server_id
                    )));
                }
                let model_name = format!("mcp__{server}__{tool_name}");
                if existing_names.contains(&model_name) || !model_names.insert(model_name.clone()) {
                    return Err(McpToolsAdapterError::Conflict(format!(
                        "MCP model Tool name collides: {model_name}"
                    )));
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
                    restriction: restriction.clone(),
                    idempotency: ToolIdempotency::NonIdempotent,
                    concurrency: ToolConcurrency::GlobalSerial,
                };
                descriptor
                    .validate()
                    .map_err(|error| McpToolsAdapterError::Protocol(error.to_string()))?;
                registrations.push((
                    descriptor,
                    Arc::new(GuardedMcpToolExecutor {
                        manager: manager.clone(),
                        tool_name: tool.name.clone(),
                    }) as Arc<dyn GuardedToolExecutor>,
                ));
            }
        }
        let tool_count = registrations.len();
        for (descriptor, executor) in registrations {
            runtime
                .register(descriptor, executor)
                .map_err(McpToolsAdapterError::ToolRuntime)?;
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

async fn shutdown_mcp_managers(managers: &BTreeMap<McpServerId, Arc<McpServerConnectionManager>>) {
    for manager in managers.values() {
        manager.shutdown().await;
    }
}

struct GuardedMcpToolExecutor {
    manager: Arc<McpServerConnectionManager>,
    tool_name: String,
}

#[async_trait]
impl GuardedToolExecutor for GuardedMcpToolExecutor {
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
        if execution.approval.is_none()
            || execution.effective_policy.bounds().approval != ApprovalPolicy::Required
        {
            return ToolOutcome::Rejected {
                code: "mcp_approval_missing".to_owned(),
                message: "MCP Tool requires verified Host approval".to_owned(),
            };
        }
        let bounds = execution.effective_policy.bounds();
        let programs = self.manager.config.allowed_programs();
        let network_targets = self.manager.config.allowed_network_targets();
        let environment = self.manager.config.environment_names();
        let credentials = self.manager.config.credential_references();
        if !programs.is_subset(&bounds.process.allowed_programs)
            || !network_targets.is_subset(&bounds.network.allowed_targets)
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
                ToolOutcome::UnknownEffect { message }
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
        return Err(GuardedMcpCallError::ToolError(result.to_string()));
    }
    Ok(result)
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
        self.connection
            .close()
            .await
            .map_err(|error| error.to_string())
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
    process_group_id: Option<u32>,
    max_frame_bytes: usize,
}

impl StdioMcpTransport {
    async fn connect(
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> Result<Self, String> {
        let mut cmd = Command::new(command);
        cmd.args(args);
        // MCP stdio servers receive only the Host-configured environment.
        cmd.env_clear();
        cmd.envs(env);
        isolate_mcp_process_group(&mut cmd);
        cmd.kill_on_drop(true)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());

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

        Ok(Self {
            state: tokio::sync::Mutex::new(StdioMcpTransportState {
                child,
                stdin,
                stdout: BufReader::new(stdout),
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
                        return Err("mcp process closed stdout".to_owned());
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
        Ok(())
    }
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
fn isolate_mcp_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn isolate_mcp_process_group(_command: &mut Command) {}

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
    let _ = timeout(Duration::from_secs(1), child.wait()).await;
}
