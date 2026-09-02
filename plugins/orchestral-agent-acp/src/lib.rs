//! Provider-neutral Agent Client Protocol (ACP) integration.
//!
//! ACP wire names and compatibility handling remain in this plugin. The
//! Orchestral core sees only [`AgentConnector`](orchestral_core::agent_connector::AgentConnector)
//! and [`AgentProvider`](orchestral_core::agent_protocol::spi::AgentProvider).

mod provider;
mod transport;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};

use async_trait::async_trait;
use orchestral_core::agent_connector::{
    AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorErrorCode,
    AgentConnectorHealth, AgentConnectorId, AgentSessionActivity, AgentSessionActivityId,
    AgentSessionActivityKind, AgentSessionActivityStatus, AgentSessionCapabilities,
    AgentSessionCreationDescriptor, AgentSessionDetail, AgentSessionListQuery, AgentSessionPage,
    AgentSessionState, AgentSessionSummary, AgentSessionTurn, AgentSessionTurnId,
    AgentSessionTurnStatus, CreateAgentSessionRequest,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, Content, ProviderBindingRef};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

pub use transport::{AcpProcessConfig, AcpTransportError};
use transport::{AcpRpcClient, AcpTransportEvent};

const ACP_PROTOCOL_VERSION: u64 = 1;

#[derive(Debug, Clone)]
pub struct AcpConnectorConfig {
    pub process: AcpProcessConfig,
    pub connector_id: String,
    pub display_name: String,
}

impl AcpConnectorConfig {
    pub fn new(
        connector_id: impl Into<String>,
        display_name: impl Into<String>,
        process: AcpProcessConfig,
    ) -> Self {
        Self {
            process,
            connector_id: connector_id.into(),
            display_name: display_name.into(),
        }
    }

    pub fn opencode() -> Self {
        Self::new(
            "acp/opencode",
            "OpenCode",
            AcpProcessConfig::new("opencode").with_args(["acp"]),
        )
    }
}

struct ConnectedClient {
    rpc: Arc<AcpRpcClient>,
    version: Option<String>,
}

struct CachedPage {
    sessions: Vec<AgentSessionSummary>,
    remote_next_cursor: Option<String>,
}

pub struct AcpConnector {
    config: AcpConnectorConfig,
    client: AsyncMutex<Option<Arc<ConnectedClient>>>,
    sessions: StdMutex<BTreeMap<AgentSessionId, AgentSessionSummary>>,
    page_remainders: StdMutex<HashMap<String, CachedPage>>,
    provider_state: StdMutex<provider::ProviderState>,
}

impl AcpConnector {
    pub fn new(config: AcpConnectorConfig) -> Self {
        Self {
            config,
            client: AsyncMutex::new(None),
            sessions: StdMutex::new(BTreeMap::new()),
            page_remainders: StdMutex::new(HashMap::new()),
            provider_state: StdMutex::new(provider::ProviderState::default()),
        }
    }

    async fn client(&self) -> Result<Arc<ConnectedClient>, AgentConnectorError> {
        let mut client = self.client.lock().await;
        if let Some(client) = client.as_ref().filter(|client| client.rpc.is_connected()) {
            return Ok(Arc::clone(client));
        }
        *client = None;
        self.provider_state().reset_connection_state();
        let rpc = AcpRpcClient::spawn(&self.config.process)
            .await
            .map_err(connector_transport_error)?;
        let initialized = rpc
            .request(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientCapabilities": {},
                    "clientInfo": {
                        "name": "orchestral",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            )
            .await
            .map_err(connector_transport_error)?;
        if initialized.get("protocolVersion").and_then(Value::as_u64) != Some(ACP_PROTOCOL_VERSION)
        {
            return Err(AgentConnectorError::protocol(
                "ACP Agent negotiated an unsupported protocol version",
            ));
        }
        validate_required_capabilities(&initialized)?;
        let version = initialized
            .pointer("/agentInfo/version")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let connected = Arc::new(ConnectedClient { rpc, version });
        *client = Some(Arc::clone(&connected));
        Ok(connected)
    }

    fn connector_id(&self) -> AgentConnectorId {
        AgentConnectorId::new(self.config.connector_id.clone())
    }

    fn provider_binding(&self) -> ProviderBindingRef {
        ProviderBindingRef::new(self.config.connector_id.clone())
    }

    fn provider_state(&self) -> MutexGuard<'_, provider::ProviderState> {
        self.provider_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn session_summary(&self, value: &Value) -> Result<AgentSessionSummary, AgentConnectorError> {
        let session_id = required_string(value, "sessionId", "ACP session")?;
        let cwd = required_string(value, "cwd", "ACP session")?;
        let summary = AgentSessionSummary {
            connector_id: self.connector_id(),
            session_id: AgentSessionId::new(session_id),
            title: optional_string(value, "title")?,
            preview: None,
            cwd: Some(cwd),
            created_at_unix_ms: None,
            updated_at_unix_ms: None,
            state: AgentSessionState::Detached,
            extensions: BTreeMap::new(),
        };
        summary.validate_for(&self.connector_id())?;
        self.remember_session(summary.clone());
        Ok(summary)
    }

    fn remember_session(&self, summary: AgentSessionSummary) {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(summary.session_id.clone(), summary);
    }

    async fn resolve_summary(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        if let Some(summary) = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
        {
            return Ok(summary);
        }
        let client = self.client().await?;
        let mut cursor: Option<String> = None;
        let mut seen_cursors = std::collections::BTreeSet::new();
        loop {
            let mut params = json!({});
            if let Some(cursor) = cursor.as_ref() {
                params["cursor"] = Value::String(cursor.clone());
            }
            let result = client
                .rpc
                .request("session/list", params)
                .await
                .map_err(connector_transport_error)?;
            for value in result
                .get("sessions")
                .and_then(Value::as_array)
                .ok_or_else(|| AgentConnectorError::protocol("ACP session/list omitted sessions"))?
            {
                let summary = self.session_summary(value)?;
                if summary.session_id == *session_id {
                    return Ok(summary);
                }
            }
            cursor = optional_string(&result, "nextCursor")?;
            if cursor
                .as_ref()
                .is_some_and(|cursor| !seen_cursors.insert(cursor.clone()))
            {
                return Err(AgentConnectorError::protocol(
                    "ACP session/list repeated a pagination cursor",
                ));
            }
            if cursor.is_none() {
                return Err(AgentConnectorError::new(
                    AgentConnectorErrorCode::NotFound,
                    format!("ACP session does not exist: {session_id}"),
                    false,
                ));
            }
        }
    }

    #[cfg(test)]
    fn with_client(config: AcpConnectorConfig, rpc: Arc<AcpRpcClient>) -> Self {
        Self {
            config,
            client: AsyncMutex::new(Some(Arc::new(ConnectedClient {
                rpc,
                version: Some("fixture/1".to_owned()),
            }))),
            sessions: StdMutex::new(BTreeMap::new()),
            page_remainders: StdMutex::new(HashMap::new()),
            provider_state: StdMutex::new(provider::ProviderState::default()),
        }
    }
}

fn validate_required_capabilities(initialized: &Value) -> Result<(), AgentConnectorError> {
    if initialized
        .pointer("/agentCapabilities/loadSession")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err(AgentConnectorError::unsupported(
            "ACP connector requires the Agent to advertise loadSession",
        ));
    }
    if !initialized
        .pointer("/agentCapabilities/sessionCapabilities/list")
        .is_some_and(Value::is_object)
    {
        return Err(AgentConnectorError::unsupported(
            "ACP connector requires the Agent to advertise session/list",
        ));
    }
    Ok(())
}

impl Default for AcpConnector {
    fn default() -> Self {
        Self::new(AcpConnectorConfig::opencode())
    }
}

#[async_trait]
impl AgentConnector for AcpConnector {
    fn describe(&self) -> AgentConnectorDescriptor {
        AgentConnectorDescriptor {
            connector_id: self.connector_id(),
            provider_binding: self.provider_binding(),
            agent_family: "coding-agent".to_owned(),
            display_name: self.config.display_name.clone(),
            capabilities: AgentSessionCapabilities {
                list: true,
                read: true,
                create: true,
            },
            creation: Some(AgentSessionCreationDescriptor {
                accepts_cwd: true,
                default_cwd: std::env::current_dir()
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned()),
                input_schema: None,
                connection_hint: None,
            }),
            actions: Vec::new(),
        }
    }

    async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
        let client = self.client().await?;
        Ok(AgentConnectorHealth::ready(client.version.clone()))
    }

    async fn list_sessions(
        &self,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
        query.validate()?;
        if query.search.is_some() {
            return Err(AgentConnectorError::unsupported(
                "ACP 0.21 session/list does not define text search",
            ));
        }
        if let Some(cursor) = query
            .cursor
            .as_ref()
            .filter(|cursor| cursor.starts_with("orchestral-acp-page:"))
        {
            let cached = self
                .page_remainders
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(cursor)
                .ok_or_else(|| AgentConnectorError::invalid("ACP page cursor expired"))?;
            let page = page_from_cached(self, cached, query.limit);
            page.validate_for(&self.connector_id(), query.limit)?;
            return Ok(page);
        }

        let client = self.client().await?;
        let mut params = json!({});
        if let Some(cursor) = query.cursor {
            params["cursor"] = Value::String(cursor);
        }
        if let Some(cwd) = query.cwd {
            params["cwd"] = Value::String(cwd);
        }
        let result = client
            .rpc
            .request("session/list", params)
            .await
            .map_err(connector_transport_error)?;
        let sessions = result
            .get("sessions")
            .and_then(Value::as_array)
            .ok_or_else(|| AgentConnectorError::protocol("ACP session/list omitted sessions"))?
            .iter()
            .map(|value| self.session_summary(value))
            .collect::<Result<Vec<_>, _>>()?;
        let page = page_from_cached(
            self,
            CachedPage {
                sessions,
                remote_next_cursor: optional_string(&result, "nextCursor")?,
            },
            query.limit,
        );
        page.validate_for(&self.connector_id(), query.limit)?;
        Ok(page)
    }

    async fn read_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty"));
        }
        let mut summary = self.resolve_summary(session_id).await?;
        let cwd = summary
            .cwd
            .clone()
            .ok_or_else(|| AgentConnectorError::protocol("ACP session omitted cwd"))?;
        let client = self.client().await?;
        let mut events = client.rpc.subscribe();
        client
            .rpc
            .request(
                "session/load",
                json!({"sessionId": session_id.as_str(), "cwd": cwd, "mcpServers": []}),
            )
            .await
            .map_err(connector_transport_error)?;
        let updates = drain_session_updates(&mut events, session_id);
        summary.state = AgentSessionState::Idle;
        self.remember_session(summary.clone());
        let turns = history_turns(updates)?;
        let detail = AgentSessionDetail {
            summary,
            turns,
            pending_requests: Vec::new(),
            next_cursor: None,
        };
        detail.validate_for(&self.connector_id())?;
        Ok(detail)
    }

    async fn create_session(
        &self,
        request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        if request.title.is_some() || !request.options.is_null() || !request.extensions.is_empty() {
            return Err(AgentConnectorError::unsupported(
                "ACP session/new does not define title, options, or connector extensions",
            ));
        }
        let cwd = request
            .cwd
            .filter(|cwd| !cwd.trim().is_empty())
            .ok_or_else(|| AgentConnectorError::invalid("ACP session creation requires cwd"))?;
        let client = self.client().await?;
        let result = client
            .rpc
            .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
            .await
            .map_err(connector_transport_error)?;
        let session_id = required_string(&result, "sessionId", "ACP session/new response")?;
        let summary = AgentSessionSummary {
            connector_id: self.connector_id(),
            session_id: AgentSessionId::new(session_id),
            title: None,
            preview: None,
            cwd: Some(cwd),
            created_at_unix_ms: None,
            updated_at_unix_ms: None,
            state: AgentSessionState::Idle,
            extensions: BTreeMap::new(),
        };
        summary.validate_for(&self.connector_id())?;
        self.remember_session(summary.clone());
        self.provider_state()
            .loaded_sessions
            .insert(summary.session_id.clone());
        Ok(summary)
    }
}

fn page_from_cached(
    connector: &AcpConnector,
    mut cached: CachedPage,
    limit: u32,
) -> AgentSessionPage {
    let remaining = if cached.sessions.len() > limit as usize {
        cached.sessions.split_off(limit as usize)
    } else {
        Vec::new()
    };
    let next_cursor = if remaining.is_empty() {
        cached.remote_next_cursor
    } else {
        let token = format!("orchestral-acp-page:{}", uuid::Uuid::new_v4());
        connector
            .page_remainders
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                token.clone(),
                CachedPage {
                    sessions: remaining,
                    remote_next_cursor: cached.remote_next_cursor,
                },
            );
        Some(token)
    };
    AgentSessionPage {
        sessions: cached.sessions,
        next_cursor,
    }
}

fn drain_session_updates(
    events: &mut tokio::sync::broadcast::Receiver<AcpTransportEvent>,
    session_id: &AgentSessionId,
) -> Vec<Value> {
    let mut updates = Vec::new();
    loop {
        match events.try_recv() {
            Ok(AcpTransportEvent::Message(message))
                if message.get("method").and_then(Value::as_str) == Some("session/update")
                    && message.pointer("/params/sessionId").and_then(Value::as_str)
                        == Some(session_id.as_str()) =>
            {
                if let Some(update) = message.pointer("/params/update") {
                    updates.push(update.clone());
                }
            }
            Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            | Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
        }
    }
    updates
}

fn history_turns(updates: Vec<Value>) -> Result<Vec<AgentSessionTurn>, AgentConnectorError> {
    let mut activities = Vec::<AgentSessionActivity>::new();
    let mut indexes = HashMap::<String, usize>::new();
    for (sequence, update) in updates.into_iter().enumerate() {
        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let stable_id = update
            .get("messageId")
            .or_else(|| update.get("toolCallId"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("history-{sequence}"));
        let content = content_from_update(&update);
        if let Some(index) = indexes.get(&stable_id).copied() {
            if let Some(text) = content {
                activities[index].content.push(Content::text(text));
            }
            activities[index].status = activity_status(&update);
            activities[index].details = update;
            continue;
        }
        let activity_kind = match kind {
            "user_message_chunk" => AgentSessionActivityKind::UserMessage,
            "agent_message_chunk" => AgentSessionActivityKind::AgentMessage,
            "agent_thought_chunk" => AgentSessionActivityKind::Reasoning,
            "plan" => AgentSessionActivityKind::Plan,
            "tool_call" | "tool_call_update" => AgentSessionActivityKind::ToolCall,
            _ => AgentSessionActivityKind::Other,
        };
        let title = update
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let activity = AgentSessionActivity {
            activity_id: AgentSessionActivityId::new(format!("acp-{stable_id}")),
            kind: activity_kind,
            status: activity_status(&update),
            title,
            content: content.into_iter().map(Content::text).collect(),
            details: update,
        };
        indexes.insert(stable_id, activities.len());
        activities.push(activity);
    }
    if activities.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(vec![AgentSessionTurn {
            turn_id: AgentSessionTurnId::new("acp-loaded-history"),
            status: AgentSessionTurnStatus::Completed,
            activities,
        }])
    }
}

fn content_from_update(update: &Value) -> Option<String> {
    update
        .pointer("/content/text")
        .and_then(Value::as_str)
        .or_else(|| update.get("rawOutput").and_then(Value::as_str))
        .map(str::to_owned)
}

fn activity_status(update: &Value) -> AgentSessionActivityStatus {
    match update.get("status").and_then(Value::as_str) {
        Some("pending") => AgentSessionActivityStatus::Pending,
        Some("in_progress") => AgentSessionActivityStatus::Active,
        Some("failed") => AgentSessionActivityStatus::Failed,
        _ => AgentSessionActivityStatus::Completed,
    }
}

fn required_string(
    value: &Value,
    field: &str,
    context: &str,
) -> Result<String, AgentConnectorError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AgentConnectorError::protocol(format!("{context} omitted {field}")))
}

fn optional_string(value: &Value, field: &str) -> Result<Option<String>, AgentConnectorError> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(AgentConnectorError::protocol(format!(
            "ACP response returned invalid {field}"
        ))),
    }
}

fn connector_transport_error(error: AcpTransportError) -> AgentConnectorError {
    let (code, retryable) = match error {
        AcpTransportError::Spawn(_) => (AgentConnectorErrorCode::Unavailable, false),
        AcpTransportError::Io(_)
        | AcpTransportError::Closed
        | AcpTransportError::Disconnected(_)
        | AcpTransportError::Timeout => (AgentConnectorErrorCode::Unavailable, true),
        AcpTransportError::Rpc(_)
        | AcpTransportError::InvalidJson(_)
        | AcpTransportError::FrameTooLarge { .. }
        | AcpTransportError::MissingResult => (AgentConnectorErrorCode::Protocol, false),
    };
    AgentConnectorError::new(code, error.to_string(), retryable)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{duplex, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};

    use super::*;

    #[tokio::test]
    async fn connector_uses_acp_session_methods_and_normalizes_replayed_history() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = AcpRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = AcpConnector::with_client(
            AcpConnectorConfig::new(
                "acp/fixture",
                "ACP Fixture",
                AcpProcessConfig::new("unused"),
            ),
            rpc,
        );
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let list = read_request(&mut lines).await;
            assert_eq!(list["method"], "session/list");
            write_result(
                &mut server_write,
                &list,
                json!({"sessions":[{"sessionId":"s1","cwd":"/repo","title":"Fix"}]}),
            )
            .await;
            let load = read_request(&mut lines).await;
            assert_eq!(load["method"], "session/load");
            for update in [
                json!({"sessionUpdate":"user_message_chunk","messageId":"m1","content":{"type":"text","text":"fix"}}),
                json!({"sessionUpdate":"agent_message_chunk","messageId":"m2","content":{"type":"text","text":"done"}}),
                json!({"sessionUpdate":"tool_call","toolCallId":"t1","title":"cargo test","status":"completed"}),
            ] {
                server_write
                    .write_all(format!("{}\n", json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":update}})).as_bytes())
                    .await
                    .unwrap();
            }
            write_result(&mut server_write, &load, json!({})).await;
            let create = read_request(&mut lines).await;
            assert_eq!(create["method"], "session/new");
            write_result(&mut server_write, &create, json!({"sessionId":"s2"})).await;
        });

        let page = connector
            .list_sessions(AgentSessionListQuery::default())
            .await
            .unwrap();
        assert_eq!(page.sessions[0].session_id.as_str(), "s1");
        let detail = connector
            .read_session(&AgentSessionId::new("s1"))
            .await
            .unwrap();
        assert_eq!(detail.turns[0].activities.len(), 3);
        assert_eq!(
            detail.turns[0].activities[1].kind,
            AgentSessionActivityKind::AgentMessage
        );
        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some("/repo".to_owned()),
                title: None,
                options: Value::Null,
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(created.session_id.as_str(), "s2");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connector_pages_large_acp_responses_without_duplicates_or_omissions() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = AcpRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = AcpConnector::with_client(
            AcpConnectorConfig::new(
                "acp/fixture",
                "ACP Fixture",
                AcpProcessConfig::new("unused"),
            ),
            rpc,
        );
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let list = read_request(&mut lines).await;
            let sessions = (0..55)
                .map(|index| {
                    json!({
                        "sessionId": format!("s-{index:02}"),
                        "cwd": "/repo",
                        "title": format!("Session {index}")
                    })
                })
                .collect::<Vec<_>>();
            write_result(&mut server_write, &list, json!({"sessions":sessions})).await;
        });

        let first = connector
            .list_sessions(AgentSessionListQuery::default())
            .await
            .unwrap();
        assert_eq!(first.sessions.len(), 50);
        let second = connector
            .list_sessions(AgentSessionListQuery {
                cursor: first.next_cursor,
                ..AgentSessionListQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(second.sessions.len(), 5);
        assert!(second.next_cursor.is_none());
        let ids = first
            .sessions
            .into_iter()
            .chain(second.sessions)
            .map(|session| session.session_id.as_str().to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), 55);
        assert_eq!(ids.first().map(String::as_str), Some("s-00"));
        assert_eq!(ids.last().map(String::as_str), Some("s-54"));
        server.await.unwrap();
    }

    #[test]
    fn initialization_requires_the_optional_acp_directory_capabilities() {
        let supported = json!({
            "agentCapabilities": {
                "loadSession": true,
                "sessionCapabilities": {"list": {}}
            }
        });
        assert!(validate_required_capabilities(&supported).is_ok());
        assert!(validate_required_capabilities(&json!({
            "agentCapabilities": {"sessionCapabilities": {"list": {}}}
        }))
        .is_err());
        assert!(validate_required_capabilities(&json!({
            "agentCapabilities": {"loadSession": true}
        }))
        .is_err());
    }

    async fn read_request(
        lines: &mut tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    ) -> Value {
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn write_result<W: AsyncWrite + Unpin>(writer: &mut W, request: &Value, result: Value) {
        writer
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":request["id"],"result":result})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }
}
