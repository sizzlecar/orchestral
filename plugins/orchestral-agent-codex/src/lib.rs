//! Codex integration through the supported `codex app-server` protocol.
//!
//! This crate intentionally depends only on Orchestral contracts. Codex wire
//! names and compatibility handling stay inside this concrete plugin.

mod normalize;
mod provider;
mod transport;

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use orchestral_core::agent_connector::{
    paginate_session_detail, AgentConnector, AgentConnectorDescriptor, AgentConnectorError,
    AgentConnectorErrorCode, AgentConnectorHealth, AgentConnectorId, AgentSessionActionDescriptor,
    AgentSessionActionExecution, AgentSessionActionId, AgentSessionActionOutcome,
    AgentSessionActionStatus, AgentSessionCapabilities, AgentSessionDetail, AgentSessionListQuery,
    AgentSessionPage, AgentSessionReadQuery, AgentSessionState, AgentSessionSummary,
    CreateAgentSessionRequest, InvokeAgentSessionActionRequest, SESSION_COMPACT_ACTION,
    SESSION_FORK_ACTION, SESSION_RENAME_ACTION, SESSION_REVIEW_ACTION,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

pub use transport::{CodexAppServerConfig, CodexAppServerEndpoint, CodexTransportError};

use crate::normalize::NormalizationLimits;
use crate::transport::CodexRpcClient;

const CONNECTOR_ID: &str = "codex/local";
const PROVIDER_BINDING: &str = "codex/local";
const SESSION_DETAIL_CACHE_ENTRIES: usize = 2;
const SESSION_LIST_CACHE_ENTRIES: usize = 8;
const SESSION_LIST_CACHE_TTL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SessionListCacheKey {
    cursor: Option<String>,
    limit: u32,
    cwd: Option<String>,
    search: Option<String>,
}

impl From<&AgentSessionListQuery> for SessionListCacheKey {
    fn from(query: &AgentSessionListQuery) -> Self {
        Self {
            cursor: query.cursor.clone(),
            limit: query.limit,
            cwd: query.cwd.clone(),
            search: query.search.clone(),
        }
    }
}

struct SessionListCacheEntry {
    inserted_at: Instant,
    page: AgentSessionPage,
}

#[derive(Default)]
struct SessionDetailCache {
    entries: BTreeMap<AgentSessionId, Arc<AgentSessionDetail>>,
    lru: VecDeque<AgentSessionId>,
}

impl SessionDetailCache {
    fn get_current(&mut self, summary: &AgentSessionSummary) -> Option<Arc<AgentSessionDetail>> {
        if !matches!(
            summary.state,
            AgentSessionState::Idle | AgentSessionState::Detached
        ) {
            return None;
        }
        let detail = self
            .entries
            .get(&summary.session_id)
            .filter(|detail| detail.summary == *summary)
            .cloned()?;
        self.touch(&summary.session_id);
        Some(detail)
    }

    fn insert(&mut self, detail: AgentSessionDetail) {
        let session_id = detail.summary.session_id.clone();
        self.entries.insert(session_id.clone(), Arc::new(detail));
        self.touch(&session_id);
        while self.entries.len() > SESSION_DETAIL_CACHE_ENTRIES {
            if let Some(evicted) = self.lru.pop_front() {
                self.entries.remove(&evicted);
            }
        }
    }

    fn remove(&mut self, session_id: &AgentSessionId) {
        self.entries.remove(session_id);
        self.lru.retain(|candidate| candidate != session_id);
    }

    fn touch(&mut self, session_id: &AgentSessionId) {
        self.lru.retain(|candidate| candidate != session_id);
        self.lru.push_back(session_id.clone());
    }
}

struct ConnectedClient {
    rpc: Arc<CodexRpcClient>,
    user_agent: String,
}

/// Long-lived local Codex connector. By default it attaches to Codex's shared
/// app-server daemon; UI subscribers never own native Codex connections.
pub struct CodexConnector {
    config: CodexAppServerConfig,
    client: AsyncMutex<Option<Arc<ConnectedClient>>>,
    limits: NormalizationLimits,
    provider_state: StdMutex<provider::ProviderState>,
    session_cache: StdMutex<SessionDetailCache>,
    session_list_cache: StdMutex<BTreeMap<SessionListCacheKey, SessionListCacheEntry>>,
    session_list_gate: AsyncMutex<()>,
    #[cfg(test)]
    reconnect_clients: StdMutex<VecDeque<Arc<ConnectedClient>>>,
}

impl CodexConnector {
    pub fn new(config: CodexAppServerConfig) -> Self {
        Self {
            config,
            client: AsyncMutex::new(None),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
            session_cache: StdMutex::new(SessionDetailCache::default()),
            session_list_cache: StdMutex::new(BTreeMap::new()),
            session_list_gate: AsyncMutex::new(()),
            #[cfg(test)]
            reconnect_clients: StdMutex::new(VecDeque::new()),
        }
    }

    async fn client(&self) -> Result<Arc<ConnectedClient>, AgentConnectorError> {
        let mut client = self.client.lock().await;
        if let Some(client) = client.as_ref().filter(|client| client.rpc.is_connected()) {
            return Ok(Arc::clone(client));
        }
        *client = None;
        self.invalidate_session_list_cache();
        self.provider_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reset_connection_state();
        #[cfg(test)]
        if let Some(connected) = self
            .reconnect_clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
        {
            *client = Some(Arc::clone(&connected));
            return Ok(connected);
        }
        let (rpc, user_agent) = CodexRpcClient::connect(&self.config)
            .await
            .map_err(connector_transport_error)?;
        let connected = Arc::new(ConnectedClient { rpc, user_agent });
        *client = Some(Arc::clone(&connected));
        Ok(connected)
    }

    async fn read_summary(
        &self,
        client: &ConnectedClient,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        let result = client
            .rpc
            .request(
                "thread/read",
                json!({"threadId": session_id.as_str(), "includeTurns": false}),
            )
            .await
            .map_err(connector_transport_error)?;
        let thread = result
            .get("thread")
            .ok_or_else(|| AgentConnectorError::protocol("thread/read omitted thread"))?;
        normalize::session_summary(&self.describe().connector_id, thread)
    }

    fn cache_session_detail(&self, detail: AgentSessionDetail) {
        self.session_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(detail);
    }

    fn cached_session_detail(
        &self,
        summary: &AgentSessionSummary,
    ) -> Option<Arc<AgentSessionDetail>> {
        self.session_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_current(summary)
    }

    fn invalidate_session_cache(&self, session_id: &AgentSessionId) {
        self.session_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
    }

    fn cached_session_page(&self, query: &AgentSessionListQuery) -> Option<AgentSessionPage> {
        let key = SessionListCacheKey::from(query);
        self.session_list_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .filter(|entry| entry.inserted_at.elapsed() <= SESSION_LIST_CACHE_TTL)
            .map(|entry| entry.page.clone())
    }

    fn cache_session_page(&self, query: &AgentSessionListQuery, page: AgentSessionPage) {
        let mut cache = self
            .session_list_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache.len() >= SESSION_LIST_CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(
            SessionListCacheKey::from(query),
            SessionListCacheEntry {
                inserted_at: Instant::now(),
                page,
            },
        );
    }

    fn invalidate_session_list_cache(&self) {
        self.session_list_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    #[cfg(test)]
    fn with_client(rpc: Arc<CodexRpcClient>, user_agent: impl Into<String>) -> Self {
        Self {
            config: CodexAppServerConfig {
                dispatch_journal_dir: None,
                ..CodexAppServerConfig::default()
            },
            client: AsyncMutex::new(Some(Arc::new(ConnectedClient {
                rpc,
                user_agent: user_agent.into(),
            }))),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
            session_cache: StdMutex::new(SessionDetailCache::default()),
            session_list_cache: StdMutex::new(BTreeMap::new()),
            session_list_gate: AsyncMutex::new(()),
            reconnect_clients: StdMutex::new(VecDeque::new()),
        }
    }

    #[cfg(test)]
    fn with_reconnect_client(rpc: Arc<CodexRpcClient>, reconnect_rpc: Arc<CodexRpcClient>) -> Self {
        Self {
            config: CodexAppServerConfig {
                dispatch_journal_dir: None,
                ..CodexAppServerConfig::default()
            },
            client: AsyncMutex::new(Some(Arc::new(ConnectedClient {
                rpc,
                user_agent: "codex/test".to_owned(),
            }))),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
            session_cache: StdMutex::new(SessionDetailCache::default()),
            session_list_cache: StdMutex::new(BTreeMap::new()),
            session_list_gate: AsyncMutex::new(()),
            reconnect_clients: StdMutex::new(VecDeque::from([Arc::new(ConnectedClient {
                rpc: reconnect_rpc,
                user_agent: "codex/test-reconnected".to_owned(),
            })])),
        }
    }
}

impl Default for CodexConnector {
    fn default() -> Self {
        Self::new(CodexAppServerConfig::default())
    }
}

#[async_trait]
impl AgentConnector for CodexConnector {
    fn describe(&self) -> AgentConnectorDescriptor {
        AgentConnectorDescriptor {
            connector_id: AgentConnectorId::new(CONNECTOR_ID),
            provider_binding: ProviderBindingRef::new(PROVIDER_BINDING),
            agent_family: "coding-agent".to_owned(),
            display_name: "Codex".to_owned(),
            capabilities: AgentSessionCapabilities {
                create: true,
                ..AgentSessionCapabilities::discoverable()
            },
            actions: vec![
                AgentSessionActionDescriptor {
                    action_id: AgentSessionActionId::new(SESSION_COMPACT_ACTION),
                    title: "Compact context".to_owned(),
                    description:
                        "Compact this session's native context while preserving its history"
                            .to_owned(),
                    input_schema: None,
                    execution: AgentSessionActionExecution::Run,
                },
                AgentSessionActionDescriptor {
                    action_id: AgentSessionActionId::new(SESSION_REVIEW_ACTION),
                    title: "Review changes".to_owned(),
                    description: "Run a native code review in this session".to_owned(),
                    input_schema: Some(json!({
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["target"],
                        "properties": {
                            "target": {
                                "type": "string",
                                "title": "Target (uncommitted_changes, base_branch, commit, custom)"
                            },
                            "branch": {"type": "string", "title": "Base branch"},
                            "sha": {"type": "string", "title": "Commit SHA"},
                            "title": {"type": "string", "title": "Commit title"},
                            "instructions": {"type": "string", "title": "Custom instructions"}
                        }
                    })),
                    execution: AgentSessionActionExecution::Run,
                },
                AgentSessionActionDescriptor {
                    action_id: AgentSessionActionId::new(SESSION_FORK_ACTION),
                    title: "Fork session".to_owned(),
                    description: "Create a new session from this session's persisted history"
                        .to_owned(),
                    input_schema: None,
                    execution: AgentSessionActionExecution::Immediate,
                },
                AgentSessionActionDescriptor {
                    action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                    title: "Rename session".to_owned(),
                    description: "Set the session's display name".to_owned(),
                    input_schema: Some(json!({
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["name"],
                        "properties": {"name": {"type": "string", "minLength": 1}}
                    })),
                    execution: AgentSessionActionExecution::Immediate,
                },
            ],
        }
    }

    async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
        let client = self.client().await?;
        Ok(AgentConnectorHealth::ready(Some(client.user_agent.clone())))
    }

    async fn list_sessions(
        &self,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
        query.validate()?;
        if let Some(page) = self.cached_session_page(&query) {
            return Ok(page);
        }
        let _gate = self.session_list_gate.lock().await;
        if let Some(page) = self.cached_session_page(&query) {
            return Ok(page);
        }
        let client = self.client().await?;
        let mut params = json!({
            "limit": query.limit,
            "sortKey": "updated_at"
        });
        insert_optional(
            &mut params,
            "cursor",
            query.cursor.clone().map(Value::String),
        );
        insert_optional(&mut params, "cwd", query.cwd.clone().map(Value::String));
        insert_optional(
            &mut params,
            "searchTerm",
            query.search.clone().map(Value::String),
        );
        let result = client
            .rpc
            .request("thread/list", params)
            .await
            .map_err(connector_transport_error)?;
        let page = normalize::session_page(&self.describe().connector_id, &result)?;
        page.validate_for(&self.describe().connector_id, query.limit)?;
        self.cache_session_page(&query, page.clone());
        Ok(page)
    }

    async fn read_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty"));
        }
        let client = self.client().await?;
        let result = match client
            .rpc
            .request(
                "thread/read",
                json!({"threadId": session_id.as_str(), "includeTurns": true}),
            )
            .await
        {
            Ok(result) => result,
            Err(error) if unmaterialized_thread_read(&error) => client
                .rpc
                .request(
                    "thread/read",
                    json!({"threadId": session_id.as_str(), "includeTurns": false}),
                )
                .await
                .map_err(connector_transport_error)?,
            Err(error) => return Err(connector_transport_error(error)),
        };
        let detail =
            normalize::session_detail(&self.describe().connector_id, &result, &self.limits)?;
        detail.validate_for(&self.describe().connector_id)?;
        self.cache_session_detail(detail.clone());
        Ok(detail)
    }

    async fn read_session_page(
        &self,
        session_id: &AgentSessionId,
        query: AgentSessionReadQuery,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        query.validate()?;
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty"));
        }
        let client = self.client().await?;
        let summary = self.read_summary(&client, session_id).await?;
        let detail = match self.cached_session_detail(&summary) {
            Some(detail) => detail,
            None => {
                let detail = self.read_session(session_id).await?;
                Arc::new(detail)
            }
        };
        paginate_session_detail(&detail, query)
    }

    async fn create_session(
        &self,
        request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        let cwd = non_empty_optional(request.cwd, "cwd")?;
        let title = non_empty_optional(request.title, "title")?;
        if !request.extensions.is_empty() {
            return Err(AgentConnectorError::invalid(
                "Codex session creation does not accept connector extensions",
            ));
        }
        let client = self.client().await?;
        let mut params = json!({
            "experimentalRawEvents": false,
            "persistExtendedHistory": true
        });
        insert_optional(&mut params, "cwd", cwd.map(Value::String));
        let result = client
            .rpc
            .request("thread/start", params)
            .await
            .map_err(connector_transport_error)?;
        let thread = result
            .get("thread")
            .ok_or_else(|| AgentConnectorError::protocol("thread/start omitted thread"))?;
        let mut summary = normalize::session_summary(&self.describe().connector_id, thread)?;
        if let Some(title) = title {
            client
                .rpc
                .request(
                    "thread/name/set",
                    json!({"threadId": summary.session_id.as_str(), "name": title}),
                )
                .await
                .map_err(|error| {
                    connector_transport_error(error).with_details(json!({
                        "createdSessionId": summary.session_id.as_str()
                    }))
                })?;
            summary.title = Some(title);
        }
        summary.validate_for(&self.describe().connector_id)?;
        self.provider_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .mark_loaded(summary.session_id.clone());
        self.cache_session_detail(AgentSessionDetail {
            summary: summary.clone(),
            turns: Vec::new(),
            pending_requests: Vec::new(),
            next_cursor: None,
        });
        self.invalidate_session_list_cache();
        Ok(summary)
    }

    async fn invoke_action(
        &self,
        request: InvokeAgentSessionActionRequest,
    ) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
        if request.session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty"));
        }
        let session = match request.action_id.as_str() {
            SESSION_FORK_ACTION => {
                if !request.arguments.is_null() {
                    return Err(AgentConnectorError::invalid(
                        "session.fork takes no arguments",
                    ));
                }
                let client = self.client().await?;
                let result = client
                    .rpc
                    .request(
                        "thread/fork",
                        json!({
                            "threadId": request.session_id.as_str(),
                            "persistExtendedHistory": true
                        }),
                    )
                    .await
                    .map_err(connector_transport_error)?;
                let thread = result
                    .get("thread")
                    .ok_or_else(|| AgentConnectorError::protocol("thread/fork omitted thread"))?;
                let summary = normalize::session_summary(&self.describe().connector_id, thread)?;
                self.provider_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .mark_loaded(summary.session_id.clone());
                Some(summary)
            }
            SESSION_RENAME_ACTION => {
                let name = required_action_string(&request.arguments, "name")?;
                let client = self.client().await?;
                client
                    .rpc
                    .request(
                        "thread/name/set",
                        json!({"threadId": request.session_id.as_str(), "name": name}),
                    )
                    .await
                    .map_err(connector_transport_error)?;
                Some(self.read_summary(&client, &request.session_id).await?)
            }
            _ => {
                return Err(AgentConnectorError::unsupported(format!(
                    "Codex does not declare action {}",
                    request.action_id
                )))
            }
        };
        self.invalidate_session_list_cache();
        Ok(AgentSessionActionOutcome {
            status: AgentSessionActionStatus::Completed,
            session,
            content: Vec::new(),
            details: Value::Null,
        })
    }
}

fn non_empty_optional(
    value: Option<String>,
    field: &str,
) -> Result<Option<String>, AgentConnectorError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(AgentConnectorError::invalid(format!(
            "{field} must not be empty"
        ))),
        value => Ok(value),
    }
}

fn required_action_string(arguments: &Value, field: &str) -> Result<String, AgentConnectorError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| AgentConnectorError::invalid("action arguments must be an object"))?;
    if object.len() != 1 {
        return Err(AgentConnectorError::invalid(
            "rename action accepts only the name argument",
        ));
    }
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AgentConnectorError::invalid("rename action requires a non-empty name"))
}

fn insert_optional(object: &mut Value, key: &str, value: Option<Value>) {
    if let (Some(object), Some(value)) = (object.as_object_mut(), value) {
        object.insert(key.to_owned(), value);
    }
}

fn connector_transport_error(error: CodexTransportError) -> AgentConnectorError {
    let (code, retryable) = match error {
        CodexTransportError::Spawn(_) | CodexTransportError::DaemonStart(_) => {
            (AgentConnectorErrorCode::Unavailable, false)
        }
        CodexTransportError::Timeout
        | CodexTransportError::Closed
        | CodexTransportError::Disconnected(_)
        | CodexTransportError::Io(_)
        | CodexTransportError::SharedDaemonConnect { .. }
        | CodexTransportError::WebSocket(_) => (AgentConnectorErrorCode::Unavailable, true),
        CodexTransportError::Rpc(_) => (AgentConnectorErrorCode::Protocol, false),
        CodexTransportError::InvalidJson(_)
        | CodexTransportError::FrameTooLarge { .. }
        | CodexTransportError::MissingResult => (AgentConnectorErrorCode::Protocol, false),
    };
    AgentConnectorError::new(code, error.to_string(), retryable)
}

/// Codex deliberately has no rollout to return between `thread/start` and the
/// first user message. Metadata is still readable, so adapt that provider
/// lifecycle state to an empty Orchestral session instead of surfacing it as a
/// failed request. Keep the match narrow so unrelated protocol failures remain
/// visible.
fn unmaterialized_thread_read(error: &CodexTransportError) -> bool {
    matches!(
        error,
        CodexTransportError::Rpc(message)
            if message.contains("includeTurns is unavailable before first user message")
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use futures_util::StreamExt;
    use orchestral_core::agent_connector::{
        AgentSessionReadQuery, AgentSessionState, CreateAgentSessionRequest,
        InvokeAgentSessionActionRequest,
    };
    use orchestral_core::agent_protocol::wire::{
        AgentEvent, AgentProviderStreamItem, AgentRunEnvelope, AgentStartRequest, Content,
        ProtocolVersion, RunId,
    };
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};

    use super::*;

    #[tokio::test]
    async fn connector_lists_and_reads_sessions_over_jsonl() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.149.1");
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let list: Value =
                serde_json::from_str(&requests.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(list["method"], "thread/list");
            assert_eq!(list["params"]["searchTerm"], "compiler");
            server_write.write_all(format!("{}\n", json!({
                "id": list["id"],
                "result": {
                    "data": [{
                        "id": "thread-1", "preview": "compiler fix", "cwd": "/repo",
                        "createdAt": 10, "updatedAt": 20, "status": {"type": "idle"},
                        "modelProvider": "openai", "cliVersion": "0.149.1", "source": "cli"
                    }],
                    "nextCursor": "next-page",
                    "futureField": true
                }
            })).as_bytes()).await.unwrap();

            let read: Value =
                serde_json::from_str(&requests.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(read["method"], "thread/read");
            assert_eq!(read["params"]["includeTurns"], true);
            server_write.write_all(format!("{}\n", json!({
                "id": read["id"],
                "result": {"thread": {
                    "id": "thread-1", "preview": "compiler fix", "cwd": "/repo",
                    "createdAt": 10, "updatedAt": 20, "status": {"type": "idle"},
                    "turns": [{"id": "turn-1", "status": "completed", "items": [
                        {"type": "userMessage", "id": "u1", "content": [{"type": "text", "text": "fix"}]},
                        {"type": "agentMessage", "id": "a1", "text": "done"}
                    ]}]
                }}
            })).as_bytes()).await.unwrap();
        });

        assert_eq!(
            connector.health().await.unwrap().version.as_deref(),
            Some("codex/0.149.1")
        );
        let query = AgentSessionListQuery {
            limit: 50,
            search: Some("compiler".to_owned()),
            ..Default::default()
        };
        let page = connector.list_sessions(query.clone()).await.unwrap();
        assert_eq!(page.sessions.len(), 1);
        assert_eq!(page.sessions[0].state, AgentSessionState::Idle);
        assert_eq!(page.next_cursor.as_deref(), Some("next-page"));
        assert_eq!(connector.list_sessions(query).await.unwrap(), page);
        let detail = connector
            .read_session(&AgentSessionId::new("thread-1"))
            .await
            .unwrap();
        assert_eq!(detail.turns.len(), 1);
        assert_eq!(detail.turns[0].activities.len(), 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn connector_creates_forks_and_renames_sessions_with_declared_rpc_methods() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.149.1");
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();

            let start = read_request(&mut requests).await;
            assert_eq!(start["method"], "thread/start");
            assert_eq!(start["params"]["cwd"], "/repo");
            assert_eq!(start["params"]["experimentalRawEvents"], false);
            assert_eq!(start["params"]["persistExtendedHistory"], true);
            write_result(
                &mut server_write,
                &start,
                json!({"thread": thread("thread-new", Value::Null)}),
            )
            .await;

            let initial_name = read_request(&mut requests).await;
            assert_eq!(initial_name["method"], "thread/name/set");
            assert_eq!(initial_name["params"]["threadId"], "thread-new");
            assert_eq!(initial_name["params"]["name"], "Compiler work");
            write_result(&mut server_write, &initial_name, json!({})).await;

            let fork = read_request(&mut requests).await;
            assert_eq!(fork["method"], "thread/fork");
            assert_eq!(fork["params"]["threadId"], "thread-new");
            assert_eq!(fork["params"]["persistExtendedHistory"], true);
            write_result(
                &mut server_write,
                &fork,
                json!({"thread": thread("thread-fork", "Compiler work fork")}),
            )
            .await;

            let rename = read_request(&mut requests).await;
            assert_eq!(rename["method"], "thread/name/set");
            assert_eq!(rename["params"]["threadId"], "thread-fork");
            assert_eq!(rename["params"]["name"], "Release review");
            write_result(&mut server_write, &rename, json!({})).await;

            let read = read_request(&mut requests).await;
            assert_eq!(read["method"], "thread/read");
            assert_eq!(read["params"]["includeTurns"], false);
            write_result(
                &mut server_write,
                &read,
                json!({"thread": thread("thread-fork", "Release review")}),
            )
            .await;
        });

        let descriptor = connector.describe();
        assert!(descriptor.capabilities.create);
        assert!(descriptor
            .action(&AgentSessionActionId::new(SESSION_FORK_ACTION))
            .is_some());
        assert!(descriptor
            .action(&AgentSessionActionId::new(SESSION_RENAME_ACTION))
            .is_some());

        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some("/repo".to_owned()),
                title: Some("Compiler work".to_owned()),
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(created.session_id.as_str(), "thread-new");
        assert_eq!(created.title.as_deref(), Some("Compiler work"));

        let forked = connector
            .invoke_action(InvokeAgentSessionActionRequest {
                session_id: created.session_id,
                action_id: AgentSessionActionId::new(SESSION_FORK_ACTION),
                arguments: Value::Null,
                run_id: None,
            })
            .await
            .unwrap()
            .session
            .unwrap();
        assert_eq!(forked.session_id.as_str(), "thread-fork");

        let renamed = connector
            .invoke_action(InvokeAgentSessionActionRequest {
                session_id: forked.session_id,
                action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                arguments: json!({"name": "Release review"}),
                run_id: None,
            })
            .await
            .unwrap()
            .session
            .unwrap();
        assert_eq!(renamed.title.as_deref(), Some("Release review"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn newly_created_session_reads_as_empty_before_its_first_user_message() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.149.1");
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();

            let start = read_request(&mut requests).await;
            assert_eq!(start["method"], "thread/start");
            write_result(
                &mut server_write,
                &start,
                json!({"thread": thread("thread-new", Value::Null)}),
            )
            .await;

            let read_with_turns = read_request(&mut requests).await;
            assert_eq!(read_with_turns["method"], "thread/read");
            assert_eq!(read_with_turns["params"]["includeTurns"], true);
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": read_with_turns["id"],
                            "error": {
                                "code": -32602,
                                "message": "thread thread-new is not materialized yet; includeTurns is unavailable before first user message"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let metadata_read = read_request(&mut requests).await;
            assert_eq!(metadata_read["method"], "thread/read");
            assert_eq!(metadata_read["params"]["includeTurns"], false);
            write_result(
                &mut server_write,
                &metadata_read,
                json!({"thread": thread("thread-new", Value::Null)}),
            )
            .await;
        });

        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: None,
                title: None,
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        let detail = connector.read_session(&created.session_id).await.unwrap();

        assert_eq!(detail.summary.session_id, created.session_id);
        assert!(detail.turns.is_empty());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn newly_created_session_starts_first_turn_without_resume() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.149.1");
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();

            let thread_start = read_request(&mut requests).await;
            assert_eq!(thread_start["method"], "thread/start");
            write_result(
                &mut server_write,
                &thread_start,
                json!({"thread": thread("thread-new", Value::Null)}),
            )
            .await;

            let turn_start = read_request(&mut requests).await;
            assert_eq!(turn_start["method"], "turn/start");
            assert_eq!(turn_start["params"]["threadId"], "thread-new");
            write_result(
                &mut server_write,
                &turn_start,
                json!({"turn": {"id": "turn-first", "status": "inProgress", "items": []}}),
            )
            .await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"method": "turn/completed", "params": {"threadId": "thread-new", "turn": {"id": "turn-first", "status": "completed", "items": []}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: None,
                title: None,
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        let descriptor =
            <CodexConnector as orchestral_core::agent_protocol::spi::AgentProvider>::describe(
                &connector,
            );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            created.session_id,
            RunId::new("run-first"),
            vec![Content::text("hello")],
        )
        .unwrap();
        let request = AgentStartRequest::new(
            AgentRunEnvelope::seal(run.spec).unwrap(),
            ProviderBindingRef::new("codex/local"),
            &descriptor,
        )
        .unwrap();
        let mut stream =
            <CodexConnector as orchestral_core::agent_protocol::spi::AgentProvider>::start(
                &connector, request,
            )
            .await
            .unwrap()
            .stream;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            if matches!(
                item.unwrap(),
                AgentProviderStreamItem::Event(event)
                    if matches!(event.payload, AgentEvent::DeliveryCommitted { .. })
            ) {
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn session_pages_are_bounded_and_reuse_revision_validated_cache() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.149.1");
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let items = (0..60)
                .map(|index| {
                    json!({
                        "type": "commandExecution",
                        "id": format!("command-{index}"),
                        "command": format!("command {index}"),
                        "status": "completed",
                        "aggregatedOutput": "x".repeat(10_000)
                    })
                })
                .collect::<Vec<_>>();

            for include_turns in [false, true, false] {
                let read = read_request(&mut requests).await;
                assert_eq!(read["method"], "thread/read");
                assert_eq!(read["params"]["includeTurns"], include_turns);
                let mut value = thread("thread-large", Value::Null);
                if include_turns {
                    value["turns"] = json!([{
                        "id": "turn-1",
                        "status": "completed",
                        "items": items
                    }]);
                }
                write_result(&mut server_write, &read, json!({"thread": value})).await;
            }
        });

        let query = AgentSessionReadQuery {
            cursor: None,
            limit: 100,
        };
        let first = connector
            .read_session_page(&AgentSessionId::new("thread-large"), query.clone())
            .await
            .unwrap();
        let second = connector
            .read_session_page(&AgentSessionId::new("thread-large"), query)
            .await
            .unwrap();

        assert!(first.next_cursor.is_some());
        assert_eq!(first, second);
        assert_eq!(
            first.turns[0]
                .activities
                .last()
                .unwrap()
                .activity_id
                .as_str(),
            "command-59"
        );
        assert!(serde_json::to_vec(&first).unwrap().len() < 512 * 1_024);
        server.await.unwrap();
    }

    #[test]
    fn connector_rejects_invalid_create_and_action_arguments_before_side_effects() {
        let connector = CodexConnector::default();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let create_error = runtime
            .block_on(connector.create_session(CreateAgentSessionRequest {
                cwd: Some("  ".to_owned()),
                title: None,
                extensions: BTreeMap::new(),
            }))
            .unwrap_err();
        assert_eq!(create_error.code, AgentConnectorErrorCode::InvalidRequest);

        let rename_error = runtime
            .block_on(connector.invoke_action(InvokeAgentSessionActionRequest {
                session_id: AgentSessionId::new("thread-1"),
                action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                arguments: json!({"name": "", "unexpected": true}),
                run_id: None,
            }))
            .unwrap_err();
        assert_eq!(rename_error.code, AgentConnectorErrorCode::InvalidRequest);
    }

    fn thread(id: &str, name: impl Into<Value>) -> Value {
        json!({
            "id": id,
            "name": name.into(),
            "preview": "session preview",
            "cwd": "/repo",
            "createdAt": 10,
            "updatedAt": 20,
            "status": {"type": "idle"}
        })
    }

    async fn read_request<R>(requests: &mut tokio::io::Lines<BufReader<R>>) -> Value
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        serde_json::from_str(&requests.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn write_result<W>(writer: &mut W, request: &Value, result: Value)
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        writer
            .write_all(format!("{}\n", json!({"id": request["id"], "result": result})).as_bytes())
            .await
            .unwrap();
    }
}
