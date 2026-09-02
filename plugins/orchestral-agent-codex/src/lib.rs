//! Codex integration through the supported `codex app-server` protocol.
//!
//! This crate intentionally depends only on Orchestral contracts. Codex wire
//! names and compatibility handling stay inside this concrete plugin.

mod generated_artifact;
mod normalize;
mod provider;
mod transport;

#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use orchestral_core::agent_connector::{
    paginate_session_detail, AgentConnector, AgentConnectorDescriptor, AgentConnectorError,
    AgentConnectorErrorCode, AgentConnectorHealth, AgentConnectorId, AgentSessionActionDescriptor,
    AgentSessionActionExecution, AgentSessionActionId, AgentSessionActionOutcome,
    AgentSessionActionStatus, AgentSessionCapabilities, AgentSessionChange, AgentSessionChangeKind,
    AgentSessionCreationDescriptor, AgentSessionDetail, AgentSessionListQuery, AgentSessionPage,
    AgentSessionReadQuery, AgentSessionSummary, AgentSessionTurnId, AgentSessionTurnStatus,
    CreateAgentSessionRequest, InvokeAgentSessionActionRequest, SESSION_COMPACT_ACTION,
    SESSION_FORK_ACTION, SESSION_RENAME_ACTION, SESSION_REVIEW_ACTION,
    SESSION_SET_PERMISSIONS_ACTION,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
use orchestral_core::io::{ArtifactPublisher, ArtifactResolver, BlobStore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Mutex as AsyncMutex, Notify};

pub use transport::{CodexAppServerConfig, CodexAppServerEndpoint, CodexTransportError};

use crate::normalize::NormalizationLimits;
use crate::transport::{CodexRpcClient, CodexTransportEvent};

const CONNECTOR_ID: &str = "codex/local";
const PROVIDER_BINDING: &str = "codex/local";
const SESSION_LIST_CACHE_ENTRIES: usize = 8;
const SESSION_LIST_CACHE_SNAPSHOT_VERSION: u32 = 1;
const SESSION_LIST_REFRESH_AFTER: Duration = Duration::from_secs(30);
const SESSION_QUEUE_PAGE_LIMIT: u32 = 100;
const SESSION_DEFERRED_QUEUE_LIMIT: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionListCacheSnapshot {
    version: u32,
    entries: Vec<SessionListCacheSnapshotEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionListCacheSnapshotEntry {
    key: SessionListCacheKey,
    page: AgentSessionPage,
}

struct ConnectedClient {
    rpc: Arc<CodexRpcClient>,
    user_agent: String,
}

/// Long-lived local Codex connector. By default it attaches to Codex's shared
/// app-server daemon; UI subscribers never own native Codex connections.
pub struct CodexConnector {
    config: CodexAppServerConfig,
    creation_schema: Value,
    artifact_resolver: Option<Arc<dyn ArtifactResolver>>,
    artifact_blob_store: Option<Arc<dyn BlobStore>>,
    artifact_publisher: Option<Arc<dyn ArtifactPublisher>>,
    generated_artifacts: generated_artifact::GeneratedArtifactProjection,
    client: AsyncMutex<Option<Arc<ConnectedClient>>>,
    limits: NormalizationLimits,
    provider_state: StdMutex<provider::ProviderState>,
    session_list_cache: StdMutex<BTreeMap<SessionListCacheKey, SessionListCacheEntry>>,
    session_list_cache_path: Option<PathBuf>,
    session_list_gate: AsyncMutex<()>,
    session_list_refresh_needed: Notify,
    #[cfg(test)]
    reconnect_clients: StdMutex<VecDeque<Arc<ConnectedClient>>>,
}

impl CodexConnector {
    pub fn new(config: CodexAppServerConfig) -> Self {
        Self {
            config,
            creation_schema: codex_session_creation_schema(),
            artifact_resolver: None,
            artifact_blob_store: None,
            artifact_publisher: None,
            generated_artifacts: generated_artifact::GeneratedArtifactProjection::default(),
            client: AsyncMutex::new(None),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
            session_list_cache: StdMutex::new(BTreeMap::new()),
            session_list_cache_path: None,
            session_list_gate: AsyncMutex::new(()),
            session_list_refresh_needed: Notify::new(),
            #[cfg(test)]
            reconnect_clients: StdMutex::new(VecDeque::new()),
        }
    }

    pub fn with_artifact_resolver(
        config: CodexAppServerConfig,
        artifact_resolver: Arc<dyn ArtifactResolver>,
    ) -> Self {
        let mut connector = Self::new(config);
        connector.artifact_resolver = Some(artifact_resolver);
        connector
    }

    pub fn with_artifact_services(
        config: CodexAppServerConfig,
        artifact_resolver: Arc<dyn ArtifactResolver>,
        artifact_blob_store: Arc<dyn BlobStore>,
        artifact_publisher: Arc<dyn ArtifactPublisher>,
    ) -> Self {
        let mut connector = Self::new(config);
        connector.artifact_resolver = Some(artifact_resolver);
        connector.artifact_blob_store = Some(artifact_blob_store);
        connector.artifact_publisher = Some(artifact_publisher);
        connector
    }

    pub fn with_artifact_io(
        config: CodexAppServerConfig,
        artifact_resolver: Arc<dyn ArtifactResolver>,
        artifact_blob_store: Arc<dyn BlobStore>,
    ) -> Self {
        let mut connector = Self::new(config);
        connector.artifact_resolver = Some(artifact_resolver);
        connector.artifact_blob_store = Some(artifact_blob_store);
        connector
    }

    /// Installs a durable last-known-good session-list projection.
    ///
    /// Codex may need several seconds to scan rollout files for `thread/list`.
    /// A Host restart must not put that scan on the browser's critical path,
    /// so the connector serves this snapshot immediately while the composition
    /// root refreshes it in the background.
    pub fn with_session_list_cache_path(mut self, path: PathBuf) -> Self {
        self.session_list_cache_path = Some(path.clone());
        if let Err(error) = self.load_session_list_cache(&path) {
            tracing::warn!(path = %path.display(), %error, "ignored Codex session-list snapshot");
        }
        self
    }

    async fn client(&self) -> Result<Arc<ConnectedClient>, AgentConnectorError> {
        let mut client = self.client.lock().await;
        if let Some(client) = client.as_ref().filter(|client| client.rpc.is_connected()) {
            return Ok(Arc::clone(client));
        }
        *client = None;
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

    async fn read_persisted_session_page(
        &self,
        client: &ConnectedClient,
        session_id: &AgentSessionId,
        summary: AgentSessionSummary,
        query: AgentSessionReadQuery,
        initial_result: Result<Value, CodexTransportError>,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        let live_edge = query.cursor.is_none();
        let mut native_limit = query.limit;
        let mut initial_result = Some(initial_result);
        for attempt in 0..2 {
            let result = match initial_result.take() {
                Some(result) => result,
                None => {
                    self.request_session_items_page(client, session_id, &query, native_limit)
                        .await
                }
            };
            let result = match result {
                Ok(result) => result,
                Err(error) if unmaterialized_history_page(&error) => {
                    return Ok(AgentSessionDetail {
                        summary,
                        turns: Vec::new(),
                        pending_requests: Vec::new(),
                        next_cursor: None,
                    });
                }
                Err(error) if item_pagination_unavailable(&error) => {
                    return self
                        .read_legacy_turn_page(client, session_id, summary, query)
                        .await;
                }
                Err(error) => return Err(connector_transport_error(error)),
            };
            let detail =
                normalize::session_items_page(summary.clone(), &result, live_edge, &self.limits)?;
            let native_activity_count = detail
                .turns
                .iter()
                .map(|turn| turn.activities.len())
                .sum::<usize>();
            let native_next_cursor = detail.next_cursor.clone();
            let mut bounded = paginate_session_detail(
                &detail,
                AgentSessionReadQuery {
                    cursor: None,
                    limit: native_limit,
                },
            )?;
            let bounded_activity_count = bounded
                .turns
                .iter()
                .map(|turn| turn.activities.len())
                .sum::<usize>();
            if attempt == 0
                && bounded_activity_count > 0
                && bounded_activity_count < native_activity_count
            {
                // Reissue the native page at the byte-bounded item count so its
                // nextCursor starts immediately after the last returned item.
                native_limit = bounded_activity_count as u32;
                continue;
            }
            bounded.next_cursor = native_next_cursor;
            self.generated_artifacts
                .enrich_detail(
                    self.artifact_blob_store.as_ref(),
                    session_id.as_str(),
                    &result,
                    &mut bounded,
                )
                .await;
            return Ok(bounded);
        }
        Err(AgentConnectorError::protocol(
            "thread/items/list could not produce a stable bounded page",
        ))
    }

    async fn request_session_items_page(
        &self,
        client: &ConnectedClient,
        session_id: &AgentSessionId,
        query: &AgentSessionReadQuery,
        limit: u32,
    ) -> Result<Value, CodexTransportError> {
        client
            .rpc
            .request(
                "thread/items/list",
                json!({
                    "threadId": session_id.as_str(),
                    "cursor": query.cursor,
                    "limit": limit,
                    "sortDirection": "desc"
                }),
            )
            .await
    }

    async fn read_legacy_turn_page(
        &self,
        client: &ConnectedClient,
        session_id: &AgentSessionId,
        summary: AgentSessionSummary,
        query: AgentSessionReadQuery,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        let turn_limit = query.limit.div_ceil(2).max(1);
        let result = client
            .rpc
            .request(
                "thread/turns/list",
                json!({
                    "threadId": session_id.as_str(),
                    "cursor": query.cursor,
                    "limit": turn_limit,
                    "sortDirection": "desc",
                    "itemsView": "summary"
                }),
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(error) if unmaterialized_history_page(&error) => {
                return Ok(AgentSessionDetail {
                    summary,
                    turns: Vec::new(),
                    pending_requests: Vec::new(),
                    next_cursor: None,
                });
            }
            Err(error) => return Err(connector_transport_error(error)),
        };
        let detail = normalize::session_turns_page(summary, &result, &self.limits)?;
        let native_next_cursor = detail.next_cursor.clone();
        let mut bounded = paginate_session_detail(
            &detail,
            AgentSessionReadQuery {
                cursor: None,
                limit: query.limit,
            },
        )?;
        bounded.next_cursor = native_next_cursor;
        Ok(bounded)
    }

    /// Reads the native deferred queue separately from the transcript cache.
    /// A cached terminal history must never hide a newly queued message.
    async fn read_deferred_turns(
        &self,
        client: &ConnectedClient,
        session_id: &AgentSessionId,
    ) -> Result<Vec<orchestral_core::agent_connector::AgentSessionTurn>, AgentConnectorError> {
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut turns = Vec::new();
        loop {
            let result = client
                .rpc
                .request(
                    "thread/queue/list",
                    json!({
                        "threadId": session_id.as_str(),
                        "cursor": cursor,
                        "limit": SESSION_QUEUE_PAGE_LIMIT
                    }),
                )
                .await
                .map_err(connector_transport_error)?;
            let submissions = result
                .get("data")
                .and_then(Value::as_array)
                .ok_or_else(|| AgentConnectorError::protocol("thread/queue/list omitted data"))?;
            for submission in submissions {
                if turns.len() >= SESSION_DEFERRED_QUEUE_LIMIT {
                    return Ok(turns);
                }
                turns.push(normalize::deferred_queue_turn(
                    submission,
                    turns.len() + 1,
                    &self.limits,
                )?);
            }
            cursor = result
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let Some(next) = cursor.as_ref() else {
                return Ok(turns);
            };
            if next.trim().is_empty() || !seen_cursors.insert(next.clone()) {
                return Err(AgentConnectorError::protocol(
                    "thread/queue/list returned a non-advancing cursor",
                ));
            }
        }
    }

    fn cached_session_page(&self, query: &AgentSessionListQuery) -> Option<AgentSessionPage> {
        let key = SessionListCacheKey::from(query);
        let cached = self
            .session_list_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .map(|entry| (entry.page.clone(), entry.inserted_at.elapsed()));
        if cached
            .as_ref()
            .is_some_and(|(_, age)| *age >= SESSION_LIST_REFRESH_AFTER)
        {
            self.session_list_refresh_needed.notify_one();
        }
        cached.map(|(page, _)| page)
    }

    fn cache_session_page(&self, query: &AgentSessionListQuery, page: AgentSessionPage) {
        {
            let mut cache = self
                .session_list_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = SessionListCacheKey::from(query);
            if cache.len() >= SESSION_LIST_CACHE_ENTRIES && !cache.contains_key(&key) {
                if let Some(oldest) = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.inserted_at)
                    .map(|(key, _)| key.clone())
                {
                    cache.remove(&oldest);
                }
            }
            cache.insert(
                key,
                SessionListCacheEntry {
                    inserted_at: Instant::now(),
                    page,
                },
            );
        }
        if let Some(path) = &self.session_list_cache_path {
            if let Err(error) = self.persist_session_list_cache(path) {
                tracing::warn!(path = %path.display(), %error, "could not persist Codex session-list snapshot");
            }
        }
    }

    fn invalidate_session_list_cache(&self) {
        // Keep the last-known-good projection readable. The application-level
        // refresh loop will replace it shortly; clearing here would put the
        // expensive Codex rollout scan back on the next browser request.
        self.session_list_refresh_needed.notify_one();
    }

    fn load_session_list_cache(&self, path: &Path) -> Result<(), String> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        let snapshot: SessionListCacheSnapshot =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if snapshot.version != SESSION_LIST_CACHE_SNAPSHOT_VERSION {
            return Err(format!("unsupported snapshot version {}", snapshot.version));
        }
        let mut cache = self
            .session_list_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in snapshot
            .entries
            .into_iter()
            .take(SESSION_LIST_CACHE_ENTRIES)
        {
            cache.insert(
                entry.key,
                SessionListCacheEntry {
                    inserted_at: Instant::now(),
                    page: entry.page,
                },
            );
        }
        Ok(())
    }

    fn persist_session_list_cache(&self, path: &Path) -> Result<(), String> {
        let snapshot = {
            let cache = self
                .session_list_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            SessionListCacheSnapshot {
                version: SESSION_LIST_CACHE_SNAPSHOT_VERSION,
                entries: cache
                    .iter()
                    .map(|(key, entry)| SessionListCacheSnapshotEntry {
                        key: key.clone(),
                        page: entry.page.clone(),
                    })
                    .collect(),
            }
        };
        let bytes = serde_json::to_vec(&snapshot).map_err(|error| error.to_string())?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
        std::fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
        std::fs::rename(&temporary, path).map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn fetch_session_page(
        &self,
        query: &AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
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
        let started_at = Instant::now();
        let result = client
            .rpc
            .request("thread/list", params)
            .await
            .map_err(connector_transport_error)?;
        let page = normalize::session_page(&self.describe().connector_id, &result)?;
        page.validate_for(&self.describe().connector_id, query.limit)?;
        self.cache_session_page(query, page.clone());
        tracing::info!(
            elapsed_ms = started_at.elapsed().as_millis(),
            sessions = page.sessions.len(),
            "refreshed Codex session-list snapshot"
        );
        Ok(page)
    }

    /// Refreshes one session-list projection without making browser requests
    /// wait for Codex's rollout scan.
    pub async fn refresh_session_list(
        &self,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
        query.validate()?;
        let _gate = self.session_list_gate.lock().await;
        self.fetch_session_page(&query).await
    }

    /// Waits until a cache reader or connector mutation asks the composition
    /// root to refresh. Multiple requests coalesce into one notification.
    pub async fn wait_for_session_list_refresh(&self) {
        self.session_list_refresh_needed.notified().await;
    }

    /// Refreshes only when the last successful projection is old enough.
    pub async fn refresh_session_list_if_stale(
        &self,
        query: AgentSessionListQuery,
        max_age: Duration,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
        query.validate()?;
        let _gate = self.session_list_gate.lock().await;
        let key = SessionListCacheKey::from(&query);
        if let Some(page) = self
            .session_list_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .filter(|entry| entry.inserted_at.elapsed() < max_age)
            .map(|entry| entry.page.clone())
        {
            return Ok(page);
        }
        self.fetch_session_page(&query).await
    }

    #[cfg(test)]
    fn with_client(rpc: Arc<CodexRpcClient>, user_agent: impl Into<String>) -> Self {
        Self {
            config: CodexAppServerConfig {
                allow_deferred_queue: true,
                dispatch_journal_dir: None,
                ..CodexAppServerConfig::default()
            },
            creation_schema: codex_session_creation_schema(),
            artifact_resolver: None,
            artifact_blob_store: None,
            artifact_publisher: None,
            generated_artifacts: generated_artifact::GeneratedArtifactProjection::default(),
            client: AsyncMutex::new(Some(Arc::new(ConnectedClient {
                rpc,
                user_agent: user_agent.into(),
            }))),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
            session_list_cache: StdMutex::new(BTreeMap::new()),
            session_list_cache_path: None,
            session_list_gate: AsyncMutex::new(()),
            session_list_refresh_needed: Notify::new(),
            reconnect_clients: StdMutex::new(VecDeque::new()),
        }
    }

    #[cfg(test)]
    fn with_reconnect_client(rpc: Arc<CodexRpcClient>, reconnect_rpc: Arc<CodexRpcClient>) -> Self {
        Self {
            config: CodexAppServerConfig {
                allow_deferred_queue: true,
                dispatch_journal_dir: None,
                ..CodexAppServerConfig::default()
            },
            creation_schema: codex_session_creation_schema(),
            artifact_resolver: None,
            artifact_blob_store: None,
            artifact_publisher: None,
            generated_artifacts: generated_artifact::GeneratedArtifactProjection::default(),
            client: AsyncMutex::new(Some(Arc::new(ConnectedClient {
                rpc,
                user_agent: "codex/test".to_owned(),
            }))),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
            session_list_cache: StdMutex::new(BTreeMap::new()),
            session_list_cache_path: None,
            session_list_gate: AsyncMutex::new(()),
            session_list_refresh_needed: Notify::new(),
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
            creation: Some(AgentSessionCreationDescriptor {
                accepts_cwd: true,
                default_cwd: std::env::current_dir()
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned()),
                input_schema: Some(self.creation_schema.clone()),
                connection_hint: Some("Shared daemon · unix://".to_owned()),
            }),
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
                AgentSessionActionDescriptor {
                    action_id: AgentSessionActionId::new(SESSION_SET_PERMISSIONS_ACTION),
                    title: "Change permissions".to_owned(),
                    description: "Change the sandbox and approval policy used by subsequent turns"
                        .to_owned(),
                    input_schema: Some(permission_settings_schema()),
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
        self.fetch_session_page(&query).await
    }

    async fn read_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        self.read_session_page(
            session_id,
            AgentSessionReadQuery {
                cursor: None,
                limit: 500,
            },
        )
        .await
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
        let live_edge = query.cursor.is_none();
        // These reads are independent. Starting them together keeps a normal
        // PWA refresh to one app-server round trip instead of adding metadata,
        // history, and deferred-queue latency.
        let summary_read = self.read_summary(&client, session_id);
        let history_read =
            self.request_session_items_page(&client, session_id, &query, query.limit);
        let deferred_read = async {
            if live_edge {
                self.read_deferred_turns(&client, session_id).await
            } else {
                Ok(Vec::new())
            }
        };
        let (summary, initial_history, deferred_turns) =
            tokio::join!(summary_read, history_read, deferred_read);
        let summary = summary?;
        let mut page = self
            .read_persisted_session_page(&client, session_id, summary, query, initial_history)
            .await?;
        if live_edge {
            page.turns.extend(deferred_turns?);
        }
        page.validate_for(&self.describe().connector_id)?;
        Ok(page)
    }

    async fn subscribe_session_changes(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<broadcast::Receiver<AgentSessionChange>, AgentConnectorError> {
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty"));
        }
        let client = self.client().await?;
        // Subscribe to transport notifications before attaching so events
        // emitted during the metadata-only resume cannot be lost.
        let mut native = client.rpc.subscribe();
        let already_loaded = self
            .provider_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_loaded(session_id);
        if !already_loaded {
            let attach = client
                .rpc
                .request(
                    "thread/resume",
                    json!({
                        "threadId": session_id.as_str(),
                        "excludeTurns": true,
                        "persistExtendedHistory": true
                    }),
                )
                .await;
            if matches!(
                &attach,
                Err(CodexTransportError::Rpc(message))
                    if message.contains("already has an active writer")
            ) && provider::client_has_loaded_session(&client.rpc, session_id).await
            {
                self.provider_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .mark_loaded(session_id.clone());
            } else {
                if let Err(CodexTransportError::Rpc(message)) = &attach {
                    if message.contains("already has an active writer") {
                        return Err(AgentConnectorError::unsupported(
                            "live session observation is unavailable for a thread owned by another process",
                        ));
                    }
                }
                attach.map_err(connector_transport_error)?;
                self.provider_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .mark_loaded(session_id.clone());
            }
        }
        let connector_id = self.describe().connector_id;
        let watched_session_id = session_id.clone();
        let limits = self.limits.clone();
        let artifact_blob_store = self.artifact_blob_store.clone();
        let generated_artifacts = self.generated_artifacts.clone();
        let (changes, receiver) = broadcast::channel(64);
        tokio::spawn(async move {
            let mut sequence = 0_u64;
            loop {
                match native.recv().await {
                    Ok(CodexTransportEvent::Message(message))
                        if is_session_change_notification(&message, &watched_session_id) =>
                    {
                        let mut change = native_session_change(&message, &limits);
                        generated_artifacts
                            .enrich_change(
                                artifact_blob_store.as_ref(),
                                watched_session_id.as_str(),
                                &message,
                                &mut change,
                            )
                            .await;
                        sequence = sequence.saturating_add(1);
                        if changes
                            .send(AgentSessionChange {
                                connector_id: connector_id.clone(),
                                session_id: watched_session_id.clone(),
                                sequence,
                                change,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    Ok(CodexTransportEvent::Message(_)) => {}
                    Ok(CodexTransportEvent::Disconnected { .. })
                    | Err(broadcast::error::RecvError::Closed) => return,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        sequence = sequence.saturating_add(1);
                        if changes
                            .send(AgentSessionChange {
                                connector_id: connector_id.clone(),
                                session_id: watched_session_id.clone(),
                                sequence,
                                change: AgentSessionChangeKind::RefreshRequired {
                                    reason: "native_notification_gap".to_owned(),
                                },
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }
        });
        Ok(receiver)
    }

    async fn create_session(
        &self,
        request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        let cwd = non_empty_optional(request.cwd, "cwd")?;
        let title = non_empty_optional(request.title, "title")?;
        let settings = (!request.options.is_null())
            .then(|| session_creation_settings(&request.options))
            .transpose()?;
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
        if let Some(settings) = settings {
            params["sandbox"] = Value::String(settings.sandbox_mode);
            params["approvalPolicy"] = Value::String(settings.approval_policy);
            if let Some(model) = settings.model {
                params["model"] = Value::String(model);
            }
            if let Some(reasoning_effort) = settings.reasoning_effort {
                params["config"] = json!({"model_reasoning_effort": reasoning_effort});
            }
        }
        if self.artifact_publisher.is_some() {
            params["dynamicTools"] = provider::artifact_dynamic_tools();
        }
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
            SESSION_SET_PERMISSIONS_ACTION => {
                let permissions = permission_settings(&request.arguments)?;
                let client = self.client().await?;
                client
                    .rpc
                    .request(
                        "thread/settings/update",
                        json!({
                            "threadId": request.session_id.as_str(),
                            "sandboxPolicy": sandbox_policy(&permissions.sandbox_mode),
                            "approvalPolicy": permissions.approval_policy,
                        }),
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct PermissionSettings {
    sandbox_mode: String,
    approval_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionCreationSettings {
    sandbox_mode: String,
    approval_policy: String,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    reasoning_effort: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct CodexModelCache {
    #[serde(default)]
    models: Vec<CodexModelCacheEntry>,
}

#[derive(Debug, Deserialize)]
struct CodexModelCacheEntry {
    slug: String,
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    supported_in_api: bool,
    #[serde(default)]
    default_reasoning_level: Option<String>,
    #[serde(default)]
    supported_reasoning_levels: Vec<CodexReasoningLevel>,
}

#[derive(Debug, Deserialize)]
struct CodexReasoningLevel {
    effort: String,
}

#[derive(Debug, Default, Deserialize)]
struct CodexUserDefaults {
    model: Option<String>,
    model_reasoning_effort: Option<String>,
}

fn codex_session_creation_schema() -> Value {
    const FALLBACK_MODELS: [&str; 6] = [
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
        "gpt-5.5",
        "gpt-5.4",
        "gpt-5.4-mini",
    ];
    const REASONING_ORDER: [&str; 6] = ["low", "medium", "high", "xhigh", "max", "ultra"];

    let home = codex_home();
    let cache = fs::read_to_string(home.join("models_cache.json"))
        .ok()
        .and_then(|contents| serde_json::from_str::<CodexModelCache>(&contents).ok())
        .unwrap_or_default();
    let visible_models = cache
        .models
        .iter()
        .filter(|model| model.visibility == "list" && model.supported_in_api)
        .collect::<Vec<_>>();
    let models = if visible_models.is_empty() {
        FALLBACK_MODELS
            .iter()
            .map(|model| (*model).to_owned())
            .collect::<Vec<_>>()
    } else {
        visible_models
            .iter()
            .map(|model| model.slug.clone())
            .collect::<Vec<_>>()
    };
    let mut reasoning_efforts = REASONING_ORDER
        .iter()
        .filter(|effort| {
            visible_models.is_empty()
                || visible_models.iter().any(|model| {
                    model
                        .supported_reasoning_levels
                        .iter()
                        .any(|level| level.effort == **effort)
                })
        })
        .map(|effort| (*effort).to_owned())
        .collect::<Vec<_>>();
    if reasoning_efforts.is_empty() {
        reasoning_efforts = REASONING_ORDER
            .iter()
            .map(|effort| (*effort).to_owned())
            .collect();
    }
    let defaults = fs::read_to_string(home.join("config.toml"))
        .ok()
        .and_then(|contents| toml::from_str::<CodexUserDefaults>(&contents).ok())
        .unwrap_or_default();
    let default_model = defaults
        .model
        .filter(|model| models.contains(model))
        .unwrap_or_else(|| models[0].clone());
    let model_default_effort = visible_models
        .iter()
        .find(|model| model.slug == default_model)
        .and_then(|model| model.default_reasoning_level.clone());
    let default_reasoning_effort = defaults
        .model_reasoning_effort
        .filter(|effort| reasoning_efforts.contains(effort))
        .or(model_default_effort)
        .filter(|effort| reasoning_efforts.contains(effort))
        .unwrap_or_else(|| "medium".to_owned());

    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["model", "reasoning_effort", "sandbox_mode", "approval_policy"],
        "properties": {
            "model": {
                "type": "string",
                "title": "模型",
                "default": default_model,
                "enum": models
            },
            "reasoning_effort": {
                "type": "string",
                "title": "推理强度",
                "default": default_reasoning_effort,
                "enum": reasoning_efforts
            },
            "sandbox_mode": {
                "type": "string",
                "title": "文件与命令权限",
                "default": "workspace-write",
                "enum": ["read-only", "workspace-write", "danger-full-access"]
            },
            "approval_policy": {
                "type": "string",
                "title": "审批策略",
                "default": "on-request",
                "enum": ["on-request", "never"]
            }
        }
    })
}

fn codex_home() -> PathBuf {
    std::env::var_os("CODEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .map(|home| home.join(".codex"))
        })
        .unwrap_or_else(|| PathBuf::from(".codex"))
}

fn permission_settings_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["sandbox_mode", "approval_policy"],
        "properties": {
            "sandbox_mode": {
                "type": "string",
                "title": "文件与命令权限",
                "default": "workspace-write",
                "enum": ["read-only", "workspace-write", "danger-full-access"]
            },
            "approval_policy": {
                "type": "string",
                "title": "审批策略",
                "default": "on-request",
                "enum": ["on-request", "never"]
            }
        }
    })
}

fn permission_settings(arguments: &Value) -> Result<PermissionSettings, AgentConnectorError> {
    let settings: PermissionSettings =
        serde_json::from_value(arguments.clone()).map_err(|error| {
            AgentConnectorError::invalid(format!("invalid permission settings: {error}"))
        })?;
    validate_permission_settings(&settings.sandbox_mode, &settings.approval_policy)?;
    Ok(settings)
}

fn session_creation_settings(
    arguments: &Value,
) -> Result<SessionCreationSettings, AgentConnectorError> {
    let settings: SessionCreationSettings =
        serde_json::from_value(arguments.clone()).map_err(|error| {
            AgentConnectorError::invalid(format!("invalid session creation settings: {error}"))
        })?;
    validate_permission_settings(&settings.sandbox_mode, &settings.approval_policy)?;
    for (field, value) in [
        ("model", settings.model.as_deref()),
        ("reasoning_effort", settings.reasoning_effort.as_deref()),
    ] {
        if value.is_some_and(|value| value.trim().is_empty()) {
            return Err(AgentConnectorError::invalid(format!(
                "{field} must not be empty"
            )));
        }
    }
    Ok(settings)
}

fn validate_permission_settings(
    sandbox_mode: &str,
    approval_policy: &str,
) -> Result<(), AgentConnectorError> {
    if !matches!(
        sandbox_mode,
        "read-only" | "workspace-write" | "danger-full-access"
    ) {
        return Err(AgentConnectorError::invalid(
            "sandbox_mode must be read-only, workspace-write, or danger-full-access",
        ));
    }
    if !matches!(approval_policy, "on-request" | "never") {
        return Err(AgentConnectorError::invalid(
            "approval_policy must be on-request or never",
        ));
    }
    Ok(())
}

fn sandbox_policy(mode: &str) -> Value {
    match mode {
        "read-only" => json!({"type": "readOnly"}),
        "workspace-write" => json!({"type": "workspaceWrite"}),
        "danger-full-access" => json!({"type": "dangerFullAccess"}),
        _ => unreachable!("permission settings are validated before mapping"),
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

/// Codex deliberately has no persisted page between `thread/start` and the
/// first user message. Metadata is still readable, so expose an empty page.
fn unmaterialized_history_page(error: &CodexTransportError) -> bool {
    matches!(
        error,
        CodexTransportError::Rpc(message)
            if message.contains("unavailable before first user message")
                || message.contains("missing source rollout")
    )
}

fn item_pagination_unavailable(error: &CodexTransportError) -> bool {
    matches!(
        error,
        CodexTransportError::Rpc(message)
            if message.contains("thread/items/list is not supported")
                || message.contains("method not found")
                || message.contains("Method not found")
    )
}

fn is_session_change_notification(message: &Value, session_id: &AgentSessionId) -> bool {
    let method = message.get("method").and_then(Value::as_str);
    let notification_session_id = message.pointer("/params/threadId").and_then(Value::as_str);
    notification_session_id == Some(session_id.as_str())
        && matches!(
            method,
            Some(
                "thread/status/changed"
                    | "thread/name/updated"
                    | "thread/queue/changed"
                    | "thread/compacted"
                    | "thread/reverted"
                    | "thread/closed"
                    | "thread/deleted"
                    | "turn/started"
                    | "item/completed"
                    | "turn/completed"
                    | "serverRequest/resolved"
            )
        )
}

fn native_session_change(message: &Value, limits: &NormalizationLimits) -> AgentSessionChangeKind {
    let method = message
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let turn_id = message.pointer("/params/turnId").and_then(Value::as_str);
    match (method, turn_id) {
        ("item/completed", Some(turn_id)) => {
            let Some(item) = message.pointer("/params/item") else {
                return AgentSessionChangeKind::RefreshRequired {
                    reason: "item_completed_without_item".to_owned(),
                };
            };
            AgentSessionChangeKind::ActivityUpsert {
                turn_id: AgentSessionTurnId::new(turn_id),
                turn_status: AgentSessionTurnStatus::Active,
                activity: normalize::normalize_activity(turn_id, 0, item, limits),
            }
        }
        ("turn/started", Some(turn_id)) => AgentSessionChangeKind::TurnStatus {
            turn_id: AgentSessionTurnId::new(turn_id),
            status: AgentSessionTurnStatus::Active,
        },
        ("turn/completed", Some(turn_id)) => AgentSessionChangeKind::TurnStatus {
            turn_id: AgentSessionTurnId::new(turn_id),
            status: match message
                .pointer("/params/turn/status")
                .and_then(Value::as_str)
            {
                Some("failed") => AgentSessionTurnStatus::Failed,
                Some("interrupted") => AgentSessionTurnStatus::Interrupted,
                _ => AgentSessionTurnStatus::Completed,
            },
        },
        _ => AgentSessionChangeKind::RefreshRequired {
            reason: method.replace('/', "_"),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use chrono::Utc;
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

    const ONE_PIXEL_PNG: &str =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

    struct RecordingWriteBlobStore {
        writes: AtomicUsize,
    }

    #[async_trait]
    impl BlobStore for RecordingWriteBlobStore {
        async fn write(
            &self,
            mut request: orchestral_core::io::BlobWriteRequest,
        ) -> Result<orchestral_core::io::BlobMeta, orchestral_core::io::BlobIoError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            let mut bytes = Vec::new();
            while let Some(chunk) = request.body.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            let digest = orchestral_core::agent_protocol::wire::Digest::sha256(&bytes).to_string();
            let now = Utc::now();
            Ok(orchestral_core::io::BlobMeta {
                id: orchestral_core::io::BlobId::new(&digest),
                file_name: request.file_name,
                mime_type: request.mime_type,
                byte_size: bytes.len() as u64,
                checksum_sha256: Some(digest),
                metadata: request.metadata,
                created_at: now,
                updated_at: now,
            })
        }

        async fn read(
            &self,
            _: &orchestral_core::io::BlobId,
        ) -> Result<orchestral_core::io::BlobRead, orchestral_core::io::BlobIoError> {
            Err(orchestral_core::io::BlobIoError::Unsupported(
                "unused".to_owned(),
            ))
        }

        async fn head(
            &self,
            _: &orchestral_core::io::BlobId,
        ) -> Result<orchestral_core::io::BlobHead, orchestral_core::io::BlobIoError> {
            Err(orchestral_core::io::BlobIoError::Unsupported(
                "unused".to_owned(),
            ))
        }

        async fn delete(
            &self,
            _: &orchestral_core::io::BlobId,
        ) -> Result<bool, orchestral_core::io::BlobIoError> {
            Err(orchestral_core::io::BlobIoError::Unsupported(
                "unused".to_owned(),
            ))
        }
    }

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
            assert_eq!(read["params"]["includeTurns"], false);
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": read["id"],
                            "result": {"thread": {
                                "id": "thread-1", "preview": "compiler fix", "cwd": "/repo",
                                "createdAt": 10, "updatedAt": 20, "status": {"type": "idle"}
                            }}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let items = read_request(&mut requests).await;
            assert_eq!(items["method"], "thread/items/list");
            assert_eq!(items["params"]["limit"], 500);
            write_result(
                &mut server_write,
                &items,
                json!({
                    "data": [
                        {"turnId": "turn-1", "item": {"type": "agentMessage", "id": "a1", "text": "done"}},
                        {"turnId": "turn-1", "item": {"type": "userMessage", "id": "u1", "content": [{"type": "text", "text": "fix"}]}}
                    ],
                    "nextCursor": null,
                    "backwardsCursor": "newer"
                }),
            )
            .await;

            let queue = read_request(&mut requests).await;
            assert_eq!(queue["method"], "thread/queue/list");
            assert_eq!(queue["params"]["threadId"], "thread-1");
            write_result(
                &mut server_write,
                &queue,
                json!({
                    "data": [{
                        "id": "queue-1",
                        "clientUserMessageId": "client-1",
                        "input": [{"type": "text", "text": "queued follow-up"}]
                    }],
                    "nextCursor": null
                }),
            )
            .await;
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
        assert_eq!(detail.turns.len(), 2);
        assert_eq!(detail.turns[0].activities.len(), 2);
        assert_eq!(
            detail.turns[1].status,
            orchestral_core::agent_connector::AgentSessionTurnStatus::Pending
        );
        assert_eq!(detail.turns[1].activities[0].details["phase"], "deferred");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn session_snapshot_publishes_native_generated_image_as_downloadable_artifact() {
        let (client_io, server_io) = duplex(256 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let store = Arc::new(RecordingWriteBlobStore {
            writes: AtomicUsize::new(0),
        });
        let store_trait: Arc<dyn BlobStore> = store.clone();
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.artifact_blob_store = Some(store_trait);
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let summary = read_request(&mut requests).await;
            assert_eq!(summary["method"], "thread/read");
            write_result(
                &mut server_write,
                &summary,
                json!({"thread": thread("thread-image", Value::Null)}),
            )
            .await;

            let items = read_request(&mut requests).await;
            assert_eq!(items["method"], "thread/items/list");
            write_result(
                &mut server_write,
                &items,
                json!({
                    "data": [{
                        "turnId": "turn-image",
                        "item": {
                            "type": "Extension",
                            "kind": "image_gen.generation",
                            "id": "image-1",
                            "status": "completed",
                            "result": ONE_PIXEL_PNG,
                            "savedPath": "/private/generated/image-1.png",
                            "failure": null
                        }
                    }],
                    "nextCursor": null
                }),
            )
            .await;

            let queue = read_request(&mut requests).await;
            assert_eq!(queue["method"], "thread/queue/list");
            write_result(
                &mut server_write,
                &queue,
                json!({"data": [], "nextCursor": null}),
            )
            .await;
        });

        let detail = connector
            .read_session(&AgentSessionId::new("thread-image"))
            .await
            .unwrap();

        let activity = &detail.turns[0].activities[0];
        assert_eq!(
            activity.kind,
            orchestral_core::agent_connector::AgentSessionActivityKind::AgentMessage
        );
        assert_eq!(activity.content.len(), 2);
        assert!(matches!(
            activity.content[1].body,
            orchestral_core::agent_protocol::wire::ContentBody::Artifact(_)
        ));
        assert_eq!(activity.content[1].media_type, "image/png");
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn session_list_snapshot_survives_restart_without_a_native_round_trip() {
        let root = tempfile::tempdir().unwrap();
        let snapshot_path = root.path().join("session-list-cache.json");
        let query = AgentSessionListQuery {
            limit: 25,
            ..AgentSessionListQuery::default()
        };
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test")
            .with_session_list_cache_path(snapshot_path.clone());
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let request = read_request(&mut requests).await;
            assert_eq!(request["method"], "thread/list");
            write_result(
                &mut server_write,
                &request,
                json!({
                    "data": [{
                        "id": "thread-cached",
                        "preview": "cached preview",
                        "cwd": "/repo",
                        "createdAt": 10,
                        "updatedAt": 20,
                        "status": {"type": "idle"}
                    }],
                    "nextCursor": null
                }),
            )
            .await;
        });
        let expected = connector.list_sessions(query.clone()).await.unwrap();
        server.await.unwrap();
        assert!(snapshot_path.is_file());
        drop(connector);

        let (client_io, server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, _server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let restarted = CodexConnector::with_client(rpc, "codex/test")
            .with_session_list_cache_path(snapshot_path);
        let started = Instant::now();
        let actual =
            tokio::time::timeout(Duration::from_millis(50), restarted.list_sessions(query))
                .await
                .expect("persisted projection must not wait for Codex")
                .unwrap();
        assert_eq!(actual, expected);
        assert!(started.elapsed() < Duration::from_millis(50));
        let mut requests = BufReader::new(server_read).lines();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), requests.next_line())
                .await
                .is_err(),
            "cache hit unexpectedly called thread/list"
        );
    }

    #[tokio::test]
    async fn concurrent_cold_session_lists_share_one_native_scan() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let query = AgentSessionListQuery {
            limit: 25,
            ..AgentSessionListQuery::default()
        };
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let request = read_request(&mut requests).await;
            assert_eq!(request["method"], "thread/list");
            tokio::time::sleep(Duration::from_millis(20)).await;
            write_result(
                &mut server_write,
                &request,
                json!({"data": [], "nextCursor": null}),
            )
            .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(20), requests.next_line())
                    .await
                    .is_err(),
                "concurrent cache miss issued a second thread/list"
            );
        });

        let (left, right) = tokio::join!(
            connector.list_sessions(query.clone()),
            connector.list_sessions(query)
        );
        assert_eq!(left.unwrap(), right.unwrap());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn session_change_subscription_attaches_without_history_and_filters_notifications() {
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
            let resume = read_request(&mut requests).await;
            assert_eq!(resume["method"], "thread/resume");
            assert_eq!(resume["params"]["threadId"], "thread-watch");
            assert_eq!(resume["params"]["excludeTurns"], true);
            write_result(
                &mut server_write,
                &resume,
                json!({"thread": thread("thread-watch", Value::Null)}),
            )
            .await;
            server_write
                .write_all(
                    format!(
                        "{}\n{}\n{}\n",
                        json!({
                            "method": "item/completed",
                            "params": {"threadId": "thread-other", "turnId": "turn-1"}
                        }),
                        json!({
                            "method": "item/completed",
                            "params": {
                                "threadId": "thread-watch",
                                "turnId": "turn-2",
                                "item": {
                                    "type": "agentMessage",
                                    "id": "assistant-2",
                                    "text": "live answer"
                                }
                            }
                        }),
                        json!({
                            "method": "turn/completed",
                            "params": {
                                "threadId": "thread-watch",
                                "turnId": "turn-2",
                                "turn": {"status": "completed"}
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut changes = connector
            .subscribe_session_changes(&AgentSessionId::new("thread-watch"))
            .await
            .unwrap();
        let change = tokio::time::timeout(Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(change.connector_id.as_str(), "codex/local");
        assert_eq!(change.session_id.as_str(), "thread-watch");
        assert_eq!(change.sequence, 1);
        assert!(matches!(
            change.change,
            AgentSessionChangeKind::ActivityUpsert { activity, .. }
                if activity.activity_id.as_str() == "assistant-2"
        ));
        let completed = tokio::time::timeout(Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.sequence, 2);
        assert!(matches!(
            completed.change,
            AgentSessionChangeKind::TurnStatus {
                status: AgentSessionTurnStatus::Completed,
                ..
            }
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn live_generated_image_is_published_before_the_session_upsert_is_emitted() {
        let (client_io, server_io) = duplex(256 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let store = Arc::new(RecordingWriteBlobStore {
            writes: AtomicUsize::new(0),
        });
        let store_trait: Arc<dyn BlobStore> = store.clone();
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.artifact_blob_store = Some(store_trait);
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let resume = read_request(&mut requests).await;
            assert_eq!(resume["method"], "thread/resume");
            write_result(
                &mut server_write,
                &resume,
                json!({"thread": thread("thread-live-image", Value::Null)}),
            )
            .await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "method": "item/completed",
                            "params": {
                                "threadId": "thread-live-image",
                                "turnId": "turn-image",
                                "item": {
                                    "type": "Extension",
                                    "kind": "image_gen.generation",
                                    "id": "image-live-1",
                                    "status": "completed",
                                    "result": format!("data:image/png;base64,{ONE_PIXEL_PNG}"),
                                    "savedPath": "/private/generated/image-live-1.png",
                                    "failure": null
                                }
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut changes = connector
            .subscribe_session_changes(&AgentSessionId::new("thread-live-image"))
            .await
            .unwrap();
        let change = tokio::time::timeout(Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap();

        let AgentSessionChangeKind::ActivityUpsert { activity, .. } = change.change else {
            panic!("expected an image activity upsert");
        };
        assert_eq!(
            activity.kind,
            orchestral_core::agent_connector::AgentSessionActivityKind::AgentMessage
        );
        assert_eq!(activity.content.len(), 2);
        assert!(matches!(
            activity.content[1].body,
            orchestral_core::agent_protocol::wire::ContentBody::Artifact(_)
        ));
        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn session_subscription_observes_a_writer_loaded_in_the_shared_daemon() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.152.0");
        let (notify, notify_ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let resume = read_request(&mut requests).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": resume["id"],
                            "error": {
                                "code": -32000,
                                "message": "thread thread-watch already has an active writer"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let loaded = read_request(&mut requests).await;
            assert_eq!(loaded["method"], "thread/loaded/list");
            write_result(
                &mut server_write,
                &loaded,
                json!({"data": ["thread-watch"], "nextCursor": null}),
            )
            .await;
            notify_ready.await.unwrap();
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "method": "item/completed",
                            "params": {"threadId": "thread-watch", "turnId": "turn-2"}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut changes = connector
            .subscribe_session_changes(&AgentSessionId::new("thread-watch"))
            .await
            .unwrap();
        notify.send(()).unwrap();
        let change = tokio::time::timeout(Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(change.session_id.as_str(), "thread-watch");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn newly_created_session_subscription_reuses_the_loaded_thread() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.152.0");
        let (notify, notify_ready) = tokio::sync::oneshot::channel();
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
            notify_ready.await.unwrap();
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "method": "item/completed",
                            "params": {"threadId": "thread-new", "turnId": "turn-1"}
                        })
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
                options: Value::Null,
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        let mut changes = connector
            .subscribe_session_changes(&created.session_id)
            .await
            .unwrap();
        notify.send(()).unwrap();
        let change = tokio::time::timeout(Duration::from_secs(1), changes.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(change.session_id, created.session_id);
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
            assert_eq!(start["params"]["sandbox"], "workspace-write");
            assert_eq!(start["params"]["approvalPolicy"], "on-request");
            assert_eq!(start["params"]["model"], "gpt-5.6-terra");
            assert_eq!(
                start["params"]["config"],
                json!({"model_reasoning_effort": "high"})
            );
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

            let permissions = read_request(&mut requests).await;
            assert_eq!(permissions["method"], "thread/settings/update");
            assert_eq!(permissions["params"]["threadId"], "thread-fork");
            assert_eq!(
                permissions["params"]["sandboxPolicy"],
                json!({"type": "dangerFullAccess"})
            );
            assert_eq!(permissions["params"]["approvalPolicy"], "never");
            write_result(&mut server_write, &permissions, json!({})).await;

            let read_after_permissions = read_request(&mut requests).await;
            assert_eq!(read_after_permissions["method"], "thread/read");
            write_result(
                &mut server_write,
                &read_after_permissions,
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
        assert!(descriptor
            .action(&AgentSessionActionId::new(SESSION_SET_PERMISSIONS_ACTION))
            .is_some());
        assert_eq!(
            descriptor
                .creation
                .as_ref()
                .and_then(|creation| creation.connection_hint.as_deref()),
            Some("Shared daemon · unix://")
        );
        let creation_schema = descriptor
            .creation
            .as_ref()
            .and_then(|creation| creation.input_schema.as_ref())
            .expect("Codex creation options must be declared");
        assert!(!creation_schema["properties"]["model"]["enum"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(!creation_schema["properties"]["reasoning_effort"]["enum"]
            .as_array()
            .unwrap()
            .is_empty());

        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some("/repo".to_owned()),
                title: Some("Compiler work".to_owned()),
                options: json!({
                    "sandbox_mode": "workspace-write",
                    "approval_policy": "on-request",
                    "model": "gpt-5.6-terra",
                    "reasoning_effort": "high"
                }),
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

        let updated = connector
            .invoke_action(InvokeAgentSessionActionRequest {
                session_id: renamed.session_id,
                action_id: AgentSessionActionId::new(SESSION_SET_PERMISSIONS_ACTION),
                arguments: json!({
                    "sandbox_mode": "danger-full-access",
                    "approval_policy": "never"
                }),
                run_id: None,
            })
            .await
            .unwrap()
            .session
            .unwrap();
        assert_eq!(updated.title.as_deref(), Some("Release review"));
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

            let metadata_read = read_request(&mut requests).await;
            assert_eq!(metadata_read["method"], "thread/read");
            assert_eq!(metadata_read["params"]["includeTurns"], false);
            write_result(
                &mut server_write,
                &metadata_read,
                json!({"thread": thread("thread-new", Value::Null)}),
            )
            .await;

            let items = read_request(&mut requests).await;
            assert_eq!(items["method"], "thread/items/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": items["id"],
                            "error": {
                                "code": -32602,
                                "message": "thread thread-new is not materialized yet; thread/items/list is unavailable before first user message"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let queue = read_request(&mut requests).await;
            assert_eq!(queue["method"], "thread/queue/list");
            write_result(
                &mut server_write,
                &queue,
                json!({"data": [], "nextCursor": null}),
            )
            .await;
        });

        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: None,
                title: None,
                options: Value::Null,
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
    async fn codex_0152_missing_source_rollout_reads_as_empty_history() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.152.0");
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

            let metadata_read = read_request(&mut requests).await;
            assert_eq!(metadata_read["method"], "thread/read");
            write_result(
                &mut server_write,
                &metadata_read,
                json!({"thread": thread("thread-new", Value::Null)}),
            )
            .await;

            let items = read_request(&mut requests).await;
            assert_eq!(items["method"], "thread/items/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": items["id"],
                            "error": {"code": -32601, "message": "method not found"}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let queue = read_request(&mut requests).await;
            assert_eq!(queue["method"], "thread/queue/list");
            write_result(
                &mut server_write,
                &queue,
                json!({"data": [], "nextCursor": null}),
            )
            .await;

            let turns = read_request(&mut requests).await;
            assert_eq!(turns["method"], "thread/turns/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": turns["id"],
                            "error": {
                                "code": -32602,
                                "message": "invalid paginated history lineage for thread-new: missing source rollout"
                            }
                        })
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
                options: Value::Null,
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

            let thread_read = read_request(&mut requests).await;
            assert_eq!(thread_read["method"], "thread/turns/list");
            assert_eq!(thread_read["params"]["threadId"], "thread-new");
            write_result(
                &mut server_write,
                &thread_read,
                json!({"data": [], "nextCursor": null, "backwardsCursor": null}),
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
                options: Value::Null,
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
    async fn session_pages_use_native_item_pagination_without_full_thread_reads() {
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
            for (page_index, queue_id) in [(1, "queue-old"), (2, "queue-new")] {
                let summary = read_request(&mut requests).await;
                assert_eq!(summary["method"], "thread/read");
                assert_eq!(summary["params"]["includeTurns"], false);
                write_result(
                    &mut server_write,
                    &summary,
                    json!({"thread": thread("thread-large", Value::Null)}),
                )
                .await;

                let history = read_request(&mut requests).await;
                assert_eq!(history["method"], "thread/items/list");
                assert_eq!(history["params"]["limit"], 100);
                assert_eq!(history["params"]["sortDirection"], "desc");
                write_result(
                    &mut server_write,
                    &history,
                    json!({
                        "data": [
                            {"turnId": "turn-2", "item": {"type": "agentMessage", "id": format!("answer-{page_index}"), "text": "done"}},
                            {"turnId": "turn-2", "item": {"type": "userMessage", "id": "user-2", "content": [{"type": "text", "text": "continue"}]}},
                            {"turnId": "turn-1", "item": {"type": "commandExecution", "id": "command-59", "command": "cargo test", "status": "completed", "aggregatedOutput": "ok"}}
                        ],
                        "nextCursor": format!("older-{page_index}"),
                        "backwardsCursor": "newer"
                    }),
                )
                .await;

                let queue = read_request(&mut requests).await;
                assert_eq!(queue["method"], "thread/queue/list");
                write_result(
                    &mut server_write,
                    &queue,
                    json!({
                        "data": [{
                            "id": queue_id,
                            "clientUserMessageId": format!("client-{page_index}"),
                            "input": [{"type": "text", "text": "queued message"}]
                        }],
                        "nextCursor": null
                    }),
                )
                .await;
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

        assert_eq!(first.next_cursor.as_deref(), Some("older-1"));
        assert_ne!(first, second);
        assert_eq!(
            first.turns[1]
                .activities
                .last()
                .unwrap()
                .activity_id
                .as_str(),
            "answer-1"
        );
        assert_eq!(
            first.turns.last().unwrap().activities[0].details["queue_submission_id"],
            "queue-old"
        );
        assert_eq!(
            second.turns.last().unwrap().activities[0].details["queue_submission_id"],
            "queue-new"
        );
        assert!(serde_json::to_vec(&first).unwrap().len() < 64 * 1_024);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn latest_session_page_dispatches_independent_native_reads_concurrently() {
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
            // Do not answer until all independent reads have arrived. A
            // sequential implementation would time out here.
            let mut pending = Vec::new();
            for _ in 0..3 {
                pending.push(read_request(&mut requests).await);
            }
            let mut methods = pending
                .iter()
                .filter_map(|request| request["method"].as_str())
                .collect::<Vec<_>>();
            methods.sort_unstable();
            assert_eq!(
                methods,
                vec!["thread/items/list", "thread/queue/list", "thread/read"]
            );
            // Return responses in the reverse order to also verify JSON-RPC
            // correlation does not rely on response ordering.
            for request in pending.into_iter().rev() {
                let result = match request["method"].as_str().unwrap() {
                    "thread/read" => {
                        json!({"thread": thread("thread-parallel", Value::Null)})
                    }
                    "thread/items/list" => json!({
                        "data": [{
                            "turnId": "turn-1",
                            "item": {"type": "agentMessage", "id": "answer-1", "text": "done"}
                        }],
                        "nextCursor": null
                    }),
                    "thread/queue/list" => json!({"data": [], "nextCursor": null}),
                    method => panic!("unexpected method {method}"),
                };
                write_result(&mut server_write, &request, result).await;
            }
        });

        let page = connector
            .read_session_page(
                &AgentSessionId::new("thread-parallel"),
                AgentSessionReadQuery {
                    cursor: None,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.turns[0].activities[0].activity_id.as_str(), "answer-1");
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
                options: Value::Null,
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

        let permission_error = runtime
            .block_on(connector.invoke_action(InvokeAgentSessionActionRequest {
                session_id: AgentSessionId::new("thread-1"),
                action_id: AgentSessionActionId::new(SESSION_SET_PERMISSIONS_ACTION),
                arguments: json!({
                    "sandbox_mode": "unbounded",
                    "approval_policy": "never"
                }),
                run_id: None,
            }))
            .unwrap_err();
        assert_eq!(
            permission_error.code,
            AgentConnectorErrorCode::InvalidRequest
        );
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
