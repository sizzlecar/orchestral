//! Claude Code's local session directory and bidirectional stream-json SDK.
//!
//! Native transcript and control-wire details stay in this plugin. Existing
//! terminals receive messages through their peer inbox and expose history and
//! approvals. Native peer-origin semantics remain intact; SDK sessions receive
//! direct user input.

mod approvals;
mod delivery;
mod history;
mod observation;
mod peer;
mod provider;
mod transport;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use orchestral_core::agent_connector::{
    AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorErrorCode,
    AgentConnectorHealth, AgentConnectorId, AgentSessionActionDescriptor,
    AgentSessionActionExecution, AgentSessionActionId, AgentSessionActionOutcome,
    AgentSessionCapabilities, AgentSessionChange, AgentSessionCreationDescriptor,
    AgentSessionDetail, AgentSessionExecutionProfile, AgentSessionListQuery, AgentSessionPage,
    AgentSessionState, AgentSessionSummary, AgentSessionTextInput, CreateAgentSessionRequest,
    InvokeAgentSessionActionRequest,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use approvals::run_permission_hook;

/// Host-local configuration. No credentials are copied or stored by the plugin.
#[derive(Debug, Clone)]
pub struct ClaudeCodeConfig {
    pub executable: PathBuf,
    /// Claude's native config directory (`CLAUDE_CONFIG_DIR` or `~/.claude`).
    pub config_dir: PathBuf,
    /// Host-owned metadata for sessions which have not had their first turn.
    pub session_store_dir: PathBuf,
    pub request_timeout: Duration,
}

pub(crate) const CONNECTOR_ID: &str = "claude/local";

struct CachedHistory {
    modified: Option<SystemTime>,
    size: u64,
    detail: AgentSessionDetail,
}

/// Complete Agent integration for Claude Code, registered as `claude/local`.
pub struct ClaudeCodeConnector {
    config: ClaudeCodeConfig,
    history_cache: Arc<Mutex<BTreeMap<PathBuf, CachedHistory>>>,
    provider_state: Arc<Mutex<provider::ProviderState>>,
    peer: tokio::sync::OnceCell<Arc<peer::PeerInbox>>,
    peer_gate: tokio::sync::Mutex<()>,
    native_approvals: Arc<approvals::NativeApprovals>,
    #[cfg(unix)]
    approval_bridge: tokio::sync::OnceCell<Arc<approvals::ApprovalBridge>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeSession {
    pid: u32,
    session_id: String,
    cwd: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    updated_at: Option<i64>,
    #[serde(default)]
    messaging_socket_path: Option<PathBuf>,
    #[serde(default)]
    peer_protocol: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CreatedSession {
    session_id: String,
    cwd: String,
    title: Option<String>,
    created_at_unix_ms: i64,
}

impl ClaudeCodeConnector {
    pub fn new(config: ClaudeCodeConfig) -> Self {
        Self {
            config,
            history_cache: Arc::default(),
            provider_state: Arc::default(),
            peer: tokio::sync::OnceCell::new(),
            peer_gate: tokio::sync::Mutex::new(()),
            native_approvals: Arc::default(),
            #[cfg(unix)]
            approval_bridge: tokio::sync::OnceCell::new(),
        }
    }

    fn provider_state(&self) -> MutexGuard<'_, provider::ProviderState> {
        lock(&self.provider_state)
    }

    async fn scan(&self) -> Result<Vec<AgentSessionDetail>, AgentConnectorError> {
        let config = self.config.clone();
        let cache = self.history_cache.clone();
        // Never race a live dispatch claim. Listing after a restart also repairs
        // confirmations which arrived after the original HTTP request ended.
        let gate = self.peer_gate.try_lock().ok();
        let reconcile = gate.is_some();
        let mut sessions = tokio::task::spawn_blocking(move || {
            if reconcile {
                peer::reconcile(&config)?;
            }
            scan(&config, &cache, None)
        })
        .await
        .map_err(|error| {
            AgentConnectorError::protocol(format!("Claude session scan failed: {error}"))
        })??;
        drop(gate);
        for detail in &mut sessions {
            if let Some(state) = self
                .provider_state()
                .session_state(&detail.summary.session_id)
            {
                if state != AgentSessionState::Idle {
                    detail.summary.state = state;
                    detail.summary.input_action = None;
                }
            }
            detail.pending_requests = self.native_approvals.pending(&detail.summary.session_id);
            if !detail.pending_requests.is_empty() {
                detail.summary.state = AgentSessionState::WaitingApproval;
            }
        }
        Ok(sessions)
    }

    fn native_approvals_enabled(&self) -> bool {
        #[cfg(unix)]
        {
            self.approval_bridge.get().is_some()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    async fn native_owner(&self, session_id: &AgentSessionId) -> Result<bool, AgentConnectorError> {
        let root = self.config.config_dir.clone();
        let session_id = session_id.as_str().to_owned();
        tokio::task::spawn_blocking(move || {
            native_sessions(&root).map(|sessions| {
                sessions
                    .iter()
                    .any(|session| session.session_id == session_id)
            })
        })
        .await
        .map_err(|error| AgentConnectorError::protocol(error.to_string()))?
    }

    async fn transcript_exists(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<bool, AgentConnectorError> {
        let root = self.config.config_dir.join("projects");
        let filename = format!("{}.jsonl", session_id.as_str());
        tokio::task::spawn_blocking(move || {
            for project in entries(&root)? {
                if project.is_dir() && project.join(&filename).is_file() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
        .map_err(|error| AgentConnectorError::protocol(error.to_string()))?
    }
}

#[async_trait]
impl AgentConnector for ClaudeCodeConnector {
    fn describe(&self) -> AgentConnectorDescriptor {
        AgentConnectorDescriptor {
            connector_id: AgentConnectorId::new(CONNECTOR_ID), provider_binding: ProviderBindingRef::new(CONNECTOR_ID),
            agent_family: "coding-agent".to_owned(), display_name: "Claude Code".to_owned(),
            capabilities: AgentSessionCapabilities { list: true, read: true, create: true, resolve_requests: self.native_approvals_enabled() },
            creation: Some(AgentSessionCreationDescriptor {
                accepts_cwd: true, default_cwd: std::env::current_dir().ok().map(|path| path.to_string_lossy().into_owned()), input_schema: None,
                connection_hint: Some(if self.native_approvals_enabled() {
                    "已有 Claude 终端可接收 PWA 消息和处理工具审批；Claude 会给这些消息附加跨会话来源说明。PWA 新建会话使用直接用户输入，支持审批和取消。"
                } else {
                    "已有 Claude 终端可接收 PWA 消息，Claude 会附加跨会话来源说明；启用 --claude-approvals 后可在 PWA 处理工具审批。PWA 新建会话使用直接用户输入。"
                }.to_owned()),
            }), actions: vec![AgentSessionActionDescriptor {
                action_id: AgentSessionActionId::new(peer::INPUT_ACTION),
                title: "发送到现有 Claude 终端".to_owned(),
                description: "Claude 会将此消息标为其他会话的同伴请求，并附加来源说明；这不是用户指令，不能用来批准操作。".to_owned(),
                input_schema: Some(serde_json::json!({"type":"object","properties":{"submission_id":{"type":"string"},"text":{"type":"string"}},"required":["submission_id","text"],"additionalProperties":false})),
                input_channel: true,
                execution: AgentSessionActionExecution::Immediate,
            }],
        }
    }

    async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
        let output = tokio::time::timeout(
            self.config.request_timeout,
            tokio::process::Command::new(&self.config.executable)
                .arg("--version")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| {
            AgentConnectorError::new(
                AgentConnectorErrorCode::Unavailable,
                "Claude version check timed out",
                true,
            )
        })?
        .map_err(io_error)?;
        if !output.status.success() {
            return Err(AgentConnectorError::new(
                AgentConnectorErrorCode::Unavailable,
                "Claude Code executable is unavailable",
                false,
            ));
        }
        Ok(AgentConnectorHealth::ready(Some(
            String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        )))
    }

    async fn list_sessions(
        &self,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
        query.validate()?;
        let offset = match query.cursor.as_deref() {
            None => 0,
            Some(cursor) => cursor
                .strip_prefix("claude-sessions:")
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or_else(|| {
                    AgentConnectorError::invalid("invalid Claude session-list cursor")
                })?,
        };
        let search = query.search.as_ref().map(|text| text.to_lowercase());
        let mut summaries = self
            .scan()
            .await?
            .into_iter()
            .map(|detail| detail.summary)
            .filter(|summary| {
                query
                    .cwd
                    .as_ref()
                    .is_none_or(|cwd| summary.cwd.as_ref() == Some(cwd))
                    && search.as_ref().is_none_or(|search| {
                        [&summary.title, &summary.preview, &summary.cwd]
                            .iter()
                            .filter_map(|value| value.as_ref())
                            .any(|text| text.to_lowercase().contains(search))
                    })
            })
            .collect::<Vec<_>>();
        summaries.sort_by_key(|summary| {
            std::cmp::Reverse((summary.updated_at_unix_ms, summary.session_id.clone()))
        });
        let has_more = summaries.len() > offset.saturating_add(query.limit as usize);
        let page = AgentSessionPage {
            sessions: summaries
                .into_iter()
                .skip(offset)
                .take(query.limit as usize)
                .collect(),
            next_cursor: has_more
                .then(|| format!("claude-sessions:{}", offset + query.limit as usize)),
        };
        page.validate_for(&AgentConnectorId::new(CONNECTOR_ID), query.limit)?;
        Ok(page)
    }

    async fn invoke_action(
        &self,
        request: InvokeAgentSessionActionRequest,
    ) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
        if request.action_id.as_str() != peer::INPUT_ACTION || request.run_id.is_some() {
            return Err(AgentConnectorError::unsupported(
                "unsupported Claude session action",
            ));
        }
        let input: AgentSessionTextInput = serde_json::from_value(request.arguments)
            .map_err(|error| AgentConnectorError::invalid(error.to_string()))?;
        input.validate()?;
        peer::submit(self, &request.session_id, input).await
    }

    async fn subscribe_session_changes(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<tokio::sync::broadcast::Receiver<AgentSessionChange>, AgentConnectorError> {
        observation::subscribe(self, session_id).await
    }

    async fn resolve_request(
        &self,
        request: orchestral_core::agent_connector::ResolveAgentSessionRequest,
    ) -> Result<(), AgentConnectorError> {
        self.native_approvals.resolve(request).await
    }

    async fn read_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        validate_session_id(session_id.as_str())?;
        let detail = self
            .scan()
            .await?
            .into_iter()
            .find(|detail| detail.summary.session_id == *session_id)
            .ok_or_else(|| {
                AgentConnectorError::new(
                    AgentConnectorErrorCode::NotFound,
                    "Claude session does not exist",
                    false,
                )
            })?;
        detail.validate_for(&AgentConnectorId::new(CONNECTOR_ID))?;
        Ok(detail)
    }

    async fn create_session(
        &self,
        request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        if !(request.options.is_null()
            || request
                .options
                .as_object()
                .is_some_and(|options| options.is_empty()))
            || !request.extensions.is_empty()
        {
            return Err(AgentConnectorError::unsupported(
                "Claude session creation accepts cwd and title only",
            ));
        }
        let cwd = request
            .cwd
            .filter(|cwd| !cwd.trim().is_empty())
            .ok_or_else(|| AgentConnectorError::invalid("Claude session creation requires cwd"))?;
        let path = tokio::fs::canonicalize(&cwd).await.map_err(io_error)?;
        if !path.is_dir() {
            return Err(AgentConnectorError::invalid(
                "Claude working directory must be a directory",
            ));
        }
        let session = CreatedSession {
            session_id: uuid::Uuid::new_v4().to_string(),
            cwd: path.to_string_lossy().into_owned(),
            title: request.title,
            created_at_unix_ms: chrono::Utc::now().timestamp_millis(),
        };
        tokio::fs::create_dir_all(&self.config.session_store_dir)
            .await
            .map_err(io_error)?;
        let bytes = serde_json::to_vec(&session)
            .map_err(|error| AgentConnectorError::protocol(error.to_string()))?;
        tokio::fs::write(
            self.config
                .session_store_dir
                .join(format!("{}.json", session.session_id)),
            bytes,
        )
        .await
        .map_err(io_error)?;
        Ok(created_summary(&session))
    }
}

fn scan(
    config: &ClaudeCodeConfig,
    cache: &Mutex<BTreeMap<PathBuf, CachedHistory>>,
    session_id: Option<&AgentSessionId>,
) -> Result<Vec<AgentSessionDetail>, AgentConnectorError> {
    let mut sessions = BTreeMap::<String, AgentSessionDetail>::new();
    for path in entries(&config.session_store_dir)? {
        if session_id
            .is_some_and(|id| path.file_stem().and_then(|stem| stem.to_str()) != Some(id.as_str()))
        {
            continue;
        }
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            let session: CreatedSession = read_json(&path)?;
            validate_session_id(&session.session_id)?;
            sessions.insert(
                session.session_id.clone(),
                AgentSessionDetail {
                    summary: created_summary(&session),
                    turns: Vec::new(),
                    pending_requests: Vec::new(),
                    next_cursor: None,
                },
            );
        }
    }
    let mut present = std::collections::BTreeSet::new();
    for project in entries(&config.config_dir.join("projects"))? {
        if !project.is_dir() {
            continue;
        }
        for path in entries(&project)? {
            if session_id.is_some_and(|id| {
                path.file_stem().and_then(|stem| stem.to_str()) != Some(id.as_str())
            }) {
                continue;
            }
            if path
                .extension()
                .is_none_or(|extension| extension != "jsonl")
            {
                continue;
            }
            let Some(id) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .filter(|id| uuid::Uuid::parse_str(id).is_ok())
            else {
                continue;
            };
            let metadata = path.metadata().map_err(io_error)?;
            let modified = metadata.modified().ok();
            present.insert(path.clone());
            let mut cache = lock(cache);
            if !cache
                .get(&path)
                .is_some_and(|cached| cached.modified == modified && cached.size == metadata.len())
            {
                let records = history::records(&path)?;
                let detail = transcript_detail(id, &records);
                cache.insert(
                    path.clone(),
                    CachedHistory {
                        modified,
                        size: metadata.len(),
                        detail,
                    },
                );
            }
            let mut detail = cache[&path].detail.clone();
            if let Some(created) = sessions.get(id) {
                if detail.summary.title.is_none() {
                    detail.summary.title = created.summary.title.clone();
                }
                if detail.summary.cwd.is_none() {
                    detail.summary.cwd = created.summary.cwd.clone();
                }
            }
            sessions.insert(id.to_owned(), detail);
        }
    }
    if session_id.is_none() {
        lock(cache).retain(|path, _| present.contains(path));
    }
    for native in native_sessions(&config.config_dir)? {
        if session_id.is_some_and(|id| native.session_id != id.as_str()) {
            continue;
        }
        let detail = sessions
            .entry(native.session_id.clone())
            .or_insert_with(|| AgentSessionDetail {
                summary: created_summary(&CreatedSession {
                    session_id: native.session_id.clone(),
                    cwd: native.cwd.clone(),
                    title: native.name.clone(),
                    created_at_unix_ms: native.started_at.unwrap_or(0),
                }),
                turns: Vec::new(),
                pending_requests: Vec::new(),
                next_cursor: None,
            });
        // Keep sending available while preserving Claude's native peer origin.
        // This does not grant the sender the terminal's approval authority.
        if cfg!(unix) && native.peer_protocol == Some(1) && native.messaging_socket_path.is_some() {
            detail.summary.input_action = Some(AgentSessionActionId::new(peer::INPUT_ACTION));
        }
        detail.summary.cwd = Some(native.cwd);
        if native.name.is_some() {
            detail.summary.title = native.name;
        }
        detail.summary.updated_at_unix_ms = native.updated_at.or(detail.summary.updated_at_unix_ms);
        detail.summary.state = match native.status.as_deref() {
            Some("idle") => AgentSessionState::Idle,
            Some("waiting_for_permission" | "waiting_approval") => {
                AgentSessionState::WaitingApproval
            }
            Some("waiting_for_input") => AgentSessionState::WaitingInput,
            _ => AgentSessionState::BusyElsewhere,
        };
    }
    peer::overlay_pending(config, &mut sessions)?;
    Ok(sessions.into_values().collect())
}

fn transcript_detail(id: &str, records: &[Value]) -> AgentSessionDetail {
    let mut summary = created_summary(&CreatedSession {
        session_id: id.to_owned(),
        cwd: String::new(),
        title: None,
        created_at_unix_ms: 0,
    });
    summary.cwd = None;
    summary.created_at_unix_ms = records.iter().find_map(history::timestamp);
    summary.updated_at_unix_ms = records.iter().rev().find_map(history::timestamp);
    for record in records {
        if let Some(cwd) = record
            .get("cwd")
            .and_then(Value::as_str)
            .filter(|cwd| !cwd.is_empty())
        {
            summary.cwd = Some(cwd.to_owned());
        }
        if let Some(title) = record
            .get("customTitle")
            .or_else(|| record.get("aiTitle"))
            .and_then(Value::as_str)
        {
            summary.title = Some(title.to_owned());
        }
        if record.get("type").and_then(Value::as_str) == Some("user")
            && summary.preview.is_none()
            && record.get("isMeta").and_then(Value::as_bool) != Some(true)
        {
            let text = history::text(record.pointer("/message/content").unwrap_or(&Value::Null));
            if !text.is_empty() {
                summary.preview = Some(text.chars().take(200).collect());
            }
        }
        if let Some(model) = record
            .pointer("/message/model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty() && !model.starts_with('<'))
        {
            summary.execution_profile.model = Some(model.to_owned());
        }
    }
    AgentSessionDetail {
        summary,
        turns: history::turns(records),
        pending_requests: Vec::new(),
        next_cursor: None,
    }
}

fn created_summary(session: &CreatedSession) -> AgentSessionSummary {
    AgentSessionSummary {
        connector_id: AgentConnectorId::new(CONNECTOR_ID),
        session_id: AgentSessionId::new(&session.session_id),
        title: session.title.clone(),
        preview: None,
        cwd: Some(session.cwd.clone()),
        created_at_unix_ms: Some(session.created_at_unix_ms),
        updated_at_unix_ms: Some(session.created_at_unix_ms),
        state: AgentSessionState::Detached,
        input_action: None,
        execution_profile: AgentSessionExecutionProfile::default(),
        extensions: BTreeMap::new(),
    }
}

fn native_sessions(root: &Path) -> Result<Vec<NativeSession>, AgentConnectorError> {
    let mut sessions = Vec::new();
    for path in entries(&root.join("sessions"))? {
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        // Claude can replace registry entries while they are being read.
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error(error)),
        };
        let session: NativeSession = serde_json::from_slice(&bytes).map_err(|error| {
            AgentConnectorError::new(
                AgentConnectorErrorCode::Unavailable,
                format!("Claude process registry is being updated: {error}"),
                true,
            )
        })?;
        if validate_session_id(&session.session_id).is_err() || session.cwd.is_empty() {
            continue;
        }
        #[cfg(unix)]
        let alive = i32::try_from(session.pid)
            .ok()
            .filter(|pid| *pid > 0)
            .is_some_and(|pid| unsafe { libc::kill(pid, 0) } == 0);
        #[cfg(not(unix))]
        let alive = session.pid > 0;
        if alive {
            sessions.push(session);
        }
    }
    Ok(sessions)
}

fn entries(root: &Path) -> Result<Vec<PathBuf>, AgentConnectorError> {
    match std::fs::read_dir(root) {
        Ok(entries) => entries
            .map(|entry| entry.map(|entry| entry.path()).map_err(io_error))
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(io_error(error)),
    }
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, AgentConnectorError> {
    serde_json::from_slice(&std::fs::read(path).map_err(io_error)?).map_err(|error| {
        AgentConnectorError::protocol(format!("invalid Claude session metadata: {error}"))
    })
}

fn validate_session_id(id: &str) -> Result<(), AgentConnectorError> {
    uuid::Uuid::parse_str(id)
        .map(|_| ())
        .map_err(|_| AgentConnectorError::invalid("Claude session id must be a UUID"))
}

fn io_error(error: std::io::Error) -> AgentConnectorError {
    AgentConnectorError::new(
        AgentConnectorErrorCode::Unavailable,
        format!("Claude session I/O: {error}"),
        true,
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn connector(root: &Path) -> ClaudeCodeConnector {
        ClaudeCodeConnector::new(ClaudeCodeConfig {
            executable: "claude-fixture".into(),
            config_dir: root.join("native"),
            session_store_dir: root.join("host"),
            request_timeout: Duration::from_secs(2),
        })
    }

    #[tokio::test]
    async fn discovers_native_transcripts_filters_pages_and_preserves_created_sessions() {
        let root = tempfile::tempdir().unwrap();
        let connector = connector(root.path());
        let project = connector.config.config_dir.join("projects").join("project");
        std::fs::create_dir_all(&project).unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let records = [
            json!({"type":"user","uuid":"u1","sessionId":id,"cwd":root.path(),"timestamp":"2026-10-01T01:00:00Z","message":{"content":"inspect source"}}),
            json!({"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":id,"timestamp":"2026-10-01T01:00:01Z","message":{"model":"claude-sonnet","content":[{"type":"text","text":"inspected"}],"stop_reason":"end_turn"}}),
            json!({"type":"ai-title","aiTitle":"Code inspection","sessionId":id}),
        ];
        std::fs::write(
            project.join(format!("{id}.jsonl")),
            records
                .iter()
                .map(|value| format!("{value}\n"))
                .collect::<String>(),
        )
        .unwrap();
        // Subagent files live one level deeper and are never separate main sessions.
        std::fs::create_dir_all(project.join(&id).join("subagents")).unwrap();
        std::fs::write(
            project.join(&id).join("subagents/agent-child.jsonl"),
            "ignored",
        )
        .unwrap();
        let detail = connector
            .read_session(&AgentSessionId::new(&id))
            .await
            .unwrap();
        assert_eq!(detail.summary.title.as_deref(), Some("Code inspection"));
        assert_eq!(
            detail.summary.execution_profile.model.as_deref(),
            Some("claude-sonnet")
        );
        assert_eq!(detail.turns[0].activities.len(), 2);
        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some(root.path().to_string_lossy().into_owned()),
                title: Some("New work".to_owned()),
                options: json!({}),
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        let restarted = tests::connector(root.path());
        assert_eq!(
            restarted
                .read_session(&created.session_id)
                .await
                .unwrap()
                .summary,
            created
        );
        let page = restarted
            .list_sessions(AgentSessionListQuery {
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.sessions[0].session_id, created.session_id);
        let second = restarted
            .list_sessions(AgentSessionListQuery {
                limit: 1,
                cursor: page.next_cursor,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(second.sessions[0].session_id.as_str(), id);
        let filtered = restarted
            .list_sessions(AgentSessionListQuery {
                search: Some("inspection".to_owned()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(filtered.sessions.len(), 1);
        assert!(restarted
            .read_session(&AgentSessionId::new("../../escape"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn cached_transcript_refreshes_when_claude_appends_a_message() {
        let root = tempfile::tempdir().unwrap();
        let connector = connector(root.path());
        let project = connector.config.config_dir.join("projects/project");
        std::fs::create_dir_all(&project).unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let path = project.join(format!("{id}.jsonl"));
        let user =
            json!({"type":"user","uuid":"u1","cwd":root.path(),"message":{"content":"first"}});
        std::fs::write(&path, format!("{user}\n")).unwrap();
        assert_eq!(
            connector
                .read_session(&AgentSessionId::new(&id))
                .await
                .unwrap()
                .turns[0]
                .activities
                .len(),
            1
        );
        let assistant = json!({"type":"assistant","uuid":"a1","parentUuid":"u1","message":{"content":[{"type":"text","text":"reply"}],"stop_reason":"end_turn"}});
        std::fs::write(&path, format!("{user}\n{assistant}\n")).unwrap();
        assert_eq!(
            connector
                .read_session(&AgentSessionId::new(&id))
                .await
                .unwrap()
                .turns[0]
                .activities
                .len(),
            2
        );
    }
}
