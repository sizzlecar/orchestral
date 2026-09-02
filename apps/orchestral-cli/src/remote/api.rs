use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{DefaultBodyLimit, MatchedPath};
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use orchestral_core::agent_connector::{
    AgentConnectorId, AgentSessionActionId, AgentSessionActionOutcome, AgentSessionChange,
    AgentSessionListQuery, AgentSessionPage, AgentSessionReadQuery, AgentSessionSummary,
    CreateAgentSessionRequest, InvokeAgentSessionActionRequest,
};
use orchestral_core::agent_protocol::spi::AgentStartError;
use orchestral_core::agent_protocol::wire::{
    AgentCommand, AgentCommandEnvelope, AgentRejectionCode, AgentRunView, AgentSessionId,
    ApprovalDecision, ArtifactRef, ArtifactRefWithDigest, CommandAck, CommandAckState, CommandId,
    Content, ContentBody, Digest, PendingRequest, PendingRequestPayload, RequestId,
    RequestResolution, RunId,
};
use orchestral_core::io::{ArtifactResolver, BlobStore};
use orchestral_runtime::api::AgentApi;
use orchestral_runtime::{
    AgentControlError, AgentControlEvent, AgentDirectory, AgentDirectoryError, AgentSdkError,
    ApprovalBridgeError, InMemoryHostApprovalBroker,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::Instrument;

use super::auth::{GatewayAuthenticator, GatewayPrincipal};
use super::session_coordinator::AgentSessionCoordinatorRegistry;
use super::state::{DevicePrincipal, DeviceView, PairingClaim, RemoteRegistry, SessionView};

const APPROVAL_GRANT_TTL_MS: i64 = 5 * 60 * 1_000;
const RUN_SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_secs(15);
const RUN_SUPERVISOR_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const RUN_SUPERVISOR_MAX_BACKOFF: Duration = Duration::from_secs(5);
const REQUEST_ID_HEADER: &str = "x-request-id";

#[derive(Default)]
pub struct RunSupervisorRegistry {
    active: Mutex<BTreeSet<String>>,
}

impl RunSupervisorRegistry {
    fn key(connector_id: Option<&AgentConnectorId>, run_id: &RunId) -> String {
        format!(
            "{}\0{}",
            connector_id.map_or("orchestral", AgentConnectorId::as_str),
            run_id.as_str()
        )
    }

    fn begin(&self, key: &str) -> bool {
        self.active
            .lock()
            .expect("Run supervisor registry lock poisoned")
            .insert(key.to_owned())
    }

    fn finish(&self, key: &str) {
        self.active
            .lock()
            .expect("Run supervisor registry lock poisoned")
            .remove(key);
    }
}

#[derive(Clone)]
pub struct RemoteApiState {
    pub agent: AgentApi,
    pub agent_directory: Arc<AgentDirectory>,
    pub approvals: Arc<InMemoryHostApprovalBroker>,
    pub registry: RemoteRegistry,
    pub gateway_authenticator: Option<Arc<dyn GatewayAuthenticator>>,
    pub run_supervisors: Arc<RunSupervisorRegistry>,
    pub(super) session_coordinators: Arc<AgentSessionCoordinatorRegistry>,
    pub artifact_resolver: Option<Arc<dyn ArtifactResolver>>,
    pub artifact_blob_store: Option<Arc<dyn BlobStore>>,
}

#[derive(Debug, Clone)]
enum RemotePrincipal {
    Device(DevicePrincipal),
    Gateway(GatewayPrincipal),
}

#[derive(Debug, Clone)]
struct RequestLogContext {
    request_id: String,
}

#[derive(Debug, Clone)]
struct ApiErrorLogCode(String);

struct SseLifecycleLog {
    request_id: String,
    stream_id: String,
    stream_kind: &'static str,
    connector_id: String,
    session_id: Option<String>,
    run_id: Option<String>,
    opened_at: Instant,
    close_reason: Option<&'static str>,
}

impl SseLifecycleLog {
    fn open_agent_session(
        request: &RequestLogContext,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
    ) -> Self {
        Self::open(
            request,
            "agent_session",
            connector_id.as_str(),
            Some(session_id.as_str()),
            None,
        )
    }

    fn open_run(
        request: &RequestLogContext,
        connector_id: Option<&AgentConnectorId>,
        run_id: &RunId,
    ) -> Self {
        Self::open(
            request,
            "run",
            connector_id.map_or("orchestral", AgentConnectorId::as_str),
            None,
            Some(run_id.as_str()),
        )
    }

    fn open(
        request: &RequestLogContext,
        stream_kind: &'static str,
        connector_id: &str,
        session_id: Option<&str>,
        run_id: Option<&str>,
    ) -> Self {
        let lifecycle = Self {
            request_id: request.request_id.clone(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            stream_kind,
            connector_id: connector_id.to_owned(),
            session_id: session_id.map(str::to_owned),
            run_id: run_id.map(str::to_owned),
            opened_at: Instant::now(),
            close_reason: None,
        };
        tracing::info!(
            request_id = %lifecycle.request_id,
            stream_id = %lifecycle.stream_id,
            stream_kind = lifecycle.stream_kind,
            connector_id = %lifecycle.connector_id,
            session_id = lifecycle.session_id.as_deref().unwrap_or("-"),
            run_id = lifecycle.run_id.as_deref().unwrap_or("-"),
            "SSE stream opened"
        );
        lifecycle
    }

    fn close_as(&mut self, reason: &'static str) {
        self.close_reason = Some(reason);
    }

    fn lagged(&self, skipped: u64) {
        tracing::warn!(
            request_id = %self.request_id,
            stream_id = %self.stream_id,
            stream_kind = self.stream_kind,
            connector_id = %self.connector_id,
            session_id = self.session_id.as_deref().unwrap_or("-"),
            run_id = self.run_id.as_deref().unwrap_or("-"),
            skipped,
            "SSE subscriber lagged"
        );
    }
}

impl Drop for SseLifecycleLog {
    fn drop(&mut self) {
        let lifetime_ms = u64::try_from(self.opened_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        tracing::info!(
            request_id = %self.request_id,
            stream_id = %self.stream_id,
            stream_kind = self.stream_kind,
            connector_id = %self.connector_id,
            session_id = self.session_id.as_deref().unwrap_or("-"),
            run_id = self.run_id.as_deref().unwrap_or("-"),
            close_reason = self.close_reason.unwrap_or("client_disconnected"),
            lifetime_ms,
            "SSE stream closed"
        );
    }
}

impl RemotePrincipal {
    fn current_device_id(&self) -> Option<&str> {
        match self {
            Self::Device(principal) => Some(&principal.device_id),
            Self::Gateway(_) => None,
        }
    }
}

pub fn router(state: RemoteApiState) -> Router {
    let protected = Router::new()
        .route("/me", get(me))
        .route("/devices", get(list_devices))
        .route("/devices/{device_id}", delete(revoke_device))
        .route("/sessions", get(list_sessions).post(create_session))
        .route("/sessions/{session_id}", get(get_session))
        .route("/sessions/{session_id}/runs", post(start_run))
        .route("/agent-connectors", get(list_agent_connectors))
        .route(
            "/agent-sessions",
            get(list_agent_sessions).post(create_agent_session),
        )
        .route("/agent-session", get(get_agent_session))
        .route("/agent-session/stream", get(agent_session_stream))
        .route("/agent-session/actions", post(invoke_agent_session_action))
        .route("/agent-runs", post(start_agent_run))
        .route("/runs/{run_id}", get(inspect_run))
        .route("/runs/{run_id}/events", get(run_events))
        .route("/runs/{run_id}/stream", get(run_stream))
        .route("/runs/{run_id}/recover", post(recover_run))
        .route("/runs/{run_id}/steer", post(steer_run))
        .route("/runs/{run_id}/cancel", post(cancel_run))
        .route(
            "/runs/{run_id}/requests/{request_id}/input",
            post(resolve_input),
        )
        .route(
            "/runs/{run_id}/requests/{request_id}/approval",
            post(resolve_approval),
        )
        .layer(middleware::from_fn_with_state(state.clone(), authenticate));

    Router::new()
        .route("/health", get(health))
        .route("/pairing/claim", post(claim_pairing))
        .merge(protected)
        .layer(DefaultBodyLimit::max(256 * 1_024))
        .layer(middleware::from_fn(no_store))
        .layer(middleware::from_fn(log_request))
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    protocol: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        protocol: "orchestral-remote-v1",
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairingClaimRequest {
    secret: String,
    device_name: String,
}

async fn claim_pairing(
    State(state): State<RemoteApiState>,
    Json(request): Json<PairingClaimRequest>,
) -> Result<Json<PairingClaim>, ApiError> {
    let claim = state
        .registry
        .claim_pairing(&request.secret, &request.device_name)
        .await
        .map_err(|error| ApiError::unauthorized("pairing_failed", error.to_string()))?;
    Ok(Json(claim))
}

#[derive(Debug, Serialize)]
struct MeResponse {
    auth_mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    attributes: BTreeMap<String, String>,
}

async fn me(Extension(principal): Extension<RemotePrincipal>) -> Json<MeResponse> {
    Json(match principal {
        RemotePrincipal::Device(principal) => MeResponse {
            auth_mode: "device_token",
            device_id: Some(principal.device_id),
            subject: None,
            attributes: BTreeMap::new(),
        },
        RemotePrincipal::Gateway(principal) => MeResponse {
            auth_mode: "gateway_jwt",
            device_id: None,
            subject: principal.subject,
            attributes: principal.attributes,
        },
    })
}

async fn list_devices(
    State(state): State<RemoteApiState>,
    Extension(principal): Extension<RemotePrincipal>,
) -> Json<Vec<DeviceView>> {
    Json(
        state
            .registry
            .devices(principal.current_device_id().unwrap_or_default())
            .await,
    )
}

async fn revoke_device(
    State(state): State<RemoteApiState>,
    Path(device_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .registry
        .revoke_device(&device_id)
        .await
        .map_err(|error| ApiError::not_found("device_not_found", error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_sessions(
    State(state): State<RemoteApiState>,
) -> Result<Json<Vec<SessionView>>, ApiError> {
    Ok(Json(session_views(&state).await?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateSessionRequest {
    #[serde(default)]
    session_id: Option<String>,
}

async fn create_session(
    State(state): State<RemoteApiState>,
    Json(request): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<SessionView>), ApiError> {
    let preferred = request.session_id.map(AgentSessionId::new);
    let session_id = state.agent.create_session(preferred).await?;
    let timestamp = chrono::Utc::now().timestamp_millis();
    let session = SessionView {
        id: session_id.as_str().to_owned(),
        created_at_unix_ms: timestamp,
        updated_at_unix_ms: timestamp,
        run_ids: Vec::new(),
    };
    Ok((StatusCode::CREATED, Json(session)))
}

async fn get_session(
    State(state): State<RemoteApiState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionView>, ApiError> {
    session_views(&state)
        .await?
        .into_iter()
        .find(|session| session.id == session_id)
        .map(Json)
        .ok_or_else(|| ApiError::not_found("session_not_found", "session was not found"))
}

async fn list_agent_connectors(
    State(state): State<RemoteApiState>,
) -> Json<Vec<orchestral_core::agent_connector::AgentConnectorDescriptor>> {
    Json(state.agent_directory.connectors().await)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSessionsQuery {
    connector_id: String,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default = "default_agent_session_limit")]
    limit: u32,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    search: Option<String>,
}

const fn default_agent_session_limit() -> u32 {
    50
}

async fn list_agent_sessions(
    State(state): State<RemoteApiState>,
    Query(query): Query<AgentSessionsQuery>,
) -> Result<Json<AgentSessionPage>, ApiError> {
    let connector_id = AgentConnectorId::new(query.connector_id);
    Ok(Json(
        state
            .agent_directory
            .list_sessions(
                &connector_id,
                AgentSessionListQuery {
                    cursor: query.cursor,
                    limit: query.limit,
                    cwd: query.cwd,
                    search: query.search,
                },
            )
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateExternalAgentSessionRequest {
    connector_id: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    options: serde_json::Value,
    #[serde(default)]
    extensions: BTreeMap<String, serde_json::Value>,
}

async fn create_agent_session(
    State(state): State<RemoteApiState>,
    Json(request): Json<CreateExternalAgentSessionRequest>,
) -> Result<(StatusCode, Json<AgentSessionSummary>), ApiError> {
    let connector_id = AgentConnectorId::new(request.connector_id);
    let cwd = expand_host_home(request.cwd)?;
    let summary = state
        .agent_directory
        .create_session(
            &connector_id,
            CreateAgentSessionRequest {
                cwd,
                title: request.title,
                options: request.options,
                extensions: request.extensions,
            },
        )
        .await?;
    Ok((StatusCode::CREATED, Json(summary)))
}

fn expand_host_home(cwd: Option<String>) -> Result<Option<String>, ApiError> {
    let Some(cwd) = cwd else {
        return Ok(None);
    };
    let expanded = if cwd == "~" || cwd.starts_with("~/") {
        let home = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "host_home_unavailable",
                    "Host cannot expand ~ because its home directory is unavailable",
                )
            })?;
        if cwd == "~" {
            home
        } else {
            home.join(cwd.trim_start_matches("~/"))
        }
        .to_string_lossy()
        .into_owned()
    } else {
        cwd
    };
    Ok(Some(expanded))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSessionQuery {
    connector_id: String,
    session_id: String,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn get_agent_session(
    State(state): State<RemoteApiState>,
    Query(query): Query<AgentSessionQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let connector_id = AgentConnectorId::new(query.connector_id);
    let session_id = AgentSessionId::new(query.session_id);
    // Establish the Host-owned native subscription before reading the
    // snapshot. Events committed while the snapshot is in flight are then
    // replayable after `stream_cursor`, closing the read/subscribe race without
    // forcing every browser to perform a second full read.
    let stream_cursor = match state
        .session_coordinators
        .get(&connector_id, &session_id)
        .ensure_hub(state.agent_directory.clone(), &connector_id, &session_id)
        .await
    {
        Ok(hub) => Some(hub.cursor()),
        Err(error) => {
            tracing::debug!(
                connector_id = %connector_id.as_str(),
                session_id = %session_id.as_str(),
                %error,
                "Agent session has no live Hub; bounded polling remains available"
            );
            None
        }
    };
    let detail = state
        .agent_directory
        .read_session_page(
            &connector_id,
            &session_id,
            AgentSessionReadQuery {
                cursor: query.cursor,
                limit: query.limit.unwrap_or(100),
            },
        )
        .await?;
    let controlled_runs = controlled_session_runs(&state, &connector_id, &session_id).await?;
    let mut payload = serde_json::to_value(RemoteAgentSessionDetail {
        detail,
        controlled_runs,
        stream_cursor,
    })
    .map_err(|error| ApiError::internal("agent_session_encode_failed", error.to_string()))?;
    enrich_artifact_access(&mut payload, state.artifact_resolver.as_deref()).await;
    let body = serde_json::to_vec(&payload)
        .map_err(|error| ApiError::internal("agent_session_encode_failed", error.to_string()))?;
    let etag = format!(
        "\"{}\"",
        orchestral_core::agent_protocol::wire::Digest::sha256(&body)
    );
    let mut response = if request_etag_matches(&headers, &etag) {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
    };
    response.headers_mut().insert(
        header::ETAG,
        header::HeaderValue::from_str(&etag).expect("SHA-256 ETag is a valid header value"),
    );
    Ok(response)
}

#[derive(Debug, Serialize)]
struct RemoteAgentSessionDetail {
    #[serde(flatten)]
    detail: orchestral_core::agent_connector::AgentSessionDetail,
    /// Latest Host-controlled Run for this native session. The connector
    /// transcript alone cannot preserve this identity across a browser reload,
    /// yet commands must target the existing Run instead of starting a second
    /// controller for the same Codex thread.
    controlled_runs: Vec<ControlledRemoteRunView>,
    /// Canonical Host-side event cursor captured before the native snapshot
    /// read. Clients resume from this point and receive any concurrent changes
    /// from the shared session Hub.
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_cursor: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ControlledRemoteRunView {
    #[serde(flatten)]
    run: RemoteRunView,
    /// Stable wall-clock anchor used to merge this Host-side mirror into the
    /// bounded native transcript. The native page can already contain the
    /// response while the correlated user item lives on an older page.
    created_at_unix_ms: i64,
}

async fn controlled_session_runs(
    state: &RemoteApiState,
    connector_id: &AgentConnectorId,
    session_id: &AgentSessionId,
) -> Result<Vec<ControlledRemoteRunView>, ApiError> {
    let agent = state.agent_directory.agent_api(connector_id).await?;
    let mut catalog = agent
        .catalog_runs()
        .await?
        .into_iter()
        .filter(|entry| entry.session_id == *session_id)
        .collect::<Vec<_>>();
    catalog.sort_by_key(|entry| {
        std::cmp::Reverse((entry.updated_at_unix_ms, entry.created_at_unix_ms))
    });
    let Some(entry) = catalog.into_iter().next() else {
        return Ok(Vec::new());
    };
    // Native session history is authoritative and must remain readable after
    // a connector capability upgrade. A controlled Run registered against an
    // older descriptor is supplementary history; the current controller
    // cannot safely rehydrate it, so omit it instead of failing the session.
    if !agent.can_control_run(&entry.run_id).await? {
        return Ok(Vec::new());
    }
    let view = agent.inspect(&entry.run_id).await?;
    let created_at_unix_ms = entry.created_at_unix_ms;
    let remote = RemoteRunView {
        input: agent.initial_input(&entry.run_id).await?,
        view,
    };
    if !remote.view.state.is_terminal() {
        spawn_run_supervisor(
            state.clone(),
            agent.clone(),
            Some(connector_id.clone()),
            entry.run_id,
        );
    }
    Ok(vec![ControlledRemoteRunView {
        run: remote,
        created_at_unix_ms,
    }])
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSessionStreamQuery {
    connector_id: String,
    session_id: String,
    #[serde(default)]
    after: Option<u64>,
}

async fn agent_session_stream(
    State(state): State<RemoteApiState>,
    Extension(request_context): Extension<RequestLogContext>,
    Query(query): Query<AgentSessionStreamQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let connector_id = AgentConnectorId::new(query.connector_id);
    let session_id = AgentSessionId::new(query.session_id);
    let header_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    let initial_cursor = query.after.unwrap_or_default().max(header_cursor);
    let hub = state
        .session_coordinators
        .get(&connector_id, &session_id)
        .ensure_hub(state.agent_directory.clone(), &connector_id, &session_id)
        .await?;
    let subscription = hub.subscribe(initial_cursor);
    let replay = subscription.replay;
    let mut live = subscription.live;
    let lifecycle =
        SseLifecycleLog::open_agent_session(&request_context, &connector_id, &session_id);
    let artifact_resolver = state.artifact_resolver.clone();
    let stream = async_stream::stream! {
        let mut lifecycle = lifecycle;
        let mut cursor = initial_cursor;
        for change in replay {
            cursor = change.sequence;
            match agent_session_change_event(&change, artifact_resolver.as_deref()).await {
                Ok(event) => yield Ok(event),
                Err(error) => {
                    lifecycle.close_as("replay_event_encode_failed");
                    tracing::error!(
                        request_id = %lifecycle.request_id,
                        stream_id = %lifecycle.stream_id,
                        %error,
                        "could not encode replayed Agent session SSE event"
                    );
                    yield Ok(api_error_event(&ApiError::internal(
                        "session_stream_encode_failed",
                        error.to_string(),
                    )));
                    return;
                }
            }
        }
        loop {
            match live.recv().await {
                Ok(change) => {
                    if change.sequence != 0 && change.sequence <= cursor {
                        continue;
                    }
                    // Every native mutation is significant. In particular,
                    // item/completed is commonly followed immediately by
                    // turn/completed; retaining only the latter drops the
                    // actual message until the next full snapshot.
                    match agent_session_change_event(&change, artifact_resolver.as_deref()).await {
                        Ok(event) => {
                            cursor = change.sequence;
                            yield Ok(event)
                        },
                        Err(error) => {
                            lifecycle.close_as("event_encode_failed");
                            tracing::error!(
                                request_id = %lifecycle.request_id,
                                stream_id = %lifecycle.stream_id,
                                %error,
                                "could not encode Agent session SSE event"
                            );
                            yield Ok(api_error_event(&ApiError::internal(
                                "session_stream_encode_failed",
                                error.to_string(),
                            )));
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    lifecycle.lagged(skipped);
                    // The broadcast ring is only the fast path. Recover from
                    // the Hub replay before declaring a snapshot gap.
                    let replacement = hub.subscribe(cursor);
                    live = replacement.live;
                    for change in replacement.replay {
                        match agent_session_change_event(&change, artifact_resolver.as_deref()).await {
                            Ok(event) => {
                                cursor = change.sequence;
                                yield Ok(event)
                            },
                            Err(error) => {
                                lifecycle.close_as("gap_replay_encode_failed");
                                tracing::error!(
                                    request_id = %lifecycle.request_id,
                                    stream_id = %lifecycle.stream_id,
                                    %error,
                                    "could not encode Agent session gap replay"
                                );
                                yield Ok(api_error_event(&ApiError::internal(
                                    "session_stream_encode_failed",
                                    error.to_string(),
                                )));
                                return;
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    lifecycle.close_as("source_closed");
                    return;
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("keep-alive"),
    ))
}

async fn agent_session_change_event(
    change: &AgentSessionChange,
    artifact_resolver: Option<&dyn ArtifactResolver>,
) -> Result<Event, axum::Error> {
    let mut payload = serde_json::to_value(change).map_err(axum::Error::new)?;
    enrich_artifact_access(&mut payload, artifact_resolver).await;
    Event::default()
        .event("session_changed")
        .id(change.sequence.to_string())
        .json_data(payload)
}

/// Adds non-durable, storage-resolved access data to API views while keeping
/// the Agent Protocol's permanent Artifact identity free of expiring URLs.
/// The same projection is used by bounded snapshots and incremental SSE
/// events, so every client renders the same object-store address.
async fn enrich_artifact_access(
    payload: &mut serde_json::Value,
    artifact_resolver: Option<&dyn ArtifactResolver>,
) {
    let Some(artifact_resolver) = artifact_resolver else {
        return;
    };
    let mut artifacts = BTreeMap::<String, ArtifactRefWithDigest>::new();
    collect_artifact_references(payload, &mut artifacts);
    let mut access = BTreeMap::<String, serde_json::Value>::new();
    for (reference, artifact) in artifacts {
        match artifact_resolver.resolve(&artifact).await {
            Ok(resolved) => {
                access.insert(
                    reference,
                    serde_json::json!({
                        "uri": resolved.uri,
                        "file_name": resolved.file_name,
                        "media_type": resolved.media_type,
                        "byte_size": resolved.byte_size,
                        "expires_at": resolved.expires_at,
                    }),
                );
            }
            Err(error) => {
                tracing::warn!(
                    artifact_ref = %artifact.artifact_ref,
                    %error,
                    "could not resolve Artifact access for remote session view"
                );
            }
        }
    }
    inject_artifact_access(payload, &access);
}

fn collect_artifact_references(
    value: &serde_json::Value,
    artifacts: &mut BTreeMap<String, ArtifactRefWithDigest>,
) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_artifact_references(value, artifacts);
            }
        }
        serde_json::Value::Object(object) => {
            if object
                .get("body")
                .and_then(|body| body.get("kind"))
                .and_then(serde_json::Value::as_str)
                == Some("artifact")
            {
                if let Some(artifact) = object
                    .get("body")
                    .and_then(|body| body.get("value"))
                    .cloned()
                    .and_then(|value| serde_json::from_value::<ArtifactRefWithDigest>(value).ok())
                    .filter(|artifact| artifact.validate_integrity().is_ok())
                {
                    artifacts.insert(artifact.artifact_ref.to_string(), artifact);
                }
            }
            for value in object.values() {
                collect_artifact_references(value, artifacts);
            }
        }
        _ => {}
    }
}

fn inject_artifact_access(
    value: &mut serde_json::Value,
    access: &BTreeMap<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                inject_artifact_access(value, access);
            }
        }
        serde_json::Value::Object(object) => {
            let reference = object
                .get("body")
                .and_then(|body| body.get("value"))
                .and_then(|value| value.get("artifact_ref"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            if let Some(resolved) = reference.and_then(|reference| access.get(&reference)) {
                object.insert("access".to_owned(), resolved.clone());
            }
            for value in object.values_mut() {
                inject_artifact_access(value, access);
            }
        }
        _ => {}
    }
}

fn request_etag_matches(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|values| {
            values
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == "*" || candidate == etag)
        })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvokeExternalAgentSessionActionRequest {
    connector_id: String,
    session_id: String,
    action_id: String,
    #[serde(default)]
    arguments: serde_json::Value,
    #[serde(default)]
    run_id: Option<String>,
}

async fn invoke_agent_session_action(
    State(state): State<RemoteApiState>,
    Json(request): Json<InvokeExternalAgentSessionActionRequest>,
) -> Result<Json<AgentSessionActionOutcome>, ApiError> {
    Ok(Json(
        state
            .agent_directory
            .invoke_action(
                &AgentConnectorId::new(request.connector_id),
                InvokeAgentSessionActionRequest {
                    session_id: AgentSessionId::new(request.session_id),
                    action_id: AgentSessionActionId::new(request.action_id),
                    arguments: request.arguments,
                    run_id: request.run_id.map(RunId::new),
                },
            )
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRunRequest {
    run_id: String,
    input: String,
    #[serde(default)]
    attachments: Vec<RemoteArtifactInput>,
}

#[derive(Debug, Serialize)]
struct StartRunResponse {
    run_id: RunId,
    view: RemoteRunView,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartAgentRunRequest {
    connector_id: String,
    session_id: String,
    run_id: String,
    input: String,
    #[serde(default)]
    attachments: Vec<RemoteArtifactInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteArtifactInput {
    artifact_ref: String,
    digest: String,
    file_name: String,
    media_type: String,
    byte_size: u64,
}

#[derive(Debug, Serialize)]
struct StartAgentRunResponse {
    connector_id: AgentConnectorId,
    run_id: RunId,
    operation: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_id: Option<CommandId>,
    view: RemoteRunView,
}

async fn start_agent_run(
    State(state): State<RemoteApiState>,
    Json(request): Json<StartAgentRunRequest>,
) -> Result<(StatusCode, Json<StartAgentRunResponse>), ApiError> {
    let input = message_content(&state, &request.input, &request.attachments).await?;
    let connector_id = AgentConnectorId::new(request.connector_id);
    let session_id = AgentSessionId::new(request.session_id);
    let run_id = RunId::new(request.run_id);
    let coordinator = state.session_coordinators.get(&connector_id, &session_id);
    let _operation_guard = coordinator.operation().lock().await;
    let agent = state.agent_directory.agent_api(&connector_id).await?;

    // A retry of the same browser operation is a pure read. This also keeps a
    // response lost after start from being reinterpreted as a steer on retry.
    if agent.has_run(&run_id).await? {
        if agent.initial_input(&run_id).await? != input {
            return Err(ApiError::conflict(
                "run_id_conflict",
                "run_id was already used with different input",
            ));
        }
        let view = RemoteRunView {
            view: agent.inspect(&run_id).await?,
            input: agent.initial_input(&run_id).await?,
        };
        spawn_run_supervisor(
            state.clone(),
            agent,
            Some(connector_id.clone()),
            run_id.clone(),
        );
        log_agent_input_accepted(&connector_id, &session_id, &run_id, "replayed", None);
        return Ok((
            StatusCode::OK,
            Json(StartAgentRunResponse {
                connector_id,
                run_id,
                operation: "replayed",
                command_id: None,
                view,
            }),
        ));
    }

    if let Some(entry) = latest_session_run(&agent, &session_id).await? {
        let mut current = agent.inspect(&entry.run_id).await?;
        if current.state.status()
            == orchestral_core::agent_protocol::reference::AgentRunStatus::Unknown
        {
            current = agent.recover(&entry.run_id).await?;
        }
        if !current.state.is_terminal() {
            let command_id = CommandId::new(format!("agent-submit-{}", run_id.as_str()));
            let command = AgentCommandEnvelope::new(
                command_id.clone(),
                entry.run_id.clone(),
                None,
                AgentCommand::Steer {
                    content: input.clone(),
                },
            )?;
            let ack = command_run(&agent, &entry.run_id, command).await?;
            if !matches!(
                ack.state,
                CommandAckState::Accepted { .. } | CommandAckState::Applied { .. }
            ) {
                return Err(ApiError::conflict(
                    "agent_session_command_rejected",
                    "the active Agent Run did not accept this session message",
                ));
            }
            let view = RemoteRunView {
                view: agent.inspect(&entry.run_id).await?,
                input: agent.initial_input(&entry.run_id).await?,
            };
            spawn_run_supervisor(
                state.clone(),
                agent,
                Some(connector_id.clone()),
                entry.run_id.clone(),
            );
            log_agent_input_accepted(
                &connector_id,
                &session_id,
                &entry.run_id,
                "steered",
                Some(&command_id),
            );
            return Ok((
                StatusCode::OK,
                Json(StartAgentRunResponse {
                    connector_id,
                    run_id: entry.run_id,
                    operation: "steered",
                    command_id: Some(command_id),
                    view,
                }),
            ));
        }
    }

    let handle = state
        .agent_directory
        .start_content(&connector_id, &session_id, Some(run_id.clone()), input)
        .await?;
    spawn_run_supervisor(
        state.clone(),
        agent.clone(),
        Some(connector_id.clone()),
        run_id.clone(),
    );
    let view = RemoteRunView {
        view: handle.inspect().await?,
        input: agent.initial_input(&run_id).await?,
    };
    log_agent_input_accepted(&connector_id, &session_id, &run_id, "started", None);
    Ok((
        StatusCode::CREATED,
        Json(StartAgentRunResponse {
            connector_id,
            run_id,
            operation: "started",
            command_id: None,
            view,
        }),
    ))
}

fn log_agent_input_accepted(
    connector_id: &AgentConnectorId,
    session_id: &AgentSessionId,
    run_id: &RunId,
    operation: &'static str,
    command_id: Option<&CommandId>,
) {
    tracing::info!(
        connector_id = %connector_id.as_str(),
        session_id = %session_id.as_str(),
        run_id = %run_id.as_str(),
        operation,
        command_id = command_id.map(CommandId::as_str).unwrap_or("-"),
        "Agent session input accepted"
    );
}

async fn latest_session_run(
    agent: &AgentApi,
    session_id: &AgentSessionId,
) -> Result<Option<orchestral_core::agent_protocol::spi::AgentRunCatalogEntry>, ApiError> {
    let mut catalog = agent
        .catalog_runs()
        .await?
        .into_iter()
        .filter(|entry| entry.session_id == *session_id)
        .collect::<Vec<_>>();
    catalog.sort_by_key(|entry| {
        std::cmp::Reverse((entry.updated_at_unix_ms, entry.created_at_unix_ms))
    });
    let Some(latest) = catalog.into_iter().next() else {
        return Ok(None);
    };
    // A connector upgrade may leave durable journals registered against its
    // previous descriptor. They remain history, but must not be inspected,
    // recovered, or steered through the new controller. Treat the native
    // session as having no current Host Run so a fresh compatible Run can
    // attach to it.
    if !agent.can_control_run(&latest.run_id).await? {
        return Ok(None);
    }
    Ok(Some(latest))
}

async fn message_content(
    state: &RemoteApiState,
    text: &str,
    attachments: &[RemoteArtifactInput],
) -> Result<Vec<Content>, ApiError> {
    if text.trim().is_empty() && attachments.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_agent_input",
            "message text or at least one attachment is required",
        ));
    }
    if attachments.len() > 10 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "too_many_attachments",
            "a message may contain at most 10 attachments",
        ));
    }
    let resolver = if attachments.is_empty() {
        None
    } else {
        Some(state.artifact_resolver.as_ref().ok_or_else(|| {
            ApiError::service_unavailable(
                "artifact_resolver_unavailable",
                "Artifact storage is not configured on this Host",
            )
        })?)
    };

    let mut total_bytes = 0_u64;
    let mut validated = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        validate_remote_artifact(attachment)?;
        total_bytes = total_bytes
            .checked_add(attachment.byte_size)
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "attachments_too_large",
                    "attachment byte total overflowed",
                )
            })?;
        if total_bytes > 10 * 64 * 1024 * 1024 {
            return Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "attachments_too_large",
                "attachment total exceeds 640 MiB",
            ));
        }
        let artifact = ArtifactRefWithDigest {
            artifact_ref: ArtifactRef::new(&attachment.artifact_ref),
            digest: Digest::new(&attachment.digest),
        };
        let resolved = resolver
            .expect("non-empty attachments require a resolver")
            .resolve(&artifact)
            .await
            .map_err(artifact_resolve_error)?;
        if resolved.media_type != attachment.media_type
            || resolved.byte_size != attachment.byte_size
            || resolved.artifact != artifact
        {
            return Err(ApiError::conflict(
                "artifact_metadata_conflict",
                "Artifact metadata changed after upload",
            ));
        }
        validated.push((attachment, artifact));
    }

    let mut description = if text.trim().is_empty() {
        "请查看并处理随消息附上的文件。".to_owned()
    } else {
        text.to_owned()
    };
    if !validated.is_empty() {
        description.push_str("\n\n附件（内容已由 Host 按 SHA-256 校验）：");
        for (index, (attachment, _)) in validated.iter().enumerate() {
            description.push_str(&format!(
                "\n{}. {}（{}，{} bytes，sha256 {}）",
                index + 1,
                attachment.file_name,
                attachment.media_type,
                attachment.byte_size,
                attachment.digest
            ));
        }
    }
    let mut content = vec![Content::text(description)];
    content.extend(validated.into_iter().map(|(attachment, artifact)| Content {
        media_type: attachment.media_type.clone(),
        schema_id: None,
        body: ContentBody::Artifact(artifact),
    }));
    Ok(content)
}

fn validate_remote_artifact(attachment: &RemoteArtifactInput) -> Result<(), ApiError> {
    if attachment.artifact_ref != attachment.digest
        || attachment.digest.len() != 64
        || !attachment
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_artifact_identity",
            "Artifact reference must equal its SHA-256 digest",
        ));
    }
    if attachment.file_name.trim().is_empty()
        || attachment.file_name.len() > 255
        || attachment.file_name.chars().any(char::is_control)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_artifact_file_name",
            "Artifact file name is invalid",
        ));
    }
    if attachment.media_type.trim().is_empty()
        || attachment.media_type.len() > 160
        || attachment.media_type.chars().any(char::is_control)
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_artifact_media_type",
            "Artifact media type is invalid",
        ));
    }
    if attachment.byte_size == 0 || attachment.byte_size > 64 * 1024 * 1024 {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "artifact_too_large",
            "Artifact must be between 1 byte and 64 MiB",
        ));
    }
    Ok(())
}

fn artifact_resolve_error(error: orchestral_core::io::ArtifactResolveError) -> ApiError {
    use orchestral_core::io::ArtifactResolveError;
    match error {
        ArtifactResolveError::Invalid(message) => {
            ApiError::new(StatusCode::BAD_REQUEST, "invalid_artifact", message)
        }
        ArtifactResolveError::NotFound(message) => {
            ApiError::not_found("artifact_not_found", message)
        }
        ArtifactResolveError::Integrity(message) => {
            ApiError::conflict("artifact_integrity_failed", message)
        }
        ArtifactResolveError::Unavailable(message) | ArtifactResolveError::Internal(message) => {
            tracing::warn!(%message, "Artifact resolver failed");
            ApiError::service_unavailable(
                "artifact_resolver_unavailable",
                "Artifact storage is temporarily unavailable",
            )
        }
    }
}

#[derive(Debug, Serialize)]
struct RemoteRunView {
    #[serde(flatten)]
    view: AgentRunView,
    /// Immutable initial Run input. This is read from the controller-owned
    /// RunSpec rather than copied into the mobile session registry.
    input: Vec<Content>,
}

async fn start_run(
    State(state): State<RemoteApiState>,
    Path(session_id): Path<String>,
    Json(request): Json<StartRunRequest>,
) -> Result<(StatusCode, Json<StartRunResponse>), ApiError> {
    if !request.attachments.is_empty() {
        return Err(ApiError::new(
            StatusCode::NOT_IMPLEMENTED,
            "agent_artifacts_unsupported",
            "the built-in Agent does not declare Artifact input support",
        ));
    }
    let session_id = AgentSessionId::new(session_id);
    state.agent.create_session(Some(session_id.clone())).await?;
    let run_id = RunId::new(request.run_id);
    let handle = state
        .agent
        .start_text(&session_id, Some(run_id.clone()), request.input)
        .await?;
    spawn_remembered_approval_driver(state.clone(), run_id.clone());
    let view = RemoteRunView {
        view: handle.inspect().await?,
        input: state.agent.initial_input(&run_id).await?,
    };
    Ok((StatusCode::CREATED, Json(StartRunResponse { run_id, view })))
}

pub(super) fn spawn_remembered_approval_driver(state: RemoteApiState, run_id: RunId) {
    spawn_run_supervisor(state.clone(), state.agent.clone(), None, run_id);
}

pub(super) fn spawn_run_supervisor(
    state: RemoteApiState,
    agent: AgentApi,
    connector_id: Option<AgentConnectorId>,
    run_id: RunId,
) {
    let key = RunSupervisorRegistry::key(connector_id.as_ref(), &run_id);
    if !state.run_supervisors.begin(&key) {
        return;
    }
    let registry = state.run_supervisors.clone();
    tokio::spawn(async move {
        supervise_run(state, agent, connector_id, run_id).await;
        registry.finish(&key);
    });
}

async fn supervise_run(
    state: RemoteApiState,
    agent: AgentApi,
    connector_id: Option<AgentConnectorId>,
    run_id: RunId,
) {
    let mut failures = 0_u32;
    loop {
        // Subscribe before inspecting so a transition committed between the
        // two operations remains observable by this supervisor.
        let mut live = match agent.subscribe(&run_id).await {
            Ok(live) => live,
            Err(error) => {
                failures = failures.saturating_add(1);
                tracing::warn!(
                    connector_id = connector_id.as_ref().map(AgentConnectorId::as_str),
                    run_id = %run_id.as_str(),
                    %error,
                    "could not subscribe to supervised Agent Run"
                );
                tokio::time::sleep(run_supervisor_backoff(failures)).await;
                continue;
            }
        };
        let view = match agent.inspect(&run_id).await {
            Ok(view) => view,
            Err(error) => {
                failures = failures.saturating_add(1);
                tracing::warn!(
                    connector_id = connector_id.as_ref().map(AgentConnectorId::as_str),
                    run_id = %run_id.as_str(),
                    %error,
                    "could not inspect supervised Agent Run"
                );
                tokio::time::sleep(run_supervisor_backoff(failures)).await;
                continue;
            }
        };
        if view.state.is_terminal() {
            return;
        }
        if view.state.status()
            == orchestral_core::agent_protocol::reference::AgentRunStatus::Unknown
        {
            match agent.recover(&run_id).await {
                Ok(_) => {
                    // Recovery acknowledgement is only the start of restored
                    // observation. A Provider stream that fails immediately
                    // can put the Run back into Unknown before the next loop;
                    // retaining exponential backoff prevents an unbounded
                    // continuity_lost/restored journal storm.
                    failures = failures.saturating_add(1);
                    tracing::info!(
                        connector_id = connector_id.as_ref().map(AgentConnectorId::as_str),
                        run_id = %run_id.as_str(),
                        "restored Agent Run continuity"
                    );
                    tokio::time::sleep(run_supervisor_backoff(failures)).await;
                }
                Err(error) => {
                    if !is_retryable_agent_error(&error) {
                        tracing::info!(
                            connector_id = connector_id.as_ref().map(AgentConnectorId::as_str),
                            run_id = %run_id.as_str(),
                            %error,
                            "Agent Run requires manual recovery; stopped automatic retries"
                        );
                        return;
                    }
                    failures = failures.saturating_add(1);
                    tracing::warn!(
                        connector_id = connector_id.as_ref().map(AgentConnectorId::as_str),
                        run_id = %run_id.as_str(),
                        %error,
                        retry_after_ms = run_supervisor_backoff(failures).as_millis(),
                        "Agent Run recovery attempt failed"
                    );
                    tokio::time::sleep(run_supervisor_backoff(failures)).await;
                }
            }
            continue;
        }

        failures = 0;
        apply_remembered_approvals(&state, &agent, &run_id, &view.pending_requests).await;
        match tokio::time::timeout(RUN_SUPERVISOR_POLL_INTERVAL, live.recv()).await {
            Ok(Ok(_)) | Ok(Err(broadcast::error::RecvError::Lagged(_))) | Err(_) => {}
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                failures = failures.saturating_add(1);
                tokio::time::sleep(run_supervisor_backoff(failures)).await;
            }
        }
    }
}

/// Distinguishes transient supervision failures from durable contract or
/// recovery-boundary rejections. Retrying a non-retryable protocol error can
/// never change the outcome and otherwise produces an endless warning loop.
pub(super) fn is_retryable_agent_error(error: &AgentSdkError) -> bool {
    match error {
        AgentSdkError::InvalidInput(_) => false,
        AgentSdkError::Protocol(error)
        | AgentSdkError::Control(AgentControlError::Protocol(error)) => error.retryable,
        AgentSdkError::Control(AgentControlError::Start(AgentStartError::Rejected(rejection))) => {
            rejection.retryable
        }
        AgentSdkError::Control(AgentControlError::Start(AgentStartError::OutcomeUnknown(_)))
        | AgentSdkError::Control(AgentControlError::Journal(_))
        | AgentSdkError::ControlStreamClosed(_) => true,
        AgentSdkError::Control(
            AgentControlError::RunNotFound(_)
            | AgentControlError::ContinuityUnknown(_)
            | AgentControlError::RecoveryMismatch(_),
        ) => false,
        _ => true,
    }
}

async fn apply_remembered_approvals(
    state: &RemoteApiState,
    agent: &AgentApi,
    run_id: &RunId,
    requests: &[PendingRequest],
) {
    for request in requests {
        if !matches!(request.payload, PendingRequestPayload::Approval { .. }) {
            continue;
        }
        let Ok(Some(grant_ref)) = state
            .approvals
            .approve_if_remembered(&request.request_id, approval_expiry_ms())
        else {
            continue;
        };
        let Ok(command) = AgentCommandEnvelope::new(
            CommandId::new(format!(
                "host-remembered-approval-{}",
                request.request_id.as_str()
            )),
            run_id.clone(),
            Some(request.request_id.clone()),
            AgentCommand::ResolveRequest {
                response: RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: Some(grant_ref),
                },
            },
        ) else {
            continue;
        };
        if let Err(error) = agent.command(command).await {
            tracing::warn!(
                run_id = %run_id.as_str(),
                request_id = %request.request_id.as_str(),
                %error,
                "could not apply remembered approval"
            );
        }
    }
}

fn run_supervisor_backoff(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(8);
    RUN_SUPERVISOR_INITIAL_BACKOFF
        .saturating_mul(2_u32.saturating_pow(exponent))
        .min(RUN_SUPERVISOR_MAX_BACKOFF)
}

async fn inspect_run(
    State(state): State<RemoteApiState>,
    Path(run_id): Path<String>,
    Query(query): Query<RunTargetQuery>,
) -> Result<Json<RemoteRunView>, ApiError> {
    let (agent, run_id) = require_run(&state, query.connector_id.as_deref(), run_id).await?;
    Ok(Json(RemoteRunView {
        view: agent.inspect(&run_id).await?,
        input: agent.initial_input(&run_id).await?,
    }))
}

async fn recover_run(
    State(state): State<RemoteApiState>,
    Path(run_id): Path<String>,
    Query(query): Query<RunTargetQuery>,
) -> Result<Json<RemoteRunView>, ApiError> {
    let (agent, run_id) = require_run(&state, query.connector_id.as_deref(), run_id).await?;
    let current = agent.inspect(&run_id).await?;
    let view = if current.state.status()
        == orchestral_core::agent_protocol::reference::AgentRunStatus::Unknown
    {
        agent.recover(&run_id).await?
    } else {
        current
    };
    Ok(Json(RemoteRunView {
        view,
        input: agent.initial_input(&run_id).await?,
    }))
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RunTargetQuery {
    #[serde(default)]
    connector_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct EventsQuery {
    #[serde(default)]
    after: u64,
    #[serde(default)]
    connector_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct EventsResponse {
    after: u64,
    next: u64,
    records: Vec<orchestral_core::agent_protocol::wire::AgentJournalRecord>,
}

async fn run_events(
    State(state): State<RemoteApiState>,
    Path(run_id): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<EventsResponse>, ApiError> {
    let (agent, run_id) = require_run(&state, query.connector_id.as_deref(), run_id).await?;
    let records = agent.events(&run_id, query.after).await?;
    let next = records
        .last()
        .map_or(query.after, |record| record.event.run_seq);
    Ok(Json(EventsResponse {
        after: query.after,
        next,
        records,
    }))
}

async fn run_stream(
    State(state): State<RemoteApiState>,
    Extension(request_context): Extension<RequestLogContext>,
    Path(run_id): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let connector_id = query.connector_id.as_deref().map(AgentConnectorId::new);
    let (agent, run_id) = require_run(
        &state,
        connector_id.as_ref().map(AgentConnectorId::as_str),
        run_id,
    )
    .await?;
    let header_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    let initial_cursor = query.after.max(header_cursor);
    // Subscribe before replay. Any event committed between these operations is
    // either present in replay or remains queued; sequence filtering removes
    // the harmless overlap.
    let mut live = agent.subscribe(&run_id).await?;
    let lifecycle = SseLifecycleLog::open_run(&request_context, connector_id.as_ref(), &run_id);
    let stream = async_stream::stream! {
        let mut lifecycle = lifecycle;
        let mut cursor = initial_cursor;
        match replay_events(&agent, &run_id, &mut cursor).await {
            Ok(events) => {
                for event in events {
                    yield Ok(event);
                }
            }
            Err(error) => {
                lifecycle.close_as("initial_replay_failed");
                tracing::error!(
                    request_id = %lifecycle.request_id,
                    stream_id = %lifecycle.stream_id,
                    status = error.status.as_u16(),
                    error_code = %error.body.code,
                    "could not replay initial Run SSE events"
                );
                yield Ok(api_error_event(&error));
                return;
            }
        }

        loop {
            match live.recv().await {
                Ok(AgentControlEvent::Durable(record)) => {
                    if record.event.run_seq <= cursor {
                        continue;
                    }
                    cursor = record.event.run_seq;
                    yield Ok(durable_event(record.as_ref()));
                }
                Ok(AgentControlEvent::Telemetry(telemetry)) => {
                    match Event::default().event("telemetry").json_data(&telemetry) {
                        Ok(event) => yield Ok(event),
                        Err(error) => {
                            lifecycle.close_as("telemetry_encode_failed");
                            tracing::error!(
                                request_id = %lifecycle.request_id,
                                stream_id = %lifecycle.stream_id,
                                %error,
                                "could not encode Run telemetry SSE event"
                            );
                            yield Ok(api_error_event(&ApiError::internal(
                                "stream_encode_failed",
                                error.to_string(),
                            )));
                            return;
                        }
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    lifecycle.lagged(skipped);
                    match replay_events(&agent, &run_id, &mut cursor).await {
                        Ok(events) => {
                            for event in events {
                                yield Ok(event);
                            }
                        }
                        Err(error) => {
                            lifecycle.close_as("lag_replay_failed");
                            tracing::error!(
                                request_id = %lifecycle.request_id,
                                stream_id = %lifecycle.stream_id,
                                status = error.status.as_u16(),
                                error_code = %error.body.code,
                                "could not replay Run SSE events after subscriber lag"
                            );
                            yield Ok(api_error_event(&error));
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    match replay_events(&agent, &run_id, &mut cursor).await {
                        Ok(events) => {
                            for event in events {
                                yield Ok(event);
                            }
                            lifecycle.close_as("source_closed");
                        }
                        Err(error) => {
                            lifecycle.close_as("source_closed_replay_failed");
                            tracing::error!(
                                request_id = %lifecycle.request_id,
                                stream_id = %lifecycle.stream_id,
                                status = error.status.as_u16(),
                                error_code = %error.body.code,
                                "could not replay final Run SSE events after source closed"
                            );
                        }
                    }
                    return;
                }
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("keep-alive"),
    ))
}

async fn replay_events(
    agent: &AgentApi,
    run_id: &RunId,
    cursor: &mut u64,
) -> Result<Vec<Event>, ApiError> {
    let records = agent.events(run_id, *cursor).await?;
    let mut events = Vec::with_capacity(records.len());
    for record in records {
        if record.event.run_seq <= *cursor {
            continue;
        }
        *cursor = record.event.run_seq;
        events.push(durable_event(&record));
    }
    Ok(events)
}

fn durable_event(record: &orchestral_core::agent_protocol::wire::AgentJournalRecord) -> Event {
    match Event::default()
        .event("durable")
        .id(record.event.run_seq.to_string())
        .json_data(record)
    {
        Ok(event) => event,
        Err(error) => api_error_event(&ApiError::internal(
            "stream_encode_failed",
            error.to_string(),
        )),
    }
}

fn api_error_event(error: &ApiError) -> Event {
    Event::default().event("error").data(
        serde_json::to_string(&error.body)
            .unwrap_or_else(|_| r#"{"code":"stream_failed","message":"stream failed"}"#.to_owned()),
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TextCommandRequest {
    command_id: String,
    text: String,
    #[serde(default)]
    attachments: Vec<RemoteArtifactInput>,
}

async fn steer_run(
    State(state): State<RemoteApiState>,
    Path(run_id): Path<String>,
    Query(query): Query<RunTargetQuery>,
    Json(request): Json<TextCommandRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let content = message_content(&state, &request.text, &request.attachments).await?;
    let connector_id = query.connector_id.as_deref().map(AgentConnectorId::new);
    let (agent, run_id) =
        require_commandable_run(&state, query.connector_id.as_deref(), run_id).await?;
    let command = AgentCommandEnvelope::new(
        CommandId::new(request.command_id),
        run_id.clone(),
        None,
        AgentCommand::Steer { content },
    )?;
    Ok(Json(
        command_run_for_session(&state, connector_id.as_ref(), &agent, &run_id, command).await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelRequest {
    command_id: String,
    reason: String,
}

async fn cancel_run(
    State(state): State<RemoteApiState>,
    Path(run_id): Path<String>,
    Query(query): Query<RunTargetQuery>,
    Json(request): Json<CancelRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let connector_id = query.connector_id.as_deref().map(AgentConnectorId::new);
    let (agent, run_id) =
        require_commandable_run(&state, query.connector_id.as_deref(), run_id).await?;
    let command = AgentCommandEnvelope::new(
        CommandId::new(request.command_id),
        run_id.clone(),
        None,
        AgentCommand::Cancel {
            reason: request.reason,
        },
    )?;
    Ok(Json(
        command_run_for_session(&state, connector_id.as_ref(), &agent, &run_id, command).await?,
    ))
}

async fn resolve_input(
    State(state): State<RemoteApiState>,
    Path((run_id, request_id)): Path<(String, String)>,
    Query(query): Query<RunTargetQuery>,
    Json(request): Json<TextCommandRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let content = message_content(&state, &request.text, &request.attachments).await?;
    let connector_id = query.connector_id.as_deref().map(AgentConnectorId::new);
    let (agent, run_id) =
        require_commandable_run(&state, query.connector_id.as_deref(), run_id).await?;
    let request_id = RequestId::new(request_id);
    let pending = require_pending_for(&agent, &run_id, &request_id).await?;
    if !matches!(pending.payload, PendingRequestPayload::Input { .. }) {
        return Err(ApiError::conflict(
            "request_kind_mismatch",
            "pending request does not accept text input",
        ));
    }
    let command = AgentCommandEnvelope::new(
        CommandId::new(request.command_id),
        run_id.clone(),
        Some(request_id),
        AgentCommand::ResolveRequest {
            response: RequestResolution::Input { content },
        },
    )?;
    Ok(Json(
        command_run_for_session(&state, connector_id.as_ref(), &agent, &run_id, command).await?,
    ))
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ApprovalChoice {
    AllowOnce,
    AllowSession,
    Deny,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalRequest {
    command_id: String,
    decision: ApprovalChoice,
}

async fn resolve_approval(
    State(state): State<RemoteApiState>,
    Path((run_id, request_id)): Path<(String, String)>,
    Query(query): Query<RunTargetQuery>,
    Json(request): Json<ApprovalRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let connector_id = query.connector_id.as_deref().map(AgentConnectorId::new);
    let (agent, run_id) =
        require_commandable_run(&state, query.connector_id.as_deref(), run_id).await?;
    let request_id = RequestId::new(request_id);
    let pending = require_pending_for(&agent, &run_id, &request_id).await?;
    let PendingRequestPayload::Approval {
        session_approval_scope,
        ..
    } = pending.payload
    else {
        return Err(ApiError::conflict(
            "request_kind_mismatch",
            "pending request is not an approval",
        ));
    };
    let (decision, grant_ref) = match request.decision {
        ApprovalChoice::Deny => (ApprovalDecision::Deny, None),
        ApprovalChoice::AllowOnce => (
            ApprovalDecision::Allow,
            Some(state.approvals.approve(&request_id, approval_expiry_ms())?),
        ),
        ApprovalChoice::AllowSession => {
            if session_approval_scope.is_none() {
                return Err(ApiError::conflict(
                    "session_approval_unavailable",
                    "this operation cannot be approved for the session",
                ));
            }
            (
                ApprovalDecision::Allow,
                Some(
                    state
                        .approvals
                        .approve_for_session(&request_id, approval_expiry_ms())?,
                ),
            )
        }
    };
    let command = AgentCommandEnvelope::new(
        CommandId::new(request.command_id),
        run_id.clone(),
        Some(request_id),
        AgentCommand::ResolveRequest {
            response: RequestResolution::Approval {
                decision,
                grant_ref,
            },
        },
    )?;
    Ok(Json(
        command_run_for_session(&state, connector_id.as_ref(), &agent, &run_id, command).await?,
    ))
}

async fn require_run(
    state: &RemoteApiState,
    connector_id: Option<&str>,
    run_id: String,
) -> Result<(AgentApi, RunId), ApiError> {
    let agent = match connector_id {
        Some(connector_id) => {
            state
                .agent_directory
                .agent_api(&AgentConnectorId::new(connector_id))
                .await?
        }
        None => state.agent.clone(),
    };
    let run_id = RunId::new(run_id);
    if !agent.has_run(&run_id).await? {
        return Err(ApiError::not_found("run_not_found", "run was not found"));
    }
    spawn_run_supervisor(
        state.clone(),
        agent.clone(),
        connector_id.map(AgentConnectorId::new),
        run_id.clone(),
    );
    Ok((agent, run_id))
}

async fn require_commandable_run(
    state: &RemoteApiState,
    connector_id: Option<&str>,
    run_id: String,
) -> Result<(AgentApi, RunId), ApiError> {
    let (agent, run_id) = require_run(state, connector_id, run_id).await?;
    if agent.inspect(&run_id).await?.state.status()
        == orchestral_core::agent_protocol::reference::AgentRunStatus::Unknown
    {
        return Err(ApiError::service_unavailable(
            "run_recovery_pending",
            "Agent Run continuity is being recovered; retry this command shortly",
        ));
    }
    Ok((agent, run_id))
}

async fn command_run(
    agent: &AgentApi,
    run_id: &RunId,
    command: AgentCommandEnvelope,
) -> Result<CommandAck, ApiError> {
    let command_id = command.command_id.clone();
    let protocol_request_id = command.request_id.clone();
    let command_kind = match &command.payload {
        AgentCommand::Steer { .. } => "steer",
        AgentCommand::ResolveRequest { response } => match response {
            RequestResolution::Input { .. } => "resolve_input",
            RequestResolution::Approval { .. } => "resolve_approval",
            RequestResolution::ExternalResult { .. } => "resolve_external_result",
            _ => "resolve_unknown",
        },
        AgentCommand::Cancel { .. } => "cancel",
        _ => "unknown",
    };
    let result = agent.command(command).await;
    match result {
        Ok(ack) => {
            let ack_state = match &ack.state {
                CommandAckState::Accepted { .. } => "accepted",
                CommandAckState::Applied { .. } => "applied",
                CommandAckState::Rejected { .. } => "rejected",
                CommandAckState::Unsupported { .. } => "unsupported",
                _ => "unknown",
            };
            tracing::info!(
                run_id = %run_id.as_str(),
                command_id = %command_id.as_str(),
                protocol_request_id = protocol_request_id
                    .as_ref()
                    .map(RequestId::as_str)
                    .unwrap_or("-"),
                command_kind,
                ack_state,
                duplicate = ack.duplicate,
                "Agent command acknowledged"
            );
            Ok(ack)
        }
        Err(_error)
            if agent.inspect(run_id).await.is_ok_and(|view| {
                view.state.status()
                    == orchestral_core::agent_protocol::reference::AgentRunStatus::Unknown
            }) =>
        {
            tracing::warn!(
                run_id = %run_id.as_str(),
                command_id = %command_id.as_str(),
                protocol_request_id = protocol_request_id
                    .as_ref()
                    .map(RequestId::as_str)
                    .unwrap_or("-"),
                command_kind,
                "Agent command outcome is pending continuity recovery"
            );
            Err(ApiError::service_unavailable(
                "run_recovery_pending",
                "Agent Run continuity is being recovered; retry this command shortly",
            ))
        }
        Err(error) => {
            tracing::warn!(
                run_id = %run_id.as_str(),
                command_id = %command_id.as_str(),
                protocol_request_id = protocol_request_id
                    .as_ref()
                    .map(RequestId::as_str)
                    .unwrap_or("-"),
                command_kind,
                "Agent command failed"
            );
            Err(error.into())
        }
    }
}

async fn command_run_for_session(
    state: &RemoteApiState,
    connector_id: Option<&AgentConnectorId>,
    agent: &AgentApi,
    run_id: &RunId,
    command: AgentCommandEnvelope,
) -> Result<CommandAck, ApiError> {
    let Some(connector_id) = connector_id else {
        return command_run(agent, run_id, command).await;
    };
    let session_id = agent
        .catalog_runs()
        .await?
        .into_iter()
        .find(|entry| entry.run_id == *run_id)
        .map(|entry| entry.session_id)
        .ok_or_else(|| ApiError::not_found("run_not_found", "run was not found"))?;
    let coordinator = state.session_coordinators.get(connector_id, &session_id);
    let _operation_guard = coordinator.operation().lock().await;
    command_run(agent, run_id, command).await
}

/// Projects the remote conversation list directly from durable Run
/// registrations. The Agent journal is the only Session-to-Run source of
/// truth; empty Sessions remain process-local until their first Run starts.
async fn session_views(state: &RemoteApiState) -> Result<Vec<SessionView>, AgentSdkError> {
    let fallback = chrono::Utc::now().timestamp_millis();
    let mut catalog = state.agent.catalog_runs().await?;
    catalog.sort_by(|left, right| {
        left.created_at_unix_ms
            .cmp(&right.created_at_unix_ms)
            .then_with(|| left.run_id.cmp(&right.run_id))
    });

    let mut sessions = BTreeMap::<String, SessionView>::new();
    for run in catalog {
        let created = if run.created_at_unix_ms > 0 {
            run.created_at_unix_ms
        } else {
            fallback
        };
        let updated = if run.updated_at_unix_ms > 0 {
            run.updated_at_unix_ms
        } else {
            created
        };
        let session = sessions
            .entry(run.session_id.as_str().to_owned())
            .or_insert_with(|| SessionView {
                id: run.session_id.as_str().to_owned(),
                created_at_unix_ms: created,
                updated_at_unix_ms: updated,
                run_ids: Vec::new(),
            });
        session.created_at_unix_ms = session.created_at_unix_ms.min(created);
        session.updated_at_unix_ms = session.updated_at_unix_ms.max(updated);
        session.run_ids.push(run.run_id.as_str().to_owned());
    }
    let mut sessions = sessions.into_values().collect::<Vec<_>>();
    sessions.sort_by_key(|session| std::cmp::Reverse(session.updated_at_unix_ms));
    Ok(sessions)
}

async fn require_pending_for(
    agent: &AgentApi,
    run_id: &RunId,
    request_id: &RequestId,
) -> Result<PendingRequest, ApiError> {
    let view = agent.inspect(run_id).await?;
    view.pending_requests
        .into_iter()
        .find(|request| request.request_id == *request_id)
        .ok_or_else(|| {
            ApiError::conflict(
                "request_not_pending",
                "request is no longer pending for this run",
            )
        })
}

async fn authenticate(
    State(state): State<RemoteApiState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    if let Some(authenticator) = &state.gateway_authenticator {
        let assertion = request
            .headers()
            .get(authenticator.header_name())
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ApiError::unauthorized(
                    "gateway_authentication_required",
                    "a signed gateway identity is required",
                )
            })?;
        let principal = authenticator
            .authenticate(assertion)
            .await
            .map_err(|error| {
                ApiError::unauthorized("gateway_authentication_failed", error.to_string())
            })?;
        request
            .extensions_mut()
            .insert(RemotePrincipal::Gateway(principal));
        return Ok(next.run(request).await);
    }

    let token = bearer_token(request.headers()).ok_or_else(|| {
        ApiError::unauthorized(
            "authentication_required",
            "device authentication is required",
        )
    })?;
    let principal = state.registry.authenticate(token).await.map_err(|_| {
        ApiError::unauthorized(
            "authentication_failed",
            "device authentication is invalid or revoked",
        )
    })?;
    request
        .extensions_mut()
        .insert(RemotePrincipal::Device(principal));
    Ok(next.run(request).await)
}

async fn log_request(mut request: Request<axum::body::Body>, next: Next) -> Response {
    let context = RequestLogContext {
        request_id: uuid::Uuid::new_v4().to_string(),
    };
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("<unmatched>")
        .to_owned();
    let cf_ray = request
        .headers()
        .get("cf-ray")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_owned();
    request.extensions_mut().insert(context.clone());

    let started_at = Instant::now();
    let span = tracing::info_span!(
        "http_request",
        request_id = %context.request_id,
        method = %method,
        route = %route,
        cf_ray = %cf_ray,
    );
    let mut response = next.run(request).instrument(span.clone()).await;
    if let Ok(value) = HeaderValue::from_str(&context.request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }

    let status = response.status();
    let response_ready_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    let error_code = response
        .extensions()
        .get::<ApiErrorLogCode>()
        .map(|code| code.0.as_str())
        .unwrap_or("-");
    span.in_scope(|| {
        if status.is_server_error() {
            tracing::error!(
                status = status.as_u16(),
                response_ready_ms,
                error_code,
                "HTTP request completed"
            );
        } else if status.is_client_error() {
            tracing::warn!(
                status = status.as_u16(),
                response_ready_ms,
                error_code,
                "HTTP request completed"
            );
        } else {
            tracing::info!(
                status = status.as_u16(),
                response_ready_ms,
                "HTTP request completed"
            );
        }
    });
    response
}

async fn no_store(request: Request<axum::body::Body>, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store, private"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    response
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
}

fn approval_expiry_ms() -> i64 {
    chrono::Utc::now()
        .timestamp_millis()
        .saturating_add(APPROVAL_GRANT_TTL_MS)
}

#[derive(Debug, Clone, Serialize)]
struct ApiErrorBody {
    code: String,
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    body: ApiErrorBody,
}

impl ApiError {
    fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ApiErrorBody {
                code: code.into(),
                message: message.into(),
            },
        }
    }

    fn unauthorized(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, code, message)
    }

    fn not_found(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    fn conflict(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, code, message)
    }

    fn service_unavailable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, code, message)
    }

    fn internal(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = ApiErrorLogCode(self.body.code.clone());
        let mut response = (self.status, Json(self.body)).into_response();
        response.extensions_mut().insert(code);
        response
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self::internal("host_state_failed", error.to_string())
    }
}

impl From<AgentSdkError> for ApiError {
    fn from(error: AgentSdkError) -> Self {
        match &error {
            AgentSdkError::InvalidInput(_) => Self::new(
                StatusCode::BAD_REQUEST,
                "invalid_agent_input",
                error.to_string(),
            ),
            _ => Self::internal("agent_control_failed", error.to_string()),
        }
    }
}

impl From<AgentDirectoryError> for ApiError {
    fn from(error: AgentDirectoryError) -> Self {
        match &error {
            AgentDirectoryError::ConnectorNotFound(_) => {
                Self::not_found("agent_connector_not_found", error.to_string())
            }
            AgentDirectoryError::Connector(connector_error) => {
                use orchestral_core::agent_connector::AgentConnectorErrorCode;
                match connector_error.code {
                    AgentConnectorErrorCode::InvalidRequest => Self::new(
                        StatusCode::BAD_REQUEST,
                        "invalid_agent_connector_request",
                        error.to_string(),
                    ),
                    AgentConnectorErrorCode::NotFound => {
                        Self::not_found("agent_session_not_found", error.to_string())
                    }
                    AgentConnectorErrorCode::Busy | AgentConnectorErrorCode::LeaseConflict => {
                        Self::conflict("agent_connector_busy", error.to_string())
                    }
                    AgentConnectorErrorCode::Unsupported => Self::new(
                        StatusCode::NOT_IMPLEMENTED,
                        "agent_connector_unsupported",
                        error.to_string(),
                    ),
                    AgentConnectorErrorCode::Unavailable => Self::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "agent_connector_unavailable",
                        error.to_string(),
                    ),
                    AgentConnectorErrorCode::Protocol | AgentConnectorErrorCode::OutcomeUnknown => {
                        Self::internal("agent_connector_failed", error.to_string())
                    }
                    _ => Self::internal("agent_connector_failed", error.to_string()),
                }
            }
            AgentDirectoryError::Agent(AgentSdkError::Control(AgentControlError::Start(
                AgentStartError::Rejected(rejection),
            ))) if rejection
                .details
                .get("code")
                .and_then(serde_json::Value::as_str)
                == Some("live_control_unavailable") =>
            {
                Self::conflict("live_control_unavailable", rejection.message.clone())
            }
            AgentDirectoryError::Agent(AgentSdkError::Control(AgentControlError::Start(
                AgentStartError::Rejected(rejection),
            ))) => match rejection.code {
                AgentRejectionCode::SessionConflict | AgentRejectionCode::RunIdConflict => {
                    Self::conflict("agent_session_conflict", rejection.message.clone())
                }
                AgentRejectionCode::InvalidSpec => Self::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_agent_input",
                    rejection.message.clone(),
                ),
                AgentRejectionCode::UnsupportedProtocol
                | AgentRejectionCode::UnsupportedCapability
                | AgentRejectionCode::UnsupportedResource => Self::new(
                    StatusCode::NOT_IMPLEMENTED,
                    "agent_capability_unsupported",
                    rejection.message.clone(),
                ),
                AgentRejectionCode::ProviderUnavailable => Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "agent_provider_unavailable",
                    rejection.message.clone(),
                ),
                _ => Self::new(
                    StatusCode::BAD_REQUEST,
                    "agent_rejected",
                    rejection.message.clone(),
                ),
            },
            _ => Self::internal("agent_directory_failed", error.to_string()),
        }
    }
}

impl From<orchestral_core::agent_protocol::wire::AgentProtocolError> for ApiError {
    fn from(error: orchestral_core::agent_protocol::wire::AgentProtocolError) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_command",
            error.to_string(),
        )
    }
}

impl From<ApprovalBridgeError> for ApiError {
    fn from(error: ApprovalBridgeError) -> Self {
        match error {
            ApprovalBridgeError::RequestNotFound(_)
            | ApprovalBridgeError::SessionScopeUnavailable(_) => {
                Self::conflict("approval_unavailable", error.to_string())
            }
            _ => Self::internal("approval_bridge_failed", error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{HeaderName, Request};
    use futures_util::{stream, StreamExt};
    use http_body_util::BodyExt;
    use orchestral_agent_protocol_testkit::{
        ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory,
        SessionfulRecoverFactory, TestProbes,
    };
    use orchestral_core::agent_connector::{
        AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorHealth,
        AgentSessionActionDescriptor, AgentSessionActionExecution, AgentSessionActionOutcome,
        AgentSessionActionStatus, AgentSessionActivity, AgentSessionActivityId,
        AgentSessionActivityKind, AgentSessionActivityStatus, AgentSessionCapabilities,
        AgentSessionDetail, AgentSessionState, AgentSessionSummary, AgentSessionTurn,
        AgentSessionTurnId, AgentSessionTurnStatus, CreateAgentSessionRequest,
        InvokeAgentSessionActionRequest, SESSION_FORK_ACTION, SESSION_RENAME_ACTION,
        SESSION_REVIEW_ACTION,
    };
    use orchestral_core::agent_protocol::{
        spi::{
            AgentProvider, AgentRecovery, AgentRecoveryRequest, AgentStart, AgentStartError,
            InMemoryAgentJournalStore,
        },
        wire::{
            AgentAdmission, AgentCapabilities, AgentDescriptor, AgentDescriptorEnvelope,
            AgentEvent, AgentEventDraft, AgentExecutionRef, AgentId, AgentProtocolError,
            AgentProtocolErrorCode, AgentProviderId, AgentProviderStreamItem, AgentRunEnvelope,
            EffectMediation, PendingRequestKind, ProviderBindingRef, ProviderCommandDisposition,
            ProviderCommandOutcome,
        },
        AGENT_PROTOCOL_V1,
    };
    use orchestral_core::io::{ArtifactResolveError, ResolvedArtifact};
    use orchestral_core::tool_protocol::{
        ApprovalBinding, CapabilityRequest, EffectScope, ToolCallId, ToolId,
    };
    use orchestral_runtime::{AgentApprovalBridge, AgentController};
    use tokio::sync::broadcast;
    use tower::ServiceExt;

    #[test]
    fn supervision_retries_only_retryable_agent_failures() {
        let permanent =
            AgentSdkError::Control(AgentControlError::Protocol(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "manual recovery required",
            )));
        assert!(!is_retryable_agent_error(&permanent));

        let transient = AgentSdkError::Control(AgentControlError::Protocol(
            AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                "provider restarting",
            )
            .with_retryable(true),
        ));
        assert!(is_retryable_agent_error(&transient));

        let missing =
            AgentSdkError::Control(AgentControlError::RunNotFound(RunId::new("missing-run")));
        assert!(!is_retryable_agent_error(&missing));
    }

    #[test]
    fn session_creation_expands_host_home_shortcuts_without_touching_other_paths() {
        let home = std::env::var("HOME").expect("test Host has a home directory");
        assert_eq!(
            expand_host_home(Some("~/rust_ws/project".to_owned())).unwrap(),
            Some(
                PathBuf::from(home)
                    .join("rust_ws/project")
                    .to_string_lossy()
                    .into_owned()
            )
        );
        assert_eq!(
            expand_host_home(Some("/srv/project".to_owned())).unwrap(),
            Some("/srv/project".to_owned())
        );
    }

    use super::super::auth::GatewayAuthError;

    struct StaticGatewayAuthenticator {
        header_name: HeaderName,
    }

    struct StaticAgentConnector;

    struct DirectArtifactResolver;

    #[async_trait]
    impl ArtifactResolver for DirectArtifactResolver {
        async fn resolve(
            &self,
            artifact: &ArtifactRefWithDigest,
        ) -> Result<ResolvedArtifact, ArtifactResolveError> {
            Ok(ResolvedArtifact {
                artifact: artifact.clone(),
                uri: format!(
                    "https://orchestral-files.example/v1/blobs/{}?capability=signed",
                    artifact.artifact_ref
                ),
                file_name: Some("generated.png".to_owned()),
                media_type: "image/png".to_owned(),
                byte_size: 123,
                expires_at: None,
            })
        }
    }

    struct ObservableAgentConnector {
        subscriptions: Arc<AtomicUsize>,
        changes: broadcast::Sender<AgentSessionChange>,
    }

    struct DisconnectFirstProvider {
        inner: Arc<dyn AgentProvider>,
    }

    struct HoldingProvider {
        inner: Arc<dyn AgentProvider>,
    }

    struct UnrecoverableDisconnectProvider {
        inner: Arc<dyn AgentProvider>,
    }

    #[test]
    fn active_external_agent_writer_is_reported_as_a_conflict() {
        let rejection = orchestral_core::agent_protocol::wire::AgentRejection::new(
            AgentRejectionCode::SessionConflict,
            "thread already has an active writer",
        );
        let error = AgentDirectoryError::Agent(AgentSdkError::Control(AgentControlError::Start(
            AgentStartError::Rejected(rejection),
        )));

        let response = ApiError::from(error);
        assert_eq!(response.status, StatusCode::CONFLICT);
        assert_eq!(response.body.code, "agent_session_conflict");
    }

    #[test]
    fn realtime_only_agent_writer_conflict_has_a_stable_api_code() {
        let rejection = orchestral_core::agent_protocol::wire::AgentRejection::new(
            AgentRejectionCode::UnsupportedCapability,
            "live control unavailable",
        )
        .with_details(serde_json::json!({"code": "live_control_unavailable"}));
        let error = AgentDirectoryError::Agent(AgentSdkError::Control(AgentControlError::Start(
            AgentStartError::Rejected(rejection),
        )));

        let response = ApiError::from(error);
        assert_eq!(response.status, StatusCode::CONFLICT);
        assert_eq!(response.body.code, "live_control_unavailable");
    }

    #[tokio::test]
    async fn session_views_add_direct_storage_access_without_changing_artifact_identity() {
        let digest = "a".repeat(64);
        let mut payload = serde_json::json!({
            "turns": [{
                "activities": [{
                    "content": [{
                        "media_type": "image/png",
                        "schema_id": null,
                        "body": {
                            "kind": "artifact",
                            "value": {
                                "artifact_ref": digest,
                                "digest": "a".repeat(64)
                            }
                        }
                    }]
                }]
            }]
        });

        enrich_artifact_access(&mut payload, Some(&DirectArtifactResolver)).await;

        let content = payload.pointer("/turns/0/activities/0/content/0").unwrap();
        assert_eq!(
            content.pointer("/body/value/artifact_ref"),
            Some(&serde_json::Value::String("a".repeat(64)))
        );
        assert_eq!(
            content.pointer("/access/uri").and_then(serde_json::Value::as_str),
            Some(
                "https://orchestral-files.example/v1/blobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?capability=signed"
            )
        );
        assert_eq!(
            content.pointer("/access/byte_size"),
            Some(&serde_json::json!(123))
        );
    }

    #[tokio::test]
    async fn latest_session_run_ignores_a_journal_from_an_old_controller_contract() {
        let factory = ScriptedStatelessFactory::conformant().expect("fixture descriptor");
        let scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
        let journal = Arc::new(InMemoryAgentJournalStore::default());
        let previous = Arc::new(
            AgentController::with_journal_store(
                factory.create(scenario.clone(), TestProbes::default()),
                ProviderBindingRef::new("previous-binding"),
                journal.clone(),
            )
            .expect("previous controller binds"),
        );
        let execution = previous
            .start(scenario.start_request.run.clone())
            .await
            .expect("previous Run starts");
        previous
            .wait_for_terminal(&execution.run_id)
            .await
            .expect("previous Run completes");
        drop(previous);

        let upgraded = Arc::new(
            AgentController::with_journal_store(
                factory.create(scenario.clone(), TestProbes::default()),
                ProviderBindingRef::new("upgraded-binding"),
                journal,
            )
            .expect("upgraded controller binds"),
        );
        let agent = AgentApi::new(upgraded);

        assert!(
            latest_session_run(&agent, &scenario.start_request.run.spec.session_id)
                .await
                .expect("catalog remains readable")
                .is_none()
        );
    }

    #[async_trait]
    impl AgentProvider for DisconnectFirstProvider {
        fn describe(&self) -> AgentDescriptorEnvelope {
            self.inner.describe()
        }

        async fn start(
            &self,
            request: orchestral_core::agent_protocol::wire::AgentStartRequest,
        ) -> Result<AgentStart, AgentStartError> {
            let started = self.inner.start(request).await?;
            Ok(AgentStart {
                execution: started.execution,
                admission: started.admission,
                stream: started.stream.take(1).boxed(),
            })
        }

        async fn command(
            &self,
            execution: &AgentExecutionRef,
            command: AgentCommandEnvelope,
        ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
            self.inner.command(execution, command).await
        }

        async fn recover(
            &self,
            request: AgentRecoveryRequest,
        ) -> Result<AgentRecovery, AgentProtocolError> {
            self.inner.recover(request).await
        }
    }

    #[async_trait]
    impl AgentProvider for HoldingProvider {
        fn describe(&self) -> AgentDescriptorEnvelope {
            self.inner.describe()
        }

        async fn start(
            &self,
            request: orchestral_core::agent_protocol::wire::AgentStartRequest,
        ) -> Result<AgentStart, AgentStartError> {
            let started = self.inner.start(request).await?;
            Ok(AgentStart {
                execution: started.execution,
                admission: started.admission,
                stream: started.stream.take(1).chain(stream::pending()).boxed(),
            })
        }

        async fn command(
            &self,
            _execution: &AgentExecutionRef,
            command: AgentCommandEnvelope,
        ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
            Ok(ProviderCommandDisposition {
                command_id: command.command_id,
                run_id: command.run_id,
                outcome: ProviderCommandOutcome::Accepted,
                duplicate: false,
            })
        }

        async fn recover(
            &self,
            request: AgentRecoveryRequest,
        ) -> Result<AgentRecovery, AgentProtocolError> {
            self.inner.recover(request).await
        }
    }

    #[async_trait]
    impl AgentProvider for UnrecoverableDisconnectProvider {
        fn describe(&self) -> AgentDescriptorEnvelope {
            self.inner.describe()
        }

        async fn start(
            &self,
            request: orchestral_core::agent_protocol::wire::AgentStartRequest,
        ) -> Result<AgentStart, AgentStartError> {
            let started = self.inner.start(request).await?;
            Ok(AgentStart {
                execution: started.execution,
                admission: started.admission,
                stream: started.stream.take(1).boxed(),
            })
        }

        async fn command(
            &self,
            execution: &AgentExecutionRef,
            command: AgentCommandEnvelope,
        ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
            self.inner.command(execution, command).await
        }

        async fn recover(
            &self,
            _request: AgentRecoveryRequest,
        ) -> Result<AgentRecovery, AgentProtocolError> {
            Err(AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                "fixture recovery remains unavailable",
            ))
        }
    }

    impl StaticAgentConnector {
        fn summary() -> AgentSessionSummary {
            AgentSessionSummary {
                connector_id: AgentConnectorId::new("fixture/local"),
                session_id: AgentSessionId::new("fixture-session"),
                title: Some("Existing fixture session".to_owned()),
                preview: Some("resume me".to_owned()),
                cwd: Some("/fixture/workspace".to_owned()),
                created_at_unix_ms: Some(1_000),
                updated_at_unix_ms: Some(2_000),
                state: AgentSessionState::Idle,
                extensions: BTreeMap::new(),
            }
        }

        fn created_summary() -> AgentSessionSummary {
            AgentSessionSummary {
                session_id: AgentSessionId::new("fixture-created"),
                title: Some("Created from HTTP".to_owned()),
                ..Self::summary()
            }
        }
    }

    #[async_trait]
    impl AgentConnector for StaticAgentConnector {
        fn describe(&self) -> AgentConnectorDescriptor {
            AgentConnectorDescriptor {
                connector_id: AgentConnectorId::new("fixture/local"),
                provider_binding: ProviderBindingRef::new("fixture/external"),
                agent_family: "test-agent".to_owned(),
                display_name: "Fixture Agent".to_owned(),
                capabilities: AgentSessionCapabilities {
                    create: true,
                    ..AgentSessionCapabilities::discoverable()
                },
                creation: None,
                actions: vec![
                    AgentSessionActionDescriptor {
                        action_id: AgentSessionActionId::new(SESSION_FORK_ACTION),
                        title: "Fork".to_owned(),
                        description: "Fork a fixture session".to_owned(),
                        input_schema: None,
                        execution: AgentSessionActionExecution::Immediate,
                    },
                    AgentSessionActionDescriptor {
                        action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                        title: "Rename".to_owned(),
                        description: "Rename a fixture session".to_owned(),
                        input_schema: Some(serde_json::json!({"type": "object"})),
                        execution: AgentSessionActionExecution::Immediate,
                    },
                    AgentSessionActionDescriptor {
                        action_id: AgentSessionActionId::new(SESSION_REVIEW_ACTION),
                        title: "Review".to_owned(),
                        description: "Review fixture changes".to_owned(),
                        input_schema: Some(serde_json::json!({"type": "object"})),
                        execution: AgentSessionActionExecution::Run,
                    },
                ],
            }
        }

        async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
            Ok(AgentConnectorHealth::ready(Some("test".to_owned())))
        }

        async fn list_sessions(
            &self,
            _query: AgentSessionListQuery,
        ) -> Result<AgentSessionPage, AgentConnectorError> {
            Ok(AgentSessionPage {
                sessions: vec![Self::summary()],
                next_cursor: None,
            })
        }

        async fn read_session(
            &self,
            session_id: &AgentSessionId,
        ) -> Result<AgentSessionDetail, AgentConnectorError> {
            if session_id.as_str() == "fixture-created" {
                return Ok(AgentSessionDetail {
                    summary: Self::created_summary(),
                    turns: Vec::new(),
                    pending_requests: Vec::new(),
                    next_cursor: None,
                });
            }
            if session_id.as_str() != "fixture-session" {
                return Err(AgentConnectorError::new(
                    orchestral_core::agent_connector::AgentConnectorErrorCode::NotFound,
                    "fixture session not found",
                    false,
                ));
            }
            Ok(AgentSessionDetail {
                summary: Self::summary(),
                turns: vec![AgentSessionTurn {
                    turn_id: AgentSessionTurnId::new("turn-large"),
                    status: AgentSessionTurnStatus::Completed,
                    activities: (0..60)
                        .map(|index| AgentSessionActivity {
                            activity_id: AgentSessionActivityId::new(format!("activity-{index}")),
                            kind: AgentSessionActivityKind::Command,
                            status: AgentSessionActivityStatus::Completed,
                            title: Some(format!("command-{index}")),
                            content: vec![Content::text("x".repeat(10_000))],
                            details: serde_json::Value::Null,
                        })
                        .collect(),
                }],
                pending_requests: Vec::new(),
                next_cursor: None,
            })
        }

        async fn create_session(
            &self,
            request: CreateAgentSessionRequest,
        ) -> Result<AgentSessionSummary, AgentConnectorError> {
            assert_eq!(request.cwd.as_deref(), Some("/fixture/new"));
            assert_eq!(request.title.as_deref(), Some("Created from HTTP"));
            assert!(request.extensions.is_empty());
            Ok(Self::created_summary())
        }

        async fn invoke_action(
            &self,
            request: InvokeAgentSessionActionRequest,
        ) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
            assert_eq!(request.session_id.as_str(), "fixture-session");
            assert_eq!(request.action_id.as_str(), SESSION_RENAME_ACTION);
            assert_eq!(request.arguments["name"], "Renamed over HTTP");
            let mut summary = Self::summary();
            summary.title = Some("Renamed over HTTP".to_owned());
            Ok(AgentSessionActionOutcome {
                status: AgentSessionActionStatus::Completed,
                session: Some(summary),
                content: Vec::new(),
                details: serde_json::Value::Null,
            })
        }
    }

    #[async_trait]
    impl AgentConnector for ObservableAgentConnector {
        fn describe(&self) -> AgentConnectorDescriptor {
            StaticAgentConnector.describe()
        }

        async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
            StaticAgentConnector.health().await
        }

        async fn list_sessions(
            &self,
            query: AgentSessionListQuery,
        ) -> Result<AgentSessionPage, AgentConnectorError> {
            StaticAgentConnector.list_sessions(query).await
        }

        async fn read_session(
            &self,
            session_id: &AgentSessionId,
        ) -> Result<AgentSessionDetail, AgentConnectorError> {
            StaticAgentConnector.read_session(session_id).await
        }

        async fn subscribe_session_changes(
            &self,
            session_id: &AgentSessionId,
        ) -> Result<broadcast::Receiver<AgentSessionChange>, AgentConnectorError> {
            if session_id.as_str() != "fixture-session" {
                return Err(AgentConnectorError::invalid("unexpected fixture session"));
            }
            self.subscriptions.fetch_add(1, Ordering::SeqCst);
            Ok(self.changes.subscribe())
        }
    }

    #[async_trait]
    impl GatewayAuthenticator for StaticGatewayAuthenticator {
        fn header_name(&self) -> &HeaderName {
            &self.header_name
        }

        async fn authenticate(&self, token: &str) -> Result<GatewayPrincipal, GatewayAuthError> {
            if token != "valid-gateway-assertion" {
                return Err(GatewayAuthError::Invalid(
                    "test assertion was rejected".to_owned(),
                ));
            }
            Ok(GatewayPrincipal {
                subject: Some("gateway-user".to_owned()),
                attributes: BTreeMap::from([("email".to_owned(), "person@example.com".to_owned())]),
            })
        }
    }

    struct ApprovalProvider {
        descriptor: AgentDescriptorEnvelope,
        events: broadcast::Sender<AgentEventDraft>,
        approvals: Arc<InMemoryHostApprovalBroker>,
    }

    impl ApprovalProvider {
        fn new(approvals: Arc<InMemoryHostApprovalBroker>) -> Self {
            let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
                provider_id: AgentProviderId::new("test.remote-approval"),
                agent_id: AgentId::new("approval-v1"),
                supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
                accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
                capabilities: AgentCapabilities {
                    session_reuse: true,
                    pending_request_kinds: BTreeSet::from([PendingRequestKind::Approval]),
                    effect_mediation: EffectMediation::HostMediated,
                    ..AgentCapabilities::default()
                },
                extensions: Default::default(),
            })
            .unwrap();
            let (events, _) = broadcast::channel(32);
            Self {
                descriptor,
                events,
                approvals,
            }
        }

        fn draft(
            run_id: &RunId,
            event_id: impl Into<String>,
            causation_id: Option<CommandId>,
            payload: AgentEvent,
        ) -> AgentEventDraft {
            AgentEventDraft {
                event_id: orchestral_core::agent_protocol::wire::AgentEventId::new(event_id),
                run_id: run_id.clone(),
                causation_id,
                source_fingerprint: None,
                payload,
            }
        }
    }

    #[async_trait]
    impl AgentProvider for ApprovalProvider {
        fn describe(&self) -> AgentDescriptorEnvelope {
            self.descriptor.clone()
        }

        async fn start(
            &self,
            request: orchestral_core::agent_protocol::wire::AgentStartRequest,
        ) -> Result<AgentStart, AgentStartError> {
            request
                .validate_for_descriptor(&self.descriptor)
                .map_err(AgentStartError::OutcomeUnknown)?;
            let execution = AgentExecutionRef::for_start(&request, &self.descriptor)
                .map_err(AgentStartError::OutcomeUnknown)?;
            let run_id = execution.run_id.clone();
            self.approvals
                .stage(
                    &RequestId::new("approval-request"),
                    ApprovalBinding {
                        run_id: run_id.clone(),
                        call_id: ToolCallId::new("approval-call"),
                        tool_id: ToolId::new("test/write"),
                        args_digest: orchestral_core::agent_protocol::wire::Digest::sha256(
                            "write args",
                        ),
                        operation_digest: orchestral_core::agent_protocol::wire::Digest::sha256(
                            "write operation",
                        ),
                        permission_digest: orchestral_core::agent_protocol::wire::Digest::sha256(
                            "write permission",
                        ),
                        requested_capabilities: CapabilityRequest::from_effects(BTreeSet::from([
                            EffectScope::FilesystemWrite,
                        ])),
                        session_approval_scope: None,
                        policy_digest: orchestral_core::agent_protocol::wire::Digest::sha256(
                            "test policy",
                        ),
                    },
                )
                .await
                .map_err(|error| {
                    AgentStartError::OutcomeUnknown(AgentProtocolError::new(
                        AgentProtocolErrorCode::Internal,
                        error.to_string(),
                    ))
                })?;
            let mut receiver = self.events.subscribe();
            let _ = self.events.send(Self::draft(
                &run_id,
                "approval-run-started",
                None,
                AgentEvent::RunStarted,
            ));
            let _ = self.events.send(Self::draft(
                &run_id,
                "approval-request-opened",
                None,
                AgentEvent::RequestOpened {
                    request: PendingRequest {
                        request_id: RequestId::new("approval-request"),
                        blocking: true,
                        payload: PendingRequestPayload::Approval {
                            operation_digest: orchestral_core::agent_protocol::wire::Digest::sha256(
                                "write operation",
                            ),
                            requested_scope: vec!["filesystem_write".to_owned()],
                            session_approval_scope: None,
                            reason: "write the requested file".to_owned(),
                        },
                    },
                },
            ));
            let stream = async_stream::stream! {
                loop {
                    match receiver.recv().await {
                        Ok(event) => yield Ok(AgentProviderStreamItem::Event(Box::new(event))),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
            .boxed();
            Ok(AgentStart {
                execution,
                admission: AgentAdmission {
                    skipped_optional_bindings: Vec::new(),
                },
                stream,
            })
        }

        async fn command(
            &self,
            execution: &AgentExecutionRef,
            command: AgentCommandEnvelope,
        ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
            command.verify_digest()?;
            let outcome = match (&command.request_id, &command.payload) {
                (Some(request_id), AgentCommand::ResolveRequest { response }) => {
                    let resolution = response.clone();
                    self.events
                        .send(Self::draft(
                            &execution.run_id,
                            format!("approval-resolved-{}", command.command_id.as_str()),
                            Some(command.command_id.clone()),
                            AgentEvent::RequestResolved {
                                request_id: request_id.clone(),
                                resolution_digest: resolution.digest()?,
                                resolution,
                            },
                        ))
                        .map_err(|_| {
                            AgentProtocolError::new(
                                AgentProtocolErrorCode::Internal,
                                "approval test stream is closed",
                            )
                        })?;
                    ProviderCommandOutcome::Accepted
                }
                _ => ProviderCommandOutcome::Unsupported {
                    feature: "command".to_owned(),
                },
            };
            Ok(ProviderCommandDisposition {
                command_id: command.command_id,
                run_id: command.run_id,
                outcome,
                duplicate: false,
            })
        }

        async fn recover(
            &self,
            _request: AgentRecoveryRequest,
        ) -> Result<AgentRecovery, AgentProtocolError> {
            Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "approval fixture does not recover",
            ))
        }
    }

    async fn test_app() -> (Router, String) {
        test_app_with_gateway(None).await
    }

    async fn holding_agent_app() -> (Router, String) {
        let factory = ScriptedStatelessFactory::conformant().unwrap();
        let descriptor = factory.descriptor();
        let scenario = ProviderScenario::standard(&descriptor).unwrap();
        let controller = Arc::new(
            AgentController::new(
                factory.create(scenario, TestProbes::default()),
                ProviderBindingRef::new("remote-test"),
            )
            .unwrap(),
        );
        let ticket = super::super::state::PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::in_memory(Some(ticket));
        let claim = registry.claim_pairing(&secret, "Two tabs").await.unwrap();
        let approvals =
            Arc::new(InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap());
        let external_factory = SessionfulRecoverFactory::new().unwrap();
        let external_scenario = ProviderScenario::standard(&external_factory.descriptor()).unwrap();
        let external_provider = Arc::new(HoldingProvider {
            inner: external_factory.create(external_scenario, TestProbes::default()),
        });
        let agent_directory = Arc::new(AgentDirectory::new());
        agent_directory
            .register(Arc::new(StaticAgentConnector), external_provider)
            .await
            .unwrap();
        (
            router(RemoteApiState {
                agent: AgentApi::new(controller),
                agent_directory,
                approvals,
                registry,
                gateway_authenticator: None,
                run_supervisors: Arc::default(),
                session_coordinators: Arc::default(),
                artifact_resolver: None,
                artifact_blob_store: None,
            }),
            claim.token,
        )
    }

    async fn test_app_with_gateway(
        gateway_authenticator: Option<Arc<dyn GatewayAuthenticator>>,
    ) -> (Router, String) {
        let factory = ScriptedStatelessFactory::conformant().unwrap();
        let descriptor = factory.descriptor();
        let scenario = ProviderScenario::standard(&descriptor).unwrap();
        let provider = factory.create(scenario, TestProbes::default());
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("remote-test")).unwrap(),
        );
        let ticket = super::super::state::PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::in_memory(Some(ticket));
        let claim = registry.claim_pairing(&secret, "Test phone").await.unwrap();
        let approvals =
            Arc::new(InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap());
        let external_factory = SessionfulRecoverFactory::new().unwrap();
        let external_scenario = ProviderScenario::standard(&external_factory.descriptor()).unwrap();
        let external_provider = Arc::new(DisconnectFirstProvider {
            inner: external_factory.create(external_scenario, TestProbes::default()),
        });
        let agent_directory = Arc::new(AgentDirectory::new());
        agent_directory
            .register(Arc::new(StaticAgentConnector), external_provider)
            .await
            .unwrap();
        (
            router(RemoteApiState {
                agent: AgentApi::new(controller),
                agent_directory,
                approvals,
                registry,
                gateway_authenticator,
                run_supervisors: Arc::default(),
                session_coordinators: Arc::default(),
                artifact_resolver: None,
                artifact_blob_store: None,
            }),
            claim.token,
        )
    }

    async fn stale_external_agent_app() -> (Router, String) {
        let journal = Arc::new(InMemoryAgentJournalStore::default());
        let previous_factory = ScriptedStatelessFactory::conformant().unwrap();
        let previous_scenario = ProviderScenario::standard(&previous_factory.descriptor()).unwrap();
        let previous = Arc::new(
            AgentController::with_journal_store(
                previous_factory.create(previous_scenario.clone(), TestProbes::default()),
                ProviderBindingRef::new("fixture/previous"),
                journal.clone(),
            )
            .unwrap(),
        );
        let mut previous_spec = previous_scenario.start_request.run.spec;
        previous_spec.session_id = AgentSessionId::new("fixture-session");
        previous_spec.run_id = RunId::new("fixture-stale-run");
        let previous_execution = previous
            .start(AgentRunEnvelope::seal(previous_spec).unwrap())
            .await
            .unwrap();
        previous
            .wait_for_terminal(&previous_execution.run_id)
            .await
            .unwrap();
        drop(previous);

        let external_factory = SessionfulRecoverFactory::new().unwrap();
        let external_scenario = ProviderScenario::standard(&external_factory.descriptor()).unwrap();
        let external_provider = Arc::new(DisconnectFirstProvider {
            inner: external_factory.create(external_scenario, TestProbes::default()),
        });
        let agent_directory = Arc::new(AgentDirectory::new());
        agent_directory
            .register_with_journal(Arc::new(StaticAgentConnector), external_provider, journal)
            .await
            .unwrap();

        let generic_factory = ScriptedStatelessFactory::conformant().unwrap();
        let generic_scenario = ProviderScenario::standard(&generic_factory.descriptor()).unwrap();
        let generic_controller = Arc::new(
            AgentController::new(
                generic_factory.create(generic_scenario, TestProbes::default()),
                ProviderBindingRef::new("remote-test"),
            )
            .unwrap(),
        );
        let ticket = super::super::state::PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::in_memory(Some(ticket));
        let claim = registry
            .claim_pairing(&secret, "Descriptor upgrade phone")
            .await
            .unwrap();
        let approvals =
            Arc::new(InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap());
        (
            router(RemoteApiState {
                agent: AgentApi::new(generic_controller),
                agent_directory,
                approvals,
                registry,
                gateway_authenticator: None,
                run_supervisors: Arc::default(),
                session_coordinators: Arc::default(),
                artifact_resolver: None,
                artifact_blob_store: None,
            }),
            claim.token,
        )
    }

    async fn approval_app() -> (Router, String) {
        let approvals =
            Arc::new(InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap());
        let provider = Arc::new(ApprovalProvider::new(approvals.clone()));
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("remote-test")).unwrap(),
        );
        let ticket = super::super::state::PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::in_memory(Some(ticket));
        let claim = registry
            .claim_pairing(&secret, "Approval phone")
            .await
            .unwrap();
        (
            router(RemoteApiState {
                agent: AgentApi::new(controller),
                agent_directory: Arc::new(AgentDirectory::new()),
                approvals,
                registry,
                gateway_authenticator: None,
                run_supervisors: Arc::default(),
                session_coordinators: Arc::default(),
                artifact_resolver: None,
                artifact_blob_store: None,
            }),
            claim.token,
        )
    }

    async fn unrecoverable_app() -> (Router, String) {
        let approvals =
            Arc::new(InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap());
        let provider = Arc::new(UnrecoverableDisconnectProvider {
            inner: Arc::new(ApprovalProvider::new(approvals.clone())),
        });
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("remote-test")).unwrap(),
        );
        let ticket = super::super::state::PairingTicket::issue(60_000).unwrap();
        let secret = ticket.secret().to_owned();
        let registry = RemoteRegistry::in_memory(Some(ticket));
        let claim = registry
            .claim_pairing(&secret, "Recovery phone")
            .await
            .unwrap();
        (
            router(RemoteApiState {
                agent: AgentApi::new(controller),
                agent_directory: Arc::new(AgentDirectory::new()),
                approvals,
                registry,
                gateway_authenticator: None,
                run_supervisors: Arc::default(),
                session_coordinators: Arc::default(),
                artifact_resolver: None,
                artifact_blob_store: None,
            }),
            claim.token,
        )
    }

    fn authorized(method: &str, uri: &str, token: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn multiple_clients_share_one_connector_subscription_and_sequence() {
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let (changes, _) = broadcast::channel(16);
        let connector = Arc::new(ObservableAgentConnector {
            subscriptions: subscriptions.clone(),
            changes: changes.clone(),
        });
        let factory = SessionfulRecoverFactory::new().unwrap();
        let scenario = ProviderScenario::standard(&factory.descriptor()).unwrap();
        let directory = Arc::new(AgentDirectory::new());
        directory
            .register(connector, factory.create(scenario, TestProbes::default()))
            .await
            .unwrap();

        let connector_id = AgentConnectorId::new("fixture/local");
        let session_id = AgentSessionId::new("fixture-session");
        let registry = AgentSessionCoordinatorRegistry::default();
        let coordinator = registry.get(&connector_id, &session_id);
        let first_hub = coordinator
            .ensure_hub(directory.clone(), &connector_id, &session_id)
            .await
            .unwrap();
        let second_hub = registry
            .get(&connector_id, &session_id)
            .ensure_hub(directory, &connector_id, &session_id)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first_hub, &second_hub));
        assert_eq!(subscriptions.load(Ordering::SeqCst), 1);

        let mut phone = first_hub.subscribe(0).live;
        let mut desktop = second_hub.subscribe(0).live;
        changes
            .send(AgentSessionChange {
                connector_id: connector_id.clone(),
                session_id: session_id.clone(),
                sequence: 900,
                change: orchestral_core::agent_connector::AgentSessionChangeKind::RefreshRequired {
                    reason: "fixture-change".to_owned(),
                },
            })
            .unwrap();
        let phone_change = tokio::time::timeout(Duration::from_secs(1), phone.recv())
            .await
            .unwrap()
            .unwrap();
        let desktop_change = tokio::time::timeout(Duration::from_secs(1), desktop.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(phone_change, desktop_change);
        assert_eq!(phone_change.sequence, 1);
    }

    #[tokio::test]
    async fn protected_routes_require_a_paired_device() {
        let (app, _) = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sessions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("every API response carries its log correlation id");
        uuid::Uuid::parse_str(request_id).expect("request id is a UUID");
        assert!(response.extensions().get::<ApiErrorLogCode>().is_some());
    }

    #[tokio::test]
    async fn concurrent_agent_session_posts_share_one_run_and_retry_one_command() {
        let (app, token) = holding_agent_app().await;
        let request = |run_id: &str, input: &str| {
            authorized(
                "POST",
                "/agent-runs",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "session_id": "fixture-session",
                    "run_id": run_id,
                    "input": input
                }),
            )
        };

        let (left, right) = tokio::join!(
            app.clone().oneshot(request("tab-a", "message from tab A")),
            app.clone().oneshot(request("tab-b", "message from tab B")),
        );
        let mut responses = Vec::new();
        for response in [left.unwrap(), right.unwrap()] {
            assert!(matches!(
                response.status(),
                StatusCode::CREATED | StatusCode::OK
            ));
            let body = response.into_body().collect().await.unwrap().to_bytes();
            responses.push(serde_json::from_slice::<serde_json::Value>(&body).unwrap());
        }
        let started = responses
            .iter()
            .find(|response| response["operation"] == "started")
            .expect("one tab starts the session Run");
        let steered = responses
            .iter()
            .find(|response| response["operation"] == "steered")
            .expect("the other tab steers that same Run");
        assert_eq!(started["run_id"], steered["run_id"]);
        let active_run_id = started["run_id"].as_str().unwrap();
        let steered_operation_id = if active_run_id == "tab-a" {
            "tab-b"
        } else {
            "tab-a"
        };

        let retry = app
            .clone()
            .oneshot(request(
                steered_operation_id,
                if steered_operation_id == "tab-a" {
                    "message from tab A"
                } else {
                    "message from tab B"
                },
            ))
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::OK);
        let body = retry.into_body().collect().await.unwrap().to_bytes();
        let retry: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(retry["operation"], "steered");
        assert_eq!(retry["run_id"], active_run_id);

        let response = app
            .oneshot(authorized(
                "GET",
                &format!("/runs/{active_run_id}/events?connector_id=fixture%2Flocal&after=0"),
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let events: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let command_id = format!("agent-submit-{steered_operation_id}");
        let command_count = events["records"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|record| {
                record["event"]["payload"]["type"] == "command_received"
                    && record["event"]["payload"]["command"]["command_id"] == command_id
            })
            .count();
        assert_eq!(command_count, 1, "network retry must not steer twice");
    }

    #[tokio::test]
    async fn external_agent_session_can_be_discovered_read_and_started_through_http() {
        let (app, token) = test_app().await;

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/agent-connectors",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let connectors: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(connectors[0]["connector_id"], "fixture/local");

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/agent-sessions?connector_id=fixture%2Flocal&limit=25",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let sessions: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(sessions["sessions"][0]["session_id"], "fixture-session");

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/agent-session?connector_id=fixture%2Flocal&session_id=fixture-session&limit=100",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let session_etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(body.len() < 512 * 1_024);
        let session: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(session["summary"]["title"], "Existing fixture session");
        assert!(session["next_cursor"].is_string());
        assert_eq!(
            session["turns"][0]["activities"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()["activity_id"],
            "activity-59"
        );

        let mut conditional = authorized(
            "GET",
            "/agent-session?connector_id=fixture%2Flocal&session_id=fixture-session&limit=100",
            &token,
            serde_json::Value::Null,
        );
        conditional.headers_mut().insert(
            header::IF_NONE_MATCH,
            header::HeaderValue::from_str(&session_etag).unwrap(),
        );
        let response = app.clone().oneshot(conditional).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()[header::ETAG], session_etag);
        assert!(response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty());

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/agent-sessions",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "cwd": "/fixture/new",
                    "title": "Created from HTTP"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let created: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(created["session_id"], "fixture-created");

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/agent-session/actions",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "session_id": "fixture-session",
                    "action_id": "session.rename",
                    "arguments": {"name": "Renamed over HTTP"}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let renamed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(renamed["session"]["title"], "Renamed over HTTP");

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/agent-session/actions",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "session_id": "fixture-session",
                    "action_id": "session.review",
                    "arguments": {"target": "uncommitted_changes"},
                    "run_id": "fixture-review-run"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let review: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(review["status"]["state"], "running");
        assert_eq!(review["status"]["run_id"], "fixture-review-run");

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/agent-session/actions",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "session_id": "fixture-session",
                    "action_id": "session.undeclared"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/agent-runs",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "session_id": "fixture-created",
                    "run_id": "fixture-external-run",
                    "input": "continue the existing session"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let started: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(started["connector_id"], "fixture/local");
        assert_eq!(started["run_id"], "fixture-external-run");

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/agent-session?connector_id=fixture%2Flocal&session_id=fixture-created&limit=100",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let refreshed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            refreshed["controlled_runs"][0]["execution"]["run_id"],
            "fixture-external-run"
        );
        assert!(refreshed["controlled_runs"][0]["created_at_unix_ms"].is_i64());

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/runs/fixture-external-run?connector_id=fixture%2Flocal",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let run: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            run["input"][0]["body"]["value"],
            "continue the existing session"
        );

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/runs/fixture-external-run/events?connector_id=fixture%2Flocal&after=0",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let events: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(events["records"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
        assert!(events["next"].as_u64().is_some_and(|next| next > 0));

        let mut recovered_terminal = false;
        for _ in 0..100 {
            let response = app
                .clone()
                .oneshot(authorized(
                    "GET",
                    "/runs/fixture-external-run?connector_id=fixture%2Flocal",
                    &token,
                    serde_json::Value::Null,
                ))
                .await
                .unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let view: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if view["state"]["state"] == "terminal" {
                recovered_terminal = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            recovered_terminal,
            "the Host supervisor automatically recovers the disconnected stream"
        );

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/runs/fixture-external-run/events?connector_id=fixture%2Flocal&after=0",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let events: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let event_types = events["records"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|record| record["event"]["payload"]["type"].as_str())
            .collect::<Vec<_>>();
        assert!(event_types.contains(&"continuity_lost"));
        assert!(event_types.contains(&"continuity_restored"));
    }

    #[tokio::test]
    async fn old_connector_contract_does_not_block_session_read_or_new_http_run() {
        let (app, token) = stale_external_agent_app().await;

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/agent-session?connector_id=fixture%2Flocal&session_id=fixture-session&limit=100",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let session: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(session["summary"]["session_id"], "fixture-session");
        assert!(session["controlled_runs"].as_array().unwrap().is_empty());

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/agent-runs",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "session_id": "fixture-session",
                    "run_id": "fixture-current-run",
                    "input": "continue after the connector upgrade"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let started: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(started["operation"], "started");
        assert_eq!(started["run_id"], "fixture-current-run");

        let retry = app
            .oneshot(authorized(
                "POST",
                "/agent-runs",
                &token,
                serde_json::json!({
                    "connector_id": "fixture/local",
                    "session_id": "fixture-session",
                    "run_id": "fixture-current-run",
                    "input": "continue after the connector upgrade"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::OK);
        let body = retry.into_body().collect().await.unwrap().to_bytes();
        let replayed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(replayed["operation"], "replayed");
        assert_eq!(replayed["run_id"], "fixture-current-run");
    }

    #[tokio::test]
    async fn gateway_mode_accepts_a_verified_identity_without_a_bearer_token() {
        let authenticator = Arc::new(StaticGatewayAuthenticator {
            header_name: HeaderName::from_static("x-gateway-jwt"),
        });
        let (app, _) = test_app_with_gateway(Some(authenticator)).await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/me")
                    .header("x-gateway-jwt", "valid-gateway-assertion")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let me: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(me["auth_mode"], "gateway_jwt");
        assert_eq!(me["attributes"]["email"], "person@example.com");

        for assertion in [None, Some("forged-assertion")] {
            let mut request = Request::builder().uri("/sessions");
            if let Some(assertion) = assertion {
                request = request.header("x-gateway-jwt", assertion);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn commands_return_recovery_pending_while_continuity_is_unknown() {
        let (app, token) = unrecoverable_app().await;
        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/sessions",
                &token,
                serde_json::json!({"session_id": "recovery-session"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/sessions/recovery-session/runs",
                &token,
                serde_json::json!({
                    "run_id": "recovery-run",
                    "input": "wait for recovery"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut unknown = false;
        for _ in 0..50 {
            let response = app
                .clone()
                .oneshot(authorized(
                    "GET",
                    "/runs/recovery-run",
                    &token,
                    serde_json::Value::Null,
                ))
                .await
                .unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let view: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if view["state"]["state"] == "unknown" {
                unknown = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(unknown, "fixture Run did not enter Unknown continuity");

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/runs/recovery-run/steer",
                &token,
                serde_json::json!({
                    "command_id": "steer-during-recovery",
                    "text": "do not duplicate this"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["code"], "run_recovery_pending");
    }

    #[tokio::test]
    async fn approval_is_host_signed_and_resolves_the_pending_request() {
        let (app, token) = approval_app().await;
        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/sessions",
                &token,
                serde_json::json!({"session_id": "approval-session"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/sessions/approval-session/runs",
                &token,
                serde_json::json!({
                    "run_id": "approval-run",
                    "input": "perform the write"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut pending_seen = false;
        for _ in 0..50 {
            let response = app
                .clone()
                .oneshot(authorized(
                    "GET",
                    "/runs/approval-run",
                    &token,
                    serde_json::Value::Null,
                ))
                .await
                .unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let view: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if view["pending_requests"]
                .as_array()
                .is_some_and(|items| items.len() == 1)
            {
                pending_seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending_seen, "approval request did not become visible");

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/runs/approval-run/requests/approval-request/approval",
                &token,
                serde_json::json!({
                    "command_id": "approve-from-phone",
                    "decision": "allow_once"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let ack: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(matches!(
            ack["state"]["state"].as_str(),
            Some("accepted" | "applied")
        ));

        let mut resolved = None;
        for _ in 0..50 {
            let response = app
                .clone()
                .oneshot(authorized(
                    "GET",
                    "/runs/approval-run/events?after=0",
                    &token,
                    serde_json::Value::Null,
                ))
                .await
                .unwrap();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
            resolved = page["records"]
                .as_array()
                .and_then(|records| {
                    records
                        .iter()
                        .find(|record| record["event"]["payload"]["type"] == "request_resolved")
                })
                .cloned();
            if resolved.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let resolved = resolved.expect("approval resolution is durable");
        let resolution = &resolved["event"]["payload"]["resolution"];
        assert_eq!(resolution["type"], "approval");
        assert_eq!(resolution["decision"], "allow");
        assert!(resolution["grant_ref"]
            .as_str()
            .is_some_and(|grant| !grant.is_empty()));
    }

    #[tokio::test]
    async fn revoking_the_current_device_invalidates_its_next_request() {
        let (app, token) = test_app().await;
        let response = app
            .clone()
            .oneshot(authorized("GET", "/me", &token, serde_json::Value::Null))
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let me: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let device_id = me["device_id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(authorized(
                "DELETE",
                &format!("/devices/{device_id}"),
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = app
            .oneshot(authorized(
                "GET",
                "/sessions",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn session_start_inspect_and_cursor_replay_use_real_agent_api() {
        let (app, token) = test_app().await;
        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/sessions",
                &token,
                serde_json::json!({"session_id": "mobile-session"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .clone()
            .oneshot(authorized(
                "POST",
                "/sessions/mobile-session/runs",
                &token,
                serde_json::json!({
                    "run_id": "mobile-run",
                    "input": "complete the deterministic fixture"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let started: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            started["view"]["input"][0]["body"]["value"],
            "complete the deterministic fixture"
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/runs/mobile-run",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let inspected: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            inspected["input"][0]["body"]["value"],
            "complete the deterministic fixture"
        );

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/sessions",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let sessions: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(sessions[0]["id"], "mobile-session");
        assert_eq!(sessions[0]["run_ids"], serde_json::json!(["mobile-run"]));

        let response = app
            .clone()
            .oneshot(authorized(
                "GET",
                "/runs/mobile-run/events?after=0",
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let records = payload["records"].as_array().unwrap();
        assert!(!records.is_empty());
        let next = payload["next"].as_u64().unwrap();

        let response = app
            .oneshot(authorized(
                "GET",
                &format!("/runs/mobile-run/events?after={next}"),
                &token,
                serde_json::Value::Null,
            ))
            .await
            .unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(payload["records"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn command_identity_is_forwarded_for_retry_deduplication() {
        let (app, token) = test_app().await;
        app.clone()
            .oneshot(authorized(
                "POST",
                "/sessions",
                &token,
                serde_json::json!({"session_id": "mobile-session"}),
            ))
            .await
            .unwrap();
        app.clone()
            .oneshot(authorized(
                "POST",
                "/sessions/mobile-session/runs",
                &token,
                serde_json::json!({"run_id": "mobile-run", "input": "do it"}),
            ))
            .await
            .unwrap();

        let command = || {
            authorized(
                "POST",
                "/runs/mobile-run/cancel",
                &token,
                serde_json::json!({
                    "command_id": "mobile-command-1",
                    "reason": "stop from phone"
                }),
            )
        };
        let first = app.clone().oneshot(command()).await.unwrap();
        let second = app.oneshot(command()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
        let body = second.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["duplicate"], true);
    }

    #[tokio::test]
    async fn sse_replay_uses_durable_sequence_ids_after_the_requested_cursor() {
        let (app, token) = test_app().await;
        app.clone()
            .oneshot(authorized(
                "POST",
                "/sessions",
                &token,
                serde_json::json!({"session_id": "stream-session"}),
            ))
            .await
            .unwrap();
        app.clone()
            .oneshot(authorized(
                "POST",
                "/sessions/stream-session/runs",
                &token,
                serde_json::json!({"run_id": "stream-run", "input": "do it"}),
            ))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let request = Request::builder()
            .uri("/runs/stream-run/stream?after=0")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("last-event-id", "2")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "no-store, private"
        );
        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("SSE response carries its lifecycle correlation id");
        uuid::Uuid::parse_str(request_id).expect("request id is a UUID");
        let mut stream = response.into_body().into_data_stream();
        let frame = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let text = String::from_utf8(frame.to_vec()).unwrap();
        assert!(text.contains("event: durable"));
        assert!(!text.contains("id: 1\n"));
        assert!(!text.contains("id: 2\n"));
    }
}
