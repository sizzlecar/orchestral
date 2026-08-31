use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use orchestral_core::agent_protocol::wire::{
    AgentCommand, AgentCommandEnvelope, AgentRunView, AgentSessionId, ApprovalDecision, CommandAck,
    CommandId, Content, PendingRequest, PendingRequestPayload, RequestId, RequestResolution, RunId,
};
use orchestral_runtime::api::AgentApi;
use orchestral_runtime::{
    AgentControlEvent, AgentSdkError, ApprovalBridgeError, InMemoryHostApprovalBroker,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use super::state::{DevicePrincipal, DeviceView, PairingClaim, RemoteRegistry, SessionView};

const APPROVAL_GRANT_TTL_MS: i64 = 5 * 60 * 1_000;

#[derive(Clone)]
pub struct RemoteApiState {
    pub agent: AgentApi,
    pub approvals: Arc<InMemoryHostApprovalBroker>,
    pub registry: RemoteRegistry,
}

pub fn router(state: RemoteApiState) -> Router {
    let protected = Router::new()
        .route("/me", get(me))
        .route("/devices", get(list_devices))
        .route("/devices/{device_id}", delete(revoke_device))
        .route("/sessions", get(list_sessions).post(create_session))
        .route("/sessions/{session_id}", get(get_session))
        .route("/sessions/{session_id}/runs", post(start_run))
        .route("/runs/{run_id}", get(inspect_run))
        .route("/runs/{run_id}/events", get(run_events))
        .route("/runs/{run_id}/stream", get(run_stream))
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
    device_id: String,
}

async fn me(Extension(principal): Extension<DevicePrincipal>) -> Json<MeResponse> {
    Json(MeResponse {
        device_id: principal.device_id,
    })
}

async fn list_devices(
    State(state): State<RemoteApiState>,
    Extension(principal): Extension<DevicePrincipal>,
) -> Json<Vec<DeviceView>> {
    Json(state.registry.devices(&principal.device_id).await)
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

async fn list_sessions(State(state): State<RemoteApiState>) -> Json<Vec<SessionView>> {
    Json(state.registry.sessions().await)
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
    let session = state.registry.register_session(session_id.as_str()).await?;
    Ok((StatusCode::CREATED, Json(session)))
}

async fn get_session(
    State(state): State<RemoteApiState>,
    Path(session_id): Path<String>,
) -> Result<Json<SessionView>, ApiError> {
    state
        .registry
        .session(&session_id)
        .await
        .map(Json)
        .ok_or_else(|| ApiError::not_found("session_not_found", "session was not found"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StartRunRequest {
    run_id: String,
    input: String,
}

#[derive(Debug, Serialize)]
struct StartRunResponse {
    run_id: RunId,
    view: RemoteRunView,
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
    require_session(&state, &session_id).await?;
    let session_id = AgentSessionId::new(session_id);
    state.agent.create_session(Some(session_id.clone())).await?;
    let run_id = RunId::new(request.run_id);
    let handle = state
        .agent
        .start_text(&session_id, Some(run_id.clone()), request.input)
        .await?;
    state
        .registry
        .record_run(session_id.as_str(), run_id.as_str())
        .await?;
    spawn_remembered_approval_driver(state.clone(), run_id.clone());
    let view = RemoteRunView {
        view: handle.inspect().await?,
        input: state.agent.initial_input(&run_id).await?,
    };
    Ok((StatusCode::CREATED, Json(StartRunResponse { run_id, view })))
}

fn spawn_remembered_approval_driver(state: RemoteApiState, run_id: RunId) {
    tokio::spawn(async move {
        let Ok(mut live) = state.agent.subscribe(&run_id).await else {
            return;
        };
        loop {
            let Ok(view) = state.agent.inspect(&run_id).await else {
                return;
            };
            for request in &view.pending_requests {
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
                let _ = state.agent.command(command).await;
            }
            if view.state.is_terminal() {
                return;
            }
            match live.recv().await {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });
}

async fn inspect_run(
    State(state): State<RemoteApiState>,
    Path(run_id): Path<String>,
) -> Result<Json<RemoteRunView>, ApiError> {
    let run_id = require_run(&state, run_id).await?;
    Ok(Json(RemoteRunView {
        view: state.agent.inspect(&run_id).await?,
        input: state.agent.initial_input(&run_id).await?,
    }))
}

#[derive(Debug, Deserialize, Default)]
struct EventsQuery {
    #[serde(default)]
    after: u64,
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
    let run_id = require_run(&state, run_id).await?;
    let records = state.agent.events(&run_id, query.after).await?;
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
    Path(run_id): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let run_id = require_run(&state, run_id).await?;
    let header_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default();
    let initial_cursor = query.after.max(header_cursor);
    // Subscribe before replay. Any event committed between these operations is
    // either present in replay or remains queued; sequence filtering removes
    // the harmless overlap.
    let mut live = state.agent.subscribe(&run_id).await?;
    let agent = state.agent.clone();
    let stream = async_stream::stream! {
        let mut cursor = initial_cursor;
        match replay_events(&agent, &run_id, &mut cursor).await {
            Ok(events) => {
                for event in events {
                    yield Ok(event);
                }
            }
            Err(error) => {
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
                            yield Ok(api_error_event(&ApiError::internal(
                                "stream_encode_failed",
                                error.to_string(),
                            )));
                            return;
                        }
                    }
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    match replay_events(&agent, &run_id, &mut cursor).await {
                        Ok(events) => {
                            for event in events {
                                yield Ok(event);
                            }
                        }
                        Err(error) => {
                            yield Ok(api_error_event(&error));
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    if let Ok(events) = replay_events(&agent, &run_id, &mut cursor).await {
                        for event in events {
                            yield Ok(event);
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
}

async fn steer_run(
    State(state): State<RemoteApiState>,
    Path(run_id): Path<String>,
    Json(request): Json<TextCommandRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let run_id = require_run(&state, run_id).await?;
    let command = AgentCommandEnvelope::new(
        CommandId::new(request.command_id),
        run_id,
        None,
        AgentCommand::Steer {
            content: vec![Content::text(request.text)],
        },
    )?;
    Ok(Json(state.agent.command(command).await?))
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
    Json(request): Json<CancelRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let run_id = require_run(&state, run_id).await?;
    let command = AgentCommandEnvelope::new(
        CommandId::new(request.command_id),
        run_id,
        None,
        AgentCommand::Cancel {
            reason: request.reason,
        },
    )?;
    Ok(Json(state.agent.command(command).await?))
}

async fn resolve_input(
    State(state): State<RemoteApiState>,
    Path((run_id, request_id)): Path<(String, String)>,
    Json(request): Json<TextCommandRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let run_id = require_run(&state, run_id).await?;
    let request_id = RequestId::new(request_id);
    let pending = require_pending(&state, &run_id, &request_id).await?;
    if !matches!(pending.payload, PendingRequestPayload::Input { .. }) {
        return Err(ApiError::conflict(
            "request_kind_mismatch",
            "pending request does not accept text input",
        ));
    }
    let command = AgentCommandEnvelope::new(
        CommandId::new(request.command_id),
        run_id,
        Some(request_id),
        AgentCommand::ResolveRequest {
            response: RequestResolution::Input {
                content: vec![Content::text(request.text)],
            },
        },
    )?;
    Ok(Json(state.agent.command(command).await?))
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
    Json(request): Json<ApprovalRequest>,
) -> Result<Json<CommandAck>, ApiError> {
    let run_id = require_run(&state, run_id).await?;
    let request_id = RequestId::new(request_id);
    let pending = require_pending(&state, &run_id, &request_id).await?;
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
        run_id,
        Some(request_id),
        AgentCommand::ResolveRequest {
            response: RequestResolution::Approval {
                decision,
                grant_ref,
            },
        },
    )?;
    Ok(Json(state.agent.command(command).await?))
}

async fn require_session(state: &RemoteApiState, session_id: &str) -> Result<(), ApiError> {
    if state.registry.session(session_id).await.is_none() {
        return Err(ApiError::not_found(
            "session_not_found",
            "session was not found",
        ));
    }
    Ok(())
}

async fn require_run(state: &RemoteApiState, run_id: String) -> Result<RunId, ApiError> {
    if !state.registry.owns_run(&run_id).await {
        return Err(ApiError::not_found("run_not_found", "run was not found"));
    }
    Ok(RunId::new(run_id))
}

async fn require_pending(
    state: &RemoteApiState,
    run_id: &RunId,
    request_id: &RequestId,
) -> Result<PendingRequest, ApiError> {
    let view = state.agent.inspect(run_id).await?;
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
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
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

    fn internal(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
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
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::Request;
    use futures_util::StreamExt;
    use http_body_util::BodyExt;
    use orchestral_agent_protocol_testkit::{
        ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, TestProbes,
    };
    use orchestral_core::agent_protocol::{
        spi::{AgentProvider, AgentRecovery, AgentRecoveryRequest, AgentStart, AgentStartError},
        wire::{
            AgentAdmission, AgentCapabilities, AgentDescriptor, AgentDescriptorEnvelope,
            AgentEvent, AgentEventDraft, AgentExecutionRef, AgentId, AgentProtocolError,
            AgentProtocolErrorCode, AgentProviderId, AgentProviderStreamItem, EffectMediation,
            PendingRequestKind, ProviderBindingRef, ProviderCommandDisposition,
            ProviderCommandOutcome,
        },
        AGENT_PROTOCOL_V1,
    };
    use orchestral_core::tool_protocol::{
        ApprovalBinding, CapabilityRequest, EffectScope, ToolCallId, ToolId,
    };
    use orchestral_runtime::{AgentApprovalBridge, AgentController};
    use tokio::sync::broadcast;
    use tower::ServiceExt;

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
        (
            router(RemoteApiState {
                agent: AgentApi::new(controller),
                approvals,
                registry,
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
                approvals,
                registry,
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
