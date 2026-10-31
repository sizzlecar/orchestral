//! Discovery and authenticated forwarding to the process that owns a local
//! conversation. Journals stay single-writer; the gateway never recovers or
//! executes another live Host's Run.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use axum::body::{to_bytes, Body};
use axum::extract::{FromRequestParts, Path as AxumPath, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures_util::{stream, StreamExt};
use orchestral_core::agent_protocol::wire::AgentSessionId;
use orchestral_runtime::{AgentDirectory, AgentRunHandle};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_util::sync::CancellationToken;

use crate::agent::AgentHost;
use crate::mcp_config::user_config_root;

use super::state::{NativeSessionDefaults, SessionView};
use super::{router, PairingTicket, RemoteApiState, RemoteRegistry};

const REQUEST_LIMIT: usize = 256 * 1024;
const HEADER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    version: u32,
    instance: String,
    session_id: String,
    address: SocketAddr,
    token: String,
    active: bool,
}

impl Registration {
    fn validate(&self) -> anyhow::Result<()> {
        if self.version != 1
            || self.instance.is_empty()
            || self.session_id.is_empty()
            || self.token.is_empty()
            || self.address.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST)
            || self.address.port() == 0
        {
            bail!("invalid local Host registration");
        }
        Ok(())
    }
}

fn registry_root() -> anyhow::Result<PathBuf> {
    Ok(user_config_root()?.join("local-hosts"))
}

fn private_directory(path: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub(crate) enum LocalRunStartError {
    Unavailable,
    Busy,
    Agent(orchestral_runtime::AgentSdkError),
}

pub(crate) struct LocalRunRequest {
    pub run_id: String,
    pub input: String,
    pub reply: oneshot::Sender<Result<AgentRunHandle, LocalRunStartError>>,
}

pub(crate) struct LocalHostOwner {
    pub session: SessionView,
    requests: mpsc::Sender<LocalRunRequest>,
    stopped: CancellationToken,
}

impl LocalHostOwner {
    pub async fn start(
        &self,
        run_id: String,
        input: String,
    ) -> Result<AgentRunHandle, LocalRunStartError> {
        let (reply, result) = oneshot::channel();
        self.requests
            .try_send(LocalRunRequest {
                run_id,
                input,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => LocalRunStartError::Busy,
                mpsc::error::TrySendError::Closed(_) => LocalRunStartError::Unavailable,
            })?;
        result.await.map_err(|_| LocalRunStartError::Unavailable)?
    }
}

pub(crate) struct LocalHostServer {
    registration_path: PathBuf,
    registration: Registration,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    pub requests: mpsc::Receiver<LocalRunRequest>,
    pub model_requests: mpsc::Receiver<super::model_controls::LocalModelRequest>,
    pub models: Arc<super::model_controls::LocalModelControls>,
}

impl LocalHostServer {
    pub async fn start(host: &AgentHost, session_id: &AgentSessionId) -> anyhow::Result<Self> {
        Self::start_in(host, session_id, registry_root()?).await
    }

    async fn start_in(
        host: &AgentHost,
        session_id: &AgentSessionId,
        root: PathBuf,
    ) -> anyhow::Result<Self> {
        private_directory(&root)?;
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let ticket = PairingTicket::issue(60_000)?;
        let registry = RemoteRegistry::in_memory(Some(ticket.clone()));
        let credential = registry
            .claim_pairing(ticket.secret(), "Local Host gateway")
            .await?;
        let (sender, requests) = mpsc::channel(32);
        let (models, model_requests) = super::model_controls::LocalModelControls::new(host);
        let stop = CancellationToken::new();
        let now = chrono::Utc::now().timestamp_millis();
        let owner = Arc::new(LocalHostOwner {
            session: SessionView {
                id: session_id.as_str().to_owned(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                run_ids: Vec::new(),
                cwd: Some(host.workspace_root.to_string_lossy().into_owned()),
                execution_profile: host.execution_profile.clone(),
            },
            requests: sender,
            stopped: stop.clone(),
        });
        let state = RemoteApiState {
            agent: host.api.clone(),
            agent_directory: Arc::new(AgentDirectory::new()),
            native_session_defaults: NativeSessionDefaults {
                cwd: owner.session.cwd.clone(),
                execution_profile: host.execution_profile.clone(),
            },
            approvals: host.approvals.clone(),
            registry,
            gateway_authenticator: None,
            run_supervisors: Arc::default(),
            session_coordinators: Arc::default(),
            artifact_resolver: None,
            artifact_blob_store: None,
            local_hosts: None,
            local_owner: Some(owner),
            native_models: Some(Arc::new(super::model_controls::NativeModelControls::Local(
                models.clone(),
            ))),
        };
        let registration = Registration {
            version: 1,
            instance: uuid::Uuid::new_v4().to_string(),
            session_id: session_id.as_str().to_owned(),
            address: listener.local_addr()?,
            token: credential.token,
            active: true,
        };
        let path = root.join(format!("{}.json", registration.instance));
        write_registration(&path, &registration)?;
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router(state))
                .with_graceful_shutdown(stopped.cancelled_owned())
                .await
            {
                tracing::warn!(%error, "Local Host transport stopped");
            }
        });
        Ok(Self {
            registration_path: path,
            registration,
            stop,
            task,
            requests,
            model_requests,
            models,
        })
    }
}

impl Drop for LocalHostServer {
    fn drop(&mut self) {
        self.stop.cancel();
        // Keep an inactive route so a gateway restart cannot reinterpret this
        // Session as its own and start a different conversation under its ID.
        self.registration.active = false;
        let _ = write_registration(&self.registration_path, &self.registration);
        self.task.abort();
    }
}

fn write_registration(path: &Path, registration: &Registration) -> anyhow::Result<()> {
    use std::io::Write;
    let temp = path.with_extension(format!("{}.pending", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| {
        let mut file = options.open(&temp)?;
        file.write_all(&serde_json::to_vec(registration)?)?;
        std::fs::rename(&temp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

pub(crate) struct LocalHostDirectory {
    root: PathBuf,
    client: reqwest::Client,
    runs: RwLock<BTreeMap<String, Registration>>,
}

impl LocalHostDirectory {
    pub fn open() -> anyhow::Result<Self> {
        Self::at(registry_root()?)
    }

    fn at(root: PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            root,
            // Local control must never use the user's model/network proxy,
            // or follow redirects with a process capability token.
            client: reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(1))
                .build()?,
            runs: RwLock::new(BTreeMap::new()),
        })
    }

    async fn registrations(&self) -> anyhow::Result<Vec<Registration>> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let entries = match std::fs::read_dir(root) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(error.into()),
            };
            let mut records = Vec::new();
            for entry in entries {
                let entry = entry?;
                if !entry.file_type()?.is_file()
                    || entry.path().extension().is_none_or(|ext| ext != "json")
                {
                    continue;
                }
                if entry.metadata()?.len() > 16 * 1024 {
                    continue;
                }
                let bytes = match std::fs::read(entry.path()) {
                    Ok(bytes) if bytes.len() <= 16 * 1024 => bytes,
                    _ => continue,
                };
                let record: Registration = match serde_json::from_slice(&bytes) {
                    Ok(record) => record,
                    Err(_) => continue,
                };
                if record.validate().is_ok() {
                    records.push(record);
                }
            }
            Ok(records)
        })
        .await?
    }

    async fn probe(&self, owner: &Registration) -> anyhow::Result<SessionView> {
        if !owner.active {
            bail!("local Host has exited");
        }
        let response = self
            .client
            .get(format!("http://{}/sessions", owner.address))
            .bearer_auth(&owner.token)
            .send()
            .await?
            .error_for_status()?;
        let sessions: Vec<SessionView> = response.json().await?;
        sessions
            .into_iter()
            .find(|session| session.id == owner.session_id)
            .context("local Host no longer owns the registered session")
    }

    pub async fn sessions(&self) -> anyhow::Result<Vec<SessionView>> {
        let owners = self.registrations().await?;
        let found = stream::iter(owners.into_iter().filter(|owner| owner.active))
            .map(|owner| async move {
                let session = tokio::time::timeout(HEADER_TIMEOUT, self.probe(&owner))
                    .await
                    .ok()?
                    .ok()?;
                Some((owner, session))
            })
            .buffer_unordered(8)
            .filter_map(std::future::ready)
            .collect::<Vec<_>>()
            .await;
        let mut sessions = BTreeMap::new();
        let mut discovered_runs = BTreeMap::<String, Registration>::new();
        let mut runs = self.runs.write().await;
        for (owner, session) in found {
            for run in &session.run_ids {
                if discovered_runs
                    .get(run)
                    .or_else(|| runs.get(run))
                    .is_some_and(|previous| previous.session_id != owner.session_id)
                {
                    bail!("Run identity is claimed by multiple local Sessions");
                }
                discovered_runs.insert(run.clone(), owner.clone());
            }
            if sessions.insert(session.id.clone(), session).is_some() {
                bail!("multiple live Hosts claim the same session");
            }
        }
        runs.extend(discovered_runs);
        Ok(sessions.into_values().collect())
    }

    async fn session_owner(&self, session: &str) -> anyhow::Result<Option<Registration>> {
        let owners = self
            .registrations()
            .await?
            .into_iter()
            .filter(|owner| owner.session_id == session)
            .collect::<Vec<_>>();
        if owners.len() <= 1 {
            return Ok(owners.into_iter().next());
        }
        let mut live = None;
        for owner in owners.iter().filter(|owner| owner.active) {
            if tokio::time::timeout(HEADER_TIMEOUT, self.probe(owner))
                .await
                .is_ok_and(|result| result.is_ok())
                && live.replace(owner.clone()).is_some()
            {
                bail!("multiple live Hosts claim the same session");
            }
        }
        // Retain a dead owner as an unavailable route instead of starting
        // a second conversation under the same identity in this gateway.
        Ok(live.or_else(|| owners.into_iter().next()))
    }

    async fn owner(
        &self,
        session: Option<&str>,
        run: Option<&str>,
    ) -> anyhow::Result<Option<Registration>> {
        if let Some(session) = session {
            return self.session_owner(session).await;
        }
        if let Some(run) = run {
            // Refresh claims before commands too: newly resumed Hosts have new
            // capabilities, and ambiguous Run identities must fail closed.
            self.sessions().await?;
            return Ok(self.runs.read().await.get(run).cloned());
        }
        Ok(None)
    }

    async fn forward(&self, owner: &Registration, request: Request) -> anyhow::Result<Response> {
        if !owner.active {
            bail!("local Host has exited");
        }
        let path = request
            .uri()
            .path_and_query()
            .context("missing request path")?
            .as_str()
            .to_owned();
        let method = request.method().clone();
        let content_type = request.headers().get(header::CONTENT_TYPE).cloned();
        let body = to_bytes(request.into_body(), REQUEST_LIMIT).await?;
        let mut outgoing = self
            .client
            .request(method, format!("http://{}{path}", owner.address))
            .bearer_auth(&owner.token)
            .body(body);
        if let Some(content_type) = content_type {
            outgoing = outgoing.header(header::CONTENT_TYPE, content_type);
        }
        let response = tokio::time::timeout(HEADER_TIMEOUT, outgoing.send()).await??;
        let mut builder = Response::builder().status(response.status());
        for name in [header::CONTENT_TYPE, header::CACHE_CONTROL] {
            if let Some(value) = response.headers().get(&name) {
                builder = builder.header(name, value);
            }
        }
        // Forward the live stream without buffering it or imposing a total
        // inference deadline. Dropping the browser body drops the upstream.
        Ok(builder.body(Body::from_stream(response.bytes_stream()))?)
    }
}

fn unavailable(error: impl std::fmt::Display) -> Response {
    tracing::warn!(%error, "Local Host request unavailable");
    (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({
        "code": "local_host_unavailable",
        "message": "The terminal owning this session is unavailable. Reopen it with resume before retrying."
    }))).into_response()
}

/// Runs after public authentication. Internal bearer credentials never reach
/// the browser, and public credentials are never forwarded to a local process.
pub(super) async fn route_local_host(
    State(state): State<RemoteApiState>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_owned();
    let connector = reqwest::Url::parse(&format!("http://localhost{}", request.uri()))
        .ok()
        .is_some_and(|url| url.query_pairs().any(|(key, _)| key == "connector_id"));
    let (mut parts, body) = request.into_parts();
    let params = AxumPath::<BTreeMap<String, String>>::from_request_parts(&mut parts, &())
        .await
        .map(|params| params.0)
        .unwrap_or_default();
    let request = Request::from_parts(parts, body);
    let session = params.get("session_id").map(String::as_str);
    let run = params.get("run_id").map(String::as_str);
    if let Some(owner) = &state.local_owner {
        if owner.stopped.is_cancelled() {
            return unavailable("local Host has stopped");
        }
        let allowed = !connector
            && (path == "/sessions" && request.method() == axum::http::Method::GET
                || path.starts_with("/sessions/") && session == Some(owner.session.id.as_str())
                || path.starts_with("/runs/") && run.is_some());
        if !allowed {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({"code":"local_host_route_not_found","message":"This process only serves its current session."}))).into_response();
        }
        if let Some(run) = run {
            match state.agent.catalog_runs().await {
                Ok(runs)
                    if runs.iter().any(|entry| {
                        entry.run_id.as_str() == run
                            && entry.session_id.as_str() == owner.session.id
                    }) => {}
                _ => return StatusCode::NOT_FOUND.into_response(),
            }
        }
    }
    if !connector && (path.starts_with("/sessions/") || path.starts_with("/runs/")) {
        if let Some(directory) = &state.local_hosts {
            match directory.owner(session, run).await {
                Ok(Some(owner)) => {
                    if let Some(run) = run {
                        if state
                            .agent
                            .has_run(&orchestral_core::agent_protocol::wire::RunId::new(run))
                            .await
                            .unwrap_or(true)
                        {
                            return unavailable("Run identity is claimed by gateway and terminal");
                        }
                    }
                    return match directory.forward(&owner, request).await {
                        Ok(response) => response,
                        Err(error) => unavailable(error),
                    };
                }
                Ok(None) => {}
                Err(error) => return unavailable(error),
            }
        }
    }
    let response = next.run(request).await;
    if let Some(owner) = &state.local_owner {
        // Graceful HTTP shutdown alone waits forever for an open SSE body.
        // Retiring this owner must release every stream's controller/journal
        // references so switching away does not retain the session lease.
        let (parts, body) = response.into_parts();
        return Response::from_parts(
            parts,
            Body::from_stream(
                body.into_data_stream()
                    .take_until(owner.stopped.clone().cancelled_owned()),
            ),
        );
    }
    response
}

#[cfg(test)]
mod tests;
