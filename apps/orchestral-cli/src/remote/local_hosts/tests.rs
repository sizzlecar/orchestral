use super::*;
use axum::http::Request;
use axum::Router;
use http_body_util::BodyExt;
use orchestral_agent_protocol_testkit::{
    ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, TestProbes,
};
use orchestral_core::agent_protocol::wire::{ProviderBindingRef, RunId};
use orchestral_runtime::api::AgentApi;
use orchestral_runtime::{AgentController, InMemoryHostApprovalBroker};
use serde_json::{json, Value};
use tower::ServiceExt;

struct TestRoot(PathBuf);
impl TestRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("orch-host-test-{}", uuid::Uuid::new_v4()));
        private_directory(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn fixture() -> (RemoteApiState, String) {
    let factory = ScriptedStatelessFactory::conformant().unwrap();
    let provider = factory.create(
        ProviderScenario::standard(&factory.descriptor()).unwrap(),
        TestProbes::default(),
    );
    let controller = Arc::new(
        AgentController::new(provider, ProviderBindingRef::new("local-host-test")).unwrap(),
    );
    let ticket = PairingTicket::issue(60_000).unwrap();
    let registry = RemoteRegistry::in_memory(Some(ticket.clone()));
    let token = registry
        .claim_pairing(ticket.secret(), "fixture")
        .await
        .unwrap()
        .token;
    (
        RemoteApiState {
            agent: AgentApi::new(controller),
            agent_directory: Arc::new(AgentDirectory::new()),
            native_session_defaults: NativeSessionDefaults::default(),
            approvals: Arc::new(
                InMemoryHostApprovalBroker::new(b"0123456789abcdef0123456789abcdef").unwrap(),
            ),
            registry,
            gateway_authenticator: None,
            run_supervisors: Arc::default(),
            session_coordinators: Arc::default(),
            artifact_resolver: None,
            artifact_blob_store: None,
            local_hosts: None,
            local_owner: None,
        },
        token,
    )
}

struct OwnerFixture {
    state: RemoteApiState,
    record: Registration,
    started: mpsc::UnboundedReceiver<AgentRunHandle>,
    server: tokio::task::JoinHandle<()>,
    driver: tokio::task::JoinHandle<()>,
}
impl Drop for OwnerFixture {
    fn drop(&mut self) {
        self.server.abort();
        self.driver.abort();
    }
}

async fn owner(root: &Path, session_id: &str, api: Option<AgentApi>) -> OwnerFixture {
    let (mut state, token) = fixture().await;
    if let Some(api) = api {
        state.agent = api;
    }
    let (sender, mut requests) = mpsc::channel::<LocalRunRequest>(32);
    let (observed, started) = mpsc::unbounded_channel();
    state.local_owner = Some(Arc::new(LocalHostOwner {
        session: SessionView {
            id: session_id.to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            run_ids: Vec::new(),
            cwd: Some("/fixture/workspace".into()),
            execution_profile: Default::default(),
        },
        requests: sender,
        stopped: CancellationToken::new(),
    }));
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let record = Registration {
        version: 1,
        instance: uuid::Uuid::new_v4().to_string(),
        session_id: session_id.into(),
        address: listener.local_addr().unwrap(),
        token,
        active: true,
    };
    write_registration(&root.join(format!("{}.json", record.instance)), &record).unwrap();
    let api = state.agent.clone();
    let session = AgentSessionId::new(session_id);
    let driver = tokio::spawn(async move {
        while let Some(request) = requests.recv().await {
            let result = api
                .start_text(&session, Some(RunId::new(request.run_id)), request.input)
                .await;
            if let Ok(handle) = &result {
                let _ = observed.send(handle.clone());
            }
            let _ = request
                .reply
                .send(result.map_err(LocalRunStartError::Agent));
        }
    });
    let app = router(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    OwnerFixture {
        state,
        record,
        started,
        server,
        driver,
    }
}

async fn request(app: &Router, token: &str, method: &str, path: &str, body: Value) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn json_body(response: Response) -> Value {
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn gateway_discovers_idle_owner_and_forwards_start_history_and_sse_with_public_auth() {
    let root = TestRoot::new();
    let mut owner = owner(&root.0, "terminal-session", None).await;
    let (mut gateway, token) = fixture().await;
    let gateway_api = gateway.agent.clone();
    gateway.local_hosts = Some(Arc::new(LocalHostDirectory::at(root.0.clone()).unwrap()));
    // Exercise the same nested path used by the installed PWA gateway.
    let app = Router::new().nest("/api/v1", router(gateway));
    let denied = request(&app, "wrong", "GET", "/api/v1/sessions", Value::Null).await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let list = json_body(request(&app, &token, "GET", "/api/v1/sessions", Value::Null).await).await;
    assert_eq!(list[0]["id"], "terminal-session");
    assert_eq!(list[0]["cwd"], "/fixture/workspace");
    assert!(list[0]["run_ids"].as_array().unwrap().is_empty());
    assert!(!list.to_string().contains(&owner.record.token));
    let start = json!({"run_id":"remote-run", "input":"Continue this session"});
    for _ in 0..2 {
        let response = request(
            &app,
            &token,
            "POST",
            "/api/v1/sessions/terminal-session/runs",
            start.clone(),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "{}",
            json_body(response).await
        );
    }
    let handle = owner.started.recv().await.unwrap();
    assert_eq!(handle.run_id().as_str(), "remote-run");
    assert!(
        gateway_api.catalog_runs().await.unwrap().is_empty(),
        "gateway must never execute the owner's Run"
    );
    assert_eq!(owner.state.agent.catalog_runs().await.unwrap().len(), 1);
    let view = request(&app, &token, "GET", "/api/v1/runs/remote-run", Value::Null).await;
    assert_eq!(view.status(), StatusCode::OK);
    let events = request(
        &app,
        &token,
        "GET",
        "/api/v1/runs/remote-run/events?after=0",
        Value::Null,
    )
    .await;
    assert_eq!(events.status(), StatusCode::OK);
    assert!(!json_body(events).await["records"]
        .as_array()
        .unwrap()
        .is_empty());
    let stream = request(
        &app,
        &token,
        "GET",
        "/api/v1/runs/remote-run/stream?after=0",
        Value::Null,
    )
    .await;
    assert_eq!(stream.status(), StatusCode::OK);
    assert!(stream.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    let mut body = stream.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!frame.into_data().unwrap().is_empty());
    // Knowing the public token does not authenticate a request to a local owner.
    let denied = request(
        &router(owner.state.clone()),
        &token,
        "GET",
        "/sessions",
        Value::Null,
    )
    .await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let other = request(
        &router(owner.state.clone()),
        &owner.record.token,
        "POST",
        "/sessions/other/runs",
        start,
    )
    .await;
    assert_eq!(other.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cached_run_routes_to_resumed_owner_and_unavailable_owner_never_starts_in_gateway() {
    let root = TestRoot::new();
    let first = owner(&root.0, "resume-session", None).await;
    first
        .state
        .agent
        .create_session(Some(AgentSessionId::new("resume-session")))
        .await
        .unwrap();
    first
        .state
        .agent
        .start_text(
            &AgentSessionId::new("resume-session"),
            Some(RunId::new("saved-run")),
            "hello",
        )
        .await
        .unwrap();
    let directory = LocalHostDirectory::at(root.0.clone()).unwrap();
    assert_eq!(directory.sessions().await.unwrap().len(), 1);
    let api = first.state.agent.clone();
    let mut inactive = first.record.clone();
    inactive.active = false;
    write_registration(
        &root.0.join(format!("{}.json", inactive.instance)),
        &inactive,
    )
    .unwrap();
    drop(first);
    let (mut gateway, token) = fixture().await;
    let gateway_api = gateway.agent.clone();
    gateway.local_hosts = Some(Arc::new(directory));
    let app = router(gateway.clone());
    let response = request(
        &app,
        &token,
        "POST",
        "/sessions/resume-session/runs",
        json!({"run_id":"must-not-start", "input":"hello"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(gateway_api.catalog_runs().await.unwrap().is_empty());
    let second = owner(&root.0, "resume-session", Some(api)).await;
    let directory = gateway.local_hosts.as_ref().unwrap();
    assert_eq!(
        directory
            .owner(None, Some("saved-run"))
            .await
            .unwrap()
            .unwrap()
            .instance,
        second.record.instance
    );
    let response = request(&app, &token, "GET", "/runs/saved-run", Value::Null).await;
    assert_eq!(response.status(), StatusCode::OK);
    // Fresh gateway discovery does not depend on any in-memory cache.
    assert_eq!(
        LocalHostDirectory::at(root.0.clone())
            .unwrap()
            .sessions()
            .await
            .unwrap()[0]
            .run_ids,
        vec!["saved-run"]
    );
}

#[tokio::test]
async fn duplicate_live_owners_and_non_loopback_registrations_are_rejected() {
    let root = TestRoot::new();
    let first = owner(&root.0, "same", None).await;
    let _second = owner(&root.0, "same", None).await;
    let directory = LocalHostDirectory::at(root.0.clone()).unwrap();
    assert!(directory.sessions().await.is_err());
    assert!(directory.session_owner("same").await.is_err());
    let mut malicious = first.record.clone();
    malicious.address = "203.0.113.1:80".parse().unwrap();
    assert!(malicious.validate().is_err());
    write_registration(&root.0.join("invalid.json"), &malicious).unwrap();
    assert_eq!(directory.registrations().await.unwrap().len(), 2);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&root.0).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(root.0.join("invalid.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn colliding_run_ids_cannot_route_a_command_to_another_session() {
    let root = TestRoot::new();
    let first = owner(&root.0, "first", None).await;
    let second = owner(&root.0, "second", None).await;
    for (owner, session) in [(&first, "first"), (&second, "second")] {
        let session = AgentSessionId::new(session);
        owner
            .state
            .agent
            .create_session(Some(session.clone()))
            .await
            .unwrap();
        owner
            .state
            .agent
            .start_text(&session, Some(RunId::new("colliding-run")), "hello")
            .await
            .unwrap();
    }
    let directory = LocalHostDirectory::at(root.0.clone()).unwrap();
    assert!(directory.owner(None, Some("colliding-run")).await.is_err());
}
