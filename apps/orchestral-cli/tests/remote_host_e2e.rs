use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use orchestral_cli::remote::{PairingTicket, RemoteRegistry};

struct TestRoot {
    path: PathBuf,
}

impl TestRoot {
    fn new() -> Self {
        let nonce = uuid::Uuid::new_v4();
        let path = std::env::temp_dir().join(format!(
            "orchestral-remote-host-e2e-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn released_binary_serves_embedded_pwa_and_authenticated_host_api() {
    let root = TestRoot::new();
    let config = root.path.join("orchestral.yaml");
    let source = fs::read_to_string(repository_root().join("configs/orchestral.cli.yaml")).unwrap();
    fs::write(&config, source).unwrap();

    let state_file = root.path.join("remote-control.json");
    let ticket = PairingTicket::issue(60_000).unwrap();
    let secret = ticket.secret().to_owned();
    let registry = RemoteRegistry::open(&state_file, Some(ticket)).unwrap();
    let claim = registry.claim_pairing(&secret, "E2E phone").await.unwrap();

    let address = reserve_address();
    let child = Command::new(env!("CARGO_BIN_EXE_orchestral"))
        .current_dir(&root.path)
        // The released Host opens its Agent journals with a single-writer
        // lease. E2E state must never alias a developer's running Host.
        .env("ORCHESTRAL_HOME", root.path.join("orchestral-home"))
        .env("OPENAI_API_KEY", "fixture-key")
        .args([
            "serve",
            "--listen",
            &address.to_string(),
            "--state-file",
            state_file.to_str().unwrap(),
            "--config",
            config.to_str().unwrap(),
            "--backend",
            "openai",
            "--model",
            "fixture-model",
            "--no-mcp",
            "--no-skills",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);

    let health = wait_for_response(address, "GET", "/api/v1/health", &[], "", &mut child.0);
    assert_eq!(health.status, 200, "{}", health.body);
    assert!(health.body.contains("orchestral-remote-v1"));

    let shell = request(address, "GET", "/", &[], "");
    assert_eq!(shell.status, 200);
    assert!(shell.headers.contains("content-security-policy:"));
    assert!(shell.body.contains("manifest.webmanifest"));

    let sessions = request(
        address,
        "POST",
        "/api/v1/sessions",
        &[
            ("Authorization", &format!("Bearer {}", claim.token)),
            ("Content-Type", "application/json"),
        ],
        r#"{"session_id":"mobile-e2e-session"}"#,
    );
    assert_eq!(sessions.status, 201, "{}", sessions.body);
    assert!(sessions.body.contains("mobile-e2e-session"));
}

struct HttpResponse {
    status: u16,
    headers: String,
    body: String,
}

#[tokio::test]
async fn gateway_model_controls_are_session_scoped_and_recover_from_run_provenance() {
    use serde_json::{json, Value};
    let root = TestRoot::new();
    let config = root.path.join("orchestral.yaml");
    fs::write(
        &config,
        fs::read_to_string(repository_root().join("configs/orchestral.cli.yaml")).unwrap(),
    )
    .unwrap();
    let state_file = root.path.join("remote-control.json");
    let ticket = PairingTicket::issue(60_000).unwrap();
    let registry = RemoteRegistry::open(&state_file, Some(ticket.clone())).unwrap();
    let claim = registry
        .claim_pairing(ticket.secret(), "Model settings phone")
        .await
        .unwrap();
    let model_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1", model_listener.local_addr().unwrap());
    model_listener.set_nonblocking(true).unwrap();
    let model_server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(40);
        let mut calls = 0;
        while calls < 4 {
            assert!(Instant::now() < deadline, "waiting for fixture requests");
            let (mut stream, _) = match model_listener.accept() {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("{error}"),
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let (header_end, length) = loop {
                let count = stream.read(&mut chunk).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let text = String::from_utf8_lossy(&bytes[..end]);
                    let length = text
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                        .unwrap_or(0);
                    break (end + 4, length);
                }
            };
            while bytes.len() < header_end + length {
                let count = stream.read(&mut chunk).unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
            }
            let body = if length == 0 {
                json!({"data":[{"id":"fixture-model"},{"id":"alternate-model"}]})
            } else {
                let request: Value =
                    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
                if calls == 2 {
                    assert_eq!(request["model"], "fixture-model");
                    assert!(request.get("reasoning_effort").is_none());
                } else {
                    assert_eq!(request["model"], "alternate-model");
                    assert_eq!(request["reasoning_effort"], "xhigh");
                }
                if calls == 3 {
                    assert!(request["messages"].to_string().contains("answer-1"));
                    assert!(!request["messages"].to_string().contains("answer-2"));
                }
                let response = if calls == 0 {
                    json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"model-approval","function":{"name":"exec_command","arguments":json!({"cmd":"echo model-approval-ok","sandbox_permissions":"require_escalated","justification":"Run the requested local fixture","wait_mode":"completion","yield_time_ms":1}).to_string()}}]},"finish_reason":"tool_calls"}]})
                } else {
                    json!({"choices":[{"delta":{"content":format!("answer-{calls}")},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":4}})
                };
                calls += 1;
                response
            };
            let encoded = if length == 0 {
                serde_json::to_vec(&body).unwrap()
            } else {
                format!("data: {body}\n\ndata: [DONE]\n\n").into_bytes()
            };
            let content_type = if length == 0 {
                "application/json"
            } else {
                "text/event-stream"
            };
            write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",encoded.len()).unwrap();
            stream.write_all(&encoded).unwrap();
        }
    });
    let address = reserve_address();
    let start_host = || {
        ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_orchestral"))
                .current_dir(&root.path)
                .env("ORCHESTRAL_HOME", root.path.join("home"))
                .env("OPENAI_API_KEY", "fixture-key")
                .args([
                    "serve",
                    "--listen",
                    &address.to_string(),
                    "--state-file",
                    state_file.to_str().unwrap(),
                    "--config",
                    config.to_str().unwrap(),
                    "--backend",
                    "openai",
                    "--model",
                    "fixture-model",
                    "--base-url",
                    &endpoint,
                    "--no-auth",
                    "--no-mcp",
                    "--no-skills",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        )
    };
    let mut child = start_host();
    wait_for_response(address, "GET", "/api/v1/health", &[], "", &mut child.0);
    let authorization = format!("Bearer {}", claim.token);
    let headers = [
        ("Authorization", authorization.as_str()),
        ("Content-Type", "application/json"),
    ];
    let call = |method: &str, path: &str, body: Value| {
        let response = try_request(address, method, path, &headers, &body.to_string())
            .unwrap_or_else(|error| panic!("{method} {path}: {error}"));
        assert!(
            response.status < 300,
            "HTTP {}: {}",
            response.status,
            response.body
        );
        serde_json::from_str::<Value>(&response.body).unwrap()
    };
    for id in ["session-a", "session-b"] {
        call("POST", "/api/v1/sessions", json!({"session_id":id}));
    }
    let path = "/api/v1/sessions/session-a/model-settings";
    call(
        "POST",
        path,
        json!({"model":"alternate-model","expected_model":"fixture-model"}),
    );
    call(
        "POST",
        path,
        json!({"reasoning_effort":"xhigh","expected_model":"alternate-model"}),
    );
    let original = call(
        "GET",
        "/api/v1/sessions/session-b/model-settings",
        Value::Null,
    );
    assert_eq!(original["profile"]["model"], "fixture-model");
    let wait_done = |id: &str| {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let view = call("GET", &format!("/api/v1/runs/{id}"), Value::Null);
            if view["state"]["state"] == "terminal" {
                assert_eq!(view["state"]["terminal"]["type"], "delivered", "{view}");
                break;
            }
            assert!(Instant::now() < deadline, "{view}");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    for (session, run) in [("session-a", "run-a"), ("session-b", "run-b")] {
        call(
            "POST",
            &format!("/api/v1/sessions/{session}/runs"),
            json!({"run_id":run,"input":format!("Input for {session}")}),
        );
        if session == "session-a" {
            let deadline = Instant::now() + Duration::from_secs(15);
            let request_id = loop {
                let view = call("GET", &format!("/api/v1/runs/{run}"), Value::Null);
                if let Some(id) = view["pending_requests"][0]["request_id"].as_str() {
                    break id.to_owned();
                }
                assert!(Instant::now() < deadline, "approval did not open: {view}");
                std::thread::sleep(Duration::from_millis(20));
            };
            call(
                "POST",
                &format!("/api/v1/runs/{run}/requests/{request_id}/approval"),
                json!({"command_id":"approve-selected-model","decision":"allow_once"}),
            );
        }
        wait_done(run);
    }
    drop(child);
    let mut child = start_host();
    wait_for_response(address, "GET", "/api/v1/health", &[], "", &mut child.0);
    let recovered = call("GET", path, Value::Null);
    assert_eq!(recovered["profile"]["model"], "alternate-model");
    assert_eq!(recovered["profile"]["reasoning_effort"], "xhigh");
    call(
        "POST",
        "/api/v1/sessions/session-a/runs",
        json!({"run_id":"run-a2","input":"Continue from the recorded answer"}),
    );
    wait_done("run-a2");
    drop(child);
    model_server.join().unwrap();
}

fn wait_for_response(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
    child: &mut Child,
) -> HttpResponse {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(stream) = child.stderr.as_mut() {
                stream.read_to_string(&mut stderr).unwrap();
            }
            panic!("Host gateway exited before it became ready ({status}): {stderr}");
        }
        if let Ok(response) = try_request(address, method, path, headers, body) {
            return response;
        }
        assert!(
            Instant::now() < deadline,
            "Host gateway did not become ready"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> HttpResponse {
    try_request(address, method, path, headers, body).unwrap()
}

fn try_request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> std::io::Result<HttpResponse> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(250))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, body) = response.split_once("\r\n\r\n").unwrap_or((&response, ""));
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .unwrap_or_default();
    Ok(HttpResponse {
        status,
        headers: headers.to_ascii_lowercase(),
        body: body.to_owned(),
    })
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
