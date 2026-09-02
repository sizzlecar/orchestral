use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use orchestral_cli::remote::{PairingTicket, RemoteRegistry};

struct TestRoot {
    path: PathBuf,
}

impl TestRoot {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
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
