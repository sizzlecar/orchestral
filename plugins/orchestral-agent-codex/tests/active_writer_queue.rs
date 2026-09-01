#![cfg(unix)]

use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orchestral_agent_codex::{CodexAppServerConfig, CodexAppServerEndpoint, CodexConnector};
use orchestral_core::agent_connector::AgentConnectorId;
use orchestral_core::agent_protocol::reference::AgentRunStatus;
use orchestral_core::agent_protocol::wire::{AgentSessionId, ContentBody};
use orchestral_runtime::AgentDirectory;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;

const QUEUED_RESPONSE: &str = "ORCHESTRAL_QUEUED_TURN_OK";

/// Real compatibility check for the ownership boundary that a protocol fake
/// cannot prove: one Codex app-server owns the rollout writer while a second
/// app-server, used by Orchestral, submits through Codex's durable queue.
///
/// The test uses an isolated CODEX_HOME and a local Responses API stub. It
/// never reads or writes the developer's real Codex sessions and spends no
/// model quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an installed Codex CLI with thread/queue experimental APIs"]
async fn queues_into_a_thread_owned_by_another_codex_process() {
    let codex = find_codex().expect("set CODEX_BIN or put codex on PATH");
    let fixture = TempDir::new().expect("create isolated Codex fixture");
    let (base_url, model_requests, mock_task) = start_responses_stub().await;
    write_codex_config(fixture.path(), &base_url);
    let wrapper = write_codex_wrapper(fixture.path(), &codex);

    let mut owner = AppServerClient::start(&wrapper).await;
    owner.initialize().await;
    let started = owner
        .request(
            "thread/start",
            json!({"cwd": fixture.path().to_string_lossy()}),
        )
        .await;
    let thread_id = started
        .pointer("/result/thread/id")
        .and_then(Value::as_str)
        .expect("Codex thread/start must return thread.id")
        .to_owned();
    owner
        .request(
            "turn/start",
            json!({
                "threadId": thread_id,
                "input": [{"type": "text", "text": "owner turn"}],
                "clientUserMessageId": "owner-first-turn"
            }),
        )
        .await;

    let writer_lock = fixture
        .path()
        .join("thread-writer-locks")
        .join(format!("{thread_id}.lock"));
    wait_for_path(&writer_lock).await;

    let connector = Arc::new(CodexConnector::new(CodexAppServerConfig {
        executable: wrapper,
        endpoint: CodexAppServerEndpoint::PrivateStdio,
        dispatch_journal_dir: Some(fixture.path().join("orchestral-dispatch")),
        request_timeout: Duration::from_secs(10),
        max_frame_bytes: 16 * 1024 * 1024,
        daemon_start_timeout: Duration::from_secs(2),
    }));
    let directory = AgentDirectory::new();
    directory
        .register(connector.clone(), connector)
        .await
        .expect("register isolated Codex connector");
    let session_id = AgentSessionId::new(thread_id.clone());
    let handle = directory
        .start_text(
            &AgentConnectorId::new("codex/local"),
            &session_id,
            None,
            "queued turn from Orchestral",
        )
        .await
        .expect("active writer must route through the durable queue");
    let outcome = tokio::time::timeout(Duration::from_secs(30), handle.wait_until_blocked())
        .await
        .expect("owner did not consume the queued turn")
        .expect("queued Orchestral turn failed");

    assert_eq!(outcome.view.state.status(), AgentRunStatus::Delivered);
    let delivery = outcome.view.delivery.expect("queued turn must deliver");
    let response = match delivery.final_response.body {
        ContentBody::Inline(Value::String(text)) => text,
        other => panic!("unexpected queued delivery body: {other:?}"),
    };
    assert_eq!(response, QUEUED_RESPONSE);
    assert!(
        writer_lock.exists(),
        "the original owner must retain its lock"
    );
    assert_eq!(model_requests.load(Ordering::SeqCst), 2);

    let queue = owner
        .request("thread/queue/list", json!({"threadId": thread_id}))
        .await;
    assert!(queue
        .pointer("/result/data")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty));

    mock_task.abort();
}

struct AppServerClient {
    _child: Child,
    stdin: ChildStdin,
    incoming: mpsc::UnboundedReceiver<Value>,
    next_id: u64,
}

impl AppServerClient {
    async fn start(executable: &Path) -> Self {
        let mut child = Command::new(executable)
            .args(["app-server", "--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start owner Codex app-server");
        let stdin = child.stdin.take().expect("owner stdin");
        let stdout = child.stdout.take().expect("owner stdout");
        let (sender, incoming) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Ok(message) = serde_json::from_str(&line) {
                    let _ = sender.send(message);
                }
            }
        });
        Self {
            _child: child,
            stdin,
            incoming,
            next_id: 1,
        }
    }

    async fn initialize(&mut self) {
        let response = self
            .request(
                "initialize",
                json!({
                    "clientInfo": {"name": "orchestral-owner-e2e", "version": "0"},
                    "capabilities": {"experimentalApi": true}
                }),
            )
            .await;
        assert!(response.get("error").is_none(), "{response}");
        self.notify("initialized", json!({})).await;
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.stdin
            .write_all(
                format!(
                    "{}\n",
                    json!({"id": id, "method": method, "params": params})
                )
                .as_bytes(),
            )
            .await
            .expect("write owner request");
        loop {
            let message = tokio::time::timeout(Duration::from_secs(20), self.incoming.recv())
                .await
                .expect("owner app-server response timed out")
                .expect("owner app-server closed");
            if message.get("id").and_then(Value::as_u64) == Some(id) {
                assert!(message.get("error").is_none(), "{method} failed: {message}");
                return message;
            }
        }
    }

    async fn notify(&mut self, method: &str, params: Value) {
        self.stdin
            .write_all(format!("{}\n", json!({"method": method, "params": params})).as_bytes())
            .await
            .expect("write owner notification");
    }
}

async fn start_responses_stub() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Responses API stub");
    let address = listener.local_addr().expect("stub address");
    let requests = Arc::new(AtomicUsize::new(0));
    let request_counter = Arc::clone(&requests);
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let index = request_counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(serve_response(stream, index));
        }
    });
    (format!("http://{address}/v1"), requests, task)
}

async fn serve_response(mut stream: TcpStream, index: usize) {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = stream.read(&mut buffer).await.expect("read stub request");
        if read == 0 {
            return;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let delay = if index == 0 {
        Duration::from_millis(900)
    } else {
        // Force the observer to see Codex's non-owner `interrupted`/null
        // snapshot before the queued turn reaches its durable completion.
        Duration::from_millis(2_500)
    };
    tokio::time::sleep(delay).await;
    let response_text = if index == 0 {
        "OWNER_FIRST_TURN_OK"
    } else {
        QUEUED_RESPONSE
    };
    let response_id = format!("resp-{}", index + 1);
    let item_id = format!("msg-{}", index + 1);
    let events = [
        json!({"type": "response.created", "response": {"id": response_id}}),
        json!({
            "type": "response.output_item.added",
            "item": {
                "type": "message",
                "role": "assistant",
                "id": item_id,
                "content": [{"type": "output_text", "text": ""}]
            }
        }),
        json!({"type": "response.output_text.delta", "delta": response_text}),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "message",
                "role": "assistant",
                "id": item_id,
                "content": [{"type": "output_text", "text": response_text}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": response_id,
                "usage": {
                    "input_tokens": 0,
                    "input_tokens_details": null,
                    "output_tokens": 0,
                    "output_tokens_details": null,
                    "total_tokens": 0
                }
            }
        }),
    ];
    let body = events
        .into_iter()
        .map(|event| {
            format!(
                "event: {}\ndata: {event}\n\n",
                event["type"].as_str().unwrap()
            )
        })
        .collect::<String>();
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(format!("{headers}{body}").as_bytes())
        .await
        .expect("write stub response");
}

fn write_codex_config(codex_home: &Path, base_url: &str) {
    fs::write(
        codex_home.join("config.toml"),
        format!(
            "model = \"gpt-5.4\"\nmodel_provider = \"mock\"\napproval_policy = \"never\"\n[model_providers.mock]\nname = \"mock\"\nbase_url = \"{base_url}\"\nenv_key = \"OPENAI_API_KEY\"\nwire_api = \"responses\"\n"
        ),
    )
    .expect("write isolated Codex config");
}

fn write_codex_wrapper(codex_home: &Path, codex: &Path) -> PathBuf {
    let wrapper = codex_home.join("codex-e2e");
    fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nexport CODEX_HOME={}\nexport OPENAI_API_KEY=dummy\nexec {} \"$@\"\n",
            shell_quote(codex_home),
            shell_quote(codex)
        ),
    )
    .expect("write Codex wrapper");
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755))
        .expect("make Codex wrapper executable");
    wrapper
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

fn find_codex() -> Option<PathBuf> {
    env::var_os("CODEX_BIN")
        .map(PathBuf::from)
        .filter(|path| path.is_file())
        .or_else(|| {
            env::var_os("PATH").and_then(|path| {
                env::split_paths(&path)
                    .map(|directory| directory.join("codex"))
                    .find(|candidate| candidate.is_file())
            })
        })
}

async fn wait_for_path(path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Codex did not acquire writer lock at {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
