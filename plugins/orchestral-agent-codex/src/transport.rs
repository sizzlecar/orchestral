use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
#[cfg(unix)]
use futures_util::stream::{SplitSink, SplitStream};
#[cfg(unix)]
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::io::{
    self, AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader,
};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, oneshot, Mutex};
use tokio::time::{sleep, timeout, Instant};
#[cfg(unix)]
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
#[cfg(unix)]
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
#[cfg(unix)]
use tokio_tungstenite::{client_async_with_config, WebSocketStream};

// `thread/read(includeTurns=true)` is one JSON-RPC frame and Codex currently
// offers no turn pagination. Long-lived coding sessions can legitimately
// exceed 64 MiB before Orchestral applies its bounded normalization.
const DEFAULT_MAX_FRAME_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_DAEMON_START_TIMEOUT: Duration = Duration::from_secs(5);

/// Selects how the connector reaches Codex's app-server control plane.
///
/// A shared daemon is the safe default: every UI and remote controller talks
/// to the same in-memory thread manager, so attaching to a running thread does
/// not create a competing rollout writer. Private stdio remains available as
/// an explicit compatibility mode for older Codex releases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexAppServerEndpoint {
    SharedDaemon {
        socket_path: PathBuf,
        auto_start: bool,
    },
    PrivateStdio,
}

#[cfg(unix)]
async fn start_shared_daemon(config: &CodexAppServerConfig) -> Result<(), CodexTransportError> {
    let child = Command::new(&config.executable)
        .args(["app-server", "daemon", "start"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(CodexTransportError::Spawn)?;
    let output = timeout(config.daemon_start_timeout, child.wait_with_output())
        .await
        .map_err(|_| CodexTransportError::DaemonStart("daemon start command timed out".to_owned()))?
        .map_err(CodexTransportError::Io)?;
    if output.status.success() {
        return Ok(());
    }
    let detail = if output.stderr.is_empty() {
        String::from_utf8_lossy(&output.stdout)
    } else {
        String::from_utf8_lossy(&output.stderr)
    };
    Err(CodexTransportError::DaemonStart(
        detail.chars().take(2048).collect(),
    ))
}

#[derive(Debug, Clone)]
pub struct CodexAppServerConfig {
    pub executable: PathBuf,
    pub endpoint: CodexAppServerEndpoint,
    pub request_timeout: Duration,
    pub max_frame_bytes: usize,
    pub daemon_start_timeout: Duration,
}

impl Default for CodexAppServerConfig {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("codex"),
            endpoint: default_endpoint(),
            request_timeout: Duration::from_secs(30),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            daemon_start_timeout: DEFAULT_DAEMON_START_TIMEOUT,
        }
    }
}

fn default_endpoint() -> CodexAppServerEndpoint {
    #[cfg(unix)]
    {
        let codex_home = std::env::var_os("CODEX_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .filter(|value| !value.is_empty())
                    .map(PathBuf::from)
                    .map(|home| home.join(".codex"))
            })
            .unwrap_or_else(|| PathBuf::from(".codex"));
        CodexAppServerEndpoint::SharedDaemon {
            socket_path: codex_home
                .join("app-server-control")
                .join("app-server-control.sock"),
            auto_start: true,
        }
    }
    #[cfg(not(unix))]
    {
        CodexAppServerEndpoint::PrivateStdio
    }
}

#[derive(Debug, Error)]
pub enum CodexTransportError {
    #[error("failed to start Codex app-server: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("Codex app-server I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("failed to connect to Codex shared app-server at {socket}: {message}")]
    SharedDaemonConnect { socket: PathBuf, message: String },
    #[error("failed to start Codex shared app-server daemon: {0}")]
    DaemonStart(String),
    #[error("Codex app-server WebSocket failed: {0}")]
    WebSocket(String),
    #[error("Codex app-server returned invalid JSON: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("Codex app-server frame exceeded {limit} bytes")]
    FrameTooLarge { limit: usize },
    #[error("Codex app-server closed the connection")]
    Closed,
    #[error("Codex app-server disconnected: {0}")]
    Disconnected(String),
    #[error("Codex app-server request timed out")]
    Timeout,
    #[error("Codex app-server rejected the request: {0}")]
    Rpc(String),
    #[error("Codex app-server response did not contain a result")]
    MissingResult,
}

type RpcReply = Result<Value, CodexTransportError>;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<RpcReply>>>>;

#[async_trait]
trait RpcWriter: Send {
    async fn send_json(&mut self, message: &Value) -> Result<(), CodexTransportError>;

    async fn send_pong(&mut self, _payload: Vec<u8>) -> Result<(), CodexTransportError> {
        Ok(())
    }
}

struct JsonLineWriter {
    inner: Box<dyn AsyncWrite + Send + Unpin>,
}

#[async_trait]
impl RpcWriter for JsonLineWriter {
    async fn send_json(&mut self, message: &Value) -> Result<(), CodexTransportError> {
        let mut bytes = serde_json::to_vec(message).map_err(CodexTransportError::InvalidJson)?;
        bytes.push(b'\n');
        self.inner
            .write_all(&bytes)
            .await
            .map_err(CodexTransportError::Io)?;
        self.inner.flush().await.map_err(CodexTransportError::Io)
    }
}

#[cfg(unix)]
struct WebSocketWriter {
    inner: SplitSink<WebSocketStream<UnixStream>, WebSocketMessage>,
}

#[cfg(unix)]
#[async_trait]
impl RpcWriter for WebSocketWriter {
    async fn send_json(&mut self, message: &Value) -> Result<(), CodexTransportError> {
        let text = serde_json::to_string(message).map_err(CodexTransportError::InvalidJson)?;
        self.inner
            .send(WebSocketMessage::Text(text.into()))
            .await
            .map_err(|error| CodexTransportError::WebSocket(error.to_string()))
    }

    async fn send_pong(&mut self, payload: Vec<u8>) -> Result<(), CodexTransportError> {
        self.inner
            .send(WebSocketMessage::Pong(payload.into()))
            .await
            .map_err(|error| CodexTransportError::WebSocket(error.to_string()))
    }
}

type DynWriter = Box<dyn RpcWriter>;

#[derive(Debug, Clone)]
pub(crate) enum CodexTransportEvent {
    Message(Value),
    Disconnected { reason: String },
}

pub struct CodexRpcClient {
    writer: Arc<Mutex<DynWriter>>,
    pending: Pending,
    notifications: broadcast::Sender<CodexTransportEvent>,
    connected: Arc<AtomicBool>,
    next_id: Mutex<u64>,
    request_timeout: Duration,
    _child: Option<Arc<Mutex<Child>>>,
}

impl CodexRpcClient {
    pub async fn connect(
        config: &CodexAppServerConfig,
    ) -> Result<(Arc<Self>, String), CodexTransportError> {
        let client = match &config.endpoint {
            CodexAppServerEndpoint::PrivateStdio => Self::spawn_private(config).await?,
            CodexAppServerEndpoint::SharedDaemon {
                socket_path,
                auto_start,
            } => Self::connect_shared(config, socket_path, *auto_start).await?,
        };
        let initialized = client
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "orchestral",
                        "title": "Orchestral",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            )
            .await?;
        client.notify("initialized", json!({})).await?;
        let user_agent = initialized
            .get("userAgent")
            .and_then(Value::as_str)
            .unwrap_or("codex")
            .to_owned();
        Ok((client, user_agent))
    }

    async fn spawn_private(
        config: &CodexAppServerConfig,
    ) -> Result<Arc<Self>, CodexTransportError> {
        let mut child = Command::new(&config.executable)
            .args(["app-server", "--listen", "stdio://"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(CodexTransportError::Spawn)?;
        let stdin = child.stdin.take().ok_or(CodexTransportError::Closed)?;
        let stdout = child.stdout.take().ok_or(CodexTransportError::Closed)?;
        if let Some(mut stderr) = child.stderr.take() {
            tokio::spawn(async move {
                // app-server owns a long-lived stderr pipe. Draining a single
                // line can eventually fill the OS pipe and deadlock Codex
                // while stdout still appears healthy to the Host.
                let _ = io::copy(&mut stderr, &mut io::sink()).await;
            });
        }
        Ok(Self::from_io_with_child(
            stdout,
            stdin,
            config.request_timeout,
            config.max_frame_bytes,
            Some(child),
        ))
    }

    #[cfg(unix)]
    async fn connect_shared(
        config: &CodexAppServerConfig,
        socket_path: &PathBuf,
        auto_start: bool,
    ) -> Result<Arc<Self>, CodexTransportError> {
        if let Ok(client) = Self::connect_shared_once(config, socket_path).await {
            return Ok(client);
        }
        if !auto_start {
            return Self::connect_shared_once(config, socket_path).await;
        }

        start_shared_daemon(config).await?;
        let deadline = Instant::now() + config.daemon_start_timeout;
        loop {
            let error = match Self::connect_shared_once(config, socket_path).await {
                Ok(client) => return Ok(client),
                Err(error) => error,
            };
            if Instant::now() >= deadline {
                return Err(error);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    #[cfg(not(unix))]
    async fn connect_shared(
        _config: &CodexAppServerConfig,
        socket_path: &PathBuf,
        _auto_start: bool,
    ) -> Result<Arc<Self>, CodexTransportError> {
        Err(CodexTransportError::SharedDaemonConnect {
            socket: socket_path.clone(),
            message: "Unix control sockets are unavailable on this platform; configure PrivateStdio explicitly"
                .to_owned(),
        })
    }

    #[cfg(unix)]
    async fn connect_shared_once(
        config: &CodexAppServerConfig,
        socket_path: &PathBuf,
    ) -> Result<Arc<Self>, CodexTransportError> {
        let stream = UnixStream::connect(socket_path).await.map_err(|error| {
            CodexTransportError::SharedDaemonConnect {
                socket: socket_path.clone(),
                message: error.to_string(),
            }
        })?;
        let websocket_config = WebSocketConfig::default()
            .max_message_size(Some(config.max_frame_bytes))
            .max_frame_size(Some(config.max_frame_bytes));
        let handshake = timeout(
            config.request_timeout,
            client_async_with_config("ws://localhost/", stream, Some(websocket_config)),
        )
        .await
        .map_err(|_| CodexTransportError::Timeout)?
        .map_err(|error| CodexTransportError::SharedDaemonConnect {
            socket: socket_path.clone(),
            message: error.to_string(),
        })?;
        Ok(Self::from_websocket(
            handshake.0,
            config.request_timeout,
            config.max_frame_bytes,
        ))
    }

    #[cfg(test)]
    pub(crate) fn from_io<R, W>(
        reader: R,
        writer: W,
        request_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Arc<Self>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Self::from_io_with_child(reader, writer, request_timeout, max_frame_bytes, None)
    }

    fn from_io_with_child<R, W>(
        reader: R,
        writer: W,
        request_timeout: Duration,
        max_frame_bytes: usize,
        child: Option<Child>,
    ) -> Arc<Self>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (notifications, _) = broadcast::channel(256);
        let connected = Arc::new(AtomicBool::new(true));
        let writer: Arc<Mutex<DynWriter>> = Arc::new(Mutex::new(Box::new(JsonLineWriter {
            inner: Box::new(writer),
        })));
        let client = Arc::new(Self {
            writer,
            pending: Arc::clone(&pending),
            notifications: notifications.clone(),
            connected: Arc::clone(&connected),
            next_id: Mutex::new(1),
            request_timeout,
            _child: child.map(|child| Arc::new(Mutex::new(child))),
        });
        tokio::spawn(read_loop(
            BufReader::new(reader),
            pending,
            notifications,
            connected,
            max_frame_bytes,
        ));
        client
    }

    #[cfg(unix)]
    fn from_websocket(
        websocket: WebSocketStream<UnixStream>,
        request_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Arc<Self> {
        let (sink, stream) = websocket.split();
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (notifications, _) = broadcast::channel(256);
        let connected = Arc::new(AtomicBool::new(true));
        let writer: Arc<Mutex<DynWriter>> =
            Arc::new(Mutex::new(Box::new(WebSocketWriter { inner: sink })));
        let client = Arc::new(Self {
            writer: Arc::clone(&writer),
            pending: Arc::clone(&pending),
            notifications: notifications.clone(),
            connected: Arc::clone(&connected),
            next_id: Mutex::new(1),
            request_timeout,
            _child: None,
        });
        tokio::spawn(websocket_read_loop(
            stream,
            writer,
            pending,
            notifications,
            connected,
            max_frame_bytes,
        ));
        client
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<CodexTransportEvent> {
        self.notifications.subscribe()
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub(crate) async fn respond(
        &self,
        id: Value,
        result: Value,
    ) -> Result<(), CodexTransportError> {
        self.write(&json!({"id": id, "result": result})).await
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value, CodexTransportError> {
        let id = {
            let mut next = self.next_id.lock().await;
            let id = *next;
            *next = next.saturating_add(1);
            id
        };
        let key = id.to_string();
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(key.clone(), sender);
        if let Err(error) = self
            .write(&json!({"method": method, "id": id, "params": params}))
            .await
        {
            self.pending.lock().await.remove(&key);
            return Err(error);
        }
        match timeout(self.request_timeout, receiver).await {
            Ok(Ok(Ok(result))) => Ok(result),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(CodexTransportError::Closed),
            Err(_) => {
                self.pending.lock().await.remove(&key);
                Err(CodexTransportError::Timeout)
            }
        }
    }

    pub async fn notify(&self, method: &str, params: Value) -> Result<(), CodexTransportError> {
        self.write(&json!({"method": method, "params": params}))
            .await
    }

    async fn write(&self, message: &Value) -> Result<(), CodexTransportError> {
        if !self.is_connected() {
            return Err(CodexTransportError::Closed);
        }
        let mut writer = self.writer.lock().await;
        if let Err(error) = writer.send_json(message).await {
            self.connected.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }
}

async fn read_loop<R>(
    mut reader: R,
    pending: Pending,
    notifications: broadcast::Sender<CodexTransportEvent>,
    connected: Arc<AtomicBool>,
    max_frame_bytes: usize,
) where
    R: AsyncBufRead + Send + Unpin + 'static,
{
    let disconnect_reason = loop {
        let frame = match read_frame(&mut reader, max_frame_bytes).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break "Codex app-server closed stdout".to_owned(),
            Err(error) => break error.to_string(),
        };
        let message = match serde_json::from_slice::<Value>(&frame) {
            Ok(message) => message,
            Err(error) => break CodexTransportError::InvalidJson(error).to_string(),
        };
        route_message(message, &pending, &notifications).await;
    };
    finish_disconnect(disconnect_reason, &pending, &notifications, &connected).await;
}

#[cfg(unix)]
async fn websocket_read_loop(
    mut stream: SplitStream<WebSocketStream<UnixStream>>,
    writer: Arc<Mutex<DynWriter>>,
    pending: Pending,
    notifications: broadcast::Sender<CodexTransportEvent>,
    connected: Arc<AtomicBool>,
    max_frame_bytes: usize,
) {
    let disconnect_reason = loop {
        match stream.next().await {
            Some(Ok(WebSocketMessage::Text(text))) => {
                if text.len() > max_frame_bytes {
                    break CodexTransportError::FrameTooLarge {
                        limit: max_frame_bytes,
                    }
                    .to_string();
                }
                let message = match serde_json::from_str::<Value>(&text) {
                    Ok(message) => message,
                    Err(error) => break CodexTransportError::InvalidJson(error).to_string(),
                };
                route_message(message, &pending, &notifications).await;
            }
            Some(Ok(WebSocketMessage::Ping(payload))) => {
                if let Err(error) = writer.lock().await.send_pong(payload.to_vec()).await {
                    break error.to_string();
                }
            }
            Some(Ok(WebSocketMessage::Pong(_))) => {}
            Some(Ok(WebSocketMessage::Close(frame))) => {
                break frame.map_or_else(
                    || "Codex shared app-server closed the WebSocket".to_owned(),
                    |frame| {
                        format!(
                            "Codex shared app-server closed the WebSocket: {}",
                            frame.reason
                        )
                    },
                );
            }
            Some(Ok(WebSocketMessage::Binary(_))) => {
                break "Codex shared app-server sent an unsupported binary frame".to_owned();
            }
            Some(Ok(WebSocketMessage::Frame(_))) => {}
            Some(Err(error)) => {
                break CodexTransportError::WebSocket(error.to_string()).to_string()
            }
            None => break "Codex shared app-server closed the WebSocket".to_owned(),
        }
    };
    finish_disconnect(disconnect_reason, &pending, &notifications, &connected).await;
}

async fn route_message(
    message: Value,
    pending: &Pending,
    notifications: &broadcast::Sender<CodexTransportEvent>,
) {
    if let Some(id) = message.get("id") {
        if message.get("method").is_none() {
            let key = id_key(id);
            if let Some(sender) = pending.lock().await.remove(&key) {
                let reply = if let Some(error) = message.get("error") {
                    Err(CodexTransportError::Rpc(
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown JSON-RPC error")
                            .to_owned(),
                    ))
                } else {
                    message
                        .get("result")
                        .cloned()
                        .ok_or(CodexTransportError::MissingResult)
                };
                let _ = sender.send(reply);
                return;
            }
        }
    }
    let _ = notifications.send(CodexTransportEvent::Message(message));
}

async fn finish_disconnect(
    disconnect_reason: String,
    pending: &Pending,
    notifications: &broadcast::Sender<CodexTransportEvent>,
    connected: &Arc<AtomicBool>,
) {
    if !connected.swap(false, Ordering::AcqRel) {
        return;
    }
    let senders = {
        let mut guard = pending.lock().await;
        guard.drain().map(|(_, sender)| sender).collect::<Vec<_>>()
    };
    for sender in senders {
        let _ = sender.send(Err(CodexTransportError::Disconnected(
            disconnect_reason.clone(),
        )));
    }
    let _ = notifications.send(CodexTransportEvent::Disconnected {
        reason: disconnect_reason,
    });
}

async fn read_frame<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<Option<Vec<u8>>, CodexTransportError>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(CodexTransportError::Io)?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(CodexTransportError::Closed)
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if frame.len().saturating_add(take) > max_frame_bytes {
            return Err(CodexTransportError::FrameTooLarge {
                limit: max_frame_bytes,
            });
        }
        frame.extend_from_slice(&available[..take]);
        reader.consume(take);
        if frame.last() == Some(&b'\n') {
            frame.pop();
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

fn id_key(id: &Value) -> String {
    id.as_str().map_or_else(|| id.to_string(), str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{duplex, AsyncBufReadExt};
    #[cfg(unix)]
    use tokio::net::UnixListener;
    #[cfg(unix)]
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn correlates_out_of_order_replies_and_keeps_notifications() {
        let (client_io, server_io) = duplex(16 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let client = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut notifications = client.subscribe();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let first: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let second: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            server_write
                .write_all(b"{\"method\":\"turn/started\",\"params\":{\"turnId\":\"t1\"}}\n")
                .await
                .unwrap();
            let second_reply = json!({"id": second["id"], "result": {"value": 2}});
            let first_reply = json!({"id": first["id"], "result": {"value": 1}});
            server_write
                .write_all(format!("{second_reply}\n{first_reply}\n").as_bytes())
                .await
                .unwrap();
        });
        let (first, second) = tokio::join!(
            client.request("first", json!({})),
            client.request("second", json!({}))
        );
        assert_eq!(first.unwrap()["value"], 1);
        assert_eq!(second.unwrap()["value"], 2);
        assert_eq!(
            match notifications.recv().await.unwrap() {
                CodexTransportEvent::Message(message) => message["method"].clone(),
                CodexTransportEvent::Disconnected { reason } => {
                    panic!("unexpected disconnect: {reason}")
                }
            },
            json!("turn/started")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reports_connection_close_to_requests_and_subscribers() {
        let (client_io, server_io) = duplex(16 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, server_write) = tokio::io::split(server_io);
        let client = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut notifications = client.subscribe();
        let request = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.request("pending", json!({})).await }
        });
        let mut reader = BufReader::new(server_read);
        let mut request_line = String::new();
        reader.read_line(&mut request_line).await.unwrap();
        drop(reader);
        drop(server_write);

        assert!(matches!(
            request.await.unwrap(),
            Err(CodexTransportError::Disconnected(ref reason))
                if reason.contains("closed stdout")
        ));
        let event = notifications.recv().await.unwrap();
        assert!(matches!(
            event,
            CodexTransportEvent::Disconnected { ref reason }
                if reason.contains("closed stdout")
        ));
        assert!(!client.is_connected());
        assert!(matches!(
            client.request("after-close", json!({})).await,
            Err(CodexTransportError::Closed)
        ));
    }

    #[tokio::test]
    async fn closes_connection_when_a_frame_exceeds_the_bound() {
        let (client_io, server_io) = duplex(256);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let client = CodexRpcClient::from_io(client_read, client_write, Duration::from_secs(1), 32);
        let request = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.request("large", json!({})).await }
        });
        let mut server_reader = BufReader::new(server_read);
        let mut request_line = Vec::new();
        server_reader
            .read_until(b'\n', &mut request_line)
            .await
            .unwrap();
        server_write
            .write_all(b"{\"id\":1,\"result\":{\"farTooLarge\":\"abcdefghijklmnopqrstuvwxyz\"}}\n")
            .await
            .unwrap();
        assert!(matches!(
            request.await.unwrap(),
            Err(CodexTransportError::Disconnected(ref reason))
                if reason.contains("frame exceeded 32 bytes")
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connects_to_shared_daemon_over_unix_websocket() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("control.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();

            let initialize = match websocket.next().await.unwrap().unwrap() {
                WebSocketMessage::Text(text) => serde_json::from_str::<Value>(&text).unwrap(),
                other => panic!("unexpected initialize frame: {other:?}"),
            };
            websocket
                .send(WebSocketMessage::Text(
                    json!({
                        "id": initialize["id"],
                        "result": {"userAgent": "codex/shared-test"}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();

            let initialized = match websocket.next().await.unwrap().unwrap() {
                WebSocketMessage::Text(text) => serde_json::from_str::<Value>(&text).unwrap(),
                other => panic!("unexpected initialized frame: {other:?}"),
            };
            assert_eq!(initialized["method"], "initialized");

            let request = match websocket.next().await.unwrap().unwrap() {
                WebSocketMessage::Text(text) => serde_json::from_str::<Value>(&text).unwrap(),
                other => panic!("unexpected request frame: {other:?}"),
            };
            websocket
                .send(WebSocketMessage::Text(
                    json!({"id": request["id"], "result": {"ok": true}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        });

        let config = CodexAppServerConfig {
            endpoint: CodexAppServerEndpoint::SharedDaemon {
                socket_path,
                auto_start: false,
            },
            request_timeout: Duration::from_secs(1),
            ..CodexAppServerConfig::default()
        };
        let (client, user_agent) = CodexRpcClient::connect(&config).await.unwrap();
        assert_eq!(user_agent, "codex/shared-test");
        assert_eq!(
            client.request("fixture/read", json!({})).await.unwrap()["ok"],
            true
        );
        server.await.unwrap();
    }

    #[test]
    fn default_frame_limit_covers_long_lived_codex_sessions() {
        let config = CodexAppServerConfig::default();
        assert!(config.max_frame_bytes >= 256 * 1024 * 1024);
        #[cfg(unix)]
        assert!(matches!(
            config.endpoint,
            CodexAppServerEndpoint::SharedDaemon {
                auto_start: true,
                ..
            }
        ));
    }
}
