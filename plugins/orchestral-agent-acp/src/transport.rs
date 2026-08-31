use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use thiserror::Error;
use tokio::io::{
    self, AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, oneshot, Mutex};
use tokio::time::timeout;

const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct AcpProcessConfig {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub request_timeout: Duration,
    pub max_frame_bytes: usize,
}

impl AcpProcessConfig {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            args: Vec::new(),
            request_timeout: Duration::from_secs(30),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }
}

#[derive(Debug, Error)]
pub enum AcpTransportError {
    #[error("failed to start ACP Agent: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("ACP transport I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("ACP transport returned invalid JSON: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("ACP frame exceeded {limit} bytes")]
    FrameTooLarge { limit: usize },
    #[error("ACP transport closed the connection")]
    Closed,
    #[error("ACP transport disconnected: {0}")]
    Disconnected(String),
    #[error("ACP request timed out")]
    Timeout,
    #[error("ACP peer rejected the request: {0}")]
    Rpc(String),
    #[error("ACP response did not contain a result")]
    MissingResult,
}

type RpcReply = Result<Value, AcpTransportError>;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<RpcReply>>>>;
type DynWriter = Box<dyn AsyncWrite + Send + Unpin>;

#[derive(Debug, Clone)]
pub(crate) enum AcpTransportEvent {
    Message(Value),
    Disconnected { reason: String },
}

pub(crate) struct AcpRpcClient {
    writer: Arc<Mutex<DynWriter>>,
    pending: Pending,
    events: broadcast::Sender<AcpTransportEvent>,
    connected: Arc<AtomicBool>,
    next_id: Mutex<u64>,
    request_timeout: Duration,
    _child: Option<Arc<Mutex<Child>>>,
}

impl AcpRpcClient {
    pub(crate) async fn spawn(config: &AcpProcessConfig) -> Result<Arc<Self>, AcpTransportError> {
        let mut child = Command::new(&config.executable)
            .args(&config.args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(AcpTransportError::Spawn)?;
        let stdin = child.stdin.take().ok_or(AcpTransportError::Closed)?;
        let stdout = child.stdout.take().ok_or(AcpTransportError::Closed)?;
        if let Some(mut stderr) = child.stderr.take() {
            tokio::spawn(async move {
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
        let (events, _) = broadcast::channel(512);
        let connected = Arc::new(AtomicBool::new(true));
        let client = Arc::new(Self {
            writer: Arc::new(Mutex::new(Box::new(writer))),
            pending: Arc::clone(&pending),
            events: events.clone(),
            connected: Arc::clone(&connected),
            next_id: Mutex::new(1),
            request_timeout,
            _child: child.map(|child| Arc::new(Mutex::new(child))),
        });
        tokio::spawn(read_loop(
            BufReader::new(reader),
            pending,
            events,
            connected,
            max_frame_bytes,
        ));
        client
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<AcpTransportEvent> {
        self.events.subscribe()
    }

    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub(crate) async fn request(
        &self,
        method: &str,
        params: Value,
    ) -> Result<Value, AcpTransportError> {
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
            .write(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await
        {
            self.pending.lock().await.remove(&key);
            return Err(error);
        }
        match timeout(self.request_timeout, receiver).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => Err(AcpTransportError::Closed),
            Err(_) => {
                self.pending.lock().await.remove(&key);
                Err(AcpTransportError::Timeout)
            }
        }
    }

    pub(crate) async fn notify(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(), AcpTransportError> {
        self.write(&json!({"jsonrpc": "2.0", "method": method, "params": params}))
            .await
    }

    pub(crate) async fn respond(&self, id: Value, result: Value) -> Result<(), AcpTransportError> {
        self.write(&json!({"jsonrpc": "2.0", "id": id, "result": result}))
            .await
    }

    pub(crate) async fn respond_error(
        &self,
        id: Value,
        code: i64,
        message: impl Into<String>,
    ) -> Result<(), AcpTransportError> {
        self.write(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message.into()}
        }))
        .await
    }

    async fn write(&self, message: &Value) -> Result<(), AcpTransportError> {
        if !self.is_connected() {
            return Err(AcpTransportError::Closed);
        }
        let mut bytes = serde_json::to_vec(message).map_err(AcpTransportError::InvalidJson)?;
        bytes.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&bytes).await.map_err(|error| {
            self.connected.store(false, Ordering::Release);
            AcpTransportError::Io(error)
        })?;
        writer.flush().await.map_err(|error| {
            self.connected.store(false, Ordering::Release);
            AcpTransportError::Io(error)
        })
    }
}

async fn read_loop<R>(
    mut reader: R,
    pending: Pending,
    events: broadcast::Sender<AcpTransportEvent>,
    connected: Arc<AtomicBool>,
    max_frame_bytes: usize,
) where
    R: AsyncBufRead + Send + Unpin + 'static,
{
    let disconnect_reason = loop {
        let frame = match read_frame(&mut reader, max_frame_bytes).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break "ACP Agent closed stdout".to_owned(),
            Err(error) => break error.to_string(),
        };
        let message = match serde_json::from_slice::<Value>(&frame) {
            Ok(message) => message,
            Err(error) => break AcpTransportError::InvalidJson(error).to_string(),
        };
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            break "ACP message omitted jsonrpc 2.0 marker".to_owned();
        }
        if let Some(id) = message.get("id") {
            if message.get("method").is_none() {
                if let Some(sender) = pending.lock().await.remove(&id_key(id)) {
                    let reply = if let Some(error) = message.get("error") {
                        Err(AcpTransportError::Rpc(rpc_error_message(error)))
                    } else {
                        message
                            .get("result")
                            .cloned()
                            .ok_or(AcpTransportError::MissingResult)
                    };
                    let _ = sender.send(reply);
                    continue;
                }
            }
        }
        let _ = events.send(AcpTransportEvent::Message(message));
    };
    connected.store(false, Ordering::Release);
    let senders = {
        let mut guard = pending.lock().await;
        guard.drain().map(|(_, sender)| sender).collect::<Vec<_>>()
    };
    for sender in senders {
        let _ = sender.send(Err(AcpTransportError::Disconnected(
            disconnect_reason.clone(),
        )));
    }
    let _ = events.send(AcpTransportEvent::Disconnected {
        reason: disconnect_reason,
    });
}

async fn read_frame<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<Option<Vec<u8>>, AcpTransportError>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(AcpTransportError::Io)?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(AcpTransportError::Closed)
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if frame.len().saturating_add(take) > max_frame_bytes {
            return Err(AcpTransportError::FrameTooLarge {
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

fn rpc_error_message(error: &Value) -> String {
    let code = error.get("code").map(Value::to_string);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown JSON-RPC error");
    code.map_or_else(|| message.to_owned(), |code| format!("{code}: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncBufReadExt};

    #[tokio::test]
    async fn preserves_bidirectional_requests_and_jsonrpc_identity() {
        let (client_io, server_io) = duplex(16 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let client = AcpRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut events = client.subscribe();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let request: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(request["jsonrpc"], "2.0");
            assert_eq!(request["method"], "initialize");
            server_write
                .write_all(
                    format!(
                        "{}\n{}\n",
                        json!({"jsonrpc":"2.0","id":"permission-1","method":"session/request_permission","params":{"sessionId":"s1"}}),
                        json!({"jsonrpc":"2.0","id":request["id"],"result":{"protocolVersion":1}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let response: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(response["id"], "permission-1");
            assert_eq!(response["result"]["outcome"]["outcome"], "cancelled");
        });

        let initialized = client.request("initialize", json!({})).await.unwrap();
        assert_eq!(initialized["protocolVersion"], 1);
        let request = match events.recv().await.unwrap() {
            AcpTransportEvent::Message(message) => message,
            AcpTransportEvent::Disconnected { reason } => panic!("{reason}"),
        };
        client
            .respond(
                request["id"].clone(),
                json!({"outcome":{"outcome":"cancelled"}}),
            )
            .await
            .unwrap();
        server.await.unwrap();
    }
}
