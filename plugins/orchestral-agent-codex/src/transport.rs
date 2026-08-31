use std::collections::HashMap;
use std::path::PathBuf;
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

const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct CodexAppServerConfig {
    pub executable: PathBuf,
    pub request_timeout: Duration,
    pub max_frame_bytes: usize,
}

impl Default for CodexAppServerConfig {
    fn default() -> Self {
        Self {
            executable: PathBuf::from("codex"),
            request_timeout: Duration::from_secs(30),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

#[derive(Debug, Error)]
pub enum CodexTransportError {
    #[error("failed to start Codex app-server: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("Codex app-server I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("Codex app-server returned invalid JSON: {0}")]
    InvalidJson(#[source] serde_json::Error),
    #[error("Codex app-server frame exceeded {limit} bytes")]
    FrameTooLarge { limit: usize },
    #[error("Codex app-server closed the connection")]
    Closed,
    #[error("Codex app-server request timed out")]
    Timeout,
    #[error("Codex app-server rejected the request: {0}")]
    Rpc(String),
    #[error("Codex app-server response did not contain a result")]
    MissingResult,
}

type RpcReply = Result<Value, String>;
type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<RpcReply>>>>;
type DynWriter = Box<dyn AsyncWrite + Send + Unpin>;

pub struct CodexRpcClient {
    writer: Arc<Mutex<DynWriter>>,
    pending: Pending,
    _notifications: broadcast::Sender<Value>,
    next_id: Mutex<u64>,
    request_timeout: Duration,
    _child: Option<Arc<Mutex<Child>>>,
}

impl CodexRpcClient {
    pub async fn spawn(
        config: &CodexAppServerConfig,
    ) -> Result<(Arc<Self>, String), CodexTransportError> {
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
        let client = Self::from_io_with_child(
            stdout,
            stdin,
            config.request_timeout,
            config.max_frame_bytes,
            Some(child),
        );
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
        let client = Arc::new(Self {
            writer: Arc::new(Mutex::new(Box::new(writer))),
            pending: Arc::clone(&pending),
            _notifications: notifications.clone(),
            next_id: Mutex::new(1),
            request_timeout,
            _child: child.map(|child| Arc::new(Mutex::new(child))),
        });
        tokio::spawn(read_loop(
            BufReader::new(reader),
            pending,
            notifications,
            max_frame_bytes,
        ));
        client
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Value> {
        self._notifications.subscribe()
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
            Ok(Ok(Err(message))) => Err(CodexTransportError::Rpc(message)),
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
        let mut bytes = serde_json::to_vec(message).map_err(CodexTransportError::InvalidJson)?;
        bytes.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer
            .write_all(&bytes)
            .await
            .map_err(CodexTransportError::Io)?;
        writer.flush().await.map_err(CodexTransportError::Io)
    }
}

async fn read_loop<R>(
    mut reader: R,
    pending: Pending,
    notifications: broadcast::Sender<Value>,
    max_frame_bytes: usize,
) where
    R: AsyncBufRead + Send + Unpin + 'static,
{
    loop {
        let frame = match read_frame(&mut reader, max_frame_bytes).await {
            Ok(Some(frame)) => frame,
            Ok(None) | Err(_) => break,
        };
        let Ok(message) = serde_json::from_slice::<Value>(&frame) else {
            continue;
        };
        if let Some(id) = message.get("id") {
            if message.get("method").is_none() {
                let key = id_key(id);
                if let Some(sender) = pending.lock().await.remove(&key) {
                    let reply = if let Some(error) = message.get("error") {
                        Err(error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown JSON-RPC error")
                            .to_owned())
                    } else {
                        message
                            .get("result")
                            .cloned()
                            .ok_or_else(|| "response omitted result".to_owned())
                    };
                    let _ = sender.send(reply);
                    continue;
                }
            }
        }
        let _ = notifications.send(message);
    }
    let senders = {
        let mut guard = pending.lock().await;
        guard.drain().map(|(_, sender)| sender).collect::<Vec<_>>()
    };
    for sender in senders {
        let _ = sender.send(Err("connection closed".to_owned()));
    }
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
    use tokio::io::{duplex, AsyncBufReadExt};

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
            notifications.recv().await.unwrap()["method"],
            "turn/started"
        );
        server.await.unwrap();
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
            Err(CodexTransportError::Rpc(_)) | Err(CodexTransportError::Closed)
        ));
    }
}
