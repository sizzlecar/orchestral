use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, oneshot, Mutex};

use crate::ClaudeCodeConfig;

#[derive(Debug, Error)]
pub(crate) enum TransportError {
    #[error("Claude stream I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("Claude control request timed out")]
    Timeout,
    #[error("Claude stream disconnected: {0}")]
    Disconnected(String),
    #[error("Claude stream protocol: {0}")]
    Protocol(String),
}

#[derive(Clone, Debug)]
pub(crate) enum TransportEvent {
    Message(Value),
    Disconnected(String),
}

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, TransportError>>>>>;

pub(crate) struct ClaudeStream {
    writer: Mutex<Box<dyn AsyncWrite + Send + Unpin>>,
    pending: Pending,
    events: broadcast::Sender<TransportEvent>,
    connected: Arc<AtomicBool>,
    request_timeout: Duration,
    child: Option<Mutex<Child>>,
}

impl ClaudeStream {
    pub(crate) async fn spawn(
        config: &ClaudeCodeConfig,
        cwd: &Path,
        args: &[String],
    ) -> Result<Arc<Self>, TransportError> {
        let mut child = Command::new(&config.executable)
            .args(args)
            .current_dir(cwd)
            .env("CLAUDE_CONFIG_DIR", &config.config_dir)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| TransportError::Protocol("missing stdin".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| TransportError::Protocol("missing stdout".to_owned()))?;
        if let Some(mut stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await;
            });
        }
        Ok(Self::from_io(
            stdout,
            stdin,
            config.request_timeout,
            Some(child),
        ))
    }

    pub(crate) fn from_io<R, W>(
        reader: R,
        writer: W,
        request_timeout: Duration,
        child: Option<Child>,
    ) -> Arc<Self>
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let (events, _) = broadcast::channel(2048);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let connected = Arc::new(AtomicBool::new(true));
        let client = Arc::new(Self {
            writer: Mutex::new(Box::new(writer)),
            pending: pending.clone(),
            events: events.clone(),
            connected: connected.clone(),
            request_timeout,
            child: child.map(Mutex::new),
        });
        tokio::spawn(read_loop(
            BufReader::new(reader),
            pending,
            events,
            connected,
        ));
        client
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<TransportEvent> {
        self.events.subscribe()
    }

    pub(crate) async fn control(&self, request: Value) -> Result<Value, TransportError> {
        let id = format!("orch-{}", uuid::Uuid::new_v4());
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), sender);
        if let Err(error) = self
            .write(&json!({"type":"control_request","request_id":id,"request":request}))
            .await
        {
            self.pending.lock().await.remove(&id);
            return Err(error);
        }
        let result = match tokio::time::timeout(self.request_timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(TransportError::Disconnected(
                "control channel closed".to_owned(),
            )),
            Err(_) => Err(TransportError::Timeout),
        };
        self.pending.lock().await.remove(&id);
        result
    }

    pub(crate) async fn respond(&self, id: &str, response: Value) -> Result<(), TransportError> {
        self.write(&json!({"type":"control_response","response":{"subtype":"success","request_id":id,"response":response}})).await
    }

    pub(crate) async fn respond_error(&self, id: &str, error: &str) -> Result<(), TransportError> {
        self.write(&json!({"type":"control_response","response":{"subtype":"error","request_id":id,"error":error}})).await
    }

    pub(crate) async fn write(&self, message: &Value) -> Result<(), TransportError> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(TransportError::Disconnected("stdout closed".to_owned()));
        }
        let mut bytes = serde_json::to_vec(message)
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        bytes.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }

    pub(crate) async fn close(&self) -> Result<(), TransportError> {
        let _ = self.writer.lock().await.shutdown().await;
        if let Some(child) = &self.child {
            let mut child = child.lock().await;
            match tokio::time::timeout(Duration::from_secs(2), child.wait()).await {
                Ok(result) => {
                    result?;
                }
                Err(_) => {
                    child.kill().await?;
                }
            }
        }
        Ok(())
    }
}

async fn read_loop<R: AsyncBufRead + Send + Unpin + 'static>(
    mut reader: R,
    pending: Pending,
    events: broadcast::Sender<TransportEvent>,
    connected: Arc<AtomicBool>,
) {
    let reason = loop {
        let frame = match read_frame(&mut reader).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break "Claude closed stdout".to_owned(),
            Err(error) => break error.to_string(),
        };
        if frame.is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_slice(&frame) {
            Ok(value) => value,
            Err(error) => break format!("invalid Claude stream JSON: {error}"),
        };
        if message.get("type").and_then(Value::as_str) == Some("control_response") {
            if let Some(response) = message.get("response") {
                if let Some(id) = response.get("request_id").and_then(Value::as_str) {
                    if let Some(sender) = pending.lock().await.remove(id) {
                        let result = match response.get("subtype").and_then(Value::as_str) {
                            Some("success") => {
                                Ok(response.get("response").cloned().unwrap_or(json!({})))
                            }
                            _ => Err(TransportError::Protocol(
                                response
                                    .get("error")
                                    .and_then(Value::as_str)
                                    .unwrap_or("control request rejected")
                                    .to_owned(),
                            )),
                        };
                        let _ = sender.send(result);
                        continue;
                    }
                }
            }
        }
        let _ = events.send(TransportEvent::Message(message));
    };
    connected.store(false, Ordering::Release);
    for (_, sender) in pending.lock().await.drain() {
        let _ = sender.send(Err(TransportError::Disconnected(reason.clone())));
    }
    let _ = events.send(TransportEvent::Disconnected(reason));
}

async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, TransportError> {
    const MAX_FRAME: usize = 16 * 1024 * 1024;
    let mut frame = Vec::new();
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(TransportError::Protocol(
                    "unterminated stream frame".to_owned(),
                ))
            };
        }
        let count = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(buffer.len(), |position| position + 1);
        if frame.len().saturating_add(count) > MAX_FRAME {
            return Err(TransportError::Protocol(
                "stream frame exceeds 16 MiB".to_owned(),
            ));
        }
        frame.extend_from_slice(&buffer[..count]);
        reader.consume(count);
        if frame.last() == Some(&b'\n') {
            frame.pop();
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn control_responses_are_correlated_without_consuming_permission_events() {
        let (client, server) = tokio::io::duplex(16384);
        let (reader, writer) = tokio::io::split(client);
        let (reader_server, mut writer_server) = tokio::io::split(server);
        let rpc = ClaudeStream::from_io(reader, writer, Duration::from_secs(1), None);
        let mut events = rpc.subscribe();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(reader_server).lines();
            let request: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let permission = json!({"type":"control_request","request_id":"permission","request":{"subtype":"can_use_tool","tool_name":"Write","input":{}}});
            let reply = json!({"type":"control_response","response":{"subtype":"success","request_id":request["request_id"],"response":{"ready":true}}});
            writer_server
                .write_all(format!("{permission}\n{reply}\n").as_bytes())
                .await
                .unwrap();
            let response: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(response["response"]["request_id"], "permission");
            assert_eq!(response["response"]["response"]["behavior"], "deny");
        });
        assert_eq!(
            rpc.control(json!({"subtype":"initialize"})).await.unwrap()["ready"],
            true
        );
        let TransportEvent::Message(event) = events.recv().await.unwrap() else {
            panic!("missing permission event")
        };
        assert_eq!(event["request_id"], "permission");
        rpc.respond(
            "permission",
            json!({"behavior":"deny","message":"declined"}),
        )
        .await
        .unwrap();
        server.await.unwrap();
    }
}
