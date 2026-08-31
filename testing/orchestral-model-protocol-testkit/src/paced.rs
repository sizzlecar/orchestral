use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const MAX_REQUEST_HEADER_BYTES: usize = 64 * 1024;

/// HTTP/SSE fixture that stays active longer than an adapter's idle timeout
/// while ensuring every individual read arrives within that timeout.
pub struct PacedSseServer {
    endpoint: String,
    task: Option<JoinHandle<Result<(), String>>>,
}

impl PacedSseServer {
    pub async fn start(chunks: Vec<Vec<u8>>, interval: Duration) -> Result<Self, String> {
        if chunks.is_empty() || interval.is_zero() {
            return Err("paced SSE fixture requires chunks and a non-zero interval".to_owned());
        }
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        let address = listener.local_addr().map_err(|error| error.to_string())?;
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.map_err(|error| error.to_string())?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 4_096];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket
                    .read(&mut buffer)
                    .await
                    .map_err(|error| error.to_string())?;
                if read == 0 {
                    return Err("HTTP client closed before sending request headers".to_owned());
                }
                request.extend_from_slice(&buffer[..read]);
                if request.len() > MAX_REQUEST_HEADER_BYTES {
                    return Err("HTTP request headers exceeded the fixture limit".to_owned());
                }
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n",
                )
                .await
                .map_err(|error| error.to_string())?;
            for (index, chunk) in chunks.into_iter().enumerate() {
                if index > 0 {
                    tokio::time::sleep(interval).await;
                }
                socket
                    .write_all(&chunk)
                    .await
                    .map_err(|error| error.to_string())?;
                socket.flush().await.map_err(|error| error.to_string())?;
            }
            socket.shutdown().await.map_err(|error| error.to_string())?;
            Ok(())
        });
        Ok(Self {
            endpoint: format!("http://{address}"),
            task: Some(task),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn finish(mut self) -> Result<(), String> {
        self.task
            .take()
            .expect("paced SSE task is present until finish")
            .await
            .map_err(|error| format!("paced SSE task failed: {error}"))?
    }
}

impl Drop for PacedSseServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
