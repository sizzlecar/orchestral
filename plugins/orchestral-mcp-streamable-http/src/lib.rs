//! MCP `2026-07-28` Streamable HTTP transport.
//!
//! This plugin owns HTTP/TLS/framing only. MCP negotiation, Tool schema
//! pinning, policy, approval, and effect journaling remain in runtime.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures_util::StreamExt;
use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::mcp_protocol::{
    McpTransportAuthority, McpTransportCancellation, McpTransportConnection, McpTransportError,
    McpTransportFactory, McpTransportKind, McpTransportRequest, MCP_STATELESS_PROTOCOL_2026_07_28,
};
use orchestral_core::tool_protocol::EffectScope;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE};
use reqwest::{Client, Response, Url};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

pub const DEFAULT_MAX_MCP_HTTP_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct ResolvedCredentialHeader {
    pub reference: String,
    pub value: String,
}

impl std::fmt::Debug for ResolvedCredentialHeader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedCredentialHeader")
            .field("reference", &self.reference)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct StreamableHttpMcpTransportConfig {
    pub endpoint: String,
    /// Every Host-supplied header is authority-bearing and must have an opaque
    /// credential reference. Protocol and `Mcp-Param-*` headers are derived.
    pub credential_headers: BTreeMap<String, ResolvedCredentialHeader>,
    pub max_frame_bytes: usize,
}

impl StreamableHttpMcpTransportConfig {
    pub fn unauthenticated(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            credential_headers: BTreeMap::new(),
            max_frame_bytes: DEFAULT_MAX_MCP_HTTP_FRAME_BYTES,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid MCP Streamable HTTP configuration: {0}")]
pub struct StreamableHttpMcpConfigError(String);

impl StreamableHttpMcpConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

#[derive(Clone)]
pub struct StreamableHttpMcpTransportFactory {
    client: Client,
    endpoint: Url,
    configured_headers: HeaderMap,
    authority: McpTransportAuthority,
    max_frame_bytes: usize,
}

impl StreamableHttpMcpTransportFactory {
    pub fn new(
        config: StreamableHttpMcpTransportConfig,
    ) -> Result<Self, StreamableHttpMcpConfigError> {
        if config.max_frame_bytes == 0 {
            return Err(StreamableHttpMcpConfigError::new(
                "max_frame_bytes must be positive",
            ));
        }
        let endpoint = Url::parse(config.endpoint.trim())
            .map_err(|error| StreamableHttpMcpConfigError::new(error.to_string()))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(StreamableHttpMcpConfigError::new(
                "endpoint must be an absolute http(s) URL without userinfo or fragment",
            ));
        }

        let mut configured_headers = HeaderMap::new();
        let mut normalized_names = BTreeSet::new();
        let mut credential_references = BTreeSet::new();
        for (name, credential) in &config.credential_headers {
            if credential.reference.trim().is_empty()
                || credential.reference.chars().any(char::is_control)
                || credential.value.is_empty()
            {
                return Err(StreamableHttpMcpConfigError::new(
                    "credential header requires a non-empty reference and resolved value",
                ));
            }
            insert_configured_header(
                &mut configured_headers,
                &mut normalized_names,
                name,
                &credential.value,
            )?;
            credential_references.insert(credential.reference.clone());
        }

        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|error| StreamableHttpMcpConfigError::new(error.to_string()))?;
        let mut effect_scopes =
            BTreeSet::from([EffectScope::Network, EffectScope::ExternalSideEffect]);
        if !credential_references.is_empty() {
            effect_scopes.insert(EffectScope::SecretRead);
        }
        let binding_view = json!({
            "transport": "streamable_http",
            "endpoint": endpoint.as_str(),
            "credentialHeaders": config.credential_headers.iter().map(|(name, credential)| {
                (name, credential.reference.as_str())
            }).collect::<BTreeMap<_, _>>(),
            "maxFrameBytes": config.max_frame_bytes,
            "redirects": false,
            "environmentProxy": false,
        });
        let authority = McpTransportAuthority {
            kind: McpTransportKind::StreamableHttp,
            binding_digest: Digest::sha256(
                serde_jcs::to_vec(&binding_view)
                    .map_err(|error| StreamableHttpMcpConfigError::new(error.to_string()))?,
            ),
            effect_scopes,
            process_programs: BTreeSet::new(),
            allow_child_processes: false,
            filesystem_read_roots: BTreeSet::new(),
            filesystem_write_roots: BTreeSet::new(),
            sandbox_profiles: BTreeSet::new(),
            network_targets: BTreeSet::from([endpoint.as_str().to_owned()]),
            allow_unrestricted_network: false,
            environment_variables: BTreeSet::new(),
            credential_references,
        };
        authority
            .validate()
            .map_err(|error| StreamableHttpMcpConfigError::new(error.to_string()))?;
        Ok(Self {
            client,
            endpoint,
            configured_headers,
            authority,
            max_frame_bytes: config.max_frame_bytes,
        })
    }
}

#[async_trait]
impl McpTransportFactory for StreamableHttpMcpTransportFactory {
    fn authority(&self) -> &McpTransportAuthority {
        &self.authority
    }

    async fn connect(&self) -> Result<Box<dyn McpTransportConnection>, McpTransportError> {
        Ok(Box::new(StreamableHttpMcpTransport {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
            configured_headers: self.configured_headers.clone(),
            max_frame_bytes: self.max_frame_bytes,
        }))
    }
}

struct StreamableHttpMcpTransport {
    client: Client,
    endpoint: Url,
    configured_headers: HeaderMap,
    max_frame_bytes: usize,
}

impl StreamableHttpMcpTransport {
    fn build_request(
        &self,
        request: McpTransportRequest,
    ) -> Result<reqwest::Request, McpTransportError> {
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": request.id,
            "method": request.method,
            "params": request.params,
        }))
        .map_err(|error| McpTransportError::Protocol(error.to_string()))?;
        if body.len() > self.max_frame_bytes {
            return Err(McpTransportError::Protocol(format!(
                "MCP HTTP request exceeds the {} byte limit",
                self.max_frame_bytes
            )));
        }
        let payload: Value = serde_json::from_slice(&body)
            .map_err(|error| McpTransportError::Protocol(error.to_string()))?;
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| McpTransportError::Protocol("MCP method is missing".to_owned()))?;
        let params = payload
            .get("params")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                McpTransportError::Protocol("MCP params must be an object".to_owned())
            })?;
        let protocol_version = params
            .get("_meta")
            .and_then(Value::as_object)
            .and_then(|meta| meta.get("io.modelcontextprotocol/protocolVersion"))
            .and_then(Value::as_str)
            .filter(|version| *version == MCP_STATELESS_PROTOCOL_2026_07_28)
            .ok_or_else(|| {
                McpTransportError::Protocol(
                    "MCP HTTP request omitted matching 2026-07-28 metadata".to_owned(),
                )
            })?;

        let mut headers = self.configured_headers.clone();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        insert_runtime_header(&mut headers, "mcp-protocol-version", protocol_version)?;
        insert_runtime_header(&mut headers, "mcp-method", method)?;
        if let Some(name) = routing_name(method, params)? {
            insert_runtime_header(&mut headers, "mcp-name", &encode_header_value(name))?;
        }
        for (suffix, value) in &request.parameter_headers {
            if !is_http_token(suffix) {
                return Err(McpTransportError::Protocol(format!(
                    "invalid MCP parameter header suffix '{suffix}'"
                )));
            }
            insert_runtime_header(
                &mut headers,
                &format!("mcp-param-{suffix}"),
                &encode_header_value(value),
            )?;
        }

        self.client
            .post(self.endpoint.clone())
            .headers(headers)
            .body(body)
            .build()
            .map_err(|error| McpTransportError::Transport(error.to_string()))
    }
}

#[async_trait]
impl McpTransportConnection for StreamableHttpMcpTransport {
    fn kind(&self) -> McpTransportKind {
        McpTransportKind::StreamableHttp
    }

    fn cancellation(&self) -> McpTransportCancellation {
        McpTransportCancellation::DropExchange
    }

    async fn request(
        &self,
        request: McpTransportRequest,
        cancellation: CancellationToken,
    ) -> Result<Value, McpTransportError> {
        let request_id = request.id;
        let request = self.build_request(request)?;

        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(McpTransportError::Transport(
                    "MCP HTTP request was cancelled".to_owned(),
                ));
            }
            response = self.client.execute(request) => {
                response.map_err(|error| McpTransportError::Transport(error.to_string()))?
            },
        };
        decode_http_response(response, request_id, self.max_frame_bytes, cancellation).await
    }

    async fn notification(
        &self,
        _method: &str,
        _params: Value,
        _cancellation: CancellationToken,
    ) -> Result<(), McpTransportError> {
        Err(McpTransportError::Protocol(
            "MCP 2026-07-28 defines no client notifications over Streamable HTTP".to_owned(),
        ))
    }

    async fn close(&self) -> Result<(), McpTransportError> {
        // There is no protocol session to close. Dropping each in-flight
        // response stream is the request-scoped cancellation signal.
        Ok(())
    }
}

fn insert_configured_header(
    headers: &mut HeaderMap,
    normalized_names: &mut BTreeSet<String>,
    name: &str,
    value: &str,
) -> Result<(), StreamableHttpMcpConfigError> {
    let normalized = name.to_ascii_lowercase();
    if is_reserved_header(&normalized) || !normalized_names.insert(normalized) {
        return Err(StreamableHttpMcpConfigError::new(format!(
            "configured header '{name}' is reserved or duplicated"
        )));
    }
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|error| StreamableHttpMcpConfigError::new(error.to_string()))?;
    let value = HeaderValue::from_str(value)
        .map_err(|error| StreamableHttpMcpConfigError::new(error.to_string()))?;
    headers.insert(name, value);
    Ok(())
}

fn is_reserved_header(name: &str) -> bool {
    matches!(
        name,
        "accept"
            | "content-length"
            | "content-type"
            | "host"
            | "mcp-method"
            | "mcp-name"
            | "mcp-protocol-version"
            | "mcp-session-id"
            | "last-event-id"
    ) || name.starts_with("mcp-param-")
}

fn insert_runtime_header(
    headers: &mut HeaderMap,
    name: &str,
    value: &str,
) -> Result<(), McpTransportError> {
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|error| McpTransportError::Protocol(error.to_string()))?;
    let value = HeaderValue::from_str(value)
        .map_err(|error| McpTransportError::Protocol(error.to_string()))?;
    headers.insert(name, value);
    Ok(())
}

fn routing_name<'a>(
    method: &str,
    params: &'a serde_json::Map<String, Value>,
) -> Result<Option<&'a str>, McpTransportError> {
    let field = match method {
        "tools/call" | "prompts/get" => Some("name"),
        "resources/read" => Some("uri"),
        _ => None,
    };
    field
        .map(|field| {
            params.get(field).and_then(Value::as_str).ok_or_else(|| {
                McpTransportError::Protocol(format!(
                    "MCP {method} request omitted string params.{field}"
                ))
            })
        })
        .transpose()
}

fn is_http_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn encode_header_value(value: &str) -> String {
    let bytes = value.as_bytes();
    let sentinel = value.starts_with("=?base64?") && value.ends_with("?=");
    let edge_whitespace = bytes
        .first()
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
        || bytes
            .last()
            .is_some_and(|byte| matches!(byte, b' ' | b'\t'));
    let plain_ascii = bytes.iter().all(|byte| matches!(byte, b'\t' | b' '..=b'~'));
    if plain_ascii && !edge_whitespace && !sentinel {
        value.to_owned()
    } else {
        format!("=?base64?{}?=", BASE64_STANDARD.encode(bytes))
    }
}

async fn decode_http_response(
    response: Response,
    request_id: u64,
    max_frame_bytes: usize,
    cancellation: CancellationToken,
) -> Result<Value, McpTransportError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_frame_bytes as u64)
    {
        return Err(McpTransportError::Transport(format!(
            "MCP HTTP response exceeds the {max_frame_bytes} byte limit"
        )));
    }
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        })
        .ok_or_else(|| {
            McpTransportError::Protocol("MCP HTTP response omitted Content-Type".to_owned())
        })?;
    let decoded = match content_type.as_str() {
        "application/json" => {
            let bytes = read_bounded_body(response, max_frame_bytes, cancellation).await?;
            let message = serde_json::from_slice::<Value>(&bytes)
                .map_err(|error| McpTransportError::Protocol(error.to_string()))?;
            decode_jsonrpc_message(message, request_id)
        }
        "text/event-stream" => {
            read_sse_response(response, request_id, max_frame_bytes, cancellation).await
        }
        other => Err(McpTransportError::Protocol(format!(
            "unsupported MCP HTTP response Content-Type '{other}'"
        ))),
    };
    match (status.is_success(), decoded) {
        (_, Err(error @ McpTransportError::Rpc { .. })) => Err(error),
        (true, result) => result,
        (false, Ok(_)) => Err(McpTransportError::Protocol(format!(
            "MCP HTTP returned status {status} with a success response"
        ))),
        (false, Err(error)) => Err(error),
    }
}

async fn read_bounded_body(
    response: Response,
    max_frame_bytes: usize,
    cancellation: CancellationToken,
) -> Result<Vec<u8>, McpTransportError> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(McpTransportError::Transport(
                    "MCP HTTP response was cancelled".to_owned(),
                ));
            }
            chunk = stream.next() => chunk,
        };
        let Some(chunk) = chunk else {
            return Ok(body);
        };
        let chunk = chunk.map_err(|error| McpTransportError::Transport(error.to_string()))?;
        if body.len().saturating_add(chunk.len()) > max_frame_bytes {
            return Err(McpTransportError::Transport(format!(
                "MCP HTTP response exceeds the {max_frame_bytes} byte limit"
            )));
        }
        body.extend_from_slice(&chunk);
    }
}

async fn read_sse_response(
    response: Response,
    request_id: u64,
    max_frame_bytes: usize,
    cancellation: CancellationToken,
) -> Result<Value, McpTransportError> {
    let mut stream = response.bytes_stream();
    let mut buffered = Vec::new();
    let mut event_data = Vec::<Vec<u8>>::new();
    let mut received = 0usize;
    loop {
        let chunk = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(McpTransportError::Transport(
                    "MCP HTTP SSE response was cancelled".to_owned(),
                ));
            }
            chunk = stream.next() => chunk,
        };
        match chunk {
            Some(Ok(chunk)) => {
                received = received.saturating_add(chunk.len());
                if received > max_frame_bytes {
                    return Err(McpTransportError::Transport(format!(
                        "MCP HTTP SSE response exceeds the {max_frame_bytes} byte limit"
                    )));
                }
                buffered.extend_from_slice(&chunk);
                while let Some(index) = buffered.iter().position(|byte| *byte == b'\n') {
                    let mut line = buffered.drain(..=index).collect::<Vec<_>>();
                    line.pop();
                    if line.last() == Some(&b'\r') {
                        line.pop();
                    }
                    if let Some(result) = process_sse_line(&line, &mut event_data, request_id)? {
                        return Ok(result);
                    }
                }
            }
            Some(Err(error)) => return Err(McpTransportError::Transport(error.to_string())),
            None => {
                if !buffered.is_empty() {
                    if buffered.last() == Some(&b'\r') {
                        buffered.pop();
                    }
                    if let Some(result) = process_sse_line(&buffered, &mut event_data, request_id)?
                    {
                        return Ok(result);
                    }
                }
                if let Some(result) = dispatch_sse_event(&mut event_data, request_id)? {
                    return Ok(result);
                }
                return Err(McpTransportError::Protocol(
                    "MCP HTTP SSE stream ended before the final response".to_owned(),
                ));
            }
        }
    }
}

fn process_sse_line(
    line: &[u8],
    event_data: &mut Vec<Vec<u8>>,
    request_id: u64,
) -> Result<Option<Value>, McpTransportError> {
    if line.is_empty() {
        return dispatch_sse_event(event_data, request_id);
    }
    if line.first() == Some(&b':') {
        return Ok(None);
    }
    let (field, value) = line
        .iter()
        .position(|byte| *byte == b':')
        .map(|index| (&line[..index], &line[index + 1..]))
        .map(|(field, value)| {
            let value = value.strip_prefix(b" ").unwrap_or(value);
            (field, value)
        })
        .unwrap_or((line, &[]));
    if field == b"data" {
        event_data.push(value.to_vec());
    }
    Ok(None)
}

fn dispatch_sse_event(
    event_data: &mut Vec<Vec<u8>>,
    request_id: u64,
) -> Result<Option<Value>, McpTransportError> {
    if event_data.is_empty() {
        return Ok(None);
    }
    let mut data = Vec::new();
    for (index, line) in event_data.drain(..).enumerate() {
        if index > 0 {
            data.push(b'\n');
        }
        data.extend_from_slice(&line);
    }
    let message = serde_json::from_slice::<Value>(&data)
        .map_err(|error| McpTransportError::Protocol(error.to_string()))?;
    if message.get("id").is_none() && message.get("method").is_some() {
        if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(McpTransportError::Protocol(
                "MCP SSE notification omitted jsonrpc 2.0".to_owned(),
            ));
        }
        return Ok(None);
    }
    decode_jsonrpc_message(message, request_id).map(Some)
}

fn decode_jsonrpc_message(message: Value, request_id: u64) -> Result<Value, McpTransportError> {
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(McpTransportError::Protocol(
            "MCP response omitted jsonrpc 2.0".to_owned(),
        ));
    }
    if message.get("id").is_some() && message.get("method").is_some() {
        return Err(McpTransportError::Protocol(
            "MCP server initiated an independent request on a response stream".to_owned(),
        ));
    }
    if message.get("id").and_then(Value::as_u64) != Some(request_id) {
        return Err(McpTransportError::Protocol(format!(
            "MCP server returned an unexpected response id while waiting for {request_id}"
        )));
    }
    if let Some(error) = message.get("error") {
        return Err(McpTransportError::Rpc {
            code: error.get("code").and_then(Value::as_i64).unwrap_or(-32000),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| error.to_string()),
        });
    }
    message.get("result").cloned().ok_or_else(|| {
        McpTransportError::Protocol("MCP response omitted both result and error".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::mcp_protocol::McpTransportFactory;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{timeout, Duration};

    fn request(id: u64, method: &str, params: Value) -> McpTransportRequest {
        McpTransportRequest {
            id,
            method: method.to_owned(),
            params,
            parameter_headers: BTreeMap::new(),
        }
    }

    fn modern_params(value: Value) -> Value {
        let mut value = value.as_object().cloned().unwrap();
        value.insert(
            "_meta".to_owned(),
            json!({
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {}
            }),
        );
        Value::Object(value)
    }

    async fn serve_once(response: Vec<Vec<u8>>) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            for chunk in response {
                socket.write_all(&chunk).await.unwrap();
                tokio::task::yield_now().await;
            }
            request
        });
        (format!("http://{address}/mcp"), handle)
    }

    async fn read_http_request(socket: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        let header_end = loop {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            if let Some(index) = request.windows(4).position(|value| value == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
            })
            .unwrap();
        while request.len() < header_end + content_length {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
        }
        request
    }

    #[test]
    fn header_encoding_matches_the_base64_sentinel_contract() {
        assert_eq!(encode_header_value("us-west1"), "us-west1");
        assert_eq!(
            encode_header_value("Hello, 世界"),
            "=?base64?SGVsbG8sIOS4lueVjA==?="
        );
        assert_eq!(encode_header_value(" padded "), "=?base64?IHBhZGRlZCA=?=");
        assert_eq!(
            encode_header_value("=?base64?literal?="),
            "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="
        );
    }

    #[test]
    fn two_thousand_five_hundred_model_ssrf_mutations_cannot_change_the_host_endpoint() {
        const MUTATIONS: usize = 2_500;
        let endpoint = "https://mcp.example.test/fixed";
        let factory = StreamableHttpMcpTransportFactory::new(
            StreamableHttpMcpTransportConfig::unauthenticated(endpoint),
        )
        .unwrap();
        let transport = StreamableHttpMcpTransport {
            client: factory.client.clone(),
            endpoint: factory.endpoint.clone(),
            configured_headers: factory.configured_headers.clone(),
            max_frame_bytes: factory.max_frame_bytes,
        };
        let suffixes = [
            "Host",
            "Forwarded",
            "X-Forwarded-Host",
            "Content-Type",
            "Mcp-Protocol-Version",
        ];
        for index in 0..MUTATIONS {
            let attacker_target = format!("http://169.254.169.254/latest/{index}");
            let suffix = suffixes[index % suffixes.len()];
            let request = transport
                .build_request(McpTransportRequest {
                    id: index as u64 + 1,
                    method: "tools/call".to_owned(),
                    params: json!({
                        "name": "inspect",
                        "arguments": {
                            "url": attacker_target,
                            "endpoint": attacker_target,
                            "redirect": attacker_target,
                            "proxy": attacker_target
                        },
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": MCP_STATELESS_PROTOCOL_2026_07_28
                        }
                    }),
                    parameter_headers: BTreeMap::from([(
                        suffix.to_owned(),
                        attacker_target.clone(),
                    )]),
                })
                .unwrap();
            assert_eq!(request.url().as_str(), endpoint);
            assert_eq!(request.method(), reqwest::Method::POST);
            assert!(request.headers().get("host").is_none());
            assert_eq!(
                request
                    .headers()
                    .get(format!("mcp-param-{suffix}"))
                    .unwrap()
                    .to_str()
                    .unwrap(),
                attacker_target
            );
        }
        assert_eq!(
            factory.authority.network_targets,
            BTreeSet::from([endpoint.to_owned()])
        );
    }

    #[tokio::test]
    async fn json_exchange_sends_required_and_parameter_headers() {
        let body = br#"{"jsonrpc":"2.0","id":7,"result":{"resultType":"complete"}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let (endpoint, captured) = serve_once(vec![response.into_bytes(), body.to_vec()]).await;
        let factory = StreamableHttpMcpTransportFactory::new(
            StreamableHttpMcpTransportConfig::unauthenticated(endpoint),
        )
        .unwrap();
        let connection = factory.connect().await.unwrap();
        let mut request = request(
            7,
            "tools/call",
            modern_params(json!({"name": "echo", "arguments": {"region": "世界"}})),
        );
        request
            .parameter_headers
            .insert("Region".to_owned(), "世界".to_owned());
        assert_eq!(
            connection
                .request(request, CancellationToken::new())
                .await
                .unwrap(),
            json!({"resultType": "complete"})
        );
        let captured = String::from_utf8(captured.await.unwrap()).unwrap();
        let lowercase = captured.to_ascii_lowercase();
        assert!(lowercase.contains("mcp-protocol-version: 2026-07-28\r\n"));
        assert!(lowercase.contains("mcp-method: tools/call\r\n"));
        assert!(lowercase.contains("mcp-name: echo\r\n"));
        assert!(lowercase.contains("mcp-param-region: =?base64?5liw55wm?=\r\n"));
    }

    #[tokio::test]
    async fn fragmented_sse_ignores_notifications_and_returns_final_response() {
        let chunks = vec![
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n: keepalive\r\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\r\n\r\n"
                .to_vec(),
            b"data: {\"jsonrpc\":\"2.0\",\"id\":9,\"result\":{\"resultType\":\"complete\",\"content\":[]}}\r\n\r\n"
                .to_vec(),
        ];
        let (endpoint, captured) = serve_once(chunks).await;
        let factory = StreamableHttpMcpTransportFactory::new(
            StreamableHttpMcpTransportConfig::unauthenticated(endpoint),
        )
        .unwrap();
        let connection = factory.connect().await.unwrap();
        let result = connection
            .request(
                request(9, "tools/list", modern_params(json!({}))),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result["resultType"], "complete");
        captured.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_body_is_rejected_before_decode() {
        let body = vec![b'x'; 2048];
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let (endpoint, captured) = serve_once(vec![response.into_bytes(), body]).await;
        let mut config = StreamableHttpMcpTransportConfig::unauthenticated(endpoint);
        config.max_frame_bytes = 1024;
        let factory = StreamableHttpMcpTransportFactory::new(config).unwrap();
        let connection = factory.connect().await.unwrap();
        let error = connection
            .request(
                request(1, "tools/list", modern_params(json!({}))),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("1024 byte limit"));
        captured.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_drops_only_the_in_flight_sse_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (headers_sent, headers_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http_request(&mut socket).await;
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n: waiting\r\n\r\n",
                )
                .await
                .unwrap();
            headers_sent.send(()).unwrap();
            let mut byte = [0_u8; 1];
            match timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .expect("cancelled response stream should close within one second")
            {
                Ok(0) => true,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::BrokenPipe
                    ) =>
                {
                    true
                }
                _ => false,
            }
        });
        let factory = StreamableHttpMcpTransportFactory::new(
            StreamableHttpMcpTransportConfig::unauthenticated(format!("http://{address}/mcp")),
        )
        .unwrap();
        let connection = factory.connect().await.unwrap();
        let cancellation = CancellationToken::new();
        let request_cancellation = cancellation.clone();
        let client = tokio::spawn(async move {
            connection
                .request(
                    request(11, "tools/list", modern_params(json!({}))),
                    request_cancellation,
                )
                .await
        });
        headers_received.await.unwrap();
        cancellation.cancel();
        let error = timeout(Duration::from_secs(1), client)
            .await
            .expect("cancelled request should return within one second")
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert!(server.await.unwrap());
    }

    #[tokio::test]
    async fn redirects_are_not_followed_for_post_requests() {
        let body = br#"{"jsonrpc":"2.0","id":12,"result":{}}"#;
        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirected\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let (endpoint, captured) = serve_once(vec![response.into_bytes(), body.to_vec()]).await;
        let factory = StreamableHttpMcpTransportFactory::new(
            StreamableHttpMcpTransportConfig::unauthenticated(endpoint),
        )
        .unwrap();
        let connection = factory.connect().await.unwrap();
        let error = connection
            .request(
                request(12, "tools/list", modern_params(json!({}))),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("status 307"));
        captured.await.unwrap();
    }

    #[test]
    fn configured_headers_cannot_override_protocol_routing() {
        let mut config =
            StreamableHttpMcpTransportConfig::unauthenticated("https://example.com/mcp");
        config.credential_headers.insert(
            "Mcp-Method".to_owned(),
            ResolvedCredentialHeader {
                reference: "env:MCP_TOKEN".to_owned(),
                value: "other".to_owned(),
            },
        );
        assert!(StreamableHttpMcpTransportFactory::new(config).is_err());
    }
}
