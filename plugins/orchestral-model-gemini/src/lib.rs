//! Gemini-native HTTP adapter for the canonical Orchestral Model Protocol.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::error::Error as _;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{stream, StreamExt};
use google_cloud_auth::credentials::service_account::{
    AccessSpecifier, Builder as ServiceAccountBuilder,
};
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder as GoogleCredentialsBuilder};
use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::model_protocol::{
    ModelBackend, ModelCapabilities, ModelContent, ModelContextEstimate, ModelDescriptor,
    ModelError, ModelErrorCode, ModelEvent, ModelEventId, ModelFinishReason, ModelMessage,
    ModelRequest, ModelRequestId, ModelRole, ModelStream, ModelStreamEvent, ModelTokenAccounting,
    ModelTokenMeter, ModelTokenMeterDescriptor, ModelToolCallId, ModelToolDefinition, ModelUsage,
};
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

mod metadata;

const GOOGLE_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const GEMINI_THOUGHT_SIGNATURE_EXTENSION: &str = "google.gemini/thought_signature";
const GEMINI_FUNCTION_CALL_ID_EXTENSION: &str = "google.gemini/function_call_id";

#[async_trait]
pub trait GeminiAccessTokenProvider: Send + Sync {
    async fn access_token(&self) -> Result<String, ModelError>;
}

#[derive(Clone)]
pub enum GeminiAuthentication {
    ApiKey(String),
    BearerToken(String),
    AccessTokenProvider(Arc<dyn GeminiAccessTokenProvider>),
}

impl GeminiAuthentication {
    fn is_valid(&self) -> bool {
        match self {
            Self::ApiKey(value) | Self::BearerToken(value) => !value.trim().is_empty(),
            Self::AccessTokenProvider(_) => true,
        }
    }

    async fn apply(
        &self,
        request: RequestBuilder,
        cancellation: &CancellationToken,
    ) -> Result<RequestBuilder, ModelError> {
        match self {
            Self::ApiKey(value) => Ok(request.header("x-goog-api-key", value)),
            Self::BearerToken(value) => Ok(request.bearer_auth(value)),
            Self::AccessTokenProvider(provider) => {
                let token = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(cancelled_error()),
                    token = provider.access_token() => token?,
                };
                if token.trim().is_empty() {
                    return Err(ModelError::new(
                        ModelErrorCode::Authentication,
                        "Google access-token provider returned an empty token",
                    ));
                }
                Ok(request.bearer_auth(token))
            }
        }
    }
}

/// Google ADC/service-account bridge for Gemini's dynamic bearer-token mode.
///
/// The Google SDK owns token caching and refresh. The Gemini adapter asks for a
/// token for every model request, so long-running Agent sessions do not retain an
/// expired access token.
#[derive(Clone, Debug)]
pub struct GoogleCloudAccessTokenProvider {
    credentials: AccessTokenCredentials,
}

impl GoogleCloudAccessTokenProvider {
    /// Resolve Google's standard Application Default Credentials chain.
    pub fn application_default() -> Result<Self, ModelError> {
        let credentials = GoogleCredentialsBuilder::default()
            .with_scopes([GOOGLE_CLOUD_PLATFORM_SCOPE])
            .build_access_token_credentials()
            .map_err(|error| {
                google_authentication_error("resolve Application Default Credentials", error)
            })?;
        Ok(Self { credentials })
    }

    /// Load an explicitly supplied service-account key without changing process
    /// environment variables or exposing the key outside this provider.
    pub fn from_service_account_file(path: impl AsRef<Path>) -> Result<Self, ModelError> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|error| {
            google_authentication_error(
                &format!("read Google credential file '{}'", path.display()),
                error,
            )
        })?;
        let value = serde_json::from_slice(&bytes).map_err(|error| {
            google_authentication_error(
                &format!("parse Google credential file '{}'", path.display()),
                error,
            )
        })?;
        let credentials = ServiceAccountBuilder::new(value)
            .with_access_specifier(AccessSpecifier::from_scopes([GOOGLE_CLOUD_PLATFORM_SCOPE]))
            .build_access_token_credentials()
            .map_err(|error| {
                google_authentication_error(
                    &format!("load service-account credential '{}'", path.display()),
                    error,
                )
            })?;
        Ok(Self { credentials })
    }
}

#[async_trait]
impl GeminiAccessTokenProvider for GoogleCloudAccessTokenProvider {
    async fn access_token(&self) -> Result<String, ModelError> {
        self.credentials
            .access_token()
            .await
            .map(|token| token.token)
            .map_err(|error| google_authentication_error("acquire Google access token", error))
    }
}

fn google_authentication_error(context: &str, error: impl std::fmt::Display) -> ModelError {
    ModelError::new(
        ModelErrorCode::Authentication,
        format!("{context}: {error}"),
    )
}

#[derive(Clone, Copy)]
pub enum GeminiThinkingLevel {
    Minimal,
    Low,
    Medium,
    High,
}

impl GeminiThinkingLevel {
    fn as_wire_value(self) -> &'static str {
        match self {
            Self::Minimal => "MINIMAL",
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
        }
    }
}

#[derive(Clone)]
pub struct GeminiModelConfig {
    pub backend_id: String,
    pub endpoint: String,
    pub authentication: GeminiAuthentication,
    pub model: String,
    pub temperature: f32,
    pub thinking_level: Option<GeminiThinkingLevel>,
    pub default_max_output_tokens: u64,
    pub max_context_tokens: Option<u64>,
    /// Maximum time without response activity. Streaming generations have no
    /// total deadline and may legitimately run longer than this duration.
    pub timeout: Duration,
    pub max_buffered_events: usize,
}

impl GeminiModelConfig {
    pub fn validate(&self) -> Result<(), ModelError> {
        let model = self.model.strip_prefix("models/").unwrap_or(&self.model);
        if self.backend_id.trim().is_empty()
            || self.endpoint.trim().is_empty()
            || !self.authentication.is_valid()
            || model.is_empty()
            || !model.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
            })
            || !(0.0..=2.0).contains(&self.temperature)
            || self.default_max_output_tokens == 0
            || self.max_buffered_events == 0
            || self.timeout.is_zero()
        {
            return Err(ModelError::invalid_request(
                "invalid Gemini ModelBackend configuration",
            ));
        }
        Ok(())
    }

    fn stream_url(&self) -> String {
        let model = self.model.strip_prefix("models/").unwrap_or(&self.model);
        format!(
            "{}/models/{model}:streamGenerateContent?alt=sse",
            self.endpoint.trim_end_matches('/')
        )
    }
}

pub struct GeminiModelBackend {
    client: Arc<RwLock<Client>>,
    config: GeminiModelConfig,
    metadata: tokio::sync::OnceCell<Option<metadata::ResolvedMetadata>>,
    metadata_cache: Option<std::path::PathBuf>,
    metadata_discovery: bool,
    background_metadata_refresh: bool,
    include_thoughts: bool,
    response_header_timeout: Duration,
    catalog_url: Option<String>,
}

fn build_http_client(config: &GeminiModelConfig) -> Result<Client, ModelError> {
    Client::builder()
        .connect_timeout(config.timeout.min(Duration::from_secs(30)))
        .read_timeout(config.timeout)
        .tcp_keepalive(Duration::from_secs(15))
        .tcp_keepalive_interval(Duration::from_secs(10))
        .tcp_keepalive_retries(3)
        .pool_idle_timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(1)
        .build()
        .map_err(|error| ModelError::new(ModelErrorCode::Internal, error.to_string()))
}

impl GeminiModelBackend {
    pub fn new(config: GeminiModelConfig) -> Result<Self, ModelError> {
        config.validate()?;
        let client = build_http_client(&config)?;
        // Custom gateways own their model identities. Do not infer their
        // capacities from a public provider's catalog.
        let catalog_url = metadata::public_google_endpoint(&config.endpoint)
            .then(|| "https://models.dev/api.json".to_owned());
        Ok(Self {
            client: Arc::new(RwLock::new(client)),
            response_header_timeout: config.timeout,
            config,
            metadata: tokio::sync::OnceCell::new(),
            metadata_cache: None,
            metadata_discovery: true,
            background_metadata_refresh: false,
            include_thoughts: false,
            catalog_url,
        })
    }

    pub(crate) fn http_client(&self) -> Client {
        self.client
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn reset_http_client(&self) {
        if let Ok(fresh) = build_http_client(&self.config) {
            *self
                .client
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = fresh;
        }
    }

    /// Persist public capacity metadata between processes. No credentials or
    /// prompts are stored or sent to the model catalog.
    pub fn with_metadata_cache(mut self, directory: impl Into<std::path::PathBuf>) -> Self {
        self.metadata_cache = Some(directory.into());
        self
    }

    /// Disable optional model metadata requests, retaining explicit capacity.
    pub fn with_metadata_discovery(mut self, enabled: bool) -> Self {
        self.metadata_discovery = enabled;
        self
    }

    /// Resolve from a local snapshot immediately and refresh the persistent
    /// cache in the background. Updated capacities apply to future backend
    /// instances; an active backend's recovery identity remains unchanged.
    pub fn with_background_metadata_refresh(mut self) -> Self {
        self.background_metadata_refresh = true;
        self
    }

    /// Select an optional model catalog used when native metadata lacks limits.
    pub fn with_model_catalog(mut self, url: Option<String>) -> Self {
        self.catalog_url = url;
        self
    }

    /// Request user-visible thought summaries without changing the model's
    /// thinking effort or budget. Models must support includeThoughts.
    pub fn with_thought_summaries(mut self, enabled: bool) -> Self {
        self.include_thoughts = enabled;
        self
    }

    /// Bound waiting for HTTP response headers independently of streamed body
    /// reads. This is not a total generation deadline; the SDK defaults to the
    /// configured idle timeout until the Host supplies a separate ceiling.
    pub fn with_response_header_timeout(mut self, timeout: Duration) -> Result<Self, ModelError> {
        if timeout.is_zero() {
            return Err(ModelError::invalid_request(
                "response header timeout must be positive",
            ));
        }
        self.response_header_timeout = timeout;
        Ok(self)
    }

    fn build_request_body(&self, request: &ModelRequest) -> Result<Value, ModelError> {
        let (system, contents) = encode_messages(&request.messages)?;
        let mut generation = Map::from_iter([
            ("temperature".to_owned(), json!(self.config.temperature)),
            (
                "maxOutputTokens".to_owned(),
                json!(request
                    .max_output_tokens
                    .unwrap_or(self.config.default_max_output_tokens)),
            ),
        ]);
        let mut thinking = Map::new();
        if self.include_thoughts {
            thinking.insert("includeThoughts".to_owned(), json!(true));
        }
        if let Some(thinking_level) = self.config.thinking_level {
            thinking.insert(
                "thinkingLevel".to_owned(),
                json!(thinking_level.as_wire_value()),
            );
        }
        if !thinking.is_empty() {
            generation.insert("thinkingConfig".to_owned(), Value::Object(thinking));
        }
        if let Some(schema) = &request.output_schema {
            generation.insert(
                "responseMimeType".to_owned(),
                Value::String("application/json".to_owned()),
            );
            generation.insert("responseJsonSchema".to_owned(), schema.clone());
        }
        let mut body = Map::from_iter([
            ("contents".to_owned(), Value::Array(contents)),
            ("generationConfig".to_owned(), Value::Object(generation)),
        ]);
        if !system.is_empty() {
            body.insert(
                "systemInstruction".to_owned(),
                json!({"parts": system.into_iter().map(|text| json!({"text": text})).collect::<Vec<_>>() }),
            );
        }
        if !request.tools.is_empty() {
            body.insert(
                "tools".to_owned(),
                json!([{
                    "functionDeclarations": request.tools.iter().map(|tool| json!({
                        "name": tool.name,
                        "description": tool.description,
                        "parametersJsonSchema": tool.input_schema,
                    })).collect::<Vec<_>>()
                }]),
            );
        }
        Ok(Value::Object(body))
    }
}

impl ModelTokenMeter for GeminiModelBackend {
    fn supports_observed_prefix_estimation(&self) -> bool {
        true
    }

    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        let config = serde_json::to_vec(&(
            &self.config.model,
            self.config.temperature.to_bits(),
            self.config
                .thinking_level
                .map(GeminiThinkingLevel::as_wire_value),
            self.config.default_max_output_tokens,
            self.include_thoughts,
            "request-scoped-gemini-call-identity/v1",
            3_u64,
            "observed-prefix-input-delta/v1",
        ))
        .expect("Gemini token meter scalar configuration is serializable");
        ModelTokenMeterDescriptor {
            strategy: "google-gemini/wire-json-upper-bound".to_owned(),
            version: "3".to_owned(),
            accounting: ModelTokenAccounting::ConservativeUpperBound,
            config_digest: Digest::sha256(config),
        }
    }

    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        let request = ModelRequest {
            request_id: ModelRequestId::new("token-meter"),
            messages: messages.to_vec(),
            tools: tools.to_vec(),
            output_schema: None,
            max_output_tokens: None,
            extensions: BTreeMap::new(),
        };
        let body = self.build_request_body(&request)?;
        let wire_bytes = serde_json::to_vec(&body)
            .map_err(|error| ModelError::invalid_request(error.to_string()))?
            .len() as u64;
        Ok(wire_bytes
            .saturating_add(64)
            .saturating_add((messages.len() as u64).saturating_mul(16))
            .saturating_add((tools.len() as u64).saturating_mul(32)))
    }

    fn estimate_context_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<ModelContextEstimate, ModelError> {
        let framing = 64_u64
            .saturating_add((messages.len() as u64).saturating_mul(16))
            .saturating_add((tools.len() as u64).saturating_mul(32));
        // Planning only: subsequent requests are calibrated against observed
        // provider input usage by the runtime's verified-prefix meter. Keep the
        // byte upper bound unchanged for hard resource/cost reservations.
        let bytes = self
            .count_request_input(messages, tools)?
            .saturating_sub(framing);
        Ok(ModelContextEstimate {
            tokens: bytes.div_ceil(3).saturating_add(framing),
            accounting: ModelTokenAccounting::Estimated,
        })
    }
}

#[async_trait]
impl ModelBackend for GeminiModelBackend {
    fn descriptor(&self) -> ModelDescriptor {
        let metadata = self.metadata.get().and_then(Option::as_ref);
        let mut extensions = BTreeMap::from([(
            "google-gemini/model".to_owned(),
            Value::String(self.config.model.clone()),
        )]);
        if let Some(metadata) = metadata {
            extensions.insert(
                "google-gemini/model_metadata".to_owned(),
                metadata.description(),
            );
            extensions.insert(
                "orchestral/context-capacity-source".to_owned(),
                Value::String(metadata.source.clone()),
            );
        }
        ModelDescriptor {
            backend_id: self.config.backend_id.clone(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                parallel_tool_calls: true,
                structured_output: true,
                max_context_tokens: self
                    .config
                    .max_context_tokens
                    .or_else(|| metadata.map(|value| value.context_tokens)),
                max_output_tokens: metadata.and_then(|value| value.output_tokens),
            },
            extensions,
        }
    }

    async fn resolve_descriptor(&self) -> Result<ModelDescriptor, ModelError> {
        if self.metadata_discovery && self.config.max_context_tokens.is_none() {
            self.metadata
                .get_or_init(|| async {
                    if self.background_metadata_refresh {
                        metadata::local_snapshot_and_refresh(self).await
                    } else {
                        metadata::discover(self).await
                    }
                })
                .await;
        }
        let descriptor = self.descriptor();
        descriptor.validate()?;
        Ok(descriptor)
    }

    async fn start(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        self.descriptor().validate()?;
        let body = self.build_request_body(&request)?;
        let started = Instant::now();
        let client = self.http_client();
        let http_request = self
            .config
            .authentication
            .apply(client.post(self.config.stream_url()), &cancellation)
            .await
            .map_err(|error| request_timing(error, "authentication", started))?
            .json(&body);
        tracing::debug!(request_id = %request.request_id, elapsed_ms = started.elapsed().as_millis(), "Gemini authentication complete");
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled_error()),
            response = tokio::time::timeout(self.response_header_timeout, http_request.send()) => response,
        }
        .map_err(|_| {
            self.reset_http_client();
            request_timing(
                ModelError::new(ModelErrorCode::Unavailable, format!(
                    "Gemini response headers were not received within {:?}", self.response_header_timeout
                )).with_retryable(true).with_details(json!({
                    "kind":"timeout", "timeout":true,
                    "response_header_timeout_ms":self.response_header_timeout.as_millis() as u64,
                })),
                "response_headers", started,
            )
        })?
        .map_err(|error| {
            self.reset_http_client();
            request_timing(map_transport_error(error), "response_headers", started)
        })?;
        let status = response.status();
        tracing::debug!(request_id = %request.request_id, elapsed_ms = started.elapsed().as_millis(), %status, "Gemini response headers received");
        if !status.is_success() {
            let bytes = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(cancelled_error()),
                bytes = response.bytes() => bytes,
            }
            .map_err(|error| request_timing(map_transport_error(error), "error_body", started))?;
            return Err(request_timing(
                map_http_error(status, &bytes),
                "response_headers",
                started,
            ));
        }
        Ok(gemini_event_stream(
            request,
            response.bytes_stream().boxed(),
            cancellation,
            self.config.max_buffered_events,
        ))
    }
}

fn request_timing(mut error: ModelError, phase: &str, started: Instant) -> ModelError {
    let mut details = match error.details {
        Value::Object(details) => details,
        Value::Null => Map::new(),
        original => Map::from_iter([("cause".to_owned(), original)]),
    };
    details.insert("phase".to_owned(), json!(phase));
    details.insert(
        "elapsed_ms".to_owned(),
        json!(started.elapsed().as_millis() as u64),
    );
    error.details = Value::Object(details);
    error
}

const MAX_SSE_FRAME_BYTES: usize = 1024 * 1024;

type HttpByteStream = BoxStream<'static, Result<Bytes, reqwest::Error>>;

struct SseDecoder {
    buffer: Vec<u8>,
    data_lines: Vec<String>,
    frames: VecDeque<String>,
    max_frames: usize,
}

impl SseDecoder {
    fn new(max_frames: usize) -> Self {
        Self {
            buffer: Vec::new(),
            data_lines: Vec::new(),
            frames: VecDeque::new(),
            max_frames,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Result<(), ModelError> {
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() > MAX_SSE_FRAME_BYTES {
            return Err(ModelError::protocol(
                "Gemini SSE frame exceeds the Host limit",
            ));
        }
        while let Some(position) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = self.buffer.drain(..=position).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.process_line(&line)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), ModelError> {
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.process_line(&line)?;
        }
        self.dispatch_frame()?;
        Ok(())
    }

    fn process_line(&mut self, line: &[u8]) -> Result<(), ModelError> {
        let line = std::str::from_utf8(line)
            .map_err(|error| ModelError::protocol(format!("Gemini SSE is not UTF-8: {error}")))?;
        if line.is_empty() {
            self.dispatch_frame()?;
        } else if !line.starts_with(':') {
            if let Some(data) = line.strip_prefix("data:") {
                let data = data.strip_prefix(' ').unwrap_or(data);
                self.data_lines.push(data.to_owned());
                if self.data_lines.iter().map(String::len).sum::<usize>() > MAX_SSE_FRAME_BYTES {
                    return Err(ModelError::protocol(
                        "Gemini SSE data exceeds the Host limit",
                    ));
                }
            }
        }
        Ok(())
    }

    fn dispatch_frame(&mut self) -> Result<(), ModelError> {
        if !self.data_lines.is_empty() {
            if self.frames.len() >= self.max_frames {
                return Err(ModelError::protocol(
                    "Gemini SSE buffered-event limit exceeded",
                ));
            }
            self.frames.push_back(self.data_lines.join("\n"));
            self.data_lines.clear();
        }
        Ok(())
    }
}

struct GeminiStreamState {
    request_id: orchestral_core::model_protocol::ModelRequestId,
    bytes: HttpByteStream,
    cancellation: CancellationToken,
    decoder: SseDecoder,
    pending: VecDeque<Result<ModelStreamEvent, ModelError>>,
    sequence: u64,
    next_call: u64,
    finish_reason: Option<ModelFinishReason>,
    raw_finish_reason: Option<String>,
    prompt_block_reason: Option<String>,
    last_usage: Option<ModelUsage>,
    emitted_content: bool,
    terminated: bool,
    max_buffered_events: usize,
    started: Instant,
    received_body_bytes: u64,
    last_bytes_at: Option<Instant>,
}

impl GeminiStreamState {
    fn emit(&mut self, payload: ModelEvent) -> Result<(), ModelError> {
        if self.pending.len() >= self.max_buffered_events {
            return Err(ModelError::protocol(
                "Gemini canonical-event buffer limit exceeded",
            ));
        }
        self.sequence += 1;
        self.pending.push_back(Ok(ModelStreamEvent {
            request_id: self.request_id.clone(),
            event_id: ModelEventId::new(format!(
                "{}-gemini-{}",
                self.request_id.as_str(),
                self.sequence
            )),
            sequence: self.sequence,
            payload,
        }));
        Ok(())
    }

    fn fail(&mut self, error: ModelError) {
        self.terminated = true;
        let mut error = request_timing(error, "response_body", self.started);
        if let Some(details) = error.details.as_object_mut() {
            details.insert(
                "received_body_bytes".to_owned(),
                json!(self.received_body_bytes),
            );
            details.insert("content_received".to_owned(), json!(self.emitted_content));
            details.insert(
                "idle_ms".to_owned(),
                json!(self
                    .last_bytes_at
                    .unwrap_or(self.started)
                    .elapsed()
                    .as_millis() as u64),
            );
        }
        self.pending.push_back(Err(error));
    }

    fn handle_frame(&mut self, frame: String) -> Result<(), ModelError> {
        if frame.trim() == "[DONE]" {
            return self.finalize();
        }
        let response: Value = serde_json::from_str(&frame)
            .map_err(|error| ModelError::protocol(format!("invalid Gemini SSE JSON: {error}")))?;
        if let Some(error) = response.get("error") {
            return Err(ModelError::new(
                ModelErrorCode::Unavailable,
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Gemini returned an error"),
            ));
        }
        if let Some(block_reason) = response
            .pointer("/promptFeedback/blockReason")
            .and_then(Value::as_str)
            .filter(|reason| !reason.is_empty())
        {
            self.prompt_block_reason = Some(block_reason.to_owned());
            self.finish_reason = Some(ModelFinishReason::ContentFilter);
        }
        if let Some(candidate) = response
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|candidates| candidates.first())
        {
            for part in candidate
                .pointer("/content/parts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(text) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    if part.get("thought").and_then(Value::as_bool) == Some(true) {
                        self.emit(ModelEvent::ReasoningSummaryDelta {
                            delta: text.to_owned(),
                        })?;
                    } else {
                        self.emitted_content = true;
                        self.emit(ModelEvent::TextDelta {
                            delta: text.to_owned(),
                        })?;
                    }
                }
                if part.get("functionCall").is_some() {
                    self.emit_function_call(part)?;
                }
            }
            if let Some(reason) = candidate
                .get("finishReason")
                .and_then(Value::as_str)
                .filter(|reason| !reason.is_empty())
            {
                self.raw_finish_reason = Some(reason.to_owned());
                self.finish_reason = Some(map_finish_reason(reason));
            }
        }
        if let Some(usage) = response.get("usageMetadata") {
            let usage = parse_usage_metadata(usage);
            if self.last_usage.as_ref() != Some(&usage) {
                self.last_usage = Some(usage.clone());
                self.emit(ModelEvent::Usage { usage })?;
            }
        }
        Ok(())
    }

    fn emit_function_call(&mut self, part: &Value) -> Result<(), ModelError> {
        let call = part
            .get("functionCall")
            .ok_or_else(|| ModelError::protocol("Gemini Tool-call part omitted functionCall"))?;
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| ModelError::protocol("Gemini functionCall omitted name"))?;
        self.next_call += 1;
        let native_id = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let id = native_id
            .map(|id| {
                format!(
                    "{}-gemini-native-{}",
                    self.request_id,
                    Digest::sha256(id.as_bytes())
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "{}-gemini-call-{}",
                    self.request_id.as_str(),
                    self.next_call
                )
            });
        let arguments =
            serde_json::to_string(call.get("args").unwrap_or(&Value::Object(Map::new())))
                .map_err(|error| ModelError::protocol(error.to_string()))?;
        let call_id = ModelToolCallId::new(id);
        let mut extensions = part
            .get("thoughtSignature")
            .and_then(Value::as_str)
            .filter(|signature| !signature.is_empty())
            .map(|signature| {
                BTreeMap::from([(
                    GEMINI_THOUGHT_SIGNATURE_EXTENSION.to_owned(),
                    Value::String(signature.to_owned()),
                )])
            })
            .unwrap_or_default();
        if let Some(native_id) = native_id {
            extensions.insert(
                GEMINI_FUNCTION_CALL_ID_EXTENSION.to_owned(),
                json!(native_id),
            );
        }
        self.emitted_content = true;
        self.emit(ModelEvent::ToolCallStart {
            call_id: call_id.clone(),
            name: name.to_owned(),
            extensions,
        })?;
        self.emit(ModelEvent::ToolCallArgumentsDelta {
            call_id: call_id.clone(),
            delta: arguments,
        })?;
        self.emit(ModelEvent::ToolCallEnd { call_id })?;
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ModelError> {
        if self.terminated {
            return Ok(());
        }
        let mut reason = self
            .finish_reason
            .clone()
            .unwrap_or(ModelFinishReason::Stop);
        if self.prompt_block_reason.is_some() {
            reason = ModelFinishReason::ContentFilter;
        }
        if self.next_call > 0 && reason == ModelFinishReason::Stop {
            reason = ModelFinishReason::ToolCalls;
        }
        if !self.emitted_content
            && !matches!(
                reason,
                ModelFinishReason::ContentFilter | ModelFinishReason::Length
            )
        {
            return Err(empty_generation_error(
                self.raw_finish_reason.as_deref(),
                self.last_usage.as_ref(),
            ));
        }
        self.emit(ModelEvent::Finish { reason })?;
        self.terminated = true;
        Ok(())
    }
}

fn gemini_stream_state(
    request: ModelRequest,
    bytes: HttpByteStream,
    cancellation: CancellationToken,
    max_buffered_events: usize,
) -> GeminiStreamState {
    GeminiStreamState {
        request_id: request.request_id,
        bytes,
        cancellation,
        decoder: SseDecoder::new(max_buffered_events),
        pending: VecDeque::new(),
        sequence: 0,
        next_call: 0,
        finish_reason: None,
        raw_finish_reason: None,
        prompt_block_reason: None,
        last_usage: None,
        emitted_content: false,
        terminated: false,
        max_buffered_events,
        started: Instant::now(),
        received_body_bytes: 0,
        last_bytes_at: None,
    }
}

fn gemini_event_stream(
    request: ModelRequest,
    bytes: HttpByteStream,
    cancellation: CancellationToken,
    max_buffered_events: usize,
) -> ModelStream {
    let state = gemini_stream_state(request, bytes, cancellation, max_buffered_events);
    stream::unfold(state, |mut state| async move {
        loop {
            if let Some(item) = state.pending.pop_front() {
                return Some((item, state));
            }
            if state.terminated {
                return None;
            }
            if let Some(frame) = state.decoder.frames.pop_front() {
                if let Err(error) = state.handle_frame(frame) {
                    state.fail(error);
                }
                continue;
            }
            let next = tokio::select! {
                biased;
                _ = state.cancellation.cancelled() => {
                    state.fail(cancelled_error());
                    continue;
                }
                next = state.bytes.next() => next,
            };
            match next {
                Some(Ok(chunk)) => {
                    if !chunk.is_empty() {
                        if state.received_body_bytes == 0 {
                            tracing::debug!(request_id = %state.request_id, elapsed_ms = state.started.elapsed().as_millis(), "Gemini first response bytes received after headers");
                        }
                        state.received_body_bytes = state.received_body_bytes.saturating_add(chunk.len() as u64);
                        state.last_bytes_at = Some(Instant::now());
                    }
                    if let Err(error) = state.decoder.push(&chunk) {
                        state.fail(error);
                    }
                }
                Some(Err(error)) => state.fail(map_transport_error(error)),
                None => {
                    if let Err(error) = state.decoder.finish() {
                        state.fail(error);
                    } else if state.decoder.frames.is_empty() {
                        if let Err(error) = state.finalize() {
                            state.fail(error);
                        }
                    }
                }
            }
        }
    })
    .boxed()
}

fn map_finish_reason(reason: &str) -> ModelFinishReason {
    match reason {
        "STOP" => ModelFinishReason::Stop,
        "MAX_TOKENS" => ModelFinishReason::Length,
        "SAFETY" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII" => ModelFinishReason::ContentFilter,
        _ => ModelFinishReason::Other,
    }
}

fn encode_messages(messages: &[ModelMessage]) -> Result<(Vec<String>, Vec<Value>), ModelError> {
    let mut system = Vec::new();
    let mut contents = Vec::new();
    let mut function_names = HashMap::<String, (String, String)>::new();
    for message in messages {
        match message.role {
            ModelRole::System => system.push(flatten_text(&message.content)?),
            ModelRole::User => contents.push(json!({
                "role": "user",
                "parts": text_parts(&message.content)?,
            })),
            ModelRole::Assistant => {
                let mut parts = Vec::new();
                for content in &message.content {
                    match content {
                        ModelContent::Text { text } => parts.push(json!({"text": text})),
                        ModelContent::Json { value } | ModelContent::Data { value, .. } => {
                            parts.push(json!({"text": value.to_string()}))
                        }
                        ModelContent::ToolCall {
                            call_id,
                            name,
                            arguments,
                            extensions,
                        } => {
                            let native_id = extensions.get(GEMINI_FUNCTION_CALL_ID_EXTENSION)
                                .map(|value| value.as_str().filter(|id| !id.is_empty()).ok_or_else(||
                                    ModelError::invalid_request("Gemini function-call identity extension must be a non-empty string")))
                                .transpose()?.unwrap_or(call_id.as_str());
                            function_names.insert(
                                call_id.as_str().to_owned(),
                                (name.clone(), native_id.to_owned()),
                            );
                            let mut part = json!({
                                "functionCall": {
                                    "id": native_id,
                                    "name": name,
                                    "args": arguments,
                                }
                            });
                            if let Some(signature) = extensions
                                .get(GEMINI_THOUGHT_SIGNATURE_EXTENSION)
                                .map(|value| {
                                    value.as_str().filter(|value| !value.is_empty()).ok_or_else(
                                        || {
                                            ModelError::invalid_request(
                                                "Gemini thought-signature extension must be a non-empty string",
                                            )
                                        },
                                    )
                                })
                                .transpose()?
                            {
                                part.as_object_mut()
                                    .expect("Gemini function-call part is an object")
                                    .insert(
                                        "thoughtSignature".to_owned(),
                                        Value::String(signature.to_owned()),
                                    );
                            }
                            parts.push(part);
                        }
                        ModelContent::ToolResult { .. } => {
                            return Err(ModelError::invalid_request(
                                "ToolResult content requires the Tool role",
                            ))
                        }
                        _ => {
                            return Err(ModelError::new(
                                ModelErrorCode::Unsupported,
                                "unsupported canonical content block",
                            ))
                        }
                    }
                }
                contents.push(json!({"role": "model", "parts": parts}));
            }
            ModelRole::Tool => {
                let mut parts = Vec::new();
                for content in &message.content {
                    let ModelContent::ToolResult {
                        call_id,
                        result,
                        is_error,
                    } = content
                    else {
                        return Err(ModelError::invalid_request(
                            "Tool messages may contain ToolResult blocks only",
                        ));
                    };
                    let (name, native_id) =
                        function_names.get(call_id.as_str()).ok_or_else(|| {
                            ModelError::invalid_request(
                                "Gemini ToolResult has no preceding ToolCall name binding",
                            )
                        })?;
                    parts.push(json!({
                        "functionResponse": {
                            "id": native_id,
                            "name": name,
                            "response": {"result": result, "is_error": is_error},
                        }
                    }));
                }
                contents.push(json!({"role": "user", "parts": parts}));
            }
            _ => {
                return Err(ModelError::new(
                    ModelErrorCode::Unsupported,
                    "unsupported canonical model role",
                ))
            }
        }
    }
    Ok((system, contents))
}

fn text_parts(content: &[ModelContent]) -> Result<Vec<Value>, ModelError> {
    content
        .iter()
        .map(|content| match content {
            ModelContent::Text { text } => Ok(json!({"text": text})),
            ModelContent::Data {
                media_type,
                value: Value::String(data),
            } if media_type.starts_with("image/") && !data.trim().is_empty() => {
                let raw = data
                    .strip_prefix(&format!("data:{media_type};base64,"))
                    .unwrap_or(data.as_str());
                Ok(json!({
                    "inlineData": {
                        "mimeType": media_type,
                        "data": raw,
                    }
                }))
            }
            ModelContent::Json { value } | ModelContent::Data { value, .. } => {
                Ok(json!({"text": value.to_string()}))
            }
            _ => Err(ModelError::invalid_request(
                "user messages cannot contain Tool call/result blocks",
            )),
        })
        .collect()
}

fn flatten_text(content: &[ModelContent]) -> Result<String, ModelError> {
    text_parts(content).map(|parts| {
        parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n")
    })
}

fn parse_usage_metadata(usage: &Value) -> ModelUsage {
    // Gemini reports thinking output and server-side tool input separately.
    // Cached prompt tokens are already included in promptTokenCount.
    let combined = |primary: &str, supplemental: &str| {
        let primary = usage.get(primary)?.as_u64()?;
        let supplemental = match usage.get(supplemental) {
            None => 0,
            Some(value) => value.as_u64()?,
        };
        primary.checked_add(supplemental)
    };
    ModelUsage {
        input_tokens: combined("promptTokenCount", "toolUsePromptTokenCount"),
        output_tokens: combined("candidatesTokenCount", "thoughtsTokenCount"),
    }
}

#[cfg(test)]
fn parse_response(
    request: &ModelRequest,
    response: &Value,
) -> Result<Vec<ModelStreamEvent>, ModelError> {
    let mut state = gemini_stream_state(
        request.clone(),
        stream::empty().boxed(),
        CancellationToken::new(),
        256,
    );
    state.handle_frame(response.to_string())?;
    state.finalize()?;
    state.pending.into_iter().collect()
}

fn cancelled_error() -> ModelError {
    ModelError::new(ModelErrorCode::Cancelled, "model request cancelled")
}

fn empty_generation_error(finish_reason: Option<&str>, usage: Option<&ModelUsage>) -> ModelError {
    let finish_reason = finish_reason.unwrap_or("missing");
    ModelError::new(
        ModelErrorCode::Unavailable,
        format!(
            "Gemini returned an empty generation (finishReason={finish_reason}); the request can be retried"
        ),
    )
    .with_retryable(true)
    .with_details(json!({
        "kind": "empty_generation",
        "finish_reason": finish_reason,
        "usage": usage.map(|usage| json!({
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
        })),
    }))
}

fn map_transport_error(error: reqwest::Error) -> ModelError {
    let kind = if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_request() {
        "request"
    } else if error.is_body() {
        "response_body"
    } else if error.is_decode() {
        "decode"
    } else {
        "transport"
    };
    let mut message = error.to_string();
    let mut source = error.source();
    let mut depth = 0;
    while let Some(cause) = source {
        let cause_message = cause.to_string();
        if !cause_message.is_empty() && !message.contains(&cause_message) {
            message.push_str(": ");
            message.push_str(&cause_message);
        }
        source = cause.source();
        depth += 1;
        if depth == 4 {
            break;
        }
    }
    ModelError::new(ModelErrorCode::Unavailable, message)
        .with_retryable(true)
        .with_details(json!({
            "kind": kind,
            "timeout": error.is_timeout(),
            "connect": error.is_connect(),
            "status": error.status().map(|status| status.as_u16()),
            "url": error.url().map(|url| url.as_str()),
        }))
}

fn map_http_error(status: StatusCode, body: &[u8]) -> ModelError {
    let code = match status.as_u16() {
        401 | 403 => ModelErrorCode::Authentication,
        429 => ModelErrorCode::RateLimited,
        500..=599 => ModelErrorCode::Unavailable,
        _ => ModelErrorCode::InvalidRequest,
    };
    let message = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| String::from_utf8_lossy(body).chars().take(1_000).collect());
    ModelError::new(code, format!("HTTP {status}: {message}"))
        .with_retryable(status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use orchestral_core::model_protocol::{ModelRequestId, ModelToolDefinition};
    use orchestral_model_protocol_testkit::{
        ModelConformanceSuite, ModelFixtureFactory, ModelFixtureResponse, ModelFixtureScenario,
        ModelStreamStressCase, ModelStreamStressFault, ModelStreamStressSuite, PacedSseServer,
    };

    #[tokio::test]
    async fn authentication_modes_set_only_the_selected_header() {
        let client = Client::new();
        let cancellation = CancellationToken::new();
        let api_key_request = GeminiAuthentication::ApiKey("fixture-key".to_owned())
            .apply(client.post("http://127.0.0.1"), &cancellation)
            .await
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            api_key_request
                .headers()
                .get("x-goog-api-key")
                .unwrap()
                .to_str()
                .unwrap(),
            "fixture-key"
        );
        assert!(!api_key_request.headers().contains_key("authorization"));

        let bearer_request = GeminiAuthentication::BearerToken("fixture-token".to_owned())
            .apply(client.post("http://127.0.0.1"), &cancellation)
            .await
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            bearer_request
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer fixture-token"
        );
        assert!(!bearer_request.headers().contains_key("x-goog-api-key"));
    }

    struct CountingTokenProvider {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl GeminiAccessTokenProvider for CountingTokenProvider {
        async fn access_token(&self) -> Result<String, ModelError> {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Ok(format!("dynamic-token-{call}"))
        }
    }

    #[tokio::test]
    async fn dynamic_token_provider_is_resolved_for_every_request() {
        let provider = Arc::new(CountingTokenProvider {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let authentication = GeminiAuthentication::AccessTokenProvider(provider.clone());
        let client = Client::new();
        let cancellation = CancellationToken::new();

        for expected in ["Bearer dynamic-token-1", "Bearer dynamic-token-2"] {
            let request = authentication
                .apply(client.post("http://127.0.0.1"), &cancellation)
                .await
                .unwrap()
                .build()
                .unwrap();
            assert_eq!(
                request
                    .headers()
                    .get("authorization")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                expected
            );
        }
        assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn authentication_rejects_empty_credentials() {
        assert!(!GeminiAuthentication::ApiKey("  ".to_owned()).is_valid());
        assert!(!GeminiAuthentication::BearerToken(String::new()).is_valid());
    }

    struct GeminiConformanceFixture;

    impl ModelFixtureFactory for GeminiConformanceFixture {
        fn adapter_name(&self) -> &'static str {
            "gemini-native"
        }

        fn backend(
            &self,
            _scenario: ModelFixtureScenario,
            endpoint: &str,
        ) -> Result<std::sync::Arc<dyn ModelBackend>, ModelError> {
            GeminiModelBackend::new(GeminiModelConfig {
                backend_id: "gemini-conformance".to_owned(),
                endpoint: endpoint.to_owned(),
                authentication: GeminiAuthentication::ApiKey("fixture-key".to_owned()),
                model: "fixture-model".to_owned(),
                temperature: 0.0,
                thinking_level: None,
                default_max_output_tokens: 64,
                max_context_tokens: Some(8_192),
                timeout: Duration::from_secs(1),
                max_buffered_events: 128,
            })
            .map(|backend| std::sync::Arc::new(backend) as std::sync::Arc<dyn ModelBackend>)
        }

        fn response(&self, scenario: ModelFixtureScenario) -> ModelFixtureResponse {
            let body = match scenario {
                ModelFixtureScenario::Text => concat!(
                    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello \"}]}}]}\n\n",
                    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"world\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":7,\"candidatesTokenCount\":2}}\n\n"
                )
                .as_bytes()
                .to_vec(),
                ModelFixtureScenario::Tool => concat!(
                    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{",
                    "\"id\":\"call-1\",\"name\":\"echo\",\"args\":{\"value\":\"hello\"}}}]},",
                    "\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":8,",
                    "\"candidatesTokenCount\":3}}\n\n"
                )
                .as_bytes()
                .to_vec(),
                ModelFixtureScenario::Malformed => b"data: not-json\n\n".to_vec(),
                ModelFixtureScenario::Stalled => return ModelFixtureResponse::Stall,
            };
            ModelFixtureResponse::Complete(body)
        }
    }

    fn request() -> ModelRequest {
        ModelRequest {
            request_id: ModelRequestId::new("request-1"),
            messages: vec![ModelMessage::text(ModelRole::User, "use echo")],
            tools: vec![ModelToolDefinition {
                name: "echo".to_owned(),
                description: "Echo a value".to_owned(),
                input_schema: json!({"type": "object"}),
            }],
            output_schema: None,
            max_output_tokens: Some(128),
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn ten_thousand_gemini_wire_requests_never_exceed_the_metered_upper_bound() {
        let backend = GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "gemini-meter-gate".to_owned(),
            endpoint: "http://127.0.0.1".to_owned(),
            authentication: GeminiAuthentication::ApiKey("fixture-key".to_owned()),
            model: "fixture-model".to_owned(),
            temperature: 0.2,
            thinking_level: Some(GeminiThinkingLevel::Low),
            default_max_output_tokens: 256,
            max_context_tokens: Some(32_768),
            timeout: Duration::from_secs(1),
            max_buffered_events: 8,
        })
        .unwrap();
        let descriptor = backend.meter_descriptor();
        descriptor.validate().unwrap();
        assert!(backend.supports_observed_prefix_estimation());
        let planning_request = request();
        let planning = backend
            .estimate_context_input(&planning_request.messages, &planning_request.tools)
            .unwrap();
        assert_eq!(planning.accounting, ModelTokenAccounting::Estimated);
        assert!(
            planning.tokens
                < backend
                    .count_request_input(&planning_request.messages, &planning_request.tools)
                    .unwrap()
        );
        assert_eq!(
            descriptor.accounting,
            ModelTokenAccounting::ConservativeUpperBound
        );
        assert_eq!(
            backend
                .build_request_body(&request())
                .unwrap()
                .pointer("/generationConfig/thinkingConfig/thinkingLevel"),
            Some(&json!("LOW"))
        );

        let mut state = 0xbb67_ae85_84ca_a73b_u64;
        for case in 0..10_000_u64 {
            state = state
                .wrapping_mul(2_862_933_555_777_941_757)
                .wrapping_add(3_037_000_493);
            let text = format!(
                "case={case}; nonce={state:016x}; {}",
                "wire-context-安全".repeat((state as usize % 17) + 1)
            );
            let messages = vec![ModelMessage::text(ModelRole::User, text)];
            let tools = if state & 1 == 0 {
                vec![ModelToolDefinition {
                    name: format!("tool_{}", case % 13),
                    description: "Generated tool for token accounting".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {"value": {"type": "string"}}
                    }),
                }]
            } else {
                Vec::new()
            };
            let actual = ModelRequest {
                request_id: ModelRequestId::new(format!("meter-{case}")),
                messages: messages.clone(),
                tools: tools.clone(),
                output_schema: None,
                max_output_tokens: None,
                extensions: BTreeMap::new(),
            };
            let wire_bytes = serde_json::to_vec(&backend.build_request_body(&actual).unwrap())
                .unwrap()
                .len() as u64;
            let upper_bound = backend.count_request_input(&messages, &tools).unwrap();
            assert!(upper_bound >= wire_bytes, "case {case} under-counted");
        }
    }

    #[tokio::test]
    async fn passes_shared_model_backend_conformance_suite() {
        let report = ModelConformanceSuite::default()
            .run(&GeminiConformanceFixture)
            .await;
        assert!(report.is_conformant(), "{:#?}", report.results());
    }

    #[tokio::test]
    async fn stalled_responses_report_the_failed_phase_and_elapsed_time() {
        use axum::{body::Body, response::Response, routing::post, Router};

        for phase in ["response_headers", "response_body"] {
            let app = Router::new().route(
                "/{*path}",
                post(move || async move {
                    if phase == "response_headers" {
                        std::future::pending::<()>().await;
                    }
                    Response::new(Body::from_stream(
                        stream::once(std::future::ready(Ok::<_, std::io::Error>(
                            Bytes::from_static(b": connected\n\n"),
                        )))
                        .chain(stream::pending()),
                    ))
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let backend = GeminiModelBackend::new(GeminiModelConfig {
                backend_id: "gemini-timeout".to_owned(),
                endpoint,
                authentication: GeminiAuthentication::ApiKey("fixture-key".to_owned()),
                model: "fixture-model".to_owned(),
                temperature: 0.0,
                thinking_level: None,
                default_max_output_tokens: 64,
                max_context_tokens: Some(8_192),
                timeout: Duration::from_millis(100),
                max_buffered_events: 8,
            })
            .unwrap();
            let error = tokio::time::timeout(Duration::from_secs(5), async {
                match backend.start(request(), CancellationToken::new()).await {
                    Err(error) => error,
                    Ok(mut events) => events.next().await.unwrap().unwrap_err(),
                }
            })
            .await
            .unwrap();
            assert_eq!(error.details["phase"], phase);
            assert_eq!(error.details["timeout"], true);
            assert!(error.details["elapsed_ms"].as_u64().unwrap() >= 100);
            server.abort();
        }
    }

    #[tokio::test]
    async fn stalled_body_after_partial_output_reports_idle_time_and_received_bytes() {
        use axum::{body::Body, response::Response, routing::post, Router};
        let chunk = Bytes::from_static(b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Partial answer\"}]}}]}\n\n");
        let expected_bytes = chunk.len() as u64;
        let app = Router::new().route(
            "/{*path}",
            post(move || {
                let chunk = chunk.clone();
                async move {
                    Response::new(Body::from_stream(
                        stream::once(std::future::ready(Ok::<_, std::io::Error>(chunk)))
                            .chain(stream::pending()),
                    ))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let backend = GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "partial-body-fixture".into(),
            endpoint,
            authentication: GeminiAuthentication::ApiKey("fixture".into()),
            model: "fixture".into(),
            temperature: 0.0,
            thinking_level: None,
            default_max_output_tokens: 256,
            max_context_tokens: None,
            timeout: Duration::from_millis(100),
            max_buffered_events: 16,
        })
        .unwrap();
        let mut events = backend
            .start(request(), CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(events.next().await.unwrap().unwrap().payload,
            ModelEvent::TextDelta { ref delta } if delta == "Partial answer"));
        let error = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.details["phase"], "response_body");
        assert_eq!(error.details["timeout"], true);
        assert_eq!(error.details["content_received"], true);
        assert_eq!(error.details["received_body_bytes"], expected_bytes);
        assert!(error.details["idle_ms"].as_u64().unwrap() >= 100);
        assert!(
            events.next().await.is_none(),
            "partial output must not fabricate Finish"
        );
        server.abort();
    }

    #[tokio::test]
    async fn response_header_deadline_is_independent_from_body_idle_timeout() {
        use axum::{routing::post, Router};
        let app = Router::new().route(
            "/{*path}",
            post(|| async { std::future::pending::<String>().await }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let backend = GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "headers-fixture".into(),
            endpoint,
            authentication: GeminiAuthentication::ApiKey("fixture".into()),
            model: "fixture".into(),
            temperature: 0.0,
            thinking_level: None,
            default_max_output_tokens: 256,
            max_context_tokens: None,
            timeout: Duration::from_secs(300),
            max_buffered_events: 16,
        })
        .unwrap()
        .with_response_header_timeout(Duration::from_millis(100))
        .unwrap();
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            backend.start(request(), CancellationToken::new()),
        )
        .await
        .unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("stalled headers must time out"),
        };
        assert_eq!(error.code, ModelErrorCode::Unavailable);
        assert!(error.retryable);
        assert_eq!(error.details["phase"], "response_headers");
        assert_eq!(error.details["response_header_timeout_ms"], 100);
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(backend
            .with_response_header_timeout(Duration::ZERO)
            .is_err());
        server.abort();
    }

    #[tokio::test]
    async fn response_header_timeout_rotates_the_pooled_http_client_for_retries() {
        use axum::{routing::post, Router};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_handler = attempts.clone();
        let app = Router::new().route(
            "/{*path}",
            post(move || {
                let attempts = attempts_handler.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        std::future::pending::<String>().await
                    } else {
                        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"recovered\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":1}}\n\n".to_owned()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let backend = GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "retry-rotation-fixture".into(),
            endpoint,
            authentication: GeminiAuthentication::ApiKey("fixture".into()),
            model: "fixture".into(),
            temperature: 0.0,
            thinking_level: None,
            default_max_output_tokens: 256,
            max_context_tokens: None,
            timeout: Duration::from_secs(5),
            max_buffered_events: 16,
        })
        .unwrap()
        .with_response_header_timeout(Duration::from_millis(80))
        .unwrap();

        let first = backend
            .start(request(), CancellationToken::new())
            .await
            .err()
            .expect("first attempt must time out");
        assert_eq!(first.details["phase"], "response_headers");
        let mut second = backend
            .start(request(), CancellationToken::new())
            .await
            .expect("second attempt after client rotation must succeed");
        assert!(matches!(
            second.next().await.unwrap().unwrap().payload,
            ModelEvent::TextDelta { ref delta } if delta == "recovered"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn active_stream_may_outlive_its_idle_timeout() {
        let server = PacedSseServer::start(
            vec![
                b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello \"}]}}]}\n\n".to_vec(),
                b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"world\"}]}}]}\n\n".to_vec(),
                b"data: {\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":7,\"candidatesTokenCount\":2}}\n\n".to_vec(),
            ],
            Duration::from_millis(550),
        )
        .await
        .unwrap();
        let backend = GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "gemini-paced-stream".to_owned(),
            endpoint: server.endpoint().to_owned(),
            authentication: GeminiAuthentication::ApiKey("fixture-key".to_owned()),
            model: "fixture-model".to_owned(),
            temperature: 0.0,
            thinking_level: None,
            default_max_output_tokens: 64,
            max_context_tokens: Some(8_192),
            timeout: Duration::from_secs(1),
            max_buffered_events: 128,
        })
        .unwrap()
        .with_response_header_timeout(Duration::from_millis(250))
        .unwrap();
        let started = tokio::time::Instant::now();
        let mut stream = backend
            .start(request(), CancellationToken::new())
            .await
            .unwrap();
        let mut finished = false;
        while let Some(event) = stream.next().await {
            finished |= matches!(
                event.unwrap().payload,
                ModelEvent::Finish {
                    reason: ModelFinishReason::Stop
                }
            );
        }
        assert!(started.elapsed() > Duration::from_secs(1));
        assert!(finished);
        server.finish().await.unwrap();
    }

    fn stress_request(case: &ModelStreamStressCase) -> ModelRequest {
        let mut request = request();
        request.request_id = case.request_id();
        request
    }

    fn stress_wire(case: &ModelStreamStressCase) -> Vec<u8> {
        let fragments = case.text_fragments();
        let data = |value: Value| format!("data: {}\n\n", value);
        match case.fault() {
            ModelStreamStressFault::MalformedFrame => format!(
                "{}data: not-json\n\n{}",
                data(json!({"candidates":[{"content":{"parts":[{"text": fragments[0]}]}}]})),
                data(json!({"candidates":[{"content":{"parts":[{"text":"must-not-arrive"}]},"finishReason":"STOP"}]})),
            )
            .into_bytes(),
            ModelStreamStressFault::BufferedBurst if case.index() % 10 == 3 => data(json!({
                "candidates": [{"content": {"parts": [
                    {"functionCall": {"id": "burst-a", "name": "echo", "args": {}}},
                    {"functionCall": {"id": "burst-b", "name": "echo", "args": {}}}
                ]}}]
            }))
            .into_bytes(),
            ModelStreamStressFault::BufferedBurst => (0..5)
                .map(|index| {
                    data(json!({
                        "candidates": [{"content": {"parts": [{"text": format!("burst-{index}")}]}}]
                    }))
                })
                .collect::<String>()
                .into_bytes(),
            ModelStreamStressFault::None
            | ModelStreamStressFault::ExtraAfterTerminal
            | ModelStreamStressFault::CancelBeforePoll => {
                let mut body = format!(
                    "{}{}data: [DONE]\n\n",
                    data(json!({"candidates":[{"content":{"parts":[{"text": fragments[0]}]}}]})),
                    data(json!({
                        "candidates": [{
                            "content": {"parts": [{"text": fragments[1]}]},
                            "finishReason": "STOP"
                        }],
                        "usageMetadata": {"promptTokenCount": 7, "candidatesTokenCount": 2}
                    })),
                );
                if case.fault() == ModelStreamStressFault::ExtraAfterTerminal {
                    body.push_str(&data(json!({
                        "candidates": [{"content": {"parts": [{"text": "must-not-arrive"}]}}]
                    })));
                    body.push_str("data: [DONE]\n\n");
                }
                body.into_bytes()
            }
        }
    }

    #[tokio::test]
    async fn ten_thousand_chunk_fault_and_backpressure_plans_preserve_gemini_stream_invariants() {
        let report = ModelStreamStressSuite::default()
            .run("gemini-native", |case| {
                let cancellation = CancellationToken::new();
                if case.cancel_before_poll() {
                    cancellation.cancel();
                }
                let chunks = case
                    .split_wire(&stress_wire(case))
                    .into_iter()
                    .map(|chunk| Ok::<Bytes, reqwest::Error>(Bytes::from(chunk)));
                Ok(gemini_event_stream(
                    stress_request(case),
                    futures_util::stream::iter(chunks).boxed(),
                    cancellation,
                    case.max_buffered_events(),
                ))
            })
            .await;
        assert!(report.is_conformant(), "{report:#?}");
        assert_eq!(report.total_cases(), 10_000);
        assert_eq!(report.successful_cases(), 4_000);
        assert_eq!(report.protocol_failures(), 4_000);
        assert_eq!(report.cancellations(), 2_000);
    }

    #[tokio::test]
    async fn usage_includes_thinking_and_tool_input_without_counting_cached_tokens_twice() {
        for (metadata, expected_input, expected_output) in [
            (
                json!({"promptTokenCount": 7, "candidatesTokenCount": 2,
                    "thoughtsTokenCount": 11, "toolUsePromptTokenCount": 3,
                    "cachedContentTokenCount": 4, "totalTokenCount": 23}),
                Some(10),
                Some(13),
            ),
            (
                json!({"promptTokenCount": 7, "candidatesTokenCount": 2}),
                Some(7),
                Some(2),
            ),
            (
                json!({"promptTokenCount": 7, "thoughtsTokenCount": 11}),
                Some(7),
                None,
            ),
            (
                json!({"candidatesTokenCount": 2, "thoughtsTokenCount": "invalid"}),
                None,
                None,
            ),
            (
                json!({"candidatesTokenCount": u64::MAX, "thoughtsTokenCount": 1}),
                None,
                None,
            ),
        ] {
            let response = json!({
                "candidates": [{"content": {"parts": [{"text": "done"}]}, "finishReason": "STOP"}],
                "usageMetadata": metadata,
            });
            let expected = ModelUsage {
                input_tokens: expected_input,
                output_tokens: expected_output,
            };
            let events = parse_response(&request(), &response).unwrap();
            assert!(events.iter().any(
                |event| matches!(&event.payload, ModelEvent::Usage { usage } if usage == &expected)
            ));
            let bytes = Bytes::from(format!("data: {response}\n\n"));
            let stream = gemini_event_stream(
                request(),
                stream::iter([Ok::<Bytes, reqwest::Error>(bytes)]).boxed(),
                CancellationToken::new(),
                128,
            );
            let events = stream.try_collect::<Vec<_>>().await.unwrap();
            let usage = events
                .iter()
                .filter_map(|event| match &event.payload {
                    ModelEvent::Usage { usage } => Some(usage),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(usage, vec![&expected]);
        }
    }

    #[test]
    fn parses_native_function_call_and_usage() {
        let request = request();
        let events = parse_response(
            &request,
            &json!({
                "candidates": [{
                    "content": {"parts": [{"functionCall": {
                        "id": "call-1", "name": "echo", "args": {"value": "hello"}
                    }}]},
                    "finishReason": "STOP"
                }],
                "usageMetadata": {"promptTokenCount": 8, "candidatesTokenCount": 3}
            }),
        )
        .unwrap();
        assert_eq!(events.len(), 5);
        for (index, event) in events.iter().enumerate() {
            event
                .validate_for(&request.request_id, index as u64 + 1)
                .unwrap();
        }
        assert!(matches!(
            events.last().unwrap().payload,
            ModelEvent::Finish {
                reason: ModelFinishReason::ToolCalls
            }
        ));
    }

    #[test]
    fn preserves_thought_signature_across_tool_continuation() {
        let request = request();
        let events = parse_response(
            &request,
            &json!({
                "candidates": [{
                    "content": {"parts": [{
                        "functionCall": {
                            "id": "call-1",
                            "name": "echo",
                            "args": {"value": "hello"}
                        },
                        "thoughtSignature": "opaque-signature"
                    }]},
                    "finishReason": "STOP"
                }]
            }),
        )
        .unwrap();
        let (call_id, name, extensions) = events
            .iter()
            .find_map(|event| match &event.payload {
                ModelEvent::ToolCallStart {
                    call_id,
                    name,
                    extensions,
                } => Some((call_id.clone(), name.clone(), extensions.clone())),
                _ => None,
            })
            .expect("function call start");
        assert_eq!(
            extensions.get(GEMINI_THOUGHT_SIGNATURE_EXTENSION),
            Some(&Value::String("opaque-signature".to_owned()))
        );

        let messages = vec![
            ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::ToolCall {
                    call_id: call_id.clone(),
                    name,
                    arguments: json!({"value": "hello"}),
                    extensions,
                }],
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: vec![ModelContent::ToolResult {
                    call_id,
                    result: json!({"result": "hello"}),
                    is_error: false,
                }],
            },
        ];
        let (_, encoded) = encode_messages(&messages).unwrap();
        assert_eq!(
            encoded[0]["parts"][0]["thoughtSignature"],
            "opaque-signature"
        );
        assert_eq!(encoded[0]["parts"][0]["functionCall"]["id"], "call-1");
        assert_eq!(encoded[1]["parts"][0]["functionResponse"]["id"], "call-1");
    }

    #[test]
    fn native_call_ids_are_scoped_to_requests_and_stable_on_replay() {
        let response = json!({"candidates":[{"content":{"parts":[
            {"functionCall":{"id":"reused-native-id","name":"echo","args":{"value":"first"}}},
            {"functionCall":{"id":"reused-native-id","name":"echo","args":{"value":"second"}}}
        ]},"finishReason":"STOP"}]});
        let ids = |request: &ModelRequest| {
            parse_response(request, &response)
                .unwrap()
                .into_iter()
                .filter_map(|event| {
                    if let ModelEvent::ToolCallStart { call_id, .. } = event.payload {
                        Some(call_id)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        let first = request();
        let a = ids(&first);
        assert_eq!(a, ids(&first), "replay retains execution identity");
        assert_eq!(
            a[0], a[1],
            "duplicate IDs in one response remain detectable"
        );
        let mut second = first;
        second.request_id = ModelRequestId::new("request-2");
        assert_ne!(
            a[0],
            ids(&second)[0],
            "new model requests own distinct executions"
        );
    }

    #[test]
    fn summaries_do_not_change_default_thinking_effort() {
        let backend = GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "summary-fixture".to_owned(),
            endpoint: "http://localhost".to_owned(),
            authentication: GeminiAuthentication::ApiKey("fixture".to_owned()),
            model: "fixture".to_owned(),
            temperature: 0.0,
            thinking_level: None,
            default_max_output_tokens: 128,
            max_context_tokens: None,
            timeout: Duration::from_secs(1),
            max_buffered_events: 16,
        })
        .unwrap()
        .with_thought_summaries(true);
        let body = backend.build_request_body(&request()).unwrap();
        assert_eq!(
            body["generationConfig"]["thinkingConfig"],
            json!({"includeThoughts":true})
        );
    }

    #[tokio::test]
    async fn fragmented_thought_summary_and_length_finish_preserve_usage_without_an_answer() {
        for summary in [None, Some("Inspecting the interface before editing.")] {
            let parts = summary
                .map(|text| vec![json!({"text":text,"thought":true})])
                .unwrap_or_default();
            let response = json!({"candidates":[{"content":{"parts":parts},"finishReason":"MAX_TOKENS"}],
                "usageMetadata":{"promptTokenCount":1500,"thoughtsTokenCount":4096,"candidatesTokenCount":0}});
            let raw = format!("data: {response}\n\n");
            let chunks = raw
                .as_bytes()
                .chunks(7)
                .map(|chunk| Ok::<Bytes, reqwest::Error>(Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>();
            let mut stream = gemini_event_stream(
                request(),
                stream::iter(chunks).boxed(),
                CancellationToken::new(),
                128,
            );
            let mut events = Vec::new();
            while let Some(event) = stream.next().await {
                events.push(event.unwrap());
            }
            for (i, event) in events.iter().enumerate() {
                event
                    .validate_for(&request().request_id, i as u64 + 1)
                    .unwrap();
            }
            assert!(!events
                .iter()
                .any(|e| matches!(e.payload, ModelEvent::TextDelta { .. })));
            let summaries = events
                .iter()
                .filter_map(|e| match &e.payload {
                    ModelEvent::ReasoningSummaryDelta { delta } => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(summaries, summary.into_iter().collect::<Vec<_>>());
            assert!(events.iter().any(|e| matches!(&e.payload, ModelEvent::Usage { usage } if usage.input_tokens == Some(1500) && usage.output_tokens == Some(4096))));
            assert!(matches!(
                events.last().unwrap().payload,
                ModelEvent::Finish {
                    reason: ModelFinishReason::Length
                }
            ));
        }
    }

    #[test]
    fn maps_function_response_to_the_original_function_name() {
        let messages = vec![
            ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::ToolCall {
                    call_id: ModelToolCallId::new("call-1"),
                    name: "echo".to_owned(),
                    arguments: json!({"value": "hello"}),
                    extensions: BTreeMap::new(),
                }],
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: vec![ModelContent::ToolResult {
                    call_id: ModelToolCallId::new("call-1"),
                    result: json!({"result": "hello"}),
                    is_error: false,
                }],
            },
        ];
        let (_, encoded) = encode_messages(&messages).unwrap();
        assert_eq!(encoded[1]["parts"][0]["functionResponse"]["name"], "echo");
    }

    #[tokio::test]
    async fn fragmented_sse_maps_text_function_call_usage_and_finish() {
        let request = request();
        let raw = concat!(
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello \"}]}}]}\n\n",
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{",
            "\"id\":\"call-1\",\"name\":\"echo\",\"args\":{\"value\":\"world\"}},",
            "\"thoughtSignature\":\"opaque-stream-signature\"}]},",
            "\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":8,",
            "\"candidatesTokenCount\":3}}\n\n"
        );
        let chunks = raw
            .as_bytes()
            .chunks(17)
            .map(|chunk| Ok::<Bytes, reqwest::Error>(Bytes::copy_from_slice(chunk)))
            .collect::<Vec<_>>();
        let mut stream = gemini_event_stream(
            request.clone(),
            stream::iter(chunks).boxed(),
            CancellationToken::new(),
            128,
        );
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event.expect("valid event"));
        }
        for (index, event) in events.iter().enumerate() {
            event
                .validate_for(&request.request_id, index as u64 + 1)
                .expect("contiguous sequence");
        }
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            ModelEvent::ToolCallStart { call_id, name, extensions }
                if call_id.as_str().starts_with("request-1-gemini-native-")
                    && extensions.get(GEMINI_FUNCTION_CALL_ID_EXTENSION) == Some(&json!("call-1"))
                    && name == "echo"
                    && extensions.get(GEMINI_THOUGHT_SIGNATURE_EXTENSION)
                        == Some(&Value::String("opaque-stream-signature".to_owned()))
        )));
        assert!(matches!(
            events.last().map(|event| &event.payload),
            Some(ModelEvent::Finish {
                reason: ModelFinishReason::ToolCalls
            })
        ));
    }

    #[tokio::test]
    async fn empty_stop_generation_is_retryable_unavailable_not_protocol() {
        let request = request();
        let bytes = Bytes::from_static(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":9,\"candidatesTokenCount\":0}}\n\n",
        );
        let mut stream = gemini_event_stream(
            request,
            stream::iter([Ok::<Bytes, reqwest::Error>(bytes)]).boxed(),
            CancellationToken::new(),
            128,
        );
        assert!(matches!(
            stream.next().await.expect("usage event").expect("usage"),
            ModelStreamEvent {
                payload: ModelEvent::Usage { .. },
                ..
            }
        ));
        let error = stream
            .next()
            .await
            .expect("empty generation error")
            .expect_err("empty STOP must not complete successfully");
        assert_eq!(error.code, ModelErrorCode::Unavailable);
        assert!(error.retryable);
        assert!(error.message.contains("finishReason=STOP"));
        assert_eq!(error.details["kind"], "empty_generation");
    }

    #[tokio::test]
    async fn prompt_feedback_block_maps_to_content_filter_finish() {
        let request = request();
        let bytes =
            Bytes::from_static(b"data: {\"promptFeedback\":{\"blockReason\":\"SAFETY\"}}\n\n");
        let mut stream = gemini_event_stream(
            request,
            stream::iter([Ok::<Bytes, reqwest::Error>(bytes)]).boxed(),
            CancellationToken::new(),
            128,
        );
        assert!(matches!(
            stream.next().await.expect("finish event").expect("finish"),
            ModelStreamEvent {
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::ContentFilter
                },
                ..
            }
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn streams_ten_thousand_deltas_without_sequence_gaps() {
        let mut bytes = Vec::with_capacity(10_001);
        for _ in 0..10_000 {
            bytes.push(Ok::<Bytes, reqwest::Error>(Bytes::from_static(
                b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"x\"}]}}]}\n\n",
            )));
        }
        bytes.push(Ok(Bytes::from_static(
            b"data: {\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}\n\n",
        )));
        let request = request();
        let mut stream = gemini_event_stream(
            request.clone(),
            stream::iter(bytes).boxed(),
            CancellationToken::new(),
            128,
        );
        let mut count = 0_u64;
        while let Some(event) = stream.next().await {
            let event = event.expect("valid event");
            count += 1;
            event
                .validate_for(&request.request_id, count)
                .expect("contiguous sequence");
        }
        assert_eq!(count, 10_001);
    }
}
