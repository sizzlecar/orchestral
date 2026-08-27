//! Gemini-native HTTP adapter for the canonical Orchestral Model Protocol.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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
    ModelBackend, ModelCapabilities, ModelContent, ModelDescriptor, ModelError, ModelErrorCode,
    ModelEvent, ModelEventId, ModelFinishReason, ModelMessage, ModelRequest, ModelRequestId,
    ModelRole, ModelStream, ModelStreamEvent, ModelTokenAccounting, ModelTokenMeter,
    ModelTokenMeterDescriptor, ModelToolCallId, ModelToolDefinition, ModelUsage,
};
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

const GOOGLE_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

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
    client: Client,
    config: GeminiModelConfig,
}

impl GeminiModelBackend {
    pub fn new(config: GeminiModelConfig) -> Result<Self, ModelError> {
        config.validate()?;
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|error| ModelError::new(ModelErrorCode::Internal, error.to_string()))?;
        Ok(Self { client, config })
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
        if let Some(thinking_level) = self.config.thinking_level {
            generation.insert(
                "thinkingConfig".to_owned(),
                json!({"thinkingLevel": thinking_level.as_wire_value()}),
            );
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
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        let config = serde_json::to_vec(&(
            &self.config.model,
            self.config.temperature.to_bits(),
            self.config
                .thinking_level
                .map(GeminiThinkingLevel::as_wire_value),
            self.config.default_max_output_tokens,
        ))
        .expect("Gemini token meter scalar configuration is serializable");
        ModelTokenMeterDescriptor {
            strategy: "google-gemini/wire-json-upper-bound".to_owned(),
            version: "1".to_owned(),
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
}

#[async_trait]
impl ModelBackend for GeminiModelBackend {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: self.config.backend_id.clone(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                parallel_tool_calls: true,
                structured_output: true,
                max_context_tokens: self.config.max_context_tokens,
            },
            extensions: BTreeMap::from([(
                "google-gemini/model".to_owned(),
                Value::String(self.config.model.clone()),
            )]),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        self.descriptor().validate()?;
        let body = self.build_request_body(&request)?;
        let http_request = self
            .config
            .authentication
            .apply(self.client.post(self.config.stream_url()), &cancellation)
            .await?
            .json(&body);
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled_error()),
            response = http_request.send() => response,
        }
        .map_err(map_transport_error)?;
        let status = response.status();
        if !status.is_success() {
            let bytes = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(cancelled_error()),
                bytes = response.bytes() => bytes,
            }
            .map_err(map_transport_error)?;
            return Err(map_http_error(status, &bytes));
        }
        Ok(gemini_event_stream(
            request,
            response.bytes_stream().boxed(),
            cancellation,
            self.config.max_buffered_events,
        ))
    }
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
    last_usage: Option<ModelUsage>,
    emitted_content: bool,
    terminated: bool,
    max_buffered_events: usize,
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
                    self.emitted_content = true;
                    self.emit(ModelEvent::TextDelta {
                        delta: text.to_owned(),
                    })?;
                }
                if let Some(call) = part.get("functionCall") {
                    self.emit_function_call(call)?;
                }
            }
            if let Some(reason) = candidate
                .get("finishReason")
                .and_then(Value::as_str)
                .filter(|reason| !reason.is_empty())
            {
                self.finish_reason = Some(map_finish_reason(reason));
            }
        }
        if let Some(usage) = response.get("usageMetadata") {
            let usage = ModelUsage {
                input_tokens: usage.get("promptTokenCount").and_then(Value::as_u64),
                output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_u64),
            };
            if self.last_usage.as_ref() != Some(&usage) {
                self.last_usage = Some(usage.clone());
                self.emit(ModelEvent::Usage { usage })?;
            }
        }
        Ok(())
    }

    fn emit_function_call(&mut self, call: &Value) -> Result<(), ModelError> {
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| ModelError::protocol("Gemini functionCall omitted name"))?;
        self.next_call += 1;
        let id = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
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
        self.emitted_content = true;
        self.emit(ModelEvent::ToolCallStart {
            call_id: call_id.clone(),
            name: name.to_owned(),
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
        if self.next_call > 0 && reason == ModelFinishReason::Stop {
            reason = ModelFinishReason::ToolCalls;
        }
        if !self.emitted_content && reason != ModelFinishReason::ContentFilter {
            return Err(ModelError::protocol(
                "Gemini stream contained neither text nor function calls",
            ));
        }
        self.emit(ModelEvent::Finish { reason })?;
        self.terminated = true;
        Ok(())
    }
}

fn gemini_event_stream(
    request: ModelRequest,
    bytes: HttpByteStream,
    cancellation: CancellationToken,
    max_buffered_events: usize,
) -> ModelStream {
    let state = GeminiStreamState {
        request_id: request.request_id,
        bytes,
        cancellation,
        decoder: SseDecoder::new(max_buffered_events),
        pending: VecDeque::new(),
        sequence: 0,
        next_call: 0,
        finish_reason: None,
        last_usage: None,
        emitted_content: false,
        terminated: false,
        max_buffered_events,
    };
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
    let mut function_names = HashMap::<String, String>::new();
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
                        } => {
                            function_names.insert(call_id.as_str().to_owned(), name.clone());
                            parts.push(json!({
                                "functionCall": {
                                    "id": call_id.as_str(),
                                    "name": name,
                                    "args": arguments,
                                }
                            }));
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
                    let name = function_names.get(call_id.as_str()).ok_or_else(|| {
                        ModelError::invalid_request(
                            "Gemini ToolResult has no preceding ToolCall name binding",
                        )
                    })?;
                    parts.push(json!({
                        "functionResponse": {
                            "id": call_id.as_str(),
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

#[cfg(test)]
fn parse_response(
    request: &ModelRequest,
    response: &Value,
) -> Result<Vec<ModelStreamEvent>, ModelError> {
    if let Some(error) = response.get("error") {
        return Err(ModelError::new(
            ModelErrorCode::Unavailable,
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Gemini returned an error"),
        ));
    }
    let candidate = response
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|candidates| candidates.first())
        .ok_or_else(|| ModelError::protocol("Gemini response contains no candidate"))?;
    let parts = candidate
        .pointer("/content/parts")
        .and_then(Value::as_array)
        .ok_or_else(|| ModelError::protocol("Gemini candidate contains no parts"))?;
    let mut payloads = Vec::new();
    let mut tool_count = 0_usize;
    for (index, part) in parts.iter().enumerate() {
        if let Some(text) = part
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            payloads.push(ModelEvent::TextDelta {
                delta: text.to_owned(),
            });
        }
        if let Some(call) = part.get("functionCall") {
            let name = call
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| ModelError::protocol("Gemini functionCall omitted name"))?;
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!("{}-gemini-call-{}", request.request_id.as_str(), index + 1)
                });
            let arguments =
                serde_json::to_string(call.get("args").unwrap_or(&Value::Object(Map::new())))
                    .map_err(|error| ModelError::protocol(error.to_string()))?;
            let call_id = ModelToolCallId::new(id);
            payloads.extend([
                ModelEvent::ToolCallStart {
                    call_id: call_id.clone(),
                    name: name.to_owned(),
                },
                ModelEvent::ToolCallArgumentsDelta {
                    call_id: call_id.clone(),
                    delta: arguments,
                },
                ModelEvent::ToolCallEnd { call_id },
            ]);
            tool_count += 1;
        }
    }
    if let Some(usage) = response.get("usageMetadata") {
        payloads.push(ModelEvent::Usage {
            usage: ModelUsage {
                input_tokens: usage.get("promptTokenCount").and_then(Value::as_u64),
                output_tokens: usage.get("candidatesTokenCount").and_then(Value::as_u64),
            },
        });
    }
    if payloads.is_empty() {
        return Err(ModelError::protocol(
            "Gemini response contains neither text nor function calls",
        ));
    }
    let reason = if tool_count > 0 {
        ModelFinishReason::ToolCalls
    } else {
        match candidate.get("finishReason").and_then(Value::as_str) {
            Some("STOP") | None => ModelFinishReason::Stop,
            Some("MAX_TOKENS") => ModelFinishReason::Length,
            Some("SAFETY") | Some("BLOCKLIST") | Some("PROHIBITED_CONTENT") => {
                ModelFinishReason::ContentFilter
            }
            Some(_) => ModelFinishReason::Other,
        }
    };
    payloads.push(ModelEvent::Finish { reason });
    Ok(sequence_events(request, payloads))
}

#[cfg(test)]
fn sequence_events(request: &ModelRequest, payloads: Vec<ModelEvent>) -> Vec<ModelStreamEvent> {
    payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let sequence = index as u64 + 1;
            ModelStreamEvent {
                request_id: request.request_id.clone(),
                event_id: ModelEventId::new(format!(
                    "{}-gemini-{sequence}",
                    request.request_id.as_str()
                )),
                sequence,
                payload,
            }
        })
        .collect()
}

fn cancelled_error() -> ModelError {
    ModelError::new(ModelErrorCode::Cancelled, "model request cancelled")
}

fn map_transport_error(error: reqwest::Error) -> ModelError {
    ModelError::new(ModelErrorCode::Unavailable, error.to_string()).with_retryable(true)
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
    use orchestral_core::model_protocol::{ModelRequestId, ModelToolDefinition};
    use orchestral_model_protocol_testkit::{
        ModelConformanceSuite, ModelFixtureFactory, ModelFixtureResponse, ModelFixtureScenario,
        ModelStreamStressCase, ModelStreamStressFault, ModelStreamStressSuite,
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
    fn maps_function_response_to_the_original_function_name() {
        let messages = vec![
            ModelMessage {
                role: ModelRole::Assistant,
                content: vec![ModelContent::ToolCall {
                    call_id: ModelToolCallId::new("call-1"),
                    name: "echo".to_owned(),
                    arguments: json!({"value": "hello"}),
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
            "\"id\":\"call-1\",\"name\":\"echo\",\"args\":{\"value\":\"world\"}}}]},",
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
            ModelEvent::ToolCallStart { call_id, name }
                if call_id.as_str() == "call-1" && name == "echo"
        )));
        assert!(matches!(
            events.last().map(|event| &event.payload),
            Some(ModelEvent::Finish {
                reason: ModelFinishReason::ToolCalls
            })
        ));
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
