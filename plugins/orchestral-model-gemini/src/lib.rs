//! Gemini-native HTTP adapter for the canonical Orchestral Model Protocol.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{stream, StreamExt};
use orchestral_core::model_protocol::{
    ModelBackend, ModelCapabilities, ModelContent, ModelDescriptor, ModelError, ModelErrorCode,
    ModelEvent, ModelEventId, ModelFinishReason, ModelMessage, ModelRequest, ModelRole,
    ModelStream, ModelStreamEvent, ModelToolCallId, ModelUsage,
};
use reqwest::{Client, StatusCode};
use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct GeminiModelConfig {
    pub backend_id: String,
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub temperature: f32,
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
            || self.api_key.trim().is_empty()
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
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(cancelled_error()),
            response = self.client
                .post(self.config.stream_url())
                .header("x-goog-api-key", &self.config.api_key)
                .json(&body)
                .send() => response,
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
    };

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
                api_key: "fixture-key".to_owned(),
                model: "fixture-model".to_owned(),
                temperature: 0.0,
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

    #[tokio::test]
    async fn passes_shared_model_backend_conformance_suite() {
        let report = ModelConformanceSuite::default()
            .run(&GeminiConformanceFixture)
            .await;
        assert!(report.is_conformant(), "{:#?}", report.results());
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
