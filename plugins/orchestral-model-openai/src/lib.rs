//! OpenAI-compatible HTTP adapter for the canonical Orchestral Model Protocol.

use std::collections::{BTreeMap, VecDeque};
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
pub struct OpenAiCompatibleConfig {
    pub backend_id: String,
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub temperature: f32,
    pub default_max_output_tokens: u64,
    pub max_context_tokens: Option<u64>,
    pub timeout: Duration,
    pub structured_output: bool,
    pub max_buffered_events: usize,
}

impl OpenAiCompatibleConfig {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.backend_id.trim().is_empty()
            || self.endpoint.trim().is_empty()
            || self.api_key.trim().is_empty()
            || self.model.trim().is_empty()
            || !(0.0..=2.0).contains(&self.temperature)
            || self.default_max_output_tokens == 0
            || self.max_buffered_events == 0
            || self.timeout.is_zero()
        {
            return Err(ModelError::invalid_request(
                "invalid OpenAI-compatible ModelBackend configuration",
            ));
        }
        Ok(())
    }

    fn completions_url(&self) -> String {
        let endpoint = self.endpoint.trim_end_matches('/');
        if endpoint.ends_with("/chat/completions") {
            endpoint.to_owned()
        } else {
            format!("{endpoint}/chat/completions")
        }
    }
}

pub struct OpenAiCompatibleBackend {
    client: Client,
    config: OpenAiCompatibleConfig,
}

impl OpenAiCompatibleBackend {
    pub fn new(config: OpenAiCompatibleConfig) -> Result<Self, ModelError> {
        config.validate()?;
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|error| ModelError::new(ModelErrorCode::Internal, error.to_string()))?;
        Ok(Self { client, config })
    }

    fn build_request_body(&self, request: &ModelRequest) -> Result<Value, ModelError> {
        let mut body = Map::new();
        body.insert("model".to_owned(), Value::String(self.config.model.clone()));
        body.insert(
            "messages".to_owned(),
            Value::Array(encode_messages(&request.messages)?),
        );
        body.insert("stream".to_owned(), Value::Bool(true));
        body.insert("stream_options".to_owned(), json!({"include_usage": true}));
        body.insert("temperature".to_owned(), json!(self.config.temperature));
        body.insert(
            "max_tokens".to_owned(),
            json!(request
                .max_output_tokens
                .unwrap_or(self.config.default_max_output_tokens)),
        );
        if !request.tools.is_empty() {
            body.insert(
                "tools".to_owned(),
                Value::Array(
                    request
                        .tools
                        .iter()
                        .map(|tool| {
                            json!({
                                "type": "function",
                                "function": {
                                    "name": tool.name,
                                    "description": tool.description,
                                    "parameters": tool.input_schema,
                                }
                            })
                        })
                        .collect(),
                ),
            );
            body.insert("tool_choice".to_owned(), Value::String("auto".to_owned()));
        }
        if let Some(schema) = &request.output_schema {
            if !self.config.structured_output {
                return Err(ModelError::new(
                    ModelErrorCode::Unsupported,
                    "this OpenAI-compatible endpoint does not advertise structured output",
                ));
            }
            body.insert(
                "response_format".to_owned(),
                json!({
                    "type": "json_schema",
                    "json_schema": {
                        "name": "orchestral_response",
                        "strict": true,
                        "schema": schema,
                    }
                }),
            );
        }
        Ok(Value::Object(body))
    }
}

#[async_trait]
impl ModelBackend for OpenAiCompatibleBackend {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: self.config.backend_id.clone(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                parallel_tool_calls: true,
                structured_output: self.config.structured_output,
                max_context_tokens: self.config.max_context_tokens,
            },
            extensions: BTreeMap::from([(
                "openai-compatible/model".to_owned(),
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
                .post(self.config.completions_url())
                .bearer_auth(&self.config.api_key)
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
        Ok(openai_event_stream(
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
                "OpenAI SSE frame exceeds the Host limit",
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
            .map_err(|error| ModelError::protocol(format!("OpenAI SSE is not UTF-8: {error}")))?;
        if line.is_empty() {
            self.dispatch_frame()?;
        } else if !line.starts_with(':') {
            if let Some(data) = line.strip_prefix("data:") {
                let data = data.strip_prefix(' ').unwrap_or(data);
                self.data_lines.push(data.to_owned());
                if self.data_lines.iter().map(String::len).sum::<usize>() > MAX_SSE_FRAME_BYTES {
                    return Err(ModelError::protocol(
                        "OpenAI SSE data exceeds the Host limit",
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
                    "OpenAI SSE buffered-event limit exceeded",
                ));
            }
            self.frames.push_back(self.data_lines.join("\n"));
            self.data_lines.clear();
        }
        Ok(())
    }
}

#[derive(Default)]
struct OpenAiToolCallState {
    call_id: Option<ModelToolCallId>,
    name: String,
    started: bool,
    ended: bool,
}

struct OpenAiStreamState {
    request_id: orchestral_core::model_protocol::ModelRequestId,
    bytes: HttpByteStream,
    cancellation: CancellationToken,
    decoder: SseDecoder,
    pending: VecDeque<Result<ModelStreamEvent, ModelError>>,
    sequence: u64,
    calls: BTreeMap<u64, OpenAiToolCallState>,
    finish_reason: Option<ModelFinishReason>,
    emitted_content: bool,
    terminated: bool,
    max_buffered_events: usize,
}

impl OpenAiStreamState {
    fn emit(&mut self, payload: ModelEvent) -> Result<(), ModelError> {
        if self.pending.len() >= self.max_buffered_events {
            return Err(ModelError::protocol(
                "OpenAI canonical-event buffer limit exceeded",
            ));
        }
        self.sequence += 1;
        self.pending.push_back(Ok(ModelStreamEvent {
            request_id: self.request_id.clone(),
            event_id: ModelEventId::new(format!(
                "{}-openai-{}",
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
        let payload: Value = serde_json::from_str(&frame)
            .map_err(|error| ModelError::protocol(format!("invalid OpenAI SSE JSON: {error}")))?;
        if let Some(error) = payload.get("error") {
            return Err(ModelError::new(
                ModelErrorCode::Unavailable,
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("OpenAI-compatible endpoint returned an error"),
            ));
        }

        if let Some(choice) = payload
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        {
            if let Some(delta) = choice.get("delta") {
                if let Some(text) = delta
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                {
                    self.emitted_content = true;
                    self.emit(ModelEvent::TextDelta {
                        delta: text.to_owned(),
                    })?;
                }
                for fragment in delta
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    self.handle_tool_fragment(fragment)?;
                }
            }
            if let Some(reason) = choice
                .get("finish_reason")
                .and_then(Value::as_str)
                .filter(|reason| !reason.is_empty())
            {
                self.close_tool_calls()?;
                self.finish_reason = Some(map_finish_reason(reason));
            }
        }
        if let Some(usage) = payload.get("usage").filter(|usage| !usage.is_null()) {
            self.emit(ModelEvent::Usage {
                usage: ModelUsage {
                    input_tokens: usage.get("prompt_tokens").and_then(Value::as_u64),
                    output_tokens: usage.get("completion_tokens").and_then(Value::as_u64),
                },
            })?;
        }
        Ok(())
    }

    fn handle_tool_fragment(&mut self, fragment: &Value) -> Result<(), ModelError> {
        let index = fragment
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| ModelError::protocol("OpenAI Tool delta omitted index"))?;
        let state = self.calls.entry(index).or_default();
        if let Some(id) = fragment
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            let incoming = ModelToolCallId::new(id);
            if state
                .call_id
                .as_ref()
                .is_some_and(|existing| existing != &incoming)
            {
                return Err(ModelError::protocol(
                    "OpenAI changed a Tool call id while streaming",
                ));
            }
            state.call_id = Some(incoming);
        }
        if let Some(name) = fragment.pointer("/function/name").and_then(Value::as_str) {
            state.name.push_str(name);
        }
        let arguments = fragment
            .pointer("/function/arguments")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        let start = if !state.started {
            match (&state.call_id, state.name.is_empty()) {
                (Some(call_id), false) => {
                    state.started = true;
                    Some((call_id.clone(), state.name.clone()))
                }
                _ => None,
            }
        } else {
            None
        };
        let call_id = state.call_id.clone();
        if let Some((call_id, name)) = start {
            self.emitted_content = true;
            self.emit(ModelEvent::ToolCallStart { call_id, name })?;
        }
        if let Some(delta) = arguments {
            let call_id = call_id.ok_or_else(|| {
                ModelError::protocol("OpenAI Tool arguments arrived before call identity")
            })?;
            if !self.calls.get(&index).is_some_and(|state| state.started) {
                return Err(ModelError::protocol(
                    "OpenAI Tool arguments arrived before name",
                ));
            }
            self.emit(ModelEvent::ToolCallArgumentsDelta { call_id, delta })?;
        }
        Ok(())
    }

    fn close_tool_calls(&mut self) -> Result<(), ModelError> {
        let indexes = self.calls.keys().copied().collect::<Vec<_>>();
        for index in indexes {
            let state = self.calls.get_mut(&index).expect("known Tool call");
            if !state.started {
                return Err(ModelError::protocol("OpenAI ended an incomplete Tool call"));
            }
            if !state.ended {
                state.ended = true;
                let call_id = state.call_id.clone().expect("started call has identity");
                self.emit(ModelEvent::ToolCallEnd { call_id })?;
            }
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ModelError> {
        if self.terminated {
            return Ok(());
        }
        self.close_tool_calls()?;
        if !self.emitted_content {
            return Err(ModelError::protocol(
                "OpenAI stream contained neither text nor Tool calls",
            ));
        }
        let mut reason = self.finish_reason.clone().unwrap_or({
            if self.calls.is_empty() {
                ModelFinishReason::Stop
            } else {
                ModelFinishReason::ToolCalls
            }
        });
        if !self.calls.is_empty() && reason == ModelFinishReason::Stop {
            reason = ModelFinishReason::ToolCalls;
        }
        self.emit(ModelEvent::Finish { reason })?;
        self.terminated = true;
        Ok(())
    }
}

fn openai_event_stream(
    request: ModelRequest,
    bytes: HttpByteStream,
    cancellation: CancellationToken,
    max_buffered_events: usize,
) -> ModelStream {
    let state = OpenAiStreamState {
        request_id: request.request_id,
        bytes,
        cancellation,
        decoder: SseDecoder::new(max_buffered_events),
        pending: VecDeque::new(),
        sequence: 0,
        calls: BTreeMap::new(),
        finish_reason: None,
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
        "stop" => ModelFinishReason::Stop,
        "length" => ModelFinishReason::Length,
        "content_filter" => ModelFinishReason::ContentFilter,
        "tool_calls" | "function_call" => ModelFinishReason::ToolCalls,
        _ => ModelFinishReason::Other,
    }
}

fn encode_messages(messages: &[ModelMessage]) -> Result<Vec<Value>, ModelError> {
    let mut encoded = Vec::new();
    for message in messages {
        match message.role {
            ModelRole::System | ModelRole::User => {
                encoded.push(json!({
                    "role": if matches!(message.role, ModelRole::System) { "system" } else { "user" },
                    "content": flatten_text(&message.content)?,
                }));
            }
            ModelRole::Assistant => {
                let mut text = Vec::new();
                let mut calls = Vec::new();
                for content in &message.content {
                    match content {
                        ModelContent::Text { text: value } => text.push(value.clone()),
                        ModelContent::Json { value } | ModelContent::Data { value, .. } => {
                            text.push(value.to_string())
                        }
                        ModelContent::ToolCall {
                            call_id,
                            name,
                            arguments,
                        } => calls.push(json!({
                            "id": call_id.as_str(),
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": serde_json::to_string(arguments).map_err(|error| {
                                    ModelError::invalid_request(format!("invalid Tool arguments: {error}"))
                                })?,
                            }
                        })),
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
                let mut value = Map::new();
                value.insert("role".to_owned(), Value::String("assistant".to_owned()));
                value.insert(
                    "content".to_owned(),
                    if text.is_empty() {
                        Value::Null
                    } else {
                        Value::String(text.join("\n"))
                    },
                );
                if !calls.is_empty() {
                    value.insert("tool_calls".to_owned(), Value::Array(calls));
                }
                encoded.push(Value::Object(value));
            }
            ModelRole::Tool => {
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
                    encoded.push(json!({
                        "role": "tool",
                        "tool_call_id": call_id.as_str(),
                        "content": json!({"result": result, "is_error": is_error}).to_string(),
                    }));
                }
            }
            _ => {
                return Err(ModelError::new(
                    ModelErrorCode::Unsupported,
                    "unsupported canonical model role",
                ))
            }
        }
    }
    Ok(encoded)
}

fn flatten_text(content: &[ModelContent]) -> Result<String, ModelError> {
    content
        .iter()
        .map(|content| match content {
            ModelContent::Text { text } => Ok(text.clone()),
            ModelContent::Json { value } | ModelContent::Data { value, .. } => {
                Ok(value.to_string())
            }
            _ => Err(ModelError::invalid_request(
                "this role cannot contain Tool call/result blocks",
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("\n"))
}

#[cfg(test)]
fn parse_response(
    request: &ModelRequest,
    response: &Value,
) -> Result<Vec<ModelStreamEvent>, ModelError> {
    let choice = response
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(|| ModelError::protocol("OpenAI response contains no choice"))?;
    let message = choice
        .get("message")
        .and_then(Value::as_object)
        .ok_or_else(|| ModelError::protocol("OpenAI choice contains no message"))?;
    let mut payloads = Vec::new();
    if let Some(text) = message
        .get("content")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    {
        payloads.push(ModelEvent::TextDelta {
            delta: text.to_owned(),
        });
    }
    let mut tool_count = 0_usize;
    for call in message
        .get("tool_calls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let id = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| ModelError::protocol("OpenAI Tool call omitted id"))?;
        let function = call
            .get("function")
            .and_then(Value::as_object)
            .ok_or_else(|| ModelError::protocol("OpenAI Tool call omitted function"))?;
        let name = function
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| ModelError::protocol("OpenAI Tool call omitted name"))?;
        let arguments = function
            .get("arguments")
            .and_then(Value::as_str)
            .filter(|arguments| !arguments.is_empty())
            .unwrap_or("{}");
        let call_id = ModelToolCallId::new(id);
        payloads.extend([
            ModelEvent::ToolCallStart {
                call_id: call_id.clone(),
                name: name.to_owned(),
            },
            ModelEvent::ToolCallArgumentsDelta {
                call_id: call_id.clone(),
                delta: arguments.to_owned(),
            },
            ModelEvent::ToolCallEnd { call_id },
        ]);
        tool_count += 1;
    }
    if let Some(usage) = response.get("usage") {
        payloads.push(ModelEvent::Usage {
            usage: ModelUsage {
                input_tokens: usage.get("prompt_tokens").and_then(Value::as_u64),
                output_tokens: usage.get("completion_tokens").and_then(Value::as_u64),
            },
        });
    }
    if payloads.is_empty() {
        return Err(ModelError::protocol(
            "OpenAI response contains neither content nor Tool calls",
        ));
    }
    let finish = if tool_count > 0 {
        ModelFinishReason::ToolCalls
    } else {
        match choice.get("finish_reason").and_then(Value::as_str) {
            Some("stop") | None => ModelFinishReason::Stop,
            Some("length") => ModelFinishReason::Length,
            Some("content_filter") => ModelFinishReason::ContentFilter,
            Some("tool_calls") | Some("function_call") => ModelFinishReason::ToolCalls,
            Some(_) => ModelFinishReason::Other,
        }
    };
    payloads.push(ModelEvent::Finish { reason: finish });
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
                    "{}-openai-{sequence}",
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

    struct OpenAiConformanceFixture;

    impl ModelFixtureFactory for OpenAiConformanceFixture {
        fn adapter_name(&self) -> &'static str {
            "openai-compatible"
        }

        fn backend(
            &self,
            _scenario: ModelFixtureScenario,
            endpoint: &str,
        ) -> Result<std::sync::Arc<dyn ModelBackend>, ModelError> {
            OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
                backend_id: "openai-conformance".to_owned(),
                endpoint: endpoint.to_owned(),
                api_key: "fixture-key".to_owned(),
                model: "fixture-model".to_owned(),
                temperature: 0.0,
                default_max_output_tokens: 64,
                max_context_tokens: Some(8_192),
                timeout: Duration::from_secs(1),
                structured_output: true,
                max_buffered_events: 128,
            })
            .map(|backend| std::sync::Arc::new(backend) as std::sync::Arc<dyn ModelBackend>)
        }

        fn response(&self, scenario: ModelFixtureScenario) -> ModelFixtureResponse {
            let body = match scenario {
                ModelFixtureScenario::Text => concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\n",
                    "data: {\"choices\":[{\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":2}}\n\n",
                    "data: [DONE]\n\n"
                )
                .as_bytes()
                .to_vec(),
                ModelFixtureScenario::Tool => concat!(
                    "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":\\\"hello\\\"}\"}}]}}]}\n\n",
                    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
                    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":3}}\n\n",
                    "data: [DONE]\n\n"
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
            .run(&OpenAiConformanceFixture)
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
                data(json!({"choices":[{"delta":{"content": fragments[0]}}]})),
                data(json!({"choices":[{"delta":{"content":"must-not-arrive"},"finish_reason":"stop"}]})),
            )
            .into_bytes(),
            ModelStreamStressFault::BufferedBurst if case.index() % 10 == 3 => data(json!({
                "choices": [{"delta": {"tool_calls": [
                    {"index": 0, "id": "burst-a", "function": {"name": "echo", "arguments": "{}"}},
                    {"index": 1, "id": "burst-b", "function": {"name": "echo", "arguments": "{}"}}
                ]}}]
            }))
            .into_bytes(),
            ModelStreamStressFault::BufferedBurst => (0..5)
                .map(|index| {
                    data(json!({
                        "choices": [{"delta": {"content": format!("burst-{index}")}}]
                    }))
                })
                .collect::<String>()
                .into_bytes(),
            ModelStreamStressFault::None
            | ModelStreamStressFault::ExtraAfterTerminal
            | ModelStreamStressFault::CancelBeforePoll => {
                let mut body = format!(
                    "{}{}{}data: [DONE]\n\n",
                    data(json!({"choices":[{"delta":{"content": fragments[0]}}]})),
                    data(json!({"choices":[{"delta":{"content": fragments[1]},"finish_reason":"stop"}]})),
                    data(json!({"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":2}})),
                );
                if case.fault() == ModelStreamStressFault::ExtraAfterTerminal {
                    body.push_str(&data(json!({
                        "choices": [{"delta": {"content": "must-not-arrive"}}]
                    })));
                    body.push_str("data: [DONE]\n\n");
                }
                body.into_bytes()
            }
        }
    }

    #[tokio::test]
    async fn ten_thousand_chunk_fault_and_backpressure_plans_preserve_openai_stream_invariants() {
        let report = ModelStreamStressSuite::default()
            .run("openai-compatible", |case| {
                let cancellation = CancellationToken::new();
                if case.cancel_before_poll() {
                    cancellation.cancel();
                }
                let chunks = case
                    .split_wire(&stress_wire(case))
                    .into_iter()
                    .map(|chunk| Ok::<Bytes, reqwest::Error>(Bytes::from(chunk)));
                Ok(openai_event_stream(
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
    fn parses_parallel_tool_calls_into_canonical_sequence() {
        let request = request();
        let events = parse_response(
            &request,
            &json!({
                "choices": [{
                    "message": {"tool_calls": [
                        {"id": "a", "function": {"name": "echo", "arguments": "{\"value\":1}"}},
                        {"id": "b", "function": {"name": "echo", "arguments": "{\"value\":2}"}}
                    ]},
                    "finish_reason": "tool_calls"
                }],
                "usage": {"prompt_tokens": 10, "completion_tokens": 4}
            }),
        )
        .unwrap();
        assert_eq!(events.len(), 8);
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
    fn maps_tool_history_without_flattening_call_identity() {
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
        let encoded = encode_messages(&messages).unwrap();
        assert_eq!(encoded[0]["tool_calls"][0]["id"], "call-1");
        assert_eq!(encoded[1]["tool_call_id"], "call-1");
    }

    #[tokio::test]
    async fn fragmented_sse_preserves_parallel_tool_call_identity_and_order() {
        let request = request();
        let raw = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[",
            "{\"index\":0,\"id\":\"call-a\",\"function\":{\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":\"}},",
            "{\"index\":1,\"id\":\"call-b\",\"function\":{\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":\"}}",
            "]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[",
            "{\"index\":0,\"function\":{\"arguments\":\"1}\"}},",
            "{\"index\":1,\"function\":{\"arguments\":\"2}\"}}",
            "]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":4}}\n\n",
            "data: [DONE]\n\n"
        );
        let split = raw.len() / 3;
        let chunks = vec![
            Ok::<Bytes, reqwest::Error>(Bytes::copy_from_slice(&raw.as_bytes()[..split])),
            Ok(Bytes::copy_from_slice(&raw.as_bytes()[split..split + 7])),
            Ok(Bytes::copy_from_slice(&raw.as_bytes()[split + 7..])),
        ];
        let mut stream = openai_event_stream(
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
        let starts = events
            .iter()
            .filter_map(|event| match &event.payload {
                ModelEvent::ToolCallStart { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(starts, vec!["call-a", "call-b"]);
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
                b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n",
            )));
        }
        bytes.push(Ok(Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
        )));
        let request = request();
        let mut stream = openai_event_stream(
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
