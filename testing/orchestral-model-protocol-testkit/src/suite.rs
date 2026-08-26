use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use orchestral_core::model_protocol::{
    ModelBackend, ModelError, ModelErrorCode, ModelEvent, ModelFinishReason, ModelMessage,
    ModelRequest, ModelRequestId, ModelRole, ModelStreamEvent, ModelToolDefinition, ModelUsage,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const MAX_REQUEST_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelFixtureScenario {
    Text,
    Tool,
    Malformed,
    Stalled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelFixtureResponse {
    Complete(Vec<u8>),
    Stall,
}

/// Adapter-owned bridge from canonical cases to family-specific wire bytes.
pub trait ModelFixtureFactory: Send + Sync {
    fn adapter_name(&self) -> &'static str;

    fn backend(
        &self,
        scenario: ModelFixtureScenario,
        endpoint: &str,
    ) -> Result<Arc<dyn ModelBackend>, ModelError>;

    fn response(&self, scenario: ModelFixtureScenario) -> ModelFixtureResponse;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ModelConformanceCase {
    Descriptor,
    InvalidRequest,
    TextStream,
    ToolStream,
    MalformedStream,
    CancelBeforeStart,
    CancelLiveStream,
}

impl ModelConformanceCase {
    pub const ALL: [Self; 7] = [
        Self::Descriptor,
        Self::InvalidRequest,
        Self::TextStream,
        Self::ToolStream,
        Self::MalformedStream,
        Self::CancelBeforeStart,
        Self::CancelLiveStream,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelConformanceResult {
    pub case: ModelConformanceCase,
    pub failure: Option<String>,
}

impl ModelConformanceResult {
    fn passed(case: ModelConformanceCase) -> Self {
        Self {
            case,
            failure: None,
        }
    }

    fn failed(case: ModelConformanceCase, failure: impl Into<String>) -> Self {
        Self {
            case,
            failure: Some(failure.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelConformanceReport {
    adapter_name: String,
    results: Vec<ModelConformanceResult>,
}

impl ModelConformanceReport {
    pub fn adapter_name(&self) -> &str {
        &self.adapter_name
    }

    pub fn results(&self) -> &[ModelConformanceResult] {
        &self.results
    }

    pub fn is_conformant(&self) -> bool {
        self.results.len() == ModelConformanceCase::ALL.len()
            && ModelConformanceCase::ALL.iter().all(|case| {
                self.results
                    .iter()
                    .filter(|result| result.case == *case)
                    .exactly_one()
                    .is_some_and(|result| result.failure.is_none())
            })
    }
}

pub struct ModelConformanceSuite {
    case_timeout: Duration,
}

impl Default for ModelConformanceSuite {
    fn default() -> Self {
        Self {
            case_timeout: Duration::from_secs(2),
        }
    }
}

impl ModelConformanceSuite {
    pub fn with_case_timeout(case_timeout: Duration) -> Result<Self, String> {
        if case_timeout.is_zero() {
            return Err("Model conformance timeout must be positive".to_owned());
        }
        Ok(Self { case_timeout })
    }

    pub async fn run(&self, fixture: &dyn ModelFixtureFactory) -> ModelConformanceReport {
        let mut results = Vec::with_capacity(ModelConformanceCase::ALL.len());
        for case in ModelConformanceCase::ALL {
            let outcome =
                tokio::time::timeout(self.case_timeout, self.run_case(fixture, case)).await;
            let result = match outcome {
                Ok(Ok(())) => ModelConformanceResult::passed(case),
                Ok(Err(failure)) => ModelConformanceResult::failed(case, failure),
                Err(_) => ModelConformanceResult::failed(
                    case,
                    format!("case exceeded {:?}", self.case_timeout),
                ),
            };
            results.push(result);
        }
        ModelConformanceReport {
            adapter_name: fixture.adapter_name().to_owned(),
            results,
        }
    }

    async fn run_case(
        &self,
        fixture: &dyn ModelFixtureFactory,
        case: ModelConformanceCase,
    ) -> Result<(), String> {
        match case {
            ModelConformanceCase::Descriptor => descriptor_case(fixture),
            ModelConformanceCase::InvalidRequest => invalid_request_case(fixture).await,
            ModelConformanceCase::TextStream => text_stream_case(fixture).await,
            ModelConformanceCase::ToolStream => tool_stream_case(fixture).await,
            ModelConformanceCase::MalformedStream => malformed_stream_case(fixture).await,
            ModelConformanceCase::CancelBeforeStart => cancel_before_start_case(fixture).await,
            ModelConformanceCase::CancelLiveStream => cancel_live_stream_case(fixture).await,
        }
    }
}

fn descriptor_case(fixture: &dyn ModelFixtureFactory) -> Result<(), String> {
    let backend = fixture
        .backend(ModelFixtureScenario::Text, "http://127.0.0.1:1")
        .map_err(|error| error.to_string())?;
    let descriptor = backend.descriptor();
    descriptor.validate().map_err(|error| error.to_string())?;
    Ok(())
}

async fn invalid_request_case(fixture: &dyn ModelFixtureFactory) -> Result<(), String> {
    let (server, backend) = start_fixture(fixture, ModelFixtureScenario::Text).await?;
    let request = ModelRequest {
        request_id: ModelRequestId::new("model-conformance-invalid"),
        messages: Vec::new(),
        tools: Vec::new(),
        output_schema: None,
        max_output_tokens: Some(64),
        extensions: BTreeMap::new(),
    };
    let result = backend.start(request, CancellationToken::new()).await;
    let accepted = server.accepted();
    server.shutdown().await?;
    match result {
        Err(error) if error.code == ModelErrorCode::InvalidRequest && accepted == 0 => Ok(()),
        Err(error) => Err(format!(
            "invalid request returned {:?} after {accepted} HTTP accepts",
            error.code
        )),
        Ok(_) => Err("invalid request opened a Model stream".to_owned()),
    }
}

async fn text_stream_case(fixture: &dyn ModelFixtureFactory) -> Result<(), String> {
    let (server, backend) = start_fixture(fixture, ModelFixtureScenario::Text).await?;
    let request = request("model-conformance-text", false);
    let result = collect_success(backend, request.clone()).await;
    server.shutdown().await?;
    let events = result?;
    validate_trace(&request, &events)?;
    let text = events
        .iter()
        .filter_map(|event| match &event.payload {
            ModelEvent::TextDelta { delta } => Some(delta.as_str()),
            _ => None,
        })
        .collect::<String>();
    if text != "hello world" {
        return Err(format!("text deltas reconstructed as {text:?}"));
    }
    if usages(&events) != vec![expected_usage(7, 2)] {
        return Err("text usage did not map to the canonical totals".to_owned());
    }
    if !matches!(
        events.last().map(|event| &event.payload),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::Stop
        })
    ) {
        return Err("text stream did not finish with Stop".to_owned());
    }
    Ok(())
}

async fn tool_stream_case(fixture: &dyn ModelFixtureFactory) -> Result<(), String> {
    let (server, backend) = start_fixture(fixture, ModelFixtureScenario::Tool).await?;
    let request = request("model-conformance-tool", true);
    if !backend.descriptor().capabilities.tool_calls {
        let result = backend.start(request, CancellationToken::new()).await;
        let accepted = server.accepted();
        server.shutdown().await?;
        return match result {
            Err(error) if error.code == ModelErrorCode::Unsupported && accepted == 0 => Ok(()),
            Err(error) => Err(format!(
                "Tool-disabled backend returned {:?} after {accepted} HTTP accepts",
                error.code
            )),
            Ok(_) => Err("Tool-disabled backend opened a Tool stream".to_owned()),
        };
    }
    let result = collect_success(backend, request.clone()).await;
    server.shutdown().await?;
    let events = result?;
    validate_trace(&request, &events)?;

    let mut calls = BTreeMap::<String, (String, String, bool)>::new();
    for event in &events {
        match &event.payload {
            ModelEvent::ToolCallStart { call_id, name } => {
                if calls
                    .insert(
                        call_id.as_str().to_owned(),
                        (name.clone(), String::new(), false),
                    )
                    .is_some()
                {
                    return Err("Tool call started more than once".to_owned());
                }
            }
            ModelEvent::ToolCallArgumentsDelta { call_id, delta } => {
                let Some((_, arguments, ended)) = calls.get_mut(call_id.as_str()) else {
                    return Err("Tool arguments arrived before ToolCallStart".to_owned());
                };
                if *ended {
                    return Err("Tool arguments arrived after ToolCallEnd".to_owned());
                }
                arguments.push_str(delta);
            }
            ModelEvent::ToolCallEnd { call_id } => {
                let Some((_, _, ended)) = calls.get_mut(call_id.as_str()) else {
                    return Err("ToolCallEnd arrived before ToolCallStart".to_owned());
                };
                if std::mem::replace(ended, true) {
                    return Err("Tool call ended more than once".to_owned());
                }
            }
            _ => {}
        }
    }
    let Some((name, arguments, true)) = calls.get("call-1") else {
        return Err("canonical call-1 was not completed".to_owned());
    };
    if calls.len() != 1 || name != "echo" {
        return Err("Tool identity changed during adaptation".to_owned());
    }
    let arguments: Value = serde_json::from_str(arguments)
        .map_err(|error| format!("Tool arguments are not JSON: {error}"))?;
    if arguments != json!({"value": "hello"}) {
        return Err(format!("unexpected Tool arguments: {arguments}"));
    }
    if usages(&events) != vec![expected_usage(8, 3)] {
        return Err("Tool usage did not map to the canonical totals".to_owned());
    }
    if !matches!(
        events.last().map(|event| &event.payload),
        Some(ModelEvent::Finish {
            reason: ModelFinishReason::ToolCalls
        })
    ) {
        return Err("Tool stream did not normalize its finish reason".to_owned());
    }
    Ok(())
}

async fn malformed_stream_case(fixture: &dyn ModelFixtureFactory) -> Result<(), String> {
    let (server, backend) = start_fixture(fixture, ModelFixtureScenario::Malformed).await?;
    let request = request("model-conformance-malformed", false);
    let mut stream = backend
        .start(request, CancellationToken::new())
        .await
        .map_err(|error| format!("malformed fixture failed before stream: {error}"))?;
    let first = stream.next().await;
    let second = stream.next().await;
    server.shutdown().await?;
    match (first, second) {
        (Some(Err(error)), None) if error.code == ModelErrorCode::Protocol => Ok(()),
        (first, second) => Err(format!(
            "malformed stream did not fail once then terminate: {first:?}, {second:?}"
        )),
    }
}

async fn cancel_before_start_case(fixture: &dyn ModelFixtureFactory) -> Result<(), String> {
    let (server, backend) = start_fixture(fixture, ModelFixtureScenario::Stalled).await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = backend
        .start(request("model-conformance-pre-cancel", false), cancellation)
        .await;
    let accepted = server.accepted();
    server.shutdown().await?;
    match result {
        Err(error) if error.code == ModelErrorCode::Cancelled && accepted == 0 => Ok(()),
        Err(error) => Err(format!(
            "pre-cancel returned {:?} after {accepted} HTTP accepts",
            error.code
        )),
        Ok(_) => Err("pre-cancel opened a Model stream".to_owned()),
    }
}

async fn cancel_live_stream_case(fixture: &dyn ModelFixtureFactory) -> Result<(), String> {
    let (server, backend) = start_fixture(fixture, ModelFixtureScenario::Stalled).await?;
    let cancellation = CancellationToken::new();
    let mut stream = backend
        .start(
            request("model-conformance-live-cancel", false),
            cancellation.clone(),
        )
        .await
        .map_err(|error| format!("stalled stream failed to start: {error}"))?;
    cancellation.cancel();
    let first = stream.next().await;
    let second = stream.next().await;
    let accepted = server.accepted();
    server.shutdown().await?;
    match (first, second) {
        (Some(Err(error)), None) if error.code == ModelErrorCode::Cancelled => Ok(()),
        (first, second) => Err(format!(
            "live cancel did not fail once then terminate after {accepted} HTTP accepts: {first:?}, {second:?}"
        )),
    }
}

async fn collect_success(
    backend: Arc<dyn ModelBackend>,
    request: ModelRequest,
) -> Result<Vec<ModelStreamEvent>, String> {
    let mut stream = backend
        .start(request, CancellationToken::new())
        .await
        .map_err(|error| error.to_string())?;
    let mut events = Vec::new();
    while let Some(item) = stream.next().await {
        events.push(item.map_err(|error| error.to_string())?);
    }
    Ok(events)
}

fn validate_trace(request: &ModelRequest, events: &[ModelStreamEvent]) -> Result<(), String> {
    if events.is_empty() {
        return Err("Model stream emitted no canonical events".to_owned());
    }
    for (index, event) in events.iter().enumerate() {
        event
            .validate_for(&request.request_id, index as u64 + 1)
            .map_err(|error| error.to_string())?;
    }
    let finish_positions = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(event.payload, ModelEvent::Finish { .. }).then_some(index)
        })
        .collect::<Vec<_>>();
    if finish_positions != vec![events.len() - 1] {
        return Err(format!(
            "Finish must occur exactly once as the final event: {finish_positions:?}"
        ));
    }
    Ok(())
}

fn usages(events: &[ModelStreamEvent]) -> Vec<ModelUsage> {
    events
        .iter()
        .filter_map(|event| match &event.payload {
            ModelEvent::Usage { usage } => Some(usage.clone()),
            _ => None,
        })
        .collect()
}

fn expected_usage(input_tokens: u64, output_tokens: u64) -> ModelUsage {
    ModelUsage {
        input_tokens: Some(input_tokens),
        output_tokens: Some(output_tokens),
    }
}

fn request(id: &str, with_tool: bool) -> ModelRequest {
    ModelRequest {
        request_id: ModelRequestId::new(id),
        messages: vec![ModelMessage::text(ModelRole::User, "conformance request")],
        tools: with_tool
            .then(|| ModelToolDefinition {
                name: "echo".to_owned(),
                description: "Echo a value".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": false
                }),
            })
            .into_iter()
            .collect(),
        output_schema: None,
        max_output_tokens: Some(64),
        extensions: BTreeMap::new(),
    }
}

async fn start_fixture(
    fixture: &dyn ModelFixtureFactory,
    scenario: ModelFixtureScenario,
) -> Result<(MockSseServer, Arc<dyn ModelBackend>), String> {
    let server = MockSseServer::start(fixture.response(scenario)).await?;
    match fixture.backend(scenario, server.endpoint()) {
        Ok(backend) => Ok((server, backend)),
        Err(error) => {
            server.shutdown().await?;
            Err(error.to_string())
        }
    }
}

struct MockSseServer {
    endpoint: String,
    accepted: Arc<AtomicUsize>,
    shutdown: CancellationToken,
    task: Option<JoinHandle<Result<(), String>>>,
}

impl MockSseServer {
    async fn start(response: ModelFixtureResponse) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| error.to_string())?;
        let address = listener.local_addr().map_err(|error| error.to_string())?;
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_for_task = accepted.clone();
        let shutdown = CancellationToken::new();
        let shutdown_for_task = shutdown.clone();
        let task = tokio::spawn(async move {
            let accept = tokio::select! {
                _ = shutdown_for_task.cancelled() => return Ok(()),
                accept = listener.accept() => accept,
            }
            .map_err(|error| error.to_string())?;
            accepted_for_task.fetch_add(1, Ordering::SeqCst);
            let (mut socket, _) = accept;
            read_request_headers(&mut socket, &shutdown_for_task).await?;
            match response {
                ModelFixtureResponse::Complete(body) => {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    socket
                        .write_all(headers.as_bytes())
                        .await
                        .map_err(|error| error.to_string())?;
                    socket
                        .write_all(&body)
                        .await
                        .map_err(|error| error.to_string())?;
                    socket.shutdown().await.map_err(|error| error.to_string())?;
                }
                ModelFixtureResponse::Stall => {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    shutdown_for_task.cancelled().await;
                }
            }
            Ok(())
        });
        Ok(Self {
            endpoint: format!("http://{address}"),
            accepted,
            shutdown,
            task: Some(task),
        })
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    async fn shutdown(mut self) -> Result<(), String> {
        self.shutdown.cancel();
        self.task
            .take()
            .expect("mock server task is present until shutdown")
            .await
            .map_err(|error| format!("mock server task failed: {error}"))?
    }
}

impl Drop for MockSseServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn read_request_headers(
    socket: &mut TcpStream,
    shutdown: &CancellationToken,
) -> Result<(), String> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4_096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            read = socket.read(&mut buffer) => read,
        }
        .map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("HTTP client closed before sending request headers".to_owned());
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > MAX_REQUEST_HEADER_BYTES {
            return Err("HTTP request headers exceeded the fixture limit".to_owned());
        }
    }
    Ok(())
}

trait ExactlyOne: Iterator + Sized {
    fn exactly_one(mut self) -> Option<Self::Item> {
        let value = self.next()?;
        self.next().is_none().then_some(value)
    }
}

impl<T: Iterator> ExactlyOne for T {}
