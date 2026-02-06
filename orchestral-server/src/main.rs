use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_stream::stream;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use orchestral_api::{ApiService, RuntimeApi};
use orchestral_channels::{InMemoryChannelBindingStore, WebChannel};
use orchestral_stores::Event;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "orchestral-server")]
struct Args {
    #[arg(long, default_value = "config/orchestral.cli.yaml")]
    config: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    web: Arc<WebChannel<RuntimeApi, InMemoryChannelBindingStore>>,
    api: Arc<RuntimeApi>,
}

#[derive(Debug, Deserialize)]
struct SubmitRequest {
    request_id: Option<String>,
    input: String,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: String,
    message: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let api = Arc::new(RuntimeApi::from_config_path(args.config).await?);
    let web = Arc::new(WebChannel::new(
        api.clone(),
        Arc::new(InMemoryChannelBindingStore::new()),
    ));

    let state = AppState { web, api };

    let app = Router::new()
        .route("/health", get(health))
        .route("/threads/{session}/messages", post(submit_message))
        .route("/threads/{session}/events", get(stream_events))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .context("bind server listener failed")?;
    println!("orchestral-server listening on http://{}", args.listen);
    axum::serve(listener, app)
        .await
        .context("server terminated with error")
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"ok"}))
}

async fn submit_message(
    State(state): State<AppState>,
    Path(session): Path<String>,
    Json(payload): Json<SubmitRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorBody>)> {
    let resp = state
        .web
        .submit_message(&session, payload.request_id, payload.input)
        .await
        .map_err(map_api_error)?;
    Ok(Json(resp))
}

async fn stream_events(
    State(state): State<AppState>,
    Path(session): Path<String>,
) -> Result<
    Sse<impl futures_util::Stream<Item = Result<SseEvent, std::convert::Infallible>>>,
    (StatusCode, Json<ErrorBody>),
> {
    let thread_id = state
        .web
        .resolve_thread_id(&session)
        .await
        .map_err(map_api_error)?;

    let mut rx = state
        .api
        .subscribe_events(&thread_id)
        .await
        .map_err(map_api_error)?;

    let event_stream = stream! {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if event.thread_id() != thread_id {
                        continue;
                    }
                    let payload = serde_json::to_string(&event_to_json(&event))
                        .unwrap_or_else(|_| "{}".to_string());
                    yield Ok(SseEvent::default().event("runtime_event").data(payload));
                }
                Err(_) => {
                    break;
                }
            }
        }
    };

    Ok(Sse::new(event_stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(10))
            .text("keepalive"),
    ))
}

fn map_api_error(err: orchestral_api::ApiError) -> (StatusCode, Json<ErrorBody>) {
    let (status, code) = match err.code() {
        orchestral_api::ErrorCode::NotFound => (StatusCode::NOT_FOUND, "not_found"),
        orchestral_api::ErrorCode::PermissionDenied => (StatusCode::FORBIDDEN, "permission_denied"),
        orchestral_api::ErrorCode::Conflict => (StatusCode::CONFLICT, "conflict"),
        orchestral_api::ErrorCode::InvalidArgument => (StatusCode::BAD_REQUEST, "invalid_argument"),
        orchestral_api::ErrorCode::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    };
    (
        status,
        Json(ErrorBody {
            code: code.to_string(),
            message: err.to_string(),
        }),
    )
}

fn event_to_json(event: &Event) -> serde_json::Value {
    match event {
        Event::UserInput {
            thread_id,
            interaction_id,
            payload,
            timestamp,
        } => serde_json::json!({
            "type": "user_input",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "payload": payload,
            "timestamp": timestamp,
        }),
        Event::AssistantOutput {
            thread_id,
            interaction_id,
            payload,
            timestamp,
        } => serde_json::json!({
            "type": "assistant_output",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "payload": payload,
            "timestamp": timestamp,
        }),
        Event::Artifact {
            thread_id,
            interaction_id,
            reference_id,
            timestamp,
        } => serde_json::json!({
            "type": "artifact",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "reference_id": reference_id,
            "timestamp": timestamp,
        }),
        Event::ExternalEvent {
            thread_id,
            kind,
            payload,
            timestamp,
        } => serde_json::json!({
            "type": "external_event",
            "thread_id": thread_id,
            "kind": kind,
            "payload": payload,
            "timestamp": timestamp,
        }),
        Event::SystemTrace {
            thread_id,
            level,
            payload,
            timestamp,
        } => serde_json::json!({
            "type": "system_trace",
            "thread_id": thread_id,
            "level": level,
            "payload": payload,
            "timestamp": timestamp,
        }),
    }
}
