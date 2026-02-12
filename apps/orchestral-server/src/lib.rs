use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_stream::stream;
use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use orchestral_api::{ApiService, RuntimeApi, RuntimeAppBuilder};
use orchestral_channels::{InMemoryChannelBindingStore, WebChannel};
use orchestral_files::{BlobMode, BlobServiceConfig, FileService, LocalBlobStoreConfig};
use orchestral_runtime::{
    BlobStoreFactory, BootstrapError, DefaultStoreBackendFactory, HookRegistry, RuntimeApp,
    StoreBackendFactory,
};
use orchestral_stores::Event;
use orchestral_stores_backends::{
    PostgresEventStore, PostgresReferenceStore, PostgresTaskStore, RedisEventStore,
    RedisReferenceStore, RedisTaskStore,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;

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

struct ServerStoreBackendFactory;

#[async_trait]
impl StoreBackendFactory for ServerStoreBackendFactory {
    async fn build_event_store(
        &self,
        spec: &orchestral_config::StoreSpec,
    ) -> Result<Arc<dyn orchestral_core::store::EventStore>, BootstrapError> {
        match spec.backend.trim().to_ascii_lowercase().as_str() {
            "in_memory" | "memory" => DefaultStoreBackendFactory.build_event_store(spec).await,
            "redis" => {
                let url = spec.connection_url.clone().ok_or_else(|| {
                    BootstrapError::MissingStoreConnectionUrl {
                        store: "event".to_string(),
                    }
                })?;
                let prefix = spec
                    .key_prefix
                    .clone()
                    .unwrap_or_else(|| "orchestral:event".to_string());
                let store = RedisEventStore::new(&url, prefix).map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            "postgres" | "postgresql" | "pgsql" => {
                let url = spec.connection_url.clone().ok_or_else(|| {
                    BootstrapError::MissingStoreConnectionUrl {
                        store: "event".to_string(),
                    }
                })?;
                let prefix = spec
                    .key_prefix
                    .clone()
                    .unwrap_or_else(|| "orchestral".to_string());
                let store = PostgresEventStore::new(&url, prefix)
                    .await
                    .map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "event".to_string(),
                backend: backend.to_string(),
            }),
        }
    }

    async fn build_task_store(
        &self,
        spec: &orchestral_config::StoreSpec,
    ) -> Result<Arc<dyn orchestral_core::store::TaskStore>, BootstrapError> {
        match spec.backend.trim().to_ascii_lowercase().as_str() {
            "in_memory" | "memory" => DefaultStoreBackendFactory.build_task_store(spec).await,
            "redis" => {
                let url = spec.connection_url.clone().ok_or_else(|| {
                    BootstrapError::MissingStoreConnectionUrl {
                        store: "task".to_string(),
                    }
                })?;
                let prefix = spec
                    .key_prefix
                    .clone()
                    .unwrap_or_else(|| "orchestral:task".to_string());
                let store = RedisTaskStore::new(&url, prefix).map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            "postgres" | "postgresql" | "pgsql" => {
                let url = spec.connection_url.clone().ok_or_else(|| {
                    BootstrapError::MissingStoreConnectionUrl {
                        store: "task".to_string(),
                    }
                })?;
                let prefix = spec
                    .key_prefix
                    .clone()
                    .unwrap_or_else(|| "orchestral".to_string());
                let store = PostgresTaskStore::new(&url, prefix)
                    .await
                    .map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "task".to_string(),
                backend: backend.to_string(),
            }),
        }
    }

    async fn build_reference_store(
        &self,
        spec: &orchestral_config::StoreSpec,
    ) -> Result<Arc<dyn orchestral_core::store::ReferenceStore>, BootstrapError> {
        match spec.backend.trim().to_ascii_lowercase().as_str() {
            "in_memory" | "memory" => DefaultStoreBackendFactory.build_reference_store(spec).await,
            "redis" => {
                let url = spec.connection_url.clone().ok_or_else(|| {
                    BootstrapError::MissingStoreConnectionUrl {
                        store: "reference".to_string(),
                    }
                })?;
                let prefix = spec
                    .key_prefix
                    .clone()
                    .unwrap_or_else(|| "orchestral:reference".to_string());
                let store =
                    RedisReferenceStore::new(&url, prefix).map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            "postgres" | "postgresql" | "pgsql" => {
                let url = spec.connection_url.clone().ok_or_else(|| {
                    BootstrapError::MissingStoreConnectionUrl {
                        store: "reference".to_string(),
                    }
                })?;
                let prefix = spec
                    .key_prefix
                    .clone()
                    .unwrap_or_else(|| "orchestral".to_string());
                let store = PostgresReferenceStore::new(&url, prefix)
                    .await
                    .map_err(BootstrapError::Store)?;
                Ok(Arc::new(store))
            }
            backend => Err(BootstrapError::UnsupportedStoreBackend {
                store: "reference".to_string(),
                backend: backend.to_string(),
            }),
        }
    }
}

struct ServerBlobStoreFactory;

#[async_trait]
impl BlobStoreFactory for ServerBlobStoreFactory {
    async fn build_blob_store(
        &self,
        config: &orchestral_config::OrchestralConfig,
    ) -> Result<Arc<dyn orchestral_core::io::BlobStore>, BootstrapError> {
        let mode = match config.blobs.mode.trim().to_ascii_lowercase().as_str() {
            "local" => BlobMode::Local,
            "s3" => BlobMode::S3,
            "hybrid" => BlobMode::Hybrid,
            other => {
                return Err(BootstrapError::UnsupportedStoreBackend {
                    store: "blob".to_string(),
                    backend: other.to_string(),
                });
            }
        };
        let service_config = BlobServiceConfig {
            mode: mode.clone(),
            hybrid_write_to: if config.blobs.hybrid.write_to.trim().is_empty() {
                None
            } else {
                Some(config.blobs.hybrid.write_to.clone())
            },
        };

        let mut service = match config
            .blobs
            .catalog
            .backend
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "in_memory" | "memory" => FileService::with_in_memory_catalog(service_config),
            "postgres" | "postgresql" | "pgsql" => {
                let url = config
                    .blobs
                    .catalog
                    .connection_url
                    .as_ref()
                    .ok_or_else(|| BootstrapError::MissingStoreConnectionUrl {
                        store: "blob_catalog".to_string(),
                    })?;
                FileService::with_postgres_catalog(
                    service_config,
                    url,
                    config.blobs.catalog.table_prefix.clone(),
                )
                .await
                .map_err(BootstrapError::Blob)?
            }
            backend => {
                return Err(BootstrapError::UnsupportedStoreBackend {
                    store: "blob_catalog".to_string(),
                    backend: backend.to_string(),
                });
            }
        };

        service = service.with_local_defaults(LocalBlobStoreConfig {
            root_dir: config.blobs.local.root_dir.clone(),
        });

        if matches!(mode, BlobMode::S3)
            || (matches!(mode, BlobMode::Hybrid)
                && config
                    .blobs
                    .hybrid
                    .write_to
                    .trim()
                    .eq_ignore_ascii_case("s3"))
        {
            return Err(BootstrapError::UnsupportedStoreBackend {
                store: "blob".to_string(),
                backend: "s3_requires_custom_client".to_string(),
            });
        }

        Ok(Arc::new(service))
    }
}

struct ServerRuntimeAppBuilder;

#[async_trait]
impl RuntimeAppBuilder for ServerRuntimeAppBuilder {
    async fn build(&self, config_path: PathBuf) -> Result<RuntimeApp, orchestral_api::ApiError> {
        RuntimeApp::from_config_path_with_factories_and_hooks(
            config_path,
            Arc::new(ServerStoreBackendFactory),
            Arc::new(ServerBlobStoreFactory),
            Arc::new(HookRegistry::new()),
        )
        .await
        .map_err(|err| {
            orchestral_api::ApiError::Internal(format!("build runtime app failed: {}", err))
        })
    }
}

pub async fn run_server(config: PathBuf, listen: SocketAddr) -> anyhow::Result<()> {
    let api = Arc::new(
        RuntimeApi::from_config_path_with_builder(config, Arc::new(ServerRuntimeAppBuilder))
            .await?,
    );
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

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .context("bind server listener failed")?;
    println!("orchestral-server listening on http://{}", listen);
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
                Err(RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        thread_id = %thread_id,
                        skipped,
                        "sse subscriber lagged behind; dropping old events"
                    );
                    continue;
                }
                Err(RecvError::Closed) => break,
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
            task_id,
            step_id,
            timestamp,
        } => serde_json::json!({
            "type": "user_input",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "payload": payload,
            "task_id": task_id,
            "step_id": step_id,
            "timestamp": timestamp,
        }),
        Event::AssistantOutput {
            thread_id,
            interaction_id,
            payload,
            task_id,
            step_id,
            timestamp,
        } => serde_json::json!({
            "type": "assistant_output",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "payload": payload,
            "task_id": task_id,
            "step_id": step_id,
            "timestamp": timestamp,
        }),
        Event::Artifact {
            thread_id,
            interaction_id,
            reference_id,
            task_id,
            step_id,
            timestamp,
        } => serde_json::json!({
            "type": "artifact",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "reference_id": reference_id,
            "task_id": task_id,
            "step_id": step_id,
            "timestamp": timestamp,
        }),
        Event::ExternalEvent {
            thread_id,
            interaction_id,
            kind,
            payload,
            task_id,
            step_id,
            timestamp,
        } => serde_json::json!({
            "type": "external_event",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "kind": kind,
            "payload": payload,
            "task_id": task_id,
            "step_id": step_id,
            "timestamp": timestamp,
        }),
        Event::SystemTrace {
            thread_id,
            interaction_id,
            level,
            payload,
            task_id,
            step_id,
            timestamp,
        } => serde_json::json!({
            "type": "system_trace",
            "thread_id": thread_id,
            "interaction_id": interaction_id,
            "level": level,
            "payload": payload,
            "task_id": task_id,
            "step_id": step_id,
            "timestamp": timestamp,
        }),
    }
}
