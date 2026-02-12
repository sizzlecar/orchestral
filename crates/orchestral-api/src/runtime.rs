use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use orchestral_runtime::{OrchestratorResult, RuntimeApp};
use orchestral_stores::Event;

use crate::dto::{
    HistoryEventView, InteractionSubmitRequest, InteractionSubmitResponse, SubmitStatus, ThreadView,
};
use crate::{ApiError, ApiService};

#[async_trait]
pub trait RuntimeAppBuilder: Send + Sync {
    async fn build(&self, config_path: PathBuf) -> Result<RuntimeApp, ApiError>;
}

struct DefaultRuntimeAppBuilder;

#[async_trait]
impl RuntimeAppBuilder for DefaultRuntimeAppBuilder {
    async fn build(&self, config_path: PathBuf) -> Result<RuntimeApp, ApiError> {
        RuntimeApp::from_config_path(config_path)
            .await
            .map_err(|err| ApiError::Internal(format!("build runtime app failed: {}", err)))
    }
}

#[derive(Clone)]
pub struct RuntimeApi {
    config_path: PathBuf,
    app_builder: Arc<dyn RuntimeAppBuilder>,
    apps: Arc<RwLock<HashMap<String, Arc<RuntimeApp>>>>,
}

impl RuntimeApi {
    pub async fn from_config_path(config: PathBuf) -> Result<Self, ApiError> {
        Self::from_config_path_with_builder(config, Arc::new(DefaultRuntimeAppBuilder)).await
    }

    pub async fn from_config_path_with_builder(
        config: PathBuf,
        app_builder: Arc<dyn RuntimeAppBuilder>,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            config_path: config,
            app_builder,
            apps: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn runtime_app_for_thread(
        &self,
        thread_id: &str,
    ) -> Result<Arc<RuntimeApp>, ApiError> {
        self.get_app(thread_id).await
    }

    async fn create_app_for_thread(&self, thread_id: &str) -> Result<Arc<RuntimeApp>, ApiError> {
        let app = self.app_builder.build(self.config_path.clone()).await?;
        {
            let mut thread = app.orchestrator.thread_runtime.thread.write().await;
            thread.id = thread_id.into();
        }
        Ok(Arc::new(app))
    }

    async fn get_app(&self, thread_id: &str) -> Result<Arc<RuntimeApp>, ApiError> {
        if thread_id.trim().is_empty() {
            return Err(ApiError::InvalidArgument(
                "thread_id must not be empty".to_string(),
            ));
        }

        if let Some(existing) = self.apps.read().await.get(thread_id).cloned() {
            return Ok(existing);
        }

        Err(ApiError::NotFound(format!(
            "thread '{}' not found",
            thread_id
        )))
    }

    async fn get_or_create_app(&self, thread_id: &str) -> Result<Arc<RuntimeApp>, ApiError> {
        if thread_id.trim().is_empty() {
            return Err(ApiError::InvalidArgument(
                "thread_id must not be empty".to_string(),
            ));
        }

        if let Some(existing) = self.apps.read().await.get(thread_id).cloned() {
            return Ok(existing);
        }

        let created = self.create_app_for_thread(thread_id).await?;
        let mut apps = self.apps.write().await;
        if let Some(existing) = apps.get(thread_id).cloned() {
            return Ok(existing);
        }
        apps.insert(thread_id.to_string(), created.clone());
        Ok(created)
    }
}

#[async_trait]
impl ApiService for RuntimeApi {
    async fn create_thread(&self, preferred_id: Option<String>) -> Result<ThreadView, ApiError> {
        let thread_id = preferred_id
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let app = self.get_or_create_app(&thread_id).await?;
        let thread = app.orchestrator.thread_runtime.thread.read().await;
        Ok(ThreadView {
            id: thread.id.to_string(),
            created_at: thread.created_at,
            updated_at: thread.updated_at,
        })
    }

    async fn get_thread(&self, thread_id: &str) -> Result<ThreadView, ApiError> {
        let app = self.get_app(thread_id).await?;
        let thread = app.orchestrator.thread_runtime.thread.read().await;
        Ok(ThreadView {
            id: thread.id.to_string(),
            created_at: thread.created_at,
            updated_at: thread.updated_at,
        })
    }

    async fn submit_interaction(
        &self,
        thread_id: &str,
        request: InteractionSubmitRequest,
    ) -> Result<InteractionSubmitResponse, ApiError> {
        if request.input.trim().is_empty() {
            return Err(ApiError::InvalidArgument(
                "input must not be empty".to_string(),
            ));
        }

        let app = self.get_app(thread_id).await?;
        let interaction_id = request.request_id.unwrap_or_else(|| "api".to_string());
        let event = Event::user_input(thread_id.to_string(), interaction_id, json!(request.input));

        let result = app.orchestrator.handle_event(event).await.map_err(|err| {
            ApiError::Internal(format!("orchestrator handle_event failed: {}", err))
        })?;

        let response = match result {
            OrchestratorResult::Started {
                interaction_id,
                task_id,
                ..
            } => InteractionSubmitResponse {
                status: SubmitStatus::Started,
                interaction_id: Some(interaction_id.to_string()),
                task_id: Some(task_id.to_string()),
                message: None,
            },
            OrchestratorResult::Merged {
                interaction_id,
                task_id,
                ..
            } => InteractionSubmitResponse {
                status: SubmitStatus::Merged,
                interaction_id: Some(interaction_id.to_string()),
                task_id: Some(task_id.to_string()),
                message: None,
            },
            OrchestratorResult::Rejected { reason } => InteractionSubmitResponse {
                status: SubmitStatus::Rejected,
                interaction_id: None,
                task_id: None,
                message: Some(reason),
            },
            OrchestratorResult::Queued => InteractionSubmitResponse {
                status: SubmitStatus::Queued,
                interaction_id: None,
                task_id: None,
                message: Some("interaction queued".to_string()),
            },
        };

        Ok(response)
    }

    async fn query_history(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<HistoryEventView>, ApiError> {
        let app = self.get_app(thread_id).await?;
        let mut events = app
            .orchestrator
            .thread_runtime
            .query_history(limit)
            .await
            .map_err(|err| ApiError::Internal(format!("query history failed: {}", err)))?;
        events.sort_by_key(|a| a.timestamp());
        Ok(events.iter().map(event_to_view).collect())
    }

    async fn subscribe_events(
        &self,
        thread_id: &str,
    ) -> Result<broadcast::Receiver<Event>, ApiError> {
        let app = self.get_app(thread_id).await?;
        Ok(app.orchestrator.thread_runtime.subscribe_events())
    }
}

fn event_to_view(event: &Event) -> HistoryEventView {
    match event {
        Event::UserInput {
            payload, timestamp, ..
        } => HistoryEventView {
            event_type: "user_input".to_string(),
            timestamp: *timestamp,
            role: "user".to_string(),
            content: payload_to_string(payload),
        },
        Event::AssistantOutput {
            payload, timestamp, ..
        } => HistoryEventView {
            event_type: "assistant_output".to_string(),
            timestamp: *timestamp,
            role: "assistant".to_string(),
            content: payload_to_string(payload),
        },
        Event::Artifact {
            reference_id,
            timestamp,
            ..
        } => HistoryEventView {
            event_type: "artifact".to_string(),
            timestamp: *timestamp,
            role: "system".to_string(),
            content: format!("artifact:{}", reference_id),
        },
        Event::ExternalEvent {
            kind,
            payload,
            timestamp,
            ..
        } => HistoryEventView {
            event_type: "external_event".to_string(),
            timestamp: *timestamp,
            role: "system".to_string(),
            content: format!("external:{} {}", kind, payload_to_string(payload)),
        },
        Event::SystemTrace {
            level,
            payload,
            timestamp,
            ..
        } => HistoryEventView {
            event_type: "system_trace".to_string(),
            timestamp: *timestamp,
            role: "system".to_string(),
            content: format!("trace:{} {}", level, payload_to_string(payload)),
        },
    }
}

fn payload_to_string(payload: &serde_json::Value) -> String {
    if let Some(s) = payload.as_str() {
        return s.to_string();
    }
    for key in ["content", "message", "text"] {
        if let Some(s) = payload.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    payload.to_string()
}
