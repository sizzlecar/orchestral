use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::broadcast;

use orchestral_runtime::{OrchestratorResult, RuntimeApp};
use orchestral_stores::Event;

use crate::dto::{
    HistoryEventView, InteractionSubmitRequest, InteractionSubmitResponse, SubmitStatus, ThreadView,
};
use crate::{ApiError, ApiService};

#[derive(Clone)]
pub struct RuntimeApi {
    app: Arc<RuntimeApp>,
}

impl RuntimeApi {
    pub async fn from_config_path(config: PathBuf) -> Result<Self, ApiError> {
        let app = RuntimeApp::from_config_path(config)
            .await
            .map_err(|err| ApiError::Internal(format!("build runtime app failed: {}", err)))?;
        Ok(Self { app: Arc::new(app) })
    }

    pub fn runtime_app(&self) -> Arc<RuntimeApp> {
        self.app.clone()
    }
}

#[async_trait]
impl ApiService for RuntimeApi {
    async fn set_thread_id(&self, thread_id: String) -> Result<(), ApiError> {
        if thread_id.trim().is_empty() {
            return Err(ApiError::InvalidArgument(
                "thread_id must not be empty".to_string(),
            ));
        }
        let mut thread = self.app.orchestrator.thread_runtime.thread.write().await;
        thread.id = thread_id;
        Ok(())
    }

    async fn thread(&self) -> Result<ThreadView, ApiError> {
        let thread = self.app.orchestrator.thread_runtime.thread.read().await;
        Ok(ThreadView {
            id: thread.id.clone(),
            created_at: thread.created_at,
            updated_at: thread.updated_at,
        })
    }

    async fn submit_interaction(
        &self,
        request: InteractionSubmitRequest,
    ) -> Result<InteractionSubmitResponse, ApiError> {
        if request.input.trim().is_empty() {
            return Err(ApiError::InvalidArgument(
                "input must not be empty".to_string(),
            ));
        }

        let thread_id = self.app.orchestrator.thread_runtime.thread_id().await;
        let interaction_id = request.request_id.unwrap_or_else(|| "api".to_string());
        let event = Event::user_input(thread_id, interaction_id, json!(request.input));

        let result = self
            .app
            .orchestrator
            .handle_event(event)
            .await
            .map_err(|err| {
                ApiError::Internal(format!("orchestrator handle_event failed: {}", err))
            })?;

        let response = match result {
            OrchestratorResult::Started {
                interaction_id,
                task_id,
                ..
            } => InteractionSubmitResponse {
                status: SubmitStatus::Started,
                interaction_id: Some(interaction_id),
                task_id: Some(task_id),
                message: None,
            },
            OrchestratorResult::Merged {
                interaction_id,
                task_id,
                ..
            } => InteractionSubmitResponse {
                status: SubmitStatus::Merged,
                interaction_id: Some(interaction_id),
                task_id: Some(task_id),
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

    async fn query_history(&self, limit: usize) -> Result<Vec<HistoryEventView>, ApiError> {
        let mut events = self
            .app
            .orchestrator
            .thread_runtime
            .query_history(limit)
            .await
            .map_err(|err| ApiError::Internal(format!("query history failed: {}", err)))?;
        events.sort_by(|a, b| a.timestamp().cmp(&b.timestamp()));
        Ok(events.iter().map(event_to_view).collect())
    }

    fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.app.orchestrator.thread_runtime.subscribe_events()
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
