use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use orchestral_core::executor::{ExecutionProgressEvent, ExecutionProgressReporter};
use orchestral_core::spi::{HookRegistry, RuntimeHookContext, RuntimeHookEventEnvelope, SpiMeta};
use orchestral_core::store::{Event, EventBus, EventStore};

use crate::{InteractionId, ThreadId};

fn map_progress_phase_to_event_type(phase: &str) -> &'static str {
    match phase {
        "step_started" => "step.started",
        "step_completed" => "step.completed",
        "step_failed" => "step.failed",
        _ => "execution.progress",
    }
}

pub(super) struct RuntimeProgressReporter {
    thread_id: ThreadId,
    interaction_id: InteractionId,
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
    hook_registry: Arc<HookRegistry>,
}

impl RuntimeProgressReporter {
    pub(super) fn new(
        thread_id: ThreadId,
        interaction_id: InteractionId,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        hook_registry: Arc<HookRegistry>,
    ) -> Self {
        Self {
            thread_id,
            interaction_id,
            event_store,
            event_bus,
            hook_registry,
        }
    }
}

#[async_trait]
impl ExecutionProgressReporter for RuntimeProgressReporter {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
        let phase = event.phase.clone();
        let step_id = event.step_id.clone();
        let action = event.action.clone();
        let message = event.message.clone();
        let metadata = event.metadata.clone();
        let hook_ctx = RuntimeHookContext {
            thread_id: self.thread_id.clone(),
            interaction_id: self.interaction_id.clone(),
            task_id: Some(event.task_id.clone()),
            step_id: step_id.clone(),
            action: action.clone(),
            message: message.clone(),
            metadata: metadata.clone(),
            extensions: serde_json::Map::new(),
        };

        let hook_event = RuntimeHookEventEnvelope {
            meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
            event_type: map_progress_phase_to_event_type(&phase).to_string(),
            event_version: "1.0.0".to_string(),
            occurred_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            payload: serde_json::json!({
                "thread_id": self.thread_id.to_string(),
                "interaction_id": self.interaction_id.to_string(),
                "task_id": event.task_id.to_string(),
                "step_id": step_id.as_ref().map(ToString::to_string),
                "action": action,
                "phase": phase,
                "message": message,
                "metadata": metadata,
            }),
            extensions: serde_json::Map::new(),
        };
        self.hook_registry.dispatch(&hook_event, &hook_ctx).await;

        let mut payload = serde_json::Map::new();
        payload.insert(
            "category".to_string(),
            Value::String("execution_progress".to_string()),
        );
        payload.insert(
            "interaction_id".to_string(),
            Value::String(self.interaction_id.to_string()),
        );
        payload.insert(
            "task_id".to_string(),
            Value::String(event.task_id.to_string()),
        );
        payload.insert("phase".to_string(), Value::String(event.phase));
        if let Some(step_id) = event.step_id {
            payload.insert("step_id".to_string(), Value::String(step_id.to_string()));
        }
        if let Some(action) = event.action {
            payload.insert("action".to_string(), Value::String(action));
        }
        if let Some(message) = event.message {
            payload.insert("message".to_string(), Value::String(message));
        }
        if !event.metadata.is_null() {
            payload.insert("metadata".to_string(), event.metadata);
        }

        let trace = Event::trace(
            self.thread_id.clone(),
            "info",
            Value::Object(payload.clone()),
        );
        self.event_store
            .append(trace.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.event_bus
            .publish(trace)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}
