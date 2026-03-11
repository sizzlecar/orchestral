use orchestral_core::executor::{ExecutionProgressEvent, ExecutorContext};
use orchestral_core::types::Step;
use serde_json::Value;
use tracing::warn;

pub(super) async fn report_agent_progress(
    ctx: &ExecutorContext,
    step: &Step,
    kind: &str,
    action: Option<&str>,
    message: impl Into<String>,
) {
    if let Some(reporter) = &ctx.progress_reporter {
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "agent_progress_kind".to_string(),
            Value::String(kind.to_string()),
        );
        if let Some(action_name) = action.filter(|s| !s.trim().is_empty()) {
            metadata.insert(
                "agent_action".to_string(),
                Value::String(action_name.to_string()),
            );
        }
        let event = ExecutionProgressEvent::new(
            ctx.task_id.clone(),
            Some(step.id.clone()),
            Some("agent".to_string()),
            "agent_progress",
        )
        .with_message(message.into())
        .with_metadata(Value::Object(metadata));
        if let Err(err) = reporter.report(event).await {
            warn!(
                step_id = %step.id,
                error = %err,
                "failed to report agent progress"
            );
        }
    }
}
