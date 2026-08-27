use futures_util::StreamExt;
use orchestral_core::model_protocol::{
    ModelBackend, ModelEvent, ModelFinishReason, ModelMessage, ModelRequest, ModelRequestId,
    ModelRole, ModelUsage,
};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveModelSmokeReport {
    pub event_count: u64,
    pub text_bytes: usize,
    pub usage: Option<ModelUsage>,
    pub finish_reason: ModelFinishReason,
}

/// Runs one real text request through an already configured production
/// adapter. The caller owns opt-in, credentials, endpoint, model selection,
/// timeout, and any task-quality evaluation. This function checks only the
/// provider-neutral descriptor/request/stream wiring contract.
pub async fn run_live_text_smoke(
    backend: &dyn ModelBackend,
) -> Result<LiveModelSmokeReport, String> {
    backend
        .descriptor()
        .validate()
        .map_err(|error| format!("live descriptor is invalid: {error}"))?;
    let request_id = ModelRequestId::new("model-live-smoke");
    let request = ModelRequest {
        request_id: request_id.clone(),
        messages: vec![ModelMessage::text(
            ModelRole::User,
            "Reply with one short plain-text sentence confirming the connection.",
        )],
        tools: Vec::new(),
        output_schema: None,
        // Reasoning models may spend part of the output budget on hidden
        // thinking before emitting visible text. Keep this large enough for a
        // protocol smoke without making the prompt itself provider-specific.
        max_output_tokens: Some(512),
        extensions: Default::default(),
    };
    request
        .validate()
        .map_err(|error| format!("live request is invalid: {error}"))?;
    let mut stream = backend
        .start(request, CancellationToken::new())
        .await
        .map_err(|error| format!("live adapter start failed: {error}"))?;
    let mut expected_sequence = 1_u64;
    let mut event_count = 0_u64;
    let mut text = String::new();
    let mut usage = None;
    let mut finish_reason = None;

    while let Some(item) = stream.next().await {
        let event = item.map_err(|error| format!("live adapter stream failed: {error}"))?;
        if finish_reason.is_some() {
            return Err("live adapter emitted an event after Finish".to_owned());
        }
        event
            .validate_for(&request_id, expected_sequence)
            .map_err(|error| format!("live adapter emitted an invalid event: {error}"))?;
        expected_sequence = expected_sequence.saturating_add(1);
        event_count = event_count.saturating_add(1);
        match event.payload {
            ModelEvent::TextDelta { delta } => text.push_str(&delta),
            ModelEvent::Usage { usage: observed } => usage = Some(observed),
            ModelEvent::Finish { reason } => finish_reason = Some(reason),
            ModelEvent::ToolCallStart { .. }
            | ModelEvent::ToolCallArgumentsDelta { .. }
            | ModelEvent::ToolCallEnd { .. } => {
                return Err("live text smoke unexpectedly produced a Tool call".to_owned())
            }
            _ => return Err("live adapter emitted an unsupported event".to_owned()),
        }
    }

    let finish_reason = finish_reason
        .ok_or_else(|| "live adapter stream ended without exactly one Finish".to_owned())?;
    if text.trim().is_empty() {
        return Err("live adapter produced no text".to_owned());
    }
    Ok(LiveModelSmokeReport {
        event_count,
        text_bytes: text.len(),
        usage,
        finish_reason,
    })
}
