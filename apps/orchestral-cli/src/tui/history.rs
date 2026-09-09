//! Presentation-only replay; no model calls, journal writes, or Tool execution.

use orchestral_core::agent_protocol::wire::{AgentEvent, Content, ContentBody};
use orchestral_core::agent_session::AgentSessionEvent;
use orchestral_core::model_protocol::{ModelContent, ModelMessage};
use orchestral_core::session_history::{SessionHistory, SessionHistoryStatus};

use super::activity::{ActivityDetail, ActivityDetailStyle, ActivityStatus};
use super::state::{TranscriptEntry, TranscriptRole};

pub(crate) fn history_entries(history: &SessionHistory) -> Vec<TranscriptEntry> {
    let mut entries = Vec::new();
    for run in &history.runs {
        let id = run.registration.run_id();
        let records = history
            .records
            .iter()
            .filter(|record| &record.run_id == id)
            .collect::<Vec<_>>();
        if !records
            .iter()
            .any(|record| matches!(record.payload, AgentSessionEvent::RunInputCommitted { .. }))
        {
            entries.push(TranscriptEntry::user(contents_text(
                &run.registration.run().spec.input,
            )));
        }
        let mut initial_input = !records
            .iter()
            .any(|record| matches!(record.payload, AgentSessionEvent::RunInputCommitted { .. }));
        let mut final_recorded = false;
        for record in records {
            let entry_id = format!("history-{}", record.event_id);
            match &record.payload {
                AgentSessionEvent::RunInputCommitted { message } => {
                    let mut entry = TranscriptEntry::user(model_text(message));
                    entry.continuation = initial_input;
                    entry.id = Some(entry_id);
                    initial_input = true;
                    entries.push(entry)
                }
                AgentSessionEvent::RunOutputCommitted { message, .. } => {
                    entries.push(TranscriptEntry::assistant(entry_id, model_text(message)));
                    final_recorded = true;
                }
                AgentSessionEvent::ToolExchangeCommitted {
                    assistant, tool, ..
                } => {
                    let names = assistant
                        .content
                        .iter()
                        .filter_map(|item| match item {
                            ModelContent::ToolCall { name, .. } => Some(name.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let failed = tool.content.iter().any(|item| {
                        matches!(item, ModelContent::ToolResult { is_error: true, .. })
                    });
                    let detail = format!("{}\n{}", model_text(assistant), model_text(tool));
                    let mut excerpt: String = detail.chars().take(2000).collect();
                    if detail.chars().count() > 2000 {
                        excerpt.push_str("\n… (full result retained in session journal)");
                    }
                    entries.push(TranscriptEntry {
                        continuation: false,
                        id: Some(entry_id),
                        role: TranscriptRole::Tool,
                        text: names,
                        tool_status: Some(if failed {
                            ActivityStatus::Failed
                        } else {
                            ActivityStatus::Succeeded
                        }),
                        tool_details: excerpt
                            .lines()
                            .map(|text| ActivityDetail {
                                text: text.to_owned(),
                                depth: 0,
                                style: ActivityDetailStyle::Context,
                            })
                            .collect(),
                    });
                }
                AgentSessionEvent::EffectUncertaintyCommitted {
                    tool_name, message, ..
                } => {
                    entries.push(TranscriptEntry::error(
                        entry_id,
                        format!("{tool_name}: effect unknown — {message}"),
                    ));
                }
                // Compaction affects model context, never the user's original transcript.
                _ => {}
            }
        }
        for record in &run.records {
            match &record.event.payload {
                AgentEvent::DeliveryCommitted { delivery } if !final_recorded => {
                    entries.push(TranscriptEntry::assistant(
                        format!("history-delivery-{id}"),
                        contents_text(std::slice::from_ref(&delivery.final_response)),
                    ));
                    final_recorded = true;
                }
                AgentEvent::RunFailed { failure } => entries.push(TranscriptEntry::error(
                    format!("history-failed-{id}"),
                    format!("Run failed [{}]: {}", failure.code, failure.message),
                )),
                AgentEvent::RunCancelled { reason } => {
                    entries.push(TranscriptEntry::system(format!("Run cancelled: {reason}")))
                }
                AgentEvent::RunIncomplete {
                    reason,
                    partial_delivery,
                } => {
                    if let Some(response) = partial_delivery
                        .as_ref()
                        .and_then(|delivery| delivery.response.as_ref())
                    {
                        entries.push(TranscriptEntry::assistant(
                            format!("history-partial-{id}"),
                            contents_text(std::slice::from_ref(response)),
                        ));
                    }
                    entries.push(TranscriptEntry::system(format!(
                        "Run incomplete: {reason:?}"
                    )));
                }
                _ => {}
            }
        }
        if SessionHistoryStatus::of_run(run) == SessionHistoryStatus::Unfinished {
            let mut entry = TranscriptEntry::system(format!(
                "Unfinished Run {id}; execution must be reconciled before continuing."
            ));
            entry.id = Some(format!("history-unfinished-{id}"));
            entries.push(entry);
        }
    }
    entries
}

fn contents_text(contents: &[Content]) -> String {
    contents
        .iter()
        .map(|content| match &content.body {
            ContentBody::Inline(serde_json::Value::String(text)) => text.clone(),
            body => serde_json::to_string(body).unwrap_or_default(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn model_text(message: &ModelMessage) -> String {
    message
        .content
        .iter()
        .map(|content| match content {
            ModelContent::Text { text } => text.clone(),
            ModelContent::ToolCall {
                name, arguments, ..
            } => format!("{name}: {arguments}"),
            ModelContent::ToolResult { result, .. } => result.to_string(),
            content => serde_json::to_string(content).unwrap_or_default(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}
