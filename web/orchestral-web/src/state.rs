use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::model::{DeviceView, SessionView};

const MAX_TELEMETRY_IDS: usize = 800;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LoadStatus {
    #[default]
    Idle,
    Loading,
    Ready,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthStatus {
    Booting,
    Pairing,
    Authenticated,
    Unpaired,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthState {
    pub status: AuthStatus,
    pub me: Option<Value>,
    pub device: Option<DeviceView>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionsState {
    pub status: LoadStatus,
    pub items: Vec<SessionView>,
    pub selected_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DevicesState {
    pub status: LoadStatus,
    pub items: Vec<DeviceView>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: String,
    pub role: String,
    pub text: String,
    pub order: u64,
    pub optimistic: bool,
    pub partial: bool,
    pub steering: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamedOutput {
    pub output_id: String,
    pub text: String,
    pub order: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolActivity {
    pub id: String,
    pub tool_name: String,
    pub state: String,
    pub evidence: Vec<Value>,
    pub order: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommandActivity {
    pub id: String,
    pub kind: String,
    pub summary: String,
    pub request_id: Option<String>,
    pub state: String,
    pub outcome: Option<Value>,
    pub order: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Progress {
    pub message: String,
    pub fraction: Option<f64>,
    pub order: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunState {
    pub id: String,
    pub session_id: Option<String>,
    pub status: String,
    pub view: Option<Value>,
    pub cursor: u64,
    pub server_cursor: u64,
    pub event_ids: Vec<String>,
    pub sequence_ids: BTreeMap<u64, String>,
    pub messages: Vec<Message>,
    pub streamed_outputs: BTreeMap<String, StreamedOutput>,
    pub committed_output_ids: BTreeSet<String>,
    pub telemetry_ids: Vec<String>,
    pub presentation_cursor: u64,
    pub activities: Vec<ToolActivity>,
    pub commands: Vec<CommandActivity>,
    pub pending: Vec<Value>,
    pub progress: Option<Progress>,
    pub delivery: Option<Value>,
    pub partial_delivery: Option<Value>,
    pub failure: Option<Value>,
    pub started_at: Option<f64>,
    pub completed_at: Option<f64>,
    pub error: Option<String>,
}

impl RunState {
    pub fn new(id: impl Into<String>, session_id: Option<String>) -> Self {
        Self {
            id: id.into(),
            session_id,
            status: "loading".to_owned(),
            view: None,
            cursor: 0,
            server_cursor: 0,
            event_ids: Vec::new(),
            sequence_ids: BTreeMap::new(),
            messages: Vec::new(),
            streamed_outputs: BTreeMap::new(),
            committed_output_ids: BTreeSet::new(),
            telemetry_ids: Vec::new(),
            presentation_cursor: 0,
            activities: Vec::new(),
            commands: Vec::new(),
            pending: Vec::new(),
            progress: None,
            delivery: None,
            partial_delivery: None,
            failure: None,
            started_at: None,
            completed_at: None,
            error: None,
        }
    }

    fn next_order(&mut self) -> u64 {
        self.presentation_cursor = self.presentation_cursor.saturating_add(1);
        self.presentation_cursor
    }

    pub fn apply_view(&mut self, view: Value, now: f64) {
        let view_cursor = view
            .get("last_run_seq")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if view_cursor < self.cursor {
            return;
        }
        let status = status_from_view(&view);
        self.session_id = view
            .get("execution")
            .and_then(|value| value.get("session_id"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.session_id.clone());
        self.server_cursor = self.server_cursor.max(view_cursor);
        self.pending = view
            .get("pending_requests")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| self.pending.clone());
        self.delivery = view
            .get("delivery")
            .filter(|value| !value.is_null())
            .cloned()
            .or_else(|| self.delivery.clone());
        self.partial_delivery = view
            .get("partial_delivery")
            .filter(|value| !value.is_null())
            .cloned()
            .or_else(|| self.partial_delivery.clone());
        if !is_terminal(&status) && status != "accepted" {
            self.started_at.get_or_insert(now);
        }
        if is_terminal(&status) {
            self.completed_at.get_or_insert(now);
        }
        self.error = if status == "unknown" {
            view.get("state")
                .and_then(|value| value.get("reason"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| self.error.clone())
        } else {
            None
        };
        self.status = status;
        self.view = Some(view);
    }

    /// Records the initial user input after the Host accepted `start_run`.
    ///
    /// Unlike a locally queued message this is already confirmed by the HTTP
    /// response. Initial input is part of the immutable Run spec, so it does
    /// not produce the steer-only `input_committed` journal event.
    pub fn record_started_input(&mut self, input: String, now: f64) {
        self.status = "running".to_owned();
        self.started_at = Some(now);
        if self
            .messages
            .iter()
            .any(|message| message.role == "user" && message.text == input)
        {
            return;
        }
        let order = self.next_order();
        self.messages.push(Message {
            id: format!("optimistic-{}", self.id),
            role: "user".to_owned(),
            text: input,
            order,
            optimistic: false,
            partial: false,
            steering: false,
        });
    }

    pub fn optimistic_steer(&mut self, id: String, text: String) {
        let order = self.next_order();
        self.messages.push(Message {
            id,
            role: "user".to_owned(),
            text,
            order,
            optimistic: false,
            partial: false,
            steering: true,
        });
    }

    pub fn project_durable(&mut self, record: &Value, now: f64) {
        let event = record.get("event").unwrap_or(record);
        let Some(sequence) = event.get("run_seq").and_then(Value::as_u64) else {
            self.error = Some("Received a malformed durable event".to_owned());
            return;
        };
        let Some(event_id) = event.get("event_id").and_then(Value::as_str) else {
            self.error = Some("Received a malformed durable event".to_owned());
            return;
        };
        let Some(payload) = event.get("payload") else {
            self.error = Some("Received a malformed durable event".to_owned());
            return;
        };
        let Some(kind) = payload.get("type").and_then(Value::as_str) else {
            self.error = Some("Received a malformed durable event".to_owned());
            return;
        };

        if sequence <= self.cursor {
            if self
                .sequence_ids
                .get(&sequence)
                .is_some_and(|id| id != event_id)
            {
                self.error = Some(format!("Journal conflict at run_seq {sequence}"));
            }
            return;
        }
        if sequence != self.cursor.saturating_add(1) {
            self.error = Some(format!("Waiting for durable event {}", self.cursor + 1));
            return;
        }

        self.cursor = sequence;
        self.server_cursor = self.server_cursor.max(sequence);
        self.event_ids.push(event_id.to_owned());
        self.sequence_ids.insert(sequence, event_id.to_owned());
        self.error = None;

        match kind {
            "run_accepted" => self.status = "accepted".to_owned(),
            "run_started" => {
                self.status = "running".to_owned();
                self.started_at.get_or_insert(now);
            }
            "input_committed" => {
                let text = contents_text(payload.get("content"));
                let prior_order = self
                    .messages
                    .iter()
                    .find(|message| message.role == "user" && message.optimistic)
                    .map(|message| message.order);
                let order = prior_order.unwrap_or_else(|| self.next_order());
                if let Some(index) = self
                    .messages
                    .iter()
                    .position(|message| message.role == "user" && message.optimistic)
                {
                    self.messages[index] = Message {
                        id: event_id.to_owned(),
                        role: "user".to_owned(),
                        text,
                        order,
                        optimistic: false,
                        partial: false,
                        steering: false,
                    };
                } else if !text.is_empty() {
                    self.messages.push(Message {
                        id: event_id.to_owned(),
                        role: "user".to_owned(),
                        text,
                        order,
                        optimistic: false,
                        partial: false,
                        steering: false,
                    });
                }
            }
            "output_committed" => {
                let text = contents_text(payload.get("content"));
                let output_id = payload
                    .get("output_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !text.is_empty()
                    && !self.messages.iter().any(|message| {
                        message.role == "assistant" && message.text == text && !message.optimistic
                    })
                {
                    let order = self
                        .streamed_outputs
                        .get(output_id)
                        .map(|output| output.order)
                        .unwrap_or_else(|| self.next_order());
                    self.messages.push(Message {
                        id: event_id.to_owned(),
                        role: "assistant".to_owned(),
                        text,
                        order,
                        optimistic: false,
                        partial: false,
                        steering: false,
                    });
                }
                self.committed_output_ids.insert(output_id.to_owned());
                self.streamed_outputs.remove(output_id);
            }
            "request_opened" => {
                if let Some(request) = payload.get("request") {
                    let request_id = request.get("request_id").and_then(Value::as_str);
                    self.pending.retain(|item| {
                        item.get("request_id").and_then(Value::as_str) != request_id
                    });
                    self.pending.push(request.clone());
                    if request.get("blocking").and_then(Value::as_bool) == Some(true) {
                        self.status = "waiting".to_owned();
                    }
                }
            }
            "request_resolved" => {
                let request_id = payload.get("request_id").and_then(Value::as_str);
                self.pending
                    .retain(|item| item.get("request_id").and_then(Value::as_str) != request_id);
                if self.status == "waiting" && !has_blocking_request(&self.pending) {
                    self.status = "running".to_owned();
                }
            }
            "command_received" => self.project_command_received(payload, sequence),
            "command_disposition_recorded" => self.project_command_disposition(payload, sequence),
            "stop_requested" => self.status = "stopping".to_owned(),
            "delivery_committed" => {
                self.status = "delivered".to_owned();
                self.delivery = payload.get("delivery").cloned();
                self.completed_at.get_or_insert(now);
                self.pending.clear();
                self.append_terminal_message(
                    event_id,
                    self.delivery.clone(),
                    "final_response",
                    false,
                );
            }
            "run_incomplete" => {
                self.status = "incomplete".to_owned();
                self.partial_delivery = payload.get("partial_delivery").cloned();
                self.completed_at.get_or_insert(now);
                self.pending.clear();
                self.append_terminal_message(
                    event_id,
                    self.partial_delivery.clone(),
                    "response",
                    true,
                );
            }
            "run_failed" => {
                self.status = "failed".to_owned();
                self.failure = payload.get("failure").cloned();
                self.completed_at.get_or_insert(now);
                self.pending.clear();
            }
            "run_cancelled" => {
                self.status = "cancelled".to_owned();
                self.failure = Some(serde_json::json!({
                    "code": "cancelled",
                    "message": payload.get("reason").cloned().unwrap_or(Value::Null),
                }));
                self.completed_at.get_or_insert(now);
                self.pending.clear();
            }
            "continuity_lost" => {
                self.status = "unknown".to_owned();
                self.error = payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            "continuity_restored" => {
                self.status = if has_blocking_request(&self.pending) {
                    "waiting"
                } else {
                    "running"
                }
                .to_owned();
                self.error = None;
            }
            _ => {}
        }
    }

    fn project_command_received(&mut self, payload: &Value, _sequence: u64) {
        let Some(command) = payload.get("command") else {
            return;
        };
        let Some(id) = command.get("command_id").and_then(Value::as_str) else {
            return;
        };
        let previous_order = self
            .commands
            .iter()
            .find(|item| item.id == id)
            .map(|item| item.order);
        let order = previous_order.unwrap_or_else(|| self.next_order());
        self.commands.retain(|item| item.id != id);
        let command_payload = command.get("payload").unwrap_or(&Value::Null);
        self.commands.push(CommandActivity {
            id: id.to_owned(),
            kind: command_payload
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("command")
                .to_owned(),
            summary: command_summary(command_payload),
            request_id: command
                .get("request_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            state: "received".to_owned(),
            outcome: None,
            order,
        });
    }

    fn project_command_disposition(&mut self, payload: &Value, _sequence: u64) {
        let Some(id) = payload.get("command_id").and_then(Value::as_str) else {
            return;
        };
        let index = self.commands.iter().position(|item| item.id == id);
        let order = index
            .map(|index| self.commands[index].order)
            .unwrap_or_else(|| self.next_order());
        let outcome = payload.get("outcome").cloned();
        let state = outcome
            .as_ref()
            .and_then(|value| value.get("outcome"))
            .and_then(Value::as_str)
            .unwrap_or("recorded")
            .to_owned();
        let item = CommandActivity {
            id: id.to_owned(),
            kind: index
                .map(|index| self.commands[index].kind.clone())
                .unwrap_or_else(|| "command".to_owned()),
            summary: index
                .map(|index| self.commands[index].summary.clone())
                .unwrap_or_default(),
            request_id: index.and_then(|index| self.commands[index].request_id.clone()),
            state,
            outcome,
            order,
        };
        self.commands.retain(|item| item.id != id);
        self.commands.push(item);
    }

    fn append_terminal_message(
        &mut self,
        event_id: &str,
        envelope: Option<Value>,
        field: &str,
        partial: bool,
    ) {
        let text = envelope
            .as_ref()
            .and_then(|value| value.get(field))
            .map(content_text)
            .unwrap_or_default();
        if text.is_empty()
            || self.messages.iter().any(|message| {
                message.role == "assistant" && message.text == text && !message.optimistic
            })
        {
            return;
        }
        let order = self.next_order();
        self.messages.push(Message {
            id: format!("{event_id}-delivery"),
            role: "assistant".to_owned(),
            text,
            order,
            optimistic: false,
            partial,
            steering: false,
        });
    }

    pub fn project_telemetry(&mut self, telemetry: &Value) {
        let Some(telemetry_id) = telemetry.get("telemetry_id").and_then(Value::as_str) else {
            return;
        };
        if self.telemetry_ids.iter().any(|id| id == telemetry_id) {
            return;
        }
        let Some(payload) = telemetry.get("payload") else {
            return;
        };
        let Some(kind) = payload.get("type").and_then(Value::as_str) else {
            return;
        };
        self.telemetry_ids.push(telemetry_id.to_owned());
        if self.telemetry_ids.len() > MAX_TELEMETRY_IDS {
            self.telemetry_ids.remove(0);
        }

        match kind {
            "output_delta" => {
                let output_id = payload
                    .get("output_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if self.committed_output_ids.contains(output_id) {
                    return;
                }
                let text = payload.get("delta").map(content_text).unwrap_or_default();
                if text.is_empty() {
                    return;
                }
                let order = self
                    .streamed_outputs
                    .get(output_id)
                    .map(|output| output.order)
                    .unwrap_or_else(|| self.next_order());
                self.streamed_outputs
                    .entry(output_id.to_owned())
                    .and_modify(|output| output.text.push_str(&text))
                    .or_insert_with(|| StreamedOutput {
                        output_id: output_id.to_owned(),
                        text,
                        order,
                    });
            }
            "progress_reported" => {
                let order = self
                    .progress
                    .as_ref()
                    .map(|progress| progress.order)
                    .unwrap_or_else(|| self.next_order());
                self.progress = Some(Progress {
                    message: payload
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    fraction: payload.get("fraction").and_then(Value::as_f64),
                    order,
                });
            }
            "tool_activity" => {
                let Some(id) = payload.get("activity_id").and_then(Value::as_str) else {
                    return;
                };
                let existing = self.activities.iter().position(|item| item.id == id);
                let order = existing
                    .map(|index| self.activities[index].order)
                    .unwrap_or_else(|| self.next_order());
                let activity = ToolActivity {
                    id: id.to_owned(),
                    tool_name: payload
                        .get("tool_name")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_owned(),
                    state: payload
                        .get("state")
                        .and_then(Value::as_str)
                        .unwrap_or("running")
                        .to_owned(),
                    evidence: payload
                        .get("evidence")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default(),
                    order,
                };
                if let Some(index) = existing {
                    self.activities[index] = activity;
                } else {
                    self.activities.push(activity);
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionState {
    pub online: bool,
    pub stream: String,
    pub attempt: u32,
    pub error: Option<String>,
    pub last_connected_at: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct UiState {
    pub drawer_open: bool,
    pub settings_open: bool,
    pub composer_busy: bool,
    pub installing: bool,
    pub install_available: bool,
    pub notice: Option<Notice>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub message: String,
    pub tone: String,
    pub id: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AppState {
    pub auth: AuthState,
    pub sessions: SessionsState,
    pub devices: DevicesState,
    pub runs: BTreeMap<String, RunState>,
    pub run_order: Vec<String>,
    pub connection: ConnectionState,
    pub ui: UiState,
}

impl AppState {
    pub fn new(online: bool) -> Self {
        Self {
            auth: AuthState {
                status: AuthStatus::Booting,
                me: None,
                device: None,
                error: None,
            },
            sessions: SessionsState::default(),
            devices: DevicesState::default(),
            runs: BTreeMap::new(),
            run_order: Vec::new(),
            connection: ConnectionState {
                online,
                stream: if online { "idle" } else { "offline" }.to_owned(),
                attempt: 0,
                error: None,
                last_connected_at: None,
            },
            ui: UiState::default(),
        }
    }

    pub fn ensure_run(&mut self, run_id: &str, session_id: Option<String>) -> &mut RunState {
        if !self.runs.contains_key(run_id) {
            self.run_order.push(run_id.to_owned());
            self.runs
                .insert(run_id.to_owned(), RunState::new(run_id, session_id.clone()));
        }
        let run = self.runs.get_mut(run_id).expect("run was inserted");
        if session_id.is_some() {
            run.session_id = session_id;
        }
        run
    }

    pub fn selected_session(&self) -> Option<&SessionView> {
        let selected = self.sessions.selected_id.as_deref()?;
        self.sessions
            .items
            .iter()
            .find(|session| session.id == selected)
    }

    pub fn current_run(&self) -> Option<&RunState> {
        let session = self.selected_session()?;
        session
            .run_ids
            .iter()
            .rev()
            .filter_map(|run_id| self.runs.get(run_id))
            .find(|run| !is_terminal(&run.status))
            .or_else(|| {
                session
                    .run_ids
                    .iter()
                    .rev()
                    .find_map(|run_id| self.runs.get(run_id))
            })
    }

    pub fn active_run(&self) -> Option<&RunState> {
        self.current_run().filter(|run| {
            matches!(
                run.status.as_str(),
                "accepted" | "running" | "waiting" | "stopping" | "unknown"
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TimelineItem {
    Message(Message),
    Stream(StreamedOutput),
    Activity(ToolActivity),
    Command(CommandActivity),
    Progress(Progress),
}

impl TimelineItem {
    pub fn order(&self) -> u64 {
        match self {
            Self::Message(value) => value.order,
            Self::Stream(value) => value.order,
            Self::Activity(value) => value.order,
            Self::Command(value) => value.order,
            Self::Progress(value) => value.order,
        }
    }
}

pub fn timeline_for_run(run: &RunState) -> Vec<TimelineItem> {
    let mut items = Vec::new();
    items.extend(run.messages.iter().cloned().map(TimelineItem::Message));
    items.extend(
        run.streamed_outputs
            .values()
            .cloned()
            .map(TimelineItem::Stream),
    );
    items.extend(run.activities.iter().cloned().map(TimelineItem::Activity));
    items.extend(run.commands.iter().cloned().map(TimelineItem::Command));
    items.extend(run.progress.iter().cloned().map(TimelineItem::Progress));
    items.sort_by_key(TimelineItem::order);
    items
}

pub fn content_text(content: &Value) -> String {
    let body = content.get("body");
    match body
        .and_then(|value| value.get("kind"))
        .and_then(Value::as_str)
    {
        Some("inline") => match body.and_then(|value| value.get("value")) {
            Some(Value::String(value)) => value.clone(),
            Some(value) => {
                serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
            }
            None => String::new(),
        },
        Some("artifact") => body
            .and_then(|value| value.get("value"))
            .and_then(|value| value.get("artifact_ref"))
            .and_then(Value::as_str)
            .map(|reference| format!("[Artifact: {reference}]"))
            .unwrap_or_else(|| "[Artifact]".to_owned()),
        _ => String::new(),
    }
}

pub fn contents_text(contents: Option<&Value>) -> String {
    contents
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(content_text)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn status_from_view(view: &Value) -> String {
    let Some(state) = view.get("state") else {
        return "unknown".to_owned();
    };
    let Some(kind) = state.get("state").and_then(Value::as_str) else {
        return "unknown".to_owned();
    };
    if kind != "terminal" {
        return kind.to_owned();
    }
    state
        .get("terminal")
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned()
}

pub fn is_terminal(status: &str) -> bool {
    matches!(status, "delivered" | "incomplete" | "cancelled" | "failed")
}

fn has_blocking_request(pending: &[Value]) -> bool {
    pending
        .iter()
        .any(|request| request.get("blocking").and_then(Value::as_bool) == Some(true))
}

fn command_summary(payload: &Value) -> String {
    match payload.get("type").and_then(Value::as_str) {
        Some("steer") => contents_text(payload.get("content")),
        Some("cancel") => payload
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("Cancel")
            .to_owned(),
        Some("resolve_request") => format!(
            "Resolve {}",
            payload
                .get("request_id")
                .and_then(Value::as_str)
                .unwrap_or("request")
        ),
        Some(kind) => kind.to_owned(),
        None => "Command".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(value: &str) -> Value {
        serde_json::json!([{"body": {"kind": "inline", "value": value}}])
    }

    fn record(sequence: u64, id: &str, payload: Value) -> Value {
        serde_json::json!({
            "event": {
                "run_seq": sequence,
                "event_id": id,
                "payload": payload,
            }
        })
    }

    #[test]
    fn timeline_keeps_user_tool_and_assistant_arrival_order() {
        let mut run = RunState::new("run-1", Some("session-1".to_owned()));
        run.project_durable(
            &record(
                1,
                "input",
                serde_json::json!({"type": "input_committed", "content": content("inspect") }),
            ),
            1.0,
        );
        run.project_telemetry(&serde_json::json!({
            "telemetry_id": "tool-telemetry",
            "payload": {
                "type": "tool_activity",
                "activity_id": "tool-1",
                "tool_name": "file_read",
                "state": "succeeded",
                "evidence": [],
            }
        }));
        run.project_durable(
            &record(2, "output", serde_json::json!({"type": "output_committed", "output_id": "out-1", "content": content("done") })),
            2.0,
        );

        assert!(
            matches!(timeline_for_run(&run)[0], TimelineItem::Message(ref value) if value.role == "user")
        );
        assert!(matches!(
            timeline_for_run(&run)[1],
            TimelineItem::Activity(_)
        ));
        assert!(
            matches!(timeline_for_run(&run)[2], TimelineItem::Message(ref value) if value.role == "assistant")
        );
    }

    #[test]
    fn committed_stream_keeps_its_original_position() {
        let mut run = RunState::new("run-1", None);
        run.project_telemetry(&serde_json::json!({
            "telemetry_id": "delta-1",
            "payload": {
                "type": "output_delta",
                "output_id": "out-1",
                "delta": {"body": {"kind": "inline", "value": "hel"}},
            }
        }));
        run.project_telemetry(&serde_json::json!({
            "telemetry_id": "tool-1",
            "payload": {
                "type": "tool_activity",
                "activity_id": "activity-1",
                "tool_name": "file_read",
                "state": "succeeded",
                "evidence": [],
            }
        }));
        run.project_durable(
            &record(1, "output", serde_json::json!({"type": "output_committed", "output_id": "out-1", "content": content("hello") })),
            1.0,
        );

        assert!(
            matches!(timeline_for_run(&run)[0], TimelineItem::Message(ref value) if value.text == "hello")
        );
        assert!(matches!(
            timeline_for_run(&run)[1],
            TimelineItem::Activity(_)
        ));
    }

    #[test]
    fn accepted_start_input_is_not_left_in_sending_state() {
        let mut run = RunState::new("run-1", None);
        run.record_started_input("inspect the project".to_owned(), 1.0);

        assert_eq!(run.messages.len(), 1);
        assert!(!run.messages[0].optimistic);
        assert_eq!(run.messages[0].text, "inspect the project");
    }
}
