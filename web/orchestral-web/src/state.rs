use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::model::{
    AgentConnectorView, AgentSessionChangeKindView, AgentSessionChangeView, AgentSessionDetail,
    DeviceView, SessionView,
};

const MAX_TELEMETRY_IDS: usize = 800;
const INITIAL_INPUT_ORDER: u64 = 0;
const SESSION_HISTORY_ANCHOR_EXTENSION: &str = "orchestral.dev/session-history-anchor";

/// Browser-independent state machine for collapsing invalidation bursts into
/// one authoritative read plus, when events arrive during that read, one
/// trailing reconciliation pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentSessionReconcileCoordinator {
    requested: u64,
    worker_generation: Option<u64>,
}

impl AgentSessionReconcileCoordinator {
    pub fn request(&mut self, generation: u64) -> bool {
        self.requested = self.requested.saturating_add(1);
        if self.worker_generation == Some(generation) {
            return false;
        }
        self.worker_generation = Some(generation);
        true
    }

    pub fn checkpoint(&self) -> u64 {
        self.requested
    }

    pub fn has_trailing_request(&self, generation: u64, checkpoint: u64) -> bool {
        self.worker_generation == Some(generation) && self.requested != checkpoint
    }

    pub fn finish(&mut self, generation: u64) {
        if self.worker_generation == Some(generation) {
            self.worker_generation = None;
        }
    }
}

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
    pub connector_pages: BTreeMap<String, AgentSessionListState>,
    /// Last canonically applied Host event sequence for each external Agent
    /// session, keyed by `SessionView::key()`.
    pub stream_cursors: BTreeMap<String, u64>,
    /// Bounded live suffix used to rebase snapshots read concurrently with SSE.
    pub recent_changes: BTreeMap<String, Vec<AgentSessionChangeView>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentSessionListState {
    pub next_cursor: Option<String>,
    pub loading_more: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DevicesState {
    pub status: LoadStatus,
    pub items: Vec<DeviceView>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnectorsState {
    pub status: LoadStatus,
    pub items: Vec<AgentConnectorView>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub id: String,
    /// Optional native client correlation id echoed by the Agent runtime.
    pub client_id: Option<String>,
    pub role: String,
    pub text: String,
    pub order: u64,
    /// Presentation-only wall-clock anchor. It is not part of Agent Protocol
    /// ordering and is used only to merge a Host mirror into native history.
    pub occurred_at_unix_ms: Option<i64>,
    /// Stable native activity that was at the live edge when this Host input
    /// was submitted. It is a structural merge anchor, not a timestamp.
    pub native_anchor_id: Option<String>,
    pub optimistic: bool,
    /// The owning native Agent has not consumed this externally queued input.
    pub deferred: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRecoveryState {
    pub mode: String,
    pub can_start_new_run: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSupervisionState {
    pub state: String,
    pub reason: String,
    pub detected_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RunState {
    pub id: String,
    pub session_id: Option<String>,
    pub connector_id: Option<String>,
    pub status: String,
    pub view: Option<Value>,
    pub cursor: u64,
    pub server_cursor: u64,
    pub event_ids: Vec<String>,
    pub sequence_ids: BTreeMap<u64, String>,
    pub messages: Vec<Message>,
    pub streamed_outputs: BTreeMap<String, StreamedOutput>,
    pub committed_output_ids: BTreeSet<String>,
    /// Durable output event ids already represented by a terminal snapshot.
    pub terminal_supporting_event_ids: BTreeSet<String>,
    pub telemetry_ids: Vec<String>,
    pub presentation_cursor: u64,
    pub activities: Vec<ToolActivity>,
    pub commands: Vec<CommandActivity>,
    pub pending: Vec<Value>,
    /// Opaque cursor for the next, older page of an external Agent transcript.
    pub history_next_cursor: Option<String>,
    pub history_loading_earlier: bool,
    /// Once pagination starts, live-edge snapshots must not rewind the older
    /// history cursor or unlock a request that is already in flight.
    pub history_pagination_started: bool,
    /// First visible native activity id for each turn in the current live
    /// window. This preserves a structural insertion anchor when the bounded
    /// page contains a response but not its older correlated user item.
    pub history_live_turn_starts: Vec<String>,
    /// First visible activity keyed by native turn id. Incremental activity
    /// upserts extend this map without rebuilding the bounded transcript.
    pub history_live_turn_ids: BTreeMap<String, String>,
    /// Status of the newest provider-owned turn in the authoritative session
    /// projection. This is presentation state only: the synthetic history Run
    /// must never become a command target for steer, cancel, or recovery.
    pub history_latest_turn_status: Option<String>,
    pub progress: Option<Progress>,
    pub delivery: Option<Value>,
    pub partial_delivery: Option<Value>,
    pub failure: Option<Value>,
    pub started_at: Option<f64>,
    /// Authoritative Host catalog update time when this Run came from an
    /// Agent-session control-plane projection.
    pub updated_at_unix_ms: Option<i64>,
    pub completed_at: Option<f64>,
    pub error: Option<String>,
    /// Host control-plane disposition for an `unknown` protocol state.
    /// `unknown` alone cannot distinguish progress from an unrecoverable
    /// execution boundary and previously locked the composer forever.
    pub recovery: Option<RunRecoveryState>,
    /// Host-side execution liveness, independent from transport continuity.
    /// A Provider stream may remain connected while native work is stalled.
    pub supervision: Option<RunSupervisionState>,
}

impl RunState {
    pub fn new(id: impl Into<String>, session_id: Option<String>) -> Self {
        Self {
            id: id.into(),
            session_id,
            connector_id: None,
            status: "loading".to_owned(),
            view: None,
            cursor: 0,
            server_cursor: 0,
            event_ids: Vec::new(),
            sequence_ids: BTreeMap::new(),
            messages: Vec::new(),
            streamed_outputs: BTreeMap::new(),
            committed_output_ids: BTreeSet::new(),
            terminal_supporting_event_ids: BTreeSet::new(),
            telemetry_ids: Vec::new(),
            presentation_cursor: 0,
            activities: Vec::new(),
            commands: Vec::new(),
            pending: Vec::new(),
            history_next_cursor: None,
            history_loading_earlier: false,
            history_pagination_started: false,
            history_live_turn_starts: Vec::new(),
            history_live_turn_ids: BTreeMap::new(),
            history_latest_turn_status: None,
            progress: None,
            delivery: None,
            partial_delivery: None,
            failure: None,
            started_at: None,
            updated_at_unix_ms: None,
            completed_at: None,
            error: None,
            recovery: None,
            supervision: None,
        }
    }

    fn next_order(&mut self) -> u64 {
        self.presentation_cursor = self.presentation_cursor.saturating_add(1);
        self.presentation_cursor
    }

    pub fn recovery_is_manual(&self) -> bool {
        self.status == "unknown"
            && self
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.mode == "manual")
    }

    pub fn recovery_allows_new_run(&self) -> bool {
        self.recovery_is_manual()
            && self
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.can_start_new_run)
    }

    pub fn apply_view(&mut self, view: Value, now: f64) {
        if let Some(created_at) = view
            .get("created_at_unix_ms")
            .and_then(Value::as_i64)
            .filter(|created_at| *created_at > 0)
        {
            self.started_at = Some(created_at as f64);
        }
        let history_anchor_id = view
            .get("after_activity_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let initial_input = contents_text(view.get("input"));
        if !initial_input.is_empty() {
            self.confirm_initial_input(
                initial_input,
                self.started_at.map(|value| value as i64),
                history_anchor_id,
            );
        }
        let view_cursor = view
            .get("last_run_seq")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if view_cursor < self.cursor.max(self.server_cursor) {
            return;
        }
        if let Some(updated_at) = view
            .get("updated_at_unix_ms")
            .and_then(Value::as_i64)
            .filter(|updated_at| *updated_at > 0)
        {
            self.updated_at_unix_ms = Some(updated_at);
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
        match status.as_str() {
            "delivered" => self.append_terminal_message(
                self.delivery.clone(),
                "delivery_id",
                "delivery",
                "final_response",
                false,
            ),
            "incomplete" => self.append_terminal_message(
                self.partial_delivery.clone(),
                "partial_delivery_id",
                "partial-delivery",
                "response",
                true,
            ),
            _ => {}
        }
        if !is_terminal(&status) && status != "accepted" {
            self.started_at.get_or_insert(now);
        }
        if is_terminal(&status) {
            self.completed_at.get_or_insert(now);
            self.settle_unresolved_commands_for_terminal();
        }
        self.recovery = if status == "unknown" {
            view.get("recovery").and_then(|recovery| {
                Some(RunRecoveryState {
                    mode: recovery.get("mode")?.as_str()?.to_owned(),
                    can_start_new_run: recovery
                        .get("can_start_new_run")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    reason: recovery
                        .get("reason")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                })
            })
        } else {
            None
        };
        self.supervision = view.get("supervision").and_then(|supervision| {
            Some(RunSupervisionState {
                state: supervision.get("state")?.as_str()?.to_owned(),
                reason: supervision.get("reason")?.as_str()?.to_owned(),
                detected_at_unix_ms: supervision
                    .get("detected_at_unix_ms")
                    .and_then(Value::as_i64)?,
            })
        });
        self.error = if status == "unknown" {
            self.recovery
                .as_ref()
                .and_then(|recovery| recovery.reason.clone())
                .or_else(|| {
                    view.get("state")
                        .and_then(|value| value.get("reason"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .or_else(|| self.error.clone())
        } else {
            None
        };
        self.status = status;
        self.view = Some(view);
    }

    /// Records the initial user input after the Host accepted `start_run`.
    ///
    /// HTTP acceptance proves only that the immutable Run was registered. A
    /// connector may still be waiting in its native queue, so `RunStarted`
    /// remains the sole authority that advances this state to `running`.
    pub fn record_accepted_input(&mut self, input: String, now: f64) {
        if matches!(self.status.as_str(), "loading" | "submitting") {
            self.status = "accepted".to_owned();
        }
        self.started_at.get_or_insert(now);
        self.confirm_initial_input(input, Some(now as i64), None);
    }

    /// Projects a start request before the network round trip completes.
    ///
    /// The browser owns this short-lived state. It keeps the user's submitted
    /// text visible while the Host accepts the immutable Run specification,
    /// without pretending that the Run is already steerable or cancellable.
    pub fn optimistic_start_input(
        &mut self,
        input: String,
        now: f64,
        native_anchor_id: Option<String>,
    ) {
        self.status = "submitting".to_owned();
        self.started_at = Some(now);
        self.messages
            .retain(|message| !(message.role == "user" && !message.steering && message.optimistic));
        self.messages.push(Message {
            id: format!("optimistic-input-{}", self.id),
            client_id: None,
            role: "user".to_owned(),
            text: input,
            order: INITIAL_INPUT_ORDER,
            occurred_at_unix_ms: Some(now as i64),
            native_anchor_id,
            optimistic: true,
            deferred: false,
            partial: false,
            steering: false,
        });
    }

    pub fn reject_optimistic_start(&mut self, message: String, now: f64) {
        self.status = "failed".to_owned();
        self.completed_at = Some(now);
        self.error = Some(message);
        for item in &mut self.messages {
            item.optimistic = false;
        }
    }

    fn confirm_initial_input(
        &mut self,
        input: String,
        occurred_at_unix_ms: Option<i64>,
        history_anchor_id: Option<String>,
    ) {
        if let Some(message) = self
            .messages
            .iter_mut()
            .find(|message| message.role == "user" && !message.steering)
        {
            if message.optimistic || message.text == input {
                message.text = input;
                message.order = INITIAL_INPUT_ORDER;
                message.occurred_at_unix_ms = message.occurred_at_unix_ms.or(occurred_at_unix_ms);
                if message.native_anchor_id.is_none() {
                    message.native_anchor_id = history_anchor_id;
                }
                message.optimistic = false;
                return;
            }
        }
        self.messages.push(Message {
            id: format!("initial-input-{}", self.id),
            client_id: None,
            role: "user".to_owned(),
            text: input,
            order: INITIAL_INPUT_ORDER,
            occurred_at_unix_ms,
            native_anchor_id: history_anchor_id,
            optimistic: false,
            deferred: false,
            partial: false,
            steering: false,
        });
    }

    pub fn optimistic_steer(
        &mut self,
        id: String,
        text: String,
        now: f64,
        native_anchor_id: Option<String>,
    ) {
        // The durable event can arrive before the HTTP acknowledgement.
        if self.messages.iter().any(|message| message.id == id) {
            return;
        }
        let order = self.next_order();
        self.messages.push(Message {
            id,
            client_id: None,
            role: "user".to_owned(),
            text,
            order,
            occurred_at_unix_ms: Some(now as i64),
            native_anchor_id,
            optimistic: false,
            deferred: false,
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

        // Replay still builds the transcript, but must not roll back control
        // state already authenticated by a newer snapshot.
        let ahead_view = self
            .view
            .as_ref()
            .filter(|view| {
                view.get("last_run_seq")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                    >= sequence
            })
            .cloned();
        self.cursor = sequence;
        self.server_cursor = self.server_cursor.max(sequence);
        self.updated_at_unix_ms = Some(self.updated_at_unix_ms.unwrap_or_default().max(now as i64));
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
                // `start_run` acceptance confirms the optimistic input before
                // this durable event is fetched. Match the stable initial
                // projection as well as the still-optimistic form so the
                // authoritative event replaces it instead of creating a
                // second, unanchored user message.
                let prior_index = self.messages.iter().position(|message| {
                    message.role == "user"
                        && !message.steering
                        && (message.optimistic
                            || message.id == format!("optimistic-input-{}", self.id)
                            || message.id == format!("initial-input-{}", self.id)
                            || message.text == text)
                });
                let prior_order = prior_index.map(|index| self.messages[index].order);
                let prior_native_anchor =
                    prior_index.and_then(|index| self.messages[index].native_anchor_id.clone());
                let prior_occurred_at =
                    prior_index.and_then(|index| self.messages[index].occurred_at_unix_ms);
                let order = prior_order.unwrap_or_else(|| self.next_order());
                if let Some(index) = prior_index {
                    self.messages[index] = Message {
                        id: event_id.to_owned(),
                        client_id: None,
                        role: "user".to_owned(),
                        text,
                        order,
                        occurred_at_unix_ms: prior_occurred_at.or(Some(now as i64)),
                        native_anchor_id: prior_native_anchor,
                        optimistic: false,
                        deferred: false,
                        partial: false,
                        steering: false,
                    };
                } else if !text.is_empty() {
                    self.messages.push(Message {
                        id: event_id.to_owned(),
                        client_id: None,
                        role: "user".to_owned(),
                        text,
                        order,
                        occurred_at_unix_ms: Some(now as i64),
                        native_anchor_id: None,
                        optimistic: false,
                        deferred: false,
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
                    && !self.messages.iter().any(|message| message.id == event_id)
                    && !self.terminal_supporting_event_ids.contains(event_id)
                {
                    let order = self
                        .streamed_outputs
                        .get(output_id)
                        .map(|output| output.order)
                        .unwrap_or_else(|| self.next_order());
                    self.messages.push(Message {
                        id: event_id.to_owned(),
                        client_id: None,
                        role: "assistant".to_owned(),
                        text,
                        order,
                        occurred_at_unix_ms: Some(now as i64),
                        native_anchor_id: None,
                        optimistic: false,
                        deferred: false,
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
            "request_resolved" | "request_closed" => {
                let request_id = payload.get("request_id").and_then(Value::as_str);
                self.pending
                    .retain(|item| item.get("request_id").and_then(Value::as_str) != request_id);
                if self.status == "waiting" && !has_blocking_request(&self.pending) {
                    self.status = "running".to_owned();
                }
            }
            "command_received" => self.project_command_received(payload, sequence, now),
            "command_disposition_recorded" => self.project_command_disposition(payload, sequence),
            "stop_requested" => self.status = "stopping".to_owned(),
            "delivery_committed" => {
                self.status = "delivered".to_owned();
                self.delivery = payload.get("delivery").cloned();
                self.completed_at.get_or_insert(now);
                self.pending.clear();
                self.settle_unresolved_commands_for_terminal();
                self.append_terminal_message(
                    self.delivery.clone(),
                    "delivery_id",
                    "delivery",
                    "final_response",
                    false,
                );
            }
            "run_incomplete" => {
                self.status = "incomplete".to_owned();
                self.partial_delivery = payload.get("partial_delivery").cloned();
                self.completed_at.get_or_insert(now);
                self.pending.clear();
                self.settle_unresolved_commands_for_terminal();
                self.append_terminal_message(
                    self.partial_delivery.clone(),
                    "partial_delivery_id",
                    "partial-delivery",
                    "response",
                    true,
                );
            }
            "run_failed" => {
                self.status = "failed".to_owned();
                self.failure = payload.get("failure").cloned();
                self.completed_at.get_or_insert(now);
                self.pending.clear();
                self.settle_unresolved_commands_for_terminal();
            }
            "run_cancelled" => {
                self.status = "cancelled".to_owned();
                self.failure = Some(serde_json::json!({
                    "code": "cancelled",
                    "message": payload.get("reason").cloned().unwrap_or(Value::Null),
                }));
                self.completed_at.get_or_insert(now);
                self.pending.clear();
                self.settle_unresolved_commands_for_terminal();
            }
            "continuity_lost" => {
                self.status = "unknown".to_owned();
                self.recovery = Some(RunRecoveryState {
                    mode: "automatic".to_owned(),
                    can_start_new_run: false,
                    reason: None,
                });
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
                self.recovery = None;
            }
            _ => {}
        }
        if let Some(view) = ahead_view {
            self.apply_view(view, now);
        }
    }

    fn project_command_received(&mut self, payload: &Value, _sequence: u64, _now: f64) {
        let Some(command) = payload.get("command") else {
            return;
        };
        let Some(id) = command.get("command_id").and_then(Value::as_str) else {
            return;
        };
        let command_payload = command.get("payload").unwrap_or(&Value::Null);
        if command_payload.get("type").and_then(Value::as_str) == Some("steer") {
            let text = contents_text(command_payload.get("content"));
            let message_id = format!("steer-{id}");
            let history_anchor_id = command
                .get("extensions")
                .and_then(|extensions| extensions.get(SESSION_HISTORY_ANCHOR_EXTENSION))
                .and_then(|anchor| anchor.get("after_activity_id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(message) = self
                .messages
                .iter_mut()
                .find(|message| message.id == message_id)
            {
                if message.native_anchor_id.is_none() {
                    message.native_anchor_id = history_anchor_id;
                }
            } else if !text.is_empty() {
                let message_order = self.next_order();
                self.messages.push(Message {
                    id: message_id,
                    client_id: None,
                    role: "user".to_owned(),
                    text,
                    order: message_order,
                    // Durable command order comes from `run_seq`. Using each
                    // browser's receipt clock here made the same steer land at
                    // different native-history positions on different devices.
                    occurred_at_unix_ms: None,
                    native_anchor_id: history_anchor_id,
                    optimistic: false,
                    deferred: false,
                    partial: false,
                    steering: true,
                });
            }
        }
        let previous_order = self
            .commands
            .iter()
            .find(|item| item.id == id)
            .map(|item| item.order);
        let order = previous_order.unwrap_or_else(|| self.next_order());
        self.commands.retain(|item| item.id != id);
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

    fn settle_unresolved_commands_for_terminal(&mut self) {
        for command in &mut self.commands {
            if command.state == "received" {
                command.state = "rejected".to_owned();
                command.outcome = Some(serde_json::json!({
                    "outcome": "rejected",
                    "code": "run_ended_before_command_disposition",
                    "message": "The Run ended before the Agent accepted this command; it was not applied."
                }));
            }
        }
    }

    fn append_terminal_message(
        &mut self,
        envelope: Option<Value>,
        identity_field: &str,
        identity_prefix: &str,
        field: &str,
        partial: bool,
    ) {
        let identity = envelope
            .as_ref()
            .and_then(|value| value.get(identity_field))
            .and_then(value_id)
            .unwrap_or_else(|| self.id.clone());
        let message_id = format!("{identity_prefix}:{identity}");
        let supporting_event_ids = envelope
            .as_ref()
            .and_then(|value| value.get("provenance"))
            .and_then(|value| value.get("supporting_event_ids"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(value_id)
            .collect::<Vec<_>>();
        let text = envelope
            .as_ref()
            .and_then(|value| value.get(field))
            .map(content_text)
            .unwrap_or_default();
        let represented_by_output = supporting_event_ids.iter().any(|event_id| {
            self.messages
                .iter()
                .any(|message| message.id == *event_id && message.text == text)
        });
        if text.is_empty()
            || represented_by_output
            || self.messages.iter().any(|message| message.id == message_id)
        {
            return;
        }
        self.terminal_supporting_event_ids
            .extend(supporting_event_ids);
        let order = self.next_order();
        self.messages.push(Message {
            id: message_id,
            client_id: None,
            role: "assistant".to_owned(),
            text,
            order,
            occurred_at_unix_ms: None,
            native_anchor_id: None,
            optimistic: false,
            deferred: false,
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
    /// Active session source tab (`orchestral` or an Agent connector id).
    pub session_tab: Option<String>,
    /// Zero-based page within the active session source tab.
    pub session_page: usize,
    pub settings_open: bool,
    pub new_session_open: bool,
    pub session_actions_open: bool,
    pub composer_busy: bool,
    pub outbox_flushing: bool,
    pub update_available: bool,
    /// The reader deliberately left the live edge. New events remain locally
    /// merged but do not steal their scroll position; the UI exposes an
    /// explicit jump-to-latest control instead.
    pub timeline_scrolled_away: bool,
    /// Session whose transcript is currently being loaded. This is keyed so
    /// an older request cannot clear the loading state of a newer selection.
    pub loading_session: Option<String>,
    /// Pending request actions in flight, keyed as `run_id:request_id`.
    pub resolving_requests: BTreeSet<String>,
    pub request_errors: BTreeMap<String, String>,
    pub installing: bool,
    pub install_available: bool,
    pub notice: Option<Notice>,
}

impl UiState {
    pub fn show_notice(&mut self, notice: Notice) {
        self.notice = Some(notice);
    }

    pub fn dismiss_notice(&mut self, id: u64) -> bool {
        if self.notice.as_ref().is_some_and(|notice| notice.id == id) {
            self.notice = None;
            true
        } else {
            false
        }
    }

    pub fn request_action_key(run_id: &str, request_id: &str) -> String {
        format!("{run_id}:{request_id}")
    }

    pub fn request_is_resolving(&self, run_id: &str, request_id: &str) -> bool {
        self.resolving_requests
            .contains(&Self::request_action_key(run_id, request_id))
    }

    pub fn set_request_resolving(&mut self, run_id: &str, request_id: &str, resolving: bool) {
        let key = Self::request_action_key(run_id, request_id);
        if resolving {
            self.request_errors.remove(&key);
            self.resolving_requests.insert(key);
        } else {
            self.resolving_requests.remove(&key);
        }
    }
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
    pub connectors: ConnectorsState,
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
            connectors: ConnectorsState::default(),
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
        self.ensure_run_source(run_id, session_id, None)
    }

    pub fn ensure_run_source(
        &mut self,
        run_id: &str,
        session_id: Option<String>,
        connector_id: Option<String>,
    ) -> &mut RunState {
        if !self.runs.contains_key(run_id) {
            self.run_order.push(run_id.to_owned());
            self.runs
                .insert(run_id.to_owned(), RunState::new(run_id, session_id.clone()));
        }
        let run = self.runs.get_mut(run_id).expect("run was inserted");
        if session_id.is_some() {
            run.session_id = session_id;
        }
        if connector_id.is_some() {
            run.connector_id = connector_id;
        }
        run
    }

    pub fn selected_session(&self) -> Option<&SessionView> {
        let selected = self.sessions.selected_id.as_deref()?;
        self.sessions
            .items
            .iter()
            .find(|session| session.key() == selected)
    }

    /// Applies HTTP acceptance for both immediate submission and durable
    /// Outbox replay. A provisional Run may have been redirected to a command
    /// on an existing Run by the Host's session coordinator.
    pub fn accept_submission(
        &mut self,
        session_key: &str,
        provisional_id: &str,
        response: &Value,
        input: String,
        native_anchor_id: Option<String>,
        now: f64,
    ) -> Option<String> {
        let actual_id = response
            .get("run_id")
            .and_then(value_id)
            .unwrap_or_else(|| provisional_id.to_owned());
        let session = self
            .sessions
            .items
            .iter_mut()
            .find(|session| session.key() == session_key)?;
        if actual_id != provisional_id {
            session.run_ids.retain(|id| id != provisional_id);
            self.runs.remove(provisional_id);
            self.run_order.retain(|id| id != provisional_id);
        }
        if !session.run_ids.contains(&actual_id) {
            session.run_ids.push(actual_id.clone());
        }
        session.updated_at_unix_ms = session.updated_at_unix_ms.max(now as i64);
        let session_id = session.id.clone();
        let connector_id = session.connector_id.clone();
        let run = self.ensure_run_source(&actual_id, Some(session_id), connector_id);
        match response
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or("started")
        {
            "steered" => {
                if let Some(command_id) = response.get("command_id").and_then(value_id) {
                    run.optimistic_steer(
                        format!("steer-{command_id}"),
                        input,
                        now,
                        native_anchor_id,
                    );
                }
            }
            "replayed" => {}
            _ => run.record_accepted_input(input, now),
        }
        if let Some(view) = response.get("view") {
            run.apply_view(view.clone(), now);
        }
        self.reconcile_request_actions(&actual_id);
        Some(actual_id)
    }

    /// Activates a session that the Host has just created. A new Agent
    /// session has no persisted transcript yet, so presenting it must not
    /// inherit an older selection's loading state or imply that history is
    /// still being fetched.
    pub fn activate_created_agent_session(&mut self, connector_id: String, session_key: String) {
        self.sessions.selected_id = Some(session_key);
        self.ui.new_session_open = false;
        self.ui.drawer_open = false;
        self.ui.session_actions_open = false;
        self.ui.loading_session = None;
        self.ui.timeline_scrolled_away = false;
        self.ui.session_tab = Some(connector_id);
        self.ui.session_page = 0;
    }

    pub fn selected_connector(&self) -> Option<&AgentConnectorView> {
        let connector_id = self.selected_session()?.connector_id.as_deref()?;
        self.connectors
            .items
            .iter()
            .find(|connector| connector.connector_id == connector_id)
    }

    /// Complete content that can affect the selected timeline's scroll
    /// height. Live observers compare this around a projection so in-place
    /// growth of the current message follows the live edge just like a newly
    /// appended block. Session metadata-only refreshes remain scroll-neutral.
    pub fn selected_timeline_content(
        &self,
    ) -> Option<(Vec<SessionTimelineBlock>, Option<SessionRunIssue>)> {
        let session = self.selected_session()?;
        Some((
            timeline_blocks_for_session(self, session),
            latest_session_run_issue(self, session),
        ))
    }

    /// Last provider-owned activity visible when a Host input is submitted.
    /// Capturing this stable identity prevents a fresh controlled Run from
    /// being guessed into the beginning of the previous native turn.
    pub fn selected_native_tail_id(&self) -> Option<String> {
        let history_id = self.selected_session()?.history_run_id()?;
        timeline_for_run(self.runs.get(&history_id)?)
            .iter()
            .rev()
            .find_map(timeline_item_native_id)
            .map(str::to_owned)
    }

    pub fn project_agent_session(&mut self, detail: AgentSessionDetail) -> bool {
        let connector_id = detail.summary.connector_id.clone();
        let session_id = detail.summary.session_id.clone();
        let session_key = format!("{connector_id}\0{session_id}");
        let mut replay = Vec::new();
        if let Some(cursor) = detail.stream_cursor {
            // A full reconciliation may race an incremental SSE mutation. An
            // older snapshot must never replace a newer browser projection or
            // move its cursor backwards; doing so made the newest messages
            // disappear/reappear and visibly remounted the live edge.
            if self
                .sessions
                .stream_cursors
                .get(&session_key)
                .is_some_and(|current| *current > cursor)
            {
                replay = self
                    .sessions
                    .recent_changes
                    .get(&session_key)
                    .into_iter()
                    .flatten()
                    .filter(|change| change.sequence > cursor)
                    .cloned()
                    .collect();
                let current = self.sessions.stream_cursors[&session_key];
                if replay.first().map(|change| change.sequence) != cursor.checked_add(1)
                    || replay.last().map(|change| change.sequence) != Some(current)
                    || replay
                        .windows(2)
                        .any(|pair| pair[0].sequence.checked_add(1) != Some(pair[1].sequence))
                {
                    return false;
                }
            }
            self.sessions
                .stream_cursors
                .insert(session_key.clone(), cursor);
            self.sessions.recent_changes.remove(&session_key);
        }
        let timeline_before = self.agent_session_timeline_snapshot(&connector_id, &session_id);
        let run_id = format!("agent-history:{connector_id}:{session_id}");
        let projection_time = detail.summary.updated_at_unix_ms.unwrap_or_default() as f64;
        let latest_turn_status = detail.turns.last().map(|turn| turn.status.clone());
        let latest_turn_failure = detail.turns.last().and_then(|turn| turn.failure.clone());
        let controlled_runs = detail.controlled_runs.clone();
        let pending_session_state = waiting_state_for_requests(&detail.pending_requests);
        let controlled_run_ids = controlled_runs
            .iter()
            .filter_map(controlled_run_id)
            .collect::<Vec<_>>();

        // `controlled_runs` is an authoritative, bounded control-plane view,
        // not an append-only history. Keeping every Run ever observed for a
        // native session causes an old Host mirror to reappear as soon as its
        // correlation id slides out of the provider's latest history page.
        // Preserve only a local submission that has not reached the Host yet;
        // all accepted Runs are represented by the response above.
        let local_submissions = self
            .sessions
            .items
            .iter()
            .find(|session| {
                session.id == session_id
                    && session.connector_id.as_deref() == Some(connector_id.as_str())
            })
            .into_iter()
            .flat_map(|session| session.run_ids.iter())
            .filter(|candidate| *candidate != &run_id)
            .filter(|candidate| !controlled_run_ids.contains(candidate))
            .filter(|candidate| {
                self.runs
                    .get(*candidate)
                    .is_some_and(|run| run.status == "submitting")
            })
            .cloned()
            .collect::<Vec<_>>();

        if let Some(session) = self.sessions.items.iter_mut().find(|session| {
            session.id == session_id
                && session.connector_id.as_deref() == Some(connector_id.as_str())
        }) {
            session.title = detail.summary.title.clone();
            session.preview = detail.summary.preview.clone();
            session.cwd = detail.summary.cwd.clone();
            session.state = Some(detail.summary.state.clone());
            session.execution_profile = detail.summary.execution_profile.clone();
            if let Some(created_at) = detail.summary.created_at_unix_ms {
                session.created_at_unix_ms = created_at;
            }
            if let Some(updated_at) = detail.summary.updated_at_unix_ms {
                session.updated_at_unix_ms = updated_at;
            }
            session.run_ids.clear();
            session.run_ids.push(run_id.clone());
            session.run_ids.extend(controlled_run_ids.iter().cloned());
            session.run_ids.extend(local_submissions);
        } else {
            let mut session = detail.summary.clone().into_session();
            session.run_ids.extend(controlled_run_ids.iter().cloned());
            self.sessions.items.push(session);
        }

        let run = self.ensure_run_source(&run_id, Some(session_id), Some(connector_id.clone()));
        run.status = "delivered".to_owned();
        run.history_latest_turn_status = latest_turn_status;
        run.failure = latest_turn_failure;
        run.error = None;
        run.commands.clear();
        run.streamed_outputs.clear();
        project_latest_agent_history(run, detail.turns);
        run.pending = detail.pending_requests;
        if !run.history_pagination_started {
            run.history_next_cursor = detail.next_cursor;
        }
        self.reconcile_request_actions(&run_id);
        for view in controlled_runs {
            let Some(controlled_run_id) = controlled_run_id(&view) else {
                continue;
            };
            self.ensure_run_source(
                &controlled_run_id,
                Some(detail.summary.session_id.clone()),
                Some(detail.summary.connector_id.clone()),
            )
            .apply_view(view, projection_time);
            self.reconcile_request_actions(&controlled_run_id);
        }
        if let Some(waiting_state) = pending_session_state {
            if let Some(session) = self.sessions.items.iter_mut().find(|session| {
                session.id == detail.summary.session_id
                    && session.connector_id.as_deref() == Some(detail.summary.connector_id.as_str())
            }) {
                session.state = Some(waiting_state.to_owned());
            }
        }
        for change in replay {
            self.apply_agent_session_change(change, projection_time as i64);
        }
        timeline_before
            != self.agent_session_timeline_snapshot(&connector_id, &detail.summary.session_id)
    }

    /// Applies one provider-neutral live mutation without rebuilding the
    /// bounded session page. Stable activity ids make retries idempotent and
    /// preserve an existing item's presentation order.
    pub fn apply_agent_session_change(
        &mut self,
        change: AgentSessionChangeView,
        observed_at_unix_ms: i64,
    ) -> bool {
        if matches!(
            change.change,
            AgentSessionChangeKindView::RefreshRequired { .. }
        ) {
            return false;
        }
        let session_key = format!("{}\0{}", change.connector_id, change.session_id);
        if change.sequence > 0
            && self
                .sessions
                .stream_cursors
                .get(&session_key)
                .is_some_and(|cursor| change.sequence <= *cursor)
        {
            return false;
        }
        let recent = self.sessions.recent_changes.entry(session_key).or_default();
        recent.push(change.clone());
        if recent.len() > 256 {
            recent.remove(0);
        }
        let connector_id = change.connector_id;
        let session_id = change.session_id;
        let sequence = change.sequence;
        let timeline_before = self.agent_session_timeline_snapshot(&connector_id, &session_id);
        let run_id = format!("agent-history:{connector_id}:{session_id}");

        match change.change {
            AgentSessionChangeKindView::RefreshRequired { .. } => return false,
            AgentSessionChangeKindView::PendingRequestUpsert { request } => {
                if let Some(session) = self.sessions.items.iter_mut().find(|session| {
                    session.id == session_id
                        && session.connector_id.as_deref() == Some(connector_id.as_str())
                }) {
                    session.state = Some(waiting_state_for_request(&request).to_owned());
                    session.updated_at_unix_ms =
                        session.updated_at_unix_ms.max(observed_at_unix_ms);
                }
                let run = self.ensure_run_source(
                    &run_id,
                    Some(session_id.clone()),
                    Some(connector_id.clone()),
                );
                let request_id = request
                    .get("request_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                run.pending.retain(|candidate| {
                    request_id.as_ref().is_none_or(|request_id| {
                        candidate.get("request_id").and_then(Value::as_str) != Some(request_id)
                    })
                });
                run.pending.push(request);
                self.reconcile_request_actions(&run_id);
            }
            AgentSessionChangeKindView::PendingRequestClosed { request_id } => {
                self.remove_session_pending_request(&connector_id, &session_id, &request_id);
                if let Some(session) = self.sessions.items.iter_mut().find(|session| {
                    session.id == session_id
                        && session.connector_id.as_deref() == Some(connector_id.as_str())
                }) {
                    session.updated_at_unix_ms =
                        session.updated_at_unix_ms.max(observed_at_unix_ms);
                }
            }
            AgentSessionChangeKindView::TurnStatus {
                status, failure, ..
            } => {
                if let Some(session) = self.sessions.items.iter_mut().find(|session| {
                    session.id == session_id
                        && session.connector_id.as_deref() == Some(connector_id.as_str())
                }) {
                    session.state = Some(session_state_for_turn_status(&status).to_owned());
                    session.updated_at_unix_ms =
                        session.updated_at_unix_ms.max(observed_at_unix_ms);
                }
                let run = self.ensure_run_source(
                    &run_id,
                    Some(session_id.clone()),
                    Some(connector_id.clone()),
                );
                run.history_latest_turn_status = Some(status);
                run.failure = failure;
                run.error = None;
            }
            AgentSessionChangeKindView::ActivityUpsert {
                turn_id,
                turn_status,
                activity,
            } => {
                if let Some(session) = self.sessions.items.iter_mut().find(|session| {
                    session.id == session_id
                        && session.connector_id.as_deref() == Some(connector_id.as_str())
                }) {
                    session.state = Some(session_state_for_turn_status(&turn_status).to_owned());
                    session.updated_at_unix_ms =
                        session.updated_at_unix_ms.max(observed_at_unix_ms);
                    if !session.run_ids.contains(&run_id) {
                        session.run_ids.insert(0, run_id.clone());
                    }
                }

                let run = self.ensure_run_source(
                    &run_id,
                    Some(session_id.clone()),
                    Some(connector_id.clone()),
                );
                run.status = "delivered".to_owned();
                run.history_latest_turn_status = Some(turn_status);
                run.failure = None;
                run.error = None;
                let activity_id = activity.activity_id.clone();
                let client_id = activity
                    .details
                    .get("clientId")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                if !run.history_live_turn_ids.contains_key(&turn_id) {
                    run.history_live_turn_starts.push(activity_id.clone());
                    run.history_live_turn_ids
                        .insert(turn_id, activity_id.clone());
                }
                let existing_order = run
                    .messages
                    .iter()
                    .find(|message| {
                        message.id == activity_id
                            || client_id.as_ref().is_some_and(|client_id| {
                                message.client_id.as_ref() == Some(client_id)
                            })
                    })
                    .map(|message| message.order)
                    .or_else(|| {
                        run.activities
                            .iter()
                            .find(|item| item.id == activity_id)
                            .map(|item| item.order)
                    });
                run.messages.retain(|message| {
                    message.id != activity_id
                        && client_id
                            .as_ref()
                            .is_none_or(|client_id| message.client_id.as_ref() != Some(client_id))
                });
                run.activities.retain(|item| item.id != activity_id);
                if let Some(mut item) = agent_history_item(activity) {
                    let order = existing_order.unwrap_or_else(|| run.next_order());
                    item.set_order(order);
                    match item {
                        AgentHistoryItem::Message(message) => run.messages.push(message),
                        AgentHistoryItem::Activity(activity) => run.activities.push(activity),
                    }
                }
            }
        }

        self.sessions
            .stream_cursors
            .insert(format!("{connector_id}\0{session_id}"), sequence);

        timeline_before != self.agent_session_timeline_snapshot(&connector_id, &session_id)
    }

    fn agent_session_timeline_snapshot(
        &self,
        connector_id: &str,
        session_id: &str,
    ) -> Vec<AgentSessionTimelineSnapshot> {
        let Some(session) = self.sessions.items.iter().find(|session| {
            session.id == session_id && session.connector_id.as_deref() == Some(connector_id)
        }) else {
            return Vec::new();
        };
        timeline_run_ids_for_session(self, session)
            .into_iter()
            .filter_map(|run_id| {
                let run = self.runs.get(&run_id)?;
                Some(AgentSessionTimelineSnapshot {
                    run_id,
                    status: run.status.clone(),
                    blocks: timeline_blocks_for_run(run),
                    failure: run.failure.clone(),
                    error: run.error.clone(),
                })
            })
            .collect()
    }

    /// Prepends one older Agent transcript page without replacing the latest
    /// page that is already visible. Boundary overlap is deduplicated by the
    /// connector-provided activity id.
    pub fn prepend_agent_session_history(&mut self, detail: AgentSessionDetail) {
        let connector_id = detail.summary.connector_id.clone();
        let session_id = detail.summary.session_id.clone();
        let run_id = format!("agent-history:{connector_id}:{session_id}");
        let run = self.ensure_run_source(&run_id, Some(session_id), Some(connector_id));
        append_agent_history(run, detail.turns, true);
        run.history_pagination_started = true;
        run.history_next_cursor = detail.next_cursor;
        run.history_loading_earlier = false;
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
                "accepted" | "running" | "waiting" | "stopping"
            )
        })
    }

    /// A non-terminal Run whose durable SSE stream must remain attached.
    /// `unknown` means Provider continuity is being reconciled; it is not a
    /// browser transport state and must still receive ContinuityRestored.
    pub fn observable_run(&self) -> Option<&RunState> {
        self.current_run().filter(|run| {
            matches!(
                run.status.as_str(),
                "accepted" | "running" | "waiting" | "stopping" | "unknown"
            ) && !run.recovery_is_manual()
        })
    }

    pub fn pending_run(&self) -> Option<&RunState> {
        self.pending_requests()
            .first()
            .map(|(run, _)| *run)
            .or_else(|| self.current_run())
    }

    /// Union pending requests without losing their resolution route. A native
    /// request mirrored in a Host Run still belongs to the connector; it has
    /// no Generic Tool approval binding in the Host broker.
    pub fn pending_requests(&self) -> Vec<(&RunState, &Value)> {
        let Some(session) = self.selected_session() else {
            return Vec::new();
        };
        let history_id = session.history_run_id();
        let native = history_id.as_ref().and_then(|id| self.runs.get(id));
        let native_can_resolve = self.connectors.items.iter().any(|connector| {
            session.connector_id.as_deref() == Some(connector.connector_id.as_str())
                && connector.capabilities.resolve_requests
        });
        let mut seen = BTreeSet::new();
        native
            .filter(|_| native_can_resolve)
            .into_iter()
            .chain(
                session
                    .run_ids
                    .iter()
                    .rev()
                    .filter(|id| Some(*id) != history_id.as_ref())
                    .filter_map(|id| self.runs.get(id))
                    .filter(|run| !is_terminal(&run.status)),
            )
            .chain(native.filter(|_| !native_can_resolve))
            .flat_map(|run| run.pending.iter().map(move |request| (run, request)))
            .filter(|(_, request)| {
                request
                    .get("request_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| seen.insert(id))
            })
            .collect()
    }

    pub fn recoverable_run(&self) -> Option<&RunState> {
        self.current_run()
            .filter(|run| run.status == "unknown" && !run.recovery_allows_new_run())
    }

    /// Drops request-action locks that are no longer backed by a pending
    /// request in the latest durable event or bounded Run snapshot.
    pub fn reconcile_request_actions(&mut self, run_id: &str) {
        let prefix = format!("{run_id}:");
        let pending = self
            .runs
            .get(run_id)
            .map(|run| {
                run.pending
                    .iter()
                    .filter_map(|request| request.get("request_id").and_then(Value::as_str))
                    .map(|request_id| UiState::request_action_key(run_id, request_id))
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        self.ui
            .resolving_requests
            .retain(|key| !key.starts_with(&prefix) || pending.contains(key));
        self.ui
            .request_errors
            .retain(|key, _| !key.starts_with(&prefix) || pending.contains(key));
    }

    pub fn remove_pending_request(&mut self, run_id: &str, request_id: &str) -> bool {
        let removed = self.runs.get_mut(run_id).is_some_and(|run| {
            let before = run.pending.len();
            run.pending.retain(|request| {
                request.get("request_id").and_then(Value::as_str) != Some(request_id)
            });
            if run.status == "waiting" && !has_blocking_request(&run.pending) {
                run.status = "running".to_owned();
            }
            run.pending.len() != before
        });
        self.ui.set_request_resolving(run_id, request_id, false);
        self.ui
            .request_errors
            .remove(&UiState::request_action_key(run_id, request_id));
        removed
    }

    /// Converges one logical session request across its provider-history and
    /// optional Host Run projections. This is used both by SSE close events
    /// and by the successful HTTP response, so a lost/reordered close event
    /// cannot leave a duplicate approval card behind.
    pub fn remove_session_pending_request(
        &mut self,
        connector_id: &str,
        session_id: &str,
        request_id: &str,
    ) -> bool {
        let affected_runs = self
            .runs
            .iter()
            .filter(|(_, run)| {
                run.session_id.as_deref() == Some(session_id)
                    && run.connector_id.as_deref() == Some(connector_id)
            })
            .map(|(run_id, _)| run_id.clone())
            .collect::<Vec<_>>();
        let mut removed = false;
        for affected_run_id in affected_runs {
            removed |= self.remove_pending_request(&affected_run_id, request_id);
        }
        let remaining_state = self
            .runs
            .values()
            .filter(|run| {
                run.session_id.as_deref() == Some(session_id)
                    && run.connector_id.as_deref() == Some(connector_id)
            })
            .flat_map(|run| run.pending.iter())
            .map(waiting_state_for_request)
            .min_by_key(|state| usize::from(*state != "waiting_approval"));
        if let Some(session) = self.sessions.items.iter_mut().find(|session| {
            session.id == session_id && session.connector_id.as_deref() == Some(connector_id)
        }) {
            if let Some(remaining_state) = remaining_state {
                session.state = Some(remaining_state.to_owned());
            } else if matches!(
                session.state.as_deref(),
                Some("waiting_input" | "waiting_approval")
            ) {
                session.state = Some("active".to_owned());
            }
        }
        removed
    }
}

#[derive(Debug)]
enum AgentHistoryItem {
    Message(Message),
    Activity(ToolActivity),
}

impl AgentHistoryItem {
    fn id(&self) -> &str {
        match self {
            Self::Message(message) => &message.id,
            Self::Activity(activity) => &activity.id,
        }
    }

    fn set_order(&mut self, order: u64) {
        match self {
            Self::Message(message) => message.order = order,
            Self::Activity(activity) => activity.order = order,
        }
    }

    fn client_id(&self) -> Option<&str> {
        match self {
            Self::Message(message) => message.client_id.as_deref(),
            Self::Activity(_) => None,
        }
    }
}

fn project_latest_agent_history(run: &mut RunState, turns: Vec<crate::model::AgentSessionTurn>) {
    run.history_live_turn_ids = turns
        .iter()
        .filter_map(|turn| {
            turn.activities
                .first()
                .map(|activity| (turn.turn_id.clone(), activity.activity_id.clone()))
        })
        .collect();
    run.history_live_turn_starts = turns
        .iter()
        .filter_map(|turn| turn.activities.first())
        .map(|activity| activity.activity_id.clone())
        .collect();
    let mut incoming_ids = BTreeSet::new();
    let mut incoming_client_ids = BTreeSet::new();
    let mut incoming = turns
        .into_iter()
        .flat_map(|turn| turn.activities)
        .filter_map(agent_history_item)
        .filter(|item| {
            incoming_ids.insert(item.id().to_owned())
                && match item {
                    AgentHistoryItem::Message(message) => message
                        .client_id
                        .as_ref()
                        .is_none_or(|client_id| incoming_client_ids.insert(client_id.clone())),
                    AgentHistoryItem::Activity(_) => true,
                }
        })
        .collect::<Vec<_>>();

    if !run.history_pagination_started {
        // Before the user asks for older pages, the server snapshot is the
        // complete bounded live window. Rebuild it deterministically instead
        // of retaining items that slid out and assigning every overlapping id
        // a larger order on each refresh. The latter made an unchanged Codex
        // session mutate forever and caused the mobile viewport to jump.
        run.messages.clear();
        run.activities.clear();
        run.presentation_cursor = 0;
        for item in &mut incoming {
            item.set_order(run.next_order());
        }
        for item in incoming {
            match item {
                AgentHistoryItem::Message(message) => run.messages.push(message),
                AgentHistoryItem::Activity(activity) => run.activities.push(activity),
            }
        }
        return;
    }

    // The incoming page owns the order of its complete suffix. Preserve the
    // cached prefix before its first overlap, then rebuild the suffix. Merely
    // assigning new items the next order placed missing middle items after
    // their replies whenever SSE and pagination arrived in different orders.
    let mut existing = run
        .messages
        .drain(..)
        .map(AgentHistoryItem::Message)
        .chain(run.activities.drain(..).map(AgentHistoryItem::Activity))
        .collect::<Vec<_>>();
    existing.sort_by_key(|item| match item {
        AgentHistoryItem::Message(message) => message.order,
        AgentHistoryItem::Activity(activity) => activity.order,
    });
    let overlap = existing
        .iter()
        .position(|item| {
            incoming_ids.contains(item.id())
                || item
                    .client_id()
                    .is_some_and(|id| incoming_client_ids.contains(id))
        })
        .unwrap_or(existing.len());
    existing.truncate(overlap);
    existing.extend(incoming);
    run.presentation_cursor = 0;
    for mut item in existing {
        item.set_order(run.next_order());
        match item {
            AgentHistoryItem::Message(message) => run.messages.push(message),
            AgentHistoryItem::Activity(activity) => run.activities.push(activity),
        }
    }
}

fn session_state_for_turn_status(status: &str) -> &'static str {
    match status {
        "pending" | "active" => "active",
        _ => "idle",
    }
}

fn waiting_state_for_request(request: &Value) -> &'static str {
    match request.pointer("/payload/type").and_then(Value::as_str) {
        Some("approval") => "waiting_approval",
        _ => "waiting_input",
    }
}

fn waiting_state_for_requests(requests: &[Value]) -> Option<&'static str> {
    requests
        .iter()
        .map(waiting_state_for_request)
        .min_by_key(|state| usize::from(*state != "waiting_approval"))
}

fn append_agent_history(
    run: &mut RunState,
    turns: Vec<crate::model::AgentSessionTurn>,
    prepend: bool,
) {
    let mut seen_ids = run
        .messages
        .iter()
        .map(|message| message.id.clone())
        .chain(run.activities.iter().map(|activity| activity.id.clone()))
        .collect::<BTreeSet<_>>();
    let mut seen_client_ids = run
        .messages
        .iter()
        .filter_map(|message| message.client_id.clone())
        .collect::<BTreeSet<_>>();
    let mut items = turns
        .into_iter()
        .flat_map(|turn| turn.activities)
        .filter_map(agent_history_item)
        .filter(|item| {
            seen_ids.insert(item.id().to_owned())
                && match item {
                    AgentHistoryItem::Message(message) => message
                        .client_id
                        .as_ref()
                        .is_none_or(|client_id| seen_client_ids.insert(client_id.clone())),
                    AgentHistoryItem::Activity(_) => true,
                }
        })
        .collect::<Vec<_>>();

    if prepend {
        let offset = items.len() as u64;
        if offset == 0 {
            return;
        }
        shift_timeline_orders(run, offset);
        for (index, item) in items.iter_mut().enumerate() {
            item.set_order(index as u64 + 1);
        }
        run.presentation_cursor = run.presentation_cursor.saturating_add(offset);
    } else {
        for item in &mut items {
            item.set_order(run.next_order());
        }
    }

    for item in items {
        match item {
            AgentHistoryItem::Message(message) => run.messages.push(message),
            AgentHistoryItem::Activity(activity) => run.activities.push(activity),
        }
    }
}

fn agent_history_item(activity: crate::model::AgentSessionActivity) -> Option<AgentHistoryItem> {
    let text = contents_text(Some(&Value::Array(activity.content.clone())));
    let occurred_at_unix_ms =
        native_activity_timestamp_ms(&activity.activity_id).map(|value| value as i64);
    match activity.kind.as_str() {
        "user_message" | "agent_message" if !text.is_empty() => {
            Some(AgentHistoryItem::Message(Message {
                id: activity.activity_id,
                client_id: activity
                    .details
                    .get("clientId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                role: if activity.kind == "user_message" {
                    "user".to_owned()
                } else {
                    "assistant".to_owned()
                },
                text,
                order: 0,
                occurred_at_unix_ms,
                native_anchor_id: None,
                optimistic: false,
                deferred: activity.status == "pending"
                    && activity.details.get("phase").and_then(Value::as_str) == Some("deferred"),
                partial: false,
                steering: false,
            }))
        }
        "user_message" | "agent_message" => None,
        _ => {
            let mut evidence = Vec::new();
            if !activity.details.is_null() {
                evidence.push(activity.details);
            }
            if !text.is_empty() {
                evidence.push(serde_json::json!({
                    "type": "note",
                    "text": text,
                }));
            }
            Some(AgentHistoryItem::Activity(ToolActivity {
                id: activity.activity_id,
                tool_name: activity.title.unwrap_or_else(|| activity.kind.clone()),
                state: activity.status,
                evidence,
                order: 0,
            }))
        }
    }
}

fn shift_timeline_orders(run: &mut RunState, offset: u64) {
    for message in &mut run.messages {
        message.order = message.order.saturating_add(offset);
    }
    for output in run.streamed_outputs.values_mut() {
        output.order = output.order.saturating_add(offset);
    }
    for activity in &mut run.activities {
        activity.order = activity.order.saturating_add(offset);
    }
    for command in &mut run.commands {
        command.order = command.order.saturating_add(offset);
    }
    if let Some(progress) = &mut run.progress {
        progress.order = progress.order.saturating_add(offset);
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

#[derive(Debug, Clone, PartialEq)]
pub enum TimelineBlock {
    Entry(TimelineItem),
    ActivityGroup(Vec<TimelineItem>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionTimelineBlock {
    pub run_id: String,
    pub block: TimelineBlock,
}

impl SessionTimelineBlock {
    pub fn key(&self) -> String {
        let (kind, item) = match &self.block {
            TimelineBlock::Entry(item) => ("entry", item),
            TimelineBlock::ActivityGroup(items) => (
                "operations",
                items.first().expect("activity group is never empty"),
            ),
        };
        format!("{}:{kind}:{}", self.run_id, timeline_item_id(item))
    }
}

fn timeline_item_id(item: &TimelineItem) -> String {
    match item {
        TimelineItem::Message(message) => message.id.clone(),
        TimelineItem::Stream(output) => output.output_id.clone(),
        TimelineItem::Activity(activity) => activity.id.clone(),
        TimelineItem::Command(command) => command.id.clone(),
        TimelineItem::Progress(progress) => format!("progress-{}", progress.order),
    }
}

#[derive(Debug, Clone, PartialEq)]
struct AgentSessionTimelineSnapshot {
    run_id: String,
    status: String,
    blocks: Vec<TimelineBlock>,
    failure: Option<Value>,
    error: Option<String>,
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

/// Returns the session runs that still contribute unique timeline content.
///
/// A controlled external-Agent run is an optimistic Host-side mirror of a
/// native turn. Once the native transcript echoes the Orchestral client id,
/// that transcript becomes authoritative and the mirror must disappear.
/// Correlation is identity-based so two legitimate messages with identical
/// text remain visible.
pub fn timeline_run_ids_for_session(state: &AppState, session: &SessionView) -> Vec<String> {
    let represented_runs = session
        .history_run_id()
        .and_then(|history_id| state.runs.get(&history_id))
        .into_iter()
        .flat_map(|run| run.messages.iter())
        .filter_map(|message| message.client_id.as_deref())
        .filter_map(orchestral_run_id_from_any_client_id)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    session
        .run_ids
        .iter()
        .filter(|run_id| {
            run_id.starts_with("agent-history:") || !represented_runs.contains(run_id.as_str())
        })
        .cloned()
        .collect()
}

/// Returns the issue that owns the current session live edge.
///
/// Individual Run failures remain durable in `RunState`, but an older failed
/// Run must not be presented as the session's current footer after a newer
/// Run has started or completed. Doing so detached the failure from its turn
/// and made a recovered conversation look permanently broken.
pub fn latest_session_run_issue(
    state: &AppState,
    session: &SessionView,
) -> Option<SessionRunIssue> {
    let run = timeline_run_ids_for_session(state, session)
        .into_iter()
        .rev()
        .find_map(|run_id| state.runs.get(&run_id))?;
    run.failure
        .clone()
        .map(SessionRunIssue::Failure)
        .or_else(|| run.error.clone().map(SessionRunIssue::ControlError))
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionRunIssue {
    Failure(Value),
    ControlError(String),
}

/// Merges the bounded native transcript with any Host-controlled mirror.
///
/// Codex pages by activity and response size. A latest page can therefore
/// contain an assistant response while the correlated user activity is only
/// present on an older page. Each Host user input is a stable in-memory anchor
/// for placing its still-needed mirror before later native activities instead
/// of appending it to the end of the conversation.
pub fn timeline_blocks_for_session(
    state: &AppState,
    session: &SessionView,
) -> Vec<SessionTimelineBlock> {
    let run_ids = timeline_run_ids_for_session(state, session);
    let history_id = session.history_run_id();
    let Some(history_id) = history_id.filter(|history_id| state.runs.contains_key(history_id))
    else {
        return fold_session_timeline(
            run_ids
                .into_iter()
                .filter_map(|run_id| {
                    state
                        .runs
                        .get(&run_id)
                        .map(|run| (run_id, timeline_for_run(run)))
                })
                .flat_map(|(run_id, items)| {
                    items.into_iter().map(move |item| (run_id.clone(), item))
                }),
        );
    };
    let Some(history) = state.runs.get(&history_id) else {
        return Vec::new();
    };

    let history_items = timeline_for_run(history);
    let history_times = effective_native_timestamps(&history_items);
    let represented_command_ids = history
        .messages
        .iter()
        .filter_map(|message| message.client_id.as_deref())
        .filter_map(orchestral_command_identity)
        .fold(
            BTreeMap::<String, BTreeSet<String>>::new(),
            |mut commands, (run_id, command_id)| {
                commands
                    .entry(run_id.to_owned())
                    .or_default()
                    .insert(command_id.to_owned());
                commands
            },
        );
    let active_turn_start = history
        .history_latest_turn_status
        .as_deref()
        .filter(|status| !matches!(*status, "completed" | "interrupted" | "failed"))
        .and_then(|_| history.history_live_turn_starts.last())
        .and_then(|activity_id| {
            history_items
                .iter()
                .position(|item| timeline_item_native_id(item) == Some(activity_id.as_str()))
        });
    let mut insertions = BTreeMap::<usize, Vec<(String, Vec<TimelineItem>)>>::new();

    let fully_visible_runs = run_ids.into_iter().collect::<BTreeSet<_>>();
    let mut controlled = session
        .run_ids
        .iter()
        .filter(|run_id| *run_id != &history_id)
        .cloned()
        .enumerate()
        .filter_map(|(run_index, run_id)| {
            let run = state.runs.get(&run_id)?;
            let segments = if fully_visible_runs.contains(&run_id) {
                controlled_timeline_segments(run, represented_command_ids.get(&run_id))
            } else {
                unrepresented_controlled_input_segments(run, represented_command_ids.get(&run_id))
            };
            Some(segments.into_iter().enumerate().map(
                move |(segment_index, (started_at, native_anchor_id, steering, items))| {
                    (
                        started_at,
                        run_index,
                        segment_index,
                        run_id.clone(),
                        native_anchor_id,
                        steering,
                        items,
                    )
                },
            ))
        })
        .flatten()
        .collect::<Vec<_>>();
    // `session.run_ids` and each Run's presentation order are the causal Host
    // order. Durable steer events intentionally have no browser timestamp, so
    // sorting timestamps first would move those commands ahead of their own
    // initial input whenever several segments share one native boundary.
    controlled.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.2.cmp(&right.2)));

    for (started_at, _, _segment_index, run_id, native_anchor_id, steering, mut items) in controlled
    {
        let time_insertion = || {
            started_at
                .map(|started_at| (started_at / 1_000.0).floor() * 1_000.0)
                .and_then(|started_at| {
                    history_times.iter().position(|timestamp| {
                        timestamp.is_some_and(|timestamp| timestamp >= started_at)
                    })
                })
        };
        let response_turn_start = (!steering)
            .then(|| {
                controlled_response_turn_start(
                    &history_items,
                    &history.history_live_turn_starts,
                    &items,
                )
            })
            .flatten();
        let explicit_insertion = native_anchor_id
            .as_deref()
            .and_then(|activity_id| {
                history_items
                    .iter()
                    .position(|item| timeline_item_native_id(item) == Some(activity_id))
            })
            .map(|index| index.saturating_add(1));
        // A page with an older cursor is, by contract, the latest suffix of
        // the ordered native transcript. An opaque anchor that is no longer
        // in that suffix belongs before it; appending at the live edge would
        // invert the Host input and the native work it triggered.
        let before_bounded_history = native_anchor_id
            .as_ref()
            .filter(|_| history.history_next_cursor.is_some())
            .map(|_| 0);
        let insertion = if !steering {
            // A terminal Host mirror contains the same assistant response as
            // its native turn. That response is a stronger structural anchor
            // than filesystem timestamps, which Codex can rewrite while a
            // rollout is active. Insert before the matching turn even when a
            // newer native turn is already visible.
            explicit_insertion
                .or(response_turn_start)
                .or(before_bounded_history)
                .or_else(time_insertion)
                // A currently active provider turn may already contain the
                // controlled response while its user item fell off the bounded
                // page, so fill that live turn boundary. A completed turn is
                // immutable history: never split it for an uncorrelated Run.
                .unwrap_or_else(|| active_turn_start.unwrap_or(history_items.len()))
        } else {
            // Each steer captures the native edge visible at submission.
            // Time remains only a legacy fallback for pre-anchor state.
            explicit_insertion
                .or(before_bounded_history)
                .or_else(time_insertion)
                .unwrap_or(history_items.len())
        };
        if response_turn_start.is_some() {
            // The native transcript owns the response body. The controlled
            // mirror remains only to supply the missing user/command events;
            // rendering its terminal response again would duplicate it.
            items.retain(|item| {
                !matches!(item, TimelineItem::Message(message)
                    if message.role == "assistant"
                        && history_items.iter().any(|native| matches!(native,
                            TimelineItem::Message(native_message)
                                if native_message.role == "assistant"
                                    && native_message.text == message.text)))
            });
        }
        insertions
            .entry(insertion)
            .or_default()
            .push((run_id, items));
    }

    let mut merged = Vec::new();
    for (index, item) in history_items.into_iter().enumerate() {
        append_session_insertions(&mut merged, insertions.remove(&index));
        merged.push((history_id.clone(), item));
    }
    append_session_insertions(&mut merged, insertions.remove(&history_times.len()));
    fold_session_timeline(merged)
}

fn controlled_response_turn_start(
    history_items: &[TimelineItem],
    turn_start_ids: &[String],
    controlled_items: &[TimelineItem],
) -> Option<usize> {
    let controlled_responses = controlled_items
        .iter()
        .filter_map(|item| match item {
            TimelineItem::Message(message)
                if message.role == "assistant" && !message.text.is_empty() =>
            {
                Some(message.text.as_str())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    if controlled_responses.is_empty() {
        return None;
    }
    let response_index =
        history_items
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, item)| match item {
                TimelineItem::Message(message)
                    if message.role == "assistant"
                        && controlled_responses.contains(message.text.as_str()) =>
                {
                    Some(index)
                }
                _ => None,
            })?;
    turn_start_ids
        .iter()
        .filter_map(|activity_id| {
            history_items
                .iter()
                .position(|item| timeline_item_native_id(item) == Some(activity_id.as_str()))
        })
        .take_while(|index| *index <= response_index)
        .last()
        .or(Some(response_index))
}

type ControlledTimelineSegment = (Option<f64>, Option<String>, bool, Vec<TimelineItem>);

fn controlled_timeline_segments(
    run: &RunState,
    represented_command_ids: Option<&BTreeSet<String>>,
) -> Vec<ControlledTimelineSegment> {
    let mut segments = Vec::new();
    let mut anchor = run.started_at;
    let mut native_anchor_id = None;
    let mut steering = false;
    let mut items = Vec::new();

    for item in timeline_for_run(run).into_iter().filter(|item| match item {
        TimelineItem::Message(message) if message.steering => message
            .id
            .strip_prefix("steer-")
            .is_none_or(|id| !represented_command_ids.is_some_and(|ids| ids.contains(id))),
        TimelineItem::Command(command) => {
            !represented_command_ids.is_some_and(|ids| ids.contains(&command.id))
        }
        _ => true,
    }) {
        if let TimelineItem::Message(message) = &item {
            if message.role == "user" {
                if !items.is_empty() {
                    segments.push((
                        anchor,
                        native_anchor_id.take(),
                        steering,
                        std::mem::take(&mut items),
                    ));
                }
                anchor = message
                    .occurred_at_unix_ms
                    .map(|value| value as f64)
                    .or_else(|| (!message.steering).then_some(run.started_at).flatten());
                native_anchor_id = message.native_anchor_id.clone();
                steering = message.steering;
            }
        }
        items.push(item);
    }
    if !items.is_empty() {
        segments.push((anchor, native_anchor_id, steering, items));
    }
    segments
}

/// Once a native transcript owns a Run's initial turn, retain only Host steer
/// messages that have not acquired their own native command identity yet.
/// Hiding the entire Run at the first native echo made a newly accepted steer
/// disappear until Codex emitted its later user-message notification.
fn unrepresented_controlled_input_segments(
    run: &RunState,
    represented_command_ids: Option<&BTreeSet<String>>,
) -> Vec<ControlledTimelineSegment> {
    timeline_for_run(run)
        .into_iter()
        .filter_map(|item| {
            let TimelineItem::Message(message) = &item else {
                return None;
            };
            if !message.steering {
                return None;
            }
            let command_id = message.id.strip_prefix("steer-")?.to_owned();
            if represented_command_ids.is_some_and(|ids| ids.contains(&command_id)) {
                return None;
            }
            let anchor = message.occurred_at_unix_ms.map(|value| value as f64);
            let native_anchor_id = message.native_anchor_id.clone();
            let mut items = vec![item];
            if let Some(command) = run
                .commands
                .iter()
                .find(|command| command.id == command_id)
                .cloned()
            {
                items.push(TimelineItem::Command(command));
            }
            Some((anchor, native_anchor_id, true, items))
        })
        .collect()
}

fn append_session_insertions(
    merged: &mut Vec<(String, TimelineItem)>,
    insertions: Option<Vec<(String, Vec<TimelineItem>)>>,
) {
    for (run_id, items) in insertions.into_iter().flatten() {
        merged.extend(items.into_iter().map(|item| (run_id.clone(), item)));
    }
}

fn effective_native_timestamps(items: &[TimelineItem]) -> Vec<Option<f64>> {
    let mut timestamps = items
        .iter()
        .map(timeline_item_native_timestamp)
        .collect::<Vec<_>>();

    // Commands may use random UUIDs. Keep them next to the following native
    // response whose id does carry time, which also ensures a Host user input
    // is placed before the work it triggered.
    let mut next = None;
    for timestamp in timestamps.iter_mut().rev() {
        if timestamp.is_some() {
            next = *timestamp;
        } else if next.is_some() {
            *timestamp = next;
        }
    }
    let mut previous = None;
    for timestamp in &mut timestamps {
        if timestamp.is_some() {
            previous = *timestamp;
        } else if previous.is_some() {
            *timestamp = previous;
        }
    }
    timestamps
}

fn timeline_item_native_timestamp(item: &TimelineItem) -> Option<f64> {
    let id = timeline_item_native_id(item)?;
    native_activity_timestamp_ms(id).map(|timestamp| timestamp as f64)
}

fn timeline_item_native_id(item: &TimelineItem) -> Option<&str> {
    match item {
        TimelineItem::Message(message) => Some(&message.id),
        TimelineItem::Stream(output) => Some(&output.output_id),
        TimelineItem::Activity(activity) => Some(&activity.id),
        TimelineItem::Command(command) => Some(&command.id),
        TimelineItem::Progress(_) => None,
    }
}

fn native_activity_timestamp_ms(id: &str) -> Option<u64> {
    const EARLIEST_PLAUSIBLE_MS: u64 = 1_577_836_800_000;
    const LATEST_PLAUSIBLE_MS: u64 = 4_102_444_800_000;

    let compact = id.replace('-', "");
    if compact.len() == 32 && compact.as_bytes().get(12) == Some(&b'7') {
        let timestamp = u64::from_str_radix(&compact[..12], 16).ok()?;
        if (EARLIEST_PLAUSIBLE_MS..=LATEST_PLAUSIBLE_MS).contains(&timestamp) {
            return Some(timestamp);
        }
    }

    // OpenAI response item ids encode creation seconds after a 16-hex random
    // prefix and the `01` version marker (for example `msg_…01xxxxxxxx`).
    let suffix = id.split_once('_')?.1;
    if suffix.len() < 26 || suffix.get(16..18) != Some("01") {
        return None;
    }
    let timestamp = u64::from_str_radix(suffix.get(18..26)?, 16)
        .ok()?
        .saturating_mul(1_000);
    (EARLIEST_PLAUSIBLE_MS..=LATEST_PLAUSIBLE_MS)
        .contains(&timestamp)
        .then_some(timestamp)
}

fn orchestral_run_id_from_client_id(client_id: &str) -> Option<&str> {
    let value = client_id.strip_prefix("orchestral:")?;
    let (run_id, digest) = value.split_once(':')?;
    (!run_id.is_empty() && !digest.is_empty()).then_some(run_id)
}

fn orchestral_run_id_from_any_client_id(client_id: &str) -> Option<&str> {
    orchestral_run_id_from_client_id(client_id)
        .or_else(|| orchestral_command_identity(client_id).map(|(run_id, _command_id)| run_id))
}

fn orchestral_command_identity(client_id: &str) -> Option<(&str, &str)> {
    let value = client_id.strip_prefix("orchestral-command:")?;
    let mut fields = value.splitn(3, ':');
    let run_id = fields.next()?;
    let command_id = fields.next()?;
    let digest = fields.next()?;
    (!run_id.is_empty() && !command_id.is_empty() && !digest.is_empty())
        .then_some((run_id, command_id))
}

/// Folds consecutive tool and command events into one disclosure block.
///
/// Message and progress boundaries remain visible in chronological order, but
/// long agent loops no longer consume one full card per operation. The full
/// evidence stays available inside the group when the user expands it.
pub fn timeline_blocks_for_run(run: &RunState) -> Vec<TimelineBlock> {
    fold_session_timeline(
        timeline_for_run(run)
            .into_iter()
            .map(|item| (run.id.clone(), item)),
    )
    .into_iter()
    .map(|entry| entry.block)
    .collect()
}

fn fold_session_timeline(
    entries: impl IntoIterator<Item = (String, TimelineItem)>,
) -> Vec<SessionTimelineBlock> {
    let mut blocks = Vec::new();
    let mut activity_run_id = None::<String>;
    let mut activities = Vec::new();

    for (run_id, item) in entries {
        if matches!(item, TimelineItem::Activity(_) | TimelineItem::Command(_)) {
            if activity_run_id
                .as_deref()
                .is_some_and(|active| active != run_id)
            {
                flush_session_activities(&mut blocks, &mut activity_run_id, &mut activities);
            }
            activity_run_id.get_or_insert_with(|| run_id.clone());
            activities.push(item);
            continue;
        }

        flush_session_activities(&mut blocks, &mut activity_run_id, &mut activities);
        blocks.push(SessionTimelineBlock {
            run_id,
            block: TimelineBlock::Entry(item),
        });
    }

    flush_session_activities(&mut blocks, &mut activity_run_id, &mut activities);
    blocks
}

fn flush_session_activities(
    blocks: &mut Vec<SessionTimelineBlock>,
    run_id: &mut Option<String>,
    activities: &mut Vec<TimelineItem>,
) {
    if activities.is_empty() {
        return;
    }
    blocks.push(SessionTimelineBlock {
        run_id: run_id.take().expect("activity group has an owning Run"),
        block: TimelineBlock::ActivityGroup(std::mem::take(activities)),
    });
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
            .map(|reference| {
                let short = reference.chars().take(12).collect::<String>();
                let url = content
                    .pointer("/access/uri")
                    .and_then(Value::as_str)
                    .filter(|uri| uri.starts_with("https://"))
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("/api/v1/attachments/{reference}"));
                if content
                    .get("media_type")
                    .and_then(Value::as_str)
                    .is_some_and(|media_type| media_type.starts_with("image/"))
                {
                    let preview_url = format!(
                        "{url}{}preview=1",
                        if url.contains('?') { '&' } else { '?' }
                    );
                    format!("![生成图片]({preview_url})\n\n[下载原图 · {short}…]({url})")
                } else {
                    format!("[下载附件 · {short}…]({url})")
                }
            })
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

fn value_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.get("0").and_then(Value::as_str).map(str::to_owned))
}

fn controlled_run_id(view: &Value) -> Option<String> {
    view.pointer("/execution/run_id").and_then(value_id)
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
    use crate::model::{AgentSessionActivity, AgentSessionTurn};

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

    fn uuid_v7_at(timestamp_ms: u64, tail: u64) -> String {
        let timestamp = format!("{timestamp_ms:012x}");
        format!(
            "{}-{}-7000-8000-{tail:012x}",
            &timestamp[..8],
            &timestamp[8..]
        )
    }

    #[test]
    fn refresh_reconciliation_coalesces_bursts_and_allows_one_trailing_pass() {
        let mut coordinator = AgentSessionReconcileCoordinator::default();

        assert!(coordinator.request(7));
        assert!(!coordinator.request(7));
        let checkpoint = coordinator.checkpoint();
        assert!(!coordinator.has_trailing_request(7, checkpoint));

        assert!(!coordinator.request(7));
        assert!(coordinator.has_trailing_request(7, checkpoint));
        coordinator.finish(7);
        assert!(coordinator.request(7));

        // A stale worker must not clear the current generation.
        assert!(coordinator.request(8));
        coordinator.finish(7);
        assert!(!coordinator.request(8));
    }

    fn agent_detail_at(cursor: u64, activity_id: &str, text: &str) -> AgentSessionDetail {
        serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "fixture/local",
                "session_id": "thread-1",
                "updated_at_unix_ms": cursor,
                "state": "active"
            },
            "turns": [{
                "turn_id": "turn-1",
                "status": "active",
                "activities": [{
                    "activity_id": activity_id,
                    "kind": "agent_message",
                    "status": "completed",
                    "content": [{"body": {"kind": "inline", "value": text}}]
                }]
            }],
            "stream_cursor": cursor
        }))
        .unwrap()
    }

    #[test]
    fn stale_detail_snapshot_cannot_replace_a_newer_live_edge_or_rewind_cursor() {
        let mut state = AppState::new(true);
        assert!(state.project_agent_session(agent_detail_at(10, "message-new", "new")));

        assert!(!state.project_agent_session(agent_detail_at(9, "message-old", "old")));

        let session = state.sessions.items.first().unwrap();
        let messages = timeline_blocks_for_session(&state, session)
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(messages, vec!["new"]);
        assert_eq!(
            state.sessions.stream_cursors.get("fixture/local\0thread-1"),
            Some(&10)
        );
    }

    #[test]
    fn generated_image_artifact_projects_to_preview_and_download_markdown() {
        let reference = "a".repeat(64);
        let content = serde_json::json!({
            "media_type": "image/png",
            "body": {
                "kind": "artifact",
                "value": {
                    "artifact_ref": reference,
                    "digest": "a".repeat(64)
                }
            }
        });

        let projected = content_text(&content);

        assert!(projected.contains("?preview=1)"));
        assert!(projected.contains("[下载原图 · aaaaaaaaaaaa…](/api/v1/attachments/"));
        let rendered = crate::markdown::render(&projected);
        assert!(rendered.contains("<img src=\"/api/v1/attachments/"));
        assert!(rendered.contains("<a href=\"/api/v1/attachments/"));
    }

    #[test]
    fn resolved_image_artifact_uses_the_direct_object_store_url() {
        let reference = "b".repeat(64);
        let direct =
            format!("https://orchestral-files.example/v1/blobs/{reference}?capability=signed");
        let content = serde_json::json!({
            "media_type": "image/png",
            "body": {
                "kind": "artifact",
                "value": {
                    "artifact_ref": reference,
                    "digest": "b".repeat(64)
                }
            },
            "access": {
                "uri": direct,
                "media_type": "image/png",
                "byte_size": 123
            }
        });

        let projected = content_text(&content);

        assert!(projected.contains("https://orchestral-files.example/v1/blobs/"));
        assert!(projected.contains("capability=signed&preview=1"));
        assert!(!projected.contains("/api/v1/attachments/"));
        let rendered = crate::markdown::render(&projected);
        assert!(rendered.contains("<img src=\"https://orchestral-files.example/"));
        assert!(rendered.contains("<a href=\"https://orchestral-files.example/"));
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
    fn consecutive_operations_fold_into_one_timeline_block() {
        let mut run = RunState::new("run-1", None);
        run.record_accepted_input("inspect".to_owned(), 1.0);
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
            &record(
                1,
                "command-1",
                serde_json::json!({
                    "type": "command_received",
                    "command": {
                        "command_id": "resolve-1",
                        "payload": {"type": "resolve_request", "request_id": "approval-1"},
                    },
                }),
            ),
            1.0,
        );
        run.project_durable(
            &record(
                2,
                "output",
                serde_json::json!({
                    "type": "output_committed",
                    "output_id": "out-1",
                    "content": content("done"),
                }),
            ),
            2.0,
        );

        let blocks = timeline_blocks_for_run(&run);
        assert_eq!(blocks.len(), 3);
        assert!(matches!(
            blocks[0],
            TimelineBlock::Entry(TimelineItem::Message(_))
        ));
        assert!(matches!(&blocks[1], TimelineBlock::ActivityGroup(items) if items.len() == 2));
        assert!(matches!(
            blocks[2],
            TimelineBlock::Entry(TimelineItem::Message(_))
        ));
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
        run.optimistic_start_input("inspect the project".to_owned(), 0.5, None);

        assert_eq!(run.status, "submitting");
        assert!(run.messages[0].optimistic);

        run.record_accepted_input("inspect the project".to_owned(), 1.0);

        assert_eq!(run.status, "accepted");
        assert_eq!(run.messages.len(), 1);
        assert!(!run.messages[0].optimistic);
        assert_eq!(run.messages[0].text, "inspect the project");
    }

    #[test]
    fn run_view_restores_initial_input_for_a_fresh_client() {
        let mut run = RunState::new("run-1", None);
        run.project_durable(
            &record(1, "accepted", serde_json::json!({"type": "run_accepted"})),
            1.0,
        );
        run.project_durable(
            &record(
                2,
                "output",
                serde_json::json!({
                    "type": "output_committed",
                    "output_id": "out-1",
                    "content": content("done"),
                }),
            ),
            2.0,
        );
        run.apply_view(
            serde_json::json!({
                "execution": {"session_id": "session-1"},
                "state": {"state": "terminal", "terminal": {"type": "delivered", "delivery_id": "delivery-1"}},
                "last_run_seq": 2,
                "pending_requests": [],
                "input": content("inspect the project"),
            }),
            2.0,
        );

        let timeline = timeline_for_run(&run);
        assert_eq!(timeline.len(), 2);
        assert!(
            matches!(&timeline[0], TimelineItem::Message(value) if value.role == "user" && value.text == "inspect the project" && !value.optimistic)
        );
        assert!(
            matches!(&timeline[1], TimelineItem::Message(value) if value.role == "assistant" && value.text == "done")
        );
    }

    #[test]
    fn terminal_run_view_projects_delivery_once_without_waiting_for_journal_replay() {
        let delivery = serde_json::json!({
            "delivery_id": "delivery-1",
            "final_response": {"body": {"kind": "inline", "value": "Codex finished"}},
            "provenance": {"supporting_event_ids": ["output-event"]}
        });
        let view = serde_json::json!({
            "execution": {"session_id": "session-1"},
            "state": {
                "state": "terminal",
                "terminal": {"type": "delivered", "delivery_id": "delivery-1"}
            },
            "last_run_seq": 2,
            "pending_requests": [],
            "input": content("do it"),
            "delivery": delivery.clone(),
            "partial_delivery": null
        });
        let mut run = RunState::new("run-1", None);

        run.apply_view(view.clone(), 1.0);
        run.apply_view(view, 2.0);
        run.project_durable(
            &record(
                1,
                "output-event",
                serde_json::json!({
                    "type": "output_committed",
                    "output_id": "output-1",
                    "content": content("Codex finished")
                }),
            ),
            3.0,
        );
        run.project_durable(
            &record(
                2,
                "delivery-event",
                serde_json::json!({
                    "type": "delivery_committed",
                    "delivery": delivery
                }),
            ),
            4.0,
        );

        let assistant = run
            .messages
            .iter()
            .filter(|message| message.role == "assistant")
            .collect::<Vec<_>>();
        assert_eq!(assistant.len(), 1);
        assert_eq!(assistant[0].id, "delivery:delivery-1");
        assert_eq!(assistant[0].text, "Codex finished");
        assert!(!assistant[0].partial);
    }

    #[test]
    fn terminal_run_does_not_leave_received_commands_running_forever() {
        let mut run = RunState::new("run-1", None);
        run.project_durable(
            &record(
                1,
                "command-event",
                serde_json::json!({
                    "type": "command_received",
                    "command": {
                        "command_id": "command-1",
                        "payload": {"type": "steer", "content": content("too late")}
                    }
                }),
            ),
            1.0,
        );

        run.apply_view(
            serde_json::json!({
                "state": {
                    "state": "terminal",
                    "terminal": {"type": "delivered", "delivery_id": "delivery-1"}
                },
                "last_run_seq": 2,
                "pending_requests": []
            }),
            2.0,
        );

        assert_eq!(run.commands[0].state, "rejected");
        assert_eq!(
            run.commands[0]
                .outcome
                .as_ref()
                .and_then(|value| value.get("code"))
                .and_then(Value::as_str),
            Some("run_ended_before_command_disposition")
        );
    }

    #[test]
    fn terminal_view_reuses_an_already_projected_supporting_output() {
        let delivery = serde_json::json!({
            "delivery_id": "delivery-1",
            "final_response": {"body": {"kind": "inline", "value": "done"}},
            "provenance": {"supporting_event_ids": ["output-event"]}
        });
        let mut run = RunState::new("run-1", None);
        run.project_durable(
            &record(
                1,
                "output-event",
                serde_json::json!({
                    "type": "output_committed",
                    "output_id": "output-1",
                    "content": content("done")
                }),
            ),
            1.0,
        );

        run.apply_view(
            serde_json::json!({
                "state": {
                    "state": "terminal",
                    "terminal": {"type": "delivered", "delivery_id": "delivery-1"}
                },
                "last_run_seq": 2,
                "pending_requests": [],
                "delivery": delivery
            }),
            2.0,
        );

        assert_eq!(run.messages.len(), 1);
        assert_eq!(run.messages[0].id, "output-event");
        assert_eq!(run.messages[0].text, "done");
    }

    #[test]
    fn distinct_output_events_with_identical_text_remain_visible() {
        let mut run = RunState::new("run-1", None);
        for (sequence, event_id, output_id) in [
            (1, "output-event-1", "output-1"),
            (2, "output-event-2", "output-2"),
        ] {
            run.project_durable(
                &record(
                    sequence,
                    event_id,
                    serde_json::json!({
                        "type": "output_committed",
                        "output_id": output_id,
                        "content": content("same text")
                    }),
                ),
                sequence as f64,
            );
        }

        assert_eq!(run.messages.len(), 2);
        assert_eq!(run.messages[0].id, "output-event-1");
        assert_eq!(run.messages[1].id, "output-event-2");
    }

    #[test]
    fn incomplete_run_view_projects_partial_response_with_a_stable_identity() {
        let view = serde_json::json!({
            "execution": {"session_id": "session-1"},
            "state": {
                "state": "terminal",
                "terminal": {
                    "type": "incomplete",
                    "reason": {"type": "limit_reached", "limit": "model_steps"}
                }
            },
            "last_run_seq": 4,
            "pending_requests": [],
            "delivery": null,
            "partial_delivery": {
                "partial_delivery_id": "partial-1",
                "response": {"body": {"kind": "inline", "value": "Partial result"}}
            }
        });
        let mut run = RunState::new("run-1", None);

        run.apply_view(view.clone(), 1.0);
        run.apply_view(view, 2.0);

        assert_eq!(run.messages.len(), 1);
        assert_eq!(run.messages[0].id, "partial-delivery:partial-1");
        assert_eq!(run.messages[0].text, "Partial result");
        assert!(run.messages[0].partial);
    }

    #[test]
    fn connector_namespace_prevents_same_session_id_from_colliding() {
        let native: SessionView = serde_json::from_value(serde_json::json!({
            "id": "same-id",
            "created_at_unix_ms": 1,
            "updated_at_unix_ms": 1,
            "run_ids": []
        }))
        .unwrap();
        let external: SessionView = serde_json::from_value(serde_json::json!({
            "id": "same-id",
            "created_at_unix_ms": 1,
            "updated_at_unix_ms": 1,
            "run_ids": [],
            "connector_id": "codex/local"
        }))
        .unwrap();

        assert_ne!(native.key(), external.key());
    }

    #[test]
    fn newly_created_agent_session_opens_as_empty_without_a_loading_placeholder() {
        let mut state = AppState::new(true);
        state.ui.loading_session = Some("codex/local\0old-thread".to_owned());
        state.ui.new_session_open = true;
        state.ui.drawer_open = true;
        state.ui.session_actions_open = true;

        state.activate_created_agent_session(
            "codex/local".to_owned(),
            "codex/local\0new-thread".to_owned(),
        );

        assert_eq!(
            state.sessions.selected_id.as_deref(),
            Some("codex/local\0new-thread")
        );
        assert_eq!(state.ui.session_tab.as_deref(), Some("codex/local"));
        assert_eq!(state.ui.loading_session, None);
        assert!(!state.ui.new_session_open);
        assert!(!state.ui.drawer_open);
        assert!(!state.ui.session_actions_open);
    }

    #[test]
    fn run_view_projects_provider_neutral_supervision_state() {
        let mut run = RunState::new("run-stalled", Some("thread-1".to_owned()));
        run.apply_view(
            serde_json::json!({
                "input": [],
                "state": {"state": "running"},
                "last_run_seq": 2,
                "pending_requests": [],
                "supervision": {
                    "state": "interrupting",
                    "reason": "execution lease expired",
                    "detected_at_unix_ms": 1234
                }
            }),
            2_000.0,
        );

        let supervision = run.supervision.expect("supervision is projected");
        assert_eq!(supervision.state, "interrupting");
        assert_eq!(supervision.reason, "execution lease expired");
        assert_eq!(supervision.detected_at_unix_ms, 1234);
    }

    #[test]
    fn unknown_run_requires_recovery_instead_of_steer() {
        let mut state = AppState::new(true);
        state.sessions.items.push(SessionView {
            id: "thread-1".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            run_ids: vec!["run-1".to_owned()],
            connector_id: Some("agent/local".to_owned()),
            title: None,
            preview: None,
            cwd: None,
            state: Some("active".to_owned()),
            execution_profile: Default::default(),
        });
        state.sessions.selected_id = Some("agent/local\0thread-1".to_owned());
        state
            .ensure_run_source(
                "run-1",
                Some("thread-1".to_owned()),
                Some("agent/local".to_owned()),
            )
            .status = "unknown".to_owned();

        assert!(state.active_run().is_none());
        assert_eq!(
            state.recoverable_run().map(|run| run.id.as_str()),
            Some("run-1")
        );
    }

    #[test]
    fn manual_recovery_keeps_the_session_locked_until_a_terminal_event() {
        let mut state = AppState::new(true);
        state.sessions.items.push(SessionView {
            id: "thread-1".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            run_ids: vec!["run-1".to_owned()],
            connector_id: None,
            title: None,
            preview: None,
            cwd: None,
            state: Some("active".to_owned()),
            execution_profile: Default::default(),
        });
        state.sessions.selected_id = Some("thread-1".to_owned());
        state
            .ensure_run("run-1", Some("thread-1".to_owned()))
            .apply_view(
                serde_json::json!({
                    "execution": {"session_id": "thread-1", "run_id": "run-1"},
                    "state": {
                        "state": "unknown",
                        "last_confirmed_seq": 4,
                        "reason": "Host continuity is unavailable"
                    },
                    "last_run_seq": 5,
                    "pending_requests": [],
                    "recovery": {
                        "mode": "manual",
                    "can_start_new_run": false,
                        "reason": "model attempt outcome is unknown"
                    }
                }),
                1.0,
            );

        let run = state.current_run().expect("manual Run remains visible");
        assert!(run.recovery_is_manual());
        assert!(!run.recovery_allows_new_run());
        assert!(state.active_run().is_none());
        assert!(state.observable_run().is_none());
        assert!(state.recoverable_run().is_some());
    }

    #[test]
    fn request_action_locks_follow_authoritative_pending_state() {
        let mut state = AppState::new(true);
        let run = state.ensure_run("run-1", None);
        run.status = "waiting".to_owned();
        run.pending = vec![serde_json::json!({
            "request_id": "still-pending",
            "blocking": true
        })];
        state
            .ui
            .set_request_resolving("run-1", "still-pending", true);
        state
            .ui
            .set_request_resolving("run-1", "already-resolved", true);
        state
            .ui
            .set_request_resolving("another-run", "request", true);

        state.reconcile_request_actions("run-1");

        assert!(state.ui.request_is_resolving("run-1", "still-pending"));
        assert!(!state.ui.request_is_resolving("run-1", "already-resolved"));
        assert!(state.ui.request_is_resolving("another-run", "request"));

        assert!(state.remove_pending_request("run-1", "still-pending"));
        assert!(!state.ui.request_is_resolving("run-1", "still-pending"));
        assert!(state.runs["run-1"].pending.is_empty());
        assert_eq!(state.runs["run-1"].status, "running");
    }

    #[test]
    fn competing_client_request_close_clears_the_pending_card() {
        let mut run = RunState::new("run-1", None);
        run.status = "running".to_owned();
        run.project_durable(
            &serde_json::json!({
                "event_id": "opened-1",
                "run_seq": 1,
                "payload": {
                    "type": "request_opened",
                    "request": {
                        "request_id": "approval-1",
                        "blocking": true
                    }
                }
            }),
            1.0,
        );

        assert_eq!(run.status, "waiting");
        assert_eq!(run.pending.len(), 1);

        run.project_durable(
            &serde_json::json!({
                "event_id": "closed-1",
                "run_seq": 2,
                "payload": {
                    "type": "request_closed",
                    "request_id": "approval-1",
                    "reason": "resolved_by_competing_client"
                }
            }),
            2.0,
        );

        assert_eq!(run.status, "running");
        assert!(run.pending.is_empty());
    }

    #[test]
    fn external_agent_history_projects_messages_and_operations_in_order() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "title": "Existing Codex thread",
                "state": "idle"
            },
            "turns": [{
                "turn_id": "turn-1",
                "status": "completed",
                "activities": [
                    {
                        "activity_id": "user-1",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "inspect it"}}]
                    },
                    {
                        "activity_id": "command-1",
                        "kind": "command",
                        "status": "completed",
                        "title": "cargo test",
                        "content": [],
                        "details": {"type": "command", "command": "cargo test"}
                    },
                    {
                        "activity_id": "agent-1",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "tests pass"}}]
                    }
                ]
            }],
            "pending_requests": [],
            "next_cursor": "activity-offset-v1:3"
        }))
        .unwrap();
        let mut state = AppState::new(true);
        assert!(state.project_agent_session(detail.clone()));
        assert!(
            !state.project_agent_session(detail),
            "an unchanged polling snapshot must not trigger live-follow scrolling"
        );

        let run = state
            .runs
            .get("agent-history:codex/local:thread-1")
            .unwrap();
        let timeline = timeline_for_run(run);
        assert!(matches!(&timeline[0], TimelineItem::Message(message) if message.role == "user"));
        assert!(
            matches!(&timeline[1], TimelineItem::Activity(activity) if activity.tool_name == "cargo test")
        );
        assert!(
            matches!(&timeline[2], TimelineItem::Message(message) if message.role == "assistant")
        );
        assert_eq!(
            run.history_next_cursor.as_deref(),
            Some("activity-offset-v1:3")
        );
    }

    #[test]
    fn refreshed_agent_session_restores_its_active_controlled_run() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "updated_at_unix_ms": 10,
                "state": "active"
            },
            "turns": [],
            "pending_requests": [],
            "controlled_runs": [{
                "created_at_unix_ms": 5,
                "execution": {
                    "session_id": "thread-1",
                    "run_id": "controlled-run"
                },
                "state": {"state": "running"},
                "last_run_seq": 2,
                "input": [{"body": {"kind": "inline", "value": "continue"}}]
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);

        state.project_agent_session(detail);
        state.sessions.selected_id = Some("codex/local\0thread-1".to_owned());

        let session = state.sessions.items.first().unwrap();
        assert_eq!(
            session.run_ids,
            vec![
                "agent-history:codex/local:thread-1".to_owned(),
                "controlled-run".to_owned()
            ]
        );
        let active = state
            .active_run()
            .expect("controlled Run remains steerable");
        assert_eq!(active.id, "controlled-run");
        assert_eq!(active.connector_id.as_deref(), Some("codex/local"));
        assert_eq!(active.started_at, Some(5.0));
    }

    #[test]
    fn stale_native_history_preserves_the_ordered_controlled_run_suffix() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "fixture/local",
                "session_id": "thread-with-stale-index",
                "updated_at_unix_ms": 30,
                "state": "active"
            },
            "turns": [{
                "turn_id": "stale-native-turn",
                "status": "active",
                "activities": [{
                    "activity_id": "stale-native-edge",
                    "kind": "agent_message",
                    "status": "completed",
                    "content": [{"body": {"kind": "inline", "value": "indexed response"}}]
                }]
            }],
            "next_cursor": "older-native-page",
            "pending_requests": [],
            "controlled_runs": [
                {
                    "created_at_unix_ms": 10,
                    "after_activity_id": "stale-native-edge",
                    "execution": {
                        "session_id": "thread-with-stale-index",
                        "run_id": "controlled-older"
                    },
                    "state": {"state": "terminal", "terminal": {"type": "completed"}},
                    "last_run_seq": 4,
                    "input": [{"body": {"kind": "inline", "value": "first missing input"}}]
                },
                {
                    "created_at_unix_ms": 20,
                    "after_activity_id": "stale-native-edge",
                    "execution": {
                        "session_id": "thread-with-stale-index",
                        "run_id": "controlled-newer"
                    },
                    "state": {"state": "running"},
                    "last_run_seq": 2,
                    "input": [{"body": {"kind": "inline", "value": "second missing input"}}]
                }
            ]
        }))
        .unwrap();
        let mut state = AppState::new(true);

        state.project_agent_session(detail);
        state.sessions.selected_id = Some("fixture/local\0thread-with-stale-index".to_owned());

        let session = state.sessions.items.first().unwrap();
        assert_eq!(
            session.run_ids,
            [
                "agent-history:fixture/local:thread-with-stale-index",
                "controlled-older",
                "controlled-newer"
            ]
        );
        let messages = timeline_blocks_for_session(&state, session)
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            [
                "indexed response",
                "first missing input",
                "second missing input"
            ]
        );
        assert_eq!(state.active_run().unwrap().id, "controlled-newer");
    }

    #[test]
    fn bounded_native_page_places_each_controlled_steer_before_its_later_response() {
        let created_at = 1_788_303_954_016_u64;
        let steer_at = created_at + 4_000;
        let older_response_id = uuid_v7_at(created_at + 2_000, 1);
        let newer_response_id = uuid_v7_at(created_at + 6_000, 2);
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "updated_at_unix_ms": created_at + 7_000,
                "state": "active"
            },
            "turns": [{
                "turn_id": "turn-1",
                "status": "in_progress",
                "activities": [
                    {
                        "activity_id": older_response_id,
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "older response"}}]
                    },
                    {
                        "activity_id": "0d5ec963-c1f5-4d31-8c6e-ef49c79f20cb",
                        "kind": "reasoning",
                        "status": "completed",
                        "title": "Reasoning",
                        "content": []
                    },
                    {
                        "activity_id": newer_response_id,
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "later response"}}]
                    }
                ]
            }],
            "pending_requests": [],
            "controlled_runs": [{
                "created_at_unix_ms": created_at,
                "execution": {
                    "session_id": "thread-1",
                    "run_id": "controlled-run"
                },
                "state": {"state": "running"},
                "last_run_seq": 2,
                "input": [{"body": {"kind": "inline", "value": "initial request"}}]
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);
        state
            .runs
            .get_mut("controlled-run")
            .unwrap()
            .optimistic_steer(
                "steer-latest".to_owned(),
                "what is wrong?".to_owned(),
                steer_at as f64,
                None,
            );

        let session = state.sessions.items.first().unwrap();
        let messages = timeline_blocks_for_session(&state, session)
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => {
                    Some((message.role, message.text))
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            messages,
            vec![
                ("user".to_owned(), "initial request".to_owned()),
                ("assistant".to_owned(), "older response".to_owned()),
                ("user".to_owned(), "what is wrong?".to_owned()),
                ("assistant".to_owned(), "later response".to_owned()),
            ]
        );
    }

    #[test]
    fn off_page_history_anchors_keep_durable_steers_before_the_native_suffix() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "fixture/local",
                "session_id": "bounded-thread",
                "updated_at_unix_ms": 20_000,
                "state": "active"
            },
            "turns": [{
                "turn_id": "active-turn",
                "status": "in_progress",
                "activities": [
                    {
                        "activity_id": "native-reply-in-latest-suffix",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "current progress"}}]
                    },
                    {
                        "activity_id": "native-work-after-reply",
                        "kind": "command",
                        "status": "completed",
                        "title": "continuing work"
                    }
                ]
            }],
            "pending_requests": [],
            "next_cursor": "next-older-page",
            "controlled_runs": [{
                "created_at_unix_ms": 10_000,
                "after_activity_id": "initial-anchor-on-older-page",
                "execution": {
                    "session_id": "bounded-thread",
                    "run_id": "controlled-run"
                },
                "state": {"state": "running"},
                "last_run_seq": 4,
                "input": [{"body": {"kind": "inline", "value": "initial request"}}]
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);

        let run = state.runs.get_mut("controlled-run").unwrap();
        for (sequence, event_id, command_id, text, anchor) in [
            (
                1,
                "first-command-event",
                "first-command",
                "continue the work",
                "first-anchor-on-older-page",
            ),
            (
                2,
                "second-command-event",
                "second-command",
                "show progress",
                "second-anchor-on-older-page",
            ),
        ] {
            run.project_durable(
                &record(
                    sequence,
                    event_id,
                    serde_json::json!({
                        "type": "command_received",
                        "command": {
                            "command_id": command_id,
                            "payload": {
                                "type": "steer",
                                "content": content(text)
                            },
                            "extensions": {
                                "orchestral.dev/session-history-anchor": {
                                    "after_activity_id": anchor
                                }
                            }
                        }
                    }),
                ),
                20_000.0,
            );
        }

        let messages = timeline_blocks_for_session(&state, &state.sessions.items[0])
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            [
                "initial request",
                "continue the work",
                "show progress",
                "current progress"
            ]
        );
    }

    #[test]
    fn fresh_controlled_run_stays_after_the_native_edge_visible_at_submission() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-anchored",
                "state": "idle"
            },
            "turns": [{
                "turn_id": "turn-previous",
                "status": "completed",
                "activities": [
                    {
                        "activity_id": "native-user-previous",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "previous question"}}]
                    },
                    {
                        "activity_id": "native-answer-previous",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "previous answer"}}]
                    }
                ]
            }],
            "pending_requests": []
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);
        state.sessions.selected_id = Some("codex/local\0thread-anchored".to_owned());
        let native_anchor = state.selected_native_tail_id();
        assert_eq!(native_anchor.as_deref(), Some("native-answer-previous"));

        let run = state.ensure_run_source(
            "fresh-run",
            Some("thread-anchored".to_owned()),
            Some("codex/local".to_owned()),
        );
        run.optimistic_start_input("new question".to_owned(), 10_000.0, native_anchor.clone());
        run.optimistic_steer(
            "follow-up-command".to_owned(),
            "follow-up question".to_owned(),
            11_000.0,
            native_anchor,
        );
        state.sessions.items[0].run_ids.push("fresh-run".to_owned());

        let messages = timeline_blocks_for_session(&state, &state.sessions.items[0])
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            vec![
                "previous question",
                "previous answer",
                "new question",
                "follow-up question"
            ]
        );
    }

    #[test]
    fn unanchored_fresh_run_never_splits_the_last_bounded_history_turn() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "fixture/local",
                "session_id": "bounded-thread",
                "state": "active"
            },
            "turns": [{
                "turn_id": "stale-latest-turn",
                "status": "completed",
                "activities": [
                    {
                        "activity_id": "old-user",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "old question"}}]
                    },
                    {
                        "activity_id": "old-answer-part-1",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "old answer part 1"}}]
                    },
                    {
                        "activity_id": "old-reasoning",
                        "kind": "reasoning",
                        "status": "completed",
                        "content": []
                    },
                    {
                        "activity_id": "old-answer-tail",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "old answer tail"}}]
                    }
                ]
            }],
            "controlled_runs": [{
                "created_at_unix_ms": 99_000,
                "execution": {"session_id": "bounded-thread", "run_id": "fresh-run"},
                "state": {"state": "running"},
                "last_run_seq": 2,
                "input": [{"body": {"kind": "inline", "value": "new question"}}]
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);

        let messages = timeline_blocks_for_session(&state, &state.sessions.items[0])
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            [
                "old question",
                "old answer part 1",
                "old answer tail",
                "new question"
            ]
        );
    }

    #[test]
    fn projected_run_and_command_restore_their_history_anchors() {
        let mut run = RunState::new("run-1", Some("thread-1".to_owned()));
        run.apply_view(
            serde_json::json!({
                "created_at_unix_ms": 10_000,
                "after_activity_id": "visible-tail-before-run",
                "execution": {"session_id": "thread-1", "run_id": "run-1"},
                "state": {"state": "running"},
                "last_run_seq": 0,
                "input": [{"body": {"kind": "inline", "value": "initial"}}]
            }),
            10_000.0,
        );
        run.project_durable(
            &record(
                1,
                "command-event",
                serde_json::json!({
                    "type": "command_received",
                    "command": {
                        "command_id": "command-1",
                        "payload": {
                            "type": "steer",
                            "content": content("follow up")
                        },
                        "extensions": {
                            "orchestral.dev/session-history-anchor": {
                                "after_activity_id": "visible-tail-before-steer"
                            }
                        }
                    }
                }),
            ),
            11_000.0,
        );

        assert_eq!(
            run.messages[0].native_anchor_id.as_deref(),
            Some("visible-tail-before-run")
        );
        assert_eq!(
            run.messages[1].native_anchor_id.as_deref(),
            Some("visible-tail-before-steer")
        );
    }

    #[test]
    fn accepted_input_is_replaced_by_its_durable_event_without_losing_the_native_anchor() {
        let mut run = RunState::new("fresh-run", Some("thread-anchored".to_owned()));
        run.optimistic_start_input(
            "new question".to_owned(),
            10_000.0,
            Some("native-answer-previous".to_owned()),
        );

        // This is the real browser order: the start HTTP response arrives
        // before the durable stream catches up with InputCommitted.
        run.record_accepted_input("new question".to_owned(), 10_100.0);
        run.project_durable(
            &record(
                1,
                "input-event",
                serde_json::json!({
                    "type": "input_committed",
                    "content": content("new question")
                }),
            ),
            10_200.0,
        );

        assert_eq!(run.messages.len(), 1);
        assert_eq!(run.messages[0].id, "input-event");
        assert_eq!(run.messages[0].text, "new question");
        assert_eq!(
            run.messages[0].native_anchor_id.as_deref(),
            Some("native-answer-previous")
        );
        assert!(!run.messages[0].optimistic);
    }

    #[test]
    fn durable_steer_rehydrates_its_user_message_before_the_command_card() {
        let mut run = RunState::new("run-1", Some("thread-1".to_owned()));
        run.project_durable(
            &record(
                1,
                "command-event",
                serde_json::json!({
                    "type": "command_received",
                    "command": {
                        "command_id": "command-1",
                        "payload": {
                            "type": "steer",
                            "content": content("follow up")
                        }
                    }
                }),
            ),
            10_000.0,
        );

        let timeline = timeline_for_run(&run);
        assert!(matches!(
            &timeline[0],
            TimelineItem::Message(message)
                if message.id == "steer-command-1" && message.text == "follow up"
        ));
        assert!(matches!(
            &timeline[1],
            TimelineItem::Command(command) if command.id == "command-1"
        ));
    }

    #[test]
    fn two_clients_replaying_one_durable_steer_converge_on_identical_order() {
        let detail = || {
            serde_json::from_value::<AgentSessionDetail>(serde_json::json!({
                "summary": {
                    "connector_id": "codex/local",
                    "session_id": "thread-shared",
                    "state": "active"
                },
                "turns": [{
                    "turn_id": "turn-previous",
                    "status": "completed",
                    "activities": [
                        {
                            "activity_id": "native-user-previous",
                            "kind": "user_message",
                            "status": "completed",
                            "content": [{"body": {"kind": "inline", "value": "previous"}}]
                        },
                        {
                            "activity_id": "native-answer-previous",
                            "kind": "agent_message",
                            "status": "completed",
                            "content": [{"body": {"kind": "inline", "value": "answer"}}]
                        }
                    ]
                }],
                "pending_requests": []
            }))
            .unwrap()
        };
        let client = || {
            let mut state = AppState::new(true);
            state.project_agent_session(detail());
            state.sessions.selected_id = Some("codex/local\0thread-shared".to_owned());
            state.ensure_run_source(
                "run-shared",
                Some("thread-shared".to_owned()),
                Some("codex/local".to_owned()),
            );
            state.sessions.items[0]
                .run_ids
                .push("run-shared".to_owned());
            state
        };
        let visible_messages = |state: &AppState| {
            timeline_blocks_for_session(state, &state.sessions.items[0])
                .into_iter()
                .filter_map(|entry| match entry.block {
                    TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        let mut desktop = client();
        let mut phone = client();
        desktop
            .runs
            .get_mut("run-shared")
            .unwrap()
            .optimistic_steer(
                "steer-command-shared".to_owned(),
                "latest".to_owned(),
                10_000.0,
                Some("native-answer-previous".to_owned()),
            );
        let command = record(
            1,
            "command-event-shared",
            serde_json::json!({
                "type": "command_received",
                "command": {
                    "command_id": "command-shared",
                    "payload": {"type": "steer", "content": content("latest")}
                }
            }),
        );
        desktop
            .runs
            .get_mut("run-shared")
            .unwrap()
            .project_durable(&command, 10_100.0);
        phone
            .runs
            .get_mut("run-shared")
            .unwrap()
            .project_durable(&command, 99_900.0);

        assert_eq!(visible_messages(&desktop), ["previous", "answer", "latest"]);
        assert_eq!(visible_messages(&desktop), visible_messages(&phone));

        let mut echoed = detail();
        echoed.turns.push(AgentSessionTurn {
            turn_id: "turn-latest".to_owned(),
            status: "active".to_owned(),
            failure: None,
            activities: vec![AgentSessionActivity {
                activity_id: "native-command-shared".to_owned(),
                kind: "user_message".to_owned(),
                status: "completed".to_owned(),
                title: None,
                content: vec![serde_json::json!({
                    "body": {"kind": "inline", "value": "latest"}
                })],
                details: serde_json::json!({
                    "clientId": "orchestral-command:run-shared:command-shared:sha256:digest"
                }),
            }],
        });
        desktop.project_agent_session(echoed.clone());
        phone.project_agent_session(echoed);
        assert_eq!(visible_messages(&desktop), ["previous", "answer", "latest"]);
        assert_eq!(visible_messages(&desktop), visible_messages(&phone));
    }

    #[test]
    fn native_initial_echo_does_not_hide_a_later_unrepresented_steer() {
        let initial_detail = serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "active"
            },
            "turns": [{
                "turn_id": "turn-1",
                "status": "active",
                "activities": [
                    {
                        "activity_id": "native-initial",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "initial"}}],
                        "details": {"clientId": "orchestral:run-1:sha256:initial"}
                    },
                    {
                        "activity_id": "native-answer",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "working"}}]
                    }
                ]
            }],
            "pending_requests": []
        });
        let mut state = AppState::new(true);
        state.project_agent_session(serde_json::from_value(initial_detail.clone()).unwrap());
        state.sessions.selected_id = Some("codex/local\0thread-1".to_owned());
        let run = state.ensure_run_source(
            "run-1",
            Some("thread-1".to_owned()),
            Some("codex/local".to_owned()),
        );
        run.optimistic_start_input("initial".to_owned(), 1.0, None);
        run.record_accepted_input("initial".to_owned(), 2.0);
        run.optimistic_steer(
            "steer-command-1".to_owned(),
            "latest steer".to_owned(),
            3.0,
            Some("native-answer".to_owned()),
        );
        state.sessions.items[0].run_ids.push("run-1".to_owned());

        let visible_text = |state: &AppState| {
            timeline_blocks_for_session(state, &state.sessions.items[0])
                .into_iter()
                .filter_map(|entry| match entry.block {
                    TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(visible_text(&state), ["initial", "working", "latest steer"]);

        let mut native_command_detail = initial_detail;
        native_command_detail["turns"][0]["activities"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "activity_id": "native-command-1",
                "kind": "user_message",
                "status": "completed",
                "content": [{"body": {"kind": "inline", "value": "latest steer"}}],
                "details": {
                    "clientId": "orchestral-command:run-1:command-1:sha256:digest"
                }
            }));
        state.project_agent_session(serde_json::from_value(native_command_detail).unwrap());

        assert_eq!(visible_text(&state), ["initial", "working", "latest steer"]);
    }

    #[test]
    fn missing_correlated_user_uses_latest_native_turn_boundary_not_a_rewritten_file_time() {
        let earlier = uuid_v7_at(1_788_303_950_000, 1);
        let latest = uuid_v7_at(1_788_303_954_000, 2);
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "active"
            },
            "turns": [
                {
                    "turn_id": "turn-earlier",
                    "status": "completed",
                    "activities": [{
                        "activity_id": earlier,
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "earlier answer"}}]
                    }]
                },
                {
                    "turn_id": "turn-controlled",
                    "status": "running",
                    "activities": [{
                        "activity_id": latest,
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "controlled answer"}}]
                    }]
                }
            ],
            "pending_requests": [],
            "controlled_runs": [{
                "created_at_unix_ms": 1_788_303_960_000_u64,
                "execution": {"session_id": "thread-1", "run_id": "controlled-run"},
                "state": {"state": "running"},
                "last_run_seq": 2,
                "input": [{"body": {"kind": "inline", "value": "triggering question"}}]
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);
        let session = state.sessions.items.first().unwrap();
        let messages = timeline_blocks_for_session(&state, session)
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            messages,
            vec!["earlier answer", "triggering question", "controlled answer"]
        );
    }

    #[test]
    fn terminal_mirror_anchors_to_its_matching_older_turn_and_deduplicates_response() {
        let controlled_response = uuid_v7_at(1_788_303_950_000, 1);
        let newer_response = uuid_v7_at(1_788_303_954_000, 2);
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "active"
            },
            "turns": [
                {
                    "turn_id": "turn-controlled",
                    "status": "completed",
                    "activities": [{
                        "activity_id": controlled_response,
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "controlled answer"}}]
                    }]
                },
                {
                    "turn_id": "turn-newer",
                    "status": "running",
                    "activities": [{
                        "activity_id": newer_response,
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "newer answer"}}]
                    }]
                }
            ],
            "pending_requests": [],
            "controlled_runs": [{
                "created_at_unix_ms": 1_788_303_960_000_u64,
                "execution": {"session_id": "thread-1", "run_id": "controlled-run"},
                "state": {
                    "state": "terminal",
                    "terminal": {"type": "delivered", "delivery_id": "delivery-1"}
                },
                "last_run_seq": 14,
                "input": [{"body": {"kind": "inline", "value": "triggering question"}}],
                "delivery": {
                    "delivery_id": "delivery-1",
                    "final_response": {"body": {"kind": "inline", "value": "controlled answer"}},
                    "provenance": {"supporting_event_ids": []}
                }
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);
        let session = state.sessions.items.first().unwrap();
        let messages = timeline_blocks_for_session(&state, session)
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            messages,
            vec!["triggering question", "controlled answer", "newer answer"]
        );
    }

    #[test]
    fn a_sliding_live_window_converges_after_one_projection() {
        let detail = |ids: &[(&str, &str)]| {
            serde_json::from_value::<AgentSessionDetail>(serde_json::json!({
                "summary": {
                    "connector_id": "codex/local",
                    "session_id": "thread-1",
                    "state": "active"
                },
                "turns": [{
                    "turn_id": "turn-live",
                    "status": "running",
                    "activities": ids.iter().map(|(id, text)| serde_json::json!({
                        "activity_id": id,
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": text}}]
                    })).collect::<Vec<_>>()
                }],
                "pending_requests": []
            }))
            .unwrap()
        };
        let mut state = AppState::new(true);
        state.project_agent_session(detail(&[("old", "old"), ("overlap", "overlap")]));
        let shifted = detail(&[("overlap", "overlap"), ("new", "new")]);
        assert!(state.project_agent_session(shifted.clone()));
        assert!(
            !state.project_agent_session(shifted),
            "the same shifted window must not keep changing orders or scrolling"
        );
        let run = &state.runs["agent-history:codex/local:thread-1"];
        assert_eq!(run.messages.len(), 2);
        assert_eq!(run.messages[0].id, "overlap");
        assert_eq!(run.messages[1].id, "new");
    }

    #[test]
    fn codex_native_activity_ids_expose_stable_time_anchors() {
        assert_eq!(
            native_activity_timestamp_ms("rs_0123456789abcdef016a9755a4deadbeef"),
            Some(1_788_302_756_000)
        );
        let timestamp = 1_788_303_954_016;
        assert_eq!(
            native_activity_timestamp_ms(&uuid_v7_at(timestamp, 1)),
            Some(timestamp)
        );
        assert_eq!(
            native_activity_timestamp_ms("0d5ec963-c1f5-4d31-8c6e-ef49c79f20cb"),
            None
        );
    }

    #[test]
    fn native_client_id_replaces_only_its_controlled_run_mirror() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "idle"
            },
            "turns": [{
                "turn_id": "turn-1",
                "status": "completed",
                "activities": [
                    {
                        "activity_id": "native-user-1",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "你好"}}],
                        "details": {"clientId": "orchestral:run-1:sha256:digest"}
                    },
                    {
                        "activity_id": "native-agent-1",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "你好！"}}]
                    }
                ]
            }],
            "pending_requests": []
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);

        for run_id in ["run-1", "run-2"] {
            let run = state.ensure_run_source(
                run_id,
                Some("thread-1".to_owned()),
                Some("codex/local".to_owned()),
            );
            run.optimistic_start_input("你好".to_owned(), 1.0, None);
            run.record_accepted_input("你好".to_owned(), 2.0);
        }
        let session = state
            .sessions
            .items
            .iter_mut()
            .find(|session| session.id == "thread-1")
            .unwrap();
        session
            .run_ids
            .extend(["run-1".to_owned(), "run-2".to_owned()]);

        let session = state.sessions.items.first().unwrap();
        assert_eq!(
            timeline_run_ids_for_session(&state, session),
            vec![
                "agent-history:codex/local:thread-1".to_owned(),
                "run-2".to_owned()
            ]
        );
    }

    #[test]
    fn newer_success_supersedes_an_older_failure_at_the_session_live_edge() {
        let mut state = AppState::new(true);
        state.sessions.items.push(SessionView {
            id: "thread-1".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 3,
            run_ids: vec!["failed-run".to_owned(), "successful-run".to_owned()],
            connector_id: None,
            title: None,
            preview: None,
            cwd: None,
            state: Some("idle".to_owned()),
            execution_profile: Default::default(),
        });
        let failure = serde_json::json!({
            "code": "model_unavailable",
            "message": "temporary timeout"
        });
        let failed = state.ensure_run("failed-run", Some("thread-1".to_owned()));
        failed.status = "failed".to_owned();
        failed.failure = Some(failure.clone());
        state
            .ensure_run("successful-run", Some("thread-1".to_owned()))
            .status = "delivered".to_owned();

        let session = state.sessions.items.first().unwrap();
        assert_eq!(latest_session_run_issue(&state, session), None);

        let latest = state.runs.get_mut("successful-run").unwrap();
        latest.status = "failed".to_owned();
        latest.failure = Some(failure.clone());
        let session = state.sessions.items.first().unwrap();
        assert_eq!(
            latest_session_run_issue(&state, session),
            Some(SessionRunIssue::Failure(failure))
        );
    }

    #[test]
    fn native_turn_failure_is_visible_and_cleared_by_the_next_turn() {
        let failure = serde_json::json!({
            "code": "agent_transport_unavailable",
            "message": "proxy connection failed with status 502",
            "retryable": true,
            "details": null
        });
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "fixture/local",
                "session_id": "thread-1",
                "state": "idle"
            },
            "turns": [{
                "turn_id": "turn-failed",
                "status": "failed",
                "failure": failure,
                "activities": []
            }],
            "stream_cursor": 4
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);

        let session = state.sessions.items.first().unwrap();
        assert_eq!(
            latest_session_run_issue(&state, session),
            Some(SessionRunIssue::Failure(failure.clone()))
        );

        let change: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "fixture/local",
            "session_id": "thread-1",
            "sequence": 5,
            "change": {
                "type": "turn_status",
                "turn_id": "turn-next",
                "status": "active"
            }
        }))
        .unwrap();
        state.apply_agent_session_change(change, 10);

        let session = state.sessions.items.first().unwrap();
        assert_eq!(latest_session_run_issue(&state, session), None);
        let history = state
            .runs
            .get("agent-history:fixture/local:thread-1")
            .unwrap();
        assert_eq!(
            history.history_latest_turn_status.as_deref(),
            Some("active")
        );
    }

    #[test]
    fn native_steer_identity_hides_the_same_runs_initial_mirror() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "active"
            },
            "turns": [{
                "turn_id": "turn-latest",
                "status": "active",
                "activities": [{
                    "activity_id": "native-steer",
                    "kind": "user_message",
                    "status": "completed",
                    "content": [{"body": {"kind": "inline", "value": "latest steer"}}],
                    "details": {
                        "clientId": "orchestral-command:run-1:command-1:sha256:digest"
                    }
                }]
            }],
            "controlled_runs": [{
                "created_at_unix_ms": 1000,
                "execution": {"session_id": "thread-1", "run_id": "run-1"},
                "state": {"state": "running"},
                "last_run_seq": 2,
                "input": [{"body": {"kind": "inline", "value": "old initial input"}}]
            }]
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);

        let session = state.sessions.items.first().unwrap();
        assert_eq!(
            timeline_run_ids_for_session(&state, session),
            vec!["agent-history:codex/local:thread-1".to_owned()]
        );
        let messages = timeline_blocks_for_session(&state, session)
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(messages, ["latest steer"]);
    }

    #[test]
    fn authoritative_session_projection_drops_stale_controlled_runs() {
        let detail = |controlled_runs: Value| {
            serde_json::from_value::<AgentSessionDetail>(serde_json::json!({
                "summary": {
                    "connector_id": "codex/local",
                    "session_id": "thread-1",
                    "state": "active"
                },
                "turns": [{
                    "turn_id": "turn-latest",
                    "status": "active",
                    "activities": [{
                        "activity_id": "native-latest",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "latest"}}]
                    }]
                }],
                "controlled_runs": controlled_runs
            }))
            .unwrap()
        };
        let old_run = serde_json::json!([{
            "created_at_unix_ms": 1000,
            "execution": {"session_id": "thread-1", "run_id": "old-run"},
            "state": {"state": "terminal", "terminal": {"type": "failed"}},
            "last_run_seq": 4,
            "input": [{"body": {"kind": "inline", "value": "old input"}}]
        }]);
        let mut state = AppState::new(true);
        state.project_agent_session(detail(old_run));
        assert!(state.sessions.items[0]
            .run_ids
            .contains(&"old-run".to_owned()));

        state.project_agent_session(detail(serde_json::json!([])));

        assert_eq!(
            state.sessions.items[0].run_ids,
            ["agent-history:codex/local:thread-1"]
        );
        let messages = timeline_blocks_for_session(&state, &state.sessions.items[0])
            .into_iter()
            .filter_map(|entry| match entry.block {
                TimelineBlock::Entry(TimelineItem::Message(message)) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(messages, ["latest"]);
    }

    #[test]
    fn native_client_identity_is_idempotent_across_snapshot_and_sse() {
        let mut state = AppState::new(true);
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "active"
            },
            "turns": [{
                "turn_id": "turn-1",
                "status": "active",
                "activities": [
                    {
                        "activity_id": "native-user-a",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "one"}}],
                        "details": {"clientId": "orchestral-command:run-1:command-1:digest"}
                    },
                    {
                        "activity_id": "native-user-b",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "duplicate"}}],
                        "details": {"clientId": "orchestral-command:run-1:command-1:digest"}
                    }
                ]
            }],
            "stream_cursor": 1
        }))
        .unwrap();
        state.project_agent_session(detail);
        let run_id = "agent-history:codex/local:thread-1";
        assert_eq!(state.runs[run_id].messages.len(), 1);
        let stable_order = state.runs[run_id].messages[0].order;

        state.apply_agent_session_change(
            AgentSessionChangeView {
                connector_id: "codex/local".to_owned(),
                session_id: "thread-1".to_owned(),
                sequence: 2,
                change: AgentSessionChangeKindView::ActivityUpsert {
                    turn_id: "turn-1".to_owned(),
                    turn_status: "active".to_owned(),
                    activity: AgentSessionActivity {
                        activity_id: "native-user-c".to_owned(),
                        kind: "user_message".to_owned(),
                        status: "completed".to_owned(),
                        title: None,
                        content: vec![serde_json::json!({
                            "body": {"kind": "inline", "value": "authoritative"}
                        })],
                        details: serde_json::json!({
                            "clientId": "orchestral-command:run-1:command-1:digest"
                        }),
                    },
                },
            },
            3_000,
        );

        assert_eq!(state.runs[run_id].messages.len(), 1);
        assert_eq!(state.runs[run_id].messages[0].text, "authoritative");
        assert_eq!(state.runs[run_id].messages[0].order, stable_order);
    }

    #[test]
    fn native_client_identity_replaces_changed_activity_id_after_pagination() {
        let detail = |activity_id: &str, text: &str| {
            serde_json::from_value::<AgentSessionDetail>(serde_json::json!({
                "summary": {
                    "connector_id": "codex/local",
                    "session_id": "thread-1",
                    "state": "active"
                },
                "turns": [{
                    "turn_id": "turn-1",
                    "status": "active",
                    "activities": [{
                        "activity_id": activity_id,
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": text}}],
                        "details": {
                            "clientId": "orchestral-command:run-1:command-1:digest"
                        }
                    }]
                }]
            }))
            .unwrap()
        };
        let mut state = AppState::new(true);
        state.project_agent_session(detail("native-user-a", "optimistic echo"));
        let run_id = "agent-history:codex/local:thread-1";
        let stable_order = state.runs[run_id].messages[0].order;
        state
            .runs
            .get_mut(run_id)
            .unwrap()
            .history_pagination_started = true;

        state.project_agent_session(detail("native-user-b", "authoritative echo"));

        let messages = &state.runs[run_id].messages;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "native-user-b");
        assert_eq!(messages[0].text, "authoritative echo");
        assert_eq!(messages[0].order, stable_order);
    }

    #[test]
    fn deferred_agent_queue_item_is_not_presented_as_running() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "busy_elsewhere"
            },
            "turns": [{
                "turn_id": "deferred-queue-1",
                "status": "pending",
                "activities": [{
                    "activity_id": "deferred-user-queue-1",
                    "kind": "user_message",
                    "status": "pending",
                    "title": "Queued for owning Agent",
                    "content": [{"body": {"kind": "inline", "value": "continue"}}],
                    "details": {
                        "phase": "deferred",
                        "queue_submission_id": "queue-1",
                        "client_message_id": "client-1",
                        "queue_position": 1
                    }
                }]
            }],
            "pending_requests": [],
            "next_cursor": null
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);

        let run = &state.runs["agent-history:codex/local:thread-1"];
        assert_eq!(run.status, "delivered");
        assert_eq!(run.messages.len(), 1);
        assert!(run.messages[0].deferred);
        assert!(!run.messages[0].optimistic);
    }

    #[test]
    fn older_agent_history_is_deduplicated_and_prepended_without_clearing_latest() {
        let latest: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "idle"
            },
            "turns": [{
                "turn_id": "turn-new",
                "status": "completed",
                "activities": [
                    {
                        "activity_id": "user-new",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "new question"}}]
                    },
                    {
                        "activity_id": "agent-new",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "new answer"}}]
                    }
                ]
            }],
            "pending_requests": [{"request_id": "latest-request"}],
            "next_cursor": "activity-offset-v1:2"
        }))
        .unwrap();
        let older: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "idle"
            },
            "turns": [{
                "turn_id": "turn-old",
                "status": "completed",
                "activities": [
                    {
                        "activity_id": "user-old",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "old question"}}]
                    },
                    {
                        "activity_id": "tool-old",
                        "kind": "command",
                        "status": "completed",
                        "title": "old command",
                        "details": {"type": "command", "command": "pwd"}
                    },
                    {
                        "activity_id": "user-new",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "overlap"}}]
                    }
                ]
            }],
            "pending_requests": [{"request_id": "stale-request"}],
            "next_cursor": null
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(latest);
        state
            .runs
            .get_mut("agent-history:codex/local:thread-1")
            .unwrap()
            .history_loading_earlier = true;

        state.prepend_agent_session_history(older);

        let run = state
            .runs
            .get("agent-history:codex/local:thread-1")
            .unwrap();
        let timeline = timeline_for_run(run);
        assert_eq!(timeline.len(), 4);
        assert!(
            matches!(&timeline[0], TimelineItem::Message(message) if message.text == "old question")
        );
        assert!(
            matches!(&timeline[1], TimelineItem::Activity(activity) if activity.id == "tool-old")
        );
        assert!(
            matches!(&timeline[2], TimelineItem::Message(message) if message.text == "new question")
        );
        assert!(
            matches!(&timeline[3], TimelineItem::Message(message) if message.text == "new answer")
        );
        assert_eq!(
            run.messages
                .iter()
                .filter(|message| message.id == "user-new")
                .count(),
            1
        );
        assert_eq!(run.pending[0]["request_id"], "latest-request");
        assert!(run.history_next_cursor.is_none());
        assert!(!run.history_loading_earlier);
    }

    #[test]
    fn live_agent_changes_upsert_in_place_and_append_without_a_snapshot() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-live",
                "state": "active"
            },
            "turns": [],
            "pending_requests": [],
            "next_cursor": null
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);
        state.sessions.selected_id = Some("codex/local\0thread-live".to_owned());

        let user_change: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "codex/local",
            "session_id": "thread-live",
            "sequence": 1,
            "change": {
                "type": "activity_upsert",
                "turn_id": "turn-1",
                "turn_status": "active",
                "activity": {
                    "activity_id": "user-1",
                    "kind": "user_message",
                    "status": "completed",
                    "content": [{"body": {"kind": "inline", "value": "hello"}}]
                }
            }
        }))
        .unwrap();
        assert!(state.apply_agent_session_change(user_change.clone(), 2_000));
        let stable_timeline = state.selected_timeline_content();
        assert!(!state.apply_agent_session_change(user_change.clone(), 2_000));
        assert_eq!(state.selected_timeline_content(), stable_timeline);

        let mut edited = user_change;
        edited.sequence = 2;
        if let AgentSessionChangeKindView::ActivityUpsert { activity, .. } = &mut edited.change {
            activity.content = content("hello edited").as_array().unwrap().clone();
        }
        assert!(state.apply_agent_session_change(edited, 2_000));
        assert_ne!(state.selected_timeline_content(), stable_timeline);
        let edited_timeline = state.selected_timeline_content();

        let assistant_change: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "codex/local",
            "session_id": "thread-live",
            "sequence": 3,
            "change": {
                "type": "activity_upsert",
                "turn_id": "turn-1",
                "turn_status": "active",
                "activity": {
                    "activity_id": "assistant-1",
                    "kind": "agent_message",
                    "status": "completed",
                    "content": [{"body": {"kind": "inline", "value": "world"}}]
                }
            }
        }))
        .unwrap();
        assert!(state.apply_agent_session_change(assistant_change, 3_000));
        assert_ne!(state.selected_timeline_content(), edited_timeline);

        let completed_change: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "codex/local",
            "session_id": "thread-live",
            "sequence": 4,
            "change": {
                "type": "turn_status",
                "turn_id": "turn-1",
                "status": "completed"
            }
        }))
        .unwrap();
        state.apply_agent_session_change(completed_change, 4_000);

        let run = state
            .runs
            .get("agent-history:codex/local:thread-live")
            .unwrap();
        assert_eq!(run.messages.len(), 2);
        assert_eq!(run.messages[0].text, "hello edited");
        assert_eq!(run.messages[1].text, "world");
        assert_eq!(run.history_live_turn_starts, ["user-1"]);
        assert_eq!(run.history_latest_turn_status.as_deref(), Some("completed"));
        let session = state.selected_session().expect("session remains selected");
        assert_eq!(session.state.as_deref(), Some("idle"));
        assert_eq!(session.updated_at_unix_ms, 4_000);
    }

    #[test]
    fn live_session_request_changes_open_deduplicate_and_close_the_pending_panel() {
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "fixture/local",
                "session_id": "thread-request",
                "state": "active"
            },
            "turns": [],
            "pending_requests": []
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(detail);
        state.sessions.selected_id = Some("fixture/local\0thread-request".to_owned());
        let opened: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "fixture/local",
            "session_id": "thread-request",
            "sequence": 1,
            "change": {
                "type": "pending_request_upsert",
                "request": {
                    "request_id": "approval-1",
                    "blocking": true,
                    "payload": {"type": "approval", "reason": "write a file"}
                }
            }
        }))
        .unwrap();

        assert!(!state.apply_agent_session_change(opened.clone(), 2_000));
        assert!(!state.apply_agent_session_change(opened, 2_000));
        let history_id = "agent-history:fixture/local:thread-request";
        assert_eq!(state.runs[history_id].pending.len(), 1);
        assert_eq!(state.pending_run().unwrap().id, history_id);
        assert_eq!(
            state.selected_session().unwrap().state.as_deref(),
            Some("waiting_approval")
        );

        let input_opened: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "fixture/local",
            "session_id": "thread-request",
            "sequence": 2,
            "change": {
                "type": "pending_request_upsert",
                "request": {
                    "request_id": "input-1",
                    "blocking": true,
                    "payload": {"type": "input", "prompt": []}
                }
            }
        }))
        .unwrap();
        state.apply_agent_session_change(input_opened, 2_500);
        assert_eq!(state.runs[history_id].pending.len(), 2);
        state
            .ensure_run_source(
                "controlled-request-run",
                Some("thread-request".to_owned()),
                Some("fixture/local".to_owned()),
            )
            .pending
            .push(serde_json::json!({
                "request_id": "approval-1",
                "blocking": true,
                "payload": {"type": "approval", "reason": "write a file"}
            }));

        let closed: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "fixture/local",
            "session_id": "thread-request",
            "sequence": 3,
            "change": {
                "type": "pending_request_closed",
                "request_id": "approval-1"
            }
        }))
        .unwrap();
        assert!(!state.apply_agent_session_change(closed, 3_000));
        assert_eq!(state.runs[history_id].pending.len(), 1);
        assert!(state.runs["controlled-request-run"].pending.is_empty());
        assert_eq!(
            state.selected_session().unwrap().state.as_deref(),
            Some("waiting_input")
        );

        let input_closed: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "fixture/local",
            "session_id": "thread-request",
            "sequence": 4,
            "change": {
                "type": "pending_request_closed",
                "request_id": "input-1"
            }
        }))
        .unwrap();
        assert!(!state.apply_agent_session_change(input_closed, 4_000));
        assert!(state.runs[history_id].pending.is_empty());
        assert_eq!(
            state.selected_session().unwrap().state.as_deref(),
            Some("active")
        );
    }

    #[test]
    fn authoritative_agent_snapshot_drops_stale_controlled_run_and_exposes_pending() {
        let history_id = "agent-history:codex/local:thread-1";
        let mut state = AppState::new(true);
        state.sessions.items.push(SessionView {
            id: "thread-1".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            run_ids: vec![history_id.to_owned(), "controlled-run".to_owned()],
            connector_id: Some("codex/local".to_owned()),
            title: Some("Old title".to_owned()),
            preview: None,
            cwd: None,
            state: Some("active".to_owned()),
            execution_profile: Default::default(),
        });
        state.sessions.selected_id = Some("codex/local\0thread-1".to_owned());
        state
            .ensure_run_source(
                "controlled-run",
                Some("thread-1".to_owned()),
                Some("codex/local".to_owned()),
            )
            .status = "delivered".to_owned();
        let detail: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "title": "Fresh title",
                "preview": "Fresh preview",
                "updated_at_unix_ms": 42,
                "state": "active"
            },
            "turns": [],
            "pending_requests": [{
                "request_id": "approval-1",
                "payload": {"type": "approval"}
            }],
            "next_cursor": null
        }))
        .unwrap();

        state.project_agent_session(detail);

        let session = state.selected_session().unwrap();
        assert_eq!(session.title.as_deref(), Some("Fresh title"));
        assert_eq!(session.preview.as_deref(), Some("Fresh preview"));
        assert_eq!(session.updated_at_unix_ms, 42);
        assert_eq!(session.state.as_deref(), Some("waiting_approval"));
        assert!(!session.run_ids.iter().any(|run| run == "controlled-run"));
        assert_eq!(
            state.pending_run().map(|run| run.id.as_str()),
            Some(history_id)
        );
    }

    #[test]
    fn polling_latest_agent_history_preserves_older_pages_and_pagination_state() {
        let latest: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "active"
            },
            "turns": [{
                "turn_id": "latest",
                "status": "running",
                "activities": [
                    {
                        "activity_id": "agent-edge",
                        "kind": "agent_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "edge of latest window"}}]
                    },
                    {
                        "activity_id": "user-new",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "new question"}}]
                    }
                ]
            }],
            "pending_requests": [],
            "next_cursor": "cursor-1"
        }))
        .unwrap();
        let older: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "active"
            },
            "turns": [{
                "turn_id": "older",
                "status": "completed",
                "activities": [{
                    "activity_id": "user-old",
                    "kind": "user_message",
                    "status": "completed",
                    "content": [{"body": {"kind": "inline", "value": "old question"}}]
                }]
            }],
            "pending_requests": [],
            "next_cursor": "cursor-2"
        }))
        .unwrap();
        let refreshed: AgentSessionDetail = serde_json::from_value(serde_json::json!({
            "summary": {
                "connector_id": "codex/local",
                "session_id": "thread-1",
                "state": "waiting_input"
            },
            "turns": [{
                "turn_id": "latest",
                "status": "running",
                "activities": [
                    {
                        "activity_id": "user-new",
                        "kind": "user_message",
                        "status": "completed",
                        "content": [{"body": {"kind": "inline", "value": "new question updated"}}]
                    },
                    {
                        "activity_id": "agent-new",
                        "kind": "agent_message",
                        "status": "running",
                        "content": [{"body": {"kind": "inline", "value": "working"}}]
                    }
                ]
            }],
            "pending_requests": [{"request_id": "input-1"}],
            "next_cursor": "cursor-1"
        }))
        .unwrap();
        let mut state = AppState::new(true);
        state.project_agent_session(latest);
        state.prepend_agent_session_history(older);
        state
            .runs
            .get_mut("agent-history:codex/local:thread-1")
            .unwrap()
            .history_loading_earlier = true;

        state.project_agent_session(refreshed);

        let run = state
            .runs
            .get("agent-history:codex/local:thread-1")
            .unwrap();
        let timeline = timeline_for_run(run);
        assert_eq!(timeline.len(), 4);
        assert!(
            matches!(&timeline[0], TimelineItem::Message(message) if message.text == "old question")
        );
        assert!(
            matches!(&timeline[1], TimelineItem::Message(message) if message.text == "edge of latest window")
        );
        assert!(
            matches!(&timeline[2], TimelineItem::Message(message) if message.text == "new question updated")
        );
        assert!(
            matches!(&timeline[3], TimelineItem::Message(message) if message.text == "working")
        );
        assert_eq!(run.history_next_cursor.as_deref(), Some("cursor-2"));
        assert!(run.history_loading_earlier);
        assert_eq!(run.pending[0]["request_id"], "input-1");
    }

    #[test]
    fn an_old_notice_timeout_cannot_dismiss_a_newer_error() {
        let mut ui = UiState::default();
        ui.show_notice(Notice {
            message: "first".to_owned(),
            tone: "error".to_owned(),
            id: 1,
        });
        ui.show_notice(Notice {
            message: "second".to_owned(),
            tone: "error".to_owned(),
            id: 2,
        });

        assert!(!ui.dismiss_notice(1));
        assert_eq!(ui.notice.as_ref().map(|notice| notice.id), Some(2));
        assert!(ui.dismiss_notice(2));
        assert!(ui.notice.is_none());
    }

    #[test]
    fn newer_run_snapshot_survives_stale_reads_and_older_journal_replay() {
        let mut run = RunState::new("run-sync".to_owned(), None);
        let approval = serde_json::json!({"request_id": "approve", "blocking": true,
            "payload": {"type": "approval", "reason": "write"}});
        run.apply_view(
            serde_json::json!({"last_run_seq": 10, "state": {"state": "waiting"},
            "pending_requests": [approval.clone()]}),
            10.0,
        );
        run.apply_view(
            serde_json::json!({"last_run_seq": 5, "state": {"state": "running"},
            "pending_requests": []}),
            11.0,
        );
        assert_eq!(run.status, "waiting");
        assert_eq!(run.pending, vec![approval.clone()]);
        run.project_durable(
            &record(1, "started", serde_json::json!({"type": "run_started"})),
            12.0,
        );
        assert_eq!(
            run.cursor, 1,
            "historical replay still advances the transcript"
        );
        assert_eq!(run.status, "waiting");
        assert_eq!(run.pending, vec![approval]);
        run.project_durable(
            &record(
                2,
                "closed",
                serde_json::json!({"type": "request_closed", "request_id": "approve"}),
            ),
            13.0,
        );
        assert_eq!(
            run.pending.len(),
            1,
            "old close cannot hide a newer pending snapshot"
        );
    }

    #[test]
    fn steer_http_ack_after_durable_event_does_not_duplicate_the_message() {
        let mut run = RunState::new("run-sync".to_owned(), None);
        run.project_durable(&record(1, "command", serde_json::json!({"type": "command_received",
            "command": {"command_id": "submit-1", "payload": {"type": "steer", "content": content("continue")}}
        })), 1.0);
        let before = run.messages.clone();
        run.optimistic_steer(
            "steer-submit-1".to_owned(),
            "continue".to_owned(),
            2.0,
            None,
        );
        assert_eq!(run.messages, before);
    }

    #[test]
    fn pending_panel_unions_native_and_controlled_requests_by_identity() {
        let mut state = AppState::new(true);
        state.connectors.items = serde_json::from_value(serde_json::json!([{
            "connector_id": "fixture/local", "display_name": "Fixture", "agent_family": "fixture",
            "capabilities": {"list": true, "read": true, "create": false, "resolve_requests": true}
        }]))
        .unwrap();
        state.project_agent_session(agent_detail_at(1, "message", "hello"));
        let session = &mut state.sessions.items[0];
        state.sessions.selected_id = Some(session.key());
        session.run_ids.push("controlled".to_owned());
        let duplicate = serde_json::json!({"request_id": "same", "payload": {"type": "approval"}});
        let native = serde_json::json!({"request_id": "child-input", "payload": {"type": "input"}});
        state
            .runs
            .get_mut("agent-history:fixture/local:thread-1")
            .unwrap()
            .pending = vec![duplicate.clone(), native];
        let run = state.ensure_run_source(
            "controlled",
            Some("thread-1".to_owned()),
            Some("fixture/local".to_owned()),
        );
        run.status = "waiting".to_owned();
        run.pending = vec![duplicate];
        let requests = state.pending_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].0.id, "agent-history:fixture/local:thread-1");
        assert_eq!(requests[1].1["request_id"], "child-input");
        state.connectors.items[0].capabilities.resolve_requests = false;
        assert_eq!(state.pending_requests()[0].0.id, "controlled");
    }

    #[test]
    fn snapshot_rebases_live_suffix_without_losing_a_pending_request() {
        let mut state = AppState::new(true);
        state.project_agent_session(agent_detail_at(10, "old", "old"));
        let change: AgentSessionChangeView = serde_json::from_value(serde_json::json!({
            "connector_id": "fixture/local", "session_id": "thread-1", "sequence": 11,
            "change": {"type": "activity_upsert", "turn_id": "turn-1", "turn_status": "active",
                "activity": {"activity_id": "live", "kind": "agent_message", "status": "completed", "content": content("live")}}
        })).unwrap();
        state.apply_agent_session_change(change.clone(), 11);
        let mut snapshot = agent_detail_at(10, "old", "old");
        snapshot.pending_requests =
            vec![serde_json::json!({"request_id": "approval", "payload": {"type": "approval"}})];
        state.project_agent_session(snapshot);
        let history = &state.runs["agent-history:fixture/local:thread-1"];
        assert_eq!(history.pending.len(), 1);
        assert_eq!(
            history
                .messages
                .iter()
                .map(|m| m.text.as_str())
                .collect::<Vec<_>>(),
            ["old", "live"]
        );
        assert_eq!(state.sessions.stream_cursors["fixture/local\0thread-1"], 11);
        let before = state.clone();
        state.apply_agent_session_change(change, 12);
        assert_eq!(
            state, before,
            "replayed SSE events cannot mutate the live edge again"
        );
    }

    #[test]
    fn paginated_snapshot_inserts_missing_middle_items_in_native_order() {
        let mut state = AppState::new(true);
        let mut first = agent_detail_at(1, "a", "A");
        let c = agent_detail_at(1, "c", "C")
            .turns
            .remove(0)
            .activities
            .remove(0);
        first.turns[0].activities.push(c.clone());
        state.project_agent_session(first);
        state.prepend_agent_session_history(agent_detail_at(0, "prefix", "older"));
        let mut latest = agent_detail_at(2, "a", "A");
        latest.turns[0].activities.push(
            agent_detail_at(2, "b", "B")
                .turns
                .remove(0)
                .activities
                .remove(0),
        );
        latest.turns[0].activities.push(c);
        state.project_agent_session(latest);
        let history = &state.runs["agent-history:fixture/local:thread-1"];
        let messages = timeline_for_run(history)
            .into_iter()
            .filter_map(|item| match item {
                TimelineItem::Message(message) => Some(message.text),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(messages, ["older", "A", "B", "C"]);
    }
}
