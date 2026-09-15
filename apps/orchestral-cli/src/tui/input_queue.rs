//! The visible queue is projected from durable command acceptance/consumption.
use std::collections::{BTreeMap, BTreeSet};

use orchestral_core::agent_protocol::wire::{
    AgentCommand, AgentEvent, AgentJournalRecord, CommandId, ProviderCommandOutcome,
    QueuedInputOperation,
};

use super::menu::{Choice, LocalAction, Menu, MenuKind};
use super::state::{UiEffect, UiState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueuedMessage {
    pub id: String,
    pub run_id: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct InputQueueState {
    pub pending: Vec<QueuedMessage>,
    pub editing: Option<String>,
    pub saved_draft: Option<(String, usize)>,
    commands: BTreeMap<String, (QueuedInputOperation, QueuedMessage)>,
    accepted: BTreeSet<String>,
}

impl InputQueueState {
    /// Returns an input only when the runtime commits it at a model boundary.
    pub fn observe(&mut self, record: &AgentJournalRecord) -> Option<QueuedMessage> {
        match &record.event.payload {
            AgentEvent::CommandReceived { command } => {
                if let (Ok(Some(operation)), AgentCommand::Steer { content }) = (
                    QueuedInputOperation::from_command(command),
                    &command.payload,
                ) {
                    self.commands
                        .entry(command.command_id.as_str().to_owned())
                        .or_insert_with(|| {
                            (
                                operation,
                                QueuedMessage {
                                    id: command.command_id.as_str().to_owned(),
                                    run_id: command.run_id.as_str().to_owned(),
                                    text: super::app::display_contents(content),
                                },
                            )
                        });
                }
            }
            AgentEvent::CommandDispositionRecorded {
                command_id,
                outcome: ProviderCommandOutcome::Accepted,
            } => {
                if self.accepted.insert(command_id.as_str().to_owned()) {
                    if let Some((operation, message)) =
                        self.commands.get(command_id.as_str()).cloned()
                    {
                        match operation {
                            QueuedInputOperation::Enqueue => self.pending.push(message),
                            QueuedInputOperation::Replace { target } => {
                                if let Some(queued) = self
                                    .pending
                                    .iter_mut()
                                    .find(|queued| queued.id == target.as_str())
                                {
                                    *queued = message;
                                }
                            }
                            QueuedInputOperation::Withdraw { target } => {
                                self.pending.retain(|queued| queued.id != target.as_str())
                            }
                        }
                    }
                }
            }
            AgentEvent::InputCommitted { .. } => {
                if let Some(index) = record.event.causation_id.as_ref().and_then(|id| {
                    self.pending
                        .iter()
                        .position(|message| message.id == id.as_str())
                }) {
                    return Some(self.pending.remove(index));
                }
            }
            _ => {}
        }
        None
    }
}

pub(crate) fn command_id() -> String {
    format!("tui-queue-{}", uuid::Uuid::new_v4())
}

pub(crate) fn is_active(state: &UiState, message: &QueuedMessage) -> bool {
    state.run_id.as_deref() == Some(message.run_id.as_str())
        && matches!(
            state.phase,
            super::state::UiPhase::Running
                | super::state::UiPhase::WaitingInput
                | super::state::UiPhase::WaitingApproval
        )
}

pub(crate) fn menu(state: &UiState) -> Menu {
    let mut choices = Vec::new();
    for (index, message) in state.input_queue.pending.iter().enumerate() {
        let preview = message
            .text
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let active = is_active(state, message);
        choices.push(Choice::action(
            format!(
                "{}  {}  {}",
                index + 1,
                if active { "Queued" } else { "Unsent" },
                preview
            ),
            LocalAction::EditQueuedInput(message.id.clone()),
            if active {
                "Edit this pending message; it may be received while you are editing"
            } else {
                "Copy this unsent message to the composer"
            },
        ));
        if active {
            choices.push(Choice::action(
                format!("Withdraw message {}", index + 1),
                LocalAction::WithdrawQueuedInput(message.id.clone()),
                "Remove this message if the Agent has not received it yet",
            ));
        }
    }
    Menu::new(
        MenuKind::Commands,
        "Pending messages · select an action",
        choices,
    )
}

pub(crate) fn route_effect(effect: &UiEffect, state: &mut UiState) -> Option<Vec<UiEffect>> {
    match effect {
        UiEffect::HostCommand { command } if command == "/queue" => {
            state.menu = Some(menu(state));
            Some(Vec::new())
        }
        UiEffect::MenuChoice {
            kind: MenuKind::Commands,
            value,
        } if value == "/queue" => {
            state.menu = Some(menu(state));
            Some(Vec::new())
        }
        UiEffect::LocalAction {
            action: LocalAction::EditQueuedInput(id),
            ..
        } => {
            let message = state
                .input_queue
                .pending
                .iter()
                .find(|message| &message.id == id)
                .cloned();
            if let Some(message) = message {
                let active = is_active(state, &message);
                if active {
                    state
                        .input_queue
                        .saved_draft
                        .get_or_insert_with(|| (state.composer.clone(), state.composer_cursor));
                } else {
                    detach_edit(state);
                    state.input_history.push(&state.composer);
                }
                state.composer = message.text;
                state.composer_cursor = state.composer.len();
                state.input_queue.editing = active.then_some(message.id);
                state.menu = None;
            } else {
                state.ui_notice = Some("This message is no longer queued".to_owned());
            }
            Some(Vec::new())
        }
        UiEffect::LocalAction {
            action: LocalAction::WithdrawQueuedInput(id),
            ..
        } => {
            state.menu = None;
            Some(
                state
                    .input_queue
                    .pending
                    .iter()
                    .find(|message| &message.id == id && is_active(state, message))
                    .map(|message| {
                        vec![UiEffect::QueueInput {
                            run_id: message.run_id.clone(),
                            command_id: command_id(),
                            input: message.text.clone(),
                            operation: QueuedInputOperation::Withdraw {
                                target: CommandId::new(id),
                            },
                        }]
                    })
                    .unwrap_or_default(),
            )
        }
        _ => None,
    }
}

pub(crate) fn finish_edit(state: &mut UiState) {
    state.input_queue.editing = None;
    if let Some((draft, cursor)) = state.input_queue.saved_draft.take() {
        state.composer = draft;
        state.composer_cursor = cursor;
    }
}

/// Preserve both the edit and the previous draft after consumption or rejection.
pub(crate) fn detach_edit(state: &mut UiState) {
    state.input_queue.editing = None;
    if let Some((draft, _)) = state.input_queue.saved_draft.take() {
        state.input_history.push(&draft);
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::{update, UiMsg, UiPhase};
    use super::*;
    use orchestral_core::agent_protocol::wire::{
        AgentEventAuthority, AgentEventEnvelope, AgentEventId, Digest, RunId,
    };

    fn record(payload: AgentEvent, cause: Option<&str>) -> AgentJournalRecord {
        let mut event = AgentEventEnvelope {
            event_id: AgentEventId::new("event"),
            run_id: RunId::new("run"),
            run_seq: 1,
            causation_id: cause.map(CommandId::new),
            source_fingerprint: None,
            event_digest: Digest::sha256([]),
            payload,
        };
        event.event_digest = event.computed_digest().unwrap();
        AgentJournalRecord {
            authority: AgentEventAuthority::Provider,
            draft_digest: event.computed_draft_digest().unwrap(),
            event,
        }
    }

    fn accept(queue: &mut InputQueueState, id: &str, operation: QueuedInputOperation, text: &str) {
        let command = operation
            .command(
                CommandId::new(id),
                RunId::new("run"),
                vec![orchestral_core::agent_protocol::wire::Content::text(text)],
            )
            .unwrap();
        queue.observe(&record(AgentEvent::CommandReceived { command }, None));
        let accepted = record(
            AgentEvent::CommandDispositionRecorded {
                command_id: CommandId::new(id),
                outcome: ProviderCommandOutcome::Accepted,
            },
            None,
        );
        queue.observe(&accepted);
        queue.observe(&accepted);
    }

    #[test]
    fn durable_queue_replay_deduplicates_acceptance_and_marks_only_consumed_input_received() {
        let mut queue = InputQueueState::default();
        accept(&mut queue, "a", QueuedInputOperation::Enqueue, "first");
        accept(&mut queue, "b", QueuedInputOperation::Enqueue, "second");
        accept(
            &mut queue,
            "edit",
            QueuedInputOperation::Replace {
                target: CommandId::new("a"),
            },
            "edited",
        );
        accept(
            &mut queue,
            "withdraw",
            QueuedInputOperation::Withdraw {
                target: CommandId::new("b"),
            },
            "second",
        );
        assert_eq!(queue.pending.len(), 1);
        assert_eq!(queue.pending[0].text, "edited");
        let consumed = record(
            AgentEvent::InputCommitted {
                content: vec![orchestral_core::agent_protocol::wire::Content::text(
                    "edited",
                )],
            },
            Some("edit"),
        );
        assert_eq!(queue.observe(&consumed).unwrap().text, "edited");
        assert!(queue.observe(&consumed).is_none());
        assert!(queue.pending.is_empty());
    }

    #[test]
    fn queue_edit_cancel_restores_draft_and_consumption_preserves_both_drafts() {
        let mut state = UiState::new("session", "model");
        update(
            &mut state,
            UiMsg::RunStarted {
                run_id: "run".into(),
            },
        );
        accept(
            &mut state.input_queue,
            "a",
            QueuedInputOperation::Enqueue,
            "queued text",
        );
        state.composer = "previous draft".into();
        state.composer_cursor = state.composer.len();
        let edit = UiEffect::LocalAction {
            action: LocalAction::EditQueuedInput("a".into()),
            return_to: None,
        };
        route_effect(&edit, &mut state).unwrap();
        assert_eq!(state.composer, "queued text");
        update(&mut state, UiMsg::Escape);
        assert_eq!(state.composer, "previous draft");
        route_effect(&edit, &mut state).unwrap();
        update(&mut state, UiMsg::InsertText(" revised".into()));
        detach_edit(&mut state);
        assert_eq!(state.composer, "queued text revised");
        state
            .input_history
            .navigate(&mut state.composer, &mut state.composer_cursor, true);
        assert_eq!(state.composer, "previous draft");
        state
            .input_history
            .navigate(&mut state.composer, &mut state.composer_cursor, false);
        assert_eq!(state.composer, "queued text revised");
        state.phase = UiPhase::Failed;
        state.run_id = None;
        assert!(!is_active(&state, &state.input_queue.pending[0]));
        assert_eq!(
            menu(&state).choices.len(),
            1,
            "unsent input can be copied, not edited in a stopped Run"
        );
    }

    #[test]
    fn immediate_send_preserves_startup_drafts_and_respects_commands_and_focus() {
        let mut state = UiState::new("session", "model");
        state.phase = UiPhase::Running;
        state.composer = "wait for Run identity".into();
        state.composer_cursor = state.composer.len();
        assert!(update(&mut state, UiMsg::SubmitImmediately).is_empty());
        assert_eq!(state.composer, "wait for Run identity");
        state.run_id = Some("run".into());
        state.composer = "/queue".into();
        state.composer_cursor = state.composer.len();
        assert!(
            matches!(update(&mut state, UiMsg::SubmitImmediately).as_slice(), [UiEffect::HostCommand { command }] if command == "/queue")
        );
        state.composer = "preserved behind menu".into();
        state.composer_cursor = state.composer.len();
        state.menu = Some(menu(&state));
        assert!(update(&mut state, UiMsg::SubmitImmediately).is_empty());
        assert_eq!(state.composer, "preserved behind menu");
    }
}
