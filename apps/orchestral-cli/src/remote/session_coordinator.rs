use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orchestral_core::agent_connector::{
    AgentConnectorId, AgentSessionChange, AgentSessionChangeKind,
};
use orchestral_core::agent_protocol::wire::AgentSessionId;
use orchestral_runtime::{AgentDirectory, AgentDirectoryError};
use tokio::sync::broadcast;

const SESSION_REPLAY_CAPACITY: usize = 256;
const SOURCE_RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const SOURCE_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Process-level authority for one native Agent session.
///
/// Browser connections are interchangeable subscribers and command producers;
/// they never own the native subscription or the command serialization lock.
pub(super) struct AgentSessionCoordinator {
    operation: tokio::sync::Mutex<()>,
    hub: tokio::sync::Mutex<Option<Arc<AgentSessionChangeHub>>>,
}

impl AgentSessionCoordinator {
    fn new() -> Self {
        Self {
            operation: tokio::sync::Mutex::new(()),
            hub: tokio::sync::Mutex::new(None),
        }
    }

    pub(super) fn operation(&self) -> &tokio::sync::Mutex<()> {
        &self.operation
    }

    pub(super) async fn ensure_hub(
        &self,
        directory: Arc<AgentDirectory>,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
    ) -> Result<Arc<AgentSessionChangeHub>, AgentDirectoryError> {
        let mut slot = self.hub.lock().await;
        if let Some(hub) = slot.as_ref() {
            return Ok(hub.clone());
        }

        // This lock is scoped to one native session. Concurrent tabs opening
        // different sessions do not block each other, while the same session
        // establishes exactly one connector subscription.
        let source = directory
            .subscribe_session_changes(connector_id, session_id)
            .await?;
        let hub = AgentSessionChangeHub::start(
            directory,
            connector_id.clone(),
            session_id.clone(),
            source,
        );
        tracing::info!(
            connector_id = %connector_id.as_str(),
            session_id = %session_id.as_str(),
            "established shared Agent session Hub"
        );
        *slot = Some(hub.clone());
        Ok(hub)
    }
}

#[derive(Default)]
pub(super) struct AgentSessionCoordinatorRegistry {
    sessions: Mutex<BTreeMap<String, Arc<AgentSessionCoordinator>>>,
}

impl AgentSessionCoordinatorRegistry {
    pub(super) fn get(
        &self,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
    ) -> Arc<AgentSessionCoordinator> {
        let key = format!("{}\0{}", connector_id.as_str(), session_id.as_str());
        let mut sessions = self
            .sessions
            .lock()
            .expect("Agent session coordinator registry lock poisoned");
        sessions
            .entry(key)
            .or_insert_with(|| Arc::new(AgentSessionCoordinator::new()))
            .clone()
    }
}

struct AgentSessionChangeHubState {
    latest_sequence: u64,
    replay: VecDeque<AgentSessionChange>,
}

/// A single canonical event sequence fan-outs one native subscription to all
/// devices and tabs observing the same session.
pub(super) struct AgentSessionChangeHub {
    connector_id: AgentConnectorId,
    session_id: AgentSessionId,
    sender: broadcast::Sender<AgentSessionChange>,
    state: Mutex<AgentSessionChangeHubState>,
}

pub(super) struct AgentSessionChangeSubscription {
    pub(super) replay: Vec<AgentSessionChange>,
    pub(super) live: broadcast::Receiver<AgentSessionChange>,
}

impl AgentSessionChangeHub {
    fn start(
        directory: Arc<AgentDirectory>,
        connector_id: AgentConnectorId,
        session_id: AgentSessionId,
        source: broadcast::Receiver<AgentSessionChange>,
    ) -> Arc<Self> {
        let (sender, _) = broadcast::channel(SESSION_REPLAY_CAPACITY);
        let hub = Arc::new(Self {
            connector_id,
            session_id,
            sender,
            state: Mutex::new(AgentSessionChangeHubState {
                latest_sequence: 0,
                replay: VecDeque::with_capacity(SESSION_REPLAY_CAPACITY),
            }),
        });
        tokio::spawn(Self::forward_source(hub.clone(), directory, source));
        hub
    }

    pub(super) fn cursor(&self) -> u64 {
        self.state
            .lock()
            .expect("Agent session Hub state lock poisoned")
            .latest_sequence
    }

    pub(super) fn subscribe(&self, after: u64) -> AgentSessionChangeSubscription {
        // Publishing also holds this state lock through broadcast::send. The
        // receiver and replay snapshot therefore form one atomic cut: an event
        // is either in replay or queued on the live receiver, never lost in
        // between.
        let state = self
            .state
            .lock()
            .expect("Agent session Hub state lock poisoned");
        let live = self.sender.subscribe();
        let latest = state.latest_sequence;
        let replay = if after > latest {
            vec![self.refresh_change(0, "host_sequence_reset")]
        } else if after == latest {
            Vec::new()
        } else {
            let first_available = state
                .replay
                .front()
                .map(|change| change.sequence)
                .unwrap_or_else(|| latest.saturating_add(1));
            if after.saturating_add(1) < first_available {
                vec![self.refresh_change(latest, "session_replay_gap")]
            } else {
                state
                    .replay
                    .iter()
                    .filter(|change| change.sequence > after)
                    .cloned()
                    .collect()
            }
        };
        tracing::debug!(
            connector_id = %self.connector_id.as_str(),
            session_id = %self.session_id.as_str(),
            after,
            latest,
            replayed = replay.len(),
            subscribers = self.sender.receiver_count(),
            "subscribed client to shared Agent session Hub"
        );
        AgentSessionChangeSubscription { replay, live }
    }

    fn publish(&self, change: AgentSessionChangeKind) {
        let mut state = self
            .state
            .lock()
            .expect("Agent session Hub state lock poisoned");
        state.latest_sequence = state.latest_sequence.saturating_add(1);
        let change = AgentSessionChange {
            connector_id: self.connector_id.clone(),
            session_id: self.session_id.clone(),
            sequence: state.latest_sequence,
            change,
        };
        if state.replay.len() == SESSION_REPLAY_CAPACITY {
            state.replay.pop_front();
        }
        state.replay.push_back(change.clone());
        // A Hub may legitimately have no browser subscribers. Retaining the
        // canonical replay is sufficient; absence of receivers is not a reason
        // to tear down the native Agent subscription.
        let _ = self.sender.send(change);
    }

    fn refresh_change(&self, sequence: u64, reason: &str) -> AgentSessionChange {
        AgentSessionChange {
            connector_id: self.connector_id.clone(),
            session_id: self.session_id.clone(),
            sequence,
            change: AgentSessionChangeKind::RefreshRequired {
                reason: reason.to_owned(),
            },
        }
    }

    async fn forward_source(
        hub: Arc<Self>,
        directory: Arc<AgentDirectory>,
        mut source: broadcast::Receiver<AgentSessionChange>,
    ) {
        let mut reconnect_failures = 0_u32;
        loop {
            match source.recv().await {
                Ok(change)
                    if change.connector_id == hub.connector_id
                        && change.session_id == hub.session_id =>
                {
                    reconnect_failures = 0;
                    hub.publish(change.change);
                }
                Ok(change) => {
                    tracing::warn!(
                        connector_id = %hub.connector_id.as_str(),
                        session_id = %hub.session_id.as_str(),
                        source_connector_id = %change.connector_id.as_str(),
                        source_session_id = %change.session_id.as_str(),
                        "ignored Agent session change routed to the wrong Hub"
                    );
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        connector_id = %hub.connector_id.as_str(),
                        session_id = %hub.session_id.as_str(),
                        skipped,
                        "Agent session source lagged; clients will reconcile one snapshot"
                    );
                    hub.publish(AgentSessionChangeKind::RefreshRequired {
                        reason: "native_subscription_gap".to_owned(),
                    });
                }
                Err(broadcast::error::RecvError::Closed) => loop {
                    reconnect_failures = reconnect_failures.saturating_add(1);
                    tokio::time::sleep(source_reconnect_backoff(reconnect_failures)).await;
                    match directory
                        .subscribe_session_changes(&hub.connector_id, &hub.session_id)
                        .await
                    {
                        Ok(next) => {
                            source = next;
                            reconnect_failures = 0;
                            // Notifications may have been missed while the
                            // connector source was down. Reconcile once after
                            // reattachment, then resume incremental updates.
                            hub.publish(AgentSessionChangeKind::RefreshRequired {
                                reason: "native_subscription_reconnected".to_owned(),
                            });
                            tracing::info!(
                                connector_id = %hub.connector_id.as_str(),
                                session_id = %hub.session_id.as_str(),
                                "reconnected shared Agent session Hub source"
                            );
                            break;
                        }
                        Err(error) => {
                            tracing::warn!(
                                connector_id = %hub.connector_id.as_str(),
                                session_id = %hub.session_id.as_str(),
                                reconnect_failures,
                                %error,
                                "could not reconnect Agent session source"
                            );
                        }
                    }
                },
            }
        }
    }
}

fn source_reconnect_backoff(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(5);
    SOURCE_RECONNECT_INITIAL_BACKOFF
        .saturating_mul(2_u32.saturating_pow(exponent))
        .min(SOURCE_RECONNECT_MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(sequence: u64, title: &str) -> AgentSessionChange {
        AgentSessionChange {
            connector_id: AgentConnectorId::new("fixture/local"),
            session_id: AgentSessionId::new("fixture-session"),
            sequence,
            change: AgentSessionChangeKind::RefreshRequired {
                reason: title.to_owned(),
            },
        }
    }

    fn hub() -> AgentSessionChangeHub {
        let (sender, _) = broadcast::channel(SESSION_REPLAY_CAPACITY);
        AgentSessionChangeHub {
            connector_id: AgentConnectorId::new("fixture/local"),
            session_id: AgentSessionId::new("fixture-session"),
            sender,
            state: Mutex::new(AgentSessionChangeHubState {
                latest_sequence: 0,
                replay: VecDeque::new(),
            }),
        }
    }

    #[test]
    fn subscribers_share_one_sequence_and_replay_from_their_own_cursor() {
        let hub = hub();
        hub.publish(change(91, "first").change);
        hub.publish(change(3, "second").change);

        let first = hub.subscribe(0);
        let second = hub.subscribe(1);
        assert_eq!(
            first
                .replay
                .iter()
                .map(|change| change.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            second
                .replay
                .iter()
                .map(|change| change.sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn a_cursor_from_a_previous_host_epoch_gets_an_explicit_reset_barrier() {
        let hub = hub();
        hub.publish(change(1, "current").change);
        let subscription = hub.subscribe(99);
        assert_eq!(subscription.replay.len(), 1);
        assert_eq!(subscription.replay[0].sequence, 0);
        assert!(matches!(
            subscription.replay[0].change,
            AgentSessionChangeKind::RefreshRequired { ref reason }
                if reason == "host_sequence_reset"
        ));
    }
}
