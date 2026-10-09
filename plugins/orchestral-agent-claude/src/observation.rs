//! Observe the native main-session journal without attaching another execution
//! owner. Filesystem metadata bounds idle work; changed records are projected
//! into stable, provider-neutral activity and approval events.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use orchestral_core::agent_connector::{
    AgentConnector, AgentConnectorError, AgentConnectorErrorCode, AgentConnectorId,
    AgentSessionChange, AgentSessionChangeKind, AgentSessionDetail, AgentSessionState,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, PendingRequest};
use tokio::sync::broadcast;

use crate::{ClaudeCodeConfig, ClaudeCodeConnector, NativeSession, CONNECTOR_ID};

const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(PartialEq)]
struct Stamp {
    files: BTreeMap<PathBuf, (u64, Option<SystemTime>)>,
    owners: Vec<NativeSession>,
    state: Option<AgentSessionState>,
    pending: Vec<PendingRequest>,
}

fn stamp(
    config: &ClaudeCodeConfig,
    state: &std::sync::Mutex<crate::provider::ProviderState>,
    approvals: &crate::approvals::NativeApprovals,
    session: &AgentSessionId,
) -> Result<Stamp, AgentConnectorError> {
    let mut files = BTreeMap::new();
    let mut paths = vec![config
        .session_store_dir
        .join(format!("{}.json", session.as_str()))];
    for project in crate::entries(&config.config_dir.join("projects"))? {
        if project.is_dir() {
            paths.push(project.join(format!("{}.jsonl", session.as_str())));
        }
    }
    paths.extend(crate::peer::dispatch_paths(config, session)?);
    for path in paths {
        match path.metadata() {
            Ok(metadata) => {
                files.insert(path, (metadata.len(), metadata.modified().ok()));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(crate::io_error(error)),
        }
    }
    Ok(Stamp {
        files,
        owners: crate::native_sessions(&config.config_dir)?
            .into_iter()
            .filter(|owner| owner.session_id == session.as_str())
            .collect(),
        state: crate::lock(state).session_state(session),
        pending: approvals.pending(session),
    })
}

pub(crate) async fn subscribe(
    connector: &ClaudeCodeConnector,
    session: &AgentSessionId,
) -> Result<broadcast::Receiver<AgentSessionChange>, AgentConnectorError> {
    crate::validate_session_id(session.as_str())?;
    let config = connector.config.clone();
    let state = connector.provider_state.clone();
    let approvals = connector.native_approvals.clone();
    let watched = session.clone();
    // Capture the baseline before the initial snapshot so an append during
    // snapshot loading is seen on the first tick, rather than lost in a gap.
    let mut previous_stamp =
        tokio::task::spawn_blocking(move || stamp(&config, &state, &approvals, &watched))
            .await
            .map_err(|error| AgentConnectorError::protocol(error.to_string()))??;
    let mut previous = connector.read_session(session).await?;
    let config = connector.config.clone();
    let state = connector.provider_state.clone();
    let approvals = connector.native_approvals.clone();
    let cache = connector.history_cache.clone();
    let session = session.clone();
    let (sender, receiver) = broadcast::channel(64);
    tokio::spawn(async move {
        let mut sequence = 0_u64;
        loop {
            if sender.receiver_count() == 0 {
                return;
            }
            let config = config.clone();
            let state = state.clone();
            let approvals = approvals.clone();
            let cache = cache.clone();
            let watched = session.clone();
            let result = tokio::task::spawn_blocking(move || {
                let next_stamp = stamp(&config, &state, &approvals, &watched)?;
                if next_stamp == previous_stamp {
                    return Ok((next_stamp, None));
                }
                let mut detail = crate::scan(&config, &cache, Some(&watched))?
                    .into_iter()
                    .find(|detail| detail.summary.session_id == watched)
                    .ok_or_else(|| {
                        AgentConnectorError::new(
                            AgentConnectorErrorCode::NotFound,
                            "Claude session no longer exists",
                            false,
                        )
                    })?;
                if let Some(state) = next_stamp
                    .state
                    .filter(|state| *state != AgentSessionState::Idle)
                {
                    detail.summary.state = state;
                    detail.summary.input_action = None;
                }
                detail.pending_requests = next_stamp.pending.clone();
                if !detail.pending_requests.is_empty() {
                    detail.summary.state = AgentSessionState::WaitingApproval;
                }
                detail.validate_for(&AgentConnectorId::new(CONNECTOR_ID))?;
                Ok::<_, AgentConnectorError>((next_stamp, Some(detail)))
            })
            .await;
            let Ok(Ok((next_stamp, detail))) = result else {
                // Closing lets the Host hub reattach with its bounded backoff
                // and publish an explicit refresh barrier after recovery.
                return;
            };
            previous_stamp = next_stamp;
            if let Some(detail) = detail {
                for change in changes(&previous, &detail) {
                    sequence += 1;
                    if sender
                        .send(AgentSessionChange {
                            connector_id: AgentConnectorId::new(CONNECTOR_ID),
                            session_id: session.clone(),
                            sequence,
                            change,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                previous = detail;
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    });
    Ok(receiver)
}

fn changes(
    previous: &AgentSessionDetail,
    next: &AgentSessionDetail,
) -> Vec<AgentSessionChangeKind> {
    let mut changes = Vec::new();
    let old_turns: BTreeMap<_, _> = previous
        .turns
        .iter()
        .map(|turn| (&turn.turn_id, turn))
        .collect();
    let next_turns: BTreeMap<_, _> = next
        .turns
        .iter()
        .map(|turn| (&turn.turn_id, turn))
        .collect();
    let rewound = previous.turns.iter().any(|turn| {
        next_turns.get(&turn.turn_id).is_none_or(|next| {
            turn.activities.iter().any(|old| {
                !next
                    .activities
                    .iter()
                    .any(|item| item.activity_id == old.activity_id)
            })
        })
    });
    if rewound {
        changes.push(AgentSessionChangeKind::RefreshRequired {
            reason: "native_history_rewritten".to_owned(),
        });
    } else {
        for turn in &next.turns {
            let old = old_turns.get(&turn.turn_id);
            let old_activities: BTreeMap<_, _> = old
                .into_iter()
                .flat_map(|turn| &turn.activities)
                .map(|item| (&item.activity_id, item))
                .collect();
            for item in &turn.activities {
                if old_activities.get(&item.activity_id).copied() != Some(item) {
                    changes.push(AgentSessionChangeKind::ActivityUpsert {
                        turn_id: turn.turn_id.clone(),
                        turn_status: turn.status,
                        activity: item.clone(),
                    });
                }
            }
            if old.is_some_and(|old| old.status != turn.status || old.failure != turn.failure) {
                changes.push(AgentSessionChangeKind::TurnStatus {
                    turn_id: turn.turn_id.clone(),
                    status: turn.status,
                    failure: turn.failure.clone(),
                });
            }
        }
    }
    for request in &next.pending_requests {
        if !previous.pending_requests.contains(request) {
            changes.push(AgentSessionChangeKind::PendingRequestUpsert {
                request: request.clone(),
            });
        }
    }
    for request in &previous.pending_requests {
        if !next
            .pending_requests
            .iter()
            .any(|item| item.request_id == request.request_id)
        {
            changes.push(AgentSessionChangeKind::PendingRequestClosed {
                request_id: request.request_id.clone(),
            });
        }
    }
    let mut summary = previous.summary.clone();
    // Activity timestamps convey normal progress. Only metadata that has no
    // incremental representation needs a snapshot reconciliation.
    summary.updated_at_unix_ms = next.summary.updated_at_unix_ms;
    summary.preview = next.summary.preview.clone();
    if summary != next.summary {
        changes.push(AgentSessionChangeKind::RefreshRequired {
            reason: "native_session_metadata_changed".to_owned(),
        });
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::agent_connector::CreateAgentSessionRequest;
    use orchestral_core::agent_protocol::wire::{Digest, PendingRequestPayload, RequestId};
    use serde_json::json;

    async fn fixture() -> (tempfile::TempDir, ClaudeCodeConnector, AgentSessionId) {
        let root = tempfile::tempdir().unwrap();
        let connector = ClaudeCodeConnector::new(ClaudeCodeConfig {
            executable: "unused".into(),
            config_dir: root.path().join("native"),
            session_store_dir: root.path().join("host/sessions"),
            request_timeout: Duration::from_secs(1),
        });
        let session = connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some(root.path().to_string_lossy().into_owned()),
                title: Some("Work".to_owned()),
                options: json!({}),
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap()
            .session_id;
        (root, connector, session)
    }

    #[tokio::test]
    async fn native_append_is_incremental_and_isolated() {
        let (root, connector, session) = fixture().await;
        let project = connector.config.config_dir.join("projects/project");
        std::fs::create_dir_all(&project).unwrap();
        let path = project.join(format!("{}.jsonl", session.as_str()));
        let timestamp = chrono::Utc::now().to_rfc3339();
        let user = json!({"type":"user","uuid":"user-1","sessionId":session.as_str(),"cwd":root.path(),"timestamp":timestamp,"message":{"content":"inspect source"}});
        std::fs::write(&path, format!("{user}\n")).unwrap();
        let mut received = connector.subscribe_session_changes(&session).await.unwrap();
        let other = uuid::Uuid::new_v4();
        std::fs::write(
            project.join(format!("{other}.jsonl")),
            "unrelated session change\n",
        )
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(650), received.recv())
                .await
                .is_err()
        );
        let assistant = json!({"type":"assistant","uuid":"assistant-1","parentUuid":"user-1","sessionId":session.as_str(),"timestamp":timestamp,"message":{"content":[{"type":"text","text":"inspected"}],"stop_reason":"end_turn"}});
        std::fs::write(&path, format!("{user}\n{assistant}\n")).unwrap();
        let change = tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(change.connector_id.as_str(), CONNECTOR_ID);
        assert_eq!(change.session_id, session);
        assert_eq!(change.sequence, 1);
        assert!(
            matches!(change.change, AgentSessionChangeKind::ActivityUpsert { ref activity, .. } if activity.activity_id.as_str() == "claude:assistant-1:0")
        );
        let completed = received.recv().await.unwrap();
        assert_eq!(completed.sequence, 2);
        assert!(matches!(
            completed.change,
            AgentSessionChangeKind::TurnStatus { .. }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(650), received.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn native_owner_state_changes_and_exit_are_observed_without_a_transcript_append() {
        let (_root, connector, session) = fixture().await;
        let registry = connector.config.config_dir.join("sessions");
        std::fs::create_dir_all(&registry).unwrap();
        let pid = std::process::id();
        let path = registry.join(format!("{pid}.json"));
        let mut owner =
            json!({"pid":pid,"sessionId":session.as_str(),"cwd":"/workspace","status":"idle"});
        std::fs::write(&path, owner.to_string()).unwrap();
        let mut received = connector.subscribe_session_changes(&session).await.unwrap();
        owner["status"] = json!("busy");
        std::fs::write(&path, owner.to_string()).unwrap();
        let change = tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            change.change,
            AgentSessionChangeKind::RefreshRequired { .. }
        ));
        std::fs::remove_file(path).unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(exit.sequence > change.sequence);
        assert!(matches!(
            exit.change,
            AgentSessionChangeKind::RefreshRequired { .. }
        ));
    }

    #[tokio::test]
    async fn unknown_and_invalid_session_subscriptions_are_rejected() {
        let (_root, connector, _session) = fixture().await;
        assert_eq!(
            connector
                .subscribe_session_changes(&AgentSessionId::new("../invalid"))
                .await
                .unwrap_err()
                .code,
            AgentConnectorErrorCode::InvalidRequest
        );
        assert_eq!(
            connector
                .subscribe_session_changes(&AgentSessionId::new(uuid::Uuid::new_v4().to_string()))
                .await
                .unwrap_err()
                .code,
            AgentConnectorErrorCode::NotFound
        );
    }

    #[tokio::test]
    async fn approval_lifecycle_is_incremental_and_rewinds_require_reconciliation() {
        let (_root, connector, session) = fixture().await;
        let before = connector.read_session(&session).await.unwrap();
        let mut pending = before.clone();
        let request = PendingRequest {
            request_id: RequestId::new("request-1"),
            blocking: true,
            payload: PendingRequestPayload::Approval {
                operation_digest: Digest::sha256(b"operation"),
                requested_scope: vec!["external_side_effect".to_owned()],
                session_approval_scope: None,
                reason: "Bash".to_owned(),
            },
        };
        pending.pending_requests.push(request.clone());
        assert_eq!(
            changes(&before, &pending),
            vec![AgentSessionChangeKind::PendingRequestUpsert { request }]
        );
        assert_eq!(
            changes(&pending, &before),
            vec![AgentSessionChangeKind::PendingRequestClosed {
                request_id: RequestId::new("request-1")
            }]
        );
        let mut with_history = before.clone();
        with_history.turns = crate::history::turns(&[
            json!({"type":"user","uuid":"removed-turn","message":{"content":"rewound"}}),
        ]);
        assert!(
            matches!(&changes(&with_history, &before)[0], AgentSessionChangeKind::RefreshRequired { reason } if reason == "native_history_rewritten")
        );
    }
}
