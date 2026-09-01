use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use orchestral_agent_protocol_testkit::{
    ProviderFixtureFactory, ProviderScenario, ScriptedStatelessFactory, TestProbes,
};
use orchestral_core::agent_connector::{
    AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorHealth,
    AgentConnectorId, AgentSessionActionDescriptor, AgentSessionActionExecution,
    AgentSessionActionId, AgentSessionActionOutcome, AgentSessionActionStatus,
    AgentSessionActivity, AgentSessionActivityId, AgentSessionActivityKind,
    AgentSessionActivityStatus, AgentSessionCapabilities, AgentSessionDetail,
    AgentSessionListQuery, AgentSessionPage, AgentSessionState, AgentSessionSummary,
    AgentSessionTurn, AgentSessionTurnId, AgentSessionTurnStatus, CreateAgentSessionRequest,
    InvokeAgentSessionActionRequest, SESSION_COMPACT_ACTION, SESSION_REVIEW_ACTION,
};
use orchestral_core::agent_protocol::reference::AgentRunStatus;
use orchestral_core::agent_protocol::wire::{AgentSessionId, Content, ProviderBindingRef, RunId};
use orchestral_runtime::AgentDirectory;

struct FixtureConnector {
    descriptor: AgentConnectorDescriptor,
    sessions: Vec<AgentSessionSummary>,
}

impl FixtureConnector {
    fn new(connector_id: &str, provider_binding: &str, count: usize) -> Self {
        let connector_id = AgentConnectorId::new(connector_id);
        let sessions = (0..count)
            .map(|index| AgentSessionSummary {
                connector_id: connector_id.clone(),
                session_id: AgentSessionId::new(format!("session-{index:03}")),
                title: Some(format!("Session {index}")),
                preview: Some(format!("Preview {index}")),
                cwd: Some(format!("/workspace/{index}")),
                created_at_unix_ms: Some(1_000 + index as i64),
                updated_at_unix_ms: Some(2_000 + index as i64),
                state: AgentSessionState::Detached,
                extensions: BTreeMap::new(),
            })
            .collect();
        Self {
            descriptor: AgentConnectorDescriptor {
                connector_id,
                provider_binding: ProviderBindingRef::new(provider_binding),
                agent_family: "coding-agent".to_owned(),
                display_name: "Fixture Agent".to_owned(),
                capabilities: AgentSessionCapabilities::discoverable(),
                actions: vec![
                    AgentSessionActionDescriptor {
                        action_id: AgentSessionActionId::new(SESSION_COMPACT_ACTION),
                        title: "Compact".to_owned(),
                        description: "Compact context".to_owned(),
                        input_schema: None,
                        execution: AgentSessionActionExecution::Immediate,
                    },
                    AgentSessionActionDescriptor {
                        action_id: AgentSessionActionId::new(SESSION_REVIEW_ACTION),
                        title: "Review".to_owned(),
                        description: "Review changes".to_owned(),
                        input_schema: Some(serde_json::json!({"type": "object"})),
                        execution: AgentSessionActionExecution::Run,
                    },
                ],
            },
            sessions,
        }
    }

    fn activity(id: &str, kind: AgentSessionActivityKind, text: &str) -> AgentSessionActivity {
        AgentSessionActivity {
            activity_id: AgentSessionActivityId::new(id),
            kind,
            status: AgentSessionActivityStatus::Completed,
            title: None,
            content: vec![Content::text(text)],
            details: serde_json::Value::Null,
        }
    }
}

#[async_trait]
impl AgentConnector for FixtureConnector {
    fn describe(&self) -> AgentConnectorDescriptor {
        self.descriptor.clone()
    }

    async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
        Ok(AgentConnectorHealth::ready(Some("fixture-1".to_owned())))
    }

    async fn list_sessions(
        &self,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
        let offset = query
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| AgentConnectorError::invalid("invalid fixture cursor"))?;
        let end = (offset + query.limit as usize).min(self.sessions.len());
        Ok(AgentSessionPage {
            sessions: self.sessions[offset..end].to_vec(),
            next_cursor: (end < self.sessions.len()).then(|| end.to_string()),
        })
    }

    async fn read_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        let summary = self
            .sessions
            .iter()
            .find(|session| &session.session_id == session_id)
            .cloned()
            .ok_or_else(|| {
                AgentConnectorError::new(
                    orchestral_core::agent_connector::AgentConnectorErrorCode::NotFound,
                    "fixture session was not found",
                    false,
                )
            })?;
        Ok(AgentSessionDetail {
            summary,
            turns: vec![
                AgentSessionTurn {
                    turn_id: AgentSessionTurnId::new("turn-1"),
                    status: AgentSessionTurnStatus::Completed,
                    activities: vec![Self::activity(
                        "activity-user",
                        AgentSessionActivityKind::UserMessage,
                        "inspect the project",
                    )],
                },
                AgentSessionTurn {
                    turn_id: AgentSessionTurnId::new("turn-2"),
                    status: AgentSessionTurnStatus::Completed,
                    activities: vec![
                        Self::activity(
                            "activity-plan",
                            AgentSessionActivityKind::Plan,
                            "read then verify",
                        ),
                        Self::activity(
                            "activity-command",
                            AgentSessionActivityKind::Command,
                            "cargo test",
                        ),
                    ],
                },
                AgentSessionTurn {
                    turn_id: AgentSessionTurnId::new("turn-3"),
                    status: AgentSessionTurnStatus::Completed,
                    activities: vec![Self::activity(
                        "activity-file",
                        AgentSessionActivityKind::FileChange,
                        "updated src/lib.rs",
                    )],
                },
            ],
            pending_requests: Vec::new(),
            next_cursor: None,
        })
    }

    async fn create_session(
        &self,
        _request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        Err(AgentConnectorError::unsupported(
            "fixture creation disabled",
        ))
    }

    async fn invoke_action(
        &self,
        request: InvokeAgentSessionActionRequest,
    ) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
        assert_ne!(request.action_id.as_str(), SESSION_REVIEW_ACTION);
        Ok(AgentSessionActionOutcome {
            status: AgentSessionActionStatus::Completed,
            session: None,
            content: vec![Content::text(format!(
                "invoked {} for {}",
                request.action_id, request.session_id
            ))],
            details: serde_json::Value::Null,
        })
    }
}

fn fixture_provider() -> Arc<dyn orchestral_core::agent_protocol::spi::AgentProvider> {
    let factory = ScriptedStatelessFactory::conformant().expect("fixture descriptor");
    let scenario = ProviderScenario::standard(&factory.descriptor()).expect("fixture scenario");
    factory.create(scenario, TestProbes::default())
}

#[tokio::test]
async fn directory_pages_fifty_plus_sessions_without_duplicates() {
    let directory = AgentDirectory::new();
    let connector = Arc::new(FixtureConnector::new(
        "fixture/default",
        "fixture/provider",
        73,
    ));
    directory
        .register(connector, fixture_provider())
        .await
        .expect("connector registers");

    let connector_id = AgentConnectorId::new("fixture/default");
    let mut cursor = None;
    let mut observed = Vec::new();
    loop {
        let page = directory
            .list_sessions(
                &connector_id,
                AgentSessionListQuery {
                    cursor,
                    limit: 17,
                    cwd: None,
                    search: None,
                },
            )
            .await
            .expect("page lists");
        observed.extend(page.sessions.into_iter().map(|session| session.session_id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(observed.len(), 73);
    assert_eq!(observed.iter().cloned().collect::<BTreeSet<_>>().len(), 73);
}

#[tokio::test]
async fn directory_preserves_ordered_multi_turn_history() {
    let directory = AgentDirectory::new();
    directory
        .register(
            Arc::new(FixtureConnector::new(
                "fixture/default",
                "fixture/provider",
                1,
            )),
            fixture_provider(),
        )
        .await
        .expect("connector registers");

    let detail = directory
        .read_session(
            &AgentConnectorId::new("fixture/default"),
            &AgentSessionId::new("session-000"),
        )
        .await
        .expect("session reads");

    assert_eq!(detail.turns.len(), 3);
    assert_eq!(
        detail.turns[1]
            .activities
            .iter()
            .map(|activity| activity.kind)
            .collect::<Vec<_>>(),
        vec![
            AgentSessionActivityKind::Plan,
            AgentSessionActivityKind::Command
        ]
    );
    assert_eq!(
        detail.turns[2].activities[0].kind,
        AgentSessionActivityKind::FileChange
    );
}

#[tokio::test]
async fn directory_routes_new_runs_through_registered_agent_provider() {
    let directory = AgentDirectory::new();
    directory
        .register(
            Arc::new(FixtureConnector::new(
                "fixture/default",
                "fixture/provider",
                1,
            )),
            fixture_provider(),
        )
        .await
        .expect("connector registers");

    let handle = directory
        .start_text(
            &AgentConnectorId::new("fixture/default"),
            &AgentSessionId::new("session-000"),
            Some(RunId::new("external-run-1")),
            "continue this session",
        )
        .await
        .expect("run starts");
    let turn = handle.wait_until_blocked().await.expect("run completes");

    assert_eq!(turn.status(), AgentRunStatus::Delivered);
    assert_eq!(handle.run_id().as_str(), "external-run-1");
}

#[tokio::test]
async fn connector_actions_require_declared_capability() {
    let directory = AgentDirectory::new();
    directory
        .register(
            Arc::new(FixtureConnector::new(
                "fixture/default",
                "fixture/provider",
                1,
            )),
            fixture_provider(),
        )
        .await
        .expect("connector registers");

    let outcome = directory
        .invoke_action(
            &AgentConnectorId::new("fixture/default"),
            InvokeAgentSessionActionRequest {
                session_id: AgentSessionId::new("session-000"),
                action_id: AgentSessionActionId::new(SESSION_COMPACT_ACTION),
                arguments: serde_json::Value::Null,
                run_id: None,
            },
        )
        .await
        .expect("declared action invokes");
    assert_eq!(outcome.content.len(), 1);

    let error = directory
        .invoke_action(
            &AgentConnectorId::new("fixture/default"),
            InvokeAgentSessionActionRequest {
                session_id: AgentSessionId::new("session-000"),
                action_id: AgentSessionActionId::new("session.unknown"),
                arguments: serde_json::Value::Null,
                run_id: None,
            },
        )
        .await
        .expect_err("undeclared action must fail");
    assert!(error.to_string().contains("does not declare action"));
}

#[tokio::test]
async fn run_session_action_uses_agent_protocol_lifecycle() {
    let directory = AgentDirectory::new();
    directory
        .register(
            Arc::new(FixtureConnector::new(
                "fixture/default",
                "fixture/provider",
                1,
            )),
            fixture_provider(),
        )
        .await
        .expect("connector registers");

    let outcome = directory
        .invoke_action(
            &AgentConnectorId::new("fixture/default"),
            InvokeAgentSessionActionRequest {
                session_id: AgentSessionId::new("session-000"),
                action_id: AgentSessionActionId::new(SESSION_REVIEW_ACTION),
                arguments: serde_json::json!({"target": "uncommitted_changes"}),
                run_id: Some(RunId::new("review-run-1")),
            },
        )
        .await
        .expect("Run action starts");
    assert_eq!(
        outcome.status,
        AgentSessionActionStatus::Running {
            run_id: RunId::new("review-run-1")
        }
    );

    let api = directory
        .agent_api(&AgentConnectorId::new("fixture/default"))
        .await
        .unwrap();
    assert!(api.has_run(&RunId::new("review-run-1")).await.unwrap());
    let input = api
        .initial_input(&RunId::new("review-run-1"))
        .await
        .unwrap();
    assert_eq!(input, vec![Content::text("Review")]);
}
