use orchestral_agent_codex::CodexConnector;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use orchestral_core::agent_connector::{
    AgentConnector, AgentSessionListQuery, CreateAgentSessionRequest,
};
use orchestral_core::agent_protocol::reference::AgentRunStatus;
use orchestral_core::agent_protocol::wire::ContentBody;
use orchestral_runtime::AgentDirectory;

/// Opt-in compatibility check against the Codex executable installed on the
/// developer machine. The deterministic JSONL tests remain the CI contract.
#[tokio::test]
#[ignore = "requires an installed and authenticated Codex CLI"]
async fn lists_and_reads_local_codex_sessions_without_mutating_them() {
    let connector = CodexConnector::default();
    let health = connector.health().await.expect("Codex must initialize");
    assert!(health.version.is_some());

    let page = connector
        .list_sessions(AgentSessionListQuery {
            limit: 3,
            ..Default::default()
        })
        .await
        .expect("Codex must list local sessions");
    assert!(page.sessions.len() <= 3);
    if let Some(session) = page.sessions.first() {
        let detail = connector
            .read_session(&session.session_id)
            .await
            .expect("Codex must read a listed local session");
        assert_eq!(detail.summary.session_id, session.session_id);
        assert_eq!(detail.summary.connector_id, session.connector_id);
    }
}

/// Exercises Codex's real pre-materialization lifecycle without starting a
/// model turn or spending provider quota.
#[tokio::test]
#[ignore = "requires an installed and authenticated Codex CLI"]
async fn creates_and_reads_an_empty_local_codex_session() {
    let connector = CodexConnector::default();
    let session = connector
        .create_session(CreateAgentSessionRequest {
            cwd: None,
            title: Some("orchestral-empty-session-smoke".to_owned()),
            extensions: BTreeMap::new(),
        })
        .await
        .expect("Codex must create a native session");

    let detail = connector
        .read_session(&session.session_id)
        .await
        .expect("an unmaterialized Codex session must read as empty");

    assert_eq!(detail.summary.session_id, session.session_id);
    assert_eq!(detail.summary.title, session.title);
    assert!(detail.turns.is_empty());
}

/// Verifies the real app-server lifecycle that previously attempted
/// `thread/resume` before a newly created thread had its first rollout.
#[tokio::test]
#[ignore = "requires an installed and authenticated Codex CLI"]
async fn creates_and_sends_the_first_turn_without_resuming_an_empty_thread() {
    let connector = Arc::new(CodexConnector::default());
    let directory = AgentDirectory::new();
    directory
        .register(connector.clone(), connector)
        .await
        .expect("Codex connector must register");
    let session = directory
        .create_session(
            &orchestral_core::agent_connector::AgentConnectorId::new("codex/local"),
            CreateAgentSessionRequest {
                cwd: None,
                title: Some("orchestral-first-turn-smoke".to_owned()),
                extensions: BTreeMap::new(),
            },
        )
        .await
        .expect("Codex must create a native session");
    let handle = directory
        .start_text(
            &session.connector_id,
            &session.session_id,
            None,
            "Do not call tools. Reply exactly: ORCHESTRAL_CODEX_OK",
        )
        .await
        .expect("the first message must start without thread/resume");
    let turn = tokio::time::timeout(Duration::from_secs(120), handle.wait_until_blocked())
        .await
        .expect("Codex first turn timed out")
        .expect("Codex first turn failed");

    assert_eq!(turn.view.state.status(), AgentRunStatus::Delivered);
    let delivery = turn.view.delivery.expect("Codex must commit a delivery");
    let text = match &delivery.final_response.body {
        ContentBody::Inline(serde_json::Value::String(text)) => text.as_str(),
        _ => "",
    };
    assert!(text.contains("ORCHESTRAL_CODEX_OK"));
}
