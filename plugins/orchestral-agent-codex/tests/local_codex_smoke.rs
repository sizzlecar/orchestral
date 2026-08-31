use orchestral_agent_codex::CodexConnector;
use orchestral_core::agent_connector::{AgentConnector, AgentSessionListQuery};

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
