//! Read-only local compatibility check. Makes no model requests.
use std::path::PathBuf;
use std::time::Duration;

use orchestral_agent_claude::{ClaudeCodeConfig, ClaudeCodeConnector};
use orchestral_core::agent_connector::{AgentConnector, AgentSessionListQuery};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is required")?);
    let connector = ClaudeCodeConnector::new(ClaudeCodeConfig {
        executable: std::env::var_os("ORCHESTRAL_CLAUDE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/bin/claude")),
        config_dir: std::env::var_os("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude")),
        session_store_dir: std::env::temp_dir().join("orchestral-claude-inspect-unused"),
        request_timeout: Duration::from_secs(10),
    });
    println!("{}", serde_json::to_string(&connector.health().await?)?);
    for session in connector
        .list_sessions(AgentSessionListQuery::default())
        .await?
        .sessions
    {
        let detail = connector.read_session(&session.session_id).await?;
        println!(
            "{}",
            serde_json::json!({"id":session.session_id,"title":session.title,"cwd":session.cwd,"state":session.state,"turns":detail.turns.len(),"activities":detail.turns.iter().map(|turn| turn.activities.len()).sum::<usize>()})
        );
    }
    Ok(())
}
