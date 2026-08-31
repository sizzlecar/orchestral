//! Application-level composition for complete external Agent connectors.
//!
//! Core and runtime know only the connector and Agent Protocol contracts.
//! Concrete integrations are installed here so adding another Agent does not
//! add a dependency from runtime to a plugin.

use std::sync::Arc;

use anyhow::Context;
use orchestral_agent_codex::{CodexAppServerConfig, CodexConnector};
use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_runtime::AgentDirectory;

use crate::mcp_config::user_config_root;

pub(crate) async fn build_agent_directory() -> anyhow::Result<Arc<AgentDirectory>> {
    let directory = Arc::new(AgentDirectory::new());
    let mut config = CodexAppServerConfig::default();
    if let Some(executable) = std::env::var_os("ORCHESTRAL_CODEX_PATH") {
        config.executable = executable.into();
    }
    let codex = Arc::new(CodexConnector::new(config));
    let journal = Arc::new(
        FileAgentJournalStore::open(
            user_config_root()?
                .join("agent-connectors")
                .join("codex-local")
                .join("journal"),
        )
        .context("open Codex control journal")?,
    );
    directory
        .register_with_journal(codex.clone(), codex, journal)
        .await
        .context("register Codex Agent connector")?;
    Ok(directory)
}
