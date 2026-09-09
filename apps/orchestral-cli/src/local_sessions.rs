//! Built-in Agent session browsing; no model or native connector is started.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use clap::Args;
use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_core::agent_protocol::spi::InMemoryAgentJournalStore;
use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
use orchestral_core::agent_session::InMemoryAgentSessionJournalStore;
use orchestral_core::config::load_config;
use orchestral_core::session_history::{SessionHistory, SessionHistoryQuery};
use orchestral_runtime::session_history::JournalSessionHistory;

use crate::agent::AgentRunOptions;
use crate::runtime::client::resolve_runtime_config_path;

pub(crate) const GENERIC_BINDING: &str = "orchestral/generic-agent";

#[derive(Debug, Args)]
pub(crate) struct ResumeCommand {
    /// Existing built-in Agent session identity.
    #[arg(required_unless_present = "last")]
    session: Option<String>,
    /// Resume the most recently updated session in the current workspace.
    #[arg(long)]
    last: bool,
    /// Optional follow-up prompt; omit in a terminal to reopen the conversation.
    #[arg(value_name = "INPUT", num_args = 1..)]
    input: Vec<String>,
}

pub(crate) fn workspace(cwd: Option<&Path>) -> anyhow::Result<String> {
    let path = cwd
        .map(Path::to_path_buf)
        .map_or_else(std::env::current_dir, Ok)?;
    let path = std::fs::canonicalize(path).context("resolve session workspace")?;
    if !path.is_dir() {
        bail!("session workspace must be a directory");
    }
    path.to_str()
        .map(str::to_owned)
        .context("session workspace is not UTF-8")
}

pub(crate) fn open_history(config_path: Option<PathBuf>) -> anyhow::Result<JournalSessionHistory> {
    let config = load_config(&resolve_runtime_config_path(config_path)?)?;
    match config.journal.backend.as_str() {
        "fs" | "filesystem" if Path::new(&config.journal.root_dir).exists() => {
            let store = Arc::new(FileAgentJournalStore::open_read_only(
                &config.journal.root_dir,
            )?);
            Ok(JournalSessionHistory::new(
                store.clone(),
                store,
                ProviderBindingRef::new(GENERIC_BINDING),
            ))
        }
        "fs" | "filesystem" | "memory" => Ok(JournalSessionHistory::new(
            Arc::new(InMemoryAgentJournalStore::default()),
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            ProviderBindingRef::new(GENERIC_BINDING),
        )),
        backend => bail!("unsupported session journal backend: {backend}"),
    }
}

pub(crate) fn validate_workspace(history: &SessionHistory, workspace: &str) -> anyhow::Result<()> {
    for run in &history.runs {
        if let Some(origin) = orchestral_core::session_history::SessionOrigin::from_extensions(
            &run.registration.run().spec.extensions,
        )? {
            if origin.workspace != workspace {
                bail!(
                    "session {} belongs to workspace '{}'; reopen with -C '{}'",
                    history.summary.session_id,
                    origin.workspace,
                    origin.workspace
                );
            }
        }
    }
    Ok(())
}

impl ResumeCommand {
    pub(crate) async fn run(self, mut options: AgentRunOptions) -> anyhow::Result<()> {
        if options.session_id.is_some() {
            bail!("use either resume or --session-id");
        }
        let catalog = open_history(options.config.clone())?;
        let cwd = workspace(options.cwd.as_deref())?;
        let mut input = self.input;
        let id = if self.last {
            if let Some(first) = self.session {
                input.insert(0, first);
            }
            catalog.list(&SessionHistoryQuery { workspace: Some(cwd.clone()), search: None }).await?
                .into_iter().next().context("no built-in Agent sessions in this workspace; use sessions list --all to browse other or legacy sessions")?.session_id
        } else {
            AgentSessionId::new(
                self.session
                    .context("resume requires a session ID or --last")?,
            )
        };
        let history = catalog
            .read(&id)
            .await?
            .with_context(|| format!("built-in Agent session not found: {id}"))?;
        validate_workspace(&history, &cwd)?;
        options.session_id = Some(id.as_str().to_owned());
        options.input = (!input.is_empty()).then(|| input.join(" "));
        crate::agent::run(options).await
    }
}
