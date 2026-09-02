//! Application-level composition for complete external Agent connectors.
//!
//! Core and runtime know only the connector and Agent Protocol contracts.
//! Concrete integrations are installed here so adding another Agent does not
//! add a dependency from runtime to a plugin.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use orchestral_agent_codex::{CodexAppServerConfig, CodexConnector};
use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_core::agent_connector::AgentSessionListQuery;
use orchestral_core::io::{ArtifactPublisher, ArtifactResolver, BlobStore};
use orchestral_runtime::AgentDirectory;

use crate::mcp_config::user_config_root;

#[derive(Clone, Copy)]
pub(crate) enum AgentJournalAccess {
    SingleWriter,
    ReadOnly,
}

pub(crate) async fn build_agent_directory(
    artifact_resolver: Option<Arc<dyn ArtifactResolver>>,
    artifact_blob_store: Option<Arc<dyn BlobStore>>,
    artifact_publisher: Option<Arc<dyn ArtifactPublisher>>,
    journal_access: AgentJournalAccess,
) -> anyhow::Result<Arc<AgentDirectory>> {
    let directory = Arc::new(AgentDirectory::new());
    let config_root = user_config_root()?;
    let config = CodexAppServerConfig {
        executable: resolve_codex_executable(
            std::env::var_os("ORCHESTRAL_CODEX_PATH").as_deref(),
            std::env::var_os("PATH").as_deref(),
            user_home_dir().as_deref(),
        ),
        dispatch_journal_dir: Some(
            config_root
                .join("agent-connectors")
                .join("codex-local")
                .join("dispatch"),
        ),
        ..CodexAppServerConfig::default()
    };
    let session_list_cache_path = config_root
        .join("agent-connectors")
        .join("codex-local")
        .join("session-list-cache.json");
    let codex = Arc::new(
        match (artifact_resolver, artifact_blob_store, artifact_publisher) {
            (Some(resolver), Some(blob_store), Some(publisher)) => {
                CodexConnector::with_artifact_services(config, resolver, blob_store, publisher)
            }
            (Some(resolver), Some(blob_store), None) => {
                CodexConnector::with_artifact_io(config, resolver, blob_store)
            }
            (Some(resolver), None, _) => CodexConnector::with_artifact_resolver(config, resolver),
            _ => CodexConnector::new(config),
        }
        .with_session_list_cache_path(session_list_cache_path),
    );
    let journal_root = config_root
        .join("agent-connectors")
        .join("codex-local")
        .join("journal");
    let journal = Arc::new(
        match journal_access {
            AgentJournalAccess::SingleWriter => {
                FileAgentJournalStore::open_single_writer(journal_root)
            }
            AgentJournalAccess::ReadOnly => FileAgentJournalStore::open_read_only(journal_root),
        }
        .context("open Codex control journal")?,
    );
    directory
        .register_with_journal(codex.clone(), codex.clone(), journal)
        .await
        .context("register Codex Agent connector")?;
    tokio::spawn(async move {
        let query = AgentSessionListQuery {
            limit: 25,
            ..AgentSessionListQuery::default()
        };
        loop {
            codex.wait_for_session_list_refresh().await;
            // Collapse a burst of page loads or session mutations, then avoid
            // repeating a scan for a notification queued during the scan that
            // just completed.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Err(error) = codex
                .refresh_session_list_if_stale(query.clone(), std::time::Duration::ZERO)
                .await
            {
                tracing::warn!(%error, "Codex session-list background refresh failed");
            }
        }
    });
    Ok(directory)
}

fn resolve_codex_executable(
    configured: Option<&OsStr>,
    path: Option<&OsStr>,
    home: Option<&Path>,
) -> PathBuf {
    if let Some(configured) = configured.filter(|value| !value.is_empty()) {
        return PathBuf::from(configured);
    }
    let executable_name = if cfg!(windows) { "codex.exe" } else { "codex" };
    if let Some(candidate) = path.and_then(|path| {
        std::env::split_paths(path)
            .map(|directory| directory.join(executable_name))
            .find(|candidate| host_executable_file(candidate))
    }) {
        return candidate;
    }
    if let Some(candidate) = home
        .map(|home| home.join(".local").join("bin").join(executable_name))
        .filter(|candidate| host_executable_file(candidate))
    {
        return candidate;
    }
    PathBuf::from(executable_name)
}

fn host_executable_file(candidate: &Path) -> bool {
    let Ok(metadata) = candidate.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn user_home_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    let name = "USERPROFILE";
    #[cfg(not(target_os = "windows"))]
    let name = "HOME";
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn codex_resolution_falls_back_to_the_standard_user_bin() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-codex-resolution-{}",
            uuid::Uuid::new_v4()
        ));
        let executable_name = if cfg!(windows) { "codex.exe" } else { "codex" };
        let executable = root.join(".local").join("bin").join(executable_name);
        fs::create_dir_all(executable.parent().unwrap()).unwrap();
        fs::write(&executable, b"fixture").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        }

        let resolved = resolve_codex_executable(None, Some(OsStr::new("/usr/bin")), Some(&root));

        assert_eq!(resolved, executable);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explicit_codex_path_has_precedence_without_hidden_rewriting() {
        let configured = OsStr::new("/opt/agents/codex-custom");
        assert_eq!(
            resolve_codex_executable(Some(configured), None, None),
            PathBuf::from(configured)
        );
    }
}
