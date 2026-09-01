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
use orchestral_core::agent_connector::{AgentConnector, AgentSessionListQuery};
use orchestral_runtime::AgentDirectory;

use crate::mcp_config::user_config_root;

pub(crate) async fn build_agent_directory() -> anyhow::Result<Arc<AgentDirectory>> {
    let directory = Arc::new(AgentDirectory::new());
    let config = CodexAppServerConfig {
        executable: resolve_codex_executable(
            std::env::var_os("ORCHESTRAL_CODEX_PATH").as_deref(),
            std::env::var_os("PATH").as_deref(),
            user_home_dir().as_deref(),
        ),
        ..CodexAppServerConfig::default()
    };
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
        .register_with_journal(codex.clone(), codex.clone(), journal)
        .await
        .context("register Codex Agent connector")?;
    tokio::spawn(async move {
        let _ = codex
            .list_sessions(AgentSessionListQuery {
                limit: 25,
                ..AgentSessionListQuery::default()
            })
            .await;
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
