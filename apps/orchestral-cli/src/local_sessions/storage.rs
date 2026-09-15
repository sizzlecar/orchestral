//! Each local conversation owns a writer lease. Browsing includes legacy Hosts
//! and all session directories without acquiring or bypassing their leases.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub enum LocalSessionHistory {
    Single(JournalSessionHistory),
    Directory(PathBuf),
}

fn roots(base: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    if base.is_dir() {
        roots.push(base.to_owned());
    }
    let sessions = base.join("sessions");
    if sessions.is_dir() {
        for entry in std::fs::read_dir(sessions)? {
            let entry = entry?;
            // Exactly one layout level; never follow directory symlinks.
            if entry.file_type()?.is_dir() {
                roots.push(entry.path());
            }
        }
    }
    roots.sort();
    Ok(roots)
}

fn history(root: &Path) -> anyhow::Result<JournalSessionHistory> {
    let store = Arc::new(FileAgentJournalStore::open_read_only(root)?);
    Ok(JournalSessionHistory::new(
        store.clone(),
        store,
        ProviderBindingRef::new(GENERIC_BINDING),
    ))
}

impl LocalSessionHistory {
    pub async fn list(
        &self,
        query: &SessionHistoryQuery,
    ) -> anyhow::Result<Vec<orchestral_core::session_history::SessionHistorySummary>> {
        let base = match self {
            Self::Directory(base) => base,
            Self::Single(history) => return Ok(history.list(query).await?),
        };
        let mut found = std::collections::BTreeMap::new();
        for root in roots(base)? {
            for summary in history(&root)?.list(query).await? {
                if found.insert(summary.session_id.clone(), summary).is_some() {
                    bail!("session identity exists in multiple journal directories");
                }
            }
        }
        let mut result: Vec<_> = found.into_values().collect();
        result.sort_by(|a, b| {
            b.updated_at_unix_ms
                .cmp(&a.updated_at_unix_ms)
                .then_with(|| a.session_id.cmp(&b.session_id))
        });
        Ok(result)
    }

    pub async fn read(&self, id: &AgentSessionId) -> anyhow::Result<Option<SessionHistory>> {
        match self {
            Self::Single(history) => Ok(history.read(id).await?),
            Self::Directory(base) => {
                let mut found = None;
                for root in roots(base)? {
                    if let Some(session) = history(&root)?.read(id).await? {
                        if found.replace(session).is_some() {
                            bail!("session {id} exists in multiple journal directories");
                        }
                    }
                }
                Ok(found)
            }
        }
    }
}

pub(crate) async fn writer_root(base: &Path, id: &AgentSessionId) -> anyhow::Result<PathBuf> {
    let shard = base
        .join("sessions")
        .join(hex::encode(Sha256::digest(id.as_str().as_bytes())));
    // A fresh concurrent process selects the identical path for the identical
    // identity, even before its first journal record has been committed.
    let mut found = None;
    for root in roots(base)? {
        if history(&root)?.read(id).await?.is_some() && found.replace(root).is_some() {
            bail!("session {id} exists in multiple journal directories");
        }
    }
    Ok(found.unwrap_or(shard))
}
