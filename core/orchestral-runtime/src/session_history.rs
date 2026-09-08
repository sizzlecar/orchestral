//! Read-only session discovery derived from authoritative journals.

use std::collections::BTreeMap;
use std::sync::Arc;

use orchestral_core::agent_protocol::spi::{
    AgentJournalStore, AgentJournalStoreError, AgentRunCatalogEntry, StoredAgentRun,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, ContentBody, ProviderBindingRef};
use orchestral_core::agent_session::{
    validate_session_trace, AgentSessionError, AgentSessionJournalStore,
};
use orchestral_core::session_history::{
    SessionHistory, SessionHistoryQuery, SessionHistoryStatus, SessionHistorySummary, SessionOrigin,
};

#[derive(Debug, thiserror::Error)]
/// A storage or validation failure while reading a session catalog.
pub enum SessionHistoryError {
    #[error(transparent)]
    Journal(#[from] AgentJournalStoreError),
    #[error(transparent)]
    Session(#[from] AgentSessionError),
    #[error("invalid session origin: {0}")]
    Origin(#[from] serde_json::Error),
}

/// Browsing neither contacts a Provider nor rehydrates a Controller. The
/// catalog is rebuilt from journals and can be used with any storage plugin.
#[derive(Clone)]
pub struct JournalSessionHistory {
    runs: Arc<dyn AgentJournalStore>,
    sessions: Arc<dyn AgentSessionJournalStore>,
    binding: ProviderBindingRef,
}

impl JournalSessionHistory {
    /// Selects one provider binding in a shared Run journal.
    pub fn new(
        runs: Arc<dyn AgentJournalStore>,
        sessions: Arc<dyn AgentSessionJournalStore>,
        binding: ProviderBindingRef,
    ) -> Self {
        Self {
            runs,
            sessions,
            binding,
        }
    }

    /// Rebuilds matching summaries, ordered by newest storage activity and ID.
    pub async fn list(
        &self,
        query: &SessionHistoryQuery,
    ) -> Result<Vec<SessionHistorySummary>, SessionHistoryError> {
        let mut groups = BTreeMap::<AgentSessionId, Vec<AgentRunCatalogEntry>>::new();
        for entry in self.runs.catalog_runs().await? {
            groups
                .entry(entry.session_id.clone())
                .or_default()
                .push(entry);
        }
        let mut summaries = Vec::new();
        let search = query
            .search
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_lowercase();
        for (id, entries) in groups {
            if let Some(history) = self.read_entries(&id, entries).await? {
                let summary = history.summary;
                if query.workspace.as_ref().is_some_and(|workspace| {
                    summary
                        .origin
                        .as_ref()
                        .is_none_or(|origin| &origin.workspace != workspace)
                }) {
                    continue;
                }
                if !search.is_empty()
                    && !summary.title.to_lowercase().contains(&search)
                    && !id.as_str().to_lowercase().contains(&search)
                {
                    continue;
                }
                summaries.push(summary);
            }
        }
        summaries.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        Ok(summaries)
    }

    /// Returns a complete historical transcript without recovering execution.
    pub async fn read(
        &self,
        id: &AgentSessionId,
    ) -> Result<Option<SessionHistory>, SessionHistoryError> {
        let entries = self
            .runs
            .catalog_runs()
            .await?
            .into_iter()
            .filter(|entry| &entry.session_id == id)
            .collect();
        self.read_entries(id, entries).await
    }

    async fn read_entries(
        &self,
        id: &AgentSessionId,
        entries: Vec<AgentRunCatalogEntry>,
    ) -> Result<Option<SessionHistory>, SessionHistoryError> {
        let mut runs = Vec::<(AgentRunCatalogEntry, StoredAgentRun)>::new();
        for entry in entries {
            let Some(run) = self.runs.load_run(&entry.run_id).await? else {
                continue;
            };
            run.validate_shape()?;
            if run.registration.request.provider_binding == self.binding {
                runs.push((entry, run));
            }
        }
        if runs.is_empty() {
            return Ok(None);
        }
        let records = self.sessions.load_session(id).await?;
        validate_session_trace(id, &records)?;
        let mut first_seq = BTreeMap::new();
        for record in &records {
            first_seq
                .entry(record.run_id.clone())
                .or_insert(record.session_seq);
        }
        runs.sort_by_key(|(entry, _)| {
            (
                first_seq.get(&entry.run_id).copied().unwrap_or(u64::MAX),
                entry.created_at_unix_ms,
                entry.run_id.clone(),
            )
        });
        let title = runs[0]
            .1
            .registration
            .run()
            .spec
            .input
            .iter()
            .filter_map(|content| match &content.body {
                ContentBody::Inline(serde_json::Value::String(text)) => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(" ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(120)
            .collect::<String>();
        let origins = runs
            .iter()
            .map(|(_, run)| SessionOrigin::from_extensions(&run.registration.run().spec.extensions))
            .collect::<Result<Vec<_>, _>>()?;
        let origin = origins
            .iter()
            .rev()
            .flatten()
            .next()
            .filter(|latest| {
                origins
                    .iter()
                    .flatten()
                    .all(|other| other.workspace == latest.workspace)
            })
            .cloned();
        let unfinished_run_ids = runs
            .iter()
            .filter(|(_, run)| {
                SessionHistoryStatus::of_run(run) == SessionHistoryStatus::Unfinished
            })
            .map(|(entry, _)| entry.run_id.clone())
            .collect::<Vec<_>>();
        let summary = SessionHistorySummary {
            session_id: id.clone(),
            title: if title.is_empty() {
                "Untitled session".to_owned()
            } else {
                title
            },
            origin,
            updated_at_unix_ms: runs
                .iter()
                .map(|(entry, _)| entry.updated_at_unix_ms)
                .max()
                .unwrap_or_default(),
            run_count: runs.len(),
            status: if unfinished_run_ids.is_empty() {
                SessionHistoryStatus::of_run(&runs.last().expect("nonempty").1)
            } else {
                SessionHistoryStatus::Unfinished
            },
            unfinished_run_ids,
        };
        Ok(Some(SessionHistory {
            summary,
            runs: runs.into_iter().map(|(_, run)| run).collect(),
            records,
        }))
    }
}
