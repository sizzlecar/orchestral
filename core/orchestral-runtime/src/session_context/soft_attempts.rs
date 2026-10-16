//! Run-scoped deduplication of optional compaction attempts.
//!
//! This cache changes neither durable history nor the hard overflow path. An
//! unavailable summarizer must not spend another full request deadline on the
//! same source at every model round. Register before awaiting it so concurrent
//! callers and cancelled requests also cannot start duplicate soft attempts.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::{
    select_compaction_source, session_range_digest, validate_session_trace, AgentSessionCompactor,
    AgentSessionId, Digest, RunId, SessionContextError,
};

const MAX_SESSIONS: usize = 128;
const MAX_SOURCES_PER_RUN: usize = 256;

struct RunAttempts {
    run_id: RunId,
    sources: BTreeSet<Digest>,
}

#[derive(Default)]
pub(super) struct SoftCompactionAttempts {
    sessions: BTreeMap<AgentSessionId, RunAttempts>,
    recent_sessions: VecDeque<AgentSessionId>,
}

impl SoftCompactionAttempts {
    fn available(&self, session: &AgentSessionId, run: &RunId, source: &Digest) -> bool {
        self.sessions.get(session).is_none_or(|attempts| {
            attempts.run_id != *run
                || (attempts.sources.len() < MAX_SOURCES_PER_RUN
                    && !attempts.sources.contains(source))
        })
    }

    fn begin(&mut self, session: &AgentSessionId, run: &RunId, source: &Digest) -> bool {
        if !self.sessions.contains_key(session) && self.sessions.len() == MAX_SESSIONS {
            if let Some(oldest) = self.recent_sessions.pop_front() {
                self.sessions.remove(&oldest);
            }
        }
        self.recent_sessions.retain(|known| known != session);
        self.recent_sessions.push_back(session.clone());
        let attempts = self
            .sessions
            .entry(session.clone())
            .or_insert_with(|| RunAttempts {
                run_id: run.clone(),
                sources: BTreeSet::new(),
            });
        // A previous Run's outage must never permanently disable maintenance
        // for this Session. Session coordination owns the active Run identity.
        if attempts.run_id != *run {
            attempts.run_id = run.clone();
            attempts.sources.clear();
        }
        // Once this small memoization budget is full, stop only optional work
        // for the Run instead of evicting sources and retrying failed summaries.
        // Hard overflow compaction bypasses this cache entirely.
        if attempts.sources.len() >= MAX_SOURCES_PER_RUN {
            return false;
        }
        attempts.sources.insert(source.clone())
    }
}

impl AgentSessionCompactor {
    /// Check optional history maintenance without announcing another model
    /// attempt for a source already deferred during this Run.
    pub async fn has_working_set_candidate(
        &self,
        session: &AgentSessionId,
        run: &RunId,
    ) -> Result<bool, SessionContextError> {
        let records = self.journal.load_session(session).await?;
        validate_session_trace(session, &records)?;
        let Some(source) = select_compaction_source(&records, run, &self.policy) else {
            return Ok(false);
        };
        let digest = session_range_digest(&records, &source)?;
        Ok(self
            .soft_attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .available(session, run, &digest))
    }

    pub(super) fn begin_soft_attempt(
        &self,
        session: &AgentSessionId,
        run: &RunId,
        source: &Digest,
    ) -> bool {
        let allowed = self
            .soft_attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .begin(session, run, source);
        if !allowed {
            tracing::debug!(session_id = %session, run_id = %run,
                "soft compaction skipped: source already attempted or Run memoization budget exhausted");
        }
        allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sources_are_attempted_once_per_run_and_sessions_are_independent() {
        let mut attempts = SoftCompactionAttempts::default();
        let session = AgentSessionId::new("session");
        let run = RunId::new("run");
        let source = Digest::sha256("source");
        assert!(attempts.begin(&session, &run, &source));
        assert!(!attempts.begin(&session, &run, &source));
        assert!(attempts.begin(&session, &run, &Digest::sha256("new source")));
        assert!(attempts.begin(&AgentSessionId::new("other session"), &run, &source));
        assert!(attempts.begin(&session, &RunId::new("next run"), &source));
    }

    #[test]
    fn attempt_memory_is_bounded_and_new_runs_recover_the_allowance() {
        let mut attempts = SoftCompactionAttempts::default();
        let session = AgentSessionId::new("session");
        let run = RunId::new("run");
        for index in 0..MAX_SOURCES_PER_RUN {
            assert!(attempts.begin(&session, &run, &Digest::sha256(index.to_string())));
        }
        assert!(!attempts.begin(&session, &run, &Digest::sha256("new source")));
        assert!(attempts.begin(
            &session,
            &RunId::new("next run"),
            &Digest::sha256("new source")
        ));
        for index in 0..MAX_SESSIONS + 1 {
            attempts.begin(
                &AgentSessionId::new(format!("session-{index}")),
                &run,
                &Digest::sha256("source"),
            );
        }
        assert_eq!(attempts.sessions.len(), MAX_SESSIONS);
        assert_eq!(attempts.recent_sessions.len(), MAX_SESSIONS);
    }
}
