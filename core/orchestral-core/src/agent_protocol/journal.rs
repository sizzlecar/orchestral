//! Durable Agent Run registration and append-only journal SPI.
//!
//! Stores own atomic compare-and-append. Controllers own semantic reduction;
//! a store never invents `run_seq` or interprets Provider events.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::types::{
    AgentAdmission, AgentExecutionRef, AgentJournalRecord, AgentRunEnvelope, AgentStartRequest,
    RunId,
};

/// Immutable data needed to reconstruct an Agent Run reducer after restart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRunRegistration {
    pub request: AgentStartRequest,
    pub execution: AgentExecutionRef,
    pub admission: AgentAdmission,
}

impl AgentRunRegistration {
    pub fn run(&self) -> &AgentRunEnvelope {
        &self.request.run
    }

    pub fn run_id(&self) -> &RunId {
        &self.execution.run_id
    }

    pub fn validate_shape(&self) -> Result<(), AgentJournalStoreError> {
        self.request
            .run
            .validate_integrity()
            .map_err(invalid_data)?;
        self.execution.validate_integrity().map_err(invalid_data)?;
        self.admission.validate_integrity().map_err(invalid_data)?;
        if self.execution.run_id != self.request.run.spec.run_id
            || self.execution.session_id != self.request.run.spec.session_id
            || self.execution.spec_digest != self.request.run.spec_digest
            || self.execution.binding_ref != self.request.provider_binding
            || self.execution.descriptor_digest != self.request.expected_descriptor_digest
        {
            return Err(AgentJournalStoreError::InvalidData(
                "Agent Run registration identities do not agree".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Complete durable Run state used for deterministic rehydration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredAgentRun {
    pub registration: AgentRunRegistration,
    pub records: Vec<AgentJournalRecord>,
}

impl StoredAgentRun {
    pub fn validate_shape(&self) -> Result<(), AgentJournalStoreError> {
        self.registration.validate_shape()?;
        validate_record_sequence(self.registration.run_id(), &self.records)
    }

    pub fn last_run_seq(&self) -> u64 {
        self.records
            .last()
            .map(|record| record.event.run_seq)
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateAgentRunOutcome {
    Created,
    ExactExisting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendAgentRecordOutcome {
    Appended,
    ExactDuplicate,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AgentJournalStoreError {
    #[error("Agent journal storage is unavailable: {0}")]
    Unavailable(String),
    #[error("Agent Run does not exist in the journal: {0}")]
    RunNotFound(RunId),
    #[error("Agent Run registration conflicts with durable state: {0}")]
    RunConflict(RunId),
    #[error(
        "Agent journal sequence conflict for {run_id}: expected previous {expected_previous}, durable previous {actual_previous}, incoming {incoming}"
    )]
    SequenceConflict {
        run_id: RunId,
        expected_previous: u64,
        actual_previous: u64,
        incoming: u64,
    },
    #[error("Agent journal contains invalid data: {0}")]
    InvalidData(String),
}

/// Atomic persistence contract for Agent control-plane facts.
#[async_trait]
pub trait AgentJournalStore: Send + Sync {
    async fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredAgentRun>, AgentJournalStoreError>;

    /// Atomically creates immutable registration plus its initial journal
    /// prefix. An exact retry is idempotent; any other reuse conflicts.
    async fn create_run(
        &self,
        run: StoredAgentRun,
    ) -> Result<CreateAgentRunOutcome, AgentJournalStoreError>;

    /// Atomically appends exactly `expected_previous + 1`.
    ///
    /// A retry of an already committed byte-equivalent semantic record is an
    /// exact duplicate even when the current tail has advanced to that record.
    async fn append_record(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        record: AgentJournalRecord,
    ) -> Result<AppendAgentRecordOutcome, AgentJournalStoreError>;

    async fn records(
        &self,
        run_id: &RunId,
        after_run_seq: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentJournalStoreError>;
}

/// Minimal deterministic store used by tests and process-lifetime defaults.
/// Durable deployments provide the same SPI from a plugin.
#[derive(Default)]
pub struct InMemoryAgentJournalStore {
    runs: RwLock<BTreeMap<RunId, StoredAgentRun>>,
}

#[async_trait]
impl AgentJournalStore for InMemoryAgentJournalStore {
    async fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredAgentRun>, AgentJournalStoreError> {
        Ok(self.runs.read().await.get(run_id).cloned())
    }

    async fn create_run(
        &self,
        run: StoredAgentRun,
    ) -> Result<CreateAgentRunOutcome, AgentJournalStoreError> {
        run.validate_shape()?;
        let run_id = run.registration.run_id().clone();
        let mut runs = self.runs.write().await;
        if let Some(existing) = runs.get(&run_id) {
            return if existing == &run {
                Ok(CreateAgentRunOutcome::ExactExisting)
            } else {
                Err(AgentJournalStoreError::RunConflict(run_id))
            };
        }
        runs.insert(run_id, run);
        Ok(CreateAgentRunOutcome::Created)
    }

    async fn append_record(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        record: AgentJournalRecord,
    ) -> Result<AppendAgentRecordOutcome, AgentJournalStoreError> {
        record.validate_integrity().map_err(invalid_data)?;
        if record.event.run_id != *run_id {
            return Err(AgentJournalStoreError::InvalidData(
                "record crossed an Agent Run boundary".to_owned(),
            ));
        }
        let mut runs = self.runs.write().await;
        let run = runs
            .get_mut(run_id)
            .ok_or_else(|| AgentJournalStoreError::RunNotFound(run_id.clone()))?;
        if let Some(existing) = run
            .records
            .iter()
            .find(|existing| existing.event.run_seq == record.event.run_seq)
        {
            return if existing == &record {
                Ok(AppendAgentRecordOutcome::ExactDuplicate)
            } else {
                Err(AgentJournalStoreError::SequenceConflict {
                    run_id: run_id.clone(),
                    expected_previous,
                    actual_previous: run.last_run_seq(),
                    incoming: record.event.run_seq,
                })
            };
        }
        let actual_previous = run.last_run_seq();
        if actual_previous != expected_previous || record.event.run_seq != expected_previous + 1 {
            return Err(AgentJournalStoreError::SequenceConflict {
                run_id: run_id.clone(),
                expected_previous,
                actual_previous,
                incoming: record.event.run_seq,
            });
        }
        run.records.push(record);
        Ok(AppendAgentRecordOutcome::Appended)
    }

    async fn records(
        &self,
        run_id: &RunId,
        after_run_seq: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentJournalStoreError> {
        let runs = self.runs.read().await;
        let run = runs
            .get(run_id)
            .ok_or_else(|| AgentJournalStoreError::RunNotFound(run_id.clone()))?;
        Ok(run
            .records
            .iter()
            .filter(|record| record.event.run_seq > after_run_seq)
            .cloned()
            .collect())
    }
}

fn validate_record_sequence(
    run_id: &RunId,
    records: &[AgentJournalRecord],
) -> Result<(), AgentJournalStoreError> {
    if records.is_empty() {
        return Err(AgentJournalStoreError::InvalidData(
            "stored Agent Run requires its initial journal prefix".to_owned(),
        ));
    }
    for (index, record) in records.iter().enumerate() {
        record.validate_integrity().map_err(invalid_data)?;
        let expected = index as u64 + 1;
        if record.event.run_id != *run_id || record.event.run_seq != expected {
            return Err(AgentJournalStoreError::InvalidData(format!(
                "record sequence is not contiguous at expected run_seq {expected}"
            )));
        }
    }
    Ok(())
}

fn invalid_data(error: impl std::fmt::Display) -> AgentJournalStoreError {
    AgentJournalStoreError::InvalidData(error.to_string())
}
