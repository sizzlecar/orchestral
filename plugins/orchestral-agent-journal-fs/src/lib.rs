//! Crash-safe, single-writer filesystem implementation of Host Agent,
//! Agent Session, Tool Effect, and Generic Agent private checkpoint journals.
//!
//! Each Agent Run is one validated snapshot file. Updates are serialized by
//! this store instance and committed through a same-directory temporary file,
//! file sync, atomic rename, and directory sync. Deployments requiring
//! concurrent writers across processes should use a database-backed plugin.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use orchestral_core::agent_protocol::{
    spi::{
        AgentJournalStore, AgentJournalStoreError, AgentRunCatalogEntry, AppendAgentRecordOutcome,
        CreateAgentRunOutcome, StoredAgentRun,
    },
    wire::{AgentJournalRecord, AgentSessionId, Digest, RunId},
};
use orchestral_core::agent_session::{
    validate_session_trace, AgentSessionAppend, AgentSessionError, AgentSessionEventDraft,
    AgentSessionJournalStore, AgentSessionRecord,
};
use orchestral_core::tool_effect::{
    replay_tool_effect, ToolEffectAppend, ToolEffectError, ToolEffectEventDraft,
    ToolEffectJournalRecord, ToolEffectJournalStore, ToolEffectKey,
};
use orchestral_runtime::{
    AppendGenericCheckpointOutcome, CreateGenericRunOutcome, GenericAgentCheckpointStore,
    GenericAgentRunRegistration, GenericCheckpointDraft, GenericCheckpointError,
    GenericCheckpointRecord, StoredGenericAgentRun,
};

#[derive(Clone)]
pub struct FileAgentJournalStore {
    root: Arc<PathBuf>,
    writer_gate: Arc<Mutex<()>>,
}

impl FileAgentJournalStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, AgentJournalStoreError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(unavailable)?;
        let metadata = fs::metadata(&root).map_err(unavailable)?;
        if !metadata.is_dir() {
            return Err(AgentJournalStoreError::Unavailable(format!(
                "Agent journal root is not a directory: {}",
                root.display()
            )));
        }
        Ok(Self {
            root: Arc::new(root),
            writer_gate: Arc::new(Mutex::new(())),
        })
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    fn run_path(&self, run_id: &RunId) -> PathBuf {
        let digest = Digest::sha256(run_id.as_str());
        self.root.join(format!("run-{}.json", digest.as_str()))
    }

    fn session_path(&self, session_id: &AgentSessionId) -> PathBuf {
        let digest = Digest::sha256(session_id.as_str());
        self.root.join(format!("session-{}.json", digest.as_str()))
    }

    fn effect_path(&self, key: &ToolEffectKey) -> PathBuf {
        let digest = Digest::sha256(format!("{}\0{}", key.run_id.as_str(), key.call_id.as_str()));
        self.root.join(format!("effect-{}.json", digest.as_str()))
    }

    fn generic_checkpoint_path(&self, run_id: &RunId) -> PathBuf {
        let digest = Digest::sha256(run_id.as_str());
        self.root
            .join(format!("generic-checkpoint-{}.json", digest.as_str()))
    }

    fn load_sync(&self, run_id: &RunId) -> Result<Option<StoredAgentRun>, AgentJournalStoreError> {
        let path = self.run_path(run_id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(unavailable(error)),
        };
        let run = serde_json::from_slice::<StoredAgentRun>(&bytes).map_err(|error| {
            AgentJournalStoreError::InvalidData(format!(
                "could not decode {}: {error}",
                path.display()
            ))
        })?;
        run.validate_shape()?;
        if run.registration.run_id() != run_id {
            return Err(AgentJournalStoreError::InvalidData(format!(
                "journal filename identity does not match stored run_id: {}",
                path.display()
            )));
        }
        Ok(Some(run))
    }

    fn catalog_runs_sync(&self) -> Result<Vec<AgentRunCatalogEntry>, AgentJournalStoreError> {
        let mut entries = Vec::new();
        for directory_entry in fs::read_dir(self.root.as_path()).map_err(unavailable)? {
            let directory_entry = directory_entry.map_err(unavailable)?;
            let file_name = directory_entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if !file_name.starts_with("run-") || !file_name.ends_with(".json") {
                continue;
            }

            let path = directory_entry.path();
            let bytes = fs::read(&path).map_err(unavailable)?;
            let run = serde_json::from_slice::<StoredAgentRun>(&bytes).map_err(|error| {
                AgentJournalStoreError::InvalidData(format!(
                    "could not decode {}: {error}",
                    path.display()
                ))
            })?;
            run.validate_shape()?;
            let metadata = directory_entry.metadata().map_err(unavailable)?;
            let updated_at_unix_ms = metadata
                .modified()
                .ok()
                .and_then(system_time_unix_ms)
                .unwrap_or_default();
            let created_at_unix_ms = metadata
                .created()
                .ok()
                .and_then(system_time_unix_ms)
                .unwrap_or(updated_at_unix_ms);
            entries.push(AgentRunCatalogEntry {
                run_id: run.registration.run_id().clone(),
                session_id: run.registration.request.run.spec.session_id.clone(),
                created_at_unix_ms,
                updated_at_unix_ms,
            });
        }
        entries.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        Ok(entries)
    }

    fn write_sync(&self, run: &StoredAgentRun) -> Result<(), AgentJournalStoreError> {
        run.validate_shape()?;
        let destination = self.run_path(run.registration.run_id());
        let temporary = self.root.join(format!(
            ".journal-{}-{}.tmp",
            Digest::sha256(run.registration.run_id().as_str()).as_str(),
            uuid::Uuid::new_v4()
        ));
        let bytes = serde_json::to_vec(run).map_err(|error| {
            AgentJournalStoreError::InvalidData(format!("could not encode Agent journal: {error}"))
        })?;
        let write_result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &destination)?;
            File::open(self.root.as_path())?.sync_all()?;
            Ok::<(), std::io::Error>(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(unavailable(error));
        }
        Ok(())
    }

    fn load_session_sync(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<Vec<AgentSessionRecord>, AgentSessionError> {
        let path = self.session_path(session_id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(session_unavailable(error)),
        };
        let records =
            serde_json::from_slice::<Vec<AgentSessionRecord>>(&bytes).map_err(|error| {
                AgentSessionError::Corrupt(format!("could not decode {}: {error}", path.display()))
            })?;
        validate_session_trace(session_id, &records)?;
        Ok(records)
    }

    fn write_session_sync(
        &self,
        session_id: &AgentSessionId,
        records: &[AgentSessionRecord],
    ) -> Result<(), AgentSessionError> {
        validate_session_trace(session_id, records)?;
        let destination = self.session_path(session_id);
        let temporary = self.root.join(format!(
            ".session-journal-{}-{}.tmp",
            Digest::sha256(session_id.as_str()).as_str(),
            uuid::Uuid::new_v4()
        ));
        let bytes = serde_json::to_vec(records).map_err(|error| {
            AgentSessionError::Corrupt(format!("could not encode Agent Session journal: {error}"))
        })?;
        let write_result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &destination)?;
            File::open(self.root.as_path())?.sync_all()?;
            Ok::<(), std::io::Error>(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(session_unavailable(error));
        }
        Ok(())
    }

    fn load_effect_sync(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Vec<ToolEffectJournalRecord>, ToolEffectError> {
        let path = self.effect_path(key);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(effect_unavailable(error)),
        };
        let records =
            serde_json::from_slice::<Vec<ToolEffectJournalRecord>>(&bytes).map_err(|error| {
                ToolEffectError::Corrupt(format!("could not decode {}: {error}", path.display()))
            })?;
        replay_tool_effect(key, &records)?;
        Ok(records)
    }

    fn write_effect_sync(
        &self,
        key: &ToolEffectKey,
        records: &[ToolEffectJournalRecord],
    ) -> Result<(), ToolEffectError> {
        replay_tool_effect(key, records)?;
        let destination = self.effect_path(key);
        let temporary = self.root.join(format!(
            ".effect-journal-{}-{}.tmp",
            Digest::sha256(format!("{}\0{}", key.run_id.as_str(), key.call_id.as_str())).as_str(),
            uuid::Uuid::new_v4()
        ));
        let bytes = serde_json::to_vec(records).map_err(|error| {
            ToolEffectError::Corrupt(format!("could not encode Tool effect journal: {error}"))
        })?;
        let write_result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &destination)?;
            File::open(self.root.as_path())?.sync_all()?;
            Ok::<(), std::io::Error>(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(effect_unavailable(error));
        }
        Ok(())
    }

    fn load_generic_checkpoint_sync(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        let path = self.generic_checkpoint_path(run_id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(checkpoint_unavailable(error)),
        };
        let run = serde_json::from_slice::<StoredGenericAgentRun>(&bytes).map_err(|error| {
            GenericCheckpointError::InvalidData(format!(
                "could not decode {}: {error}",
                path.display()
            ))
        })?;
        run.validate()?;
        if run.registration.run_id() != run_id {
            return Err(GenericCheckpointError::InvalidData(format!(
                "Generic checkpoint filename identity does not match stored run_id: {}",
                path.display()
            )));
        }
        Ok(Some(run))
    }

    fn write_generic_checkpoint_sync(
        &self,
        run: &StoredGenericAgentRun,
    ) -> Result<(), GenericCheckpointError> {
        run.validate()?;
        let run_id = run.registration.run_id();
        let destination = self.generic_checkpoint_path(run_id);
        let temporary = self.root.join(format!(
            ".generic-checkpoint-{}-{}.tmp",
            Digest::sha256(run_id.as_str()).as_str(),
            uuid::Uuid::new_v4()
        ));
        let bytes = serde_json::to_vec(run).map_err(|error| {
            GenericCheckpointError::InvalidData(format!(
                "could not encode Generic Agent checkpoint: {error}"
            ))
        })?;
        let write_result = (|| {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, &destination)?;
            File::open(self.root.as_path())?.sync_all()?;
            Ok::<(), std::io::Error>(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(checkpoint_unavailable(error));
        }
        Ok(())
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, AgentJournalStoreError>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Result<T, AgentJournalStoreError> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await
            .map_err(|error| {
                AgentJournalStoreError::Unavailable(format!("Agent journal worker failed: {error}"))
            })?
    }

    async fn session_blocking<T, F>(&self, operation: F) -> Result<T, AgentSessionError>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Result<T, AgentSessionError> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await
            .map_err(|error| {
                AgentSessionError::StoreUnavailable(format!(
                    "Agent Session journal worker failed: {error}"
                ))
            })?
    }

    async fn effect_blocking<T, F>(&self, operation: F) -> Result<T, ToolEffectError>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Result<T, ToolEffectError> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await
            .map_err(|error| {
                ToolEffectError::StoreUnavailable(format!(
                    "Tool effect journal worker failed: {error}"
                ))
            })?
    }
}

#[async_trait]
impl AgentJournalStore for FileAgentJournalStore {
    async fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredAgentRun>, AgentJournalStoreError> {
        let run_id = run_id.clone();
        self.blocking(move |store| store.load_sync(&run_id)).await
    }

    async fn create_run(
        &self,
        run: StoredAgentRun,
    ) -> Result<CreateAgentRunOutcome, AgentJournalStoreError> {
        self.blocking(move |store| {
            let _guard = store
                .writer_gate
                .lock()
                .map_err(|_| AgentJournalStoreError::Unavailable("writer lock poisoned".into()))?;
            let run_id = run.registration.run_id().clone();
            if let Some(existing) = store.load_sync(&run_id)? {
                return if existing == run {
                    Ok(CreateAgentRunOutcome::ExactExisting)
                } else {
                    Err(AgentJournalStoreError::RunConflict(run_id))
                };
            }
            store.write_sync(&run)?;
            Ok(CreateAgentRunOutcome::Created)
        })
        .await
    }

    async fn append_record(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        record: AgentJournalRecord,
    ) -> Result<AppendAgentRecordOutcome, AgentJournalStoreError> {
        let run_id = run_id.clone();
        self.blocking(move |store| {
            let _guard = store
                .writer_gate
                .lock()
                .map_err(|_| AgentJournalStoreError::Unavailable("writer lock poisoned".into()))?;
            record
                .validate_integrity()
                .map_err(|error| AgentJournalStoreError::InvalidData(error.to_string()))?;
            if record.event.run_id != run_id {
                return Err(AgentJournalStoreError::InvalidData(
                    "record crossed an Agent Run boundary".to_owned(),
                ));
            }
            let mut run = store
                .load_sync(&run_id)?
                .ok_or_else(|| AgentJournalStoreError::RunNotFound(run_id.clone()))?;
            if let Some(existing) = run
                .records
                .iter()
                .find(|existing| existing.event.run_seq == record.event.run_seq)
            {
                return if existing == &record {
                    Ok(AppendAgentRecordOutcome::ExactDuplicate)
                } else {
                    Err(sequence_conflict(
                        &run_id,
                        expected_previous,
                        run.last_run_seq(),
                        record.event.run_seq,
                    ))
                };
            }
            let actual_previous = run.last_run_seq();
            if actual_previous != expected_previous || record.event.run_seq != expected_previous + 1
            {
                return Err(sequence_conflict(
                    &run_id,
                    expected_previous,
                    actual_previous,
                    record.event.run_seq,
                ));
            }
            run.records.push(record);
            store.write_sync(&run)?;
            Ok(AppendAgentRecordOutcome::Appended)
        })
        .await
    }

    async fn records(
        &self,
        run_id: &RunId,
        after_run_seq: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentJournalStoreError> {
        let run_id = run_id.clone();
        self.blocking(move |store| {
            let run = store
                .load_sync(&run_id)?
                .ok_or_else(|| AgentJournalStoreError::RunNotFound(run_id.clone()))?;
            Ok(run
                .records
                .into_iter()
                .filter(|record| record.event.run_seq > after_run_seq)
                .collect())
        })
        .await
    }

    async fn catalog_runs(&self) -> Result<Vec<AgentRunCatalogEntry>, AgentJournalStoreError> {
        self.blocking(|store| store.catalog_runs_sync()).await
    }
}

fn system_time_unix_ms(value: std::time::SystemTime) -> Option<i64> {
    let milliseconds = value
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis();
    i64::try_from(milliseconds).ok()
}

#[async_trait]
impl AgentSessionJournalStore for FileAgentJournalStore {
    async fn load_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<Vec<AgentSessionRecord>, AgentSessionError> {
        let session_id = session_id.clone();
        self.session_blocking(move |store| store.load_session_sync(&session_id))
            .await
    }

    async fn append(
        &self,
        draft: AgentSessionEventDraft,
    ) -> Result<AgentSessionAppend, AgentSessionError> {
        self.session_blocking(move |store| {
            draft.validate()?;
            let draft_digest = draft.digest()?;
            let _guard = store.writer_gate.lock().map_err(|_| {
                AgentSessionError::StoreUnavailable("writer lock poisoned".to_owned())
            })?;
            let mut records = store.load_session_sync(&draft.session_id)?;
            if let Some(existing) = records
                .iter()
                .find(|record| record.event_id == draft.event_id)
            {
                return if existing.draft_digest == draft_digest {
                    Ok(AgentSessionAppend {
                        record: existing.clone(),
                        exact_duplicate: true,
                    })
                } else {
                    Err(AgentSessionError::EventConflict(draft.event_id))
                };
            }
            let session_id = draft.session_id.clone();
            let record = AgentSessionRecord::seal(draft, records.len() as u64 + 1)?;
            records.push(record.clone());
            store.write_session_sync(&session_id, &records)?;
            Ok(AgentSessionAppend {
                record,
                exact_duplicate: false,
            })
        })
        .await
    }
}

#[async_trait]
impl ToolEffectJournalStore for FileAgentJournalStore {
    async fn load_effect(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Vec<ToolEffectJournalRecord>, ToolEffectError> {
        let key = key.clone();
        self.effect_blocking(move |store| store.load_effect_sync(&key))
            .await
    }

    async fn append(
        &self,
        expected_previous: u64,
        draft: ToolEffectEventDraft,
    ) -> Result<ToolEffectAppend, ToolEffectError> {
        self.effect_blocking(move |store| {
            draft.validate()?;
            let draft_digest = draft.digest()?;
            let _guard = store.writer_gate.lock().map_err(|_| {
                ToolEffectError::StoreUnavailable("writer lock poisoned".to_owned())
            })?;
            let mut records = store.load_effect_sync(&draft.key)?;
            if let Some(existing) = records
                .iter()
                .find(|record| record.event_id == draft.event_id)
            {
                return if existing.draft_digest == draft_digest {
                    Ok(ToolEffectAppend {
                        record: existing.clone(),
                        exact_duplicate: true,
                    })
                } else {
                    Err(ToolEffectError::EventConflict(draft.event_id))
                };
            }
            let actual_previous = records.len() as u64;
            if actual_previous != expected_previous {
                return Err(ToolEffectError::SequenceConflict {
                    key: draft.key,
                    expected_previous,
                    actual_previous,
                });
            }
            let key = draft.key.clone();
            let record = ToolEffectJournalRecord::seal(draft, actual_previous + 1)?;
            let mut candidate = records.clone();
            candidate.push(record.clone());
            replay_tool_effect(&key, &candidate)?;
            records.push(record.clone());
            store.write_effect_sync(&key, &records)?;
            Ok(ToolEffectAppend {
                record,
                exact_duplicate: false,
            })
        })
        .await
    }
}

impl GenericAgentCheckpointStore for FileAgentJournalStore {
    fn load_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<StoredGenericAgentRun>, GenericCheckpointError> {
        self.load_generic_checkpoint_sync(run_id)
    }

    fn create_run(
        &self,
        registration: GenericAgentRunRegistration,
    ) -> Result<CreateGenericRunOutcome, GenericCheckpointError> {
        registration.validate()?;
        let run_id = registration.run_id().clone();
        let _guard = self
            .writer_gate
            .lock()
            .map_err(|_| checkpoint_unavailable("writer lock poisoned"))?;
        if let Some(existing) = self.load_generic_checkpoint_sync(&run_id)? {
            return if existing.registration == registration {
                Ok(CreateGenericRunOutcome::ExactExisting)
            } else {
                Err(GenericCheckpointError::RunConflict(run_id))
            };
        }
        self.write_generic_checkpoint_sync(&StoredGenericAgentRun {
            registration,
            records: Vec::new(),
        })?;
        Ok(CreateGenericRunOutcome::Created)
    }

    fn append(
        &self,
        run_id: &RunId,
        expected_previous: u64,
        draft: GenericCheckpointDraft,
    ) -> Result<AppendGenericCheckpointOutcome, GenericCheckpointError> {
        draft.validate()?;
        if draft.run_id != *run_id {
            return Err(GenericCheckpointError::InvalidData(
                "checkpoint append crossed a Run boundary".to_owned(),
            ));
        }
        let draft_digest = draft.digest()?;
        let _guard = self
            .writer_gate
            .lock()
            .map_err(|_| checkpoint_unavailable("writer lock poisoned"))?;
        let mut run = self
            .load_generic_checkpoint_sync(run_id)?
            .ok_or_else(|| GenericCheckpointError::RunNotFound(run_id.clone()))?;
        if let Some(existing) = run
            .records
            .iter()
            .find(|record| record.event_id == draft.event_id)
        {
            return if existing.draft_digest == draft_digest {
                Ok(AppendGenericCheckpointOutcome::ExactDuplicate)
            } else {
                Err(GenericCheckpointError::EventConflict(draft.event_id))
            };
        }
        let actual_previous = run.last_checkpoint_seq();
        if actual_previous != expected_previous {
            return Err(GenericCheckpointError::SequenceConflict {
                run_id: run_id.clone(),
                expected_previous,
                actual_previous,
            });
        }
        let record = GenericCheckpointRecord::seal(draft, actual_previous + 1)?;
        let mut candidate = run.clone();
        candidate.records.push(record.clone());
        candidate.validate()?;
        run.records.push(record);
        self.write_generic_checkpoint_sync(&run)?;
        Ok(AppendGenericCheckpointOutcome::Appended)
    }
}

fn sequence_conflict(
    run_id: &RunId,
    expected_previous: u64,
    actual_previous: u64,
    incoming: u64,
) -> AgentJournalStoreError {
    AgentJournalStoreError::SequenceConflict {
        run_id: run_id.clone(),
        expected_previous,
        actual_previous,
        incoming,
    }
}

fn unavailable(error: impl std::fmt::Display) -> AgentJournalStoreError {
    AgentJournalStoreError::Unavailable(error.to_string())
}

fn session_unavailable(error: impl std::fmt::Display) -> AgentSessionError {
    AgentSessionError::StoreUnavailable(error.to_string())
}

fn effect_unavailable(error: impl std::fmt::Display) -> ToolEffectError {
    ToolEffectError::StoreUnavailable(error.to_string())
}

fn checkpoint_unavailable(error: impl std::fmt::Display) -> GenericCheckpointError {
    GenericCheckpointError::Unavailable(error.to_string())
}
