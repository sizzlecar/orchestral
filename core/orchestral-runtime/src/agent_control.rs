//! Host-side control plane for one provider binding.
//!
//! This is the first production vertical slice over Agent Protocol v1. It owns
//! normalized sequencing and the reducer projection for process-lifetime Runs.
//! Durable storage is deliberately a later store implementation; callers must
//! not mistake this in-memory controller for crash recovery.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use orchestral_core::agent_protocol::{
    reference::{
        AgentContinuityState, AgentRunReducer, AgentRunStatus, ApplyOutcome,
        ReconciliationProofVerifier, SequencedApply,
    },
    spi::{
        AgentJournalStore, AgentJournalStoreError, AgentProvider, AgentProviderStream,
        AgentRecoveryRequest, AgentRunCatalogEntry, AgentRunRegistration, AgentStartError,
        InMemoryAgentJournalStore, StoredAgentRun,
    },
    wire::{
        AgentCommand, AgentCommandEnvelope, AgentEvent, AgentEventAuthority, AgentEventDraft,
        AgentEventId, AgentExecutionRef, AgentJournalRecord, AgentProtocolError,
        AgentProtocolErrorCode, AgentProviderStreamItem, AgentRunEnvelope, AgentRunState,
        AgentRunView, AgentStartRequest, AgentTelemetryEnvelope, CommandAck, CommandId, Content,
        Digest, ProviderBindingRef, ReconciliationProof, ReconciliationProofRef, RunId,
    },
};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex, Notify, RwLock};
use tokio::time::timeout;

const RECOVERY_PREFIX_ITEM_TIMEOUT: Duration = Duration::from_secs(2);

/// Ordered durable facts and best-effort live telemetry emitted by a Run.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AgentControlEvent {
    Durable(Arc<AgentJournalRecord>),
    Telemetry(AgentTelemetryEnvelope),
}

/// Host control-plane failure. Run failures remain durable Agent events and
/// are not represented by this infrastructure error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentControlError {
    #[error(transparent)]
    Protocol(#[from] AgentProtocolError),
    #[error(transparent)]
    Start(#[from] AgentStartError),
    #[error(transparent)]
    Journal(#[from] AgentJournalStoreError),
    #[error("Agent Run not found: {0}")]
    RunNotFound(RunId),
    #[error("Agent Run continuity is unknown: {0}")]
    ContinuityUnknown(RunId),
    #[error("Agent recovery evidence did not match the committed Run prefix: {0}")]
    RecoveryMismatch(RunId),
}

struct RunEntry {
    request: AgentStartRequest,
    execution: AgentExecutionRef,
    reducer: AgentRunReducer,
    journal: Vec<AgentJournalRecord>,
}

struct RunSlot {
    entry: Mutex<RunEntry>,
    recovery_gate: Mutex<()>,
    events: broadcast::Sender<AgentControlEvent>,
    changed: Notify,
}

impl RunSlot {
    fn publish(&self, event: AgentControlEvent) {
        let _ = self.events.send(event);
        self.changed.notify_waiters();
    }
}

/// Process-lifetime Agent controller for one immutable Provider binding.
///
/// The controller, not the Provider, owns normalized `run_seq`, journal
/// records, public inspection, command acknowledgement, and EOF-to-Unknown
/// handling. One controller may host many isolated Runs.
pub struct AgentController {
    provider: Arc<dyn AgentProvider>,
    binding_ref: ProviderBindingRef,
    descriptor: orchestral_core::agent_protocol::wire::AgentDescriptorEnvelope,
    journal_store: Arc<dyn AgentJournalStore>,
    runs: RwLock<BTreeMap<RunId, Arc<RunSlot>>>,
    start_gate: Mutex<()>,
    event_buffer: usize,
}

impl AgentController {
    pub fn new(
        provider: Arc<dyn AgentProvider>,
        binding_ref: ProviderBindingRef,
    ) -> Result<Self, AgentProtocolError> {
        Self::with_event_buffer(provider, binding_ref, 256)
    }

    pub fn with_event_buffer(
        provider: Arc<dyn AgentProvider>,
        binding_ref: ProviderBindingRef,
        event_buffer: usize,
    ) -> Result<Self, AgentProtocolError> {
        Self::with_journal_store_and_event_buffer(
            provider,
            binding_ref,
            Arc::new(InMemoryAgentJournalStore::default()),
            event_buffer,
        )
    }

    pub fn with_journal_store(
        provider: Arc<dyn AgentProvider>,
        binding_ref: ProviderBindingRef,
        journal_store: Arc<dyn AgentJournalStore>,
    ) -> Result<Self, AgentProtocolError> {
        Self::with_journal_store_and_event_buffer(provider, binding_ref, journal_store, 256)
    }

    pub fn with_journal_store_and_event_buffer(
        provider: Arc<dyn AgentProvider>,
        binding_ref: ProviderBindingRef,
        journal_store: Arc<dyn AgentJournalStore>,
        event_buffer: usize,
    ) -> Result<Self, AgentProtocolError> {
        if binding_ref.is_empty() || event_buffer == 0 {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidSpec,
                "Agent controller requires a Provider binding and a non-zero event buffer",
            ));
        }
        let descriptor = provider.describe();
        descriptor.validate_integrity()?;
        Ok(Self {
            provider,
            binding_ref,
            descriptor,
            journal_store,
            runs: RwLock::new(BTreeMap::new()),
            start_gate: Mutex::new(()),
            event_buffer,
        })
    }

    pub fn descriptor(&self) -> &orchestral_core::agent_protocol::wire::AgentDescriptorEnvelope {
        &self.descriptor
    }

    /// Starts or idempotently reopens one immutable Run and begins consuming
    /// its atomic Provider stream in the background.
    pub async fn start(
        self: &Arc<Self>,
        run: AgentRunEnvelope,
    ) -> Result<AgentExecutionRef, AgentControlError> {
        let request = AgentStartRequest::new(run, self.binding_ref.clone(), &self.descriptor)?;
        let run_id = request.run.spec.run_id.clone();
        let _start_guard = self.start_gate.lock().await;

        if let Some(slot) = self.runs.read().await.get(&run_id).cloned() {
            let entry = slot.entry.lock().await;
            if entry.request != request {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::RunIdConflict,
                    "run_id already belongs to another immutable start request",
                )
                .into());
            }
            return Ok(entry.execution.clone());
        }

        if let Some(stored) = self.journal_store.load_run(&run_id).await? {
            if stored.registration.request != request {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::RunIdConflict,
                    "run_id already belongs to another durable start request",
                )
                .into());
            }
            let slot = self.slot_from_stored(stored)?;
            let execution = slot.entry.lock().await.execution.clone();
            self.runs.write().await.insert(run_id.clone(), slot.clone());
            let view = slot.entry.lock().await.reducer.view();
            if !view.state.is_terminal() && view.state.status() != AgentRunStatus::Unknown {
                self.mark_continuity_lost(
                    &run_id,
                    &slot,
                    "Host controller restarted without a continuously attached Provider stream"
                        .to_owned(),
                )
                .await?;
            }
            return Ok(execution);
        }

        let observed_descriptor = self.provider.describe();
        if observed_descriptor != self.descriptor {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "Provider descriptor changed after controller binding",
            )
            .into());
        }
        let start = self.provider.start(request.clone()).await?;
        start.execution.validate_for(&request, &self.descriptor)?;
        let compatibility = self
            .descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(|rejection| {
                AgentProtocolError::new(AgentProtocolErrorCode::Unsupported, rejection.to_string())
                    .with_details(rejection.details)
            })?;
        start
            .admission
            .validate_against(&request.run, &compatibility)?;

        let mut reducer = AgentRunReducer::new(
            start.execution.clone(),
            &request,
            &self.descriptor,
            start.admission.clone(),
        )?;
        let mut journal = Vec::with_capacity(start.admission.skipped_optional_bindings.len() + 4);
        push_sequenced(
            &mut journal,
            reducer.apply_host_draft(AgentEventDraft {
                event_id: AgentEventId::new(format!("host-run-accepted-{}", run_id.as_str())),
                run_id: run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunAccepted {
                    session_id: request.run.spec.session_id.clone(),
                    spec_digest: request.run.spec_digest.clone(),
                },
            })?,
        );
        for skip in &start.admission.skipped_optional_bindings {
            push_sequenced(
                &mut journal,
                reducer.apply_host_draft(AgentEventDraft {
                    event_id: AgentEventId::new(format!(
                        "host-resource-skip-{}-{}",
                        run_id.as_str(),
                        skip.binding_id.as_str()
                    )),
                    run_id: run_id.clone(),
                    causation_id: None,
                    source_fingerprint: None,
                    payload: AgentEvent::ResourceBindingSkipped { skip: skip.clone() },
                })?,
            );
        }

        let (events, _) = broadcast::channel(self.event_buffer);
        self.journal_store
            .create_run(StoredAgentRun {
                registration: AgentRunRegistration {
                    request: request.clone(),
                    execution: start.execution.clone(),
                    admission: start.admission.clone(),
                },
                records: journal.clone(),
            })
            .await?;
        let slot = Arc::new(RunSlot {
            entry: Mutex::new(RunEntry {
                request,
                execution: start.execution.clone(),
                reducer,
                journal: journal.clone(),
            }),
            recovery_gate: Mutex::new(()),
            events,
            changed: Notify::new(),
        });
        self.runs.write().await.insert(run_id.clone(), slot.clone());
        for record in journal {
            slot.publish(AgentControlEvent::Durable(Arc::new(record)));
        }

        let controller = Arc::clone(self);
        tokio::spawn(async move {
            controller.drive_stream(run_id, slot, start.stream).await;
        });
        Ok(start.execution)
    }

    /// Returns the bounded Host projection for a Run.
    pub async fn inspect(&self, run_id: &RunId) -> Result<AgentRunView, AgentControlError> {
        let slot = self.run_slot(run_id).await?;
        let view = slot.entry.lock().await.reducer.view();
        Ok(view)
    }

    /// Returns the immutable initial input from the registered Run spec.
    ///
    /// This is separate from the bounded public Run projection because it is
    /// conversation content, not reducible execution state. Authenticated Host
    /// surfaces can use it to reconstruct a transcript without duplicating the
    /// input in a transport-specific registry.
    pub async fn initial_input(&self, run_id: &RunId) -> Result<Vec<Content>, AgentControlError> {
        let slot = self.run_slot(run_id).await?;
        let input = slot.entry.lock().await.request.run.spec.input.clone();
        Ok(input)
    }

    /// Lists durable Runs directly from the Host journal. Transport-specific
    /// session registries must not maintain a second Run ownership index.
    pub async fn catalog_runs(&self) -> Result<Vec<AgentRunCatalogEntry>, AgentControlError> {
        Ok(self.journal_store.catalog_runs().await?)
    }

    /// Reports whether a durable Run was registered against this controller's
    /// current immutable Provider contract.
    ///
    /// Discovery surfaces use this before enriching a native session with a
    /// Host-controlled Run. A Provider upgrade may legitimately change its
    /// descriptor digest; those older journals remain durable history, but
    /// they cannot be rehydrated or controlled by the new binding.
    pub async fn can_control_run(&self, run_id: &RunId) -> Result<bool, AgentControlError> {
        if let Some(slot) = self.runs.read().await.get(run_id).cloned() {
            let entry = slot.entry.lock().await;
            return Ok(self.registration_matches_current(&entry.request, &entry.execution));
        }
        let Some(stored) = self.journal_store.load_run(run_id).await? else {
            return Ok(false);
        };
        stored.validate_shape()?;
        Ok(self.registration_matches_current(
            &stored.registration.request,
            &stored.registration.execution,
        ))
    }

    pub async fn has_run(&self, run_id: &RunId) -> Result<bool, AgentControlError> {
        if self.runs.read().await.contains_key(run_id) {
            return Ok(true);
        }
        Ok(self.journal_store.load_run(run_id).await?.is_some())
    }

    /// Replays normalized durable facts after the supplied Run sequence.
    pub async fn events(
        &self,
        run_id: &RunId,
        after_run_seq: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentControlError> {
        self.run_slot(run_id).await?;
        Ok(self.journal_store.records(run_id, after_run_seq).await?)
    }

    /// Subscribes to events published after subscription. Call `events` first
    /// (and again after broadcast lag) for lossless durable replay.
    pub async fn subscribe(
        &self,
        run_id: &RunId,
    ) -> Result<broadcast::Receiver<AgentControlEvent>, AgentControlError> {
        Ok(self.run_slot(run_id).await?.events.subscribe())
    }

    /// Records, forwards, and durably projects one idempotent Provider command.
    /// Replays return the existing Host acknowledgement without forwarding.
    pub async fn command(
        &self,
        command: AgentCommandEnvelope,
    ) -> Result<CommandAck, AgentControlError> {
        command.verify_digest()?;
        let slot = self.run_slot(&command.run_id).await?;
        let mut entry = slot.entry.lock().await;
        if let Ok(ack) = entry.reducer.command_ack(&command.command_id, true) {
            return Ok(ack);
        }

        let mut next_reducer = entry.reducer.clone();
        let received = next_reducer.apply_host_draft(AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "host-command-received-{}-{}",
                command.run_id.as_str(),
                command.command_id.as_str()
            )),
            run_id: command.run_id.clone(),
            causation_id: Some(command.command_id.clone()),
            source_fingerprint: None,
            payload: AgentEvent::CommandReceived {
                command: command.clone(),
            },
        })?;
        let duplicate = matches!(received.outcome, ApplyOutcome::ExactDuplicate);
        self.commit_sequenced(&slot, &mut entry, next_reducer, received)
            .await?;

        let disposition = self
            .provider
            .command(&entry.execution, command.clone())
            .await?;
        disposition.validate_for(&command)?;
        let mut next_reducer = entry.reducer.clone();
        let disposition_event = next_reducer.apply_provider_draft(disposition.to_event_draft()?)?;
        self.commit_sequenced(&slot, &mut entry, next_reducer, disposition_event)
            .await?;
        Ok(entry.reducer.command_ack(&command.command_id, duplicate)?)
    }

    /// Requests cancellation with a fresh idempotency identity. Callers that
    /// need retry-stable command IDs should construct an `AgentCommandEnvelope`
    /// once and use [`Self::command`] directly.
    pub async fn cancel(
        &self,
        run_id: &RunId,
        reason: impl Into<String>,
    ) -> Result<CommandAck, AgentControlError> {
        let command = AgentCommandEnvelope::new(
            CommandId::new(format!("host-cancel-{}", uuid::Uuid::new_v4())),
            run_id.clone(),
            None,
            AgentCommand::Cancel {
                reason: reason.into(),
            },
        )?;
        self.command(command).await
    }

    pub async fn command_ack(
        &self,
        run_id: &RunId,
        command_id: &CommandId,
    ) -> Result<CommandAck, AgentControlError> {
        let slot = self.run_slot(run_id).await?;
        let entry = slot.entry.lock().await;
        let ack = entry.reducer.command_ack(command_id, true)?;
        Ok(ack)
    }

    /// Conservatively reattaches a Provider stream after a Host-recorded EOF.
    ///
    /// The reference controller requires the recovered stream to replay the
    /// complete committed Provider prefix with stable event IDs and draft
    /// digests. It restores continuity only after that prefix matches; opaque
    /// adapters that cannot provide such evidence remain `Unknown` rather
    /// than being guessed healthy.
    pub async fn recover(
        self: &Arc<Self>,
        run_id: &RunId,
    ) -> Result<AgentRunView, AgentControlError> {
        let slot = self.run_slot(run_id).await?;
        let _recovery_guard = slot.recovery_gate.lock().await;
        let (
            start_request,
            execution,
            expected_provider_prefix,
            last_confirmed_seq,
            loss_event_digest,
        ) = {
            let entry = slot.entry.lock().await;
            let AgentRunState::Unknown {
                last_confirmed_seq, ..
            } = entry.reducer.state()
            else {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "recover requires a Run whose continuity is Unknown",
                )
                .into());
            };
            let loss_record = entry.journal.last().ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Unknown Run has no continuity-loss journal record",
                )
            })?;
            if !matches!(
                (&loss_record.authority, &loss_record.event.payload),
                (
                    AgentEventAuthority::Host { .. },
                    AgentEvent::ContinuityLost { .. }
                )
            ) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "automatic recovery is allowed only after a Host-recorded stream loss",
                )
                .into());
            }
            let prefix = entry
                .journal
                .iter()
                .filter(|record| {
                    record.event.run_seq <= last_confirmed_seq
                        && matches!(record.authority, AgentEventAuthority::Provider)
                })
                .map(|record| {
                    (
                        record.event.event_id.clone(),
                        record.draft_digest.clone(),
                        // Command dispositions are synchronous control-plane
                        // responses that the Host already validated against
                        // and committed with the command envelope. They do
                        // not belong to the Provider's native observation
                        // stream after a process restart, so requiring an
                        // adapter to replay them makes otherwise valid native
                        // continuity unrecoverable.
                        !matches!(
                            record.event.payload,
                            AgentEvent::CommandDispositionRecorded { .. }
                        ),
                    )
                })
                .collect::<Vec<_>>();
            (
                entry.request.clone(),
                entry.execution.clone(),
                prefix,
                last_confirmed_seq,
                loss_record.event.event_digest.clone(),
            )
        };

        let recovery =
            AgentRecoveryRequest::new(start_request, execution.clone(), &self.descriptor)?;
        let recovered = self.provider.recover(recovery).await?;
        let (mut stream, confirmation) = recovered.into_parts();
        let mut matched_digests = Vec::with_capacity(expected_provider_prefix.len());
        let mut optional_stream_replays = Vec::new();
        for (expected_event_id, expected_draft_digest, requires_stream_replay) in
            &expected_provider_prefix
        {
            if !requires_stream_replay {
                matched_digests.push(expected_draft_digest.clone());
                optional_stream_replays
                    .push((expected_event_id.clone(), expected_draft_digest.clone()));
                continue;
            }
            loop {
                let recovered_item = timeout(RECOVERY_PREFIX_ITEM_TIMEOUT, stream.next())
                    .await
                    .map_err(|_| AgentControlError::RecoveryMismatch(run_id.clone()))?;
                match recovered_item {
                    Some(Ok(AgentProviderStreamItem::Telemetry(telemetry))) => {
                        telemetry.validate_integrity()?;
                        if telemetry.run_id != *run_id {
                            return Err(AgentControlError::RecoveryMismatch(run_id.clone()));
                        }
                        slot.publish(AgentControlEvent::Telemetry(telemetry));
                    }
                    Some(Ok(AgentProviderStreamItem::Event(draft))) => {
                        let draft_digest = draft.computed_digest()?;
                        if draft.event_id == *expected_event_id
                            && draft_digest == *expected_draft_digest
                        {
                            // Any earlier optional command dispositions that
                            // were not emitted are already proven by the Host
                            // command journal. They cannot validly appear
                            // after this later native observation.
                            optional_stream_replays.clear();
                            matched_digests.push(expected_draft_digest.clone());
                            break;
                        }
                        if let Some(position) = optional_stream_replays.iter().position(
                            |(optional_event_id, optional_digest)| {
                                draft.event_id == *optional_event_id
                                    && draft_digest == *optional_digest
                            },
                        ) {
                            optional_stream_replays.drain(..=position);
                            continue;
                        }
                        return Err(AgentControlError::RecoveryMismatch(run_id.clone()));
                    }
                    Some(Ok(_)) | Some(Err(_)) | None => {
                        return Err(AgentControlError::RecoveryMismatch(run_id.clone()));
                    }
                }
            }
        }

        let mut recovered_fingerprint = format!("{run_id}:{last_confirmed_seq}");
        for digest in &matched_digests {
            recovered_fingerprint.push(':');
            recovered_fingerprint.push_str(digest.as_str());
        }
        let proof = ReconciliationProof::new(
            ReconciliationProofRef::new(format!(
                "host-recovery/{}/{}",
                run_id.as_str(),
                last_confirmed_seq
            )),
            last_confirmed_seq,
            loss_event_digest,
            Digest::sha256(recovered_fingerprint),
        )?;
        let verifier = ExactReconciliationVerifier {
            execution: execution.clone(),
            proof: proof.clone(),
        };
        {
            let mut entry = slot.entry.lock().await;
            let mut next_reducer = entry.reducer.clone();
            let restored = next_reducer.apply_verified_reconciliation(
                AgentEventDraft {
                    event_id: AgentEventId::new(format!(
                        "host-continuity-restored-{}-{}",
                        run_id.as_str(),
                        last_confirmed_seq
                    )),
                    run_id: run_id.clone(),
                    causation_id: None,
                    source_fingerprint: None,
                    payload: AgentEvent::ContinuityRestored {
                        proof,
                        reason: "Provider recovery prefix matched the Host journal".to_owned(),
                    },
                },
                &verifier,
            )?;
            self.commit_sequenced(&slot, &mut entry, next_reducer, restored)
                .await?;
        }

        if let Err(error) = confirmation.await {
            self.mark_continuity_lost(
                run_id,
                &slot,
                format!("Provider could not resume after reconciliation: {error}"),
            )
            .await?;
            return Err(error.into());
        }

        let controller = Arc::clone(self);
        let recovered_run_id = run_id.clone();
        let recovered_slot = slot.clone();
        tokio::spawn(async move {
            controller
                .drive_stream(recovered_run_id, recovered_slot, stream)
                .await;
        });
        self.inspect(run_id).await
    }

    /// Waits until the Run reaches an authoritative terminal. Unknown is
    /// returned explicitly rather than guessed as success or cancellation.
    pub async fn wait_for_terminal(
        &self,
        run_id: &RunId,
    ) -> Result<AgentRunView, AgentControlError> {
        let slot = self.run_slot(run_id).await?;
        loop {
            let changed = slot.changed.notified();
            let view = slot.entry.lock().await.reducer.view();
            if view.state.is_terminal() {
                return Ok(view);
            }
            if view.state.status() == AgentRunStatus::Unknown {
                return Err(AgentControlError::ContinuityUnknown(run_id.clone()));
            }
            changed.await;
        }
    }

    async fn run_slot(&self, run_id: &RunId) -> Result<Arc<RunSlot>, AgentControlError> {
        if let Some(slot) = self.runs.read().await.get(run_id).cloned() {
            return Ok(slot);
        }
        let _start_guard = self.start_gate.lock().await;
        if let Some(slot) = self.runs.read().await.get(run_id).cloned() {
            return Ok(slot);
        }
        let stored = self
            .journal_store
            .load_run(run_id)
            .await?
            .ok_or_else(|| AgentControlError::RunNotFound(run_id.clone()))?;
        let slot = self.slot_from_stored(stored)?;
        self.runs.write().await.insert(run_id.clone(), slot.clone());
        let view = slot.entry.lock().await.reducer.view();
        if !view.state.is_terminal() && view.state.status() != AgentRunStatus::Unknown {
            self.mark_continuity_lost(
                run_id,
                &slot,
                "Host controller restarted without a continuously attached Provider stream"
                    .to_owned(),
            )
            .await?;
        }
        Ok(slot)
    }

    fn slot_from_stored(&self, stored: StoredAgentRun) -> Result<Arc<RunSlot>, AgentControlError> {
        stored.validate_shape()?;
        let registration = &stored.registration;
        registration
            .execution
            .validate_for(&registration.request, &self.descriptor)?;
        let compatibility = self
            .descriptor
            .descriptor
            .check_run_compatibility(&registration.request.run)
            .map_err(|rejection| {
                AgentProtocolError::new(AgentProtocolErrorCode::Unsupported, rejection.to_string())
                    .with_details(rejection.details)
            })?;
        registration
            .admission
            .validate_against(&registration.request.run, &compatibility)?;
        let mut reducer = AgentRunReducer::new(
            registration.execution.clone(),
            &registration.request,
            &self.descriptor,
            registration.admission.clone(),
        )?;
        for record in &stored.records {
            reducer.replay_journal_record(record.clone())?;
        }
        let (events, _) = broadcast::channel(self.event_buffer);
        Ok(Arc::new(RunSlot {
            entry: Mutex::new(RunEntry {
                request: registration.request.clone(),
                execution: registration.execution.clone(),
                reducer,
                journal: stored.records,
            }),
            recovery_gate: Mutex::new(()),
            events,
            changed: Notify::new(),
        }))
    }

    fn registration_matches_current(
        &self,
        request: &AgentStartRequest,
        execution: &AgentExecutionRef,
    ) -> bool {
        request.provider_binding == self.binding_ref
            && request.expected_descriptor_digest == self.descriptor.descriptor_digest
            && execution.binding_ref == self.binding_ref
            && execution.descriptor_digest == self.descriptor.descriptor_digest
    }

    async fn drive_stream(
        self: Arc<Self>,
        run_id: RunId,
        slot: Arc<RunSlot>,
        mut stream: AgentProviderStream,
    ) {
        let mut end_reason = "Provider stream ended before an authoritative terminal".to_owned();
        while let Some(item) = stream.next().await {
            match item {
                Ok(AgentProviderStreamItem::Event(draft)) => {
                    let mut entry = slot.entry.lock().await;
                    let mut next_reducer = entry.reducer.clone();
                    match next_reducer.apply_provider_draft(*draft) {
                        Ok(sequenced) => {
                            if let Err(error) = self
                                .commit_sequenced(&slot, &mut entry, next_reducer, sequenced)
                                .await
                            {
                                end_reason =
                                    format!("Provider event could not be journaled: {error}");
                                break;
                            }
                            if entry.reducer.state().is_terminal()
                                || entry.reducer.state().status() == AgentRunStatus::Unknown
                            {
                                return;
                            }
                        }
                        Err(error) => {
                            end_reason = format!("Provider event was rejected: {error}");
                            break;
                        }
                    }
                }
                Ok(AgentProviderStreamItem::Telemetry(telemetry)) => {
                    if let Err(error) = telemetry.validate_integrity() {
                        end_reason = format!("Provider telemetry was invalid: {error}");
                        break;
                    }
                    if telemetry.run_id != run_id {
                        end_reason = "Provider telemetry crossed a Run boundary".to_owned();
                        break;
                    }
                    slot.publish(AgentControlEvent::Telemetry(telemetry));
                }
                Ok(_) => {
                    end_reason = "Provider emitted an unsupported stream item".to_owned();
                    break;
                }
                Err(error) => {
                    end_reason = format!("Provider stream failed: {error}");
                    break;
                }
            }
        }
        if let Err(error) = self.mark_continuity_lost(&run_id, &slot, end_reason).await {
            tracing::error!(run_id = %run_id, error = %error, "failed to journal continuity loss");
        }
    }

    async fn commit_sequenced(
        &self,
        slot: &RunSlot,
        entry: &mut RunEntry,
        next_reducer: AgentRunReducer,
        sequenced: SequencedApply,
    ) -> Result<(), AgentControlError> {
        if matches!(sequenced.outcome, ApplyOutcome::ExactDuplicate) {
            return Ok(());
        }
        let expected_previous = entry.reducer.view().last_run_seq.unwrap_or(0);
        self.journal_store
            .append_record(
                &entry.execution.run_id,
                expected_previous,
                sequenced.record.clone(),
            )
            .await?;
        entry.reducer = next_reducer;
        entry.journal.push(sequenced.record.clone());
        slot.publish(AgentControlEvent::Durable(Arc::new(sequenced.record)));
        Ok(())
    }

    async fn mark_continuity_lost(
        &self,
        run_id: &RunId,
        slot: &Arc<RunSlot>,
        reason: String,
    ) -> Result<(), AgentControlError> {
        let mut entry = slot.entry.lock().await;
        let view = entry.reducer.view();
        if view.state.is_terminal() || view.state.status() == AgentRunStatus::Unknown {
            return Ok(());
        }
        let last_confirmed_seq = view.last_run_seq.unwrap_or(0);
        let draft = AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "host-continuity-lost-{}-{}",
                run_id.as_str(),
                last_confirmed_seq + 1
            )),
            run_id: run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::ContinuityLost {
                last_confirmed_seq,
                reason,
            },
        };
        let mut next_reducer = entry.reducer.clone();
        let sequenced = next_reducer.apply_host_draft(draft)?;
        self.commit_sequenced(slot, &mut entry, next_reducer, sequenced)
            .await?;
        Ok(())
    }
}

fn push_sequenced(journal: &mut Vec<AgentJournalRecord>, sequenced: SequencedApply) {
    if !matches!(sequenced.outcome, ApplyOutcome::ExactDuplicate) {
        journal.push(sequenced.record);
    }
}

struct ExactReconciliationVerifier {
    execution: AgentExecutionRef,
    proof: ReconciliationProof,
}

impl ReconciliationProofVerifier for ExactReconciliationVerifier {
    fn verify(
        &self,
        execution: &AgentExecutionRef,
        continuity: &AgentContinuityState,
        proof: &ReconciliationProof,
    ) -> Result<(), AgentProtocolError> {
        if execution != &self.execution || proof != &self.proof {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "reconciliation proof is not the controller-verified evidence",
            ));
        }
        let AgentContinuityState::Unknown {
            last_confirmed_seq,
            loss_event_digest,
            ..
        } = continuity
        else {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "reconciliation requires Unknown continuity",
            ));
        };
        if *last_confirmed_seq != proof.last_confirmed_seq
            || *loss_event_digest != proof.loss_event_digest
        {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "reconciliation proof is not bound to the current continuity loss",
            ));
        }
        proof.verify_integrity()
    }
}
