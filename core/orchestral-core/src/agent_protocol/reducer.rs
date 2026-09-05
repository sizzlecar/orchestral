use std::collections::{BTreeMap, BTreeSet};

use super::types::{
    AgentAdmission, AgentCommand, AgentCommandEnvelope, AgentContinuityState, AgentDelivery,
    AgentDescriptorEnvelope, AgentEvent, AgentEventAuthority, AgentEventDraft, AgentEventEnvelope,
    AgentEventId, AgentExecutionRef, AgentExecutionSnapshot, AgentJournalRecord,
    AgentProtocolError, AgentProtocolErrorCode, AgentRunPhase, AgentRunState, AgentRunView,
    AgentStartRequest, AgentTerminalState, BindingRequirement, CommandAck, CommandId,
    CommandRecord, Digest, PartialDelivery, PendingRequestKind, ProviderCommandOutcome,
    ReconciliationProof, RunId,
};

/// Structural admission result before semantic state reduction.
#[derive(Debug, Clone)]
enum GateDecision {
    Admit(Box<AdmittedAgentEvent>),
    ExactDuplicate,
}

/// Event that passed identity, digest, and sequence validation.
#[derive(Debug, Clone)]
struct AdmittedAgentEvent(AgentJournalRecord);

impl AdmittedAgentEvent {
    pub fn record(&self) -> &AgentJournalRecord {
        &self.0
    }

    pub fn event(&self) -> &AgentEventEnvelope {
        &self.0.event
    }
}

/// Immutable identity retained by the journal's `(run_id, run_seq)` index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AdmittedEventIdentity {
    pub event_id: AgentEventId,
    pub event_digest: Digest,
    pub draft_digest: Digest,
    pub authority: AgentEventAuthority,
}

/// In-memory reference implementation of the journal admission gate.
///
/// A durable implementation must commit this index and its journal record in
/// one transaction. This type exists for deterministic reducer/conformance
/// tests and is not a crash-consistent production store.
#[derive(Debug, Clone)]
struct AgentJournalGate {
    run_id: RunId,
    by_seq: BTreeMap<u64, AdmittedEventIdentity>,
    event_ids: BTreeMap<AgentEventId, u64>,
    records_by_seq: BTreeMap<u64, AgentJournalRecord>,
}

impl AgentJournalGate {
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            by_seq: BTreeMap::new(),
            event_ids: BTreeMap::new(),
            records_by_seq: BTreeMap::new(),
        }
    }

    fn existing_draft(
        &self,
        draft: &AgentEventDraft,
        authority: &AgentEventAuthority,
    ) -> Result<Option<AgentJournalRecord>, AgentProtocolError> {
        if draft.run_id != self.run_id {
            return Err(protocol_error(
                AgentProtocolErrorCode::RunIdConflict,
                "draft run_id does not match the journal gate",
            ));
        }
        if draft.event_id.is_empty() {
            return Err(protocol_error(
                AgentProtocolErrorCode::InvalidSpec,
                "event_id must not be empty",
            ));
        }
        let Some(existing_seq) = self.event_ids.get(&draft.event_id) else {
            return Ok(None);
        };
        let existing = self
            .records_by_seq
            .get(existing_seq)
            .expect("committed event index must reference a record");
        if existing.draft_digest == draft.computed_digest()? && existing.authority == *authority {
            return Ok(Some(existing.clone()));
        }
        Err(protocol_error(
            AgentProtocolErrorCode::DuplicateConflict,
            "event_id is already bound to a different draft digest or authority",
        ))
    }

    pub fn check(&self, record: &AgentJournalRecord) -> Result<GateDecision, AgentProtocolError> {
        record.validate_integrity()?;
        let event = &record.event;
        if event.run_id != self.run_id {
            return Err(protocol_error(
                AgentProtocolErrorCode::RunIdConflict,
                "event run_id does not match the journal gate",
            ));
        }
        if event.event_id.is_empty() || event.run_seq == 0 {
            return Err(protocol_error(
                AgentProtocolErrorCode::InvalidSpec,
                "event_id must be non-empty and run_seq starts at one",
            ));
        }
        if let Some(existing) = self.by_seq.get(&event.run_seq) {
            if existing.event_id == event.event_id
                && existing.event_digest == event.event_digest
                && existing.draft_digest == record.draft_digest
                && existing.authority == record.authority
            {
                return Ok(GateDecision::ExactDuplicate);
            }
            return Err(protocol_error(
                AgentProtocolErrorCode::SequenceConflict,
                "run_seq is already bound to a different event identity or digest",
            ));
        }
        if let Some(existing_seq) = self.event_ids.get(&event.event_id) {
            return Err(protocol_error(
                AgentProtocolErrorCode::DuplicateConflict,
                format!(
                    "event_id is already bound to run_seq {existing_seq}, not {}",
                    event.run_seq
                ),
            ));
        }

        let expected = self.by_seq.last_key_value().map_or(1, |(seq, _)| seq + 1);
        if event.run_seq != expected {
            let code = if event.run_seq > expected {
                AgentProtocolErrorCode::SequenceGap
            } else {
                AgentProtocolErrorCode::SequenceConflict
            };
            return Err(protocol_error(
                code,
                format!("expected run_seq {expected}, got {}", event.run_seq),
            ));
        }

        Ok(GateDecision::Admit(Box::new(AdmittedAgentEvent(
            record.clone(),
        ))))
    }

    fn commit(&mut self, admitted: &AdmittedAgentEvent) {
        let event = admitted.event();
        let identity = AdmittedEventIdentity {
            event_id: event.event_id.clone(),
            event_digest: event.event_digest.clone(),
            draft_digest: admitted.record().draft_digest.clone(),
            authority: admitted.record().authority.clone(),
        };
        self.event_ids.insert(event.event_id.clone(), event.run_seq);
        self.by_seq.insert(event.run_seq, identity);
        self.records_by_seq
            .insert(event.run_seq, admitted.record().clone());
    }

    fn contains_committed_event(&self, event_id: &AgentEventId) -> bool {
        self.event_ids.contains_key(event_id)
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ApplyOutcome {
    Applied,
    ExactDuplicate,
    /// Structurally valid event with illegal state semantics. Its sequence is
    /// quarantined and continuity becomes Unknown, so later events cannot make
    /// the run appear healthy without explicit reconciliation.
    ProtocolViolation {
        error: AgentProtocolError,
    },
}

#[derive(Debug, Clone)]
pub struct SequencedApply {
    pub record: AgentJournalRecord,
    pub outcome: ApplyOutcome,
}

impl SequencedApply {
    pub fn event(&self) -> &AgentEventEnvelope {
        &self.record.event
    }
}

/// Host-owned verifier for a reconciliation record. Implementations resolve
/// `proof_ref` against private durable state; checking the public self-digest
/// alone is never sufficient.
pub trait ReconciliationProofVerifier: Send + Sync {
    fn verify(
        &self,
        execution: &AgentExecutionRef,
        continuity: &AgentContinuityState,
        proof: &ReconciliationProof,
    ) -> Result<(), AgentProtocolError>;
}

/// Deterministic online projection with an in-memory structural journal gate.
#[derive(Debug, Clone)]
pub struct AgentRunReducer {
    snapshot: AgentExecutionSnapshot,
    gate: AgentJournalGate,
    selected_contract: SelectedAgentContract,
}

#[derive(Debug, Clone)]
struct SelectedAgentContract {
    pending_request_kinds: BTreeSet<PendingRequestKind>,
}

impl AgentRunReducer {
    pub fn new(
        execution: AgentExecutionRef,
        request: &AgentStartRequest,
        descriptor: &AgentDescriptorEnvelope,
        admission: AgentAdmission,
    ) -> Result<Self, AgentProtocolError> {
        execution.validate_for(request, descriptor)?;
        let compatibility = descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(|rejection| {
                AgentProtocolError::new(AgentProtocolErrorCode::Unsupported, rejection.to_string())
                    .with_details(rejection.details)
            })?;
        admission.validate_against(&request.run, &compatibility)?;
        Ok(Self {
            gate: AgentJournalGate::new(execution.run_id.clone()),
            snapshot: AgentExecutionSnapshot::accepted(execution, &request.run, admission),
            selected_contract: SelectedAgentContract {
                pending_request_kinds: descriptor
                    .descriptor
                    .capabilities
                    .pending_request_kinds
                    .clone(),
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> &AgentExecutionSnapshot {
        &self.snapshot
    }

    pub fn state(&self) -> AgentRunState {
        self.snapshot.state()
    }

    /// Returns the stable, bounded Host read model. Journal output history is
    /// queried separately up to `last_run_seq`.
    pub fn view(&self) -> AgentRunView {
        AgentRunView {
            execution: self.snapshot.execution.clone(),
            admission: self.snapshot.admission.clone(),
            state: self.snapshot.state(),
            last_run_seq: self.snapshot.last_run_seq,
            pending_requests: self.snapshot.pending_requests.values().cloned().collect(),
            delivery: self.snapshot.delivery.clone(),
            partial_delivery: self.snapshot.partial_delivery.clone(),
        }
    }

    /// Returns the immutable envelope associated with a durable command id.
    pub fn recorded_command(&self, command_id: &CommandId) -> Option<&AgentCommandEnvelope> {
        self.snapshot
            .commands
            .get(command_id)
            .map(|record| &record.command)
    }

    /// Projects the durable disposition of a previously received command.
    pub fn command_ack(
        &self,
        command_id: &CommandId,
        duplicate: bool,
    ) -> Result<CommandAck, AgentProtocolError> {
        self.snapshot
            .commands
            .get(command_id)
            .ok_or_else(|| {
                protocol_error(
                    AgentProtocolErrorCode::CommandNotFound,
                    "command is absent from this Run",
                )
            })?
            .to_ack(duplicate)
    }

    /// Assigns the next normalized `run_seq` and applies the draft through the
    /// same structural and semantic gates. Provider-native cursors remain in
    /// adapter-private state; only an optional source fingerprint is public.
    pub fn apply_provider_draft(
        &mut self,
        draft: AgentEventDraft,
    ) -> Result<SequencedApply, AgentProtocolError> {
        let authority = AgentEventAuthority::Provider;
        if let Some(record) = self.gate.existing_draft(&draft, &authority)? {
            return Ok(SequencedApply {
                record,
                outcome: ApplyOutcome::ExactDuplicate,
            });
        }
        let next_seq = self.snapshot.last_run_seq.map_or(1, |seq| seq + 1);
        let record = AgentJournalRecord::seal_provider(draft, next_seq)?;
        let outcome = self.apply_record(record.clone())?;
        Ok(SequencedApply { record, outcome })
    }

    /// Sequences a Host-derived event such as `ContinuityLost`. Restoration is
    /// intentionally excluded and must use the verified API below.
    pub fn apply_host_draft(
        &mut self,
        draft: AgentEventDraft,
    ) -> Result<SequencedApply, AgentProtocolError> {
        if matches!(draft.payload, AgentEvent::ContinuityRestored { .. }) {
            return Err(protocol_error(
                AgentProtocolErrorCode::InvalidSpec,
                "ContinuityRestored requires the verified reconciliation API",
            ));
        }
        let authority = AgentEventAuthority::Host {
            reconciliation_proof_ref: None,
        };
        if let Some(record) = self.gate.existing_draft(&draft, &authority)? {
            return Ok(SequencedApply {
                record,
                outcome: ApplyOutcome::ExactDuplicate,
            });
        }
        let next_seq = self.snapshot.last_run_seq.map_or(1, |seq| seq + 1);
        let record = AgentJournalRecord::seal_host(draft, next_seq, None)?;
        let outcome = self.apply_record(record.clone())?;
        Ok(SequencedApply { record, outcome })
    }

    /// Sequences a Host-only continuity restoration after consulting the
    /// private reconciliation proof store.
    pub fn apply_verified_reconciliation(
        &mut self,
        draft: AgentEventDraft,
        verifier: &dyn ReconciliationProofVerifier,
    ) -> Result<SequencedApply, AgentProtocolError> {
        let AgentEvent::ContinuityRestored { proof, .. } = &draft.payload else {
            return Err(protocol_error(
                AgentProtocolErrorCode::InvalidSpec,
                "Host reconciliation API only accepts ContinuityRestored",
            ));
        };
        verifier.verify(&self.snapshot.execution, &self.snapshot.continuity, proof)?;
        let proof_ref = proof.proof_ref.clone();
        let authority = AgentEventAuthority::Host {
            reconciliation_proof_ref: Some(proof_ref.clone()),
        };
        if let Some(record) = self.gate.existing_draft(&draft, &authority)? {
            return Ok(SequencedApply {
                record,
                outcome: ApplyOutcome::ExactDuplicate,
            });
        }
        let next_seq = self.snapshot.last_run_seq.map_or(1, |seq| seq + 1);
        let record = AgentJournalRecord::seal_host(draft, next_seq, Some(proof_ref))?;
        let outcome = self.apply_record(record.clone())?;
        Ok(SequencedApply { record, outcome })
    }

    /// Rebuilds projection from a Host-authenticated journal. Callers must not
    /// use this entry point for untrusted Provider stream data.
    pub fn replay_journal_record(
        &mut self,
        record: AgentJournalRecord,
    ) -> Result<ApplyOutcome, AgentProtocolError> {
        self.apply_record(record)
    }

    /// Applies one event atomically in memory. A structurally valid semantic
    /// violation is quarantined, consumes its `run_seq`, and forces Unknown.
    /// Sequencing APIs return both the envelope and outcome
    /// so a durable controller can persist them together rather than diverging.
    fn apply_record(
        &mut self,
        record: AgentJournalRecord,
    ) -> Result<ApplyOutcome, AgentProtocolError> {
        let admitted = match self.gate.check(&record)? {
            GateDecision::ExactDuplicate => return Ok(ApplyOutcome::ExactDuplicate),
            GateDecision::Admit(admitted) => admitted,
        };

        let mut next = self.snapshot.clone();
        let reduction = validate_supporting_event_references(&self.gate, admitted.event())
            .and_then(|()| reduce_snapshot(&mut next, admitted.event(), &self.selected_contract));
        match reduction {
            Ok(()) => {
                self.gate.commit(&admitted);
                self.snapshot = next;
                Ok(ApplyOutcome::Applied)
            }
            Err(error) => {
                // A terminal result remains authoritative; tail events are
                // rejected without changing its projection or canonical seq.
                if matches!(self.snapshot.phase, AgentRunPhase::Terminal { .. }) {
                    return Err(error);
                }
                let last_confirmed_seq = match self.snapshot.continuity {
                    AgentContinuityState::Confirmed { through_seq } => through_seq,
                    AgentContinuityState::Unknown {
                        last_confirmed_seq, ..
                    } => last_confirmed_seq,
                };
                self.gate.commit(&admitted);
                self.snapshot.last_run_seq = Some(admitted.event().run_seq);
                self.snapshot.last_event_id = Some(admitted.event().event_id.clone());
                self.snapshot.last_event_digest = Some(admitted.event().event_digest.clone());
                self.snapshot.continuity = AgentContinuityState::Unknown {
                    last_confirmed_seq,
                    loss_event_digest: admitted.event().event_digest.clone(),
                    reason: format!("protocol violation: {error}"),
                };
                Ok(ApplyOutcome::ProtocolViolation { error })
            }
        }
    }
}

fn validate_supporting_event_references(
    gate: &AgentJournalGate,
    envelope: &AgentEventEnvelope,
) -> Result<(), AgentProtocolError> {
    let supporting_event_ids = match &envelope.payload {
        AgentEvent::DeliveryCommitted { delivery } => {
            delivery.provenance.supporting_event_ids.as_slice()
        }
        AgentEvent::RunIncomplete {
            partial_delivery: Some(partial),
            ..
        } => partial.provenance.supporting_event_ids.as_slice(),
        _ => return Ok(()),
    };
    if supporting_event_ids
        .iter()
        .any(|event_id| !gate.contains_committed_event(event_id))
    {
        return Err(invalid_transition(
            "delivery provenance references a missing, foreign, or not-yet-committed event",
        ));
    }
    Ok(())
}

fn reduce_snapshot(
    snapshot: &mut AgentExecutionSnapshot,
    envelope: &AgentEventEnvelope,
    selected_contract: &SelectedAgentContract,
) -> Result<(), AgentProtocolError> {
    if matches!(snapshot.phase, AgentRunPhase::Terminal { .. }) {
        return Err(protocol_error(
            AgentProtocolErrorCode::TerminalRun,
            "a terminal run rejects every new durable event",
        ));
    }

    if matches!(snapshot.continuity, AgentContinuityState::Unknown { .. })
        && !matches!(envelope.payload, AgentEvent::ContinuityRestored { .. })
    {
        return Err(protocol_error(
            AgentProtocolErrorCode::InvalidTransition,
            "continuity is unknown; only a reconciled ContinuityRestored event is admissible",
        ));
    }

    match &envelope.payload {
        AgentEvent::RunAccepted {
            session_id,
            spec_digest,
        } => {
            if snapshot.run_accepted
                || envelope.run_seq != 1
                || *session_id != snapshot.execution.session_id
                || *spec_digest != snapshot.execution.spec_digest
            {
                return Err(invalid_transition("invalid or conflicting RunAccepted"));
            }
            snapshot.run_accepted = true;
        }
        AgentEvent::ResourceBindingSkipped { skip } => {
            require_accepted_not_started(snapshot, "ResourceBindingSkipped")?;
            let expected = snapshot
                .admission
                .skipped_optional_bindings
                .iter()
                .find(|expected| expected.binding_id == skip.binding_id)
                .ok_or_else(|| {
                    invalid_transition("resource skip was not declared by start admission")
                })?;
            if expected != skip {
                return Err(invalid_transition(
                    "resource skip differs from the start admission",
                ));
            }
            match snapshot.resource_requirements.get(&skip.binding_id) {
                Some(BindingRequirement::Optional) => {}
                Some(BindingRequirement::Required) => {
                    return Err(invalid_transition(
                        "required resource binding cannot be silently skipped",
                    ));
                }
                None => {
                    return Err(invalid_transition(
                        "skipped resource binding is not present in the run spec",
                    ));
                }
            }
            if snapshot
                .skipped_optional_bindings
                .insert(skip.binding_id.clone(), skip.clone())
                .is_some()
            {
                return Err(invalid_transition(
                    "optional resource binding was already reported as skipped",
                ));
            }
        }
        AgentEvent::RunStarted => {
            require_accepted_not_started(snapshot, "RunStarted")?;
            let expected = snapshot
                .admission
                .skipped_optional_bindings
                .iter()
                .cloned()
                .map(|skip| (skip.binding_id.clone(), skip))
                .collect::<BTreeMap<_, _>>();
            if snapshot.skipped_optional_bindings != expected {
                return Err(invalid_transition(
                    "RunStarted requires the exact admitted resource skip set",
                ));
            }
            snapshot.phase = AgentRunPhase::Running;
        }
        AgentEvent::CommandReceived { command } => {
            require_run_accepted(snapshot)?;
            validate_command_causation(envelope, command)?;
            receive_command(snapshot, command, envelope.run_seq)?;
        }
        AgentEvent::CommandDispositionRecorded {
            command_id,
            outcome,
        } => {
            require_run_accepted(snapshot)?;
            validate_command_id_causation(envelope, command_id)?;
            record_command_disposition(snapshot, command_id, outcome, envelope.run_seq)?;
        }
        AgentEvent::InputCommitted { content } => {
            require_phase(snapshot, &[PhaseKind::Running], "InputCommitted")?;
            let (command_id, command) = causal_command(snapshot, envelope)?;
            match &command.payload {
                AgentCommand::Steer { content: requested } if requested == content => {}
                _ => {
                    return Err(invalid_transition(
                        "InputCommitted must exactly match its causal Steer command",
                    ));
                }
            }
            if snapshot
                .committed_inputs
                .insert(command_id.clone(), content.clone())
                .is_some()
            {
                return Err(invalid_transition("Steer command was already committed"));
            }
            mark_command_applied(snapshot, &command_id, envelope.run_seq)?;
        }
        AgentEvent::OutputCommitted { output_id, content } => {
            require_phase(
                snapshot,
                &[PhaseKind::Running, PhaseKind::Stopping],
                "OutputCommitted",
            )?;
            if snapshot
                .committed_outputs
                .insert(output_id.clone(), content.clone())
                .is_some()
            {
                return Err(invalid_transition("output_id was already committed"));
            }
        }
        AgentEvent::RequestOpened { request } => {
            require_phase(snapshot, &[PhaseKind::Running], "RequestOpened")?;
            if !selected_contract
                .pending_request_kinds
                .contains(&request.kind())
            {
                return Err(invalid_transition(
                    "Provider opened a pending request kind absent from its selected descriptor",
                ));
            }
            if snapshot.pending_requests.contains_key(&request.request_id)
                || snapshot.resolved_requests.contains_key(&request.request_id)
                || snapshot.closed_requests.contains(&request.request_id)
            {
                return Err(invalid_transition("request_id was already used"));
            }
            snapshot
                .pending_requests
                .insert(request.request_id.clone(), request.clone());
        }
        AgentEvent::RequestResolved {
            request_id,
            resolution,
            resolution_digest,
        } => {
            require_phase(
                snapshot,
                &[PhaseKind::Running, PhaseKind::Stopping],
                "RequestResolved",
            )?;
            let pending = snapshot
                .pending_requests
                .get(request_id)
                .ok_or_else(|| invalid_transition("request_id is not pending"))?;
            if pending.kind() != resolution.kind() {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestTypeMismatch,
                    "request resolution kind does not match the pending request",
                ));
            }
            let (_, command) = causal_command(snapshot, envelope)?;
            match (&command.request_id, &command.payload) {
                (Some(command_request_id), AgentCommand::ResolveRequest { response })
                    if command_request_id == request_id && response == resolution => {}
                _ => {
                    return Err(invalid_transition(
                        "RequestResolved does not match its causal command",
                    ));
                }
            }
            snapshot.pending_requests.remove(request_id);
            snapshot
                .resolved_requests
                .insert(request_id.clone(), resolution_digest.clone());
            let command_id = envelope
                .causation_id
                .as_ref()
                .expect("causal command was checked")
                .clone();
            mark_command_applied(snapshot, &command_id, envelope.run_seq)?;
        }
        AgentEvent::RequestClosed { request_id, .. } => {
            require_phase(
                snapshot,
                &[PhaseKind::Running, PhaseKind::Stopping],
                "RequestClosed",
            )?;
            if snapshot.pending_requests.remove(request_id).is_none() {
                return Err(invalid_transition("request_id is not pending"));
            }
            snapshot.closed_requests.insert(request_id.clone());
        }
        AgentEvent::StopRequested { reason } => {
            require_phase(
                snapshot,
                &[PhaseKind::Accepted, PhaseKind::Running],
                "StopRequested",
            )?;
            let (_, command) = causal_command(snapshot, envelope)?;
            match &command.payload {
                AgentCommand::Cancel { reason: requested } if requested == reason => {}
                _ => {
                    return Err(invalid_transition(
                        "StopRequested must exactly match its causal Cancel command",
                    ));
                }
            }
            snapshot.phase = AgentRunPhase::Stopping;
            let command_id = envelope
                .causation_id
                .as_ref()
                .expect("causal command was checked")
                .clone();
            mark_command_applied(snapshot, &command_id, envelope.run_seq)?;
        }
        AgentEvent::DeliveryCommitted { delivery } => {
            require_phase(
                snapshot,
                &[PhaseKind::Running, PhaseKind::Stopping],
                "DeliveryCommitted",
            )?;
            if !snapshot.pending_requests.is_empty() {
                return Err(invalid_transition(
                    "a complete delivery cannot leave unresolved requests",
                ));
            }
            validate_delivery_binding(snapshot, delivery)?;
            snapshot.delivery = Some(delivery.clone());
            snapshot.phase = AgentRunPhase::Terminal {
                terminal: AgentTerminalState::Delivered {
                    delivery_id: delivery.delivery_id.clone(),
                },
            };
        }
        AgentEvent::RunIncomplete {
            reason,
            partial_delivery,
        } => {
            require_run_accepted(snapshot)?;
            if let Some(partial) = partial_delivery {
                validate_partial_delivery_binding(snapshot, partial)?;
                snapshot.partial_delivery = Some(partial.clone());
            }
            snapshot.pending_requests.clear();
            snapshot.phase = AgentRunPhase::Terminal {
                terminal: AgentTerminalState::Incomplete {
                    reason: reason.clone(),
                },
            };
        }
        AgentEvent::RunFailed { failure } => {
            require_run_accepted(snapshot)?;
            snapshot.pending_requests.clear();
            snapshot.phase = AgentRunPhase::Terminal {
                terminal: AgentTerminalState::Failed {
                    failure: failure.clone(),
                },
            };
        }
        AgentEvent::RunCancelled { reason } => {
            require_phase(snapshot, &[PhaseKind::Stopping], "RunCancelled")?;
            snapshot.pending_requests.clear();
            snapshot.phase = AgentRunPhase::Terminal {
                terminal: AgentTerminalState::Cancelled {
                    reason: reason.clone(),
                },
            };
        }
        AgentEvent::ContinuityLost {
            last_confirmed_seq,
            reason,
        } => {
            require_run_accepted(snapshot)?;
            let previous_seq = snapshot.last_run_seq.unwrap_or(0);
            if *last_confirmed_seq != previous_seq {
                return Err(invalid_transition(
                    "ContinuityLost must name the previously confirmed run_seq",
                ));
            }
            snapshot.continuity = AgentContinuityState::Unknown {
                last_confirmed_seq: *last_confirmed_seq,
                loss_event_digest: envelope.event_digest.clone(),
                reason: reason.clone(),
            };
        }
        AgentEvent::ContinuityRestored { proof, .. } => {
            let AgentContinuityState::Unknown {
                last_confirmed_seq,
                loss_event_digest,
                ..
            } = &snapshot.continuity
            else {
                return Err(invalid_transition(
                    "ContinuityRestored is valid only after continuity became Unknown",
                ));
            };
            proof.verify_integrity()?;
            if proof.last_confirmed_seq != *last_confirmed_seq
                || proof.loss_event_digest != *loss_event_digest
            {
                return Err(invalid_transition(
                    "reconciliation proof is not bound to the current continuity loss",
                ));
            }
            snapshot.continuity = AgentContinuityState::Confirmed {
                through_seq: envelope.run_seq,
            };
        }
    }

    snapshot.last_run_seq = Some(envelope.run_seq);
    snapshot.last_event_id = Some(envelope.event_id.clone());
    snapshot.last_event_digest = Some(envelope.event_digest.clone());
    if !matches!(envelope.payload, AgentEvent::ContinuityLost { .. }) {
        snapshot.continuity = AgentContinuityState::Confirmed {
            through_seq: envelope.run_seq,
        };
    }
    Ok(())
}

fn require_run_accepted(snapshot: &AgentExecutionSnapshot) -> Result<(), AgentProtocolError> {
    if snapshot.run_accepted {
        Ok(())
    } else {
        Err(invalid_transition("RunAccepted must be the first event"))
    }
}

fn require_accepted_not_started(
    snapshot: &AgentExecutionSnapshot,
    event: &str,
) -> Result<(), AgentProtocolError> {
    require_run_accepted(snapshot)?;
    require_phase(snapshot, &[PhaseKind::Accepted], event)
}

#[derive(Clone, Copy)]
enum PhaseKind {
    Accepted,
    Running,
    Stopping,
}

fn require_phase(
    snapshot: &AgentExecutionSnapshot,
    allowed: &[PhaseKind],
    event: &str,
) -> Result<(), AgentProtocolError> {
    let actual = match snapshot.phase {
        AgentRunPhase::Accepted => Some(PhaseKind::Accepted),
        AgentRunPhase::Running => Some(PhaseKind::Running),
        AgentRunPhase::Stopping => Some(PhaseKind::Stopping),
        AgentRunPhase::Terminal { .. } => None,
    };
    let is_allowed = actual.is_some_and(|actual| {
        allowed
            .iter()
            .any(|allowed| std::mem::discriminant(allowed) == std::mem::discriminant(&actual))
    });
    if is_allowed {
        Ok(())
    } else {
        Err(invalid_transition(format!(
            "{event} is not allowed in the current phase"
        )))
    }
}

fn validate_command_causation(
    envelope: &AgentEventEnvelope,
    command: &AgentCommandEnvelope,
) -> Result<(), AgentProtocolError> {
    command.verify_digest()?;
    if command.run_id != envelope.run_id
        || envelope.causation_id.as_ref() != Some(&command.command_id)
    {
        return Err(invalid_transition(
            "command disposition has invalid run or causation binding",
        ));
    }
    Ok(())
}

fn validate_command_id_causation(
    envelope: &AgentEventEnvelope,
    command_id: &CommandId,
) -> Result<(), AgentProtocolError> {
    if envelope.causation_id.as_ref() != Some(command_id) {
        return Err(invalid_transition(
            "command disposition has invalid causation binding",
        ));
    }
    Ok(())
}

fn receive_command(
    snapshot: &mut AgentExecutionSnapshot,
    command: &AgentCommandEnvelope,
    run_seq: u64,
) -> Result<(), AgentProtocolError> {
    if snapshot.commands.contains_key(&command.command_id) {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::DuplicateConflict,
            "command_id was already received; Host must return the existing ledger result",
        ));
    }
    if let AgentCommand::ResolveRequest { response } = &command.payload {
        let request_id = command
            .request_id
            .as_ref()
            .expect("ResolveRequest shape validation requires request_id");
        let pending = snapshot.pending_requests.get(request_id).ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::RequestNotFound,
                "ResolveRequest does not target an open request",
            )
        })?;
        if pending.kind() != response.kind() {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RequestTypeMismatch,
                "ResolveRequest payload kind does not match the open request",
            ));
        }
    }
    snapshot.commands.insert(
        command.command_id.clone(),
        CommandRecord {
            command: command.clone(),
            received_seq: run_seq,
            disposition: None,
            disposition_seq: None,
            applied_seq: None,
        },
    );
    Ok(())
}

fn record_command_disposition(
    snapshot: &mut AgentExecutionSnapshot,
    command_id: &CommandId,
    outcome: &ProviderCommandOutcome,
    run_seq: u64,
) -> Result<(), AgentProtocolError> {
    let record = snapshot
        .commands
        .get_mut(command_id)
        .ok_or_else(|| invalid_transition("command disposition precedes CommandReceived"))?;
    if record.disposition.is_some() {
        return Err(invalid_transition(
            "command disposition was already durably recorded",
        ));
    }
    record.disposition = Some(outcome.clone());
    record.disposition_seq = Some(run_seq);
    Ok(())
}

fn mark_command_applied(
    snapshot: &mut AgentExecutionSnapshot,
    command_id: &CommandId,
    run_seq: u64,
) -> Result<(), AgentProtocolError> {
    let record = snapshot
        .commands
        .get_mut(command_id)
        .ok_or_else(|| invalid_transition("causal command is absent"))?;
    if !matches!(record.disposition, Some(ProviderCommandOutcome::Accepted)) {
        return Err(invalid_transition(
            "only an accepted Provider command can be applied",
        ));
    }
    if record.applied_seq.replace(run_seq).is_some() {
        return Err(invalid_transition("command effect was already applied"));
    }
    Ok(())
}

fn causal_command<'a>(
    snapshot: &'a AgentExecutionSnapshot,
    envelope: &AgentEventEnvelope,
) -> Result<(CommandId, &'a AgentCommandEnvelope), AgentProtocolError> {
    let command_id = envelope
        .causation_id
        .as_ref()
        .ok_or_else(|| invalid_transition("event requires a causal command_id"))?;
    let record = snapshot
        .commands
        .get(command_id)
        .ok_or_else(|| invalid_transition("causal command has not been durably recorded"))?;
    if !matches!(record.disposition, Some(ProviderCommandOutcome::Accepted)) {
        return Err(invalid_transition(
            "rejected or unsupported command cannot cause a state event",
        ));
    }
    Ok((command_id.clone(), &record.command))
}

fn validate_delivery_binding(
    snapshot: &AgentExecutionSnapshot,
    delivery: &AgentDelivery,
) -> Result<(), AgentProtocolError> {
    delivery.validate_integrity()?;
    if delivery.run_id != snapshot.execution.run_id
        || delivery.spec_digest != snapshot.execution.spec_digest
        || delivery.provenance.provider_id != snapshot.execution.provider_id
        || delivery.provenance.agent_id != snapshot.execution.agent_id
        || snapshot
            .output_schema
            .as_ref()
            .is_some_and(|schema| delivery.final_response.schema_id.as_ref() != Some(schema))
    {
        return Err(protocol_error(
            AgentProtocolErrorCode::InvalidDigest,
            "delivery is not bound to this run and spec digest",
        ));
    }
    Ok(())
}

fn validate_partial_delivery_binding(
    snapshot: &AgentExecutionSnapshot,
    partial: &PartialDelivery,
) -> Result<(), AgentProtocolError> {
    partial.validate_integrity()?;
    if partial.run_id != snapshot.execution.run_id
        || partial.spec_digest != snapshot.execution.spec_digest
        || partial.provenance.provider_id != snapshot.execution.provider_id
        || partial.provenance.agent_id != snapshot.execution.agent_id
        || snapshot.output_schema.as_ref().is_some_and(|schema| {
            partial
                .response
                .as_ref()
                .is_some_and(|response| response.schema_id.as_ref() != Some(schema))
        })
    {
        return Err(protocol_error(
            AgentProtocolErrorCode::InvalidDigest,
            "partial delivery is not bound to this run and spec digest",
        ));
    }
    Ok(())
}

fn invalid_transition(message: impl Into<String>) -> AgentProtocolError {
    protocol_error(AgentProtocolErrorCode::InvalidTransition, message)
}

fn protocol_error(code: AgentProtocolErrorCode, message: impl Into<String>) -> AgentProtocolError {
    AgentProtocolError::new(code, message)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::agent_protocol::types::AgentRunStatus;
    use crate::agent_protocol::wire::{
        AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery, AgentDescriptor,
        AgentDescriptorEnvelope, AgentEventDraft, AgentFailure, AgentId, AgentProviderId,
        AgentRunEnvelope, AgentSessionId, AgentStartRequest, BindingRequirement, CancelSupport,
        Content, ControlCapabilities, DeliveryId, EffectMediation, IncompleteReason, OutputId,
        PendingRequest, PendingRequestKind, PendingRequestPayload, Provenance, ProviderBindingRef,
        ProviderCommandOutcome, ReconciliationProof, ReconciliationProofRef, RequestId,
        RequestResolution, ResourceBinding, ResourceBindingId, ResourceBindingMode,
        ResourceBindingSkip, ResourceBindingSkipCode, ResourceCapability, ResourceId, ResourceKind,
        ResourceRef, ResourceRevision, RunId,
    };
    use crate::agent_protocol::AGENT_PROTOCOL_V1;

    fn sample_run(resources: Vec<ResourceBinding>) -> AgentRunEnvelope {
        let mut run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("session-1"),
            RunId::new("run-1"),
            vec![Content::text("do the work")],
        )
        .expect("sample run seals");
        run.spec.resources = resources;
        AgentRunEnvelope::seal(run.spec).expect("resource-bearing run seals")
    }

    fn descriptor(resources: &[ResourceBinding]) -> AgentDescriptorEnvelope {
        let mut by_kind = BTreeMap::<ResourceKind, BTreeSet<ResourceBindingMode>>::new();
        for binding in resources {
            by_kind
                .entry(binding.resource.kind.clone())
                .or_default()
                .insert(binding.mode);
        }
        AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new("provider-1"),
            agent_id: AgentId::new("agent-1"),
            supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: true,
                controls: ControlCapabilities {
                    steer: true,
                    cancel: CancelSupport::Confirmed,
                    recover: true,
                },
                pending_request_kinds: BTreeSet::from([
                    PendingRequestKind::Input,
                    PendingRequestKind::Approval,
                    PendingRequestKind::ExternalAction,
                ]),
                supported_limits: BTreeSet::new(),
                resources: by_kind
                    .into_iter()
                    .map(|(kind, modes)| ResourceCapability {
                        kind,
                        modes,
                        max_bindings: None,
                    })
                    .collect(),
                effect_mediation: EffectMediation::HostMediated,
            },
            extensions: Default::default(),
        })
        .expect("test descriptor seals")
    }

    fn start_request(
        run: AgentRunEnvelope,
        descriptor: &AgentDescriptorEnvelope,
    ) -> AgentStartRequest {
        AgentStartRequest {
            run,
            provider_binding: ProviderBindingRef::new("binding-1"),
            expected_descriptor_digest: descriptor.descriptor_digest.clone(),
        }
    }

    fn execution(
        request: &AgentStartRequest,
        descriptor: &AgentDescriptorEnvelope,
    ) -> AgentExecutionRef {
        AgentExecutionRef {
            provider_id: AgentProviderId::new("provider-1"),
            agent_id: AgentId::new("agent-1"),
            binding_ref: request.provider_binding.clone(),
            descriptor_digest: descriptor.descriptor_digest.clone(),
            session_id: request.run.spec.session_id.clone(),
            run_id: request.run.spec.run_id.clone(),
            spec_digest: request.run.spec_digest.clone(),
        }
    }

    fn make_reducer(resources: Vec<ResourceBinding>) -> (AgentRunEnvelope, AgentRunReducer) {
        make_reducer_with_admission(resources, AgentAdmission::default())
    }

    fn make_reducer_with_admission(
        resources: Vec<ResourceBinding>,
        admission: AgentAdmission,
    ) -> (AgentRunEnvelope, AgentRunReducer) {
        let descriptor = descriptor(&resources);
        make_reducer_with_descriptor(resources, admission, descriptor)
    }

    fn make_reducer_with_descriptor(
        resources: Vec<ResourceBinding>,
        admission: AgentAdmission,
        descriptor: AgentDescriptorEnvelope,
    ) -> (AgentRunEnvelope, AgentRunReducer) {
        let run = sample_run(resources);
        let request = start_request(run.clone(), &descriptor);
        let reducer = AgentRunReducer::new(
            execution(&request, &descriptor),
            &request,
            &descriptor,
            admission,
        )
        .expect("reducer initializes");
        (run, reducer)
    }

    fn draft(
        reducer: &AgentRunReducer,
        payload: AgentEvent,
        causation_id: Option<CommandId>,
    ) -> AgentEventDraft {
        let next = reducer.snapshot().last_run_seq.map_or(1, |seq| seq + 1);
        AgentEventDraft {
            event_id: AgentEventId::new(format!("event-{next}")),
            run_id: reducer.snapshot().execution.run_id.clone(),
            causation_id,
            source_fingerprint: None,
            payload,
        }
    }

    fn apply(
        reducer: &mut AgentRunReducer,
        payload: AgentEvent,
        causation_id: Option<CommandId>,
    ) -> AgentJournalRecord {
        let draft = draft(reducer, payload, causation_id);
        let sequenced = reducer
            .apply_provider_draft(draft)
            .expect("event should sequence");
        assert!(matches!(sequenced.outcome, ApplyOutcome::Applied));
        sequenced.record
    }

    fn apply_host(reducer: &mut AgentRunReducer, payload: AgentEvent) -> AgentJournalRecord {
        let draft = draft(reducer, payload, None);
        let sequenced = reducer
            .apply_host_draft(draft)
            .expect("Host event should sequence");
        assert!(matches!(sequenced.outcome, ApplyOutcome::Applied));
        sequenced.record
    }

    fn apply_host_causal(
        reducer: &mut AgentRunReducer,
        payload: AgentEvent,
        command_id: CommandId,
    ) -> AgentJournalRecord {
        let draft = draft(reducer, payload, Some(command_id));
        let sequenced = reducer
            .apply_host_draft(draft)
            .expect("causal Host event should sequence");
        assert!(matches!(sequenced.outcome, ApplyOutcome::Applied));
        sequenced.record
    }

    fn accept_and_start(run: &AgentRunEnvelope, reducer: &mut AgentRunReducer) {
        apply_host(
            reducer,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id.clone(),
                spec_digest: run.spec_digest.clone(),
            },
        );
        apply(reducer, AgentEvent::RunStarted, None);
        assert_eq!(reducer.state().status(), AgentRunStatus::Running);
    }

    fn provenance() -> Provenance {
        Provenance {
            provider_id: AgentProviderId::new("provider-1"),
            agent_id: AgentId::new("agent-1"),
            supporting_event_ids: Vec::new(),
            extensions: Default::default(),
        }
    }

    fn delivery(run: &AgentRunEnvelope) -> AgentDelivery {
        AgentDelivery {
            delivery_id: DeliveryId::new("delivery-1"),
            run_id: run.spec.run_id.clone(),
            spec_digest: run.spec_digest.clone(),
            final_response: Content::text("done"),
            outputs: Vec::new(),
            artifacts: Vec::new(),
            unresolved_issues: Vec::new(),
            usage: None,
            provenance: provenance(),
        }
    }

    fn command(
        reducer: &AgentRunReducer,
        id: &str,
        request_id: Option<RequestId>,
        payload: AgentCommand,
    ) -> AgentCommandEnvelope {
        AgentCommandEnvelope::new(
            CommandId::new(id),
            reducer.snapshot().execution.run_id.clone(),
            request_id,
            payload,
        )
        .expect("command seals")
    }

    struct TestReconciliationVerifier;

    impl ReconciliationProofVerifier for TestReconciliationVerifier {
        fn verify(
            &self,
            _execution: &AgentExecutionRef,
            _continuity: &AgentContinuityState,
            proof: &ReconciliationProof,
        ) -> Result<(), AgentProtocolError> {
            if proof.proof_ref.as_str() == "host-proof-1" {
                Ok(())
            } else {
                Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidSpec,
                    "proof is absent from Host reconciliation store",
                ))
            }
        }
    }

    fn record_command(
        reducer: &mut AgentRunReducer,
        command: &AgentCommandEnvelope,
        outcome: ProviderCommandOutcome,
    ) {
        apply_host_causal(
            reducer,
            AgentEvent::CommandReceived {
                command: command.clone(),
            },
            command.command_id.clone(),
        );
        apply(
            reducer,
            AgentEvent::CommandDispositionRecorded {
                command_id: command.command_id.clone(),
                outcome,
            },
            Some(command.command_id.clone()),
        );
    }

    #[test]
    fn complete_run_replays_to_one_terminal() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        apply(
            &mut reducer,
            AgentEvent::OutputCommitted {
                output_id: OutputId::new("output-1"),
                content: vec![Content::text("working")],
            },
            None,
        );
        let terminal = apply(
            &mut reducer,
            AgentEvent::DeliveryCommitted {
                delivery: delivery(&run),
            },
            None,
        );
        assert_eq!(reducer.state().status(), AgentRunStatus::Delivered);

        assert!(matches!(
            reducer
                .replay_journal_record(terminal)
                .expect("exact replay is idempotent"),
            ApplyOutcome::ExactDuplicate
        ));

        let tail = AgentJournalRecord::seal_provider(
            draft(
                &reducer,
                AgentEvent::RunFailed {
                    failure: AgentFailure {
                        code: "late".to_owned(),
                        message: "late failure".to_owned(),
                        retryable: false,
                        details: serde_json::Value::Null,
                    },
                },
                None,
            ),
            reducer.snapshot().last_run_seq.expect("terminal seq") + 1,
        )
        .expect("tail event seals");
        assert_eq!(
            reducer
                .replay_journal_record(tail)
                .expect_err("new terminal tail must be rejected")
                .code,
            AgentProtocolErrorCode::TerminalRun
        );
        assert_eq!(reducer.state().status(), AgentRunStatus::Delivered);
    }

    #[test]
    fn blocking_requests_are_a_projection_not_a_phase() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        let request_id = RequestId::new("request-1");
        apply(
            &mut reducer,
            AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: request_id.clone(),
                    blocking: true,
                    payload: PendingRequestPayload::Input {
                        prompt: vec![Content::text("which branch?")],
                        input_schema: None,
                    },
                },
            },
            None,
        );
        assert!(matches!(reducer.snapshot().phase, AgentRunPhase::Running));
        assert_eq!(reducer.state().status(), AgentRunStatus::Waiting);

        let resolution = RequestResolution::Input {
            content: vec![Content::text("main")],
        };
        let resolve = command(
            &reducer,
            "command-resolve",
            Some(request_id.clone()),
            AgentCommand::ResolveRequest {
                response: resolution.clone(),
            },
        );
        record_command(&mut reducer, &resolve, ProviderCommandOutcome::Accepted);
        apply(
            &mut reducer,
            AgentEvent::RequestResolved {
                request_id,
                resolution: resolution.clone(),
                resolution_digest: resolution.digest().expect("resolution digest"),
            },
            Some(resolve.command_id.clone()),
        );
        assert!(reducer
            .snapshot()
            .commands
            .get(&resolve.command_id)
            .expect("command is projected")
            .applied_seq
            .is_some());
        assert_eq!(reducer.state().status(), AgentRunStatus::Running);
    }

    #[test]
    fn provider_can_close_a_request_resolved_by_a_competing_client() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        let request_id = RequestId::new("request-external-client");
        apply(
            &mut reducer,
            AgentEvent::RequestOpened {
                request: PendingRequest {
                    request_id: request_id.clone(),
                    blocking: true,
                    payload: PendingRequestPayload::Approval {
                        operation_digest: Digest::sha256(b"operation"),
                        requested_scope: vec!["command".to_owned()],
                        session_approval_scope: None,
                        reason: "approve command".to_owned(),
                    },
                },
            },
            None,
        );

        apply(
            &mut reducer,
            AgentEvent::RequestClosed {
                request_id: request_id.clone(),
                reason: "another subscribed client resolved the native request".to_owned(),
            },
            None,
        );

        assert_eq!(reducer.state().status(), AgentRunStatus::Running);
        assert!(reducer.view().pending_requests.is_empty());
        assert!(reducer.snapshot().closed_requests.contains(&request_id));
    }

    #[test]
    fn command_effect_requires_an_accepted_provider_disposition() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        let steer = command(
            &reducer,
            "command-steer",
            None,
            AgentCommand::Steer {
                content: vec![Content::text("change direction")],
            },
        );
        apply_host_causal(
            &mut reducer,
            AgentEvent::CommandReceived {
                command: steer.clone(),
            },
            steer.command_id.clone(),
        );

        let invalid = AgentJournalRecord::seal_provider(
            draft(
                &reducer,
                AgentEvent::InputCommitted {
                    content: vec![Content::text("change direction")],
                },
                Some(steer.command_id.clone()),
            ),
            reducer.snapshot().last_run_seq.expect("seq") + 1,
        )
        .expect("event seals");
        assert!(matches!(
            reducer
                .replay_journal_record(invalid)
                .expect("violation is quarantined"),
            ApplyOutcome::ProtocolViolation { .. }
        ));
        assert_eq!(reducer.state().status(), AgentRunStatus::Unknown);
    }

    #[test]
    fn continuity_unknown_blocks_terminal_until_reconciled() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        let confirmed = reducer.snapshot().last_run_seq.expect("started seq");
        apply_host(
            &mut reducer,
            AgentEvent::ContinuityLost {
                last_confirmed_seq: confirmed,
                reason: "provider disconnected".to_owned(),
            },
        );
        assert_eq!(reducer.state().status(), AgentRunStatus::Unknown);

        let attempted = AgentJournalRecord::seal_provider(
            draft(
                &reducer,
                AgentEvent::DeliveryCommitted {
                    delivery: delivery(&run),
                },
                None,
            ),
            reducer.snapshot().last_run_seq.expect("lost seq") + 1,
        )
        .expect("event seals");
        let attempted_digest = attempted.event.event_digest.clone();
        assert!(matches!(
            reducer
                .replay_journal_record(attempted)
                .expect("unknown terminal is quarantined"),
            ApplyOutcome::ProtocolViolation { .. }
        ));
        assert_eq!(reducer.state().status(), AgentRunStatus::Unknown);

        let (last_confirmed_seq, loss_event_digest) = match &reducer.snapshot().continuity {
            AgentContinuityState::Unknown {
                last_confirmed_seq,
                loss_event_digest,
                ..
            } => (*last_confirmed_seq, loss_event_digest.clone()),
            _ => panic!("continuity should be unknown"),
        };
        assert_eq!(loss_event_digest, attempted_digest);

        let forged = ReconciliationProof::new(
            ReconciliationProofRef::new("provider-forged-proof"),
            last_confirmed_seq,
            loss_event_digest,
            Digest::sha256("provider-claimed-snapshot"),
        )
        .expect("forged proof is structurally valid");
        let forged_error = reducer
            .apply_provider_draft(draft(
                &reducer,
                AgentEvent::ContinuityRestored {
                    proof: forged,
                    reason: "provider self-attestation".to_owned(),
                },
                None,
            ))
            .expect_err("Provider cannot submit a Host-only restoration");
        assert_eq!(forged_error.code, AgentProtocolErrorCode::InvalidTransition);
        assert_eq!(reducer.state().status(), AgentRunStatus::Unknown);

        let (last_confirmed_seq, loss_event_digest) = match &reducer.snapshot().continuity {
            AgentContinuityState::Unknown {
                last_confirmed_seq,
                loss_event_digest,
                ..
            } => (*last_confirmed_seq, loss_event_digest.clone()),
            _ => panic!("continuity should remain unknown"),
        };
        let proof = ReconciliationProof::new(
            ReconciliationProofRef::new("host-proof-1"),
            last_confirmed_seq,
            loss_event_digest,
            Digest::sha256("authoritative-inspect-snapshot"),
        )
        .expect("proof seals");
        let reconciliation = draft(
            &reducer,
            AgentEvent::ContinuityRestored {
                proof,
                reason: "inspect and replay matched".to_owned(),
            },
            None,
        );
        let restored = reducer
            .apply_verified_reconciliation(reconciliation, &TestReconciliationVerifier)
            .expect("Host proof restores continuity");
        assert!(matches!(restored.outcome, ApplyOutcome::Applied));
        assert_eq!(reducer.state().status(), AgentRunStatus::Running);
        apply(
            &mut reducer,
            AgentEvent::DeliveryCommitted {
                delivery: delivery(&run),
            },
            None,
        );
        assert_eq!(reducer.state().status(), AgentRunStatus::Delivered);
    }

    #[test]
    fn optional_resource_skip_is_visible_and_required_skip_is_violation() {
        let optional = ResourceBinding {
            binding_id: ResourceBindingId::new("skill-catalog"),
            resource: ResourceRef {
                kind: ResourceKind::new("skill-catalog/v1"),
                id: ResourceId::new("skills"),
                revision: ResourceRevision::new("sha256:one"),
            },
            requirement: BindingRequirement::Optional,
            mode: ResourceBindingMode::Snapshot,
        };
        let admitted_skip = ResourceBindingSkip {
            binding_id: optional.binding_id.clone(),
            code: ResourceBindingSkipCode::UnsupportedKind,
            reason: "provider does not support skills".to_owned(),
        };
        let (run, mut reducer) = make_reducer_with_descriptor(
            vec![optional.clone()],
            AgentAdmission {
                skipped_optional_bindings: vec![admitted_skip.clone()],
            },
            descriptor(&[]),
        );
        apply_host(
            &mut reducer,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id.clone(),
                spec_digest: run.spec_digest.clone(),
            },
        );
        apply_host(
            &mut reducer,
            AgentEvent::ResourceBindingSkipped {
                skip: admitted_skip,
            },
        );
        assert_eq!(reducer.snapshot().skipped_optional_bindings.len(), 1);

        let mut required = optional;
        required.requirement = BindingRequirement::Required;
        let (required_run, mut required_reducer) = make_reducer(vec![required.clone()]);
        apply_host(
            &mut required_reducer,
            AgentEvent::RunAccepted {
                session_id: required_run.spec.session_id.clone(),
                spec_digest: required_run.spec_digest.clone(),
            },
        );
        let invalid = AgentJournalRecord::seal_host(
            draft(
                &required_reducer,
                AgentEvent::ResourceBindingSkipped {
                    skip: ResourceBindingSkip {
                        binding_id: required.binding_id,
                        code: ResourceBindingSkipCode::UnsupportedKind,
                        reason: "not supported".to_owned(),
                    },
                },
                None,
            ),
            2,
            None,
        )
        .expect("event seals");
        assert!(matches!(
            required_reducer
                .replay_journal_record(invalid)
                .expect("required skip is quarantined"),
            ApplyOutcome::ProtocolViolation { .. }
        ));
        assert_eq!(required_reducer.state().status(), AgentRunStatus::Unknown);
    }

    #[test]
    fn same_seq_conflict_and_gap_never_advance_projection() {
        let (run, mut reducer) = make_reducer(Vec::new());
        let first = apply_host(
            &mut reducer,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id.clone(),
                spec_digest: run.spec_digest.clone(),
            },
        );
        let before = reducer.snapshot().clone();
        let conflicting = AgentJournalRecord::seal_provider(
            AgentEventDraft {
                event_id: AgentEventId::new("event-conflict"),
                run_id: run.spec.run_id.clone(),
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunStarted,
            },
            first.event.run_seq,
        )
        .expect("conflict seals");
        assert_eq!(
            reducer
                .replay_journal_record(conflicting)
                .expect_err("same seq conflict is rejected")
                .code,
            AgentProtocolErrorCode::SequenceConflict
        );
        assert_eq!(reducer.snapshot(), &before);

        let gap = AgentJournalRecord::seal_provider(
            AgentEventDraft {
                event_id: AgentEventId::new("event-gap"),
                run_id: run.spec.run_id,
                causation_id: None,
                source_fingerprint: None,
                payload: AgentEvent::RunStarted,
            },
            first.event.run_seq + 2,
        )
        .expect("gap seals");
        assert_eq!(
            reducer
                .replay_journal_record(gap)
                .expect_err("gap is rejected")
                .code,
            AgentProtocolErrorCode::SequenceGap
        );
        assert_eq!(reducer.snapshot(), &before);
    }

    #[test]
    fn incomplete_is_never_a_complete_delivery() {
        let (run, mut reducer) = make_reducer(Vec::new());
        apply_host(
            &mut reducer,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id.clone(),
                spec_digest: run.spec_digest.clone(),
            },
        );
        apply(
            &mut reducer,
            AgentEvent::RunIncomplete {
                reason: IncompleteReason::Interrupted {
                    reason: "budget paused".to_owned(),
                },
                partial_delivery: None,
            },
            None,
        );
        assert_eq!(reducer.state().status(), AgentRunStatus::Incomplete);
        assert!(reducer.snapshot().delivery.is_none());
    }

    #[test]
    fn provider_recovery_draft_exact_duplicate_keeps_original_sequence() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        let recovered = AgentEventDraft {
            event_id: AgentEventId::new("stable-native-event-1"),
            run_id: run.spec.run_id.clone(),
            causation_id: None,
            source_fingerprint: Some(Digest::sha256("native-event-1")),
            payload: AgentEvent::OutputCommitted {
                output_id: OutputId::new("output-recovered"),
                content: vec![Content::text("stable")],
            },
        };
        let first = reducer
            .apply_provider_draft(recovered.clone())
            .expect("first observation commits");
        let first_seq = first.event().run_seq;
        let snapshot = reducer.snapshot().clone();
        let duplicate = reducer
            .apply_provider_draft(recovered)
            .expect("recover replay deduplicates before sequencing");
        assert!(matches!(duplicate.outcome, ApplyOutcome::ExactDuplicate));
        assert_eq!(duplicate.event().run_seq, first_seq);
        assert_eq!(reducer.snapshot(), &snapshot);
    }

    #[test]
    fn authority_matrix_rejects_cross_origin_events_without_advancing_head() {
        let (run, mut reducer) = make_reducer(Vec::new());
        let provider_accept = draft(
            &reducer,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id.clone(),
                spec_digest: run.spec_digest.clone(),
            },
            None,
        );
        assert_eq!(
            reducer
                .apply_provider_draft(provider_accept)
                .expect_err("Provider cannot forge RunAccepted")
                .code,
            AgentProtocolErrorCode::InvalidTransition
        );
        assert!(reducer.snapshot().last_run_seq.is_none());

        apply_host(
            &mut reducer,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id,
                spec_digest: run.spec_digest,
            },
        );
        let head = reducer.snapshot().last_run_seq;
        let host_started = draft(&reducer, AgentEvent::RunStarted, None);
        assert_eq!(
            reducer
                .apply_host_draft(host_started)
                .expect_err("Host cannot forge Provider RunStarted")
                .code,
            AgentProtocolErrorCode::InvalidTransition
        );
        assert_eq!(reducer.snapshot().last_run_seq, head);
    }

    #[test]
    fn run_started_requires_every_admitted_skip() {
        let optional = ResourceBinding {
            binding_id: ResourceBindingId::new("optional-skills"),
            resource: ResourceRef {
                kind: ResourceKind::new("skill-catalog/v1"),
                id: ResourceId::new("skills"),
                revision: ResourceRevision::new("revision-1"),
            },
            requirement: BindingRequirement::Optional,
            mode: ResourceBindingMode::Snapshot,
        };
        let skip = ResourceBindingSkip {
            binding_id: optional.binding_id.clone(),
            code: ResourceBindingSkipCode::UnsupportedKind,
            reason: "not supported".to_owned(),
        };
        let (run, mut reducer) = make_reducer_with_descriptor(
            vec![optional],
            AgentAdmission {
                skipped_optional_bindings: vec![skip],
            },
            descriptor(&[]),
        );
        apply_host(
            &mut reducer,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id,
                spec_digest: run.spec_digest,
            },
        );
        let started = reducer
            .apply_provider_draft(draft(&reducer, AgentEvent::RunStarted, None))
            .expect("missing admission fact is quarantined");
        assert!(matches!(
            started.outcome,
            ApplyOutcome::ProtocolViolation { .. }
        ));
        assert_eq!(reducer.state().status(), AgentRunStatus::Unknown);
    }

    #[test]
    fn structured_delivery_must_bind_the_selected_schema() {
        let mut run = sample_run(Vec::new());
        run.spec.output_schema = Some(crate::agent_protocol::wire::SchemaRef::new("schema-v1"));
        run = AgentRunEnvelope::seal(run.spec).expect("structured run seals");
        let descriptor = descriptor(&[]);
        let request = start_request(run.clone(), &descriptor);
        let mut reducer = AgentRunReducer::new(
            execution(&request, &descriptor),
            &request,
            &descriptor,
            AgentAdmission::default(),
        )
        .expect("structured reducer initializes");
        accept_and_start(&run, &mut reducer);
        let submitted = reducer
            .apply_provider_draft(draft(
                &reducer,
                AgentEvent::DeliveryCommitted {
                    delivery: delivery(&run),
                },
                None,
            ))
            .expect("schema mismatch is quarantined");
        assert!(matches!(
            submitted.outcome,
            ApplyOutcome::ProtocolViolation { .. }
        ));
        assert!(reducer.snapshot().delivery.is_none());
    }

    #[test]
    fn host_ack_becomes_applied_only_after_the_causal_effect() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        let steer = command(
            &reducer,
            "steer-ack",
            None,
            AgentCommand::Steer {
                content: vec![Content::text("new direction")],
            },
        );
        record_command(&mut reducer, &steer, ProviderCommandOutcome::Accepted);
        let accepted = reducer
            .command_ack(&steer.command_id, false)
            .expect("accepted ack projects");
        assert!(matches!(
            accepted.state,
            crate::agent_protocol::wire::CommandAckState::Accepted { .. }
        ));
        apply(
            &mut reducer,
            AgentEvent::InputCommitted {
                content: vec![Content::text("new direction")],
            },
            Some(steer.command_id.clone()),
        );
        let applied = reducer
            .command_ack(&steer.command_id, false)
            .expect("applied ack projects");
        assert!(matches!(
            applied.state,
            crate::agent_protocol::wire::CommandAckState::Applied { .. }
        ));
    }

    #[test]
    fn pending_request_kinds_must_be_declared_by_the_selected_descriptor() {
        let kinds = [
            PendingRequestKind::Input,
            PendingRequestKind::Approval,
            PendingRequestKind::ExternalAction,
        ];
        for declared_mask in 0_u8..8 {
            for (kind_index, kind) in kinds.iter().enumerate() {
                let mut descriptor = descriptor(&[]).descriptor;
                descriptor.capabilities.pending_request_kinds = kinds
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| declared_mask & (1 << index) != 0)
                    .map(|(_, kind)| kind.clone())
                    .collect();
                let descriptor = AgentDescriptorEnvelope::seal(descriptor)
                    .expect("pending capability descriptor seals");
                let (run, mut reducer) =
                    make_reducer_with_descriptor(Vec::new(), AgentAdmission::default(), descriptor);
                accept_and_start(&run, &mut reducer);
                let payload = match kind {
                    PendingRequestKind::Input => PendingRequestPayload::Input {
                        prompt: vec![Content::text("input needed")],
                        input_schema: None,
                    },
                    PendingRequestKind::Approval => PendingRequestPayload::Approval {
                        operation_digest: Digest::sha256("operation"),
                        requested_scope: vec!["workspace/write".to_owned()],
                        session_approval_scope: None,
                        reason: "write is required".to_owned(),
                    },
                    PendingRequestKind::ExternalAction => PendingRequestPayload::ExternalAction {
                        name: "external-check".to_owned(),
                        arguments: serde_json::Value::Null,
                        result_schema: None,
                    },
                };
                let result = reducer
                    .apply_provider_draft(draft(
                        &reducer,
                        AgentEvent::RequestOpened {
                            request: PendingRequest {
                                request_id: RequestId::new(format!(
                                    "request-{declared_mask}-{kind_index}"
                                )),
                                blocking: true,
                                payload,
                            },
                        },
                        None,
                    ))
                    .expect("well-formed request event sequences");
                if declared_mask & (1 << kind_index) != 0 {
                    assert!(matches!(result.outcome, ApplyOutcome::Applied));
                    assert_eq!(reducer.state().status(), AgentRunStatus::Waiting);
                } else {
                    assert!(matches!(
                        result.outcome,
                        ApplyOutcome::ProtocolViolation { .. }
                    ));
                    assert_eq!(reducer.state().status(), AgentRunStatus::Unknown);
                }
            }
        }
    }

    #[test]
    fn run_view_is_bounded_sorted_and_terminally_consistent() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        for request_id in ["request-b", "request-a"] {
            apply(
                &mut reducer,
                AgentEvent::RequestOpened {
                    request: PendingRequest {
                        request_id: RequestId::new(request_id),
                        blocking: false,
                        payload: PendingRequestPayload::Input {
                            prompt: vec![Content::text("more context")],
                            input_schema: None,
                        },
                    },
                },
                None,
            );
        }
        let view = reducer.view();
        view.validate_integrity().expect("online view is valid");
        assert_eq!(
            view.pending_requests
                .iter()
                .map(|request| request.request_id.as_str())
                .collect::<Vec<_>>(),
            vec!["request-a", "request-b"]
        );
        assert_eq!(
            view.projection_digest().expect("view digest computes"),
            reducer
                .view()
                .projection_digest()
                .expect("same projection is deterministic")
        );

        apply(
            &mut reducer,
            AgentEvent::RunFailed {
                failure: AgentFailure {
                    code: "fixture_failed".to_owned(),
                    message: "fixture stopped".to_owned(),
                    retryable: false,
                    details: serde_json::Value::Null,
                },
            },
            None,
        );
        let terminal = reducer.view();
        terminal
            .validate_integrity()
            .expect("terminal view is internally consistent");
        assert!(terminal.pending_requests.is_empty());
        assert_eq!(terminal.state.status(), AgentRunStatus::Failed);
    }

    #[test]
    fn delivery_supporting_events_must_already_exist_in_the_same_run() {
        let (run, mut reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut reducer);
        let output = apply(
            &mut reducer,
            AgentEvent::OutputCommitted {
                output_id: OutputId::new("supporting-output"),
                content: vec![Content::text("evidence")],
            },
            None,
        );
        let mut valid_delivery = delivery(&run);
        valid_delivery.provenance.supporting_event_ids = vec![output.event.event_id];
        apply(
            &mut reducer,
            AgentEvent::DeliveryCommitted {
                delivery: valid_delivery,
            },
            None,
        );
        assert_eq!(reducer.state().status(), AgentRunStatus::Delivered);

        let (run, mut dangling_reducer) = make_reducer(Vec::new());
        accept_and_start(&run, &mut dangling_reducer);
        let mut dangling = delivery(&run);
        dangling.provenance.supporting_event_ids = vec![AgentEventId::new("missing-event")];
        let result = dangling_reducer
            .apply_provider_draft(draft(
                &dangling_reducer,
                AgentEvent::DeliveryCommitted { delivery: dangling },
                None,
            ))
            .expect("dangling provenance is quarantined");
        assert!(matches!(
            result.outcome,
            ApplyOutcome::ProtocolViolation { .. }
        ));
        assert_eq!(dangling_reducer.state().status(), AgentRunStatus::Unknown);
    }

    #[test]
    fn ten_thousand_structurally_valid_sequence_mutations_never_advance_projection() {
        let (run, mut baseline) = make_reducer(Vec::new());
        apply_host(
            &mut baseline,
            AgentEvent::RunAccepted {
                session_id: run.spec.session_id,
                spec_digest: run.spec_digest,
            },
        );
        let expected_snapshot = baseline.snapshot().clone();
        let mut gaps = 0_usize;
        let mut conflicts = 0_usize;
        let mut foreign_runs = 0_usize;

        for case in 0_u64..10_000 {
            let mut candidate = baseline.clone();
            let (record, expected_code) = match case % 3 {
                0 => {
                    gaps += 1;
                    (
                        AgentJournalRecord::seal_provider(
                            AgentEventDraft {
                                event_id: AgentEventId::new(format!("gap-{case}")),
                                run_id: candidate.snapshot().execution.run_id.clone(),
                                causation_id: None,
                                source_fingerprint: None,
                                payload: AgentEvent::RunStarted,
                            },
                            3 + case,
                        )
                        .expect("gap mutation seals"),
                        AgentProtocolErrorCode::SequenceGap,
                    )
                }
                1 => {
                    conflicts += 1;
                    (
                        AgentJournalRecord::seal_provider(
                            AgentEventDraft {
                                event_id: AgentEventId::new(format!("conflict-{case}")),
                                run_id: candidate.snapshot().execution.run_id.clone(),
                                causation_id: None,
                                source_fingerprint: None,
                                payload: AgentEvent::RunStarted,
                            },
                            1,
                        )
                        .expect("sequence conflict mutation seals"),
                        AgentProtocolErrorCode::SequenceConflict,
                    )
                }
                _ => {
                    foreign_runs += 1;
                    (
                        AgentJournalRecord::seal_provider(
                            AgentEventDraft {
                                event_id: AgentEventId::new(format!("foreign-{case}")),
                                run_id: RunId::new(format!("other-run-{case}")),
                                causation_id: None,
                                source_fingerprint: None,
                                payload: AgentEvent::RunStarted,
                            },
                            2,
                        )
                        .expect("foreign run mutation seals"),
                        AgentProtocolErrorCode::RunIdConflict,
                    )
                }
            };
            assert_eq!(
                candidate
                    .replay_journal_record(record)
                    .expect_err("invalid sequence identity must reject")
                    .code,
                expected_code
            );
            assert_eq!(candidate.snapshot(), &expected_snapshot);
        }
        assert_eq!(gaps + conflicts + foreign_runs, 10_000);
        assert!(gaps > 3_000 && conflicts > 3_000 && foreign_runs > 3_000);
    }

    #[test]
    fn ten_thousand_legal_journal_traces_replay_to_the_online_projection() {
        let resources = Vec::new();
        let run = sample_run(resources.clone());
        let descriptor = descriptor(&resources);
        let request = start_request(run.clone(), &descriptor);
        let template = AgentRunReducer::new(
            execution(&request, &descriptor),
            &request,
            &descriptor,
            AgentAdmission::default(),
        )
        .expect("template reducer initializes");

        let mut delivered_traces = 0usize;
        let mut pending_terminal_traces = 0usize;
        let mut resolved_request_traces = 0usize;
        for case in 0_u64..10_000 {
            let mut online = template.clone();
            let mut records = Vec::with_capacity(8);
            let mut online_snapshots = Vec::with_capacity(8);
            records.push(apply_host(
                &mut online,
                AgentEvent::RunAccepted {
                    session_id: run.spec.session_id.clone(),
                    spec_digest: run.spec_digest.clone(),
                },
            ));
            online_snapshots.push(online.snapshot().clone());
            records.push(apply(&mut online, AgentEvent::RunStarted, None));
            online_snapshots.push(online.snapshot().clone());

            if case % 3 != 0 {
                let request_id = RequestId::new(format!("request-{case}"));
                records.push(apply(
                    &mut online,
                    AgentEvent::RequestOpened {
                        request: PendingRequest {
                            request_id: request_id.clone(),
                            blocking: true,
                            payload: PendingRequestPayload::Input {
                                prompt: vec![Content::text(format!("input-{case}"))],
                                input_schema: None,
                            },
                        },
                    },
                    None,
                ));
                online_snapshots.push(online.snapshot().clone());
                assert_eq!(online.state().status(), AgentRunStatus::Waiting);

                if case % 3 == 1 {
                    records.push(apply(
                        &mut online,
                        AgentEvent::RunFailed {
                            failure: AgentFailure {
                                code: "generated_pending_failure".to_owned(),
                                message: format!("pending trace {case} stopped"),
                                retryable: false,
                                details: serde_json::Value::Null,
                            },
                        },
                        None,
                    ));
                    online_snapshots.push(online.snapshot().clone());
                    pending_terminal_traces += 1;
                } else {
                    let resolution = RequestResolution::Input {
                        content: vec![Content::text(format!("resolution-{case}"))],
                    };
                    let resolve = command(
                        &online,
                        &format!("resolve-{case}"),
                        Some(request_id.clone()),
                        AgentCommand::ResolveRequest {
                            response: resolution.clone(),
                        },
                    );
                    records.push(apply_host_causal(
                        &mut online,
                        AgentEvent::CommandReceived {
                            command: resolve.clone(),
                        },
                        resolve.command_id.clone(),
                    ));
                    online_snapshots.push(online.snapshot().clone());
                    records.push(apply(
                        &mut online,
                        AgentEvent::CommandDispositionRecorded {
                            command_id: resolve.command_id.clone(),
                            outcome: ProviderCommandOutcome::Accepted,
                        },
                        Some(resolve.command_id.clone()),
                    ));
                    online_snapshots.push(online.snapshot().clone());
                    records.push(apply(
                        &mut online,
                        AgentEvent::RequestResolved {
                            request_id,
                            resolution: resolution.clone(),
                            resolution_digest: resolution.digest().expect("resolution digest"),
                        },
                        Some(resolve.command_id),
                    ));
                    online_snapshots.push(online.snapshot().clone());
                    resolved_request_traces += 1;
                }
            }

            if !online.state().is_terminal() {
                records.push(apply(
                    &mut online,
                    AgentEvent::OutputCommitted {
                        output_id: OutputId::new(format!("output-{case}")),
                        content: vec![Content::text(format!("value-{case}"))],
                    },
                    None,
                ));
                online_snapshots.push(online.snapshot().clone());
                let mut delivered = delivery(&run);
                delivered.delivery_id = DeliveryId::new(format!("delivery-{case}"));
                records.push(apply(
                    &mut online,
                    AgentEvent::DeliveryCommitted {
                        delivery: delivered,
                    },
                    None,
                ));
                online_snapshots.push(online.snapshot().clone());
                delivered_traces += 1;
            }

            let mut replayed = template.clone();
            for (record, expected_snapshot) in records.iter().zip(&online_snapshots) {
                assert!(matches!(
                    replayed
                        .replay_journal_record(record.clone())
                        .expect("legal record replays"),
                    ApplyOutcome::Applied
                ));
                assert_eq!(replayed.snapshot(), expected_snapshot);
            }
            assert_eq!(replayed.snapshot(), online.snapshot());
            assert_eq!(
                replayed
                    .view()
                    .projection_digest()
                    .expect("replay view digest computes"),
                online
                    .view()
                    .projection_digest()
                    .expect("online view digest computes")
            );
            for record in records {
                assert!(matches!(
                    replayed
                        .replay_journal_record(record)
                        .expect("exact replay duplicate is accepted"),
                    ApplyOutcome::ExactDuplicate
                ));
            }
        }
        assert_eq!(delivered_traces + pending_terminal_traces, 10_000);
        assert!(delivered_traces > 6_000);
        assert!(pending_terminal_traces > 3_000);
        assert!(resolved_request_traces > 3_000);
    }
}
