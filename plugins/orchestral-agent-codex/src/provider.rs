use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt};
use orchestral_core::agent_connector::{
    AgentConnectorError, AgentSessionActionInvocation, AgentSessionRequestResolution,
    SESSION_COMPACT_ACTION, SESSION_REVIEW_ACTION,
};
use orchestral_core::agent_protocol::spi::{
    AgentProvider, AgentProviderStream, AgentRecovery, AgentRecoveryRequest, AgentStart,
    AgentStartError,
};
use orchestral_core::agent_protocol::wire::{
    AgentAdmission, AgentCapabilities, AgentCommand, AgentCommandEnvelope, AgentDelivery,
    AgentDescriptor, AgentDescriptorEnvelope, AgentEvent, AgentEventDraft, AgentEventId,
    AgentExecutionRef, AgentFailure, AgentId, AgentProtocolError, AgentProtocolErrorCode,
    AgentProviderId, AgentProviderStreamItem, AgentRejection, AgentRejectionCode, AgentSessionId,
    AgentStartRequest, AgentTelemetry, AgentTelemetryEnvelope, ApprovalDecision, CancelSupport,
    CommandId, Content, ContentBody, ControlCapabilities, DeliveryId, Digest, EffectMediation,
    Extensions, IncompleteReason, OutputId, PendingRequest, PendingRequestKind,
    PendingRequestPayload, ProtocolVersion, Provenance, ProviderCommandDisposition,
    ProviderCommandOutcome, RequestId, RequestResolution, RunId, TelemetryId, ToolActivityEvidence,
    ToolActivityId, ToolActivityState,
};
use orchestral_core::io::{
    ArtifactPublishRequest, ArtifactPublisher, ArtifactResolver, BlobId, BlobStore,
};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

use crate::transport::{CodexRpcClient, CodexTransportError, CodexTransportEvent};
use crate::{CodexConnector, ConnectedClient};

const PROVIDER_ID: &str = "codex/app-server";
const AGENT_ID: &str = "codex/local";

pub(crate) fn artifact_dynamic_tools() -> Value {
    json!([{
        "type": "function",
        "name": "publish_artifact",
        "description": "Publish a file created inside this session's workspace to private Artifact storage so the remote PWA user can download it. Use this for generated reports, archives, images, spreadsheets, and other deliverables. The file must already exist under the current workspace.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Workspace-relative path, or an absolute path inside the current workspace."
                },
                "file_name": {
                    "type": "string",
                    "description": "Optional download file name."
                },
                "media_type": {
                    "type": "string",
                    "description": "Optional MIME type; inferred from common extensions when omitted."
                }
            },
            "required": ["path"]
        }
    }])
}
#[cfg(not(test))]
const EXTERNAL_QUEUE_POLL_INTERVAL: Duration = Duration::from_millis(1_500);
#[cfg(test)]
const EXTERNAL_QUEUE_POLL_INTERVAL: Duration = Duration::from_millis(25);
const EXTERNAL_QUEUE_TURN_PAGE_LIMIT: u32 = 50;
const EXTERNAL_QUEUE_AMBIGUOUS_POLL_LIMIT: u16 = 20;
const EXTERNAL_QUEUE_PAGE_LIMIT: u32 = 100;
// App-server reconstructs the rollout index for every item page. Recovery can
// otherwise reread a large active thread hundreds of times merely to locate
// one immutable client id. Use a larger bounded page only for exact identity
// reconciliation; normal transcript pagination remains at 100.
const EXTERNAL_RECOVERY_ITEM_PAGE_LIMIT: u32 = 1_000;
const EXTERNAL_HISTORY_TELEMETRY_LIMIT: usize = 256;
// A newly steered shared-writer turn can be accepted before Codex publishes it
// into either history view. Keep the live notification stream authoritative
// during that short indexing window instead of detaching on the first poll.
const DIRECT_HISTORY_MISS_LIMIT: u16 = 20;
#[cfg(not(test))]
const RECOVERY_EVIDENCE_MISS_LIMIT: u16 = 6;
#[cfg(test)]
const RECOVERY_EVIDENCE_MISS_LIMIT: u16 = 2;
#[cfg(not(test))]
const DIRECT_RUN_POLL_INTERVAL: Duration = Duration::from_millis(1_500);
#[cfg(test)]
const DIRECT_RUN_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Default)]
pub(crate) struct ProviderState {
    runs: BTreeMap<RunId, Arc<CodexRun>>,
    sessions: BTreeMap<AgentSessionId, RunId>,
    loaded_sessions: BTreeSet<AgentSessionId>,
}

impl ProviderState {
    pub(crate) fn reset_connection_state(&mut self) {
        self.loaded_sessions.clear();
    }

    pub(crate) fn mark_loaded(&mut self, session_id: AgentSessionId) {
        self.loaded_sessions.insert(session_id);
    }

    pub(crate) fn is_loaded(&self, session_id: &AgentSessionId) -> bool {
        self.loaded_sessions.contains(session_id)
    }
}

struct CodexRun {
    request: AgentStartRequest,
    execution: AgentExecutionRef,
    admission: AgentAdmission,
    artifact_publisher: Option<Arc<dyn ArtifactPublisher>>,
    critical_sender: broadcast::Sender<Result<AgentProviderStreamItem, AgentProtocolError>>,
    telemetry_sender: broadcast::Sender<AgentTelemetryEnvelope>,
    durable: Mutex<Vec<AgentEventDraft>>,
    turn_id: Mutex<Option<String>>,
    final_response: Mutex<String>,
    pending: Mutex<BTreeMap<RequestId, NativePendingRequest>>,
    commands: Mutex<BTreeMap<String, (Digest, ProviderCommandDisposition)>>,
    route: Mutex<Option<NativeRunRoute>>,
    observed_item_ids: Mutex<BTreeSet<String>>,
    cancel_request: Mutex<Option<(CommandId, String)>>,
    telemetry_seq: AtomicU64,
    direct_history_misses: AtomicU16,
    recovery_evidence_misses: AtomicU16,
    external_monitor_running: AtomicBool,
    stop_requested_published: AtomicBool,
    detached: AtomicBool,
    finalizing: AtomicBool,
    terminal: AtomicBool,
}

#[derive(Clone, Debug)]
enum NativeRunRoute {
    Direct,
    ExternalQueue {
        queued_submission_id: Option<String>,
        client_message_id: String,
        input_digest: Digest,
        phase: ExternalQueuePhase,
        ambiguous_polls: u16,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExternalQueuePhase {
    Submitting,
    Queued,
    Started,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExternalQueueObservation {
    Queued,
    TurnObserved,
    Terminal,
    Missing,
    OutcomeUnknown,
}

#[derive(Debug)]
enum DispatchClaim {
    Acquired(PathBuf),
    Existing,
}

#[derive(Clone)]
pub(super) struct NativePendingRequest {
    rpc_id: Value,
    method: String,
    kind: PendingRequestKind,
    params: Value,
}

impl NativePendingRequest {
    pub(super) fn rpc_id(&self) -> &Value {
        &self.rpc_id
    }

    pub(super) fn kind(&self) -> PendingRequestKind {
        self.kind.clone()
    }
}

fn new_codex_run(
    request: AgentStartRequest,
    execution: AgentExecutionRef,
    admission: AgentAdmission,
    artifact_publisher: Option<Arc<dyn ArtifactPublisher>>,
) -> Arc<CodexRun> {
    // Durable lifecycle events, errors, and low-volume tool activity must not
    // compete with high-volume token telemetry for retention. Output deltas
    // are intentionally lossy and restored by final DeliveryCommitted data.
    let (critical_sender, _) = broadcast::channel(1_024);
    let (telemetry_sender, _) = broadcast::channel(512);
    Arc::new(CodexRun {
        request,
        execution,
        admission,
        artifact_publisher,
        critical_sender,
        telemetry_sender,
        durable: Mutex::new(Vec::new()),
        turn_id: Mutex::new(None),
        final_response: Mutex::new(String::new()),
        pending: Mutex::new(BTreeMap::new()),
        commands: Mutex::new(BTreeMap::new()),
        route: Mutex::new(None),
        observed_item_ids: Mutex::new(BTreeSet::new()),
        cancel_request: Mutex::new(None),
        telemetry_seq: AtomicU64::new(0),
        direct_history_misses: AtomicU16::new(0),
        recovery_evidence_misses: AtomicU16::new(0),
        external_monitor_running: AtomicBool::new(false),
        stop_requested_published: AtomicBool::new(false),
        detached: AtomicBool::new(false),
        finalizing: AtomicBool::new(false),
        terminal: AtomicBool::new(false),
    })
}

fn restore_committed_provider_prefix(run: &CodexRun, prefix: &[AgentEventDraft]) {
    let mut durable = lock(&run.durable);
    debug_assert!(durable.is_empty());
    durable.extend_from_slice(prefix);
}

impl CodexConnector {
    fn provider_state(&self) -> MutexGuard<'_, ProviderState> {
        self.provider_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Rebind a non-terminal Host Run to its exact native turn after the Host
    /// process restarts. A loaded thread is only transport evidence; another
    /// client may already have started a newer turn in the same session. The
    /// immutable Orchestral client id and input digest are the ownership proof.
    async fn adopt_loaded_active_turn_for_recovery(
        &self,
        connected: &Arc<ConnectedClient>,
        run: &Arc<CodexRun>,
    ) -> Result<bool, AgentProtocolError> {
        if self.config.allow_deferred_queue
            || !client_has_loaded_session(&connected.rpc, &run.execution.session_id).await
        {
            return Ok(false);
        }
        let client_message_id = match lock(&run.route).clone() {
            Some(NativeRunRoute::ExternalQueue {
                client_message_id, ..
            }) => client_message_id,
            _ => return Ok(false),
        };
        let Some((turn_id, _correlated_item)) = find_external_turn_item(
            &connected.rpc,
            run.execution.session_id.as_str(),
            None,
            &client_message_id,
            "desc",
        )
        .await?
        else {
            return Ok(false);
        };
        // The client id contains both Run identity and immutable spec digest.
        // That is the direct-turn recovery proof. Do not compare the native
        // user item byte-for-byte: Codex intentionally materializes inline
        // image data as a local/native image entry in persisted history.
        // External queue reconciliation still validates its unmodified input
        // digest because queue submissions preserve the submitted payload.
        let Some(turn) = find_native_turn_or_loaded(
            &connected.rpc,
            &run.execution.session_id,
            &turn_id,
            "notLoaded",
        )
        .await?
        else {
            return Ok(false);
        };
        if turn.get("status").and_then(Value::as_str) == Some("interrupted")
            && !external_turn_is_terminal(&turn)
        {
            // An interrupted non-owner view is not evidence that this turn is
            // still directly controllable. Preserve the external route until
            // the shared reconciliation path proves either a live or terminal
            // boundary; mutating first strands the Run in Direct forever.
            return Ok(false);
        }
        {
            let mut state = self.provider_state();
            if state
                .sessions
                .get(&run.execution.session_id)
                .is_some_and(|existing| existing != &run.execution.run_id)
            {
                return Ok(false);
            }
            state.sessions.insert(
                run.execution.session_id.clone(),
                run.execution.run_id.clone(),
            );
            state
                .runs
                .insert(run.execution.run_id.clone(), Arc::clone(run));
            state.mark_loaded(run.execution.session_id.clone());
        }
        *lock(&run.route) = Some(NativeRunRoute::Direct);
        // Recovery must replay the same deterministic Provider prefix that the
        // Host already committed. Merely attaching the live monitor leaves an
        // empty replay stream, so AgentController waits forever for RunStarted
        // while holding the per-Run recovery gate.
        establish_turn(run, &turn_id);
        run.detached.store(false, Ordering::SeqCst);
        match turn.get("status").and_then(Value::as_str) {
            Some("inProgress") => {
                // `thread/resume` atomically subscribes this new Host
                // connection to notifications for an already loaded thread.
                // Polling in `monitor_native_run` remains the lossless fallback
                // when an older app-server reports an active-writer conflict.
                let notifications = connected
                    .rpc
                    .subscribe_session(run.execution.session_id.as_str());
                match connected
                    .rpc
                    .request(
                        "thread/resume",
                        json!({
                            "threadId": run.execution.session_id.as_str(),
                            "excludeTurns": true,
                            "persistExtendedHistory": true
                        }),
                    )
                    .await
                {
                    Ok(result) => {
                        self.remember_execution_profile(&run.execution.session_id, &result);
                    }
                    Err(error)
                        if active_writer_conflict(&error)
                            && client_has_loaded_session(
                                &connected.rpc,
                                &run.execution.session_id,
                            )
                            .await => {}
                    Err(error) => return Err(transport_to_protocol(error)),
                }
                let monitored_run = Arc::clone(run);
                let rpc = connected.rpc.clone();
                tokio::spawn(async move {
                    monitor_native_run(rpc, monitored_run, turn_id, notifications).await;
                });
            }
            Some("completed" | "failed") | Some("interrupted")
                if external_turn_is_terminal(&turn) =>
            {
                restore_direct_turn(&connected.rpc, run, &turn_id, &turn).await?;
            }
            status => {
                return Err(protocol_error(
                    format!("Codex returned an unsupported recovered turn status: {status:?}"),
                    true,
                ));
            }
        }
        Ok(true)
    }

    fn provider_descriptor() -> AgentDescriptorEnvelope {
        AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new(PROVIDER_ID),
            agent_id: AgentId::new(AGENT_ID),
            supported_protocol_versions: vec![ProtocolVersion::new(1, 0)],
            // Artifact Content is resolved immediately before native dispatch.
            // Codex receives images natively and other MIME types as a safe
            // download reference, so the adapter can truthfully accept all.
            accepted_content_types: BTreeSet::from(["*/*".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: false,
                controls: ControlCapabilities {
                    // Production defaults require the shared owner topology:
                    // accepted interactive Runs are therefore genuinely
                    // steerable. Deferred cross-process delivery is an
                    // explicit compatibility opt-in, not the PWA send path.
                    steer: true,
                    cancel: CancelSupport::BestEffort,
                    recover: true,
                },
                pending_request_kinds: BTreeSet::from([
                    PendingRequestKind::Input,
                    PendingRequestKind::Approval,
                ]),
                supported_limits: BTreeSet::new(),
                resources: Vec::new(),
                effect_mediation: EffectMediation::HostMediated,
            },
            extensions: Extensions::new(),
        })
        .expect("static Codex descriptor must be valid")
    }

    async fn start_native_run(
        &self,
        connected: Arc<ConnectedClient>,
        run: Arc<CodexRun>,
        notifications: broadcast::Receiver<CodexTransportEvent>,
    ) -> Result<(), AgentStartError> {
        let session_id = run.request.run.spec.session_id.clone();
        let action =
            AgentSessionActionInvocation::from_run(&run.request.run.spec).map_err(|error| {
                self.remove_failed_start(&run);
                AgentStartError::Rejected(AgentRejection::new(
                    AgentRejectionCode::InvalidSpec,
                    error.to_string(),
                ))
            })?;
        if let Some(action) = &action {
            validate_native_action(action).inspect_err(|_error| {
                self.remove_failed_start(&run);
            })?;
        }
        let input = if action.is_none() {
            Some(self.codex_input(&run.request).await.map_err(|error| {
                self.remove_failed_start(&run);
                AgentStartError::Rejected(AgentRejection::new(
                    AgentRejectionCode::InvalidSpec,
                    error.to_string(),
                ))
            })?)
        } else {
            None
        };
        let needs_resume = !self.provider_state().loaded_sessions.contains(&session_id);
        let mut resumed_thread = None;
        if needs_resume {
            match connected
                .rpc
                .request(
                    "thread/resume",
                    json!({
                        "threadId": session_id.as_str(),
                        "excludeTurns": true,
                        "persistExtendedHistory": true
                    }),
                )
                .await
            {
                Ok(result) => {
                    self.remember_execution_profile(&session_id, &result);
                    resumed_thread = result.get("thread").cloned();
                    self.provider_state().mark_loaded(session_id.clone());
                }
                Err(error)
                    if unmaterialized_resume_without_rollout(&error)
                        && client_has_loaded_session(&connected.rpc, &session_id).await =>
                {
                    self.provider_state().mark_loaded(session_id.clone());
                }
                Err(error) if active_writer_conflict(&error) && action.is_none() => {
                    // A shared daemon has one native writer but may expose it
                    // to several control clients. If this exact daemon lists
                    // the thread as loaded, continue against its authoritative
                    // active turn and let strict turn/steer enforce identity.
                    // A private app-server cannot see another process here, so
                    // it still takes the explicit conflict/queue path below.
                    if !self.config.allow_deferred_queue
                        && client_has_loaded_session(&connected.rpc, &session_id).await
                    {
                        self.provider_state().mark_loaded(session_id.clone());
                    } else if self.config.allow_deferred_queue {
                        return self
                            .start_external_queued_run(
                                connected.rpc.clone(),
                                run,
                                input.expect("ordinary run input must be present"),
                            )
                            .await;
                    } else {
                        self.remove_failed_start(&run);
                        return Err(AgentStartError::Rejected(
                            AgentRejection::new(
                                AgentRejectionCode::UnsupportedCapability,
                                "live_control_unavailable: this Codex thread is owned by another process and cannot receive real-time steer",
                            )
                            .with_details(json!({
                                "code": "live_control_unavailable",
                                "delivery": "realtime_only",
                                "session_id": session_id.as_str()
                            })),
                        ));
                    }
                }
                Err(error) => {
                    self.remove_failed_start(&run);
                    return Err(start_transport_error(error, false));
                }
            }
        }
        if let Some(action) = action {
            return self
                .start_native_action(connected, run, action, notifications)
                .await;
        }
        let input = input.expect("ordinary run input must be present");
        let active_turn_id = match resumed_thread.as_ref() {
            Some(thread) => active_turn_id(thread).map(str::to_owned),
            None => match latest_native_turn(&connected.rpc, &session_id).await {
                Ok(Some(turn)) => {
                    let turn = prefer_loaded_terminal_turn(&connected.rpc, &session_id, turn).await;
                    active_turn_id_from_turn(&turn)
                }
                Ok(None) | Err(_) => None,
            },
        };
        if let Some(turn_id) = active_turn_id {
            // Persist the authoritative target before dispatch so a lost
            // response can be recovered against the same native turn without
            // issuing the steer twice.
            *lock(&run.route) = Some(NativeRunRoute::Direct);
            *lock(&run.turn_id) = Some(turn_id.clone());
            let result = match connected
                .rpc
                .request(
                    "turn/steer",
                    json!({
                        "threadId": session_id.as_str(),
                        "expectedTurnId": turn_id,
                        "clientUserMessageId": queued_client_message_id(&run.execution),
                        "input": input
                    }),
                )
                .await
            {
                Ok(result) => result,
                Err(
                    error @ (CodexTransportError::Timeout
                    | CodexTransportError::Closed
                    | CodexTransportError::Disconnected(_)),
                ) => return Err(start_transport_error(error, true)),
                Err(error) => {
                    self.remove_failed_start(&run);
                    return Err(start_transport_error(error, false));
                }
            };
            let accepted_turn_id = result
                .get("turnId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    AgentStartError::OutcomeUnknown(protocol_error(
                        "Codex turn/steer omitted turnId",
                        false,
                    ))
                })?;
            if accepted_turn_id != turn_id {
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex turn/steer accepted a different active turn",
                    false,
                )));
            }
            establish_turn(&run, &turn_id);
            let monitored_turn_id = turn_id.to_owned();
            tokio::spawn(async move {
                monitor_native_run(connected.rpc.clone(), run, monitored_turn_id, notifications)
                    .await;
            });
            return Ok(());
        }
        let result = match connected
            .rpc
            .request(
                "turn/start",
                json!({
                    "threadId": session_id.as_str(),
                    "clientUserMessageId": queued_client_message_id(&run.execution),
                    "input": input
                }),
            )
            .await
        {
            Ok(result) => result,
            Err(
                error @ (CodexTransportError::Timeout
                | CodexTransportError::Closed
                | CodexTransportError::Disconnected(_)),
            ) => {
                return Err(start_transport_error(error, true));
            }
            Err(error) => {
                self.remove_failed_start(&run);
                return Err(start_transport_error(error, false));
            }
        };
        let turn_id = result
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex turn/start omitted turn.id",
                    false,
                ))
            })?
            .to_owned();
        *lock(&run.route) = Some(NativeRunRoute::Direct);
        establish_turn(&run, &turn_id);
        tokio::spawn(async move {
            monitor_native_run(connected.rpc.clone(), run, turn_id, notifications).await;
        });
        Ok(())
    }

    async fn start_external_queued_run(
        &self,
        rpc: Arc<CodexRpcClient>,
        run: Arc<CodexRun>,
        input: Value,
    ) -> Result<(), AgentStartError> {
        let session_id = run.execution.session_id.clone();
        let client_message_id = queued_client_message_id(&run.execution);
        let input_digest =
            codex_user_input_digest(&input).map_err(AgentStartError::OutcomeUnknown)?;
        // Record identity before dispatch. queue/add is not idempotent by
        // clientUserMessageId, so a lost response must be reconciled through
        // queue/history reads and must never be retried blindly.
        *lock(&run.route) = Some(NativeRunRoute::ExternalQueue {
            queued_submission_id: None,
            client_message_id: client_message_id.clone(),
            input_digest: input_digest.clone(),
            phase: ExternalQueuePhase::Submitting,
            ambiguous_polls: 0,
        });
        // The client id is derived from the immutable execution identity, so
        // this read-before-write also reconciles an add committed by a prior
        // Orchestral process whose response was lost. Codex queue/add itself
        // does not deduplicate clientUserMessageId.
        match reconcile_external_queued_run(&rpc, &run)
            .await
            .map_err(AgentStartError::OutcomeUnknown)?
        {
            ExternalQueueObservation::Queued | ExternalQueueObservation::TurnObserved => {
                run.detached.store(false, Ordering::SeqCst);
                spawn_external_queue_monitor(rpc, run);
                return Ok(());
            }
            ExternalQueueObservation::Terminal => return Ok(()),
            ExternalQueueObservation::Missing => {
                if let Some(NativeRunRoute::ExternalQueue {
                    ambiguous_polls, ..
                }) = lock(&run.route).as_mut()
                {
                    *ambiguous_polls = 0;
                }
            }
            ExternalQueueObservation::OutcomeUnknown => {
                run.detached.store(true, Ordering::SeqCst);
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex queue dispatch could not be reconciled before submission",
                    true,
                )));
            }
        }
        // Claim dispatch durably before calling Codex. This is the
        // at-most-once boundary: queue/add does not deduplicate client IDs and
        // Codex removes a queue item before its turn is necessarily visible in
        // history. A second process that sees this claim observes only; it
        // never performs another add during that dequeue/history gap.
        let claim =
            match self.claim_external_queue_dispatch(&run, &client_message_id, &input_digest) {
                Ok(claim) => claim,
                Err(error @ AgentStartError::Rejected(_)) => {
                    // No durable claim means no queue effect could have started.
                    // Release the in-memory session reservation so a transient
                    // filesystem failure remains safely retryable.
                    self.remove_failed_start(&run);
                    return Err(error);
                }
                Err(error) => return Err(error),
            };
        let claim_path = match claim {
            DispatchClaim::Acquired(path) => path,
            DispatchClaim::Existing => {
                // A claim proves only that some process crossed the local
                // at-most-once boundary. It does not prove queue/add reached
                // Codex. Keep observing the immutable identity, but do not let
                // the Host report a successfully queued Run until queue or turn
                // evidence is visible remotely.
                run.detached.store(true, Ordering::SeqCst);
                spawn_external_queue_monitor(rpc, run);
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex queue dispatch has a durable local claim but no remote queue or turn evidence yet; state reconciliation is in progress",
                    true,
                )));
            }
        };
        let result = match rpc
            .request(
                "thread/queue/add",
                json!({
                    "threadId": session_id.as_str(),
                    "input": input,
                    "clientUserMessageId": client_message_id
                }),
            )
            .await
        {
            Ok(result) => result,
            Err(
                error @ (CodexTransportError::Timeout
                | CodexTransportError::Closed
                | CodexTransportError::Disconnected(_)),
            ) => {
                run.detached.store(true, Ordering::SeqCst);
                return Err(start_transport_error(error, true));
            }
            Err(error) => {
                // A JSON-RPC rejection proves that Codex did not accept this
                // dispatch. Release only the claim owned by this attempt so a
                // corrected retry may submit it. Transport ambiguity keeps the
                // claim permanently and therefore cannot duplicate work.
                match fs::remove_file(&claim_path) {
                    Ok(()) => {
                        self.remove_failed_start(&run);
                        return Err(start_transport_error(error, false));
                    }
                    Err(cleanup) if cleanup.kind() == ErrorKind::NotFound => {
                        self.remove_failed_start(&run);
                        return Err(start_transport_error(error, false));
                    }
                    Err(cleanup) => {
                        run.detached.store(true, Ordering::SeqCst);
                        return Err(AgentStartError::OutcomeUnknown(protocol_error(
                            format!(
                                "Codex rejected queue/add but its dispatch claim '{}' could not be removed: {cleanup}",
                                claim_path.display()
                            ),
                            false,
                        )));
                    }
                }
            }
        };
        let queued_submission_id = result
            .pointer("/queuedSubmission/id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                run.detached.store(true, Ordering::SeqCst);
                AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex thread/queue/add omitted queuedSubmission.id",
                    false,
                ))
            })?
            .to_owned();
        if let Some(NativeRunRoute::ExternalQueue {
            queued_submission_id: stored_id,
            phase,
            ..
        }) = lock(&run.route).as_mut()
        {
            *stored_id = Some(queued_submission_id);
            *phase = ExternalQueuePhase::Queued;
        }
        spawn_external_queue_monitor(rpc, run);
        Ok(())
    }

    fn claim_external_queue_dispatch(
        &self,
        run: &CodexRun,
        client_message_id: &str,
        input_digest: &Digest,
    ) -> Result<DispatchClaim, AgentStartError> {
        let Some(root) = self.config.dispatch_journal_dir.as_ref() else {
            #[cfg(test)]
            return Ok(DispatchClaim::Acquired(PathBuf::new()));
            #[cfg(not(test))]
            return Err(AgentStartError::Rejected(AgentRejection::new(
                AgentRejectionCode::ProviderUnavailable,
                "Codex cross-process queueing requires a durable dispatch journal",
            )));
        };
        fs::create_dir_all(root).map_err(|error| {
            AgentStartError::Rejected(AgentRejection::new(
                AgentRejectionCode::ProviderUnavailable,
                format!(
                    "cannot create Codex dispatch journal '{}': {error}",
                    root.display()
                ),
            ))
        })?;
        let claim_key = Digest::sha256(client_message_id.as_bytes());
        let path = root.join(format!("{}.json", claim_key.as_str()));
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return Ok(DispatchClaim::Existing);
            }
            Err(error) => {
                return Err(AgentStartError::Rejected(AgentRejection::new(
                    AgentRejectionCode::ProviderUnavailable,
                    format!(
                        "cannot claim Codex queue dispatch '{}': {error}",
                        path.display()
                    ),
                )));
            }
        };
        let record = json!({
            "schema_version": 1,
            "run_id": run.execution.run_id.as_str(),
            "session_id": run.execution.session_id.as_str(),
            "client_message_id": client_message_id,
            "input_digest": input_digest.as_str()
        });
        let encoded = serde_json::to_vec(&record).map_err(|error| {
            AgentStartError::OutcomeUnknown(protocol_error(
                format!("cannot encode Codex dispatch claim: {error}"),
                false,
            ))
        })?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|error| {
                // Never remove a partially written claim: creation already
                // established the at-most-once boundary.
                AgentStartError::OutcomeUnknown(protocol_error(
                    format!(
                        "cannot durably record Codex queue dispatch '{}': {error}",
                        path.display()
                    ),
                    false,
                ))
            })?;
        #[cfg(unix)]
        fs::File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                AgentStartError::OutcomeUnknown(protocol_error(
                    format!(
                        "cannot sync Codex dispatch journal directory '{}': {error}",
                        root.display()
                    ),
                    false,
                ))
            })?;
        Ok(DispatchClaim::Acquired(path))
    }

    async fn start_native_action(
        &self,
        connected: Arc<ConnectedClient>,
        run: Arc<CodexRun>,
        action: AgentSessionActionInvocation,
        mut notifications: broadcast::Receiver<CodexTransportEvent>,
    ) -> Result<(), AgentStartError> {
        let session_id = run.execution.session_id.clone();
        let turn_id = match action.action_id.as_str() {
            SESSION_COMPACT_ACTION => {
                if !action.arguments.is_null() {
                    self.remove_failed_start(&run);
                    return Err(invalid_action("session.compact takes no arguments"));
                }
                if let Err(error) = connected
                    .rpc
                    .request(
                        "thread/compact/start",
                        json!({"threadId": session_id.as_str()}),
                    )
                    .await
                {
                    return self.action_transport_failure(&run, error);
                }
                *lock(&run.final_response) = "Session context compacted.".to_owned();
                match timeout(
                    Duration::from_secs(30),
                    wait_for_compaction_turn(&session_id, &mut notifications),
                )
                .await
                {
                    Ok(Ok(turn_id)) => turn_id,
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        run.detached.store(true, Ordering::SeqCst);
                        return Err(AgentStartError::OutcomeUnknown(protocol_error(
                            "Codex started compaction but did not identify its turn",
                            true,
                        )));
                    }
                }
            }
            SESSION_REVIEW_ACTION => {
                let target = review_target(&action.arguments).inspect_err(|_error| {
                    self.remove_failed_start(&run);
                })?;
                let result = match connected
                    .rpc
                    .request(
                        "review/start",
                        json!({
                            "threadId": session_id.as_str(),
                            "target": target,
                            "delivery": "inline"
                        }),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(error) => return self.action_transport_failure(&run, error),
                };
                result
                    .pointer("/turn/id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| {
                        AgentStartError::OutcomeUnknown(protocol_error(
                            "Codex review/start omitted turn.id",
                            false,
                        ))
                    })?
                    .to_owned()
            }
            other => {
                self.remove_failed_start(&run);
                return Err(invalid_action(format!(
                    "Codex does not support Run session action {other}"
                )));
            }
        };

        *lock(&run.route) = Some(NativeRunRoute::Direct);
        establish_turn(&run, &turn_id);
        tokio::spawn(async move {
            monitor_native_run(connected.rpc.clone(), run, turn_id, notifications).await;
        });
        Ok(())
    }

    async fn prepare_idempotent_start(&self, run: &Arc<CodexRun>) -> Result<(), AgentStartError> {
        if run.terminal.load(Ordering::SeqCst) {
            return Ok(());
        }
        let route = { lock(&run.route).clone() };
        match route {
            Some(NativeRunRoute::ExternalQueue { phase, .. }) => {
                if phase != ExternalQueuePhase::Submitting
                    && !run.detached.load(Ordering::SeqCst)
                    && run.external_monitor_running.load(Ordering::SeqCst)
                {
                    return Ok(());
                }
                let connected = self.client().await.map_err(|error| {
                    AgentStartError::OutcomeUnknown(connector_to_protocol(error))
                })?;
                match reconcile_external_queued_run(&connected.rpc, run)
                    .await
                    .map_err(AgentStartError::OutcomeUnknown)?
                {
                    ExternalQueueObservation::Queued | ExternalQueueObservation::TurnObserved => {
                        run.detached.store(false, Ordering::SeqCst);
                        spawn_external_queue_monitor(connected.rpc.clone(), Arc::clone(run));
                        Ok(())
                    }
                    ExternalQueueObservation::Terminal => Ok(()),
                    ExternalQueueObservation::Missing
                    | ExternalQueueObservation::OutcomeUnknown => {
                        run.detached.store(true, Ordering::SeqCst);
                        Err(AgentStartError::OutcomeUnknown(protocol_error(
                            "Codex queue dispatch is still being reconciled; retry the same Run identity",
                            true,
                        )))
                    }
                }
            }
            Some(NativeRunRoute::Direct) if run.detached.load(Ordering::SeqCst) => {
                Err(AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex direct start outcome is unknown; recover the existing Run",
                    true,
                )))
            }
            Some(NativeRunRoute::Direct) => Ok(()),
            None => Err(AgentStartError::OutcomeUnknown(protocol_error(
                "Codex start is still being established; retry the same Run identity",
                true,
            ))),
        }
    }

    fn action_transport_failure(
        &self,
        run: &Arc<CodexRun>,
        error: CodexTransportError,
    ) -> Result<(), AgentStartError> {
        match error {
            error @ (CodexTransportError::Timeout
            | CodexTransportError::Closed
            | CodexTransportError::Disconnected(_)) => Err(start_transport_error(error, true)),
            error => {
                self.remove_failed_start(run);
                Err(start_transport_error(error, false))
            }
        }
    }

    fn remove_failed_start(&self, run: &CodexRun) {
        let mut state = self.provider_state();
        state.runs.remove(&run.execution.run_id);
        state.sessions.remove(&run.execution.session_id);
        state.loaded_sessions.remove(&run.execution.session_id);
    }
}

fn active_turn_id(thread: &Value) -> Option<&str> {
    thread
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|turns| {
            turns
                .iter()
                .rev()
                .find(|turn| turn.get("status").and_then(Value::as_str) == Some("inProgress"))
        })
        .and_then(|turn| turn.get("id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
}

fn active_turn_id_from_turn(turn: &Value) -> Option<String> {
    (turn.get("status").and_then(Value::as_str) == Some("inProgress"))
        .then(|| turn.get("id").and_then(Value::as_str))
        .flatten()
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

async fn latest_native_turn(
    rpc: &CodexRpcClient,
    session_id: &AgentSessionId,
) -> Result<Option<Value>, CodexTransportError> {
    let result = rpc
        .request(
            "thread/turns/list",
            json!({
                "threadId": session_id.as_str(),
                "limit": 1,
                "sortDirection": "desc",
                "itemsView": "notLoaded"
            }),
        )
        .await?;
    Ok(result
        .get("data")
        .and_then(Value::as_array)
        .and_then(|turns| turns.first())
        .cloned())
}

/// The paginated history index may lag an actively written turn. Once that
/// index is exhausted, inspect the loaded thread snapshot before declaring an
/// exact native identity absent. This is a read-only Provider implementation
/// detail; the Host recovery contract remains provider-neutral.
async fn find_native_turn_or_loaded(
    rpc: &CodexRpcClient,
    session_id: &AgentSessionId,
    target_turn_id: &str,
    items_view: &str,
) -> Result<Option<Value>, AgentProtocolError> {
    if let Some(turn) =
        find_native_turn_with_items_view(rpc, session_id, target_turn_id, items_view)
            .await
            .map_err(transport_to_protocol)?
    {
        return Ok(Some(turn));
    }
    find_loaded_thread_turn(rpc, session_id.as_str(), target_turn_id).await
}

async fn find_native_turn_with_items_view(
    rpc: &CodexRpcClient,
    session_id: &AgentSessionId,
    target_turn_id: &str,
    items_view: &str,
) -> Result<Option<Value>, CodexTransportError> {
    let mut cursor = None;
    let mut seen_cursors = BTreeSet::new();
    loop {
        let result = rpc
            .request(
                "thread/turns/list",
                json!({
                    "threadId": session_id.as_str(),
                    "cursor": cursor,
                    "limit": 50,
                    "sortDirection": "desc",
                    "itemsView": items_view
                }),
            )
            .await?;
        if let Some(turn) = result
            .get("data")
            .and_then(Value::as_array)
            .and_then(|turns| {
                turns
                    .iter()
                    .find(|turn| turn.get("id").and_then(Value::as_str) == Some(target_turn_id))
            })
        {
            return Ok(Some(turn.clone()));
        }
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let Some(next) = cursor.as_ref() else {
            return Ok(None);
        };
        if next.trim().is_empty() || !seen_cursors.insert(next.clone()) {
            return Err(CodexTransportError::Rpc(
                "thread/turns/list returned a non-advancing cursor".to_owned(),
            ));
        }
    }
}

fn unmaterialized_resume_without_rollout(error: &CodexTransportError) -> bool {
    matches!(
        error,
        CodexTransportError::Rpc(message)
            if message.contains("no rollout found for thread id")
    )
}

fn active_writer_conflict(error: &CodexTransportError) -> bool {
    matches!(
        error,
        CodexTransportError::Rpc(message)
            if message.contains("already has an active writer")
    )
}

fn queued_client_message_id(execution: &AgentExecutionRef) -> String {
    format!(
        "orchestral:{}:{}",
        execution.run_id.as_str(),
        execution.spec_digest.as_str()
    )
}

pub(crate) async fn client_has_loaded_session(
    rpc: &CodexRpcClient,
    session_id: &AgentSessionId,
) -> bool {
    let mut cursor = None;
    loop {
        let result = match rpc
            .request(
                "thread/loaded/list",
                json!({"cursor": cursor, "limit": 200}),
            )
            .await
        {
            Ok(result) => result,
            Err(_) => return false,
        };
        if result
            .get("data")
            .and_then(Value::as_array)
            .is_some_and(|ids| {
                ids.iter()
                    .any(|id| id.as_str() == Some(session_id.as_str()))
            })
        {
            return true;
        }
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if cursor.is_none() {
            return false;
        }
    }
}

#[async_trait]
impl AgentProvider for CodexConnector {
    fn describe(&self) -> AgentDescriptorEnvelope {
        Self::provider_descriptor()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        let descriptor = Self::provider_descriptor();
        request
            .validate_for_descriptor(&descriptor)
            .map_err(|error| {
                AgentRejection::new(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;
        let compatibility = descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(AgentStartError::Rejected)?;
        let admission = AgentAdmission {
            skipped_optional_bindings: compatibility.skipped_optional_bindings.clone(),
        };
        admission
            .validate_against(&request.run, &compatibility)
            .map_err(|error| {
                AgentRejection::new(AgentRejectionCode::InvalidSpec, error.to_string())
            })?;
        let execution = AgentExecutionRef::for_start(&request, &descriptor).map_err(|error| {
            AgentRejection::new(AgentRejectionCode::RunIdConflict, error.to_string())
        })?;

        let (run, is_new) = {
            let mut state = self.provider_state();
            if let Some(existing) = state.runs.get(&request.run.spec.run_id) {
                if existing.request != request || existing.execution != execution {
                    return Err(AgentRejection::new(
                        AgentRejectionCode::RunIdConflict,
                        "run_id already belongs to a different Codex start",
                    )
                    .into());
                }
                (Arc::clone(existing), false)
            } else {
                if state.sessions.contains_key(&request.run.spec.session_id) {
                    let existing_terminal = state
                        .sessions
                        .get(&request.run.spec.session_id)
                        .and_then(|run_id| state.runs.get(run_id))
                        .is_some_and(|run| run.terminal.load(Ordering::SeqCst));
                    if existing_terminal {
                        state.sessions.remove(&request.run.spec.session_id);
                    } else {
                        return Err(AgentRejection::new(
                            AgentRejectionCode::SessionConflict,
                            "Codex permits one Orchestral Run per active session",
                        )
                        .into());
                    }
                }
                let run = new_codex_run(
                    request.clone(),
                    execution.clone(),
                    admission.clone(),
                    self.artifact_publisher.clone(),
                );
                state.sessions.insert(
                    request.run.spec.session_id.clone(),
                    request.run.spec.run_id.clone(),
                );
                state
                    .runs
                    .insert(request.run.spec.run_id.clone(), Arc::clone(&run));
                (run, true)
            }
        };

        if !is_new {
            self.prepare_idempotent_start(&run).await?;
            return Ok(AgentStart {
                execution: run.execution.clone(),
                admission: run.admission.clone(),
                stream: stream_for(&run),
            });
        }

        let connected = match self.client().await {
            Ok(connected) => connected,
            Err(error) => {
                self.remove_failed_start(&run);
                return Err(AgentStartError::Rejected(AgentRejection::new(
                    AgentRejectionCode::ProviderUnavailable,
                    error.to_string(),
                )));
            }
        };
        let notifications = connected
            .rpc
            .subscribe_session(run.execution.session_id.as_str());
        self.start_native_run(connected, Arc::clone(&run), notifications)
            .await?;
        Ok(AgentStart {
            execution,
            admission,
            stream: stream_for(&run),
        })
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        command.verify_digest()?;
        if command.run_id != execution.run_id {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "command run does not match execution",
            ));
        }
        let run = self
            .provider_state()
            .runs
            .get(&execution.run_id)
            .cloned()
            .ok_or_else(|| {
                AgentProtocolError::new(AgentProtocolErrorCode::RunNotFound, "Codex run not found")
            })?;
        if run.execution != *execution {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "execution identity does not match Codex run",
            ));
        }
        if let Some((digest, disposition)) = lock(&run.commands).get(command.command_id.as_str()) {
            if *digest != command.command_digest {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::DuplicateConflict,
                    "command_id was reused with different content",
                ));
            }
            let mut duplicate = disposition.clone();
            duplicate.duplicate = true;
            return Ok(duplicate);
        }
        if run.terminal.load(Ordering::SeqCst) {
            return Ok(record_command_disposition(
                &run,
                &command,
                ProviderCommandOutcome::Rejected {
                    code: AgentProtocolErrorCode::TerminalRun,
                    message: "Codex turn is already terminal; start a new Run for this message"
                        .to_owned(),
                },
            ));
        }
        let connected = self.client().await.map_err(connector_to_protocol)?;
        if matches!(lock(&run.route).as_ref(), Some(NativeRunRoute::Direct))
            && matches!(
                command.payload,
                AgentCommand::Steer { .. } | AgentCommand::Cancel { .. }
            )
        {
            match direct_turn_is_active(&connected.rpc, &run).await {
                Ok(true) => {}
                Ok(false) => {
                    return Ok(record_command_disposition(
                        &run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: if run.terminal.load(Ordering::SeqCst) {
                                AgentProtocolErrorCode::TerminalRun
                            } else {
                                AgentProtocolErrorCode::InvalidTransition
                            },
                            message: "The bound Codex turn is no longer active; the session has been reconciled. Send the message again to start a new Run".to_owned(),
                        },
                    ));
                }
                Err(error) => {
                    return Ok(record_command_disposition(
                        &run,
                        &command,
                        ProviderCommandOutcome::Rejected {
                            code: error.code,
                            message: format!(
                                "Could not verify the bound Codex turn before dispatch: {}",
                                error.message
                            ),
                        },
                    ));
                }
            }
        }
        apply_native_command(
            &connected.rpc,
            &run,
            &command,
            self.artifact_resolver.as_deref(),
            self.artifact_blob_store.as_deref(),
        )
        .await?;
        Ok(record_command_disposition(
            &run,
            &command,
            ProviderCommandOutcome::Accepted,
        ))
    }

    async fn recover(
        &self,
        request: AgentRecoveryRequest,
    ) -> Result<AgentRecovery, AgentProtocolError> {
        let descriptor = Self::provider_descriptor();
        request.validate_for(&descriptor)?;
        let existing = self
            .provider_state()
            .runs
            .get(&request.execution.run_id)
            .cloned();
        let reconstructed = existing.is_none();
        let committed_run_started = request
            .committed_provider_prefix
            .iter()
            .any(|draft| matches!(draft.payload, AgentEvent::RunStarted));
        let run = if let Some(run) = existing {
            run
        } else {
            if AgentSessionActionInvocation::from_run(&request.start_request.run.spec)
                .map_err(connector_to_protocol)?
                .is_some()
            {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::RunNotFound,
                    "Codex native actions cannot be reconstructed after provider restart",
                ));
            }
            let compatibility = descriptor
                .descriptor
                .check_run_compatibility(&request.start_request.run)
                .map_err(|error| protocol_error(error.to_string(), false))?;
            let admission = AgentAdmission {
                skipped_optional_bindings: compatibility.skipped_optional_bindings,
            };
            let input = self.codex_input(&request.start_request).await?;
            let input_digest = codex_user_input_digest(&input)?;
            let run = new_codex_run(
                request.start_request.clone(),
                request.execution.clone(),
                admission,
                self.artifact_publisher.clone(),
            );
            *lock(&run.route) = Some(NativeRunRoute::ExternalQueue {
                queued_submission_id: None,
                client_message_id: queued_client_message_id(&request.execution),
                input_digest,
                phase: if committed_run_started {
                    // Host-authenticated lifecycle evidence is stronger than
                    // a stale native queue snapshot. Recovery must never move
                    // an already-started execution back to Submitting/Queued.
                    ExternalQueuePhase::Started
                } else {
                    ExternalQueuePhase::Submitting
                },
                ambiguous_polls: 0,
            });
            restore_committed_provider_prefix(&run, request.committed_provider_prefix.as_slice());
            run.detached.store(true, Ordering::SeqCst);
            run
        };
        if run.request != request.start_request || run.execution != request.execution {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "Codex recovery identity does not match the original run",
            ));
        }
        if !reconstructed && !run.detached.load(Ordering::SeqCst) {
            // The Host can lose only its Provider-stream subscriber (for
            // example after a broadcast SequenceGap) while the exact native
            // turn monitor remains healthy. Recovery in that state is a pure
            // replay plus live re-subscription; reconnecting the app-server or
            // spawning a second native monitor would be both unnecessary and
            // unsafe.
            return Ok(AgentRecovery::reattached(stream_for(&run)));
        }

        let connected = self.client().await.map_err(connector_to_protocol)?;
        if self
            .adopt_loaded_active_turn_for_recovery(&connected, &run)
            .await?
        {
            return Ok(AgentRecovery::reattached(stream_for(&run)));
        }
        if matches!(
            lock(&run.route).as_ref(),
            Some(NativeRunRoute::ExternalQueue { .. })
        ) {
            match reconcile_external_queued_run(&connected.rpc, &run).await? {
                ExternalQueueObservation::Queued | ExternalQueueObservation::TurnObserved => {
                    run.recovery_evidence_misses.store(0, Ordering::SeqCst);
                    run.detached.store(false, Ordering::SeqCst);
                }
                ExternalQueueObservation::Terminal => {
                    run.recovery_evidence_misses.store(0, Ordering::SeqCst);
                    run.detached.store(false, Ordering::SeqCst);
                }
                ExternalQueueObservation::Missing | ExternalQueueObservation::OutcomeUnknown => {
                    let misses = run.recovery_evidence_misses.fetch_add(1, Ordering::SeqCst) + 1;
                    self.provider_state()
                        .runs
                        .insert(run.execution.run_id.clone(), Arc::clone(&run));
                    if committed_run_started && misses >= RECOVERY_EVIDENCE_MISS_LIMIT {
                        // The Host proves that native execution started, while
                        // repeated complete-index and loaded-edge scans no
                        // longer retain its immutable correlation id. Close
                        // only after a grace window: a freshly steered turn is
                        // accepted before either history view indexes it.
                        publish_incomplete(
                            &run,
                            IncompleteReason::ProviderEnded {
                                reason: "Provider-native execution is no longer retained after exhaustive recovery reconciliation"
                                    .to_owned(),
                            },
                        );
                        return Ok(AgentRecovery::reattached(stream_for(&run)));
                    }
                    return Err(protocol_error(
                        format!(
                            "Codex recovery evidence is not indexed yet (attempt {misses}/{RECOVERY_EVIDENCE_MISS_LIMIT})"
                        ),
                        true,
                    ));
                }
            }
            if reconstructed {
                let mut state = self.provider_state();
                let superseded = state
                    .sessions
                    .get(&run.execution.session_id)
                    .is_some_and(|existing_run_id| existing_run_id != &run.execution.run_id);
                if superseded {
                    // A browser may have started a replacement Run after a
                    // Host restart but before it rediscovered the original Run
                    // identity. Only the latest Run may remain commandable for
                    // the Codex session. The older durable Run still needs an
                    // authoritative terminal boundary, otherwise every stale
                    // SSE reconnect retries recovery forever.
                    drop(state);
                    if !run.terminal.load(Ordering::SeqCst) {
                        publish_incomplete(
                            &run,
                            IncompleteReason::ProviderEnded {
                                reason: "Superseded by a newer Orchestral Run controlling the same Codex session"
                                    .to_owned(),
                            },
                        );
                    }
                    state = self.provider_state();
                    state
                        .runs
                        .insert(run.execution.run_id.clone(), Arc::clone(&run));
                } else {
                    state.sessions.insert(
                        run.execution.session_id.clone(),
                        run.execution.run_id.clone(),
                    );
                    state
                        .runs
                        .insert(run.execution.run_id.clone(), Arc::clone(&run));
                }
            }
            if !run.terminal.load(Ordering::SeqCst) {
                spawn_external_queue_monitor(connected.rpc.clone(), Arc::clone(&run));
            }
            return Ok(AgentRecovery::reattached(stream_for(&run)));
        }
        let notifications = connected
            .rpc
            .subscribe_session(run.execution.session_id.as_str());
        let session_id = run.execution.session_id.clone();
        let result = connected
            .rpc
            .request(
                "thread/resume",
                json!({
                    "threadId": session_id.as_str(),
                    "excludeTurns": true,
                    "persistExtendedHistory": true
                }),
            )
            .await
            .map_err(transport_to_protocol)?;
        self.remember_execution_profile(&session_id, &result);
        self.provider_state()
            .loaded_sessions
            .insert(session_id.clone());
        let turn_id = lock(&run.turn_id).clone().ok_or_else(|| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Codex recovery omitted the original turn identity",
            )
        })?;
        let Some(turn) =
            find_native_turn_or_loaded(&connected.rpc, &session_id, &turn_id, "full").await?
        else {
            publish_incomplete(
                &run,
                IncompleteReason::ProviderEnded {
                    reason: "Provider-native execution is no longer retained after exhaustive recovery reconciliation"
                        .to_owned(),
                },
            );
            return Ok(AgentRecovery::reattached(stream_for(&run)));
        };

        run.detached.store(false, Ordering::SeqCst);
        match turn.get("status").and_then(Value::as_str) {
            Some("inProgress") => {
                let monitored_run = Arc::clone(&run);
                tokio::spawn(async move {
                    monitor_native_run(
                        connected.rpc.clone(),
                        monitored_run,
                        turn_id,
                        notifications,
                    )
                    .await;
                });
            }
            Some("completed" | "interrupted" | "failed") => {
                restore_final_response(&run, &turn);
                finish_run(&run, Some(&turn));
            }
            status => {
                run.detached.store(true, Ordering::SeqCst);
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    format!("Codex returned an unsupported recovered turn status: {status:?}"),
                )
                .with_retryable(true));
            }
        }
        Ok(AgentRecovery::reattached(stream_for(&run)))
    }
}

async fn apply_native_command(
    rpc: &CodexRpcClient,
    run: &Arc<CodexRun>,
    command: &AgentCommandEnvelope,
    artifact_resolver: Option<&dyn ArtifactResolver>,
    artifact_blob_store: Option<&dyn BlobStore>,
) -> Result<(), AgentProtocolError> {
    let route = { lock(&run.route).clone() };
    if let Some(NativeRunRoute::ExternalQueue {
        queued_submission_id,
        phase,
        ..
    }) = route
    {
        return apply_external_queued_command(
            rpc,
            run,
            command,
            queued_submission_id.as_deref(),
            phase,
        )
        .await;
    }
    let thread_id = run.execution.session_id.as_str();
    let turn_id = lock(&run.turn_id).clone().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidTransition,
            "Codex turn is not established",
        )
    })?;
    match &command.payload {
        AgentCommand::Steer { content } => {
            let result = rpc.request(
                "turn/steer",
                json!({
                    "threadId": thread_id,
                    "expectedTurnId": &turn_id,
                    "clientUserMessageId": command_client_message_id(command),
                    "input": codex_content(content, artifact_resolver, artifact_blob_store).await?
                }),
            )
            .await
            .map_err(transport_to_protocol)?;
            if result.get("turnId").and_then(Value::as_str) != Some(turn_id.as_str()) {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Codex turn/steer acknowledged a different native turn",
                ));
            }
        }
        AgentCommand::Cancel { reason } => {
            *lock(&run.cancel_request) = Some((command.command_id.clone(), reason.clone()));
            if let Err(error) = rpc
                .request(
                    "turn/interrupt",
                    json!({"threadId": thread_id, "turnId": turn_id}),
                )
                .await
            {
                clear_cancel_request(run, &command.command_id);
                return Err(transport_to_protocol(error));
            }
            publish_stop_requested(run, reason.clone(), command.command_id.clone());
        }
        AgentCommand::ResolveRequest { response } => {
            let request_id = command.request_id.as_ref().ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestNotFound,
                    "resolution omitted request id",
                )
            })?;
            let native = lock(&run.pending).remove(request_id).ok_or_else(|| {
                AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestNotFound,
                    "pending Codex request was not found",
                )
            })?;
            if native.kind != response.kind() {
                lock(&run.pending).insert(request_id.clone(), native);
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::RequestTypeMismatch,
                    "resolution kind does not match pending Codex request",
                ));
            }
            let result = native_resolution(&native, response)?;
            if let Err(error) = rpc.respond(native.rpc_id.clone(), result).await {
                lock(&run.pending).insert(request_id.clone(), native);
                return Err(transport_to_protocol(error));
            }
            publish_event(
                run,
                AgentEventDraft {
                    event_id: AgentEventId::new(format!(
                        "codex-{}-request-{}-resolved",
                        run.execution.run_id.as_str(),
                        request_id.as_str()
                    )),
                    run_id: run.execution.run_id.clone(),
                    causation_id: Some(command.command_id.clone()),
                    source_fingerprint: None,
                    payload: AgentEvent::RequestResolved {
                        request_id: request_id.clone(),
                        resolution: response.clone(),
                        resolution_digest: response.digest()?,
                    },
                },
            );
        }
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "Codex adapter does not support this command type",
            ));
        }
    }
    Ok(())
}

fn record_command_disposition(
    run: &CodexRun,
    command: &AgentCommandEnvelope,
    outcome: ProviderCommandOutcome,
) -> ProviderCommandDisposition {
    let disposition = ProviderCommandDisposition {
        command_id: command.command_id.clone(),
        run_id: command.run_id.clone(),
        outcome,
        duplicate: false,
    };
    lock(&run.commands).insert(
        command.command_id.as_str().to_owned(),
        (command.command_digest.clone(), disposition.clone()),
    );
    disposition
}

fn command_client_message_id(command: &AgentCommandEnvelope) -> String {
    format!(
        "orchestral-command:{}:{}:{}",
        command.run_id.as_str(),
        command.command_id.as_str(),
        command.command_digest.as_str()
    )
}

async fn apply_external_queued_command(
    rpc: &CodexRpcClient,
    run: &Arc<CodexRun>,
    command: &AgentCommandEnvelope,
    queued_submission_id: Option<&str>,
    phase: ExternalQueuePhase,
) -> Result<(), AgentProtocolError> {
    match &command.payload {
        AgentCommand::Cancel { reason }
            if phase == ExternalQueuePhase::Queued && queued_submission_id.is_some() =>
        {
            let queued_submission_id = queued_submission_id.expect("guarded queued submission id");
            *lock(&run.cancel_request) =
                Some((command.command_id.clone(), reason.clone()));
            let result = match rpc
                .request(
                    "thread/queue/delete",
                    json!({
                        "threadId": run.execution.session_id.as_str(),
                        "queuedSubmissionId": queued_submission_id
                    }),
                )
                .await
            {
                Ok(result) => result,
                Err(
                    error @ (CodexTransportError::Timeout
                    | CodexTransportError::Closed
                    | CodexTransportError::Disconnected(_)),
                ) => {
                    // The delete may or may not have reached Codex. Do not
                    // publish StopRequested: if the queued turn subsequently
                    // starts, that would create the illegal Host transition
                    // Stopping -> RunStarted. The retryable transport error is
                    // the only honest best-effort outcome.
                    clear_cancel_request(run, &command.command_id);
                    return Err(transport_to_protocol(error));
                }
                Err(error) => {
                    clear_cancel_request(run, &command.command_id);
                    return Err(transport_to_protocol(error));
                }
            };
            if result.get("deleted").and_then(Value::as_bool) != Some(true) {
                clear_cancel_request(run, &command.command_id);
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::InvalidTransition,
                    "Codex queued message is no longer pending; its external owner may have started it",
                ));
            }
            publish_cancelled(run, reason.clone(), command.command_id.clone());
            Ok(())
        }
        AgentCommand::Cancel { .. } if phase == ExternalQueuePhase::Submitting => {
            Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Codex queue dispatch outcome is unknown; cancellation cannot be proven safe",
            ))
        }
        AgentCommand::Cancel { .. } => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "Codex external owner may already have started this queued turn; cancellation is unavailable across process ownership",
        )),
        AgentCommand::Steer { .. } => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "Codex does not expose cross-process live steering; submit another queued message instead",
        )),
        AgentCommand::ResolveRequest { .. } => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "Codex approvals are process-local to the external session owner",
        )),
        _ => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "Codex external queued run does not support this command type",
        )),
    }
}

fn spawn_external_queue_monitor(rpc: Arc<CodexRpcClient>, run: Arc<CodexRun>) {
    if run
        .external_monitor_running
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    tokio::spawn(async move {
        monitor_external_queued_run(rpc, Arc::clone(&run)).await;
        run.external_monitor_running.store(false, Ordering::SeqCst);
    });
}

async fn monitor_external_queued_run(rpc: Arc<CodexRpcClient>, run: Arc<CodexRun>) {
    loop {
        if run.terminal.load(Ordering::SeqCst) {
            return;
        }
        match reconcile_external_queued_run(&rpc, &run).await {
            Ok(
                ExternalQueueObservation::Queued
                | ExternalQueueObservation::TurnObserved
                | ExternalQueueObservation::Missing,
            ) => {
                run.detached.store(false, Ordering::SeqCst);
            }
            Ok(ExternalQueueObservation::Terminal) => return,
            Ok(ExternalQueueObservation::OutcomeUnknown) => {
                detach_external_run(
                    &run,
                    "Codex no longer exposes the queued message or its resulting turn",
                    true,
                );
                return;
            }
            Err(error) => {
                run.detached.store(true, Ordering::SeqCst);
                let _ = run.critical_sender.send(Err(error));
                return;
            }
        }
        tokio::time::sleep(EXTERNAL_QUEUE_POLL_INTERVAL).await;
    }
}

async fn reconcile_external_queued_run(
    rpc: &CodexRpcClient,
    run: &Arc<CodexRun>,
) -> Result<ExternalQueueObservation, AgentProtocolError> {
    let route = { lock(&run.route).clone() };
    let (client_message_id, input_digest, phase) = match route {
        Some(NativeRunRoute::ExternalQueue {
            client_message_id,
            input_digest,
            phase,
            ..
        }) => (client_message_id, input_digest, phase),
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidTransition,
                "Codex run is not routed through the external queue",
            ));
        }
    };

    if phase != ExternalQueuePhase::Started {
        if let Some(submission) =
            find_queued_submission(rpc, run.execution.session_id.as_str(), &client_message_id)
                .await?
        {
            validate_correlated_input(
                submission.get("input"),
                &input_digest,
                "Codex queued submission",
            )?;
            let submission_id = submission
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| protocol_error("Codex queued submission omitted id", false))?
                .to_owned();
            if let Some(NativeRunRoute::ExternalQueue {
                queued_submission_id,
                phase,
                ambiguous_polls,
                ..
            }) = lock(&run.route).as_mut()
            {
                *queued_submission_id = Some(submission_id);
                *phase = ExternalQueuePhase::Queued;
                *ambiguous_polls = 0;
            }
            return Ok(ExternalQueueObservation::Queued);
        }
    }

    if let Some(turn) = find_external_turn(
        rpc,
        run.execution.session_id.as_str(),
        &client_message_id,
        run.detached.load(Ordering::SeqCst),
    )
    .await?
    {
        let correlated_input = turn
            .get("items")
            .and_then(Value::as_array)
            .and_then(|items| {
                items.iter().find_map(|item| {
                    (item.get("type").and_then(Value::as_str) == Some("userMessage")
                        && item.get("clientId").and_then(Value::as_str)
                            == Some(client_message_id.as_str()))
                    .then(|| item.get("content"))
                    .flatten()
                })
            });
        validate_correlated_input(correlated_input, &input_digest, "Codex queued turn")?;
        let turn_id = turn
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| protocol_error("Codex queued turn omitted id", false))?;
        establish_external_turn(run, turn_id)?;
        observe_external_turn_items(run, &turn);

        let status = turn.get("status").and_then(Value::as_str);
        let terminal = external_turn_is_terminal(&turn);
        if terminal {
            if let Ok(items) =
                load_external_turn_items(rpc, run.execution.session_id.as_str(), turn_id).await
            {
                let detailed = json!({"items": items});
                observe_external_turn_items(run, &detailed);
                restore_final_response(run, &detailed);
            }
            restore_final_response(run, &turn);
            finish_run(run, Some(&turn));
            return Ok(ExternalQueueObservation::Terminal);
        }
        if status == Some("interrupted") {
            let superseded =
                native_turn_was_superseded(rpc, &run.execution.session_id, &turn).await?;
            if superseded {
                // A thread executes only one native turn at a time. Codex may
                // omit completedAt when a non-owner reads an interrupted turn.
                // Either history view proving a newer turn is nevertheless an
                // authoritative boundary for this exact Run.
                publish_incomplete(
                    run,
                    IncompleteReason::ProviderEnded {
                        reason: "Provider-native turn was superseded by a newer turn in the same session"
                            .to_owned(),
                    },
                );
                return Ok(ExternalQueueObservation::Terminal);
            }
        }
        if matches!(status, Some("inProgress" | "interrupted" | "failed")) {
            return Ok(ExternalQueueObservation::TurnObserved);
        }
        return Err(protocol_error(
            format!("Codex queued turn returned unsupported status: {status:?}"),
            false,
        ));
    }

    let exhausted = if let Some(NativeRunRoute::ExternalQueue {
        ambiguous_polls, ..
    }) = lock(&run.route).as_mut()
    {
        *ambiguous_polls = ambiguous_polls.saturating_add(1);
        *ambiguous_polls >= EXTERNAL_QUEUE_AMBIGUOUS_POLL_LIMIT
    } else {
        true
    };
    Ok(if exhausted {
        ExternalQueueObservation::OutcomeUnknown
    } else {
        ExternalQueueObservation::Missing
    })
}

/// Proves that an ambiguous externally-owned turn can no longer be active.
///
/// Codex maintains both a paginated durable index and a loaded-thread edge.
/// The index may lag the currently loaded session, so treating its first row
/// as the sole latest turn can leave an interrupted Run commandable forever.
/// A later turn in either view is sufficient because one Codex thread executes
/// at most one native turn at a time.
async fn native_turn_was_superseded(
    rpc: &CodexRpcClient,
    session_id: &AgentSessionId,
    target_turn: &Value,
) -> Result<bool, AgentProtocolError> {
    let target_turn_id = target_turn
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| protocol_error("Codex target turn omitted id", false))?;
    let target_started_at_ms = native_turn_started_at_ms(target_turn);
    if let Some(latest) = latest_native_turn(rpc, session_id)
        .await
        .map_err(transport_to_protocol)?
    {
        let different = latest.get("id").and_then(Value::as_str) != Some(target_turn_id);
        let provably_newer = target_started_at_ms.is_some_and(|target_started_at_ms| {
            native_turn_started_at_ms(&latest)
                .is_some_and(|latest_started_at_ms| latest_started_at_ms > target_started_at_ms)
        });
        if different && provably_newer {
            return Ok(true);
        }
    }

    let loaded = read_loaded_thread(rpc, session_id.as_str()).await?;
    let turns = loaded
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| protocol_error("Codex loaded thread omitted turns", false))?;
    let Some(target_started_at_ms) = target_started_at_ms else {
        return Ok(false);
    };
    Ok(turns.iter().any(|turn| {
        turn.get("id").and_then(Value::as_str) != Some(target_turn_id)
            && native_turn_started_at_ms(turn)
                .is_some_and(|started_at_ms| started_at_ms > target_started_at_ms)
    }))
}

fn native_turn_started_at_ms(turn: &Value) -> Option<u64> {
    turn.get("startedAtMs")
        .and_then(Value::as_u64)
        .or_else(|| {
            turn.get("startedAt")
                .and_then(Value::as_u64)
                .and_then(|seconds| seconds.checked_mul(1_000))
        })
        .or_else(|| {
            let id = uuid::Uuid::parse_str(turn.get("id")?.as_str()?).ok()?;
            (id.get_version_num() == 7).then(|| {
                id.as_bytes()[..6]
                    .iter()
                    .fold(0_u64, |timestamp, byte| (timestamp << 8) | u64::from(*byte))
            })
        })
}

fn native_turn_is_newer(candidate: &Value, target: &Value) -> bool {
    native_turn_started_at_ms(target).is_some_and(|target_started_at_ms| {
        native_turn_started_at_ms(candidate)
            .is_some_and(|candidate_started_at_ms| candidate_started_at_ms > target_started_at_ms)
    })
}

async fn find_queued_submission(
    rpc: &CodexRpcClient,
    thread_id: &str,
    client_message_id: &str,
) -> Result<Option<Value>, AgentProtocolError> {
    let mut cursor = None;
    let mut seen_cursors = BTreeSet::new();
    loop {
        let result = rpc
            .request(
                "thread/queue/list",
                json!({
                    "threadId": thread_id,
                    "cursor": cursor,
                    "limit": EXTERNAL_QUEUE_PAGE_LIMIT
                }),
            )
            .await
            .map_err(transport_to_protocol)?;
        let data = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol_error("Codex thread/queue/list omitted data", false))?;
        if let Some(submission) = data.iter().find(|submission| {
            submission
                .get("clientUserMessageId")
                .and_then(Value::as_str)
                == Some(client_message_id)
        }) {
            return Ok(Some(submission.clone()));
        }
        cursor = next_page_cursor(&result, &mut seen_cursors, "thread/queue/list")?;
        if cursor.is_none() {
            return Ok(None);
        }
    }
}

async fn find_external_turn(
    rpc: &CodexRpcClient,
    thread_id: &str,
    client_message_id: &str,
    scan_native_items: bool,
) -> Result<Option<Value>, AgentProtocolError> {
    if scan_native_items {
        // A turn summary contains only its first user item and final agent
        // item. Recovery usually targets a steer submitted while that turn was
        // already active, so exhausting summary history first is both unable
        // to match and pathologically slow for long sessions. The native item
        // index is ordered newest-first and retains the exact client identity.
        let session_id = AgentSessionId::new(thread_id);
        if let Some(mut latest_turn) = latest_native_turn(rpc, &session_id)
            .await
            .map_err(transport_to_protocol)?
        {
            let latest_turn_id = latest_turn
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(latest_turn_id) = latest_turn_id {
                if let Some((_, correlated_item)) = find_external_turn_item(
                    rpc,
                    thread_id,
                    Some(&latest_turn_id),
                    client_message_id,
                    "asc",
                )
                .await?
                {
                    latest_turn["items"] = Value::Array(vec![correlated_item]);
                    return Ok(Some(latest_turn));
                }
            }
        }
        if let Some((turn_id, correlated_item)) =
            find_external_turn_item(rpc, thread_id, None, client_message_id, "desc").await?
        {
            if let Some(mut turn) =
                find_native_turn_or_loaded(rpc, &session_id, &turn_id, "notLoaded").await?
            {
                turn["items"] = Value::Array(vec![correlated_item]);
                return Ok(Some(turn));
            }
        }
    }

    let mut cursor = None;
    let mut seen_cursors = BTreeSet::new();
    loop {
        let result = rpc
            .request(
                "thread/turns/list",
                json!({
                    "threadId": thread_id,
                    "cursor": cursor,
                    "limit": EXTERNAL_QUEUE_TURN_PAGE_LIMIT,
                    "sortDirection": "desc",
                    "itemsView": "summary"
                }),
            )
            .await
            .map_err(transport_to_protocol)?;
        let turns = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol_error("Codex thread/turns/list omitted data", false))?;
        if let Some(turn) = turns
            .iter()
            .find(|turn| turn_contains_client_message(turn, client_message_id))
        {
            return Ok(Some(turn.clone()));
        }
        cursor = next_page_cursor(&result, &mut seen_cursors, "thread/turns/list")?;
        if cursor.is_none() {
            break;
        }
    }

    Ok(None)
}

async fn find_external_turn_item(
    rpc: &CodexRpcClient,
    thread_id: &str,
    turn_id: Option<&str>,
    client_message_id: &str,
    sort_direction: &str,
) -> Result<Option<(String, Value)>, AgentProtocolError> {
    let mut cursor = None;
    let mut seen_cursors = BTreeSet::new();
    loop {
        let mut params = json!({
            "threadId": thread_id,
            "cursor": cursor,
            "limit": EXTERNAL_RECOVERY_ITEM_PAGE_LIMIT,
            "sortDirection": sort_direction
        });
        if let Some(turn_id) = turn_id {
            params["turnId"] = Value::String(turn_id.to_owned());
        }
        let result = rpc
            .request("thread/items/list", params)
            .await
            .map_err(transport_to_protocol)?;
        let entries = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol_error("Codex thread/items/list omitted data", false))?;
        if let Some((turn_id, item)) = entries.iter().find_map(|entry| {
            let item = entry.get("item")?;
            (item.get("type").and_then(Value::as_str) == Some("userMessage")
                && item.get("clientId").and_then(Value::as_str) == Some(client_message_id))
            .then(|| {
                entry
                    .get("turnId")
                    .and_then(Value::as_str)
                    .filter(|turn_id| !turn_id.is_empty())
                    .map(|turn_id| (turn_id.to_owned(), item.clone()))
            })
            .flatten()
        }) {
            return Ok(Some((turn_id, item)));
        }
        cursor = next_page_cursor(&result, &mut seen_cursors, "thread/items/list")?;
        if cursor.is_none() {
            return find_loaded_thread_item(rpc, thread_id, turn_id, client_message_id).await;
        }
    }
}

async fn read_loaded_thread(
    rpc: &CodexRpcClient,
    thread_id: &str,
) -> Result<Value, AgentProtocolError> {
    let result = rpc
        .request(
            "thread/read",
            json!({"threadId": thread_id, "includeTurns": true}),
        )
        .await
        .map_err(transport_to_protocol)?;
    result
        .get("thread")
        .cloned()
        .ok_or_else(|| protocol_error("Codex thread/read omitted thread", false))
}

async fn find_loaded_thread_turn(
    rpc: &CodexRpcClient,
    thread_id: &str,
    target_turn_id: &str,
) -> Result<Option<Value>, AgentProtocolError> {
    let thread = read_loaded_thread(rpc, thread_id).await?;
    Ok(thread
        .get("turns")
        .and_then(Value::as_array)
        .and_then(|turns| {
            turns
                .iter()
                .find(|turn| turn.get("id").and_then(Value::as_str) == Some(target_turn_id))
        })
        .cloned())
}

/// Resolves the split-brain edge between Codex's durable turn index and its
/// loaded thread snapshot.
///
/// The paginated index can retain `inProgress` after the loaded thread has
/// committed the exact turn. A loaded terminal copy is strictly newer and
/// therefore wins. All side-effecting and reconciliation paths use this one
/// rule so a stale index cannot make a completed turn commandable again.
async fn prefer_loaded_terminal_turn(
    rpc: &CodexRpcClient,
    session_id: &AgentSessionId,
    indexed: Value,
) -> Value {
    if indexed.get("status").and_then(Value::as_str) != Some("inProgress") {
        return indexed;
    }
    let Some(turn_id) = indexed
        .get("id")
        .and_then(Value::as_str)
        .filter(|turn_id| !turn_id.is_empty())
    else {
        return indexed;
    };
    match find_loaded_thread_turn(rpc, session_id.as_str(), turn_id).await {
        Ok(Some(loaded)) if external_turn_is_terminal(&loaded) => loaded,
        Ok(_) | Err(_) => indexed,
    }
}

/// Polling only needs to discover completion. A metadata read exposes the
/// loaded thread's live status without parsing/serializing every rollout item.
/// Keep the full snapshot check for idle/unknown status, where the history
/// index may still incorrectly report inProgress. Command preflight continues
/// to use the stronger exact-turn check above.
async fn prefer_loaded_terminal_turn_for_poll(
    rpc: &CodexRpcClient,
    session_id: &AgentSessionId,
    indexed: Value,
) -> Value {
    if indexed.get("status").and_then(Value::as_str) != Some("inProgress") {
        return indexed;
    }
    if let Ok(summary) = rpc
        .request(
            "thread/read",
            json!({
                "threadId": session_id.as_str(), "includeTurns": false
            }),
        )
        .await
    {
        if summary.pointer("/thread/id").and_then(Value::as_str) == Some(session_id.as_str())
            && summary
                .pointer("/thread/status/type")
                .and_then(Value::as_str)
                == Some("active")
        {
            return indexed;
        }
    }
    prefer_loaded_terminal_turn(rpc, session_id, indexed).await
}

async fn find_loaded_thread_item(
    rpc: &CodexRpcClient,
    thread_id: &str,
    target_turn_id: Option<&str>,
    client_message_id: &str,
) -> Result<Option<(String, Value)>, AgentProtocolError> {
    let thread = read_loaded_thread(rpc, thread_id).await?;
    let turns = thread
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| protocol_error("Codex loaded thread omitted turns", false))?;
    Ok(turns.iter().find_map(|turn| {
        let turn_id = turn
            .get("id")
            .and_then(Value::as_str)
            .filter(|turn_id| !turn_id.is_empty())?;
        if target_turn_id.is_some_and(|expected| expected != turn_id) {
            return None;
        }
        turn.get("items")
            .and_then(Value::as_array)
            .and_then(|items| {
                items.iter().find(|item| {
                    item.get("type").and_then(Value::as_str) == Some("userMessage")
                        && item.get("clientId").and_then(Value::as_str) == Some(client_message_id)
                })
            })
            .map(|item| (turn_id.to_owned(), item.clone()))
    }))
}

async fn load_external_turn_items(
    rpc: &CodexRpcClient,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<Value>, AgentProtocolError> {
    let mut cursor = None;
    let mut seen_cursors = BTreeSet::new();
    let mut items = Vec::new();
    loop {
        let result = rpc
            .request(
                "thread/items/list",
                json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                    "cursor": cursor,
                    "limit": EXTERNAL_QUEUE_PAGE_LIMIT,
                    "sortDirection": "asc"
                }),
            )
            .await
            .map_err(transport_to_protocol)?;
        let entries = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol_error("Codex thread/items/list omitted data", false))?;
        items.extend(
            entries
                .iter()
                .filter_map(|entry| entry.get("item").cloned()),
        );
        cursor = next_page_cursor(&result, &mut seen_cursors, "thread/items/list")?;
        if cursor.is_none() {
            return Ok(items);
        }
    }
}

fn next_page_cursor(
    result: &Value,
    seen: &mut BTreeSet<String>,
    method: &str,
) -> Result<Option<String>, AgentProtocolError> {
    let Some(cursor) = result
        .get("nextCursor")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return Ok(None);
    };
    if !seen.insert(cursor.clone()) {
        return Err(protocol_error(
            format!("Codex {method} repeated its pagination cursor"),
            false,
        ));
    }
    Ok(Some(cursor))
}

fn establish_external_turn(run: &Arc<CodexRun>, turn_id: &str) -> Result<(), AgentProtocolError> {
    {
        let mut stored_turn_id = lock(&run.turn_id);
        if let Some(existing) = stored_turn_id.as_ref() {
            if existing != turn_id {
                return Err(protocol_error(
                    "Codex correlated one queued message with multiple turns",
                    false,
                ));
            }
        } else {
            *stored_turn_id = Some(turn_id.to_owned());
        }
    }
    let should_publish_started = if let Some(NativeRunRoute::ExternalQueue {
        phase,
        ambiguous_polls,
        ..
    }) = lock(&run.route).as_mut()
    {
        let changed = *phase != ExternalQueuePhase::Started;
        *phase = ExternalQueuePhase::Started;
        *ambiguous_polls = 0;
        changed
    } else {
        false
    };
    if should_publish_started {
        publish_run_started(run);
    }
    Ok(())
}

fn validate_correlated_input(
    input: Option<&Value>,
    expected: &Digest,
    source: &str,
) -> Result<(), AgentProtocolError> {
    let actual = input
        .ok_or_else(|| protocol_error(format!("{source} omitted correlated input"), false))
        .and_then(codex_user_input_digest)?;
    if &actual != expected {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::DuplicateConflict,
            format!("{source} input changed after Orchestral submitted it"),
        ));
    }
    Ok(())
}

fn codex_user_input_digest(input: &Value) -> Result<Digest, AgentProtocolError> {
    let entries = input
        .as_array()
        .ok_or_else(|| protocol_error("Codex user input must be an array", false))?;
    let all_text = entries
        .iter()
        .all(|entry| entry.get("type").and_then(Value::as_str) == Some("text"));
    if all_text {
        let text = entries
            .iter()
            .map(|entry| {
                entry
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| protocol_error("Codex text input omitted text", false))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");
        // Preserve the pre-Artifact digest for text-only queued Runs.
        return Ok(Digest::sha256(text.into_bytes()));
    }
    for entry in entries {
        match entry.get("type").and_then(Value::as_str) {
            Some("text") if entry.get("text").and_then(Value::as_str).is_some() => {}
            Some("image") if entry.get("url").and_then(Value::as_str).is_some() => {}
            kind => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::Unsupported,
                    format!("Codex queued input contains unsupported item type: {kind:?}"),
                ));
            }
        }
    }
    let encoded = serde_json::to_vec(entries)
        .map_err(|error| protocol_error(format!("encode Codex input digest: {error}"), false))?;
    Ok(Digest::sha256(encoded))
}

fn detach_external_run(run: &CodexRun, message: impl Into<String>, retryable: bool) {
    run.detached.store(true, Ordering::SeqCst);
    let _ = run
        .critical_sender
        .send(Err(protocol_error(message.into(), retryable)));
}

fn turn_contains_client_message(turn: &Value, client_message_id: &str) -> bool {
    turn.get("items")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("type").and_then(Value::as_str) == Some("userMessage")
                    && item.get("clientId").and_then(Value::as_str) == Some(client_message_id)
            })
        })
}

fn observe_external_turn_items(run: &Arc<CodexRun>, turn: &Value) {
    let terminal = external_turn_is_terminal(turn);
    let items = turn
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let eligible = items
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            let item_type = item.get("type").and_then(Value::as_str);
            let item_terminal = matches!(
                item.get("status").and_then(Value::as_str),
                Some("completed" | "failed" | "interrupted" | "declined")
            );
            item_type != Some("userMessage")
                && (terminal || item_type != Some("agentMessage"))
                && (terminal || item_terminal)
        })
        .collect::<Vec<_>>();
    let eligible_len = eligible.len();
    let edge = EXTERNAL_HISTORY_TELEMETRY_LIMIT / 2;
    for (position, (index, item)) in eligible.into_iter().enumerate() {
        // Telemetry is observational and non-durable. Keep a bounded first and
        // latest window so a large historical turn cannot overflow the live
        // broadcast and hide the durable terminal delivery from the Host.
        if eligible_len > EXTERNAL_HISTORY_TELEMETRY_LIMIT
            && position >= edge
            && position < eligible_len - edge
        {
            continue;
        }
        let item_type = item.get("type").and_then(Value::as_str);
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{}-{index}", item_type.unwrap_or("item")));
        if lock(&run.observed_item_ids).insert(item_id) {
            handle_completed_item(run, Some(item));
        }
    }
}

fn external_turn_is_terminal(turn: &Value) -> bool {
    match turn.get("status").and_then(Value::as_str) {
        Some("completed" | "failed") => true,
        // A non-owner Codex app-server normalizes an externally active turn to
        // interrupted without completedAt. Requiring the completion marker here
        // prevents a long-running external turn from being ended prematurely.
        Some("interrupted") => external_turn_has_completed(turn),
        _ => false,
    }
}

fn external_turn_has_completed(turn: &Value) -> bool {
    turn.get("completedAt")
        .is_some_and(|value| !value.is_null())
}

fn direct_turn_is_terminal_for_run(run: &CodexRun, turn: &Value) -> bool {
    external_turn_is_terminal(turn)
        || (turn.get("status").and_then(Value::as_str) == Some("interrupted")
            && lock(&run.cancel_request).is_some())
}

/// Checks the one authoritative live edge before a side-effecting direct
/// command. When the bound turn is already terminal, reconcile it immediately
/// so the Host can stop presenting an indefinitely commandable Run.
async fn direct_turn_is_active(
    rpc: &CodexRpcClient,
    run: &Arc<CodexRun>,
) -> Result<bool, AgentProtocolError> {
    let turn_id = lock(&run.turn_id).clone().ok_or_else(|| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidTransition,
            "Codex turn is not established",
        )
    })?;
    let Some(latest) = latest_native_turn(rpc, &run.execution.session_id)
        .await
        .map_err(transport_to_protocol)?
    else {
        return Ok(false);
    };
    let latest_id = latest.get("id").and_then(Value::as_str);
    if latest_id == Some(turn_id.as_str())
        && latest.get("status").and_then(Value::as_str) == Some("inProgress")
    {
        let latest = prefer_loaded_terminal_turn(rpc, &run.execution.session_id, latest).await;
        if latest.get("status").and_then(Value::as_str) == Some("inProgress") {
            return Ok(true);
        }
        restore_direct_turn(rpc, run, &turn_id, &latest).await?;
        return Ok(false);
    }
    if latest_id == Some(turn_id.as_str()) && external_turn_is_terminal(&latest) {
        restore_direct_turn(rpc, run, &turn_id, &latest).await?;
    } else {
        // Reconcile the exact target before rejecting the command. This also
        // closes a Direct Run when a newer native turn proves it was
        // superseded, instead of leaving the Host command channel occupied.
        let _ = reconcile_bound_direct_turn(rpc, run, &turn_id).await?;
    }
    Ok(false)
}

/// Polls the exact bound turn as a fallback for notification loss. Looking up
/// by turn id is intentionally separate from `latest_native_turn`: a newer
/// unrelated turn must never be adopted as this Orchestral Run.
async fn reconcile_bound_direct_turn(
    rpc: &CodexRpcClient,
    run: &Arc<CodexRun>,
    turn_id: &str,
) -> Result<bool, AgentProtocolError> {
    let latest = latest_native_turn(rpc, &run.execution.session_id)
        .await
        .map_err(transport_to_protocol)?;
    let turn = match latest.as_ref() {
        Some(latest) if latest.get("id").and_then(Value::as_str) == Some(turn_id) => Some(
            prefer_loaded_terminal_turn_for_poll(rpc, &run.execution.session_id, latest.clone())
                .await,
        ),
        _ => {
            find_native_turn_or_loaded(rpc, &run.execution.session_id, turn_id, "notLoaded").await?
        }
    };
    let Some(turn) = turn else {
        let misses = run.direct_history_misses.fetch_add(1, Ordering::SeqCst) + 1;
        if misses < DIRECT_HISTORY_MISS_LIMIT {
            tracing::debug!(
                run_id = %run.execution.run_id,
                session_id = %run.execution.session_id,
                turn_id,
                misses,
                "waiting for Codex to index a bound direct turn"
            );
            return Ok(false);
        }
        return Err(protocol_error(
            "Codex authoritative history repeatedly omitted the bound direct turn",
            true,
        ));
    };
    run.direct_history_misses.store(0, Ordering::SeqCst);
    if latest.as_ref().is_some_and(|candidate| {
        candidate.get("id").and_then(Value::as_str) != Some(turn_id)
            && native_turn_is_newer(candidate, &turn)
    }) {
        publish_incomplete(
            run,
            IncompleteReason::ProviderEnded {
                reason: "Provider-native turn was superseded by a newer turn in the same session"
                    .to_owned(),
            },
        );
        return Ok(true);
    }
    match turn.get("status").and_then(Value::as_str) {
        Some("inProgress") => Ok(false),
        Some("completed" | "failed") => {
            restore_direct_turn(rpc, run, turn_id, &turn).await?;
            Ok(true)
        }
        // `turn/interrupt` is an authoritative mutation acknowledged by the
        // same shared daemon. Some Codex releases omit completedAt from the
        // subsequent interrupted snapshot when a code-mode result was lost.
        // Once this exact Run has an accepted cancel request, interrupted is
        // therefore sufficient to converge; without that causal evidence the
        // conservative external-owner rule still applies.
        Some("interrupted") if direct_turn_is_terminal_for_run(run, &turn) => {
            restore_direct_turn(rpc, run, turn_id, &turn).await?;
            Ok(true)
        }
        Some("interrupted") => {
            if native_turn_was_superseded(rpc, &run.execution.session_id, &turn).await? {
                publish_incomplete(
                    run,
                    IncompleteReason::ProviderEnded {
                        reason: "Provider-native turn was superseded by a newer turn in the same session"
                            .to_owned(),
                    },
                );
                Ok(true)
            } else {
                Ok(false)
            }
        }
        status => Err(protocol_error(
            format!("Codex returned an unsupported bound turn status: {status:?}"),
            true,
        )),
    }
}

async fn restore_direct_turn(
    rpc: &CodexRpcClient,
    run: &Arc<CodexRun>,
    turn_id: &str,
    turn: &Value,
) -> Result<(), AgentProtocolError> {
    match load_external_turn_items(rpc, run.execution.session_id.as_str(), turn_id).await {
        Ok(items) => {
            let detailed = json!({"items": items});
            observe_external_turn_items(run, &detailed);
            restore_final_response(run, &detailed);
        }
        // Recovery cannot claim complete delivery from possibly partial live
        // deltas when its authoritative output read failed. Failed/cancelled
        // turns may still converge from their terminal status alone.
        Err(error) if turn.get("status").and_then(Value::as_str) == Some("completed") => {
            return Err(error);
        }
        Err(_) => {}
    }
    restore_final_response(run, turn);
    finish_run(run, Some(turn));
    Ok(())
}

async fn monitor_native_run(
    rpc: Arc<CodexRpcClient>,
    run: Arc<CodexRun>,
    turn_id: String,
    mut notifications: broadcast::Receiver<CodexTransportEvent>,
) {
    let poll = tokio::time::sleep(DIRECT_RUN_POLL_INTERVAL);
    tokio::pin!(poll);
    let mut reconciliation: Option<BoxFuture<'_, Result<bool, AgentProtocolError>>> = None;
    let mut had_notification_gap = false;
    loop {
        // Native history can take seconds to read on a long session. Keep
        // draining live output/approvals while that read is pending, with at
        // most one reconciliation in flight. This future belongs to the
        // monitor, so a live terminal event also cancels the obsolete read.
        let received = tokio::select! {
            biased;
            result = async { reconciliation.as_mut().unwrap().await }, if reconciliation.is_some() => {
                reconciliation = None;
                // Schedule from completion, not the previous start: a slow
                // read must never create a backlog of immediately due polls.
                poll.as_mut().reset(tokio::time::Instant::now() + DIRECT_RUN_POLL_INTERVAL);
                match result {
                    Ok(true) => return,
                    Ok(false) => continue,
                    Err(error) => {
                        run.detached.store(true, Ordering::SeqCst);
                        let _ = run.critical_sender.send(Err(error));
                        return;
                    }
                }
            }
            received = notifications.recv() => received,
            _ = &mut poll, if reconciliation.is_none() => {
                reconciliation = Some(Box::pin(reconcile_bound_direct_turn(&rpc, &run, &turn_id)));
                continue;
            }
        };
        let message = match received {
            Ok(CodexTransportEvent::Message(message)) => message,
            Ok(CodexTransportEvent::Disconnected { reason }) => {
                run.detached.store(true, Ordering::SeqCst);
                let _ = run.critical_sender.send(Err(protocol_error(
                    format!("Codex app-server disconnected: {reason}"),
                    true,
                )));
                return;
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                had_notification_gap = true;
                // Notifications are an optimization; exact native history is
                // the authority. A bounded subscriber gap must not poison the
                // Host Run or disable future steering. Reconcile immediately
                // and keep monitoring when the bound turn is still active.
                tracing::warn!(
                    run_id = %run.execution.run_id,
                    session_id = %run.execution.session_id,
                    skipped,
                    "reconciling Codex Run after a notification gap"
                );
                // Multiple gaps during a read share the same reconciliation.
                if reconciliation.is_none() {
                    reconciliation =
                        Some(Box::pin(reconcile_bound_direct_turn(&rpc, &run, &turn_id)));
                }
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => {
                run.detached.store(true, Ordering::SeqCst);
                let _ = run.critical_sender.send(Err(protocol_error(
                    "Codex app-server notification channel closed",
                    true,
                )));
                return;
            }
        };
        if !belongs_to_run(&message, &run.execution.session_id, &turn_id) {
            continue;
        }
        // Live notifications own the hot path. Poll only after this turn has
        // gone quiet; unrelated turns cannot postpone the fallback.
        poll.as_mut()
            .reset(tokio::time::Instant::now() + DIRECT_RUN_POLL_INTERVAL);
        run.direct_history_misses.store(0, Ordering::SeqCst);
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match method {
            "item/agentMessage/delta" => {
                if let Some(delta) = message.pointer("/params/delta").and_then(Value::as_str) {
                    lock(&run.final_response).push_str(delta);
                    publish_telemetry(
                        &run,
                        AgentTelemetry::OutputDelta {
                            output_id: output_id(&run),
                            delta: Content::text(delta),
                        },
                    );
                }
            }
            "item/completed" => handle_completed_item(&run, message.pointer("/params/item")),
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => {
                open_native_request(&run, &message, PendingRequestKind::Approval);
            }
            "item/tool/requestUserInput" => {
                open_native_request(&run, &message, PendingRequestKind::Input);
            }
            "serverRequest/resolved" => close_native_request(&run, &message),
            "item/tool/call" => {
                respond_dynamic_tool(&rpc, &run, &message).await;
            }
            "turn/completed" => {
                let turn = message.pointer("/params/turn");
                if had_notification_gap {
                    // The live terminal notification can overtake a slow
                    // history read. Do not commit a partial output assembled
                    // from the surviving deltas; restore this exact turn first.
                    drop(reconciliation);
                    if let Some(turn) = turn {
                        if let Err(error) = restore_direct_turn(&rpc, &run, &turn_id, turn).await {
                            run.detached.store(true, Ordering::SeqCst);
                            let _ = run.critical_sender.send(Err(error));
                        }
                    } else {
                        finish_run(&run, None);
                    }
                } else {
                    finish_run(&run, turn);
                }
                return;
            }
            _ => {}
        }
    }
}

async fn respond_dynamic_tool(rpc: &CodexRpcClient, run: &CodexRun, message: &Value) {
    let Some(id) = message.get("id").cloned() else {
        return;
    };
    let result = publish_artifact_tool(rpc, run, message.pointer("/params")).await;
    let (success, text) = match result {
        Ok(text) => (true, text),
        Err(error) => (false, error),
    };
    let _ = rpc
        .respond(
            id,
            json!({
                "contentItems": [{"type": "inputText", "text": text}],
                "success": success
            }),
        )
        .await;
}

async fn publish_artifact_tool(
    rpc: &CodexRpcClient,
    run: &CodexRun,
    params: Option<&Value>,
) -> Result<String, String> {
    let params = params.ok_or_else(|| "dynamic tool call omitted params".to_owned())?;
    if params
        .get("namespace")
        .is_some_and(|value| !value.is_null())
        || params.get("tool").and_then(Value::as_str) != Some("publish_artifact")
    {
        return Err("Orchestral does not provide this dynamic tool".to_owned());
    }
    let publisher = run
        .artifact_publisher
        .as_ref()
        .ok_or_else(|| "Artifact publication is not configured on this Host".to_owned())?;
    let arguments = params
        .get("arguments")
        .and_then(Value::as_object)
        .ok_or_else(|| "publish_artifact arguments must be an object".to_owned())?;
    let source_path = arguments
        .get("path")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "publish_artifact requires a non-empty path".to_owned())?;
    let optional_string = |name: &str| -> Result<Option<String>, String> {
        match arguments.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
            Some(_) => Err(format!(
                "publish_artifact {name} must be a non-empty string"
            )),
        }
    };
    let thread = rpc
        .request(
            "thread/read",
            json!({
                "threadId": run.execution.session_id.as_str(),
                "includeTurns": false
            }),
        )
        .await
        .map_err(|error| format!("could not read the session workspace: {error}"))?;
    let workspace_root = thread
        .pointer("/thread/cwd")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "Codex session has no workspace directory".to_owned())?;
    let artifact = publisher
        .publish(ArtifactPublishRequest {
            workspace_root: PathBuf::from(workspace_root),
            source_path: PathBuf::from(source_path),
            file_name: optional_string("file_name")?,
            media_type: optional_string("media_type")?,
        })
        .await
        .map_err(|error| error.to_string())?;
    let file_name = artifact.file_name.as_deref().unwrap_or("artifact");
    Ok(format!(
        "Published `{file_name}` ({} bytes, {}, sha256 {}). Give the user this download link: [{}]({})",
        artifact.byte_size,
        artifact.media_type,
        artifact.artifact.digest.as_str(),
        file_name,
        artifact.uri
    ))
}

fn establish_turn(run: &Arc<CodexRun>, turn_id: &str) {
    *lock(&run.turn_id) = Some(turn_id.to_owned());
    publish_run_started(run);
}

fn publish_run_started(run: &Arc<CodexRun>) {
    if lock(&run.durable)
        .iter()
        .any(|draft| matches!(draft.payload, AgentEvent::RunStarted))
    {
        return;
    }
    publish_event(
        run,
        AgentEventDraft {
            event_id: event_id(run, "started"),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunStarted,
        },
    );
}

async fn wait_for_compaction_turn(
    session_id: &AgentSessionId,
    notifications: &mut broadcast::Receiver<CodexTransportEvent>,
) -> Result<String, AgentStartError> {
    loop {
        match notifications.recv().await {
            Ok(CodexTransportEvent::Message(message)) => {
                if message.get("method").and_then(Value::as_str) != Some("item/started")
                    || message.pointer("/params/threadId").and_then(Value::as_str)
                        != Some(session_id.as_str())
                    || message.pointer("/params/item/type").and_then(Value::as_str)
                        != Some("contextCompaction")
                {
                    continue;
                }
                return message
                    .pointer("/params/turnId")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        AgentStartError::OutcomeUnknown(protocol_error(
                            "Codex compaction item omitted turnId",
                            false,
                        ))
                    });
            }
            Ok(CodexTransportEvent::Disconnected { reason }) => {
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    format!("Codex disconnected while starting compaction: {reason}"),
                    true,
                )));
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    format!("Codex compaction notifications lagged by {skipped}"),
                    true,
                )));
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Err(AgentStartError::OutcomeUnknown(protocol_error(
                    "Codex notification channel closed while starting compaction",
                    true,
                )));
            }
        }
    }
}

fn invalid_action(message: impl Into<String>) -> AgentStartError {
    AgentStartError::Rejected(AgentRejection::new(
        AgentRejectionCode::InvalidSpec,
        message,
    ))
}

fn validate_native_action(action: &AgentSessionActionInvocation) -> Result<(), AgentStartError> {
    match action.action_id.as_str() {
        SESSION_COMPACT_ACTION if action.arguments.is_null() => Ok(()),
        SESSION_COMPACT_ACTION => Err(invalid_action("session.compact takes no arguments")),
        SESSION_REVIEW_ACTION => review_target(&action.arguments).map(|_| ()),
        other => Err(invalid_action(format!(
            "Codex does not support Run session action {other}"
        ))),
    }
}

fn review_target(arguments: &Value) -> Result<Value, AgentStartError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| invalid_action("session.review arguments must be an object"))?;
    let allowed = ["target", "branch", "sha", "title", "instructions"];
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid_action(format!(
            "session.review does not accept argument {key}"
        )));
    }
    let target = action_string(object, "target")?;
    match target.as_str() {
        "uncommitted_changes" => Ok(json!({"type": "uncommittedChanges"})),
        "base_branch" => Ok(json!({
            "type": "baseBranch",
            "branch": action_string(object, "branch")?
        })),
        "commit" => {
            let mut target = json!({
                "type": "commit",
                "sha": action_string(object, "sha")?
            });
            if let Some(title) = optional_action_string(object, "title")? {
                target["title"] = Value::String(title);
            }
            Ok(target)
        }
        "custom" => Ok(json!({
            "type": "custom",
            "instructions": action_string(object, "instructions")?
        })),
        _ => Err(invalid_action(
            "session.review target must be uncommitted_changes, base_branch, commit, or custom",
        )),
    }
}

fn action_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String, AgentStartError> {
    optional_action_string(object, field)?
        .ok_or_else(|| invalid_action(format!("session.review requires {field}")))
}

fn optional_action_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<String>, AgentStartError> {
    match object.get(field) {
        None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(invalid_action(format!(
            "session.review {field} must be a non-empty string"
        ))),
    }
}

fn handle_completed_item(run: &Arc<CodexRun>, item: Option<&Value>) {
    let Some(item) = item else { return };
    match item.get("type").and_then(Value::as_str) {
        Some("agentMessage") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                *lock(&run.final_response) = text.to_owned();
            }
        }
        Some("commandExecution") => {
            let command = item
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or("command");
            publish_tool(
                run,
                item,
                "exec_command",
                vec![ToolActivityEvidence::Command {
                    command: safe_text(command, 512),
                }],
            );
        }
        Some("fileChange") => publish_tool(
            run,
            item,
            "file_change",
            vec![ToolActivityEvidence::Note {
                text: "Codex changed workspace files".to_owned(),
            }],
        ),
        Some("mcpToolCall") => publish_tool(run, item, "mcp", Vec::new()),
        Some("dynamicToolCall") => publish_tool(run, item, "dynamic_tool", Vec::new()),
        Some("contextCompaction") => publish_tool(
            run,
            item,
            "session.compact",
            vec![ToolActivityEvidence::Note {
                text: "Codex compacted the native session context".to_owned(),
            }],
        ),
        _ => {}
    }
}

fn publish_tool(
    run: &Arc<CodexRun>,
    item: &Value,
    tool_name: &str,
    evidence: Vec<ToolActivityEvidence>,
) {
    let id = item.get("id").and_then(Value::as_str).unwrap_or("unknown");
    let state = match item.get("status").and_then(Value::as_str) {
        Some("failed") => ToolActivityState::Failed,
        Some("interrupted") => ToolActivityState::Cancelled,
        _ => ToolActivityState::Succeeded,
    };
    let sequence = run.telemetry_seq.fetch_add(1, Ordering::SeqCst) + 1;
    let _ = run
        .critical_sender
        .send(Ok(AgentProviderStreamItem::Telemetry(
            AgentTelemetryEnvelope {
                telemetry_id: TelemetryId::new(format!(
                    "codex-{}-telemetry-{}",
                    run.execution.run_id.as_str(),
                    sequence
                )),
                run_id: run.execution.run_id.clone(),
                provider_seq: Some(sequence),
                payload: AgentTelemetry::ToolActivity {
                    activity_id: ToolActivityId::new(format!("codex-{id}")),
                    tool_name: tool_name.to_owned(),
                    state,
                    evidence,
                },
            },
        )));
}

fn open_native_request(run: &Arc<CodexRun>, message: &Value, kind: PendingRequestKind) {
    let Some((request, native)) = normalize_native_request(message, kind) else {
        return;
    };
    let request_id = request.request_id.clone();
    lock(&run.pending).insert(request_id.clone(), native);
    publish_event(
        run,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "codex-{}-request-{}",
                run.execution.run_id.as_str(),
                request_id.as_str()
            )),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestOpened { request },
        },
    );
}

/// Converts one Codex server request into the provider-neutral public request
/// plus the opaque native response handle. Session observation and Run
/// execution deliberately share this translation so they cannot disagree on
/// identity, kind, scope, or presentation.
pub(super) fn normalize_native_request(
    message: &Value,
    kind: PendingRequestKind,
) -> Option<(PendingRequest, NativePendingRequest)> {
    let rpc_id = message.get("id").cloned()?;
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let native_key = params
        .get("approvalId")
        .or_else(|| params.get("itemId"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| rpc_id.to_string());
    let request_id = RequestId::new(format!("codex-{native_key}"));
    let payload = match kind {
        PendingRequestKind::Approval => {
            let reason = params
                .get("reason")
                .and_then(Value::as_str)
                .or_else(|| params.get("command").and_then(Value::as_str))
                .unwrap_or("Codex requests approval for an effect");
            PendingRequestPayload::Approval {
                operation_digest: Digest::sha256(serde_json::to_vec(&params).unwrap_or_default()),
                requested_scope: approval_scopes(
                    message.get("method").and_then(Value::as_str),
                    &params,
                ),
                session_approval_scope: None,
                reason: safe_text(reason, 1_000),
            }
        }
        PendingRequestKind::Input => PendingRequestPayload::Input {
            prompt: vec![Content::text(input_prompt(&params))],
            input_schema: None,
        },
        _ => return None,
    };
    let request = PendingRequest {
        request_id,
        blocking: true,
        payload,
    };
    Some((
        request,
        NativePendingRequest {
            rpc_id,
            method: message
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            kind,
            params,
        },
    ))
}

fn close_native_request(run: &Arc<CodexRun>, message: &Value) {
    let Some(native_request_id) = message.pointer("/params/requestId") else {
        return;
    };
    let request_id = {
        let pending = lock(&run.pending);
        pending
            .iter()
            .find(|(_, native)| &native.rpc_id == native_request_id)
            .map(|(request_id, _)| request_id.clone())
    };
    let Some(request_id) = request_id else {
        return;
    };
    if lock(&run.pending).remove(&request_id).is_none() {
        return;
    }
    publish_event(
        run,
        AgentEventDraft {
            event_id: AgentEventId::new(format!(
                "codex-{}-request-{}-closed",
                run.execution.run_id.as_str(),
                request_id.as_str()
            )),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RequestClosed {
                request_id,
                reason: "Codex reported that the server request was resolved or cleared".to_owned(),
            },
        },
    );
}

fn finish_run(run: &Arc<CodexRun>, turn: Option<&Value>) {
    if run.terminal.load(Ordering::SeqCst) || run.finalizing.load(Ordering::SeqCst) {
        return;
    }
    let status = turn
        .and_then(|turn| turn.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("failed");
    match status {
        "completed" => {
            let response = lock(&run.final_response).clone();
            let output_event_id = event_id(run, "output");
            publish_terminal_events(
                run,
                vec![
                    AgentEventDraft {
                        event_id: output_event_id.clone(),
                        run_id: run.execution.run_id.clone(),
                        causation_id: None,
                        source_fingerprint: None,
                        payload: AgentEvent::OutputCommitted {
                            output_id: output_id(run),
                            content: vec![Content::text(response.clone())],
                        },
                    },
                    AgentEventDraft {
                        event_id: event_id(run, "delivered"),
                        run_id: run.execution.run_id.clone(),
                        causation_id: None,
                        source_fingerprint: None,
                        payload: AgentEvent::DeliveryCommitted {
                            delivery: AgentDelivery {
                                delivery_id: DeliveryId::new(format!(
                                    "codex-{}-delivery",
                                    run.execution.run_id.as_str()
                                )),
                                run_id: run.execution.run_id.clone(),
                                spec_digest: run.execution.spec_digest.clone(),
                                final_response: Content::text(response),
                                outputs: Vec::new(),
                                artifacts: Vec::new(),
                                unresolved_issues: Vec::new(),
                                usage: None,
                                provenance: Provenance {
                                    provider_id: run.execution.provider_id.clone(),
                                    agent_id: run.execution.agent_id.clone(),
                                    supporting_event_ids: vec![output_event_id],
                                    extensions: Extensions::new(),
                                },
                            },
                        },
                    },
                ],
            );
        }
        "interrupted" => {
            if let Some((command_id, reason)) = lock(&run.cancel_request).clone() {
                publish_cancelled(run, reason, command_id);
            } else {
                publish_incomplete(
                    run,
                    IncompleteReason::Interrupted {
                        reason: "Codex turn was interrupted by its native owner".to_owned(),
                    },
                );
            }
        }
        _ => {
            let message = turn
                .and_then(|turn| turn.pointer("/error/message"))
                .and_then(Value::as_str)
                .unwrap_or("Codex turn failed");
            publish_failure(run, "codex_turn_failed", message, false);
        }
    }
}

fn restore_final_response(run: &CodexRun, turn: &Value) {
    let response = turn
        .get("items")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().rev().find_map(|item| {
                (item.get("type").and_then(Value::as_str) == Some("agentMessage"))
                    .then(|| item.get("text").and_then(Value::as_str))
                    .flatten()
            })
        });
    if let Some(response) = response {
        *lock(&run.final_response) = response.to_owned();
    }
}

fn publish_failure(run: &Arc<CodexRun>, code: &str, message: &str, retryable: bool) {
    publish_terminal_events(
        run,
        vec![AgentEventDraft {
            event_id: event_id(run, "failed"),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunFailed {
                failure: AgentFailure {
                    code: code.to_owned(),
                    message: message.to_owned(),
                    retryable,
                    details: Value::Null,
                },
            },
        }],
    );
}

fn publish_incomplete(run: &Arc<CodexRun>, reason: IncompleteReason) {
    publish_terminal_events(
        run,
        vec![AgentEventDraft {
            event_id: event_id(run, "incomplete"),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunIncomplete {
                reason,
                partial_delivery: None,
            },
        }],
    );
}

fn publish_stop_requested(run: &Arc<CodexRun>, reason: String, command_id: CommandId) {
    if run.finalizing.load(Ordering::SeqCst) || run.terminal.load(Ordering::SeqCst) {
        return;
    }
    let mut durable = lock(&run.durable);
    if run.finalizing.load(Ordering::SeqCst)
        || run.terminal.load(Ordering::SeqCst)
        || run.stop_requested_published.load(Ordering::SeqCst)
    {
        return;
    }
    append_event_locked(
        run,
        &mut durable,
        AgentEventDraft {
            event_id: event_id(run, "stop-requested"),
            run_id: run.execution.run_id.clone(),
            causation_id: Some(command_id),
            source_fingerprint: None,
            payload: AgentEvent::StopRequested { reason },
        },
    );
    run.stop_requested_published.store(true, Ordering::SeqCst);
}

fn publish_cancelled(run: &Arc<CodexRun>, reason: String, command_id: CommandId) {
    if run
        .finalizing
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    let mut durable = lock(&run.durable);
    if !run.stop_requested_published.load(Ordering::SeqCst) {
        append_event_locked(
            run,
            &mut durable,
            AgentEventDraft {
                event_id: event_id(run, "stop-requested"),
                run_id: run.execution.run_id.clone(),
                causation_id: Some(command_id),
                source_fingerprint: None,
                payload: AgentEvent::StopRequested {
                    reason: reason.clone(),
                },
            },
        );
        run.stop_requested_published.store(true, Ordering::SeqCst);
    }
    append_event_locked(
        run,
        &mut durable,
        AgentEventDraft {
            event_id: event_id(run, "cancelled"),
            run_id: run.execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunCancelled { reason },
        },
    );
    run.terminal.store(true, Ordering::SeqCst);
}

fn clear_cancel_request(run: &CodexRun, command_id: &CommandId) {
    let mut pending = lock(&run.cancel_request);
    if pending
        .as_ref()
        .is_some_and(|(pending_id, _)| pending_id == command_id)
    {
        *pending = None;
    }
}

fn publish_terminal_events(run: &Arc<CodexRun>, drafts: Vec<AgentEventDraft>) {
    if run
        .finalizing
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    let mut durable = lock(&run.durable);
    for draft in drafts {
        append_event_locked(run, &mut durable, draft);
    }
    run.terminal.store(true, Ordering::SeqCst);
}

fn publish_event(run: &Arc<CodexRun>, draft: AgentEventDraft) {
    if run.finalizing.load(Ordering::SeqCst) || run.terminal.load(Ordering::SeqCst) {
        return;
    }
    let mut durable = lock(&run.durable);
    if run.finalizing.load(Ordering::SeqCst) || run.terminal.load(Ordering::SeqCst) {
        return;
    }
    append_event_locked(run, &mut durable, draft);
}

fn append_event_locked(run: &CodexRun, durable: &mut Vec<AgentEventDraft>, draft: AgentEventDraft) {
    if let Some(existing) = durable
        .iter()
        .find(|existing| existing.event_id == draft.event_id)
    {
        if existing != &draft {
            tracing::error!(
                run_id = %run.execution.run_id,
                event_id = %draft.event_id,
                "Codex reconstructed conflicting drafts for one durable event identity"
            );
        }
        return;
    }
    durable.push(draft.clone());
    let _ = run
        .critical_sender
        .send(Ok(AgentProviderStreamItem::Event(Box::new(draft))));
}

fn publish_telemetry(run: &Arc<CodexRun>, payload: AgentTelemetry) {
    let sequence = run.telemetry_seq.fetch_add(1, Ordering::SeqCst) + 1;
    let telemetry = AgentTelemetryEnvelope {
        telemetry_id: TelemetryId::new(format!(
            "codex-{}-telemetry-{}",
            run.execution.run_id.as_str(),
            sequence
        )),
        run_id: run.execution.run_id.clone(),
        provider_seq: Some(sequence),
        payload,
    };
    let _ = run.telemetry_sender.send(telemetry);
}

fn stream_for(run: &Arc<CodexRun>) -> AgentProviderStream {
    let (critical, telemetry, replay, closed) = {
        let durable = lock(&run.durable);
        let critical = run.critical_sender.subscribe();
        let telemetry = run.telemetry_sender.subscribe();
        let replay = durable.clone();
        let closed = run.terminal.load(Ordering::SeqCst) || run.detached.load(Ordering::SeqCst);
        (critical, telemetry, replay, closed)
    };
    let delivered_event_ids = replay
        .iter()
        .map(|draft| draft.event_id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let replay = replay
        .into_iter()
        .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft))));
    let replay = stream::iter(replay);
    if closed {
        return replay.boxed();
    }
    let state = (
        Arc::clone(run),
        critical,
        telemetry,
        delivered_event_ids,
        VecDeque::new(),
    );
    let live = stream::unfold(
        state,
        |(run, mut critical, mut telemetry, mut delivered, mut pending)| async move {
            loop {
                if let Some(item) = pending.pop_front() {
                    return Some((item, (run, critical, telemetry, delivered, pending)));
                }
                tokio::select! {
                    biased;
                    received = critical.recv() => match received {
                        Ok(item) => {
                            if let Ok(AgentProviderStreamItem::Event(draft)) = &item {
                                if !delivered.insert(draft.event_id.as_str().to_owned()) {
                                    continue;
                                }
                            }
                            return Some((item, (run, critical, telemetry, delivered, pending)));
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            // This lane contains only durable events and rare
                            // errors. Backfill durable events from the Run log
                            // rather than escalating a recoverable queue gap.
                            tracing::warn!(
                                run_id = %run.execution.run_id,
                                skipped,
                                "backfilling durable Codex events after a subscriber gap"
                            );
                            let missing = lock(&run.durable)
                                .iter()
                                .filter(|draft| !delivered.contains(draft.event_id.as_str()))
                                .cloned()
                                .collect::<Vec<_>>();
                            for draft in missing {
                                delivered.insert(draft.event_id.as_str().to_owned());
                                pending.push_back(Ok(AgentProviderStreamItem::Event(Box::new(
                                    draft,
                                ))));
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    },
                    received = telemetry.recv() => match received {
                        Ok(envelope) => {
                            return Some((
                                Ok(AgentProviderStreamItem::Telemetry(envelope)),
                                (run, critical, telemetry, delivered, pending),
                            ));
                        }
                        // Output deltas are best-effort. The exact final text
                        // arrives in the durable terminal delivery.
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return None,
                    },
                }
            }
        },
    );
    replay.chain(live).boxed()
}

pub(super) fn native_resolution(
    native: &NativePendingRequest,
    resolution: &RequestResolution,
) -> Result<Value, AgentProtocolError> {
    match resolution {
        RequestResolution::Approval { decision, .. } => {
            native_approval_resolution(native, *decision)
        }
        RequestResolution::Input { content } => native_input_resolution(native, content),
        RequestResolution::ExternalResult { .. } => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::RequestTypeMismatch,
            "Codex dynamic tool results are not declared by this adapter",
        )),
        _ => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::Unsupported,
            "Codex adapter does not support this request resolution",
        )),
    }
}

pub(super) fn native_session_resolution(
    native: &NativePendingRequest,
    resolution: &AgentSessionRequestResolution,
) -> Result<Value, AgentConnectorError> {
    let result = match resolution {
        AgentSessionRequestResolution::Approval { decision } => {
            native_approval_resolution(native, *decision)
        }
        AgentSessionRequestResolution::Input { content } => {
            native_input_resolution(native, content)
        }
        AgentSessionRequestResolution::ExternalResult { .. } => Err(AgentProtocolError::new(
            AgentProtocolErrorCode::RequestTypeMismatch,
            "Codex dynamic tool results are not declared by this adapter",
        )),
    };
    result.map_err(|error| AgentConnectorError::invalid(error.to_string()))
}

fn native_approval_resolution(
    native: &NativePendingRequest,
    decision: ApprovalDecision,
) -> Result<Value, AgentProtocolError> {
    if native.method == "item/permissions/requestApproval" {
        let permissions = match decision {
            ApprovalDecision::Allow => native
                .params
                .get("permissions")
                .cloned()
                .unwrap_or_else(|| json!({})),
            ApprovalDecision::Deny => json!({}),
            _ => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::Unsupported,
                    "Codex adapter does not support this approval decision",
                ));
            }
        };
        // The session SPI deliberately exposes only one-shot approval, so the
        // native permission grant is bounded to the current turn.
        return Ok(json!({"permissions": permissions, "scope": "turn"}));
    }
    let decision = match decision {
        ApprovalDecision::Allow => "accept",
        ApprovalDecision::Deny => "decline",
        _ => {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "Codex adapter does not support this approval decision",
            ));
        }
    };
    Ok(json!({ "decision": decision }))
}

fn native_input_resolution(
    native: &NativePendingRequest,
    content: &[Content],
) -> Result<Value, AgentProtocolError> {
    let answer = content_text(content)?;
    let questions = native
        .params
        .get("questions")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut answers = serde_json::Map::new();
    for question in questions {
        if let Some(id) = question.get("id").and_then(Value::as_str) {
            answers.insert(id.to_owned(), json!({"answers": [answer.clone()]}));
        }
    }
    Ok(json!({"answers": answers}))
}

impl CodexConnector {
    async fn codex_input(&self, request: &AgentStartRequest) -> Result<Value, AgentProtocolError> {
        codex_content(
            &request.run.spec.input,
            self.artifact_resolver.as_deref(),
            self.artifact_blob_store.as_deref(),
        )
        .await
    }
}

async fn codex_content(
    content: &[Content],
    artifact_resolver: Option<&dyn ArtifactResolver>,
    artifact_blob_store: Option<&dyn BlobStore>,
) -> Result<Value, AgentProtocolError> {
    let mut inline_text = Vec::new();
    let mut native_artifacts = Vec::new();
    for content in content {
        match (&content.media_type[..], &content.body) {
            ("text/plain", ContentBody::Inline(Value::String(text))) => {
                inline_text.push(text.clone());
            }
            (_, ContentBody::Artifact(artifact)) => {
                if content.media_type.starts_with("image/") {
                    let blob_store = artifact_blob_store.ok_or_else(|| {
                        AgentProtocolError::new(
                            AgentProtocolErrorCode::ProviderUnavailable,
                            "Codex image input requires a configured Host BlobStore",
                        )
                    })?;
                    let data_url =
                        inline_image_data_url(artifact, &content.media_type, blob_store).await?;
                    native_artifacts.push(json!({"type": "image", "url": data_url}));
                    continue;
                }
                let resolver = artifact_resolver.ok_or_else(|| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::ProviderUnavailable,
                        "Codex Artifact input requires a configured Host resolver",
                    )
                })?;
                let resolved = resolver.resolve(artifact).await.map_err(|error| {
                    AgentProtocolError::new(
                        AgentProtocolErrorCode::ProviderUnavailable,
                        format!("Codex could not resolve Artifact input: {error}"),
                    )
                    .with_retryable(true)
                })?;
                if resolved.media_type != content.media_type {
                    return Err(AgentProtocolError::new(
                        AgentProtocolErrorCode::InvalidDigest,
                        "resolved Artifact media type differs from durable Content",
                    ));
                }
                let label = resolved
                    .file_name
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("attachment");
                native_artifacts.push(json!({
                    "type": "text",
                    "text": format!(
                        "Attached file: {label} (type {}, {} bytes)\nDownload: {}",
                        resolved.media_type, resolved.byte_size, resolved.uri
                    )
                }));
            }
            _ => {
                return Err(AgentProtocolError::new(
                    AgentProtocolErrorCode::Unsupported,
                    "Codex adapter accepts text/plain inline data or Artifact content",
                ));
            }
        }
    }
    let mut native = Vec::new();
    if !inline_text.is_empty() {
        native.push(json!({"type": "text", "text": inline_text.join("\n")}));
    }
    native.extend(native_artifacts);
    if native.is_empty() {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            "Codex input must not be empty",
        ));
    }
    Ok(Value::Array(native))
}

async fn inline_image_data_url(
    artifact: &orchestral_core::agent_protocol::wire::ArtifactRefWithDigest,
    media_type: &str,
    blob_store: &dyn BlobStore,
) -> Result<String, AgentProtocolError> {
    let mut blob = blob_store
        .read(&BlobId::new(artifact.artifact_ref.as_str()))
        .await
        .map_err(|error| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                format!("Codex could not read image Artifact input: {error}"),
            )
            .with_retryable(true)
        })?;
    if blob.meta.id.as_str() != artifact.artifact_ref.as_str()
        || blob.meta.mime_type.as_deref() != Some(media_type)
        || blob
            .meta
            .checksum_sha256
            .as_deref()
            .is_some_and(|digest| digest != artifact.digest.as_str())
    {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "BlobStore image metadata differs from durable Artifact input",
        ));
    }

    let expected_size = usize::try_from(blob.meta.byte_size).map_err(|_| {
        AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidSpec,
            "image Artifact is too large to inline for Codex",
        )
    })?;
    let mut bytes = Vec::with_capacity(expected_size);
    while let Some(chunk) = blob.body.next().await {
        let chunk = chunk.map_err(|error| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::ProviderUnavailable,
                format!("Codex could not stream image Artifact input: {error}"),
            )
            .with_retryable(true)
        })?;
        bytes.extend_from_slice(&chunk);
        if bytes.len() > expected_size {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::InvalidDigest,
                "BlobStore image body exceeded its declared byte size",
            ));
        }
    }
    if bytes.len() != expected_size || Digest::sha256(&bytes) != artifact.digest {
        return Err(AgentProtocolError::new(
            AgentProtocolErrorCode::InvalidDigest,
            "BlobStore image body failed Artifact integrity validation",
        ));
    }
    Ok(format!(
        "data:{media_type};base64,{}",
        BASE64_STANDARD.encode(bytes)
    ))
}

fn content_text(content: &[Content]) -> Result<String, AgentProtocolError> {
    content
        .iter()
        .map(|content| match (&content.media_type[..], &content.body) {
            ("text/plain", ContentBody::Inline(Value::String(text))) => Ok(text.clone()),
            _ => Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "Codex adapter currently accepts inline text/plain content only",
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("\n"))
}

fn belongs_to_run(message: &Value, session_id: &AgentSessionId, turn_id: &str) -> bool {
    let params = message.get("params").unwrap_or(&Value::Null);
    params
        .get("threadId")
        .and_then(Value::as_str)
        .is_some_and(|id| id == session_id.as_str())
        && params
            .get("turnId")
            .and_then(Value::as_str)
            .or_else(|| params.pointer("/turn/id").and_then(Value::as_str))
            .is_none_or(|id| id == turn_id)
}

fn input_prompt(params: &Value) -> String {
    let text = params
        .get("questions")
        .and_then(Value::as_array)
        .map(|questions| {
            questions
                .iter()
                .filter_map(|question| question.get("question").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    if text.is_empty() {
        "Codex requests user input".to_owned()
    } else {
        safe_text(&text, 4_000)
    }
}

fn approval_scopes(method: Option<&str>, params: &Value) -> Vec<String> {
    let mut scopes = vec!["process".to_owned()];
    if method == Some("item/fileChange/requestApproval") {
        scopes.push("filesystem_write".to_owned());
    }
    if method == Some("item/permissions/requestApproval") {
        if params.pointer("/permissions/network").is_some()
            || params.pointer("/permissions/networkAccess").is_some()
        {
            scopes.push("network".to_owned());
        }
        if params.pointer("/permissions/fileSystem").is_some()
            || params.pointer("/permissions/filesystem").is_some()
        {
            scopes.push("filesystem_write".to_owned());
        }
    }
    if params.get("networkApprovalContext").is_some() {
        scopes.push("network".to_owned());
    }
    scopes
}

fn safe_text(value: &str, limit: usize) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(limit)
        .collect()
}

fn output_id(run: &CodexRun) -> OutputId {
    OutputId::new(format!("codex-{}-response", run.execution.run_id.as_str()))
}

fn event_id(run: &CodexRun, suffix: &str) -> AgentEventId {
    AgentEventId::new(format!("codex-{}-{suffix}", run.execution.run_id.as_str()))
}

fn start_transport_error(error: CodexTransportError, outcome_unknown: bool) -> AgentStartError {
    let rejection_code = match &error {
        CodexTransportError::Rpc(message) if message.contains("already has an active writer") => {
            AgentRejectionCode::SessionConflict
        }
        _ => AgentRejectionCode::ProviderUnavailable,
    };
    let protocol = transport_to_protocol(error);
    if outcome_unknown {
        AgentStartError::OutcomeUnknown(protocol)
    } else {
        AgentStartError::Rejected(
            AgentRejection::new(rejection_code, protocol.message)
                .with_retryable(protocol.retryable),
        )
    }
}

fn transport_to_protocol(error: CodexTransportError) -> AgentProtocolError {
    let retryable = matches!(
        error,
        CodexTransportError::Io(_)
            | CodexTransportError::Closed
            | CodexTransportError::Disconnected(_)
            | CodexTransportError::Timeout
    );
    protocol_error(error.to_string(), retryable)
}

fn connector_to_protocol(
    error: orchestral_core::agent_connector::AgentConnectorError,
) -> AgentProtocolError {
    AgentProtocolError::new(
        AgentProtocolErrorCode::ProviderUnavailable,
        error.to_string(),
    )
    .with_retryable(error.retryable)
}

fn protocol_error(message: impl Into<String>, retryable: bool) -> AgentProtocolError {
    AgentProtocolError::new(AgentProtocolErrorCode::ProviderUnavailable, message)
        .with_retryable(retryable)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use chrono::Utc;
    use futures_util::StreamExt;
    use orchestral_core::agent_protocol::reference::AgentRunStatus;
    use orchestral_core::agent_protocol::spi::AgentProvider;
    use orchestral_core::agent_protocol::wire::{
        AgentCommandEnvelope, AgentRunEnvelope, ApprovalGrantRef, CommandId, ProviderBindingRef,
    };
    use orchestral_runtime::AgentController;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::oneshot;

    use super::*;

    #[test]
    fn run_notification_requires_an_explicit_matching_thread() {
        let session_id = AgentSessionId::new("thread-owned");

        assert!(belongs_to_run(
            &json!({"params": {"threadId": "thread-owned", "turnId": "turn-owned"}}),
            &session_id,
            "turn-owned"
        ));
        assert!(!belongs_to_run(
            &json!({"params": {"turnId": "turn-owned"}}),
            &session_id,
            "turn-owned"
        ));
        assert!(!belongs_to_run(
            &json!({"params": {"threadId": "thread-other", "turnId": "turn-owned"}}),
            &session_id,
            "turn-owned"
        ));
    }

    struct StaticImageBlobStore {
        id: String,
        media_type: String,
        bytes: Vec<u8>,
    }

    #[async_trait]
    impl BlobStore for StaticImageBlobStore {
        async fn write(
            &self,
            _request: orchestral_core::io::BlobWriteRequest,
        ) -> Result<orchestral_core::io::BlobMeta, orchestral_core::io::BlobIoError> {
            Err(orchestral_core::io::BlobIoError::Unsupported(
                "test store is read-only".to_owned(),
            ))
        }

        async fn read(
            &self,
            blob_id: &BlobId,
        ) -> Result<orchestral_core::io::BlobRead, orchestral_core::io::BlobIoError> {
            if blob_id.as_str() != self.id {
                return Err(orchestral_core::io::BlobIoError::NotFound(
                    blob_id.to_string(),
                ));
            }
            let bytes = self.bytes.clone();
            let now = Utc::now();
            Ok(orchestral_core::io::BlobRead {
                meta: orchestral_core::io::BlobMeta {
                    id: BlobId::new(&self.id),
                    file_name: Some("fixture.png".to_owned()),
                    mime_type: Some(self.media_type.clone()),
                    byte_size: bytes.len() as u64,
                    checksum_sha256: Some(self.id.clone()),
                    metadata: json!({}),
                    created_at: now,
                    updated_at: now,
                },
                body: Box::pin(futures_util::stream::once(
                    async move { Ok(Bytes::from(bytes)) },
                )),
            })
        }

        async fn head(
            &self,
            _blob_id: &BlobId,
        ) -> Result<orchestral_core::io::BlobHead, orchestral_core::io::BlobIoError> {
            Err(orchestral_core::io::BlobIoError::Unsupported(
                "test store does not implement head".to_owned(),
            ))
        }

        async fn delete(
            &self,
            _blob_id: &BlobId,
        ) -> Result<bool, orchestral_core::io::BlobIoError> {
            Err(orchestral_core::io::BlobIoError::Unsupported(
                "test store is read-only".to_owned(),
            ))
        }
    }

    #[tokio::test]
    async fn image_artifacts_are_inlined_for_codex_instead_of_forwarding_remote_urls() {
        let bytes = b"fixture-png".to_vec();
        let digest = Digest::sha256(&bytes);
        let store = StaticImageBlobStore {
            id: digest.to_string(),
            media_type: "image/png".to_owned(),
            bytes,
        };
        let artifact = orchestral_core::agent_protocol::wire::ArtifactRefWithDigest {
            artifact_ref: orchestral_core::agent_protocol::wire::ArtifactRef::new(digest.as_str()),
            digest,
        };
        let content = vec![Content {
            media_type: "image/png".to_owned(),
            schema_id: None,
            body: ContentBody::Artifact(artifact),
        }];

        let native = codex_content(&content, None, Some(&store)).await.unwrap();

        assert_eq!(native[0]["type"], "image");
        assert_eq!(
            native[0]["url"],
            format!(
                "data:image/png;base64,{}",
                BASE64_STANDARD.encode(b"fixture-png")
            )
        );
        assert!(!native[0]["url"].as_str().unwrap().starts_with("http"));
    }

    #[test]
    fn active_writer_actions_remain_a_typed_session_conflict() {
        let error = start_transport_error(
            CodexTransportError::Rpc("thread fixture already has an active writer".to_owned()),
            false,
        );
        assert!(matches!(
            error,
            AgentStartError::Rejected(AgentRejection {
                code: AgentRejectionCode::SessionConflict,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn dispatch_claim_is_atomic_across_connector_instances() {
        let fixture = tempfile::TempDir::new().unwrap();
        let dispatch_dir = fixture.path().join("dispatch");
        let (client_io, _server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let rpc =
            CodexRpcClient::from_io(client_read, client_write, Duration::from_secs(1), 64 * 1024);
        let mut first = CodexConnector::with_client(rpc.clone(), "codex/test-first");
        first.config.dispatch_journal_dir = Some(dispatch_dir.clone());
        let mut second = CodexConnector::with_client(rpc, "codex/test-second");
        second.config.dispatch_journal_dir = Some(dispatch_dir.clone());
        let run = test_run("thread-claim", "run-claim");
        let client_message_id = queued_client_message_id(&run.execution);
        let input_digest =
            codex_user_input_digest(&json!([{"type": "text", "text": "fix the failing test"}]))
                .unwrap();

        assert!(matches!(
            first
                .claim_external_queue_dispatch(&run, &client_message_id, &input_digest)
                .unwrap(),
            DispatchClaim::Acquired(_)
        ));
        assert!(matches!(
            second
                .claim_external_queue_dispatch(&run, &client_message_id, &input_digest)
                .unwrap(),
            DispatchClaim::Existing
        ));
        assert_eq!(fs::read_dir(dispatch_dir).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn failed_dispatch_claim_releases_the_session_reservation() {
        let fixture = tempfile::TempDir::new().unwrap();
        let blocking_file = fixture.path().join("not-a-directory");
        fs::write(&blocking_file, b"fixture").unwrap();
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.config.dispatch_journal_dir = Some(blocking_file.join("dispatch"));
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write_error(
                &mut server_write,
                &resume,
                "thread already has an active writer",
            )
            .await;
            for _ in 0..2 {
                let request = next_request(&mut lines).await;
                server_write_result(
                    &mut server_write,
                    &request,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;
            }
        });

        match connector
            .start(start_request("thread-claim-failure", "run-claim-failure"))
            .await
        {
            Err(AgentStartError::Rejected(AgentRejection {
                code: AgentRejectionCode::ProviderUnavailable,
                ..
            })) => {}
            Err(error) => panic!("unexpected dispatch claim error: {error}"),
            Ok(_) => panic!("invalid dispatch journal unexpectedly accepted a run"),
        }
        {
            let state = connector.provider_state();
            assert!(state.runs.is_empty());
            assert!(state.sessions.is_empty());
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn failed_dispatch_claim_cleanup_preserves_the_uncertain_run() {
        let fixture = tempfile::TempDir::new().unwrap();
        let dispatch_dir = fixture.path().join("dispatch");
        let request = start_request("thread-claim-cleanup", "run-claim-cleanup");
        let descriptor = CodexConnector::provider_descriptor();
        let execution = AgentExecutionRef::for_start(&request, &descriptor).unwrap();
        let client_message_id = queued_client_message_id(&execution);
        let claim_key = Digest::sha256(client_message_id.as_bytes());
        let claim_path = dispatch_dir.join(format!("{}.json", claim_key.as_str()));

        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.config.dispatch_journal_dir = Some(dispatch_dir);
        let claim_path_for_server = claim_path.clone();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write_error(
                &mut server_write,
                &resume,
                "thread already has an active writer",
            )
            .await;
            for _ in 0..2 {
                let request = next_request(&mut lines).await;
                server_write_result(
                    &mut server_write,
                    &request,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;
            }

            let add = next_request(&mut lines).await;
            assert_eq!(add["method"], "thread/queue/add");
            fs::remove_file(&claim_path_for_server).unwrap();
            fs::create_dir(&claim_path_for_server).unwrap();
            server_write_error(&mut server_write, &add, "queue rejected the submission").await;
        });

        match connector.start(request).await {
            Err(AgentStartError::OutcomeUnknown(error)) => {
                assert!(error.message.contains("dispatch claim"));
            }
            Err(error) => panic!("unexpected dispatch cleanup error: {error}"),
            Ok(_) => panic!("failed dispatch claim cleanup unexpectedly accepted a run"),
        }
        {
            let state = connector.provider_state();
            assert!(state.runs.contains_key(&RunId::new("run-claim-cleanup")));
            assert!(state
                .sessions
                .contains_key(&AgentSessionId::new("thread-claim-cleanup")));
        }
        assert!(claim_path.is_dir());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn submitting_claim_does_not_succeed_only_because_a_monitor_is_running() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let run = test_run("thread-submitting", "run-submitting");
        let input = json!([{"type": "text", "text": "fix the failing test"}]);
        *lock(&run.route) = Some(NativeRunRoute::ExternalQueue {
            queued_submission_id: None,
            client_message_id: "submitting-client".to_owned(),
            input_digest: codex_user_input_digest(&input).unwrap(),
            phase: ExternalQueuePhase::Submitting,
            ambiguous_polls: 0,
        });
        run.detached.store(false, Ordering::SeqCst);
        run.external_monitor_running.store(true, Ordering::SeqCst);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            for method in ["thread/queue/list", "thread/turns/list"] {
                let request = next_request(&mut lines).await;
                assert_eq!(request["method"], method);
                server_write_result(
                    &mut server_write,
                    &request,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;
            }
        });

        match connector.prepare_idempotent_start(&run).await {
            Err(AgentStartError::OutcomeUnknown(error)) => {
                assert!(error.retryable);
                assert!(error.message.contains("still being reconciled"));
            }
            Err(error) => panic!("unexpected submitting-claim error: {error}"),
            Ok(()) => panic!("monitor activity is not remote dispatch evidence"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn persisted_claim_prevents_readd_during_dequeue_history_gap() {
        let fixture = tempfile::TempDir::new().unwrap();
        let dispatch_dir = fixture.path().join("dispatch");
        let captured_add = Arc::new(Mutex::new(None));

        let (first_io, first_server_io) = duplex(1024 * 1024);
        let (first_read, first_write) = tokio::io::split(first_io);
        let (first_server_read, mut first_server_write) = tokio::io::split(first_server_io);
        let first_rpc = CodexRpcClient::from_io(
            first_read,
            first_write,
            Duration::from_millis(30),
            1024 * 1024,
        );
        let mut first = CodexConnector::with_client(first_rpc, "codex/test-first");
        first.config.dispatch_journal_dir = Some(dispatch_dir.clone());
        let captured_for_server = Arc::clone(&captured_add);
        let first_server = tokio::spawn(async move {
            let mut lines = BufReader::new(first_server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write_error(
                &mut first_server_write,
                &resume,
                "thread already has an active writer",
            )
            .await;
            for _ in 0..2 {
                let request = next_request(&mut lines).await;
                server_write_result(
                    &mut first_server_write,
                    &request,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;
            }
            let add = next_request(&mut lines).await;
            assert_eq!(add["method"], "thread/queue/add");
            *lock(&captured_for_server) = Some(add);
            tokio::time::sleep(Duration::from_millis(60)).await;
        });
        let request = start_request("thread-gap", "run-gap");
        let first_result = first.start(request.clone()).await;
        match first_result {
            Err(AgentStartError::OutcomeUnknown(_)) => {}
            Err(error) => panic!("unexpected first dispatch error: {error}"),
            Ok(_) => panic!("lost queue/add response unexpectedly confirmed start"),
        }
        first_server.await.unwrap();
        let add = lock(&captured_add).clone().unwrap();

        let (second_io, second_server_io) = duplex(1024 * 1024);
        let (second_read, second_write) = tokio::io::split(second_io);
        let (second_server_read, mut second_server_write) = tokio::io::split(second_server_io);
        let second_rpc = CodexRpcClient::from_io(
            second_read,
            second_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut second = CodexConnector::with_client(second_rpc, "codex/test-second");
        second.config.dispatch_journal_dir = Some(dispatch_dir);
        let (evidence_tx, evidence_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let second_server = tokio::spawn(async move {
            let mut lines = BufReader::new(second_server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write_error(
                &mut second_server_write,
                &resume,
                "thread already has an active writer",
            )
            .await;
            for _ in 0..2 {
                let request = next_request(&mut lines).await;
                server_write_result(
                    &mut second_server_write,
                    &request,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;
            }
            let observed = next_request(&mut lines).await;
            assert_eq!(observed["method"], "thread/queue/list");
            server_write_result(
                &mut second_server_write,
                &observed,
                json!({
                    "data": [{
                        "id": "already-dispatched",
                        "input": add["params"]["input"],
                        "clientUserMessageId": add["params"]["clientUserMessageId"]
                    }],
                    "nextCursor": null
                }),
            )
            .await;
            evidence_tx.send(()).unwrap();
            finish_rx.await.unwrap();
        });

        match second.start(request.clone()).await {
            Err(AgentStartError::OutcomeUnknown(error)) => {
                assert!(error.retryable);
                assert!(error
                    .message
                    .contains("state reconciliation is in progress"));
            }
            Err(error) => panic!("unexpected persisted-claim error: {error}"),
            Ok(_) => panic!("a local dispatch claim without remote evidence must not succeed"),
        }
        timeout(Duration::from_secs(1), evidence_rx)
            .await
            .expect("remote queue evidence timed out")
            .unwrap();
        let observed_run = second
            .provider_state()
            .runs
            .get(&RunId::new("run-gap"))
            .cloned()
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    lock(&observed_run.route).as_ref(),
                    Some(NativeRunRoute::ExternalQueue {
                        phase: ExternalQueuePhase::Queued,
                        ..
                    })
                ) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote queue evidence was not applied");
        second
            .start(request)
            .await
            .expect("the same Run may succeed after remote queue evidence appears");
        finish_tx.send(()).unwrap();
        second_server.await.unwrap();
    }

    #[tokio::test]
    async fn queued_submission_stays_accepted_until_its_correlated_turn_is_observed() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_client(rpc, "codex/test"));
        let (queued_tx, queued_rx) = oneshot::channel();
        let (allow_turn_tx, allow_turn_rx) = oneshot::channel();
        let (turn_tx, turn_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write_error(
                &mut server_write,
                &resume,
                "thread already has an active writer",
            )
            .await;

            for method in ["thread/queue/list", "thread/turns/list"] {
                let request = next_request(&mut lines).await;
                assert_eq!(request["method"], method);
                server_write_result(
                    &mut server_write,
                    &request,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;
            }

            let add = next_request(&mut lines).await;
            assert_eq!(add["method"], "thread/queue/add");
            let client_message_id = add["params"]["clientUserMessageId"]
                .as_str()
                .expect("queue client id")
                .to_owned();
            let input = add["params"]["input"].clone();
            server_write_result(
                &mut server_write,
                &add,
                json!({
                    "queuedSubmission": {
                        "id": "queued-boundary",
                        "input": input,
                        "clientUserMessageId": client_message_id
                    }
                }),
            )
            .await;

            let queued = next_request(&mut lines).await;
            assert_eq!(queued["method"], "thread/queue/list");
            server_write_result(
                &mut server_write,
                &queued,
                json!({
                    "data": [{
                        "id": "queued-boundary",
                        "input": input,
                        "clientUserMessageId": client_message_id
                    }],
                    "nextCursor": null
                }),
            )
            .await;
            queued_tx.send(()).unwrap();
            allow_turn_rx.await.unwrap();

            let consumed = next_request(&mut lines).await;
            assert_eq!(consumed["method"], "thread/queue/list");
            server_write_result(
                &mut server_write,
                &consumed,
                json!({"data": [], "nextCursor": null}),
            )
            .await;
            let turns = next_request(&mut lines).await;
            assert_eq!(turns["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &turns,
                json!({
                    "data": [{
                        "id": "turn-boundary",
                        "status": "inProgress",
                        "completedAt": null,
                        "items": [{
                            "type": "userMessage",
                            "id": "user-boundary",
                            "clientId": client_message_id,
                            "content": input
                        }]
                    }],
                    "nextCursor": null
                }),
            )
            .await;
            turn_tx.send(()).unwrap();
            finish_rx.await.unwrap();
        });

        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = start_request("thread-boundary", "run-boundary").run;
        let execution = controller.start(run).await.unwrap();
        timeout(Duration::from_secs(1), queued_rx)
            .await
            .expect("queued observation timed out")
            .unwrap();

        let accepted = controller.inspect(&execution.run_id).await.unwrap();
        assert_eq!(accepted.state.status(), AgentRunStatus::Accepted);
        let accepted_journal = controller.events(&execution.run_id, 0).await.unwrap();
        assert_eq!(accepted_journal.len(), 1);
        assert!(matches!(
            &accepted_journal[0].event.payload,
            AgentEvent::RunAccepted { .. }
        ));

        allow_turn_tx.send(()).unwrap();
        timeout(Duration::from_secs(1), turn_rx)
            .await
            .expect("turn observation timed out")
            .unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if controller
                    .inspect(&execution.run_id)
                    .await
                    .unwrap()
                    .state
                    .status()
                    == AgentRunStatus::Running
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("correlated turn did not publish RunStarted");
        let running_journal = controller.events(&execution.run_id, 0).await.unwrap();
        assert_eq!(
            running_journal
                .iter()
                .filter(|record| matches!(&record.event.payload, AgentEvent::RunStarted))
                .count(),
            1
        );

        finish_tx.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn mismatched_external_turn_never_publishes_run_started() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let run = test_run("thread-mismatch", "run-mismatch");
        let client_message_id = "expected-client".to_owned();
        let expected_input = json!([{"type": "text", "text": "fix the failing test"}]);
        *lock(&run.route) = Some(NativeRunRoute::ExternalQueue {
            queued_submission_id: Some("queued-mismatch".to_owned()),
            client_message_id: client_message_id.clone(),
            input_digest: codex_user_input_digest(&expected_input).unwrap(),
            phase: ExternalQueuePhase::Queued,
            ambiguous_polls: 0,
        });
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let queue = next_request(&mut lines).await;
            assert_eq!(queue["method"], "thread/queue/list");
            server_write_result(
                &mut server_write,
                &queue,
                json!({"data": [], "nextCursor": null}),
            )
            .await;
            let turns = next_request(&mut lines).await;
            assert_eq!(turns["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &turns,
                json!({
                    "data": [
                        {
                            "id": "turn-other-client",
                            "status": "inProgress",
                            "items": [{
                                "type": "userMessage",
                                "id": "user-other-client",
                                "clientId": "other-client",
                                "content": expected_input
                            }]
                        },
                        {
                            "id": "turn-changed-input",
                            "status": "inProgress",
                            "items": [{
                                "type": "userMessage",
                                "id": "user-changed-input",
                                "clientId": client_message_id,
                                "content": [{"type": "text", "text": "different input"}]
                            }]
                        }
                    ],
                    "nextCursor": null
                }),
            )
            .await;
        });

        let error = reconcile_external_queued_run(&rpc, &run)
            .await
            .expect_err("changed correlated input must be rejected");
        assert_eq!(error.code, AgentProtocolErrorCode::DuplicateConflict);
        assert!(lock(&run.turn_id).is_none());
        assert!(!lock(&run.durable)
            .iter()
            .any(|event| matches!(event.payload, AgentEvent::RunStarted)));
        assert!(matches!(
            lock(&run.route).as_ref(),
            Some(NativeRunRoute::ExternalQueue {
                phase: ExternalQueuePhase::Queued,
                ..
            })
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn shared_owner_active_turn_accepts_initial_input_through_strict_steer() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write_result(
                &mut server_write,
                &resume,
                json!({
                    "thread": {
                        "id": "thread-live",
                        "turns": [{
                            "id": "turn-live",
                            "status": "inProgress",
                            "items": []
                        }]
                    }
                }),
            )
            .await;

            let steer = next_request(&mut lines).await;
            assert_eq!(steer["method"], "turn/steer");
            assert_eq!(steer["params"]["threadId"], "thread-live");
            assert_eq!(steer["params"]["expectedTurnId"], "turn-live");
            assert_eq!(steer["params"]["input"][0]["text"], "fix the failing test");
            assert!(steer["params"]["clientUserMessageId"]
                .as_str()
                .is_some_and(|id| id.starts_with("orchestral:run-live:")));
            server_write_result(&mut server_write, &steer, json!({"turnId": "turn-live"})).await;
            for notification in [
                json!({
                    "method": "item/agentMessage/delta",
                    "params": {
                        "threadId": "thread-live",
                        "turnId": "turn-live",
                        "itemId": "message-live",
                        "delta": "Steer received."
                    }
                }),
                json!({
                    "method": "turn/completed",
                    "params": {
                        "threadId": "thread-live",
                        "turn": {
                            "id": "turn-live",
                            "status": "completed",
                            "items": [{
                                "id": "message-live",
                                "type": "agentMessage",
                                "text": "Steer received."
                            }]
                        }
                    }
                }),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(start_request("thread-live", "run-live"))
            .await
            .expect("shared owner must accept a strict live steer")
            .stream;
        let mut started = false;
        let mut delivery = None;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("live steer stream timed out")
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                match event.payload {
                    AgentEvent::RunStarted => started = true,
                    AgentEvent::DeliveryCommitted { delivery: value } => {
                        delivery = Some(value);
                        break;
                    }
                    _ => {}
                }
            }
        }
        assert!(started);
        assert_eq!(
            delivery.expect("live steer must complete").final_response,
            Content::text("Steer received.")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn shared_daemon_loaded_writer_accepts_strict_steer_without_reattaching() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.config.allow_deferred_queue = false;
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write_error(
                &mut server_write,
                &resume,
                "thread thread-live already has an active writer",
            )
            .await;

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/loaded/list");
            server_write_result(
                &mut server_write,
                &loaded,
                json!({"data": ["thread-live"], "nextCursor": null}),
            )
            .await;

            let turns = next_request(&mut lines).await;
            assert_eq!(turns["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &turns,
                json!({
                    "data": [{"id": "turn-live", "status": "inProgress", "items": []}],
                    "nextCursor": null
                }),
            )
            .await;

            let loaded_turn = next_request(&mut lines).await;
            assert_eq!(loaded_turn["method"], "thread/read");
            server_write_result(
                &mut server_write,
                &loaded_turn,
                json!({"thread": {
                    "id": "thread-live",
                    "turns": [{"id": "turn-live", "status": "inProgress", "items": []}]
                }}),
            )
            .await;

            let steer = next_request(&mut lines).await;
            assert_eq!(steer["method"], "turn/steer");
            assert_eq!(steer["params"]["expectedTurnId"], "turn-live");
            server_write_result(&mut server_write, &steer, json!({"turnId": "turn-live"})).await;
            server_write
                .write_all(
                    format!(
                        "{}\n{}\n",
                        json!({
                            "method": "item/agentMessage/delta",
                            "params": {
                                "threadId": "thread-live",
                                "turnId": "turn-live",
                                "itemId": "message-live",
                                "delta": "Shared daemon steer received."
                            }
                        }),
                        json!({
                            "method": "turn/completed",
                            "params": {
                                "threadId": "thread-live",
                                "turn": {
                                    "id": "turn-live",
                                    "status": "completed",
                                    "items": [{
                                        "id": "message-live",
                                        "type": "agentMessage",
                                        "text": "Shared daemon steer received."
                                    }]
                                }
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut stream = connector
            .start(start_request("thread-live", "run-live"))
            .await
            .expect("a writer loaded in the same daemon must remain steerable")
            .stream;
        let mut delivered = None;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("shared daemon steer stream timed out")
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                if let AgentEvent::DeliveryCommitted { delivery } = event.payload {
                    delivered = Some(delivery);
                    break;
                }
            }
        }
        assert_eq!(
            delivered
                .expect("shared daemon steer must deliver")
                .final_response,
            Content::text("Shared daemon steer received.")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn active_external_writer_is_rejected_when_live_control_is_required() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.config.allow_deferred_queue = false;
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": resume["id"],
                            "error": {
                                "code": -32600,
                                "message": "thread already has an active writer"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/loaded/list");
            server_write_result(
                &mut server_write,
                &loaded,
                json!({"data": [], "nextCursor": null}),
            )
            .await;
        });

        let error = match connector
            .start(start_request("thread-embedded", "run-embedded"))
            .await
        {
            Ok(_) => panic!("embedded owner must not be presented as a successful send"),
            Err(error) => error,
        };
        let AgentStartError::Rejected(rejection) = error else {
            panic!("live-control refusal must be a deterministic rejection");
        };
        assert_eq!(rejection.code, AgentRejectionCode::UnsupportedCapability);
        assert_eq!(
            rejection.details.get("code").and_then(Value::as_str),
            Some("live_control_unavailable")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn active_writer_routes_user_message_through_durable_queue() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": resume["id"],
                            "error": {
                                "code": -32000,
                                "message": "thread thread-external already has an active writer"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let preflight_queue = next_request(&mut lines).await;
            assert_eq!(preflight_queue["method"], "thread/queue/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": preflight_queue["id"], "result": {"data": [], "nextCursor": null}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let preflight_turns = next_request(&mut lines).await;
            assert_eq!(preflight_turns["method"], "thread/turns/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": preflight_turns["id"], "result": {"data": [], "nextCursor": null}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let queue = next_request(&mut lines).await;
            assert_eq!(queue["method"], "thread/queue/add");
            assert_eq!(queue["params"]["threadId"], "thread-external");
            let client_message_id = queue["params"]["clientUserMessageId"]
                .as_str()
                .expect("queue client id")
                .to_owned();
            assert!(client_message_id.starts_with("orchestral:run-external:"));
            assert_eq!(queue["params"]["input"][0]["text"], "fix the failing test");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": queue["id"],
                            "result": {
                                "queuedSubmission": {
                                    "id": "queued-1",
                                    "input": queue["params"]["input"],
                                    "clientUserMessageId": client_message_id
                                }
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let queued = next_request(&mut lines).await;
            assert_eq!(queued["method"], "thread/queue/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": queued["id"],
                            "result": {
                                "data": [{
                                    "id": "queued-1",
                                    "input": [{"type": "text", "text": "fix the failing test", "text_elements": []}],
                                    "clientUserMessageId": client_message_id
                                }],
                                "nextCursor": null
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let consumed = next_request(&mut lines).await;
            assert_eq!(consumed["method"], "thread/queue/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": consumed["id"], "result": {"data": [], "nextCursor": null}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let turns = next_request(&mut lines).await;
            assert_eq!(turns["method"], "thread/turns/list");
            assert_eq!(turns["params"]["sortDirection"], "desc");
            assert_eq!(turns["params"]["itemsView"], "summary");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": turns["id"],
                            "result": {
                                "data": [{
                                    "id": "turn-external",
                                    "status": "interrupted",
                                    "completedAt": null,
                                    "items": [
                                        {
                                            "type": "userMessage",
                                            "id": "user-external",
                                            "clientId": client_message_id,
                                            "content": [{"type": "text", "text": "fix the failing test"}]
                                        }
                                    ]
                                }],
                                "nextCursor": null
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let interrupted_latest = next_request(&mut lines).await;
            assert_eq!(interrupted_latest["method"], "thread/turns/list");
            assert_eq!(interrupted_latest["params"]["limit"], 1);
            server_write_result(
                &mut server_write,
                &interrupted_latest,
                json!({
                    "data": [{
                        "id": "turn-external",
                        "status": "interrupted",
                        "completedAt": null
                    }],
                    "nextCursor": null
                }),
            )
            .await;

            let interrupted_loaded = next_request(&mut lines).await;
            assert_eq!(interrupted_loaded["method"], "thread/read");
            assert_eq!(interrupted_loaded["params"]["includeTurns"], true);
            server_write_result(
                &mut server_write,
                &interrupted_loaded,
                json!({
                    "thread": {
                        "id": "thread-external",
                        "turns": [{
                            "id": "turn-external",
                            "status": "interrupted",
                            "completedAt": null
                        }]
                    }
                }),
            )
            .await;

            let completed_turns = next_request(&mut lines).await;
            assert_eq!(completed_turns["method"], "thread/turns/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": completed_turns["id"],
                            "result": {
                                "data": [{
                                    "id": "turn-external",
                                    "status": "completed",
                                    "completedAt": 1_788_000_000,
                                    "items": [
                                        {
                                            "type": "userMessage",
                                            "id": "user-external",
                                            "clientId": client_message_id,
                                            "content": [{"type": "text", "text": "fix the failing test"}]
                                        },
                                        {
                                            "type": "agentMessage",
                                            "id": "agent-external",
                                            "text": "queued work completed"
                                        }
                                    ]
                                }],
                                "nextCursor": null
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let items = next_request(&mut lines).await;
            assert_eq!(items["method"], "thread/items/list");
            assert_eq!(items["params"]["turnId"], "turn-external");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": items["id"],
                            "result": {
                                "data": [
                                    {"turnId": "turn-external", "item": {
                                        "type": "userMessage", "id": "user-external",
                                        "clientId": client_message_id,
                                        "content": [{"type": "text", "text": "fix the failing test"}]
                                    }},
                                    {"turnId": "turn-external", "item": {
                                        "type": "commandExecution", "id": "command-external",
                                        "command": "cargo test", "status": "completed"
                                    }},
                                    {"turnId": "turn-external", "item": {
                                        "type": "agentMessage", "id": "agent-external",
                                        "text": "queued work completed"
                                    }}
                                ],
                                "nextCursor": null,
                                "backwardsCursor": null
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut stream = connector
            .start(start_request("thread-external", "run-external"))
            .await
            .expect("active writer must accept a durable queued message")
            .stream;
        let mut delivered = None;
        let mut saw_tool = false;
        while let Some(item) = timeout(Duration::from_secs(4), stream.next())
            .await
            .expect("queued run stream timed out")
        {
            match item.unwrap() {
                AgentProviderStreamItem::Event(event) => {
                    if let AgentEvent::DeliveryCommitted { delivery } = event.payload {
                        delivered = Some(delivery);
                        break;
                    }
                }
                AgentProviderStreamItem::Telemetry(telemetry) => {
                    saw_tool |= matches!(telemetry.payload, AgentTelemetry::ToolActivity { .. });
                }
                _ => {}
            }
        }
        assert_eq!(
            delivered.expect("queued run must deliver").final_response,
            Content::text("queued work completed")
        );
        assert!(saw_tool);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lost_queue_add_response_is_reconciled_without_duplicate_submission() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_millis(40),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "error": {"code": -32000, "message": "thread already has an active writer"}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let preflight_queue = next_request(&mut lines).await;
            assert_eq!(preflight_queue["method"], "thread/queue/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": preflight_queue["id"], "result": {"data": [], "nextCursor": null}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let preflight_turns = next_request(&mut lines).await;
            assert_eq!(preflight_turns["method"], "thread/turns/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": preflight_turns["id"], "result": {"data": [], "nextCursor": null}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let add = next_request(&mut lines).await;
            assert_eq!(add["method"], "thread/queue/add");
            let client_id = add["params"]["clientUserMessageId"]
                .as_str()
                .unwrap()
                .to_owned();
            // Deliberately lose the add response. The next request must inspect
            // the durable queue, never dispatch a second queue/add.
            let reconcile = next_request(&mut lines).await;
            assert_eq!(reconcile["method"], "thread/queue/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": reconcile["id"],
                            "result": {
                                "data": [{
                                    "id": "queued-after-timeout",
                                    "input": add["params"]["input"],
                                    "clientUserMessageId": client_id
                                }],
                                "nextCursor": null
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let monitored = next_request(&mut lines).await;
            assert_eq!(monitored["method"], "thread/queue/list");
        });

        let request = start_request("thread-timeout", "run-timeout");
        assert!(matches!(
            connector.start(request.clone()).await,
            Err(AgentStartError::OutcomeUnknown(_))
        ));
        let retried = connector
            .start(request)
            .await
            .expect("same immutable start must reconcile the queued message");
        assert_eq!(retried.execution.run_id, RunId::new("run-timeout"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn external_queue_reconciliation_follows_opaque_pagination_cursors() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let queue_page_one = next_request(&mut lines).await;
            assert_eq!(queue_page_one["method"], "thread/queue/list");
            assert!(queue_page_one["params"]["cursor"].is_null());
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": queue_page_one["id"], "result": {
                            "data": [{"id": "other", "input": [], "clientUserMessageId": "other"}],
                            "nextCursor": "queue-page-2"
                        }})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let queue_page_two = next_request(&mut lines).await;
            assert_eq!(queue_page_two["params"]["cursor"], "queue-page-2");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": queue_page_two["id"], "result": {
                            "data": [{"id": "target", "input": [{"type": "text", "text": "target"}], "clientUserMessageId": "target-client"}],
                            "nextCursor": null
                        }})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let turns_page_one = next_request(&mut lines).await;
            assert_eq!(turns_page_one["method"], "thread/turns/list");
            assert!(turns_page_one["params"]["cursor"].is_null());
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": turns_page_one["id"], "result": {
                            "data": [{"id": "other-turn", "status": "completed", "items": []}],
                            "nextCursor": "turn-page-2",
                            "backwardsCursor": "do-not-use"
                        }})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let turns_page_two = next_request(&mut lines).await;
            assert_eq!(turns_page_two["params"]["cursor"], "turn-page-2");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": turns_page_two["id"], "result": {
                            "data": [{"id": "target-turn", "status": "inProgress", "items": [{
                                "type": "userMessage", "id": "target-user", "clientId": "target-client",
                                "content": [{"type": "text", "text": "target"}]
                            }]}],
                            "nextCursor": null,
                            "backwardsCursor": "ignored"
                        }})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let queued = find_queued_submission(&rpc, "thread-pages", "target-client")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queued["id"], "target");
        let turn = find_external_turn(&rpc, "thread-pages", "target-client", false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(turn["id"], "target-turn");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn recovery_finds_a_mid_turn_steer_through_native_item_pagination() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let metadata = next_request(&mut lines).await;
            assert_eq!(metadata["method"], "thread/turns/list");
            assert_eq!(metadata["params"]["itemsView"], "notLoaded");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": metadata["id"], "result": {
                            "data": [{
                                "id": "turn-active",
                                "status": "inProgress",
                                "items": []
                            }],
                            "nextCursor": null
                        }})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let items = next_request(&mut lines).await;
            assert_eq!(items["method"], "thread/items/list");
            assert_eq!(items["params"]["turnId"], "turn-active");
            assert_eq!(items["params"]["sortDirection"], "asc");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": items["id"], "result": {
                            "data": [{
                                "turnId": "turn-active",
                                "item": {
                                    "type": "userMessage",
                                    "id": "steer-user",
                                    "clientId": "target-client",
                                    "content": [{"type": "text", "text": "continue safely"}]
                                }
                            }],
                            "nextCursor": null
                        }})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let turn = find_external_turn(&rpc, "thread-steer", "target-client", true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(turn["id"], "turn-active");
        assert_eq!(turn["status"], "inProgress");
        assert_eq!(turn["items"][0]["id"], "steer-user");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn restarted_realtime_run_adopts_only_its_correlated_shared_daemon_turn() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.config.allow_deferred_queue = false;
        let descriptor = CodexConnector::provider_descriptor();
        let start = start_request("thread-realtime", "run-realtime");
        let execution = AgentExecutionRef::for_start(&start, &descriptor).unwrap();
        let expected_client_id = queued_client_message_id(&execution);
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/loaded/list");
            server_write_result(
                &mut server_write,
                &loaded,
                json!({"data": ["thread-realtime"], "nextCursor": null}),
            )
            .await;

            let correlated = next_request(&mut lines).await;
            assert_eq!(correlated["method"], "thread/items/list");
            assert!(correlated["params"]["turnId"].is_null());
            server_write_result(
                &mut server_write,
                &correlated,
                json!({
                    "data": [{
                        "turnId": "turn-realtime",
                        "item": {
                            "id": "user-realtime",
                            "type": "userMessage",
                            "clientId": expected_client_id,
                            "content": [
                                {"type": "text", "text": "fix the failing test"},
                                {"type": "localImage", "path": "/private/tmp/materialized.png"}
                            ]
                        }
                    }],
                    "nextCursor": null
                }),
            )
            .await;

            let turns = next_request(&mut lines).await;
            assert_eq!(turns["method"], "thread/turns/list");
            assert_eq!(turns["params"]["itemsView"], "notLoaded");
            server_write_result(
                &mut server_write,
                &turns,
                json!({
                    "data": [
                        {"id": "turn-unrelated-newer", "status": "inProgress"},
                        {"id": "turn-realtime", "status": "inProgress"}
                    ],
                    "nextCursor": null
                }),
            )
            .await;

            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write_result(
                &mut server_write,
                &resume,
                json!({"thread": {"id": "thread-realtime"}}),
            )
            .await;
        });

        let recovery = AgentRecoveryRequest::new(start, execution, &descriptor).unwrap();
        let recovered = connector.recover(recovery).await.unwrap();
        let (mut stream, confirmation) = recovered.into_parts();
        let started = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            started,
            AgentProviderStreamItem::Event(draft)
                if matches!(draft.payload, AgentEvent::RunStarted)
        ));
        confirmation.await.unwrap();
        let run = connector
            .provider_state()
            .runs
            .get(&RunId::new("run-realtime"))
            .cloned()
            .unwrap();
        assert!(matches!(
            lock(&run.route).as_ref(),
            Some(NativeRunRoute::Direct)
        ));
        assert_eq!(lock(&run.turn_id).as_deref(), Some("turn-realtime"));
        assert!(!run.detached.load(Ordering::SeqCst));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn recovery_finds_an_active_item_in_the_loaded_thread_edge() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let indexed = next_request(&mut lines).await;
            assert_eq!(indexed["method"], "thread/items/list");
            server_write_result(
                &mut server_write,
                &indexed,
                json!({"data": [], "nextCursor": null}),
            )
            .await;

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/read");
            assert_eq!(loaded["params"]["includeTurns"], true);
            server_write_result(
                &mut server_write,
                &loaded,
                json!({"thread": {
                    "id": "thread-loaded-edge",
                    "turns": [{
                        "id": "turn-loaded-edge",
                        "status": "inProgress",
                        "items": [{
                            "id": "user-loaded-edge",
                            "type": "userMessage",
                            "clientId": "target-client",
                            "content": [{"type": "text", "text": "still running"}]
                        }]
                    }]
                }}),
            )
            .await;
        });

        let found =
            find_external_turn_item(&rpc, "thread-loaded-edge", None, "target-client", "desc")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(found.0, "turn-loaded-edge");
        assert_eq!(found.1["id"], "user-loaded-edge");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn committed_started_run_converges_when_native_evidence_was_evicted() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let descriptor = CodexConnector::provider_descriptor();
        let start = start_request("thread-evicted", "run-evicted");
        let execution = AgentExecutionRef::for_start(&start, &descriptor).unwrap();
        let committed_started = AgentEventDraft {
            event_id: AgentEventId::new("committed-run-evicted-started"),
            run_id: execution.run_id.clone(),
            causation_id: None,
            source_fingerprint: None,
            payload: AgentEvent::RunStarted,
        };
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            for _ in 0..RECOVERY_EVIDENCE_MISS_LIMIT {
                let latest = next_request(&mut lines).await;
                assert_eq!(latest["method"], "thread/turns/list");
                assert_eq!(latest["params"]["limit"], 1);
                server_write_result(
                    &mut server_write,
                    &latest,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;

                let items = next_request(&mut lines).await;
                assert_eq!(items["method"], "thread/items/list");
                server_write_result(
                    &mut server_write,
                    &items,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;

                let loaded = next_request(&mut lines).await;
                assert_eq!(loaded["method"], "thread/read");
                server_write_result(
                    &mut server_write,
                    &loaded,
                    json!({"thread": {"id": "thread-evicted", "turns": []}}),
                )
                .await;

                let turns = next_request(&mut lines).await;
                assert_eq!(turns["method"], "thread/turns/list");
                assert_eq!(turns["params"]["itemsView"], "summary");
                server_write_result(
                    &mut server_write,
                    &turns,
                    json!({"data": [], "nextCursor": null}),
                )
                .await;
            }
        });

        let recovery = AgentRecoveryRequest::new(start, execution, &descriptor)
            .unwrap()
            .with_committed_provider_prefix(vec![committed_started.clone()])
            .unwrap();
        let first = connector.recover(recovery.clone()).await;
        assert!(first.is_err());
        let recovered = connector.recover(recovery).await.unwrap();
        let (mut stream, confirmation) = recovered.into_parts();
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            AgentProviderStreamItem::Event(Box::new(committed_started))
        );
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            AgentProviderStreamItem::Event(draft)
                if matches!(draft.payload, AgentEvent::RunIncomplete { .. })
        ));
        assert!(stream.next().await.is_none());
        confirmation.await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconstructs_a_queued_run_after_provider_restart_without_readding() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let descriptor = CodexConnector::provider_descriptor();
        let start = start_request("thread-restart", "run-restart");
        let execution = AgentExecutionRef::for_start(&start, &descriptor).unwrap();
        let client_id = queued_client_message_id(&execution);
        let expected_client_id = client_id.clone();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            for _ in 0..2 {
                let list = next_request(&mut lines).await;
                assert_eq!(list["method"], "thread/queue/list");
                server_write
                    .write_all(
                        format!(
                            "{}\n",
                            json!({"id": list["id"], "result": {
                                "data": [{
                                    "id": "queued-before-restart",
                                    "input": [{"type": "text", "text": "fix the failing test"}],
                                    "clientUserMessageId": expected_client_id
                                }],
                                "nextCursor": null
                            }})
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });

        let recovery = AgentRecoveryRequest::new(start, execution.clone(), &descriptor).unwrap();
        let recovered = connector.recover(recovery).await.unwrap();
        let (_stream, confirmation) = recovered.into_parts();
        confirmation.await.unwrap();
        assert!(client_id.starts_with("orchestral:run-restart:"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn superseded_recovery_converges_instead_of_retrying_session_conflict() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let descriptor = CodexConnector::provider_descriptor();
        let start = start_request("thread-superseded", "run-old");
        let execution = AgentExecutionRef::for_start(&start, &descriptor).unwrap();
        let client_id = queued_client_message_id(&execution);
        let newer = test_run("thread-superseded", "run-newer");
        {
            let mut state = connector.provider_state();
            state.sessions.insert(
                AgentSessionId::new("thread-superseded"),
                RunId::new("run-newer"),
            );
            state.runs.insert(RunId::new("run-newer"), newer);
        }
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let list = next_request(&mut lines).await;
            assert_eq!(list["method"], "thread/queue/list");
            server_write_result(
                &mut server_write,
                &list,
                json!({
                    "data": [{
                        "id": "old-before-reload",
                        "input": [{"type": "text", "text": "fix the failing test"}],
                        "clientUserMessageId": client_id
                    }],
                    "nextCursor": null
                }),
            )
            .await;
        });

        let recovery = AgentRecoveryRequest::new(start, execution, &descriptor).unwrap();
        let recovered = connector.recover(recovery).await.unwrap();
        let (_stream, confirmation) = recovered.into_parts();
        confirmation.await.unwrap();
        let old = connector
            .provider_state()
            .runs
            .get(&RunId::new("run-old"))
            .cloned()
            .unwrap();
        assert!(old.terminal.load(Ordering::SeqCst));
        assert!(lock(&old.durable)
            .iter()
            .any(|event| matches!(event.payload, AgentEvent::RunIncomplete { .. })));
        assert_eq!(
            connector
                .provider_state()
                .sessions
                .get(&AgentSessionId::new("thread-superseded")),
            Some(&RunId::new("run-newer"))
        );
        server.await.unwrap();
    }

    #[test]
    fn external_history_telemetry_is_bounded_and_keeps_both_edges() {
        let run = test_run("thread-history", "run-history");
        let items = (0..700)
            .map(|index| {
                json!({
                    "type": "commandExecution",
                    "id": format!("command-{index}"),
                    "command": format!("command {index}"),
                    "status": "completed"
                })
            })
            .collect::<Vec<_>>();
        observe_external_turn_items(&run, &json!({"status": "completed", "items": items}));

        assert_eq!(
            run.telemetry_seq.load(Ordering::SeqCst),
            EXTERNAL_HISTORY_TELEMETRY_LIMIT as u64
        );
        let observed = lock(&run.observed_item_ids);
        assert!(observed.contains("command-0"));
        assert!(observed.contains("command-699"));
        assert!(!observed.contains("command-350"));
    }

    #[test]
    fn failed_without_timestamp_is_terminal_but_external_interrupted_is_ambiguous() {
        assert!(external_turn_is_terminal(
            &json!({"status": "failed", "completedAt": null})
        ));
        assert!(!external_turn_is_terminal(
            &json!({"status": "interrupted", "completedAt": null})
        ));
        assert!(external_turn_is_terminal(
            &json!({"status": "interrupted", "completedAt": 1})
        ));
    }

    #[test]
    fn accepted_cancel_makes_an_untimestamped_direct_interrupt_terminal() {
        let run = test_run("thread-cancel-stall", "run-cancel-stall");
        let interrupted = json!({"status": "interrupted", "completedAt": null});

        assert!(!direct_turn_is_terminal_for_run(&run, &interrupted));
        *lock(&run.cancel_request) = Some((
            CommandId::new("watchdog-cancel"),
            "execution lease expired".to_owned(),
        ));
        assert!(direct_turn_is_terminal_for_run(&run, &interrupted));
    }

    #[tokio::test]
    async fn newer_turn_terminalizes_a_superseded_external_run() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let run = test_run("thread-superseded-native", "run-superseded-native");
        let client_message_id = queued_client_message_id(&run.execution);
        *lock(&run.route) = Some(NativeRunRoute::ExternalQueue {
            queued_submission_id: None,
            client_message_id: client_message_id.clone(),
            input_digest: Digest::sha256("fix the failing test"),
            phase: ExternalQueuePhase::Started,
            ambiguous_polls: 0,
        });
        establish_turn(&run, "turn-superseded-native");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let exact = next_request(&mut lines).await;
            assert_eq!(exact["method"], "thread/turns/list");
            assert_eq!(exact["params"]["itemsView"], "summary");
            server_write_result(
                &mut server_write,
                &exact,
                json!({
                    "data": [{
                        "id": "turn-superseded-native",
                        "status": "interrupted",
                        "startedAt": 1,
                        "completedAt": null,
                        "items": [{
                            "id": "user-superseded-native",
                            "type": "userMessage",
                            "clientId": client_message_id,
                            "content": [{"type": "text", "text": "fix the failing test"}]
                        }]
                    }],
                    "nextCursor": null
                }),
            )
            .await;

            let latest = next_request(&mut lines).await;
            assert_eq!(latest["method"], "thread/turns/list");
            assert_eq!(latest["params"]["limit"], 1);
            server_write_result(
                &mut server_write,
                &latest,
                json!({
                    "data": [{
                        "id": "turn-newer",
                        "status": "inProgress",
                        "startedAt": 2
                    }],
                    "nextCursor": null
                }),
            )
            .await;
        });

        assert_eq!(
            reconcile_external_queued_run(&rpc, &run).await.unwrap(),
            ExternalQueueObservation::Terminal
        );
        assert!(run.terminal.load(Ordering::SeqCst));
        assert!(lock(&run.durable)
            .iter()
            .any(|draft| matches!(draft.payload, AgentEvent::RunIncomplete { .. })));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn loaded_edge_terminalizes_run_when_history_latest_is_stale() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let run = test_run("thread-stale-latest", "run-stale-latest");
        let client_message_id = queued_client_message_id(&run.execution);
        *lock(&run.route) = Some(NativeRunRoute::ExternalQueue {
            queued_submission_id: None,
            client_message_id: client_message_id.clone(),
            input_digest: Digest::sha256("fix the failing test"),
            phase: ExternalQueuePhase::Started,
            ambiguous_polls: 0,
        });
        establish_turn(&run, "01a06994-e956-7842-92a6-71ae06ccd22d");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let exact = next_request(&mut lines).await;
            assert_eq!(exact["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &exact,
                json!({
                    "data": [{
                        "id": "01a06994-e956-7842-92a6-71ae06ccd22d",
                        "status": "interrupted",
                        "completedAt": null,
                        "items": [{
                            "id": "user-stale-latest",
                            "type": "userMessage",
                            "clientId": client_message_id,
                            "content": [{"type": "text", "text": "fix the failing test"}]
                        }]
                    }],
                    "nextCursor": null
                }),
            )
            .await;

            let indexed_latest = next_request(&mut lines).await;
            assert_eq!(indexed_latest["method"], "thread/turns/list");
            assert_eq!(indexed_latest["params"]["limit"], 1);
            server_write_result(
                &mut server_write,
                &indexed_latest,
                json!({
                    "data": [{
                        "id": "01a06994-e956-7842-92a6-71ae06ccd22d",
                        "status": "interrupted"
                    }],
                    "nextCursor": null
                }),
            )
            .await;

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/read");
            assert_eq!(loaded["params"]["includeTurns"], true);
            server_write_result(
                &mut server_write,
                &loaded,
                json!({
                    "thread": {
                        "id": "thread-stale-latest",
                        // A compacted/windowed loaded edge need not retain the
                        // older target turn, but its UUIDv7 timestamp still
                        // proves that this visible turn was created later.
                        "turns": [{
                            "id": "01a069a9-aa99-7690-8aaf-2aff5dd11d22",
                            "status": "inProgress"
                        }]
                    }
                }),
            )
            .await;
        });

        assert_eq!(
            reconcile_external_queued_run(&rpc, &run).await.unwrap(),
            ExternalQueueObservation::Terminal
        );
        assert!(run.terminal.load(Ordering::SeqCst));
        assert!(lock(&run.durable)
            .iter()
            .any(|draft| matches!(draft.payload, AgentEvent::RunIncomplete { .. })));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn queued_cancel_uses_exact_delete_and_lost_response_stays_ambiguous() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, _server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_millis(25),
            1024 * 1024,
        );
        let run = test_run("thread-cancel-queued", "run-cancel-queued");
        let command = AgentCommandEnvelope::new(
            CommandId::new("cancel-queued"),
            run.execution.run_id.clone(),
            None,
            AgentCommand::Cancel {
                reason: "stop queued work".to_owned(),
            },
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let delete = next_request(&mut lines).await;
            assert_eq!(delete["method"], "thread/queue/delete");
            assert_eq!(delete["params"]["threadId"], "thread-cancel-queued");
            assert_eq!(delete["params"]["queuedSubmissionId"], "queued-cancel");
            tokio::time::sleep(Duration::from_millis(60)).await;
        });

        let error = apply_external_queued_command(
            &rpc,
            &run,
            &command,
            Some("queued-cancel"),
            ExternalQueuePhase::Queued,
        )
        .await
        .expect_err("lost delete response must remain an ambiguous best-effort stop");
        assert_eq!(error.code, AgentProtocolErrorCode::ProviderUnavailable);
        assert!(error.retryable);
        assert!(!run.terminal.load(Ordering::SeqCst));
        assert!(!lock(&run.durable)
            .iter()
            .any(|event| matches!(event.payload, AgentEvent::StopRequested { .. })));
        assert!(!lock(&run.durable)
            .iter()
            .any(|event| matches!(event.payload, AgentEvent::RunCancelled { .. })));
        server.await.unwrap();
    }

    fn start_request(session_id: &str, run_id: &str) -> AgentStartRequest {
        let descriptor = CodexConnector::provider_descriptor();
        AgentStartRequest::new(
            AgentRunEnvelope::new(
                ProtocolVersion::new(1, 0),
                AgentSessionId::new(session_id),
                RunId::new(run_id),
                vec![Content::text("fix the failing test")],
            )
            .unwrap(),
            ProviderBindingRef::new("codex/local"),
            &descriptor,
        )
        .unwrap()
    }

    fn test_run(session_id: &str, run_id: &str) -> Arc<CodexRun> {
        let descriptor = CodexConnector::provider_descriptor();
        let request = start_request(session_id, run_id);
        let execution = AgentExecutionRef::for_start(&request, &descriptor).unwrap();
        new_codex_run(request, execution, AgentAdmission::default(), None)
    }

    #[tokio::test]
    async fn recovery_reattaches_an_in_memory_run_after_host_observer_gap() {
        let (client_io, _server_io) = duplex(1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let rpc = CodexRpcClient::from_io(client_read, client_write, Duration::from_secs(1), 1024);
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let descriptor = CodexConnector::provider_descriptor();
        let run = test_run("thread-attached", "run-attached");
        *lock(&run.route) = Some(NativeRunRoute::Direct);
        establish_turn(&run, "turn-attached");
        {
            let mut state = connector.provider_state();
            state.sessions.insert(
                run.execution.session_id.clone(),
                run.execution.run_id.clone(),
            );
            state
                .runs
                .insert(run.execution.run_id.clone(), Arc::clone(&run));
        }

        let request =
            AgentRecoveryRequest::new(run.request.clone(), run.execution.clone(), &descriptor)
                .unwrap();
        let recovered = connector.recover(request).await.unwrap();
        let (mut stream, confirmation) = recovered.into_parts();
        let replayed = stream.next().await.unwrap().unwrap();

        assert!(matches!(
            replayed,
            AgentProviderStreamItem::Event(draft)
                if matches!(draft.payload, AgentEvent::RunStarted)
        ));
        confirmation.await.unwrap();
        assert!(!run.detached.load(Ordering::SeqCst));
        assert!(!run.terminal.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn telemetry_backlog_cannot_evict_a_durable_run_event() {
        let run = test_run("thread-backlog", "run-backlog");
        let mut stream = stream_for(&run);
        for _ in 0..2_048 {
            publish_telemetry(
                &run,
                AgentTelemetry::OutputDelta {
                    output_id: output_id(&run),
                    delta: Content::text("x"),
                },
            );
        }
        establish_turn(&run, "turn-backlog");

        let item = timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            item,
            AgentProviderStreamItem::Event(draft)
                if matches!(draft.payload, AgentEvent::RunStarted)
        ));
    }

    #[tokio::test]
    async fn critical_subscriber_gap_backfills_every_durable_event() {
        let run = test_run("thread-critical-gap", "run-critical-gap");
        let mut stream = stream_for(&run);
        let event_count = 1_100;
        for index in 0..event_count {
            publish_event(
                &run,
                AgentEventDraft {
                    event_id: event_id(&run, &format!("backfill-{index}")),
                    run_id: run.execution.run_id.clone(),
                    causation_id: None,
                    source_fingerprint: None,
                    payload: AgentEvent::RunStarted,
                },
            );
        }

        let mut received = BTreeSet::new();
        for _ in 0..event_count {
            let item = timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let AgentProviderStreamItem::Event(draft) = item else {
                panic!("expected a durable event");
            };
            assert!(received.insert(draft.event_id.as_str().to_owned()));
        }
        assert_eq!(received.len(), event_count);
        assert!(timeout(Duration::from_millis(25), stream.next())
            .await
            .is_err());
    }

    #[test]
    fn artifact_tool_contract_is_provider_neutral_and_workspace_scoped() {
        let tools = artifact_dynamic_tools();
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["name"], "publish_artifact");
        assert_eq!(tools[0]["inputSchema"]["required"], json!(["path"]));
        assert_eq!(
            tools[0]["inputSchema"]["additionalProperties"],
            Value::Bool(false)
        );
    }

    fn action_start_request(
        session_id: &str,
        run_id: &str,
        action_id: &str,
        arguments: Value,
    ) -> AgentStartRequest {
        let descriptor = CodexConnector::provider_descriptor();
        let mut run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new(session_id),
            RunId::new(run_id),
            vec![Content::text("session action")],
        )
        .unwrap();
        AgentSessionActionInvocation {
            action_id: orchestral_core::agent_connector::AgentSessionActionId::new(action_id),
            arguments,
        }
        .insert_into(&mut run.spec)
        .unwrap();
        AgentStartRequest::new(
            AgentRunEnvelope::seal(run.spec).unwrap(),
            ProviderBindingRef::new("codex/local"),
            &descriptor,
        )
        .unwrap()
    }

    async fn next_request(
        lines: &mut tokio::io::Lines<BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    ) -> Value {
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn server_write_result(
        writer: &mut tokio::io::WriteHalf<tokio::io::DuplexStream>,
        request: &Value,
        result: Value,
    ) {
        writer
            .write_all(format!("{}\n", json!({"id": request["id"], "result": result})).as_bytes())
            .await
            .unwrap();
    }

    async fn server_write_error(
        writer: &mut tokio::io::WriteHalf<tokio::io::DuplexStream>,
        request: &Value,
        message: &str,
    ) {
        writer
            .write_all(
                format!(
                    "{}\n",
                    json!({"id": request["id"], "error": {"code": -32000, "message": message}})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }

    #[test]
    fn descriptor_promises_only_implemented_controls() {
        let descriptor = CodexConnector::provider_descriptor();
        assert!(descriptor.descriptor.capabilities.session_reuse);
        assert!(descriptor.descriptor.capabilities.controls.steer);
        assert_eq!(
            descriptor.descriptor.capabilities.controls.cancel,
            CancelSupport::BestEffort
        );
        assert!(descriptor.descriptor.capabilities.controls.recover);
        assert_eq!(
            descriptor.descriptor.capabilities.pending_request_kinds,
            BTreeSet::from([PendingRequestKind::Input, PendingRequestKind::Approval])
        );
    }

    #[test]
    fn approval_mapping_never_grants_session_scope_implicitly() {
        let native = NativePendingRequest {
            rpc_id: json!(1),
            method: "item/commandExecution/requestApproval".to_owned(),
            kind: PendingRequestKind::Approval,
            params: json!({}),
        };
        let result = native_resolution(
            &native,
            &RequestResolution::Approval {
                decision: ApprovalDecision::Allow,
                grant_ref: Some(
                    orchestral_core::agent_protocol::wire::ApprovalGrantRef::new("single-use"),
                ),
            },
        )
        .unwrap();
        assert_eq!(result, json!({"decision": "accept"}));
    }

    #[test]
    fn permission_approval_uses_the_typed_0153_response_contract() {
        let native = NativePendingRequest {
            rpc_id: json!(2),
            method: "item/permissions/requestApproval".to_owned(),
            kind: PendingRequestKind::Approval,
            params: json!({
                "permissions": {
                    "fileSystem": {"write": ["/tmp"]},
                    "network": {"enabled": true}
                }
            }),
        };
        assert_eq!(
            native_resolution(
                &native,
                &RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: None,
                },
            )
            .unwrap(),
            json!({
                "permissions": {
                    "fileSystem": {"write": ["/tmp"]},
                    "network": {"enabled": true}
                },
                "scope": "turn"
            })
        );
        assert_eq!(
            native_resolution(
                &native,
                &RequestResolution::Approval {
                    decision: ApprovalDecision::Deny,
                    grant_ref: None,
                },
            )
            .unwrap(),
            json!({"permissions": {}, "scope": "turn"})
        );
    }

    #[tokio::test]
    async fn streams_codex_turn_to_normalized_delivery() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-1"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-1", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/agentMessage/delta", "params": {"threadId": "thread-1", "turnId": "turn-1", "itemId": "a1", "delta": "done"}}),
                json!({"method": "item/completed", "params": {"threadId": "thread-1", "turnId": "turn-1", "item": {"type": "commandExecution", "id": "c1", "command": "cargo test", "status": "completed"}}}),
                json!({"method": "item/completed", "params": {"threadId": "thread-1", "turnId": "turn-1", "item": {"type": "agentMessage", "id": "a1", "text": "done"}}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-1", "turn": {"id": "turn-1", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(start_request("thread-1", "run-1"))
            .await
            .unwrap()
            .stream;
        let mut event_types = Vec::new();
        let mut saw_tool = false;
        while let Some(item) = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            let item = item.unwrap();
            item.validate_integrity().unwrap();
            match item {
                AgentProviderStreamItem::Event(event) => {
                    event_types.push(format!("{:?}", event.payload));
                    if matches!(event.payload, AgentEvent::DeliveryCommitted { .. }) {
                        break;
                    }
                }
                AgentProviderStreamItem::Telemetry(telemetry) => {
                    saw_tool |= matches!(telemetry.payload, AgentTelemetry::ToolActivity { .. });
                }
                _ => {}
            }
        }
        assert!(event_types
            .iter()
            .any(|event| event.starts_with("RunStarted")));
        assert!(event_types
            .iter()
            .any(|event| event.starts_with("OutputCommitted")));
        assert!(event_types
            .iter()
            .any(|event| event.starts_with("DeliveryCommitted")));
        assert!(saw_tool);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn compact_action_uses_native_method_and_run_lifecycle() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-compact"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let compact = next_request(&mut lines).await;
            assert_eq!(compact["method"], "thread/compact/start");
            assert_eq!(compact["params"], json!({"threadId": "thread-compact"}));
            server_write
                .write_all(format!("{}\n", json!({"id": compact["id"], "result": {}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/started", "params": {"threadId": "thread-compact", "turnId": "turn-compact", "item": {"type": "contextCompaction", "id": "compact-1"}}}),
                json!({"method": "item/completed", "params": {"threadId": "thread-compact", "turnId": "turn-compact", "item": {"type": "contextCompaction", "id": "compact-1"}}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-compact", "turn": {"id": "turn-compact", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(action_start_request(
                "thread-compact",
                "run-compact",
                SESSION_COMPACT_ACTION,
                Value::Null,
            ))
            .await
            .unwrap()
            .stream;
        let mut delivered = None;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                if let AgentEvent::DeliveryCommitted { delivery } = event.payload {
                    delivered = Some(delivery);
                    break;
                }
            }
        }
        assert_eq!(
            delivered.unwrap().final_response,
            Content::text("Session context compacted.")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn review_action_maps_typed_target_to_native_review() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-review"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let review = next_request(&mut lines).await;
            assert_eq!(review["method"], "review/start");
            assert_eq!(
                review["params"],
                json!({
                    "threadId": "thread-review",
                    "target": {"type": "baseBranch", "branch": "main"},
                    "delivery": "inline"
                })
            );
            server_write
                .write_all(format!("{}\n", json!({"id": review["id"], "result": {"turn": {"id": "turn-review", "status": "inProgress", "items": []}, "reviewThreadId": "thread-review"}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/agentMessage/delta", "params": {"threadId": "thread-review", "turnId": "turn-review", "itemId": "review-message", "delta": "No findings."}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-review", "turn": {"id": "turn-review", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(action_start_request(
                "thread-review",
                "run-review",
                SESSION_REVIEW_ACTION,
                json!({"target": "base_branch", "branch": "main"}),
            ))
            .await
            .unwrap()
            .stream;
        let mut delivered = None;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                if let AgentEvent::DeliveryCommitted { delivery } = event.payload {
                    delivered = Some(delivery);
                    break;
                }
            }
        }
        assert_eq!(
            delivered.unwrap().final_response,
            Content::text("No findings.")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn competing_client_resolution_closes_the_host_approval_before_delivery() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write_result(
                &mut server_write,
                &resume,
                json!({"thread": {"id": "thread-race", "turns": []}}),
            )
            .await;
            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            server_write_result(
                &mut server_write,
                &start,
                json!({"turn": {"id": "turn-race", "status": "inProgress", "items": []}}),
            )
            .await;
            for notification in [
                json!({
                    "method": "item/commandExecution/requestApproval",
                    "id": 70,
                    "params": {
                        "threadId": "thread-race",
                        "turnId": "turn-race",
                        "itemId": "command-race",
                        "command": "cargo test",
                        "reason": "run tests"
                    }
                }),
                json!({
                    "method": "serverRequest/resolved",
                    "params": {"threadId": "thread-race", "requestId": 70}
                }),
                json!({
                    "method": "turn/completed",
                    "params": {
                        "threadId": "thread-race",
                        "turn": {
                            "id": "turn-race",
                            "status": "completed",
                            "items": [{
                                "id": "message-race",
                                "type": "agentMessage",
                                "text": "Tests passed."
                            }]
                        }
                    }
                }),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });

        let mut stream = connector
            .start(start_request("thread-race", "run-race"))
            .await
            .unwrap()
            .stream;
        let mut saw_open = false;
        let mut saw_closed = false;
        let mut delivered = false;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("approval race stream timed out")
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                match event.payload {
                    AgentEvent::RequestOpened { .. } => saw_open = true,
                    AgentEvent::RequestClosed { .. } => saw_closed = true,
                    AgentEvent::DeliveryCommitted { .. } => {
                        delivered = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
        assert!(saw_open && saw_closed && delivered);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn relays_approval_steer_and_cancel_without_session_grant() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-2"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-2", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "method": "item/commandExecution/requestApproval",
                            "id": 70,
                            "params": {
                                "threadId": "thread-2", "turnId": "turn-2", "itemId": "command-1",
                                "command": "git push", "reason": "network access"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let approval = next_request(&mut lines).await;
            assert_eq!(
                approval,
                json!({"id": 70, "result": {"decision": "accept"}})
            );

            let steer_preflight = next_request(&mut lines).await;
            assert_eq!(steer_preflight["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &steer_preflight,
                json!({
                    "data": [{"id": "turn-2", "status": "inProgress"}],
                    "nextCursor": null
                }),
            )
            .await;
            let steer_loaded = next_request(&mut lines).await;
            assert_eq!(steer_loaded["method"], "thread/read");
            server_write_result(
                &mut server_write,
                &steer_loaded,
                json!({"thread": {
                    "id": "thread-2",
                    "turns": [{"id": "turn-2", "status": "inProgress", "items": []}]
                }}),
            )
            .await;
            let steer = next_request(&mut lines).await;
            assert_eq!(steer["method"], "turn/steer");
            assert_eq!(steer["params"]["expectedTurnId"], "turn-2");
            assert!(steer["params"]["clientUserMessageId"]
                .as_str()
                .is_some_and(|id| id.starts_with("orchestral-command:run-2:steer-command:")));
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": steer["id"], "result": {"turnId": "turn-2"}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let cancel_preflight = next_request(&mut lines).await;
            assert_eq!(cancel_preflight["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &cancel_preflight,
                json!({
                    "data": [{"id": "turn-2", "status": "inProgress"}],
                    "nextCursor": null
                }),
            )
            .await;
            let cancel_loaded = next_request(&mut lines).await;
            assert_eq!(cancel_loaded["method"], "thread/read");
            server_write_result(
                &mut server_write,
                &cancel_loaded,
                json!({"thread": {
                    "id": "thread-2",
                    "turns": [{"id": "turn-2", "status": "inProgress", "items": []}]
                }}),
            )
            .await;
            let cancel = next_request(&mut lines).await;
            assert_eq!(cancel["method"], "turn/interrupt");
            server_write
                .write_all(format!("{}\n", json!({"id": cancel["id"], "result": {}})).as_bytes())
                .await
                .unwrap();
            server_write
                .write_all(format!("{}\n", json!({"method": "turn/completed", "params": {"threadId": "thread-2", "turn": {"id": "turn-2", "status": "interrupted", "items": []}}})).as_bytes())
                .await
                .unwrap();
        });

        let started = connector
            .start(start_request("thread-2", "run-2"))
            .await
            .unwrap();
        let execution = started.execution;
        let mut stream = started.stream;
        let request_id = loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let AgentProviderStreamItem::Event(event) = item {
                if let AgentEvent::RequestOpened { request } = event.payload {
                    break request.request_id;
                }
            }
        };
        let approval = AgentCommandEnvelope::new(
            CommandId::new("approval-command"),
            RunId::new("run-2"),
            Some(request_id),
            AgentCommand::ResolveRequest {
                response: RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: Some(ApprovalGrantRef::new("single-use-grant")),
                },
            },
        )
        .unwrap();
        let first = connector
            .command(&execution, approval.clone())
            .await
            .unwrap();
        assert!(!first.duplicate);
        let duplicate = connector.command(&execution, approval).await.unwrap();
        assert!(duplicate.duplicate);

        connector
            .command(
                &execution,
                AgentCommandEnvelope::new(
                    CommandId::new("steer-command"),
                    RunId::new("run-2"),
                    None,
                    AgentCommand::Steer {
                        content: vec![Content::text("focus on tests")],
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        connector
            .command(
                &execution,
                AgentCommandEnvelope::new(
                    CommandId::new("cancel-command"),
                    RunId::new("run-2"),
                    None,
                    AgentCommand::Cancel {
                        reason: "user stopped".to_owned(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(
                item,
                AgentProviderStreamItem::Event(event)
                    if matches!(event.payload, AgentEvent::RunCancelled { .. })
            ) {
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stale_direct_steer_is_durably_rejected_without_touching_a_newer_turn() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let run = test_run("thread-stale", "run-stale");
        *lock(&run.route) = Some(NativeRunRoute::Direct);
        let original_turn_id = "01a06994-e956-7842-92a6-71ae06ccd22d";
        let newer_turn_id = "01a069a9-aa99-7690-8aaf-2aff5dd11d22";
        *lock(&run.turn_id) = Some(original_turn_id.to_owned());
        {
            let mut state = connector.provider_state();
            state
                .runs
                .insert(run.execution.run_id.clone(), Arc::clone(&run));
            state.sessions.insert(
                run.execution.session_id.clone(),
                run.execution.run_id.clone(),
            );
        }
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let preflight = next_request(&mut lines).await;
            assert_eq!(preflight["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &preflight,
                json!({
                    "data": [{"id": newer_turn_id, "status": "inProgress"}],
                    "nextCursor": null
                }),
            )
            .await;

            let reconcile_latest = next_request(&mut lines).await;
            assert_eq!(reconcile_latest["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &reconcile_latest,
                json!({
                    "data": [{"id": newer_turn_id, "status": "inProgress"}],
                    "nextCursor": null
                }),
            )
            .await;

            let exact = next_request(&mut lines).await;
            assert_eq!(exact["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &exact,
                json!({
                    "data": [{
                        "id": original_turn_id,
                        "status": "interrupted",
                        "completedAt": null
                    }],
                    "nextCursor": null
                }),
            )
            .await;
        });
        let command = AgentCommandEnvelope::new(
            CommandId::new("stale-steer"),
            run.execution.run_id.clone(),
            None,
            AgentCommand::Steer {
                content: vec![Content::text("must not reach the newer turn")],
            },
        )
        .unwrap();
        let disposition = connector.command(&run.execution, command).await.unwrap();
        assert!(matches!(
            disposition.outcome,
            ProviderCommandOutcome::Rejected {
                code: AgentProtocolErrorCode::TerminalRun,
                ..
            }
        ));
        assert!(run.terminal.load(Ordering::SeqCst));
        assert!(lock(&run.durable)
            .iter()
            .any(|draft| matches!(draft.payload, AgentEvent::RunIncomplete { .. })));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn direct_monitor_reconciles_a_notification_gap_without_failing_the_run() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let run = test_run("thread-poll", "run-poll");
        establish_turn(&run, "turn-poll");
        let (notification_sender, notifications) = broadcast::channel(1);
        for delta in ["first", "second"] {
            notification_sender
                .send(CodexTransportEvent::Message(json!({
                    "method": "item/agentMessage/delta",
                    "params": {
                        "threadId": "thread-poll",
                        "turnId": "turn-poll",
                        "delta": delta
                    }
                })))
                .unwrap();
        }
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let latest = next_request(&mut lines).await;
            assert_eq!(latest["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &latest,
                json!({
                    "data": [{
                        "id": "turn-poll",
                        "status": "completed",
                        "completedAt": 1
                    }],
                    "nextCursor": null
                }),
            )
            .await;
            let items = next_request(&mut lines).await;
            assert_eq!(items["method"], "thread/items/list");
            assert_eq!(items["params"]["turnId"], "turn-poll");
            server_write_result(
                &mut server_write,
                &items,
                json!({
                    "data": [{
                        "turnId": "turn-poll",
                        "item": {
                            "id": "agent-poll",
                            "type": "agentMessage",
                            "status": "completed",
                            "text": "recovered by polling"
                        }
                    }],
                    "nextCursor": null
                }),
            )
            .await;
        });
        let monitored = Arc::clone(&run);
        let monitored_rpc = rpc.clone();
        let task = tokio::spawn(async move {
            monitor_native_run(
                monitored_rpc,
                monitored,
                "turn-poll".to_owned(),
                notifications,
            )
            .await;
        });
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(run.terminal.load(Ordering::SeqCst));
        assert!(lock(&run.durable).iter().any(|event| {
            matches!(
                &event.payload,
                AgentEvent::DeliveryCommitted { delivery }
                    if delivery.final_response == Content::text("recovered by polling")
            )
        }));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn slow_history_does_not_block_output_approval_or_terminal_notifications() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, _server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(10),
            64 * 1024,
        );
        let run = test_run("thread-slow", "run-slow");
        establish_turn(&run, "turn-slow");
        let (sender, notifications) = broadcast::channel(64);
        let task = tokio::spawn(monitor_native_run(
            rpc,
            run.clone(),
            "turn-slow".to_owned(),
            notifications,
        ));
        let mut lines = BufReader::new(server_read).lines();
        let history = timeout(Duration::from_secs(1), next_request(&mut lines))
            .await
            .unwrap();
        assert_eq!(history["method"], "thread/turns/list");
        // Leave history unanswered for the entire test, while publishing more
        // output than the production notification buffer could retain.
        sender.send(CodexTransportEvent::Message(json!({
            "id": 42, "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "thread-slow", "turnId": "turn-slow", "itemId": "approval", "command": "fixture"}
        }))).unwrap();
        timeout(Duration::from_secs(1), async {
            while !lock(&run.pending).contains_key(&RequestId::new("codex-approval")) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval must open before the history response");
        for batch in 1..=128 {
            for _ in 0..16 {
                sender
                    .send(CodexTransportEvent::Message(json!({
                        "method": "item/agentMessage/delta",
                        "params": {"threadId": "thread-slow", "turnId": "turn-slow", "delta": "x"}
                    })))
                    .unwrap();
            }
            timeout(Duration::from_secs(1), async {
                while lock(&run.final_response).len() < batch * 16 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("live output must drain during the history read");
        }
        sender
            .send(CodexTransportEvent::Message(json!({
                "method": "serverRequest/resolved",
                "params": {"threadId": "thread-slow", "turnId": "turn-slow", "requestId": 42}
            })))
            .unwrap();
        sender.send(CodexTransportEvent::Message(json!({
            "method": "turn/completed",
            "params": {"threadId": "thread-slow", "turn": {"id": "turn-slow", "status": "completed"}}
        }))).unwrap();
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(run.terminal.load(Ordering::SeqCst));
        assert!(!run.detached.load(Ordering::SeqCst));
        assert!(lock(&run.pending).is_empty());
        assert_eq!(lock(&run.final_response).len(), 2048);
        assert_eq!(
            lock(&run.durable)
                .iter()
                .filter(|draft| matches!(draft.payload, AgentEvent::DeliveryCommitted { .. }))
                .count(),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn direct_monitor_polls_only_when_quiet_and_waits_after_a_slow_read() {
        use futures_util::FutureExt;
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(10),
            64 * 1024,
        );
        let run = test_run("thread-quiet", "run-quiet");
        establish_turn(&run, "turn-quiet");
        let (sender, notifications) = broadcast::channel(64);
        let task = tokio::spawn(monitor_native_run(
            rpc,
            run.clone(),
            "turn-quiet".to_owned(),
            notifications,
        ));
        let mut lines = BufReader::new(server_read).lines();
        tokio::task::yield_now().await;
        for _ in 0..10 {
            tokio::time::advance(DIRECT_RUN_POLL_INTERVAL / 2).await;
            sender
                .send(CodexTransportEvent::Message(json!({
                    "method": "item/agentMessage/delta",
                    "params": {"threadId": "thread-quiet", "turnId": "turn-quiet", "delta": "x"}
                })))
                .unwrap();
            tokio::task::yield_now().await;
            assert!(
                lines.next_line().now_or_never().is_none(),
                "active output must not trigger history reads"
            );
        }
        tokio::time::advance(DIRECT_RUN_POLL_INTERVAL).await;
        let history = next_request(&mut lines).await;
        assert_eq!(history["method"], "thread/turns/list");
        tokio::time::advance(DIRECT_RUN_POLL_INTERVAL * 10).await;
        assert!(
            lines.next_line().now_or_never().is_none(),
            "at most one history read may be in flight"
        );
        server_write_result(
            &mut server_write,
            &history,
            json!({
                "data": [{"id": "turn-quiet", "status": "inProgress"}], "nextCursor": null
            }),
        )
        .await;
        let loaded = next_request(&mut lines).await;
        assert_eq!(loaded["method"], "thread/read");
        server_write_result(
            &mut server_write,
            &loaded,
            json!({
                "thread": {"id": "thread-quiet", "status": {"type": "active"}}
            }),
        )
        .await;
        assert_eq!(loaded["params"]["includeTurns"], false);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(DIRECT_RUN_POLL_INTERVAL / 2).await;
        assert!(
            lines.next_line().now_or_never().is_none(),
            "slow reads must not leave an overdue poll"
        );
        tokio::time::advance(DIRECT_RUN_POLL_INTERVAL).await;
        assert_eq!(
            next_request(&mut lines).await["method"],
            "thread/turns/list"
        );
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn terminal_after_a_gap_restores_full_output_before_committing() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(10),
            64 * 1024,
        );
        let run = test_run("thread-final-gap", "run-final-gap");
        establish_turn(&run, "turn-final-gap");
        let (sender, notifications) = broadcast::channel(2);
        for delta in ["missing", "surviving", "output"] {
            sender.send(CodexTransportEvent::Message(json!({
                "method": "item/agentMessage/delta",
                "params": {"threadId": "thread-final-gap", "turnId": "turn-final-gap", "delta": delta}
            }))).unwrap();
        }
        let task = tokio::spawn(monitor_native_run(
            rpc,
            run.clone(),
            "turn-final-gap".to_owned(),
            notifications,
        ));
        let mut lines = BufReader::new(server_read).lines();
        let history = timeout(Duration::from_secs(1), next_request(&mut lines))
            .await
            .unwrap();
        assert_eq!(history["method"], "thread/turns/list");
        sender.send(CodexTransportEvent::Message(json!({
            "method": "turn/completed",
            "params": {"threadId": "thread-final-gap", "turn": {"id": "turn-final-gap", "status": "completed"}}
        }))).unwrap();
        let items = timeout(Duration::from_secs(1), next_request(&mut lines))
            .await
            .unwrap();
        assert_eq!(items["method"], "thread/items/list");
        assert_eq!(items["params"]["turnId"], "turn-final-gap");
        assert!(!run.terminal.load(Ordering::SeqCst));
        server_write_result(&mut server_write, &items, json!({
            "data": [{"item": {"id": "answer", "type": "agentMessage", "text": "complete authoritative output"}}], "nextCursor": null
        })).await;
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        let durable = lock(&run.durable);
        let deliveries = durable
            .iter()
            .filter_map(|draft| match &draft.payload {
                AgentEvent::DeliveryCommitted { delivery } => Some(delivery),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(
            deliveries[0].final_response,
            Content::text("complete authoritative output")
        );
    }

    #[tokio::test]
    async fn failed_final_history_read_does_not_commit_partial_delivery() {
        let (client_io, server_io) = duplex(64 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc =
            CodexRpcClient::from_io(client_read, client_write, Duration::from_secs(1), 64 * 1024);
        let run = test_run("thread-final-error", "run-final-error");
        establish_turn(&run, "turn-final-error");
        *lock(&run.final_response) = "partial".to_owned();
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let items = next_request(&mut lines).await;
            assert_eq!(items["method"], "thread/items/list");
            server_write_error(&mut server_write, &items, "history unavailable").await;
        });
        assert!(restore_direct_turn(
            &rpc,
            &run,
            "turn-final-error",
            &json!({"id": "turn-final-error", "status": "completed"})
        )
        .await
        .is_err());
        assert!(!run.terminal.load(Ordering::SeqCst));
        assert!(!lock(&run.durable)
            .iter()
            .any(|draft| matches!(draft.payload, AgentEvent::DeliveryCommitted { .. })));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fresh_direct_turn_tolerates_a_transient_history_index_miss() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let run = test_run("thread-index-lag", "run-index-lag");
        establish_turn(&run, "turn-index-lag");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let latest = next_request(&mut lines).await;
            assert_eq!(latest["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &latest,
                json!({"data": [], "nextCursor": null}),
            )
            .await;

            let indexed = next_request(&mut lines).await;
            assert_eq!(indexed["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &indexed,
                json!({"data": [], "nextCursor": null}),
            )
            .await;

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/read");
            server_write_result(
                &mut server_write,
                &loaded,
                json!({"thread": {"id": "thread-index-lag", "turns": []}}),
            )
            .await;
        });

        assert!(!reconcile_bound_direct_turn(&rpc, &run, "turn-index-lag")
            .await
            .unwrap());
        assert_eq!(run.direct_history_misses.load(Ordering::SeqCst), 1);
        assert!(!run.detached.load(Ordering::SeqCst));
        assert!(!run.terminal.load(Ordering::SeqCst));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn polling_keeps_full_history_checks_for_unknown_or_mismatched_live_status() {
        for summary in [
            json!({"thread": {"id": "thread-status", "status": {"type": "notLoaded"}}}),
            json!({"thread": {"id": "another-thread", "status": {"type": "active"}}}),
        ] {
            let (client_io, server_io) = duplex(64 * 1024);
            let (client_read, client_write) = tokio::io::split(client_io);
            let (server_read, mut server_write) = tokio::io::split(server_io);
            let rpc = CodexRpcClient::from_io(
                client_read,
                client_write,
                Duration::from_secs(1),
                64 * 1024,
            );
            let server = tokio::spawn(async move {
                let mut lines = BufReader::new(server_read).lines();
                let metadata = next_request(&mut lines).await;
                assert_eq!(metadata["params"]["includeTurns"], false);
                server_write_result(&mut server_write, &metadata, summary).await;
                let full = next_request(&mut lines).await;
                assert_eq!(full["params"]["includeTurns"], true);
                server_write_result(&mut server_write, &full, json!({
                    "thread": {"id": "thread-status", "turns": [{"id": "turn-status", "status": "completed"}]}
                })).await;
            });
            let turn = prefer_loaded_terminal_turn_for_poll(
                &rpc,
                &AgentSessionId::new("thread-status"),
                json!({"id": "turn-status", "status": "inProgress"}),
            )
            .await;
            assert_eq!(turn["status"], "completed");
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn loaded_terminal_turn_overrides_a_stale_active_history_index() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let run = test_run("thread-stale-active", "run-stale-active");
        establish_turn(&run, "turn-stale-active");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let latest = next_request(&mut lines).await;
            assert_eq!(latest["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &latest,
                json!({
                    "data": [{"id": "turn-stale-active", "status": "inProgress"}],
                    "nextCursor": null
                }),
            )
            .await;

            let summary = next_request(&mut lines).await;
            assert_eq!(summary["method"], "thread/read");
            assert_eq!(summary["params"]["includeTurns"], false);
            server_write_result(
                &mut server_write,
                &summary,
                json!({
                    "thread": {"id": "thread-stale-active", "status": {"type": "idle"}}
                }),
            )
            .await;

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/read");
            assert_eq!(loaded["params"]["includeTurns"], true);
            server_write_result(
                &mut server_write,
                &loaded,
                json!({
                    "thread": {
                        "id": "thread-stale-active",
                        "turns": [{
                            "id": "turn-stale-active",
                            "status": "completed",
                            "completedAt": 1,
                            "items": []
                        }]
                    }
                }),
            )
            .await;

            let items = next_request(&mut lines).await;
            assert_eq!(items["method"], "thread/items/list");
            server_write_result(
                &mut server_write,
                &items,
                json!({
                    "data": [{
                        "turnId": "turn-stale-active",
                        "item": {
                            "id": "agent-stale-active",
                            "type": "agentMessage",
                            "status": "completed",
                            "text": "completed in the loaded snapshot"
                        }
                    }],
                    "nextCursor": null
                }),
            )
            .await;
        });

        assert!(reconcile_bound_direct_turn(&rpc, &run, "turn-stale-active")
            .await
            .unwrap());
        assert!(run.terminal.load(Ordering::SeqCst));
        assert!(lock(&run.durable).iter().any(|event| {
            matches!(
                &event.payload,
                AgentEvent::DeliveryCommitted { delivery }
                    if delivery.final_response
                        == Content::text("completed in the loaded snapshot")
            )
        }));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn host_controller_accepts_codex_event_sequence() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_client(rpc, "codex/test"));
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-3"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-3", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            for notification in [
                json!({"method": "item/completed", "params": {"threadId": "thread-3", "turnId": "turn-3", "item": {"type": "agentMessage", "id": "a3", "text": "verified"}}}),
                json!({"method": "turn/completed", "params": {"threadId": "thread-3", "turn": {"id": "turn-3", "status": "completed", "items": []}}}),
            ] {
                server_write
                    .write_all(format!("{notification}\n").as_bytes())
                    .await
                    .unwrap();
            }
        });
        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("thread-3"),
            RunId::new("run-3"),
            vec![Content::text("verify")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        let view = tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Delivered);
        assert_eq!(
            view.delivery.unwrap().final_response,
            Content::text("verified")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn host_controller_accepts_confirmed_direct_cancellation_sequence() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_client(rpc, "codex/test"));
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-cancel"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": start["id"], "result": {"turn": {"id": "turn-cancel", "status": "inProgress", "items": []}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let preflight = next_request(&mut lines).await;
            assert_eq!(preflight["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &preflight,
                json!({
                    "data": [{"id": "turn-cancel", "status": "inProgress"}],
                    "nextCursor": null
                }),
            )
            .await;
            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/read");
            server_write_result(
                &mut server_write,
                &loaded,
                json!({"thread": {
                    "id": "thread-cancel",
                    "turns": [{"id": "turn-cancel", "status": "inProgress", "items": []}]
                }}),
            )
            .await;
            let interrupt = next_request(&mut lines).await;
            assert_eq!(interrupt["method"], "turn/interrupt");
            server_write
                .write_all(format!("{}\n", json!({"id": interrupt["id"], "result": {}})).as_bytes())
                .await
                .unwrap();
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"method": "turn/completed", "params": {"threadId": "thread-cancel", "turn": {"id": "turn-cancel", "status": "interrupted", "items": []}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("thread-cancel"),
            RunId::new("run-cancel"),
            vec![Content::text("cancel me")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        controller
            .cancel(&execution.run_id, "user stopped")
            .await
            .unwrap();
        let view = timeout(
            Duration::from_secs(1),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Cancelled);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn app_server_disconnect_becomes_unknown_continuity_not_a_false_failure() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_client(rpc, "codex/test"));
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-disconnect"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-disconnect", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            // EOF is deliberately ambiguous: the native turn may still have
            // reached Codex even though this app-server process disappeared.
        });
        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("thread-disconnect"),
            RunId::new("run-disconnect"),
            vec![Content::text("keep working")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        server.await.unwrap();

        let view = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let view = controller.inspect(&execution.run_id).await.unwrap();
                if view.state.status() == AgentRunStatus::Unknown {
                    break view;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Unknown);
        assert!(view.delivery.is_none());
        assert!(matches!(
            view.state,
            orchestral_core::agent_protocol::wire::AgentRunState::Unknown { ref reason, .. }
                if reason.contains("disconnected")
        ));
    }

    #[tokio::test]
    async fn failed_resume_does_not_poison_the_next_attempt_for_that_session() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let first_resume = next_request(&mut lines).await;
            assert_eq!(first_resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": first_resume["id"], "error": {"code": -32000, "message": "temporary load failure"}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let second_resume = next_request(&mut lines).await;
            assert_eq!(second_resume["method"], "thread/resume");
            assert_eq!(second_resume["params"]["threadId"], "thread-retry");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": second_resume["id"], "result": {"thread": {"id": "thread-retry"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-retry", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
            server_write
                .write_all(format!("{}\n", json!({"method": "turn/completed", "params": {"threadId": "thread-retry", "turn": {"id": "turn-retry", "status": "completed", "items": []}}})).as_bytes())
                .await
                .unwrap();
        });

        assert!(matches!(
            connector
                .start(start_request("thread-retry", "run-retry-1"))
                .await,
            Err(AgentStartError::Rejected(_))
        ));
        let mut stream = connector
            .start(start_request("thread-retry", "run-retry-2"))
            .await
            .unwrap()
            .stream;
        loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(
                item,
                AgentProviderStreamItem::Event(event)
                    if matches!(event.payload, AgentEvent::DeliveryCommitted { .. })
            ) {
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn loaded_unmaterialized_thread_can_receive_its_first_turn_after_reconnect() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/test");
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": resume["id"],
                            "error": {
                                "code": -32000,
                                "message": "no rollout found for thread id thread-empty-loaded"
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/loaded/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": loaded["id"],
                            "result": {"data": ["thread-empty-loaded"], "nextCursor": null}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let read = next_request(&mut lines).await;
            assert_eq!(read["method"], "thread/turns/list");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": read["id"],
                            "result": {"data": [], "nextCursor": null, "backwardsCursor": null}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();

            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            assert_eq!(start["params"]["threadId"], "thread-empty-loaded");
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": start["id"],
                            "result": {"turn": {"id": "turn-first", "status": "inProgress", "items": []}}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "method": "turn/completed",
                            "params": {
                                "threadId": "thread-empty-loaded",
                                "turn": {"id": "turn-first", "status": "completed", "items": []}
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut stream = connector
            .start(start_request("thread-empty-loaded", "run-first"))
            .await
            .unwrap()
            .stream;
        loop {
            let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(
                item,
                AgentProviderStreamItem::Event(event)
                    if matches!(event.payload, AgentEvent::DeliveryCommitted { .. })
            ) {
                break;
            }
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stale_active_index_starts_a_new_turn_instead_of_steering_a_completed_turn() {
        let (client_io, server_io) = duplex(1024 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let mut connector = CodexConnector::with_client(rpc, "codex/test");
        connector.config.allow_deferred_queue = false;
        let server = tokio::spawn(async move {
            let mut lines = BufReader::new(server_read).lines();
            let resume = next_request(&mut lines).await;
            server_write_error(
                &mut server_write,
                &resume,
                "thread thread-stale already has an active writer",
            )
            .await;

            let loaded = next_request(&mut lines).await;
            assert_eq!(loaded["method"], "thread/loaded/list");
            server_write_result(
                &mut server_write,
                &loaded,
                json!({"data": ["thread-stale"], "nextCursor": null}),
            )
            .await;

            let indexed = next_request(&mut lines).await;
            assert_eq!(indexed["method"], "thread/turns/list");
            server_write_result(
                &mut server_write,
                &indexed,
                json!({
                    "data": [{"id": "turn-old", "status": "inProgress", "items": []}],
                    "nextCursor": null
                }),
            )
            .await;

            let edge = next_request(&mut lines).await;
            assert_eq!(edge["method"], "thread/read");
            server_write_result(
                &mut server_write,
                &edge,
                json!({"thread": {
                    "id": "thread-stale",
                    "turns": [{"id": "turn-old", "status": "completed", "items": []}]
                }}),
            )
            .await;

            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            assert_eq!(start["params"]["threadId"], "thread-stale");
            server_write_result(
                &mut server_write,
                &start,
                json!({"turn": {"id": "turn-new", "status": "inProgress", "items": []}}),
            )
            .await;
            server_write
                .write_all(
                    format!(
                        "{}\n{}\n",
                        json!({
                            "method": "item/completed",
                            "params": {
                                "threadId": "thread-stale",
                                "turnId": "turn-new",
                                "item": {
                                    "id": "message-new",
                                    "type": "agentMessage",
                                    "text": "new turn completed"
                                }
                            }
                        }),
                        json!({
                            "method": "turn/completed",
                            "params": {
                                "threadId": "thread-stale",
                                "turn": {
                                    "id": "turn-new",
                                    "status": "completed",
                                    "items": [{
                                        "id": "message-new",
                                        "type": "agentMessage",
                                        "text": "new turn completed"
                                    }]
                                }
                            }
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let mut stream = connector
            .start(start_request("thread-stale", "run-after-stale"))
            .await
            .expect("a stale active index must not capture the new input")
            .stream;
        let mut delivery = None;
        while let Some(item) = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("new turn stream timed out")
        {
            if let AgentProviderStreamItem::Event(event) = item.unwrap() {
                if let AgentEvent::DeliveryCommitted { delivery: value } = event.payload {
                    delivery = Some(value);
                    break;
                }
            }
        }
        assert_eq!(
            delivery.expect("new turn must complete").final_response,
            Content::text("new turn completed")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reconnect_reads_authoritative_turn_without_starting_duplicate_work() {
        let (first_client_io, first_server_io) = duplex(1024 * 1024);
        let (first_client_read, first_client_write) = tokio::io::split(first_client_io);
        let (first_server_read, mut first_server_write) = tokio::io::split(first_server_io);
        let first_rpc = CodexRpcClient::from_io(
            first_client_read,
            first_client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let (second_client_io, second_server_io) = duplex(1024 * 1024);
        let (second_client_read, second_client_write) = tokio::io::split(second_client_io);
        let (second_server_read, mut second_server_write) = tokio::io::split(second_server_io);
        let second_rpc = CodexRpcClient::from_io(
            second_client_read,
            second_client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = Arc::new(CodexConnector::with_reconnect_client(first_rpc, second_rpc));
        let first_server = tokio::spawn(async move {
            let mut lines = BufReader::new(first_server_read).lines();
            let resume = next_request(&mut lines).await;
            first_server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-recover"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let start = next_request(&mut lines).await;
            assert_eq!(start["method"], "turn/start");
            first_server_write
                .write_all(format!("{}\n", json!({"id": start["id"], "result": {"turn": {"id": "turn-recover", "status": "inProgress", "items": []}}})).as_bytes())
                .await
                .unwrap();
        });
        let second_server = tokio::spawn(async move {
            let mut lines = BufReader::new(second_server_read).lines();
            let resume = next_request(&mut lines).await;
            assert_eq!(resume["method"], "thread/resume");
            second_server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({"id": resume["id"], "result": {"thread": {"id": "thread-recover"}}})
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let read = next_request(&mut lines).await;
            assert_eq!(read["method"], "thread/turns/list");
            second_server_write
                .write_all(
                    format!(
                        "{}\n",
                        json!({
                            "id": read["id"],
                            "result": {"data": [{
                                    "id": "turn-recover",
                                    "status": "completed",
                                    "items": [{
                                        "type": "agentMessage",
                                        "id": "answer-recover",
                                        "text": "recovered answer"
                                    }]
                                }], "nextCursor": null, "backwardsCursor": null}
                        })
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });

        let provider: Arc<dyn AgentProvider> = connector;
        let controller = Arc::new(
            AgentController::new(provider, ProviderBindingRef::new("codex/local")).unwrap(),
        );
        let run = AgentRunEnvelope::new(
            ProtocolVersion::new(1, 0),
            AgentSessionId::new("thread-recover"),
            RunId::new("run-recover"),
            vec![Content::text("perform once")],
        )
        .unwrap();
        let execution = controller.start(run).await.unwrap();
        first_server.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if controller
                    .inspect(&execution.run_id)
                    .await
                    .unwrap()
                    .state
                    .status()
                    == AgentRunStatus::Unknown
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        controller.recover(&execution.run_id).await.unwrap();
        let view = tokio::time::timeout(
            Duration::from_secs(1),
            controller.wait_for_terminal(&execution.run_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(view.state.status(), AgentRunStatus::Delivered);
        assert_eq!(
            view.delivery.unwrap().final_response,
            Content::text("recovered answer")
        );
        second_server.await.unwrap();
    }
}
