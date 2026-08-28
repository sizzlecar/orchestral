//! Guarded, provider-neutral Tool execution boundary.
//!
//! An executor must opt in to the Host-owned effective policy
//! and cancellation contract by implementing [`GuardedToolExecutor`].

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex as StdMutex, RwLock, Weak};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::StreamExt;
use orchestral_core::agent_protocol::wire::{ArtifactRef, ArtifactRefWithDigest, Digest, RunId};
use orchestral_core::io::{BlobId, BlobIoError, BlobStore, BlobWriteRequest};
use orchestral_core::spi::{HookRegistry, RuntimeHookContext, RuntimeHookEventEnvelope, SpiMeta};
use orchestral_core::tool_effect::{
    replay_tool_effect, InMemoryToolEffectJournalStore, PreparedToolEffect,
    ToolAuthorizationEvidence, ToolEffectAttemptId, ToolEffectError, ToolEffectEvent,
    ToolEffectEventDraft, ToolEffectEventId, ToolEffectJournalStore, ToolEffectKey,
    ToolEffectPhase, ToolEffectProjection,
};
use orchestral_core::tool_protocol::{
    ApprovalBinding, ApprovalCapability, ApprovalCapabilityStore, ApprovalPolicy, EffectScope,
    EffectiveToolPolicy, HostApprovalVerifier, HostToolPolicy, ModelToolSchema, RunToolGrant,
    ToolArtifact, ToolCallId, ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency,
    ToolInvocation, ToolOperationPlan, ToolOperationRisk, ToolOutcome, ToolOutput,
    ToolProtocolError, ToolProtocolErrorCode, VerifiedApprovalCapability,
};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

/// The only context passed to a production Tool executor.
///
/// Policy and cancellation are Host-derived. `approval` is a non-serializable
/// proof produced by the Host verifier, never a model-provided boolean.
#[derive(Debug, Clone)]
pub struct GuardedToolExecution {
    pub invocation: ToolInvocation,
    /// Host-inspected, invocation-specific operation. Executors must stay
    /// within this plan as well as the effective authority ceiling.
    pub operation: ToolOperationPlan,
    pub effective_policy: EffectiveToolPolicy,
    pub approval: Option<VerifiedApprovalCapability>,
    pub cancellation: CancellationToken,
}

/// Explicit opt-in SPI for implementations that enforce Host Tool policy.
#[async_trait]
pub trait GuardedToolExecutor: Send + Sync {
    /// Stable identity of the pre-execution planner implemented by this Tool.
    /// It becomes part of the runtime execution contract used by recovery.
    fn planning_contract(&self) -> serde_json::Value {
        serde_json::json!({
            "contract": "orchestral.tool-operation-planner/static-envelope/v1"
        })
    }

    /// Inspects one invocation without producing an externally observable
    /// effect. The default is conservative: it requests the Tool's entire
    /// registered effect envelope. Built-ins should narrow this plan whenever
    /// their arguments provide stronger information.
    fn plan_operation(
        &self,
        invocation: &ToolInvocation,
        descriptor: &ToolDescriptor,
        _effective_policy: &EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        Ok(ToolOperationPlan {
            effect_scopes: descriptor.effect_scopes.clone(),
            targets: Default::default(),
            risk: ToolOperationRisk::Routine,
            summary: sanitize_approval_summary(
                &self.approval_summary(invocation),
                &invocation.tool_id,
            ),
        })
    }

    /// Host-owned, human-facing description for an approval prompt. It is not
    /// authority: the signed [`ApprovalBinding`] remains the exact operation.
    /// Implementations should redact credential-bearing fields.
    fn approval_summary(&self, invocation: &ToolInvocation) -> String {
        let args_digest = invocation
            .args_digest()
            .map(|digest| digest.to_string())
            .unwrap_or_else(|_| "invalid-arguments".to_owned());
        format!(
            "Invoke Tool {} with arguments {}",
            invocation.tool_id.as_str(),
            args_digest
        )
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome;
}

/// Host decision for one already-inspected Tool operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ToolPermissionDecision {
    Allow,
    RequireApproval,
    Deny { code: String, message: String },
}

/// Policy SPI kept separate from Tool planning and capability issuance.
/// Implementations decide; only the Host approval broker can issue an exact
/// capability for a reviewed operation.
pub trait ToolPermissionPolicy: Send + Sync {
    fn contract_digest(&self) -> Digest;

    fn decide(
        &self,
        descriptor: &ToolDescriptor,
        operation: &ToolOperationPlan,
        effective_policy: &EffectiveToolPolicy,
    ) -> ToolPermissionDecision;
}

/// Compatibility policy used by SDK-created runtimes: the composed static
/// approval bound remains authoritative.
#[derive(Debug, Default)]
pub struct DescriptorPermissionPolicy;

impl ToolPermissionPolicy for DescriptorPermissionPolicy {
    fn contract_digest(&self) -> Digest {
        Digest::sha256("orchestral.permission-policy/descriptor/v1")
    }

    fn decide(
        &self,
        _descriptor: &ToolDescriptor,
        _operation: &ToolOperationPlan,
        effective_policy: &EffectiveToolPolicy,
    ) -> ToolPermissionDecision {
        match effective_policy.bounds().approval {
            ApprovalPolicy::NotRequired => ToolPermissionDecision::Allow,
            ApprovalPolicy::Required => ToolPermissionDecision::RequireApproval,
            ApprovalPolicy::Deny => ToolPermissionDecision::Deny {
                code: "approval_policy_denied".to_owned(),
                message: "effective Host policy denies this Tool operation".to_owned(),
            },
            _ => ToolPermissionDecision::Deny {
                code: "approval_policy_unknown".to_owned(),
                message: "effective Host policy contains an unsupported approval mode".to_owned(),
            },
        }
    }
}

/// Default interactive workspace policy used by the CLI.
///
/// Routine work stays inside the mandatory sandbox and runs automatically.
/// Destructive operations, open-world effects, secrets, and any Tool that
/// statically requires approval still route to the reviewer.
#[derive(Debug, Default)]
pub struct WorkspacePermissionPolicy;

impl ToolPermissionPolicy for WorkspacePermissionPolicy {
    fn contract_digest(&self) -> Digest {
        Digest::sha256("orchestral.permission-policy/workspace/v1")
    }

    fn decide(
        &self,
        _descriptor: &ToolDescriptor,
        operation: &ToolOperationPlan,
        effective_policy: &EffectiveToolPolicy,
    ) -> ToolPermissionDecision {
        let bounds = effective_policy.bounds();
        if bounds.approval == ApprovalPolicy::Deny {
            return ToolPermissionDecision::Deny {
                code: "approval_policy_denied".to_owned(),
                message: "effective Host policy denies this Tool operation".to_owned(),
            };
        }
        if bounds.approval == ApprovalPolicy::Required
            || operation.risk != ToolOperationRisk::Routine
            || operation.effect_scopes.iter().any(|scope| {
                matches!(
                    scope,
                    EffectScope::Network
                        | EffectScope::SecretRead
                        | EffectScope::ExternalSideEffect
                )
            })
            || (!bounds.sandbox.required
                && operation.effect_scopes.iter().any(|scope| {
                    matches!(scope, EffectScope::Process | EffectScope::FilesystemWrite)
                }))
        {
            ToolPermissionDecision::RequireApproval
        } else {
            ToolPermissionDecision::Allow
        }
    }
}

/// The pluggable policy may only tighten the statically composed Host bound.
/// `Required` and `Deny` are ceilings, never suggestions that an application
/// policy can relax.
fn constrain_permission_decision(
    effective_policy: &EffectiveToolPolicy,
    proposed: ToolPermissionDecision,
) -> ToolPermissionDecision {
    match effective_policy.bounds().approval {
        ApprovalPolicy::Deny => ToolPermissionDecision::Deny {
            code: "approval_policy_denied".to_owned(),
            message: "effective Host policy denies this Tool operation".to_owned(),
        },
        ApprovalPolicy::Required => match proposed {
            ToolPermissionDecision::Deny { code, message } => {
                ToolPermissionDecision::Deny { code, message }
            }
            ToolPermissionDecision::Allow | ToolPermissionDecision::RequireApproval => {
                ToolPermissionDecision::RequireApproval
            }
        },
        ApprovalPolicy::NotRequired => proposed,
        _ => ToolPermissionDecision::Deny {
            code: "approval_policy_unknown".to_owned(),
            message: "effective Host policy contains an unsupported approval mode".to_owned(),
        },
    }
}

/// Produces the durable identity of one normalized permission decision.
///
/// Journal builders and recovery adapters use the same function so a change
/// from reviewed to automatic execution (or the reverse) is detected before
/// an executor can run.
pub fn tool_permission_decision_digest(
    policy: &dyn ToolPermissionPolicy,
    decision: &ToolPermissionDecision,
) -> Result<Digest, ToolProtocolError> {
    let decision = match decision {
        ToolPermissionDecision::Allow => serde_json::json!({ "kind": "allow" }),
        ToolPermissionDecision::RequireApproval => {
            serde_json::json!({ "kind": "require_approval" })
        }
        ToolPermissionDecision::Deny { code, message } => serde_json::json!({
            "kind": "deny",
            "code": code,
            "message": message,
        }),
    };
    let binding = serde_json::json!({
        "contract": "orchestral.tool-permission-decision/v1",
        "policy_contract_digest": policy.contract_digest(),
        "decision": decision,
    });
    let bytes = serde_jcs::to_vec(&binding).map_err(|error| {
        ToolProtocolError::new(
            ToolProtocolErrorCode::InvalidInvocation,
            format!("canonicalize Tool permission decision failed: {error}"),
        )
    })?;
    Ok(Digest::sha256(bytes))
}

/// Object-safe surface consumed by an Agent loop. Concrete approval stores and
/// reference-monitor state stay behind this Host-owned boundary.
#[async_trait]
pub trait AgentToolRuntime: Send + Sync {
    /// Stable identity of the Host-side execution contract used to decide
    /// whether a private Agent checkpoint may continue after restart.
    ///
    /// Implementations must cover authority ceilings, registered Tool
    /// descriptors, and other durable policy that can change whether an
    /// invocation is accepted or how its result is represented. Credentials,
    /// live ledgers, and other ephemeral state must not enter this digest.
    fn execution_contract_digest(&self) -> Result<Digest, ToolRuntimeError>;

    fn model_tool_schemas(&self) -> Result<Vec<ModelToolSchema>, ToolRuntimeError>;

    fn resolve_tool_id(&self, model_name: &str) -> Result<Option<ToolId>, ToolRuntimeError>;

    /// Reads one durable effect projection without changing its phase.
    /// Workflow recovery uses this to reject an entire replay before any new
    /// sibling Tool is dispatched when one prior invocation is unresolved.
    async fn inspect_effect(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Option<ToolEffectProjection>, ToolOutcomeRecoveryError>;

    /// Recovers an already-started invocation from the durable Effect Journal
    /// without ever calling its executor or creating a fresh effect record.
    /// `Ok(None)` means no durable outcome exists. Callers must establish
    /// exclusive recovery ownership before using this operation.
    async fn recover_outcome(
        &self,
        invocation: ToolInvocation,
        run_grant: RunToolGrant,
    ) -> Result<Option<ToolOutcome>, ToolOutcomeRecoveryError>;

    async fn invoke(
        &self,
        invocation: ToolInvocation,
        run_grant: RunToolGrant,
        approval: Option<ApprovalCapability>,
        run_cancellation: CancellationToken,
    ) -> GuardedToolResult;
}

/// Structured result returned to the Agent loop.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum GuardedToolResult {
    /// No executor was called. The Host may issue a capability for this exact
    /// binding and retry the same `(run_id, call_id)`.
    ApprovalRequired {
        binding: ApprovalBinding,
        summary: String,
    },
    /// Semantic Tool result. `cached=true` means this call joined or replayed
    /// an invocation that another caller already executed.
    Outcome { outcome: ToolOutcome, cached: bool },
}

/// Structured failure from replay-only Tool outcome recovery. This is kept
/// separate from a semantic [`ToolOutcome`] so callers cannot confuse a
/// recovery-contract violation with a result produced by the Tool.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Tool outcome recovery failed ({code}): {message}")]
pub struct ToolOutcomeRecoveryError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ToolRuntimeError {
    #[error("invalid Host Tool policy: {0}")]
    InvalidHostPolicy(#[source] ToolProtocolError),
    #[error("invalid Tool descriptor: {0}")]
    InvalidDescriptor(#[source] ToolProtocolError),
    #[error("tool id is already registered: {0}")]
    DuplicateToolId(ToolId),
    #[error("model tool name is already registered: {0}")]
    DuplicateModelName(String),
    #[error("Tool Runtime execution contract cannot be encoded: {0}")]
    InvalidExecutionContract(String),
    #[error("Tool Runtime state is unavailable")]
    StateUnavailable,
}

/// Host-owned Artifact service used by the Tool Runtime for large results.
///
/// The byte ceiling is independent from `max_output_bytes`: the latter is the
/// maximum inline context payload, while this is the hard storage ceiling.
#[derive(Clone)]
pub struct ToolArtifactStore {
    store: Arc<dyn BlobStore>,
    max_artifact_bytes: u64,
    summary_max_chars: usize,
    hooks: Option<Arc<HookRegistry>>,
}

impl ToolArtifactStore {
    pub fn new(
        store: Arc<dyn BlobStore>,
        max_artifact_bytes: u64,
        summary_max_chars: usize,
    ) -> Result<Self, ToolArtifactError> {
        if max_artifact_bytes == 0 || summary_max_chars == 0 {
            return Err(ToolArtifactError::InvalidConfig(
                "artifact byte and summary limits must be positive".to_owned(),
            ));
        }
        Ok(Self {
            store,
            max_artifact_bytes,
            summary_max_chars,
            hooks: None,
        })
    }

    /// Attaches the Host runtime hook registry to artifact lifecycle events.
    /// The registry's failure policy controls whether a hook rejection is
    /// observational (`FailOpen`) or aborts the artifact operation
    /// (`FailClosed`).
    pub fn with_hooks(mut self, hooks: Arc<HookRegistry>) -> Self {
        self.hooks = Some(hooks);
        self
    }

    pub fn max_artifact_bytes(&self) -> u64 {
        self.max_artifact_bytes
    }

    /// Resolves and verifies an immutable Artifact reference. A store cannot
    /// make corrupt or substituted bytes valid merely by returning metadata.
    pub async fn resolve(&self, artifact: &ToolArtifact) -> Result<Vec<u8>, ToolArtifactError> {
        artifact
            .validate()
            .map_err(|error| ToolArtifactError::Integrity(error.message))?;
        if artifact.byte_size > self.max_artifact_bytes {
            return Err(ToolArtifactError::LimitExceeded {
                observed: artifact.byte_size,
                maximum: self.max_artifact_bytes,
            });
        }
        let blob_id = BlobId::new(artifact.artifact.artifact_ref.as_str());
        let mut read = self.store.read(&blob_id).await?;
        if read.meta.id != blob_id
            || read.meta.byte_size != artifact.byte_size
            || read.meta.mime_type.as_deref() != Some(artifact.media_type.as_str())
        {
            return Err(ToolArtifactError::Integrity(
                "artifact metadata does not match its durable reference".to_owned(),
            ));
        }
        if let Some(checksum) = &read.meta.checksum_sha256 {
            if checksum != artifact.artifact.digest.as_str() {
                return Err(ToolArtifactError::Integrity(
                    "artifact store checksum does not match its durable digest".to_owned(),
                ));
            }
        }
        let mut bytes = Vec::with_capacity(usize::try_from(artifact.byte_size).unwrap_or(0));
        while let Some(chunk) = read.body.next().await {
            let chunk = chunk?;
            let next_size = bytes.len().saturating_add(chunk.len()) as u64;
            if next_size > artifact.byte_size || next_size > self.max_artifact_bytes {
                return Err(ToolArtifactError::Integrity(
                    "artifact body exceeded its declared size".to_owned(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != artifact.byte_size
            || Digest::sha256(&bytes) != artifact.artifact.digest
        {
            return Err(ToolArtifactError::Integrity(
                "artifact bytes do not match their declared size and digest".to_owned(),
            ));
        }
        Ok(bytes)
    }

    async fn spill(
        &self,
        invocation: &ToolInvocation,
        bytes: Vec<u8>,
        summary: String,
        cancellation: &CancellationToken,
    ) -> Result<ToolArtifact, ToolArtifactError> {
        let byte_size = bytes.len() as u64;
        let digest = Digest::sha256(&bytes);
        let lifecycle_payload = serde_json::json!({
            "protocol": "orchestral/tool-artifact/v1",
            "run_id": invocation.run_id.as_str(),
            "call_id": invocation.call_id.as_str(),
            "tool_id": invocation.tool_id.as_str(),
            "media_type": "application/json",
            "byte_size": byte_size,
            "digest": digest.as_str(),
        });
        if let Err(error) = self
            .dispatch_artifact_hook("artifact.put", invocation, lifecycle_payload.clone())
            .await
        {
            return Err(self
                .report_artifact_failure(invocation, lifecycle_payload, error)
                .await);
        }

        let result = if byte_size > self.max_artifact_bytes {
            Err(ToolArtifactError::LimitExceeded {
                observed: byte_size,
                maximum: self.max_artifact_bytes,
            })
        } else if cancellation.is_cancelled() {
            Err(ToolArtifactError::Cancelled)
        } else {
            self.write_artifact(invocation, bytes, byte_size, digest, summary, cancellation)
                .await
        };
        match result {
            Ok(artifact) => {
                let mut payload = lifecycle_payload;
                payload["artifact_ref"] =
                    serde_json::Value::String(artifact.artifact.artifact_ref.to_string());
                if let Err(error) = self
                    .dispatch_artifact_hook("artifact.commit", invocation, payload.clone())
                    .await
                {
                    return Err(self
                        .report_artifact_failure(invocation, payload, error)
                        .await);
                }
                Ok(artifact)
            }
            Err(error) => Err(self
                .report_artifact_failure(invocation, lifecycle_payload, error)
                .await),
        }
    }

    async fn report_artifact_failure(
        &self,
        invocation: &ToolInvocation,
        mut payload: serde_json::Value,
        error: ToolArtifactError,
    ) -> ToolArtifactError {
        payload["error"] = serde_json::Value::String(error.to_string());
        match self
            .dispatch_artifact_hook("artifact.fail", invocation, payload)
            .await
        {
            Ok(()) => error,
            Err(fail_error) => ToolArtifactError::HookRejected {
                event_type: "artifact.fail".to_owned(),
                message: format!("{fail_error}; original error: {error}"),
            },
        }
    }

    async fn write_artifact(
        &self,
        invocation: &ToolInvocation,
        bytes: Vec<u8>,
        byte_size: u64,
        digest: Digest,
        summary: String,
        cancellation: &CancellationToken,
    ) -> Result<ToolArtifact, ToolArtifactError> {
        let body = Box::pin(futures_util::stream::once(
            async move { Ok(Bytes::from(bytes)) },
        ));
        let request = BlobWriteRequest::new(body)
            .with_file_name(Some(format!(
                "tool-{}-{}.json",
                invocation.run_id.as_str(),
                invocation.call_id.as_str()
            )))
            .with_mime_type(Some("application/json".to_owned()))
            .with_metadata(serde_json::json!({
                "protocol": "orchestral/tool-artifact/v1",
                "run_id": invocation.run_id.as_str(),
                "call_id": invocation.call_id.as_str(),
                "tool_id": invocation.tool_id.as_str(),
                "sha256": digest.as_str(),
            }));
        let write = self.store.write(request);
        tokio::pin!(write);
        let meta = tokio::select! {
            _ = cancellation.cancelled() => return Err(ToolArtifactError::Cancelled),
            result = &mut write => result?,
        };
        if meta.id.as_str().trim().is_empty()
            || meta.byte_size != byte_size
            || meta.mime_type.as_deref() != Some("application/json")
            || meta
                .checksum_sha256
                .as_ref()
                .is_some_and(|checksum| checksum != digest.as_str())
        {
            return Err(ToolArtifactError::Integrity(
                "artifact store returned metadata inconsistent with the written bytes".to_owned(),
            ));
        }
        let artifact = ToolArtifact {
            artifact: ArtifactRefWithDigest {
                artifact_ref: ArtifactRef::new(meta.id.as_str()),
                digest,
            },
            media_type: "application/json".to_owned(),
            byte_size,
            summary,
        };
        artifact
            .validate()
            .map_err(|error| ToolArtifactError::Integrity(error.message))?;
        Ok(artifact)
    }

    async fn dispatch_artifact_hook(
        &self,
        event_type: &str,
        invocation: &ToolInvocation,
        payload: serde_json::Value,
    ) -> Result<(), ToolArtifactError> {
        let Some(hooks) = &self.hooks else {
            return Ok(());
        };
        let event = RuntimeHookEventEnvelope {
            meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
            event_type: event_type.to_owned(),
            event_version: "1.0.0".to_owned(),
            occurred_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            payload,
            extensions: serde_json::Map::new(),
        };
        let context = RuntimeHookContext {
            session_id: None,
            run_id: Some(invocation.run_id.clone()),
            workflow_id: None,
            step_id: None,
            tool_name: Some(invocation.tool_id.to_string()),
            message: None,
            metadata: serde_json::json!({
                "run_id": invocation.run_id.as_str(),
                "call_id": invocation.call_id.as_str(),
            }),
            extensions: serde_json::Map::new(),
        };
        hooks
            .dispatch_checked(&event, &context)
            .await
            .map_err(|error| ToolArtifactError::HookRejected {
                event_type: event_type.to_owned(),
                message: error.to_string(),
            })
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ToolArtifactError {
    #[error("invalid Tool Artifact configuration: {0}")]
    InvalidConfig(String),
    #[error("artifact size {observed} exceeds the Host ceiling {maximum}")]
    LimitExceeded { observed: u64, maximum: u64 },
    #[error("artifact storage failed: {0}")]
    Store(#[from] BlobIoError),
    #[error("artifact integrity check failed: {0}")]
    Integrity(String),
    #[error("artifact persistence was cancelled")]
    Cancelled,
    #[error("artifact lifecycle hook rejected {event_type}: {message}")]
    HookRejected { event_type: String, message: String },
}

struct RegisteredTool {
    descriptor: ToolDescriptor,
    executor: Arc<dyn GuardedToolExecutor>,
    global_gate: Arc<AsyncMutex<()>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InvocationIdentity {
    tool_id: ToolId,
    args_digest: Digest,
    operation_digest: Digest,
    permission_digest: Digest,
    policy_digest: Digest,
    descriptor_digest: Digest,
}

struct InvocationEntry {
    identity: InvocationIdentity,
    state: AsyncMutex<InvocationState>,
    changed: Notify,
}

enum InvocationState {
    Ready,
    Running,
    Completed(ToolOutcome),
}

enum DurableInvocationStart {
    Execute {
        verified_approval: Option<VerifiedApprovalCapability>,
    },
    Replay {
        outcome: ToolOutcome,
    },
}

struct PlannedInvocation {
    operation: ToolOperationPlan,
    effective_policy: EffectiveToolPolicy,
    permission: ToolPermissionDecision,
    permission_digest: Digest,
    approval_binding: ApprovalBinding,
}

type InvocationKey = (RunId, ToolCallId);
type PerRunGateKey = (ToolId, RunId);

/// In-process Host reference monitor and execution gate.
///
/// The policy ceiling, registry, call ledger, and approval verifier are all
/// Host-owned. Callers can grant less authority per Run but cannot replace the
/// ceiling or a registered descriptor.
pub struct GuardedToolRuntime<S> {
    host_ceiling: HostToolPolicy,
    permission_policy: Arc<dyn ToolPermissionPolicy>,
    approval_verifier: HostApprovalVerifier<S>,
    effect_journal: Arc<dyn ToolEffectJournalStore>,
    artifact_store: Option<ToolArtifactStore>,
    registry: RwLock<BTreeMap<ToolId, Arc<RegisteredTool>>>,
    invocations: StdMutex<BTreeMap<InvocationKey, Arc<InvocationEntry>>>,
    per_run_gates: StdMutex<BTreeMap<PerRunGateKey, Weak<AsyncMutex<()>>>>,
}

impl<S: ApprovalCapabilityStore> GuardedToolRuntime<S> {
    pub fn new(
        host_ceiling: HostToolPolicy,
        approval_verifier: HostApprovalVerifier<S>,
    ) -> Result<Self, ToolRuntimeError> {
        Self::new_with_effect_journal(
            host_ceiling,
            approval_verifier,
            Arc::new(InMemoryToolEffectJournalStore::default()),
        )
    }

    pub fn new_with_effect_journal(
        host_ceiling: HostToolPolicy,
        approval_verifier: HostApprovalVerifier<S>,
        effect_journal: Arc<dyn ToolEffectJournalStore>,
    ) -> Result<Self, ToolRuntimeError> {
        Self::new_with_services(host_ceiling, approval_verifier, effect_journal, None)
    }

    pub fn new_with_effect_journal_and_artifacts(
        host_ceiling: HostToolPolicy,
        approval_verifier: HostApprovalVerifier<S>,
        effect_journal: Arc<dyn ToolEffectJournalStore>,
        artifact_store: ToolArtifactStore,
    ) -> Result<Self, ToolRuntimeError> {
        Self::new_with_services(
            host_ceiling,
            approval_verifier,
            effect_journal,
            Some(artifact_store),
        )
    }

    fn new_with_services(
        host_ceiling: HostToolPolicy,
        approval_verifier: HostApprovalVerifier<S>,
        effect_journal: Arc<dyn ToolEffectJournalStore>,
        artifact_store: Option<ToolArtifactStore>,
    ) -> Result<Self, ToolRuntimeError> {
        host_ceiling
            .bounds
            .validate()
            .map_err(ToolRuntimeError::InvalidHostPolicy)?;
        Ok(Self {
            host_ceiling,
            permission_policy: Arc::new(DescriptorPermissionPolicy),
            approval_verifier,
            effect_journal,
            artifact_store,
            registry: RwLock::new(BTreeMap::new()),
            invocations: StdMutex::new(BTreeMap::new()),
            per_run_gates: StdMutex::new(BTreeMap::new()),
        })
    }

    /// Replaces the immutable invocation permission policy before the runtime
    /// is shared or registered with an Agent composition root.
    pub fn with_permission_policy(mut self, policy: Arc<dyn ToolPermissionPolicy>) -> Self {
        self.permission_policy = policy;
        self
    }

    /// Registers an immutable descriptor and policy-aware executor.
    pub fn register(
        &self,
        descriptor: ToolDescriptor,
        executor: Arc<dyn GuardedToolExecutor>,
    ) -> Result<(), ToolRuntimeError> {
        descriptor
            .validate()
            .map_err(ToolRuntimeError::InvalidDescriptor)?;
        let mut registry = self
            .registry
            .write()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?;
        if registry.contains_key(&descriptor.tool_id) {
            return Err(ToolRuntimeError::DuplicateToolId(
                descriptor.tool_id.clone(),
            ));
        }
        if registry.values().any(|registered| {
            registered.descriptor.model_schema.name == descriptor.model_schema.name
        }) {
            return Err(ToolRuntimeError::DuplicateModelName(
                descriptor.model_schema.name.clone(),
            ));
        }
        registry.insert(
            descriptor.tool_id.clone(),
            Arc::new(RegisteredTool {
                descriptor,
                executor,
                global_gate: Arc::new(AsyncMutex::new(())),
            }),
        );
        Ok(())
    }

    /// Digests only the declared execution boundary. Executor pointers,
    /// approval signing material, and mutable invocation state are
    /// intentionally excluded.
    pub fn execution_contract_digest(&self) -> Result<Digest, ToolRuntimeError> {
        let registry = self
            .registry
            .read()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?;
        let registrations = registry
            .values()
            .map(|registered| {
                serde_json::json!({
                    "descriptor": &registered.descriptor,
                    "planning_contract": registered.executor.planning_contract(),
                })
            })
            .collect::<Vec<_>>();
        let artifact_contract = self.artifact_store.as_ref().map(|store| {
            serde_json::json!({
                "max_artifact_bytes": store.max_artifact_bytes,
                "summary_max_chars": store.summary_max_chars,
                "hooks_enabled": store.hooks.is_some(),
            })
        });
        let contract = serde_json::json!({
            "contract": "orchestral.guarded-tool-runtime/v1",
            "host_ceiling": &self.host_ceiling,
            "permission_policy": self.permission_policy.contract_digest(),
            "registered_tools": registrations,
            "artifact_store": artifact_contract,
        });
        let bytes = serde_jcs::to_vec(&contract)
            .map_err(|error| ToolRuntimeError::InvalidExecutionContract(error.to_string()))?;
        Ok(Digest::sha256(bytes))
    }

    /// Projects only the model-facing schema. Host policy, effect declarations,
    /// approval state, and executor details cannot enter this return type.
    pub fn model_tool_schemas(&self) -> Result<Vec<ModelToolSchema>, ToolRuntimeError> {
        let registry = self
            .registry
            .read()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?;
        Ok(registry
            .values()
            .map(|registered| registered.descriptor.model_schema().clone())
            .collect())
    }

    pub fn resolve_tool_id(&self, model_name: &str) -> Result<Option<ToolId>, ToolRuntimeError> {
        let registry = self
            .registry
            .read()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?;
        Ok(registry
            .values()
            .find(|registered| registered.descriptor.model_schema.name == model_name)
            .map(|registered| registered.descriptor.tool_id.clone()))
    }

    /// Replays only durable Tool state. This path can close an Observed result
    /// or classify an orphaned Invoked effect as unknown, but it never creates
    /// Prepared/Invoked records and never enters an executor.
    pub async fn recover_outcome(
        &self,
        invocation: ToolInvocation,
        run_grant: RunToolGrant,
    ) -> Result<Option<ToolOutcome>, ToolOutcomeRecoveryError> {
        if let Err(error) = invocation.validate() {
            return Err(tool_outcome_recovery_error(
                "invalid_invocation",
                error.message,
            ));
        }
        let registered = match self.registered_tool(&invocation.tool_id) {
            Ok(Some(registered)) => registered,
            Ok(None) => {
                return Err(tool_outcome_recovery_error(
                    "tool_not_found",
                    format!("tool is not registered: {}", invocation.tool_id),
                ))
            }
            Err(error) => {
                return Err(tool_outcome_recovery_error(
                    "runtime_unavailable",
                    error.to_string(),
                ))
            }
        };
        if let Err(error) = registered
            .descriptor
            .model_schema
            .validate_arguments(&invocation.arguments)
        {
            return Err(tool_outcome_recovery_error(
                "input_schema_violation",
                error.message,
            ));
        }
        let effective_policy = EffectiveToolPolicy::resolve(
            &self.host_ceiling,
            &run_grant,
            &registered.descriptor.restriction,
        )
        .map_err(|error| tool_outcome_recovery_error("invalid_effective_policy", error.message))?;
        let operation = registered
            .executor
            .plan_operation(&invocation, &registered.descriptor, &effective_policy)
            .map_err(|outcome| {
                tool_outcome_recovery_error(
                    "operation_planning_failed",
                    format!("Tool operation planning failed: {outcome:?}"),
                )
            })?;
        operation
            .validate_envelope(&registered.descriptor.effect_scopes)
            .map_err(|error| {
                tool_outcome_recovery_error("invalid_operation_plan", error.message)
            })?;
        if !effective_policy.authorizes_scopes(&operation.effect_scopes) {
            return Err(tool_outcome_recovery_error(
                "policy_denied",
                "tool effects are outside the effective Host policy",
            ));
        }
        let permission = constrain_permission_decision(
            &effective_policy,
            self.permission_policy
                .decide(&registered.descriptor, &operation, &effective_policy),
        );
        let permission_digest =
            tool_permission_decision_digest(self.permission_policy.as_ref(), &permission).map_err(
                |error| tool_outcome_recovery_error("invalid_permission_decision", error.message),
            )?;
        let prepared = PreparedToolEffect {
            invocation: invocation.clone(),
            args_digest: invocation.args_digest().map_err(|error| {
                tool_outcome_recovery_error("invalid_invocation", error.message)
            })?,
            operation_digest: operation.digest().map_err(|error| {
                tool_outcome_recovery_error("invalid_operation_plan", error.message)
            })?,
            permission_digest,
            policy_digest: effective_policy.digest().map_err(|error| {
                tool_outcome_recovery_error("invalid_effective_policy", error.message)
            })?,
            descriptor_digest: registered.descriptor.digest().map_err(|error| {
                tool_outcome_recovery_error("invalid_descriptor", error.message)
            })?,
            idempotency: registered.descriptor.idempotency,
            effect_scopes: operation.effect_scopes.clone(),
        };
        let key = prepared.key();

        for _ in 0..4 {
            let records = self
                .effect_journal
                .load_effect(&key)
                .await
                .map_err(effect_journal_recovery_error)?;
            let Some(projection) =
                replay_tool_effect(&key, &records).map_err(effect_journal_recovery_error)?
            else {
                return Ok(None);
            };
            if projection.prepared != prepared {
                return Err(tool_outcome_recovery_error(
                    "call_identity_conflict",
                    "durable Tool effect identity differs for the same run_id/call_id",
                ));
            }
            match projection.phase {
                ToolEffectPhase::Prepared => return Ok(None),
                ToolEffectPhase::Observed { outcome, .. } => {
                    let outcome_digest = outcome.digest().map_err(|error| {
                        tool_outcome_recovery_error("invalid_tool_outcome", error.message)
                    })?;
                    match self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(&key, "committed"),
                                key: key.clone(),
                                payload: ToolEffectEvent::Committed { outcome_digest },
                            },
                        )
                        .await
                    {
                        Ok(_) | Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => return Err(effect_journal_recovery_error(error)),
                    }
                }
                ToolEffectPhase::Committed { outcome, .. } => return Ok(Some(outcome)),
                ToolEffectPhase::Invoked { .. } => {
                    let reason = "durable invocation has no observation after runtime recovery";
                    match self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(&key, "unknown"),
                                key: key.clone(),
                                payload: ToolEffectEvent::EffectUnknown {
                                    reason: reason.to_owned(),
                                },
                            },
                        )
                        .await
                    {
                        Ok(_) | Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => return Err(effect_journal_recovery_error(error)),
                    }
                }
                ToolEffectPhase::UnknownEffect { reason, .. } => {
                    return Ok(Some(unknown_effect(reason)))
                }
            }
        }
        Err(tool_outcome_recovery_error(
            "effect_journal_contention",
            "Tool effect journal did not converge during recovery",
        ))
    }

    /// Executes the fixed guarded pipeline:
    ///
    /// invocation/input schema → effective policy → operation planning →
    /// permission decision/approval → concurrency gate/executor → output
    /// schema and output limit.
    pub async fn invoke(
        &self,
        invocation: ToolInvocation,
        run_grant: RunToolGrant,
        approval: Option<ApprovalCapability>,
        run_cancellation: CancellationToken,
    ) -> GuardedToolResult {
        if let Err(error) = invocation.validate() {
            return rejected("invalid_invocation", error.message);
        }
        let registered = match self.registered_tool(&invocation.tool_id) {
            Ok(Some(registered)) => registered,
            Ok(None) => {
                return rejected(
                    "tool_not_found",
                    format!("tool is not registered: {}", invocation.tool_id),
                )
            }
            Err(error) => return rejected("runtime_unavailable", error.to_string()),
        };
        if let Err(error) = registered
            .descriptor
            .model_schema
            .validate_arguments(&invocation.arguments)
        {
            return rejected("input_schema_violation", error.message);
        }

        let effective_policy = match EffectiveToolPolicy::resolve(
            &self.host_ceiling,
            &run_grant,
            &registered.descriptor.restriction,
        ) {
            Ok(policy) => policy,
            Err(error) => return rejected("invalid_effective_policy", error.message),
        };
        let operation = match registered.executor.plan_operation(
            &invocation,
            &registered.descriptor,
            &effective_policy,
        ) {
            Ok(operation) => operation,
            Err(outcome) => {
                return GuardedToolResult::Outcome {
                    outcome,
                    cached: false,
                }
            }
        };
        if let Err(error) = operation.validate_envelope(&registered.descriptor.effect_scopes) {
            return rejected("invalid_operation_plan", error.message);
        }
        if !effective_policy.authorizes_scopes(&operation.effect_scopes) {
            return rejected(
                "policy_denied",
                "tool effects are outside the effective Host policy",
            );
        }
        let permission = constrain_permission_decision(
            &effective_policy,
            self.permission_policy
                .decide(&registered.descriptor, &operation, &effective_policy),
        );
        if let ToolPermissionDecision::Deny { code, message } = &permission {
            return rejected(code.clone(), message.clone());
        }
        let permission_digest =
            match tool_permission_decision_digest(self.permission_policy.as_ref(), &permission) {
                Ok(digest) => digest,
                Err(error) => return rejected("invalid_permission_decision", error.message),
            };
        let approval_binding = match ApprovalBinding::for_operation(
            &invocation,
            &operation,
            &effective_policy,
            permission_digest.clone(),
        ) {
            Ok(binding) => binding,
            Err(error) => return rejected("policy_denied", error.message),
        };
        let identity = match invocation_identity(
            &invocation,
            &operation,
            &effective_policy,
            &permission_digest,
            &registered.descriptor,
        ) {
            Ok(identity) => identity,
            Err(error) => return rejected("invalid_invocation", error.message),
        };
        let planned = PlannedInvocation {
            operation,
            effective_policy,
            permission,
            permission_digest,
            approval_binding,
        };
        let effect_key = ToolEffectKey::new(invocation.run_id.clone(), invocation.call_id.clone());
        let entry = match self.invocation_entry(&invocation, identity) {
            Ok(entry) => entry,
            Err(result) => return *result,
        };

        let verified_approval = loop {
            // Register the waiter before observing the state to avoid a missed
            // notification between unlocking and awaiting.
            let changed = entry.changed.notified();
            let mut state = entry.state.lock().await;
            match &*state {
                InvocationState::Completed(outcome) => {
                    return GuardedToolResult::Outcome {
                        outcome: outcome.clone(),
                        cached: true,
                    };
                }
                InvocationState::Running => {
                    drop(state);
                    changed.await;
                }
                InvocationState::Ready => {
                    match self
                        .prepare_durable_invocation(
                            &registered,
                            &invocation,
                            &planned,
                            approval.as_ref(),
                            &run_cancellation,
                        )
                        .await
                    {
                        Ok(DurableInvocationStart::Execute { verified_approval }) => {
                            *state = InvocationState::Running;
                            break verified_approval;
                        }
                        Ok(DurableInvocationStart::Replay { outcome }) => {
                            *state = InvocationState::Completed(outcome.clone());
                            drop(state);
                            entry.changed.notify_waiters();
                            return GuardedToolResult::Outcome {
                                outcome,
                                cached: true,
                            };
                        }
                        Err(result) => return result,
                    }
                }
            }
        };

        let execution_cancellation = run_cancellation.child_token();
        let PlannedInvocation {
            operation,
            effective_policy,
            ..
        } = planned;
        let outcome = match self
            .concurrency_gate(&registered, &invocation, &execution_cancellation)
            .await
        {
            Ok(gate_guard) => {
                let _gate_guard = gate_guard;
                self.execute(
                    registered,
                    invocation,
                    operation,
                    effective_policy,
                    verified_approval,
                    execution_cancellation,
                )
                .await
            }
            Err(outcome) => outcome,
        };

        let outcome = self.commit_durable_outcome(&effect_key, outcome).await;
        let mut state = entry.state.lock().await;
        *state = InvocationState::Completed(outcome.clone());
        drop(state);
        entry.changed.notify_waiters();
        GuardedToolResult::Outcome {
            outcome,
            cached: false,
        }
    }

    /// Loads a durable Tool effect without closing `Observed` or classifying
    /// `Invoked`. This is deliberately read-only so a workflow can perform a
    /// global recovery preflight before it dispatches any new work.
    pub async fn inspect_effect(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Option<ToolEffectProjection>, ToolOutcomeRecoveryError> {
        key.validate().map_err(effect_journal_recovery_error)?;
        let records = self
            .effect_journal
            .load_effect(key)
            .await
            .map_err(effect_journal_recovery_error)?;
        replay_tool_effect(key, &records).map_err(effect_journal_recovery_error)
    }

    async fn prepare_durable_invocation(
        &self,
        registered: &Arc<RegisteredTool>,
        invocation: &ToolInvocation,
        planned: &PlannedInvocation,
        approval: Option<&ApprovalCapability>,
        run_cancellation: &CancellationToken,
    ) -> Result<DurableInvocationStart, GuardedToolResult> {
        let PlannedInvocation {
            operation,
            effective_policy,
            permission,
            permission_digest,
            approval_binding,
        } = planned;
        let prepared = PreparedToolEffect {
            invocation: invocation.clone(),
            args_digest: invocation
                .args_digest()
                .map_err(|error| rejected("invalid_invocation", error.message))?,
            operation_digest: operation
                .digest()
                .map_err(|error| rejected("invalid_operation_plan", error.message))?,
            permission_digest: permission_digest.clone(),
            policy_digest: effective_policy
                .digest()
                .map_err(|error| rejected("invalid_effective_policy", error.message))?,
            descriptor_digest: registered
                .descriptor
                .digest()
                .map_err(|error| rejected("invalid_descriptor", error.message))?,
            idempotency: registered.descriptor.idempotency,
            effect_scopes: operation.effect_scopes.clone(),
        };
        let key = prepared.key();

        for _ in 0..4 {
            let records = self
                .effect_journal
                .load_effect(&key)
                .await
                .map_err(effect_journal_rejected)?;
            let projection = replay_tool_effect(&key, &records).map_err(effect_journal_rejected)?;
            let Some(projection) = projection else {
                match self
                    .effect_journal
                    .append(
                        0,
                        ToolEffectEventDraft {
                            event_id: effect_event_id(&key, "prepared"),
                            key: key.clone(),
                            payload: ToolEffectEvent::Prepared {
                                effect: prepared.clone(),
                            },
                        },
                    )
                    .await
                {
                    Ok(_) | Err(ToolEffectError::SequenceConflict { .. }) => continue,
                    Err(error) => return Err(effect_journal_rejected(error)),
                }
            };
            if projection.prepared != prepared {
                return Err(rejected(
                    "call_identity_conflict",
                    "durable Tool effect identity differs for the same run_id/call_id",
                ));
            }
            match projection.phase {
                ToolEffectPhase::Prepared => {
                    // Prepared records contain intent only. Cancellation here
                    // proves the executor never crossed the durable Invoked
                    // boundary, while prior Invoked/Observed/Committed phases
                    // below still retain their conservative replay semantics.
                    if run_cancellation.is_cancelled() {
                        return Err(GuardedToolResult::Outcome {
                            outcome: ToolOutcome::Cancelled,
                            cached: false,
                        });
                    }
                    let (verified_approval, authorization) =
                        if matches!(permission, ToolPermissionDecision::RequireApproval) {
                            let Some(capability) = approval else {
                                return Err(GuardedToolResult::ApprovalRequired {
                                    binding: approval_binding.clone(),
                                    summary: sanitize_approval_summary(
                                        &operation.summary,
                                        &invocation.tool_id,
                                    ),
                                });
                            };
                            let verified = self
                                .approval_verifier
                                .verify_and_consume(
                                    capability,
                                    approval_binding,
                                    chrono::Utc::now().timestamp_millis(),
                                )
                                .map_err(|error| {
                                    rejected(approval_error_code(error.code), error.message)
                                })?;
                            let evidence = ToolAuthorizationEvidence::Approval {
                                nonce: verified.nonce().clone(),
                            };
                            (Some(verified), evidence)
                        } else {
                            (None, ToolAuthorizationEvidence::Policy)
                        };
                    let appended = self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(&key, "invoked"),
                                key: key.clone(),
                                payload: ToolEffectEvent::Invoked {
                                    attempt_id: ToolEffectAttemptId::new(format!(
                                        "attempt:{}:{}",
                                        key.run_id.as_str(),
                                        key.call_id.as_str()
                                    )),
                                    authorization,
                                },
                            },
                        )
                        .await;
                    match appended {
                        Ok(_) => return Ok(DurableInvocationStart::Execute { verified_approval }),
                        Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => return Err(effect_journal_rejected(error)),
                    }
                }
                ToolEffectPhase::Observed { outcome, .. } => {
                    let outcome_digest = outcome
                        .digest()
                        .map_err(|error| rejected("invalid_tool_outcome", error.message))?;
                    match self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(&key, "committed"),
                                key: key.clone(),
                                payload: ToolEffectEvent::Committed { outcome_digest },
                            },
                        )
                        .await
                    {
                        Ok(_) => return Ok(DurableInvocationStart::Replay { outcome }),
                        Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => return Err(effect_journal_rejected(error)),
                    }
                }
                ToolEffectPhase::Committed { outcome, .. } => {
                    return Ok(DurableInvocationStart::Replay { outcome })
                }
                ToolEffectPhase::Invoked { .. } => {
                    let reason = "durable invocation has no observation after runtime recovery";
                    match self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(&key, "unknown"),
                                key: key.clone(),
                                payload: ToolEffectEvent::EffectUnknown {
                                    reason: reason.to_owned(),
                                },
                            },
                        )
                        .await
                    {
                        Ok(_) => {
                            return Ok(DurableInvocationStart::Replay {
                                outcome: unknown_effect(reason),
                            })
                        }
                        Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => {
                            return Ok(DurableInvocationStart::Replay {
                                outcome: unknown_effect(format!(
                                    "{reason}; journal update failed: {error}"
                                )),
                            })
                        }
                    }
                }
                ToolEffectPhase::UnknownEffect { reason, .. } => {
                    return Ok(DurableInvocationStart::Replay {
                        outcome: unknown_effect(reason),
                    })
                }
            }
        }
        Err(rejected(
            "effect_journal_contention",
            "Tool effect journal did not converge after concurrent updates",
        ))
    }

    async fn commit_durable_outcome(
        &self,
        key: &ToolEffectKey,
        outcome: ToolOutcome,
    ) -> ToolOutcome {
        for _ in 0..5 {
            let records = match self.effect_journal.load_effect(key).await {
                Ok(records) => records,
                Err(error) => {
                    return unknown_effect(format!(
                        "Tool effect completed but its journal is unavailable: {error}"
                    ))
                }
            };
            let projection = match replay_tool_effect(key, &records) {
                Ok(Some(projection)) => projection,
                Ok(None) => {
                    return unknown_effect(
                        "Tool effect completed without a durable Prepared record",
                    )
                }
                Err(error) => {
                    return unknown_effect(format!(
                        "Tool effect completed but its journal is corrupt: {error}"
                    ))
                }
            };
            match (&projection.phase, &outcome) {
                (ToolEffectPhase::Invoked { .. }, ToolOutcome::UnknownEffect { message }) => {
                    match self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(key, "unknown"),
                                key: key.clone(),
                                payload: ToolEffectEvent::EffectUnknown {
                                    reason: message.clone(),
                                },
                            },
                        )
                        .await
                    {
                        Ok(_) => return outcome,
                        Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => {
                            return unknown_effect(format!(
                                "{message}; journal update failed: {error}"
                            ))
                        }
                    }
                }
                (ToolEffectPhase::Invoked { .. }, _) => {
                    match self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(key, "observed"),
                                key: key.clone(),
                                payload: ToolEffectEvent::Observed {
                                    outcome: outcome.clone(),
                                },
                            },
                        )
                        .await
                    {
                        Ok(_) | Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => {
                            return unknown_effect(format!(
                                "Tool effect outcome could not be observed durably: {error}"
                            ))
                        }
                    }
                }
                (
                    ToolEffectPhase::Observed {
                        outcome: observed, ..
                    },
                    _,
                ) => {
                    if observed != &outcome {
                        return unknown_effect(
                            "durable Tool observation differs from the live outcome",
                        );
                    }
                    let outcome_digest = match outcome.digest() {
                        Ok(digest) => digest,
                        Err(error) => {
                            return unknown_effect(format!(
                                "Tool effect produced an invalid outcome: {}",
                                error.message
                            ))
                        }
                    };
                    match self
                        .effect_journal
                        .append(
                            projection.last_effect_seq,
                            ToolEffectEventDraft {
                                event_id: effect_event_id(key, "committed"),
                                key: key.clone(),
                                payload: ToolEffectEvent::Committed { outcome_digest },
                            },
                        )
                        .await
                    {
                        Ok(_) => return outcome,
                        Err(ToolEffectError::SequenceConflict { .. }) => continue,
                        Err(error) => {
                            return unknown_effect(format!(
                                "Tool effect observation was durable but commit failed: {error}"
                            ))
                        }
                    }
                }
                (
                    ToolEffectPhase::Committed {
                        outcome: committed, ..
                    },
                    _,
                ) => {
                    return if committed == &outcome {
                        committed.clone()
                    } else {
                        unknown_effect("committed Tool outcome differs from the live outcome")
                    }
                }
                (ToolEffectPhase::UnknownEffect { reason, .. }, _) => {
                    return unknown_effect(reason.clone())
                }
                (ToolEffectPhase::Prepared, _) => {
                    return unknown_effect(
                        "Tool executor was entered without a durable Invoked boundary",
                    )
                }
            }
        }
        unknown_effect("Tool effect journal did not converge while committing the outcome")
    }

    /// Releases replay and per-Run gate state once the owning Agent Run is no
    /// longer resumable in this process.
    pub fn forget_run(&self, run_id: &RunId) -> Result<(), ToolRuntimeError> {
        self.invocations
            .lock()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?
            .retain(|(entry_run_id, _), _| entry_run_id != run_id);
        self.per_run_gates
            .lock()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?
            .retain(|(_, entry_run_id), _| entry_run_id != run_id);
        Ok(())
    }

    fn registered_tool(
        &self,
        tool_id: &ToolId,
    ) -> Result<Option<Arc<RegisteredTool>>, ToolRuntimeError> {
        Ok(self
            .registry
            .read()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?
            .get(tool_id)
            .cloned())
    }

    fn invocation_entry(
        &self,
        invocation: &ToolInvocation,
        identity: InvocationIdentity,
    ) -> Result<Arc<InvocationEntry>, Box<GuardedToolResult>> {
        let key = (invocation.run_id.clone(), invocation.call_id.clone());
        let mut invocations = self.invocations.lock().map_err(|_| {
            Box::new(rejected(
                "runtime_unavailable",
                "Tool call ledger is unavailable",
            ))
        })?;
        if let Some(entry) = invocations.get(&key) {
            if entry.identity != identity {
                return Err(Box::new(rejected(
                    "call_identity_conflict",
                    "the same run_id/call_id was reused with different content or policy",
                )));
            }
            return Ok(entry.clone());
        }
        let entry = Arc::new(InvocationEntry {
            identity,
            state: AsyncMutex::new(InvocationState::Ready),
            changed: Notify::new(),
        });
        invocations.insert(key, entry.clone());
        Ok(entry)
    }

    async fn concurrency_gate(
        &self,
        registered: &Arc<RegisteredTool>,
        invocation: &ToolInvocation,
        cancellation: &CancellationToken,
    ) -> Result<Option<OwnedMutexGuard<()>>, ToolOutcome> {
        let gate = match registered.descriptor.concurrency {
            ToolConcurrency::ParallelSafe => return Ok(None),
            ToolConcurrency::PerRunSerial => {
                match self.per_run_gate(&invocation.tool_id, &invocation.run_id) {
                    Ok(gate) => gate,
                    Err(error) => {
                        return Err(ToolOutcome::Failed {
                            code: "runtime_unavailable".to_owned(),
                            message: error.to_string(),
                            retryable: true,
                        })
                    }
                }
            }
            ToolConcurrency::GlobalSerial => registered.global_gate.clone(),
            // Unknown future modes are conservatively serialized globally.
            _ => registered.global_gate.clone(),
        };
        tokio::select! {
            _ = cancellation.cancelled() => Err(ToolOutcome::Cancelled),
            guard = gate.lock_owned() => Ok(Some(guard)),
        }
    }

    fn per_run_gate(
        &self,
        tool_id: &ToolId,
        run_id: &RunId,
    ) -> Result<Arc<AsyncMutex<()>>, ToolRuntimeError> {
        let key = (tool_id.clone(), run_id.clone());
        let mut gates = self
            .per_run_gates
            .lock()
            .map_err(|_| ToolRuntimeError::StateUnavailable)?;
        if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        let gate = Arc::new(AsyncMutex::new(()));
        gates.insert(key, Arc::downgrade(&gate));
        Ok(gate)
    }

    async fn execute(
        &self,
        registered: Arc<RegisteredTool>,
        invocation: ToolInvocation,
        operation: ToolOperationPlan,
        effective_policy: EffectiveToolPolicy,
        approval: Option<VerifiedApprovalCapability>,
        cancellation: CancellationToken,
    ) -> ToolOutcome {
        let timeout_ms = effective_policy.bounds().max_timeout_ms;
        let output_invocation = invocation.clone();
        let execution = registered.executor.execute(GuardedToolExecution {
            invocation,
            operation,
            effective_policy: effective_policy.clone(),
            approval,
            cancellation: cancellation.clone(),
        });
        let execution = AssertUnwindSafe(execution).catch_unwind();
        tokio::pin!(execution);

        let outcome = match timeout_ms {
            Some(timeout_ms) => {
                let timeout = tokio::time::sleep(Duration::from_millis(timeout_ms));
                tokio::pin!(timeout);
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        let fallback = cancellation_outcome(&registered.descriptor);
                        settle_cancelled_executor(&mut execution, fallback).await
                    },
                    _ = &mut timeout => {
                        cancellation.cancel();
                        let fallback = timeout_outcome(&registered.descriptor);
                        settle_cancelled_executor(&mut execution, fallback).await
                    }
                    result = &mut execution => map_execution_result(result),
                }
            }
            None => {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        let fallback = cancellation_outcome(&registered.descriptor);
                        settle_cancelled_executor(&mut execution, fallback).await
                    },
                    result = &mut execution => map_execution_result(result),
                }
            }
        };
        // The executor future has crossed the Host's dispatch boundary. A
        // non-idempotent executor may observe the same cancellation as this
        // outer select and return `Cancelled` first; that race cannot prove
        // whether its external effect happened, so preserve the conservative
        // `UnknownEffect` contract.
        let outcome = normalize_post_dispatch_outcome(&registered.descriptor, outcome);
        normalize_completed_outcome(
            &registered.descriptor,
            &effective_policy,
            &output_invocation,
            self.artifact_store.as_ref(),
            &cancellation,
            outcome,
        )
        .await
    }
}

#[async_trait]
impl<S> AgentToolRuntime for GuardedToolRuntime<S>
where
    S: ApprovalCapabilityStore + 'static,
{
    fn execution_contract_digest(&self) -> Result<Digest, ToolRuntimeError> {
        GuardedToolRuntime::execution_contract_digest(self)
    }

    fn model_tool_schemas(&self) -> Result<Vec<ModelToolSchema>, ToolRuntimeError> {
        GuardedToolRuntime::model_tool_schemas(self)
    }

    fn resolve_tool_id(&self, model_name: &str) -> Result<Option<ToolId>, ToolRuntimeError> {
        GuardedToolRuntime::resolve_tool_id(self, model_name)
    }

    async fn inspect_effect(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Option<ToolEffectProjection>, ToolOutcomeRecoveryError> {
        GuardedToolRuntime::inspect_effect(self, key).await
    }

    async fn recover_outcome(
        &self,
        invocation: ToolInvocation,
        run_grant: RunToolGrant,
    ) -> Result<Option<ToolOutcome>, ToolOutcomeRecoveryError> {
        GuardedToolRuntime::recover_outcome(self, invocation, run_grant).await
    }

    async fn invoke(
        &self,
        invocation: ToolInvocation,
        run_grant: RunToolGrant,
        approval: Option<ApprovalCapability>,
        run_cancellation: CancellationToken,
    ) -> GuardedToolResult {
        GuardedToolRuntime::invoke(self, invocation, run_grant, approval, run_cancellation).await
    }
}

fn invocation_identity(
    invocation: &ToolInvocation,
    operation: &ToolOperationPlan,
    effective_policy: &EffectiveToolPolicy,
    permission_digest: &Digest,
    descriptor: &ToolDescriptor,
) -> Result<InvocationIdentity, ToolProtocolError> {
    Ok(InvocationIdentity {
        tool_id: invocation.tool_id.clone(),
        args_digest: invocation.args_digest()?,
        operation_digest: operation.digest()?,
        permission_digest: permission_digest.clone(),
        policy_digest: effective_policy.digest()?,
        descriptor_digest: descriptor.digest()?,
    })
}

fn effect_event_id(key: &ToolEffectKey, phase: &str) -> ToolEffectEventId {
    ToolEffectEventId::new(format!(
        "effect:{}:{}:{phase}",
        key.run_id.as_str(),
        key.call_id.as_str()
    ))
}

fn effect_journal_rejected(error: ToolEffectError) -> GuardedToolResult {
    rejected("effect_journal_unavailable", error.to_string())
}

fn tool_outcome_recovery_error(
    code: impl Into<String>,
    message: impl Into<String>,
) -> ToolOutcomeRecoveryError {
    ToolOutcomeRecoveryError {
        code: code.into(),
        message: message.into(),
    }
}

fn effect_journal_recovery_error(error: ToolEffectError) -> ToolOutcomeRecoveryError {
    tool_outcome_recovery_error("effect_journal_unavailable", error.to_string())
}

fn unknown_effect(message: impl Into<String>) -> ToolOutcome {
    ToolOutcome::UnknownEffect {
        message: message.into(),
    }
}

fn cancellation_outcome(descriptor: &ToolDescriptor) -> ToolOutcome {
    if matches!(descriptor.idempotency, ToolIdempotency::NonIdempotent) {
        unknown_effect(
            "non-idempotent Tool was cancelled after its durable invocation boundary; effect completion is unknown",
        )
    } else {
        ToolOutcome::Cancelled
    }
}

fn normalize_post_dispatch_outcome(
    descriptor: &ToolDescriptor,
    outcome: ToolOutcome,
) -> ToolOutcome {
    match outcome {
        ToolOutcome::Cancelled => cancellation_outcome(descriptor),
        outcome => outcome,
    }
}

fn timeout_outcome(descriptor: &ToolDescriptor) -> ToolOutcome {
    if matches!(descriptor.idempotency, ToolIdempotency::NonIdempotent) {
        unknown_effect(
            "non-idempotent Tool timed out after its durable invocation boundary; effect completion is unknown",
        )
    } else {
        ToolOutcome::Failed {
            code: "timeout".to_owned(),
            message: "tool execution exceeded its Host timeout".to_owned(),
            retryable: false,
        }
    }
}

async fn settle_cancelled_executor<F>(
    execution: &mut std::pin::Pin<&mut F>,
    fallback: ToolOutcome,
) -> ToolOutcome
where
    F: std::future::Future<Output = Result<ToolOutcome, Box<dyn std::any::Any + Send>>>,
{
    match tokio::time::timeout(Duration::from_millis(250), execution).await {
        Ok(result) => match map_execution_result(result) {
            outcome @ ToolOutcome::UnknownEffect { .. } => outcome,
            _ => fallback,
        },
        Err(_) => fallback,
    }
}

fn map_execution_result(result: Result<ToolOutcome, Box<dyn std::any::Any + Send>>) -> ToolOutcome {
    match result {
        Ok(outcome) => outcome,
        Err(_) => ToolOutcome::UnknownEffect {
            message: "tool executor panicked; effect completion is unknown".to_owned(),
        },
    }
}

async fn normalize_completed_outcome(
    descriptor: &ToolDescriptor,
    effective_policy: &EffectiveToolPolicy,
    invocation: &ToolInvocation,
    artifact_store: Option<&ToolArtifactStore>,
    cancellation: &CancellationToken,
    outcome: ToolOutcome,
) -> ToolOutcome {
    let ToolOutcome::Completed { output } = outcome else {
        return outcome;
    };
    let ToolOutput::Inline(output) = output else {
        return ToolOutcome::Failed {
            code: "executor_artifact_forbidden".to_owned(),
            message: "Tool executors cannot mint Artifact references; only the Host may spill validated output"
                .to_owned(),
            retryable: false,
        };
    };
    if let Err(error) = descriptor.validate_output(&output) {
        return ToolOutcome::Failed {
            code: "output_schema_violation".to_owned(),
            message: error.message,
            retryable: false,
        };
    }
    let bytes = match serde_jcs::to_vec(&output) {
        Ok(bytes) => bytes,
        Err(error) => {
            return ToolOutcome::Failed {
                code: "output_serialization_failed".to_owned(),
                message: error.to_string(),
                retryable: false,
            }
        }
    };
    let Some(inline_max_bytes) = effective_policy.bounds().max_output_bytes else {
        return ToolOutcome::Completed {
            output: ToolOutput::Inline(output),
        };
    };
    if bytes.len() as u64 <= inline_max_bytes {
        return ToolOutcome::Completed {
            output: ToolOutput::Inline(output),
        };
    }
    let Some(artifact_store) = artifact_store else {
        return ToolOutcome::Failed {
            code: "output_limit_exceeded".to_owned(),
            message: "Tool output exceeded its Host inline byte limit and no Artifact store is configured"
                .to_owned(),
            retryable: false,
        };
    };
    let summary = summarize_tool_output(
        &output,
        bytes.len() as u64,
        artifact_store.summary_max_chars,
    );
    match artifact_store
        .spill(invocation, bytes, summary, cancellation)
        .await
    {
        Ok(artifact) => ToolOutcome::Completed {
            output: ToolOutput::Artifact(artifact),
        },
        Err(ToolArtifactError::Cancelled) => cancellation_outcome(descriptor),
        Err(error) if matches!(descriptor.idempotency, ToolIdempotency::NonIdempotent) => {
            unknown_effect(format!(
                "non-idempotent Tool completed but its result could not be persisted: {error}"
            ))
        }
        Err(error) => ToolOutcome::Failed {
            code: "artifact_persistence_failed".to_owned(),
            message: error.to_string(),
            retryable: true,
        },
    }
}

fn summarize_tool_output(output: &serde_json::Value, byte_size: u64, max_chars: usize) -> String {
    let shape = match output {
        serde_json::Value::Object(values) => {
            format!("JSON object with {} top-level fields", values.len())
        }
        serde_json::Value::Array(values) => format!("JSON array with {} items", values.len()),
        serde_json::Value::String(_) => "JSON string".to_owned(),
        serde_json::Value::Number(_) => "JSON number".to_owned(),
        serde_json::Value::Bool(_) => "JSON boolean".to_owned(),
        serde_json::Value::Null => "JSON null".to_owned(),
    };
    let preview = serde_json::to_string(output).unwrap_or_else(|_| "<unavailable>".to_owned());
    let mut chars = preview.chars();
    let mut preview = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        preview.push('…');
    }
    format!("{shape}; {byte_size} bytes. Preview: {preview}")
}

fn sanitize_approval_summary(summary: &str, tool_id: &ToolId) -> String {
    const MAX_CHARS: usize = 512;
    let normalized = summary
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let normalized = if normalized.is_empty() {
        format!("Invoke Tool {}", tool_id.as_str())
    } else {
        normalized
    };
    let mut chars = normalized.chars();
    let mut bounded = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        bounded.push('…');
    }
    bounded
}

fn rejected(code: impl Into<String>, message: impl Into<String>) -> GuardedToolResult {
    GuardedToolResult::Outcome {
        outcome: ToolOutcome::Rejected {
            code: code.into(),
            message: message.into(),
        },
        cached: false,
    }
}

fn approval_error_code(code: ToolProtocolErrorCode) -> &'static str {
    match code {
        ToolProtocolErrorCode::CapabilityExpired => "approval_expired",
        ToolProtocolErrorCode::CapabilityBindingMismatch => "approval_binding_mismatch",
        ToolProtocolErrorCode::CapabilityReplayed => "approval_replayed",
        ToolProtocolErrorCode::StoreFailure => "approval_store_failure",
        ToolProtocolErrorCode::InvalidCapability => "invalid_approval_capability",
        _ => "approval_validation_failed",
    }
}
