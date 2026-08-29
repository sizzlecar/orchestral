use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(target_os = "macos")]
use std::time::Instant;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::{Digest, RunId};
use orchestral_core::io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobWriteRequest,
};
use orchestral_core::tool_effect::{
    replay_tool_effect, InMemoryToolEffectJournalStore, PreparedToolEffect,
    ToolAuthorizationEvidence, ToolEffectAppend, ToolEffectAttemptId, ToolEffectError,
    ToolEffectEvent, ToolEffectEventDraft, ToolEffectEventId, ToolEffectJournalRecord,
    ToolEffectJournalStore, ToolEffectKey, ToolEffectPhase,
};
use orchestral_core::tool_protocol::HostApprovalIssuer;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, CapabilityRequest, CapabilitySelector, EffectScope, EffectiveToolPolicy,
    EnvironmentPolicy, FilesystemPolicy, HostApprovalVerifier, HostToolPolicy,
    InMemoryApprovalCapabilityStore, InteractiveCommandPolicy, ModelToolSchema, NetworkPolicy,
    ProcessPolicy, RunToolGrant, SandboxPolicy, ToolCallId, ToolConcurrency, ToolDescriptor,
    ToolId, ToolIdempotency, ToolInvocation, ToolOperationPlan, ToolOperationRisk, ToolOutcome,
    ToolOutput, ToolPolicyBounds, ToolRestriction, TransportLaunchPolicy,
};
use orchestral_runtime::{
    tool_permission_decision_digest,
    tools::{
        guarded_artifact_read_descriptor, guarded_file_read_descriptor, guarded_shell_descriptor,
        GuardedArtifactReadExecutor, GuardedFileReadExecutor, GuardedShellExecutor,
        GUARDED_SHELL_SANDBOX_PROFILE,
    },
    DescriptorPermissionPolicy, GuardedToolExecution, GuardedToolExecutor, GuardedToolResult,
    GuardedToolRuntime, HookDispatchMode, HookError, HookExecutionPolicy, HookFailurePolicy,
    HookRegistry, InMemoryBlobStore, RuntimeHook, RuntimeHookContext, RuntimeHookEventEnvelope,
    ToolArtifactStore, ToolPermissionDecision, ToolPermissionPolicy, WorkspacePermissionPolicy,
};
#[cfg(target_os = "macos")]
use orchestral_runtime::{
    tools::{
        guarded_pty_close_descriptor, guarded_pty_create_descriptor,
        guarded_pty_create_descriptor_with_program_aliases, guarded_pty_list_descriptor,
        guarded_pty_read_descriptor, guarded_pty_write_descriptor,
        guarded_shell_descriptor_with_program_aliases, GuardedProgramAliases,
        GuardedPtyCloseExecutor, GuardedPtyCreateExecutor, GuardedPtyListExecutor,
        GuardedPtyReadExecutor, GuardedPtyWriteExecutor, GUARDED_PTY_SANDBOX_PROFILE,
    },
    PtyProcessManager,
};
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

fn strings(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn effects(values: &[EffectScope]) -> BTreeSet<EffectScope> {
    values.iter().copied().collect()
}

fn capabilities(values: &[EffectScope]) -> CapabilityRequest {
    CapabilityRequest::from_effects(effects(values))
}

fn interactive_process(command_shells: BTreeSet<String>) -> ProcessPolicy {
    ProcessPolicy {
        interactive: InteractiveCommandPolicy {
            enabled: true,
            command_shells,
            allow_child_processes: false,
        },
        transport: TransportLaunchPolicy::default(),
    }
}

fn policy(approval: ApprovalPolicy) -> ToolPolicyBounds {
    ToolPolicyBounds {
        allowed_effects: effects(&[EffectScope::Process]),
        approval,
        sandbox: SandboxPolicy {
            required: false,
            allowed_profiles: strings(&["strict"]),
        },
        process: interactive_process(strings(&["echo"])),
        filesystem: FilesystemPolicy::default(),
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(2_000),
        max_output_bytes: Some(1_024),
    }
}

fn descriptor(bounds: ToolPolicyBounds, concurrency: ToolConcurrency) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("test/echo"),
        model_schema: ModelToolSchema {
            name: "echo".to_owned(),
            description: "Echo one value".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["value"],
                "properties": { "value": { "type": "string" } },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": ["result"],
            "properties": { "result": { "type": "string" } },
            "additionalProperties": false
        }),
        effect_scopes: effects(&[EffectScope::Process]),
        restriction: ToolRestriction { bounds },
        idempotency: ToolIdempotency::IdempotentWithKey,
        concurrency,
    }
}

fn invocation(value: &str) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new("run-1"),
        call_id: ToolCallId::new("call-1"),
        tool_id: ToolId::new("test/echo"),
        arguments: json!({ "value": value }),
    }
}

fn runtime(bounds: ToolPolicyBounds) -> GuardedToolRuntime<InMemoryApprovalCapabilityStore> {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    GuardedToolRuntime::new(HostToolPolicy { bounds }, verifier).unwrap()
}

#[cfg(unix)]
fn strict_shell_security_bounds(root: &Path) -> (ToolPolicyBounds, String) {
    let root = std::fs::canonicalize(root).unwrap();
    let root = root.to_string_lossy().into_owned();
    let executable = std::fs::canonicalize("/bin/echo")
        .unwrap()
        .to_string_lossy()
        .into_owned();
    (
        ToolPolicyBounds {
            allowed_effects: effects(&[
                EffectScope::Process,
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
                EffectScope::EnvironmentRead,
                EffectScope::ExternalSideEffect,
            ]),
            approval: ApprovalPolicy::Required,
            sandbox: SandboxPolicy {
                required: true,
                allowed_profiles: strings(&[GUARDED_SHELL_SANDBOX_PROFILE]),
            },
            process: interactive_process(BTreeSet::from([executable.clone()])),
            filesystem: FilesystemPolicy {
                readable_roots: BTreeSet::from([root.clone()]),
                writable_roots: BTreeSet::from([root]),
            },
            network: NetworkPolicy::default(),
            environment: EnvironmentPolicy::default(),
            allowed_credentials: BTreeSet::new(),
            max_timeout_ms: Some(2_000),
            max_output_bytes: Some(4 * 1024),
        },
        executable,
    )
}

fn runtime_with_effect_journal(
    bounds: ToolPolicyBounds,
    journal: Arc<dyn ToolEffectJournalStore>,
) -> GuardedToolRuntime<InMemoryApprovalCapabilityStore> {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    GuardedToolRuntime::new_with_effect_journal(HostToolPolicy { bounds }, verifier, journal)
        .unwrap()
}

fn runtime_with_artifacts(
    bounds: ToolPolicyBounds,
    journal: Arc<dyn ToolEffectJournalStore>,
    artifacts: ToolArtifactStore,
) -> GuardedToolRuntime<InMemoryApprovalCapabilityStore> {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default()).unwrap();
    GuardedToolRuntime::new_with_effect_journal_and_artifacts(
        HostToolPolicy { bounds },
        verifier,
        journal,
        artifacts,
    )
    .unwrap()
}

struct EchoExecutor {
    calls: AtomicUsize,
    delay: Duration,
}

struct PlannedEchoExecutor {
    calls: AtomicUsize,
    operation: ToolOperationPlan,
}

struct LeaseEchoExecutor {
    calls: AtomicUsize,
    saw_approved_network_lease: AtomicBool,
    operation: ToolOperationPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectCrashBoundary {
    Prepared,
    Invoked,
    Observed,
    Committed,
}

impl EffectCrashBoundary {
    const ALL: [Self; 4] = [
        Self::Prepared,
        Self::Invoked,
        Self::Observed,
        Self::Committed,
    ];

    fn matches(self, event: &ToolEffectEvent) -> bool {
        matches!(
            (self, event),
            (Self::Prepared, ToolEffectEvent::Prepared { .. })
                | (Self::Invoked, ToolEffectEvent::Invoked { .. })
                | (Self::Observed, ToolEffectEvent::Observed { .. })
                | (Self::Committed, ToolEffectEvent::Committed { .. })
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectCrashSide {
    BeforeAppend,
    AfterAppendBeforeAck,
}

impl EffectCrashSide {
    const ALL: [Self; 2] = [Self::BeforeAppend, Self::AfterAppendBeforeAck];
}

struct CrashCutEffectJournal {
    inner: Arc<InMemoryToolEffectJournalStore>,
    boundary: EffectCrashBoundary,
    side: EffectCrashSide,
    fired: std::sync::atomic::AtomicBool,
}

impl CrashCutEffectJournal {
    fn new(
        inner: Arc<InMemoryToolEffectJournalStore>,
        boundary: EffectCrashBoundary,
        side: EffectCrashSide,
    ) -> Self {
        Self {
            inner,
            boundary,
            side,
            fired: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ToolEffectJournalStore for CrashCutEffectJournal {
    async fn load_effect(
        &self,
        key: &ToolEffectKey,
    ) -> Result<Vec<ToolEffectJournalRecord>, ToolEffectError> {
        self.inner.load_effect(key).await
    }

    async fn append(
        &self,
        expected_previous: u64,
        draft: ToolEffectEventDraft,
    ) -> Result<ToolEffectAppend, ToolEffectError> {
        let inject =
            self.boundary.matches(&draft.payload) && !self.fired.swap(true, Ordering::SeqCst);
        if !inject {
            return self.inner.append(expected_previous, draft).await;
        }
        match self.side {
            EffectCrashSide::BeforeAppend => Err(ToolEffectError::StoreUnavailable(format!(
                "injected crash before {:?}",
                self.boundary
            ))),
            EffectCrashSide::AfterAppendBeforeAck => {
                self.inner.append(expected_previous, draft).await?;
                Err(ToolEffectError::StoreUnavailable(format!(
                    "injected lost ACK after {:?}",
                    self.boundary
                )))
            }
        }
    }
}

struct SelectiveArtifactHook {
    fail_event: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl RuntimeHook for SelectiveArtifactHook {
    fn id(&self) -> &'static str {
        "selective_artifact_hook"
    }

    async fn on_event(
        &self,
        event: &RuntimeHookEventEnvelope,
        _context: &RuntimeHookContext,
    ) -> Result<(), HookError> {
        self.events
            .lock()
            .expect("artifact hook events lock")
            .push(event.event_type.clone());
        if event.event_type == self.fail_event {
            return Err(HookError::new(format!(
                "rejected {} for test",
                event.event_type
            )));
        }
        Ok(())
    }
}

struct RejectingBlobStore;

#[async_trait]
impl BlobStore for RejectingBlobStore {
    async fn write(&self, _request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
        Err(BlobIoError::Io(
            "injected artifact write failure".to_owned(),
        ))
    }

    async fn read(&self, blob_id: &BlobId) -> Result<BlobRead, BlobIoError> {
        Err(BlobIoError::NotFound(blob_id.to_string()))
    }

    async fn head(&self, blob_id: &BlobId) -> Result<BlobHead, BlobIoError> {
        Err(BlobIoError::NotFound(blob_id.to_string()))
    }

    async fn delete(&self, _blob_id: &BlobId) -> Result<bool, BlobIoError> {
        Ok(false)
    }
}

async fn invoke_oversized_with_artifact_hook(
    failure_policy: HookFailurePolicy,
    fail_event: &'static str,
    store: Arc<dyn BlobStore>,
) -> (GuardedToolResult, Vec<String>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = Arc::new(HookRegistry::new());
    hooks
        .set_policy(HookExecutionPolicy {
            mode: HookDispatchMode::Sequential,
            failure_policy,
            timeout: None,
        })
        .await;
    hooks
        .register(Arc::new(SelectiveArtifactHook {
            fail_event,
            events: events.clone(),
        }))
        .await;

    let mut bounds = policy(ApprovalPolicy::NotRequired);
    bounds.max_output_bytes = Some(64);
    let artifacts = ToolArtifactStore::new(store, 4 * 1024, 80)
        .unwrap()
        .with_hooks(hooks);
    let runtime = runtime_with_artifacts(
        bounds.clone(),
        Arc::new(InMemoryToolEffectJournalStore::default()),
        artifacts,
    );
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            Arc::new(EchoExecutor {
                calls: AtomicUsize::new(0),
                delay: Duration::ZERO,
            }),
        )
        .unwrap();
    let result = runtime
        .invoke(
            invocation(&"x".repeat(512)),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    let recorded = events.lock().expect("artifact hook events lock").clone();
    (result, recorded)
}

#[async_trait]
impl GuardedToolExecutor for EchoExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for PlannedEchoExecutor {
    fn planning_contract(&self) -> serde_json::Value {
        json!({ "contract": "test.planned-echo/v1" })
    }

    fn plan_operation(
        &self,
        _invocation: &ToolInvocation,
        _descriptor: &ToolDescriptor,
        _effective_policy: &EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        Ok(self.operation.clone())
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for LeaseEchoExecutor {
    fn planning_contract(&self) -> serde_json::Value {
        json!({ "contract": "test.lease-echo/v1" })
    }

    fn plan_operation(
        &self,
        _invocation: &ToolInvocation,
        _descriptor: &ToolDescriptor,
        _effective_policy: &EffectiveToolPolicy,
    ) -> Result<ToolOperationPlan, ToolOutcome> {
        Ok(self.operation.clone())
    }

    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.saw_approved_network_lease.store(
            execution.lease.was_approved()
                && execution.lease.granted().requires(EffectScope::Network),
            Ordering::SeqCst,
        );
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
        }
    }
}

async fn seed_effect_trace(
    journal: &Arc<InMemoryToolEffectJournalStore>,
    bounds: &ToolPolicyBounds,
    observed: Option<ToolOutcome>,
) -> ToolEffectKey {
    let invocation = invocation("hello");
    let descriptor = descriptor(bounds.clone(), ToolConcurrency::ParallelSafe);
    let effective = EffectiveToolPolicy::resolve(
        &HostToolPolicy {
            bounds: bounds.clone(),
        },
        &RunToolGrant {
            bounds: bounds.clone(),
        },
        &descriptor.restriction,
    )
    .unwrap();
    let planner = EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    };
    let operation = planner
        .plan_operation(&invocation, &descriptor, &effective)
        .unwrap();
    let prepared = PreparedToolEffect {
        args_digest: invocation.args_digest().unwrap(),
        invocation,
        operation_digest: operation.digest().unwrap(),
        permission_digest: tool_permission_decision_digest(
            &DescriptorPermissionPolicy,
            &ToolPermissionDecision::Allow,
        )
        .unwrap(),
        policy_digest: effective.digest().unwrap(),
        descriptor_digest: descriptor.digest().unwrap(),
        idempotency: descriptor.idempotency,
        effect_scopes: operation.required_capabilities.effects,
    };
    let key = prepared.key();
    journal
        .append(
            0,
            ToolEffectEventDraft {
                event_id: ToolEffectEventId::new("seed-prepared"),
                key: key.clone(),
                payload: ToolEffectEvent::Prepared { effect: prepared },
            },
        )
        .await
        .unwrap();
    journal
        .append(
            1,
            ToolEffectEventDraft {
                event_id: ToolEffectEventId::new("seed-invoked"),
                key: key.clone(),
                payload: ToolEffectEvent::Invoked {
                    attempt_id: ToolEffectAttemptId::new("seed-attempt"),
                    authorization: ToolAuthorizationEvidence::Policy,
                },
            },
        )
        .await
        .unwrap();
    if let Some(outcome) = observed {
        journal
            .append(
                2,
                ToolEffectEventDraft {
                    event_id: ToolEffectEventId::new("seed-observed"),
                    key: key.clone(),
                    payload: ToolEffectEvent::Observed { outcome },
                },
            )
            .await
            .unwrap();
    }
    key
}

#[tokio::test]
async fn approval_required_never_calls_executor() {
    let bounds = policy(ApprovalPolicy::Required);
    let runtime = runtime(bounds.clone());
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            executor.clone(),
        )
        .unwrap();

    let projected = runtime.model_tool_schemas().unwrap();
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].name, "echo");

    let result = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::ApprovalRequired { binding, .. } = result else {
        panic!("expected an approval request");
    };
    assert_eq!(
        binding.requested_capabilities,
        capabilities(&[EffectScope::Process])
    );
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn network_authority_is_requested_before_permission_and_resumes_with_one_lease() {
    let mut bounds = policy(ApprovalPolicy::NotRequired);
    bounds.sandbox.required = true;
    bounds
        .allowed_effects
        .extend([EffectScope::Network, EffectScope::ExternalSideEffect]);
    bounds.network.allow_unrestricted = true;

    let mut required_capabilities = CapabilityRequest::from_effects(BTreeSet::from([
        EffectScope::Process,
        EffectScope::Network,
        EffectScope::ExternalSideEffect,
    ]));
    required_capabilities.insert_resource(EffectScope::Network, CapabilitySelector::Unrestricted);
    required_capabilities.insert_resource(
        EffectScope::ExternalSideEffect,
        CapabilitySelector::Unrestricted,
    );
    let operation = ToolOperationPlan {
        required_capabilities: required_capabilities.clone(),
        risk: ToolOperationRisk::Elevated,
        summary: "Connect to an external service".to_owned(),
    };
    let executor = Arc::new(LeaseEchoExecutor {
        calls: AtomicUsize::new(0),
        saw_approved_network_lease: AtomicBool::new(false),
        operation: operation.clone(),
    });
    let runtime =
        runtime(bounds.clone()).with_permission_policy(Arc::new(WorkspacePermissionPolicy));
    let mut tool = descriptor(bounds.clone(), ToolConcurrency::ParallelSafe);
    tool.effect_scopes = bounds.allowed_effects.clone();
    runtime.register(tool, executor.clone()).unwrap();

    let first = runtime
        .invoke(
            invocation("network"),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::ApprovalRequired { binding, .. } = first else {
        panic!("ungranted network authority must enter approval")
    };
    assert_eq!(binding.requested_capabilities, required_capabilities);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);

    let capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let resumed = runtime
        .invoke(
            invocation("network"),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            Some(capability.clone()),
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        resumed,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            cached: false,
        }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert!(executor.saw_approved_network_lease.load(Ordering::SeqCst));

    let mut other_invocation = invocation("network");
    other_invocation.call_id = ToolCallId::new("call-2");
    let replay = runtime
        .invoke(
            other_invocation,
            RunToolGrant { bounds },
            Some(capability),
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        replay,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            cached: false,
        } if code == "approval_binding_mismatch"
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn workspace_policy_authorizes_the_planned_effects_not_the_tool_envelope() {
    let mut bounds = policy(ApprovalPolicy::NotRequired);
    bounds.sandbox.required = true;
    bounds.allowed_effects = effects(&[
        EffectScope::Process,
        EffectScope::Network,
        EffectScope::FilesystemWrite,
        EffectScope::ExternalSideEffect,
    ]);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let runtime = runtime_with_effect_journal(bounds.clone(), journal.clone())
        .with_permission_policy(Arc::new(WorkspacePermissionPolicy));
    let operation = ToolOperationPlan {
        required_capabilities: capabilities(&[EffectScope::Process]),
        risk: ToolOperationRisk::Elevated,
        summary: "Update the sandboxed workspace".to_owned(),
    };
    let executor = Arc::new(PlannedEchoExecutor {
        calls: AtomicUsize::new(0),
        operation: operation.clone(),
    });
    let mut tool = descriptor(bounds.clone(), ToolConcurrency::ParallelSafe);
    tool.effect_scopes = bounds.allowed_effects.clone();
    runtime.register(tool, executor.clone()).unwrap();

    let result = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            cached: false,
        }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);

    let key = ToolEffectKey::new(RunId::new("run-1"), ToolCallId::new("call-1"));
    let records = journal.load_effect(&key).await.unwrap();
    let projection = replay_tool_effect(&key, &records).unwrap().unwrap();
    assert_eq!(
        projection.prepared.effect_scopes,
        operation.required_capabilities.effects
    );
    assert_eq!(
        projection.prepared.operation_digest,
        operation.digest().unwrap()
    );
}

#[tokio::test]
async fn workspace_policy_routes_destructive_operations_to_exact_review() {
    let mut bounds = policy(ApprovalPolicy::NotRequired);
    bounds.sandbox.required = true;
    let runtime =
        runtime(bounds.clone()).with_permission_policy(Arc::new(WorkspacePermissionPolicy));
    let operation = ToolOperationPlan {
        required_capabilities: capabilities(&[EffectScope::Process]),
        risk: ToolOperationRisk::Destructive,
        summary: "Reset workspace state".to_owned(),
    };
    let executor = Arc::new(PlannedEchoExecutor {
        calls: AtomicUsize::new(0),
        operation: operation.clone(),
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            executor.clone(),
        )
        .unwrap();

    let result = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::ApprovalRequired { binding, summary } = result else {
        panic!("destructive operation must require review")
    };
    assert_eq!(
        binding.requested_capabilities,
        operation.required_capabilities
    );
    assert_eq!(binding.operation_digest, operation.digest().unwrap());
    assert_eq!(summary, operation.summary);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
}

struct AlwaysAllowPermissionPolicy;

impl ToolPermissionPolicy for AlwaysAllowPermissionPolicy {
    fn contract_digest(&self) -> Digest {
        Digest::sha256("test.permission-policy/always-allow/v1")
    }

    fn decide(
        &self,
        _descriptor: &ToolDescriptor,
        _operation: &ToolOperationPlan,
        _effective_policy: &EffectiveToolPolicy,
    ) -> ToolPermissionDecision {
        ToolPermissionDecision::Allow
    }
}

#[tokio::test]
async fn permission_spi_cannot_relax_a_required_static_policy() {
    let bounds = policy(ApprovalPolicy::Required);
    let runtime =
        runtime(bounds.clone()).with_permission_policy(Arc::new(AlwaysAllowPermissionPolicy));
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            executor.clone(),
        )
        .unwrap();

    let result = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(result, GuardedToolResult::ApprovalRequired { .. }));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn permission_spi_cannot_relax_a_denied_static_policy() {
    let bounds = policy(ApprovalPolicy::Deny);
    let runtime =
        runtime(bounds.clone()).with_permission_policy(Arc::new(AlwaysAllowPermissionPolicy));
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            executor.clone(),
        )
        .unwrap();

    let result = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "policy_denied"
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
}

struct FlippingPermissionPolicy {
    require_approval: Arc<AtomicBool>,
}

impl ToolPermissionPolicy for FlippingPermissionPolicy {
    fn contract_digest(&self) -> Digest {
        Digest::sha256("test.permission-policy/flipping/v1")
    }

    fn decide(
        &self,
        _descriptor: &ToolDescriptor,
        _operation: &ToolOperationPlan,
        _effective_policy: &EffectiveToolPolicy,
    ) -> ToolPermissionDecision {
        if self.require_approval.load(Ordering::SeqCst) {
            ToolPermissionDecision::RequireApproval
        } else {
            ToolPermissionDecision::Allow
        }
    }
}

#[tokio::test]
async fn permission_decision_cannot_flip_from_review_to_allow_on_retry() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let require_approval = Arc::new(AtomicBool::new(true));
    let runtime = runtime_with_effect_journal(bounds.clone(), journal.clone())
        .with_permission_policy(Arc::new(FlippingPermissionPolicy {
            require_approval: require_approval.clone(),
        }));
    let first_executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            first_executor.clone(),
        )
        .unwrap();

    let first = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(first, GuardedToolResult::ApprovalRequired { .. }));
    drop(runtime);

    require_approval.store(false, Ordering::SeqCst);
    let replacement = runtime_with_effect_journal(bounds.clone(), journal)
        .with_permission_policy(Arc::new(FlippingPermissionPolicy { require_approval }));
    let replacement_executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    replacement
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            replacement_executor.clone(),
        )
        .unwrap();
    let retry = replacement
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        retry,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            cached: false,
        } if code == "call_identity_conflict"
    ));
    assert_eq!(first_executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(replacement_executor.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn pre_dispatch_cancellation_stays_prepared_and_can_be_safely_retried() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let runtime = runtime_with_effect_journal(bounds.clone(), journal.clone());
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    let mut tool = descriptor(bounds.clone(), ToolConcurrency::ParallelSafe);
    tool.idempotency = ToolIdempotency::NonIdempotent;
    runtime.register(tool, executor.clone()).unwrap();
    let call = invocation("cancel-before-invoke");
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let cancelled = runtime
        .invoke(
            call.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            cancellation,
        )
        .await;
    assert!(matches!(
        cancelled,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Cancelled,
            cached: false,
        }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    let key = ToolEffectKey::new(call.run_id.clone(), call.call_id.clone());
    let records = journal.load_effect(&key).await.unwrap();
    let projection = replay_tool_effect(&key, &records).unwrap().unwrap();
    assert!(matches!(projection.phase, ToolEffectPhase::Prepared));

    let retried = runtime
        .invoke(
            call,
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        retried,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            cached: false,
        }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn guarded_shell_executes_only_after_exact_approval_inside_the_host_sandbox() {
    let workspace = std::fs::canonicalize(std::env::current_dir().unwrap())
        .unwrap()
        .to_string_lossy()
        .to_string();
    let executable = std::fs::canonicalize("/bin/echo")
        .unwrap()
        .to_string_lossy()
        .to_string();
    let declared_effects = effects(&[
        EffectScope::Process,
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::EnvironmentRead,
        EffectScope::ExternalSideEffect,
    ]);
    let bounds = ToolPolicyBounds {
        allowed_effects: declared_effects,
        approval: ApprovalPolicy::Required,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: strings(&[GUARDED_SHELL_SANDBOX_PROFILE]),
        },
        process: interactive_process(BTreeSet::from([executable.clone()])),
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([workspace.clone()]),
            writable_roots: BTreeSet::from([workspace]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(2_000),
        max_output_bytes: Some(4 * 1024),
    };
    let aliases = GuardedProgramAliases::new([("echo".to_owned(), executable.clone())]).unwrap();
    let runtime = runtime(bounds.clone());
    runtime
        .register(
            guarded_shell_descriptor_with_program_aliases(
                ToolRestriction {
                    bounds: bounds.clone(),
                },
                &aliases,
            ),
            Arc::new(GuardedShellExecutor::new(aliases)),
        )
        .unwrap();
    let invocation = ToolInvocation {
        run_id: RunId::new("guarded-shell-run"),
        call_id: ToolCallId::new("guarded-shell-call"),
        tool_id: ToolId::new("orchestral/shell_exec/v1"),
        arguments: json!({ "command": "echo", "args": ["hello"] }),
    };
    let first = runtime
        .invoke(
            invocation.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::ApprovalRequired { binding, summary } = first else {
        panic!("guarded shell must request approval before execution")
    };
    assert!(summary.contains("echo"));
    assert!(summary.contains("hello"));
    let capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let result = runtime
        .invoke(
            invocation,
            RunToolGrant { bounds },
            Some(capability),
            CancellationToken::new(),
        )
        .await;

    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            cached: false,
        } if output["stdout"] == json!("hello")
            && output["sandboxed"] == json!(true)
            && output["sandbox_backend"] == json!("macos_seatbelt")
    ));
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn one_thousand_guarded_shell_wait_cancellations_reap_processes_with_subsecond_p99() {
    const CANCELLATION_CASES: usize = 1_000;

    let parent = std::fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!("orchestral-shell-cancel-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&parent).unwrap();
    let workspace = std::fs::canonicalize(parent).unwrap();
    let marker = workspace.join("started.log");
    let pid_file = workspace.join("pid");
    let workspace_string = workspace.to_string_lossy().into_owned();
    let executable = std::fs::canonicalize("/bin/bash")
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let bounds = ToolPolicyBounds {
        allowed_effects: effects(&[
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
        ]),
        approval: ApprovalPolicy::Required,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: strings(&[GUARDED_SHELL_SANDBOX_PROFILE]),
        },
        process: interactive_process(BTreeSet::from([executable.clone()])),
        filesystem: FilesystemPolicy {
            readable_roots: strings(&[workspace_string.as_str()]),
            writable_roots: strings(&[workspace_string.as_str()]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(60_000),
        max_output_bytes: Some(4 * 1024),
    };
    let runtime = Arc::new(runtime(bounds.clone()));
    runtime
        .register(
            guarded_shell_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedShellExecutor::default()),
        )
        .unwrap();
    let script = format!(
        "printf '%s' \"$$\" > '{}'; printf x >> '{}'; while :; do :; done",
        pid_file.display(),
        marker.display()
    );
    let mut latencies = Vec::with_capacity(CANCELLATION_CASES);
    for index in 0..CANCELLATION_CASES {
        let invocation = ToolInvocation {
            run_id: RunId::new("shell-cancel-run"),
            call_id: ToolCallId::new(format!("shell-cancel-{index}")),
            tool_id: ToolId::new("orchestral/shell_exec/v1"),
            arguments: json!({
                "command": executable,
                "args": ["--noprofile", "--norc", "-c", script]
            }),
        };
        let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
            .invoke(
                invocation.clone(),
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await
        else {
            panic!("Shell cancellation gate requires exact Host approval")
        };
        let capability = HostApprovalIssuer::new(SIGNING_KEY)
            .unwrap()
            .issue(binding, i64::MAX)
            .unwrap();
        let cancellation = CancellationToken::new();
        let task = {
            let runtime = runtime.clone();
            let bounds = bounds.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                runtime
                    .invoke(
                        invocation,
                        RunToolGrant { bounds },
                        Some(capability),
                        cancellation,
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if std::fs::metadata(&marker)
                    .map(|metadata| metadata.len() >= (index + 1) as u64)
                    .unwrap_or(false)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sandboxed Shell reaches its wait boundary");

        let cancelled_at = Instant::now();
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("Shell cancellation finishes within one second")
            .unwrap();
        latencies.push(cancelled_at.elapsed());
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::UnknownEffect { .. },
                cached: false,
            }
        ));
        let pid = std::fs::read_to_string(&pid_file)
            .unwrap()
            .parse::<i32>()
            .unwrap();
        // SAFETY: signal 0 performs a read-only process existence check.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    }
    latencies.sort_unstable();
    let p99 = latencies[(CANCELLATION_CASES * 99 / 100).saturating_sub(1)];
    assert!(
        p99 <= Duration::from_secs(1),
        "Shell cancel p99 was {p99:?}"
    );

    std::fs::remove_dir_all(workspace).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn one_thousand_guarded_shell_read_cancellations_reap_pipe_holders_with_subsecond_p99() {
    const CANCELLATION_CASES: usize = 1_000;

    let parent = std::fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!("orchestral-shell-read-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&parent).unwrap();
    let workspace = std::fs::canonicalize(parent).unwrap();
    let marker = workspace.join("pipe-holder-started.log");
    let pid_file = workspace.join("pipe-holder-pid");
    let workspace_string = workspace.to_string_lossy().into_owned();
    let executable = std::fs::canonicalize("/bin/bash")
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let bounds = ToolPolicyBounds {
        allowed_effects: effects(&[
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
        ]),
        approval: ApprovalPolicy::Required,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: strings(&[GUARDED_SHELL_SANDBOX_PROFILE]),
        },
        process: interactive_process(BTreeSet::from([executable.clone()])),
        filesystem: FilesystemPolicy {
            readable_roots: strings(&[workspace_string.as_str()]),
            writable_roots: strings(&[workspace_string.as_str()]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(60_000),
        max_output_bytes: Some(4 * 1024),
    };
    let runtime = Arc::new(runtime(bounds.clone()));
    runtime
        .register(
            guarded_shell_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedShellExecutor::default()),
        )
        .unwrap();
    let script = format!(
        "(trap '' HUP; while :; do :; done) & child=$!; printf '%s' \"$child\" > '{}'; printf x >> '{}'; exit 0",
        pid_file.display(),
        marker.display()
    );
    let mut latencies = Vec::with_capacity(CANCELLATION_CASES);
    for index in 0..CANCELLATION_CASES {
        let invocation = ToolInvocation {
            run_id: RunId::new("shell-read-cancel-run"),
            call_id: ToolCallId::new(format!("shell-read-cancel-{index}")),
            tool_id: ToolId::new("orchestral/shell_exec/v1"),
            arguments: json!({
                "command": executable,
                "args": ["--noprofile", "--norc", "-c", script]
            }),
        };
        let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
            .invoke(
                invocation.clone(),
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await
        else {
            panic!("Shell read cancellation gate requires exact Host approval")
        };
        let capability = HostApprovalIssuer::new(SIGNING_KEY)
            .unwrap()
            .issue(binding, i64::MAX)
            .unwrap();
        let cancellation = CancellationToken::new();
        let task = {
            let runtime = runtime.clone();
            let bounds = bounds.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                runtime
                    .invoke(
                        invocation,
                        RunToolGrant { bounds },
                        Some(capability),
                        cancellation,
                    )
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if std::fs::metadata(&marker)
                    .map(|metadata| metadata.len() >= (index + 1) as u64)
                    .unwrap_or(false)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background pipe holder reaches the Shell read boundary");

        let cancelled_at = Instant::now();
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("Shell pipe read cancellation finishes within one second")
            .unwrap();
        latencies.push(cancelled_at.elapsed());
        assert!(
            matches!(
                &result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::UnknownEffect { .. },
                    cached: false,
                }
            ),
            "unexpected Shell read cancellation result: {result:?}"
        );
        let pid = std::fs::read_to_string(&pid_file)
            .unwrap()
            .parse::<i32>()
            .unwrap();
        // SAFETY: signal 0 performs a read-only process existence check.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    }
    latencies.sort_unstable();
    let p99 = latencies[(CANCELLATION_CASES * 99 / 100).saturating_sub(1)];
    assert!(
        p99 <= Duration::from_secs(1),
        "Shell read cancel p99 was {p99:?}"
    );

    std::fs::remove_dir_all(workspace).unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn guarded_pty_tools_are_run_scoped_and_cancel_closes_the_process() {
    let workspace = std::fs::canonicalize(std::env::current_dir().unwrap())
        .unwrap()
        .to_string_lossy()
        .to_string();
    let executable = std::fs::canonicalize("/bin/cat")
        .unwrap()
        .to_string_lossy()
        .to_string();
    let bounds = ToolPolicyBounds {
        allowed_effects: effects(&[
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
        ]),
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: false,
            allowed_profiles: strings(&[GUARDED_PTY_SANDBOX_PROFILE]),
        },
        process: interactive_process(BTreeSet::from([executable.clone()])),
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([workspace.clone()]),
            writable_roots: BTreeSet::from([workspace]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(2_000),
        max_output_bytes: Some(16 * 1024),
    };
    let aliases = GuardedProgramAliases::new([("cat".to_owned(), executable.clone())]).unwrap();
    let runtime = runtime(bounds.clone());
    let manager = Arc::new(PtyProcessManager::new(16 * 1024, Duration::from_secs(60)).unwrap());
    let restriction = || ToolRestriction {
        bounds: bounds.clone(),
    };
    runtime
        .register(
            guarded_pty_create_descriptor_with_program_aliases(restriction(), &aliases),
            Arc::new(GuardedPtyCreateExecutor::new_with_program_aliases(
                manager.clone(),
                aliases,
            )),
        )
        .unwrap();
    runtime
        .register(
            guarded_pty_write_descriptor(restriction()),
            Arc::new(GuardedPtyWriteExecutor::new(manager.clone())),
        )
        .unwrap();
    runtime
        .register(
            guarded_pty_read_descriptor(restriction()),
            Arc::new(GuardedPtyReadExecutor::new(manager.clone())),
        )
        .unwrap();
    runtime
        .register(
            guarded_pty_close_descriptor(restriction()),
            Arc::new(GuardedPtyCloseExecutor::new(manager.clone())),
        )
        .unwrap();
    runtime
        .register(
            guarded_pty_list_descriptor(restriction()),
            Arc::new(GuardedPtyListExecutor::new(manager.clone())),
        )
        .unwrap();
    let root = CancellationToken::new();
    let create = ToolInvocation {
        run_id: RunId::new("pty-run"),
        call_id: ToolCallId::new("create-1"),
        tool_id: ToolId::new("orchestral/pty_create/v1"),
        arguments: json!({ "command": "cat" }),
    };
    let GuardedToolResult::ApprovalRequired { binding, summary } = runtime
        .invoke(
            create.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            root.clone(),
        )
        .await
    else {
        panic!("PTY create must require approval")
    };
    assert!(summary.contains("cat"));
    let create_capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    let created = runtime
        .invoke(
            create,
            RunToolGrant {
                bounds: bounds.clone(),
            },
            Some(create_capability),
            root.clone(),
        )
        .await;
    assert!(matches!(
        created,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            ..
        } if output["process_id"] == json!("pty:create-1")
    ));

    let write = ToolInvocation {
        run_id: RunId::new("pty-run"),
        call_id: ToolCallId::new("write-1"),
        tool_id: ToolId::new("orchestral/pty_write/v1"),
        arguments: json!({ "process_id": "pty:create-1", "input": "hello\n" }),
    };
    let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
        .invoke(
            write.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            root.clone(),
        )
        .await
    else {
        panic!("PTY write must require approval")
    };
    let write_capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    assert!(matches!(
        runtime
            .invoke(
                write,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                Some(write_capability),
                root.clone(),
            )
            .await,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            ..
        }
    ));

    let read = runtime
        .invoke(
            ToolInvocation {
                run_id: RunId::new("pty-run"),
                call_id: ToolCallId::new("read-1"),
                tool_id: ToolId::new("orchestral/pty_read/v1"),
                arguments: json!({
                    "process_id": "pty:create-1",
                    "timeout_ms": 1_000,
                    "settle_ms": 50
                }),
            },
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            root.clone(),
        )
        .await;
    assert!(matches!(
        read,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            ..
        } if output["output"].as_str().is_some_and(|output| output.contains("hello"))
    ));

    let cross_run = runtime
        .invoke(
            ToolInvocation {
                run_id: RunId::new("other-run"),
                call_id: ToolCallId::new("cross-read"),
                tool_id: ToolId::new("orchestral/pty_read/v1"),
                arguments: json!({ "process_id": "pty:create-1", "timeout_ms": 10 }),
            },
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        cross_run,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected { ref code, .. },
            ..
        } if code == "pty_process_not_found"
    ));

    root.cancel();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if manager.list(&RunId::new("pty-run")).unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Run cancellation closes owned PTY processes");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn one_thousand_guarded_pty_reads_outside_host_roots_leak_zero_secrets() {
    const ATTEMPTS: usize = 1_000;

    let parent = std::fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!("orchestral-pty-secret-{}", uuid::Uuid::new_v4()));
    let workspace = parent.join("workspace");
    let outside = parent.join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let parent = std::fs::canonicalize(parent).unwrap();
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let outside = std::fs::canonicalize(outside).unwrap();
    let mut paths = Vec::with_capacity(ATTEMPTS);
    for index in 0..ATTEMPTS {
        let path = outside.join(format!("secret-{index}.txt"));
        std::fs::write(&path, format!("ORCHESTRAL_PTY_SENTINEL_SECRET_{index}")).unwrap();
        paths.push(path.to_string_lossy().into_owned());
    }

    let executable = std::fs::canonicalize("/bin/cat")
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let workspace_string = workspace.to_string_lossy().into_owned();
    let bounds = ToolPolicyBounds {
        allowed_effects: effects(&[
            EffectScope::Process,
            EffectScope::FilesystemRead,
            EffectScope::FilesystemWrite,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
        ]),
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: strings(&[GUARDED_PTY_SANDBOX_PROFILE]),
        },
        process: interactive_process(BTreeSet::from([executable.clone()])),
        filesystem: FilesystemPolicy {
            readable_roots: strings(&[&workspace_string]),
            writable_roots: strings(&[&workspace_string]),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(3_000),
        max_output_bytes: Some(512 * 1024),
    };
    let runtime = runtime(bounds.clone());
    let manager = Arc::new(PtyProcessManager::new(512 * 1024, Duration::from_secs(30)).unwrap());
    runtime
        .register(
            guarded_pty_create_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedPtyCreateExecutor::new(manager.clone())),
        )
        .unwrap();
    runtime
        .register(
            guarded_pty_read_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedPtyReadExecutor::new(manager.clone())),
        )
        .unwrap();

    let create = ToolInvocation {
        run_id: RunId::new("pty-secret-run"),
        call_id: ToolCallId::new("pty-secret-create"),
        tool_id: ToolId::new("orchestral/pty_create/v1"),
        arguments: json!({ "command": executable, "args": paths }),
    };
    let GuardedToolResult::ApprovalRequired { binding, .. } = runtime
        .invoke(
            create.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await
    else {
        panic!("PTY secret gate must pass through exact Host approval")
    };
    let capability = HostApprovalIssuer::new(SIGNING_KEY)
        .unwrap()
        .issue(binding, i64::MAX)
        .unwrap();
    assert!(matches!(
        runtime
            .invoke(
                create,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                Some(capability),
                CancellationToken::new(),
            )
            .await,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            ..
        }
    ));

    let read = runtime
        .invoke(
            ToolInvocation {
                run_id: RunId::new("pty-secret-run"),
                call_id: ToolCallId::new("pty-secret-read"),
                tool_id: ToolId::new("orchestral/pty_read/v1"),
                arguments: json!({
                    "process_id": "pty:pty-secret-create",
                    "timeout_ms": 2_000,
                    "settle_ms": 50
                }),
            },
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        read,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            ..
        } if output["output"]
            .as_str()
            .is_some_and(|value| !value.contains("ORCHESTRAL_PTY_SENTINEL_SECRET_"))
    ));

    let _ = manager.close(
        &RunId::new("pty-secret-run"),
        &orchestral_runtime::PtyProcessId::new("pty:pty-secret-create").unwrap(),
    );
    std::fs::remove_dir_all(parent).unwrap();
}

#[tokio::test]
async fn concurrent_and_replayed_call_executes_once_and_conflict_is_rejected() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let runtime = Arc::new(runtime(bounds.clone()));
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::from_millis(30),
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::GlobalSerial),
            executor.clone(),
        )
        .unwrap();
    let root = CancellationToken::new();

    let mut tasks = Vec::new();
    for _ in 0..12 {
        let runtime = runtime.clone();
        let grant = RunToolGrant {
            bounds: bounds.clone(),
        };
        let cancellation = root.clone();
        tasks.push(tokio::spawn(async move {
            runtime
                .invoke(invocation("hello"), grant, None, cancellation)
                .await
        }));
    }

    let mut uncached = 0;
    for task in tasks {
        match task.await.unwrap() {
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Completed { output },
                cached,
            } => {
                assert_eq!(output, json!({ "result": "hello" }).into());
                if !cached {
                    uncached += 1;
                }
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
    assert_eq!(uncached, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);

    let conflict = runtime
        .invoke(invocation("different"), RunToolGrant { bounds }, None, root)
        .await;
    let GuardedToolResult::Outcome {
        outcome: ToolOutcome::Rejected { code, .. },
        ..
    } = conflict
    else {
        panic!("expected call identity conflict");
    };
    assert_eq!(code, "call_identity_conflict");
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn committed_effect_replays_across_a_new_tool_runtime_without_execution() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let first = runtime_with_effect_journal(bounds.clone(), journal.clone());
    let first_executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    first
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            first_executor.clone(),
        )
        .unwrap();
    let first_result = first
        .invoke(
            invocation("hello"),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        first_result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { .. },
            cached: false,
        }
    ));
    assert_eq!(first_executor.calls.load(Ordering::SeqCst), 1);
    drop(first);

    let second = runtime_with_effect_journal(bounds.clone(), journal.clone());
    let second_executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    second
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            second_executor.clone(),
        )
        .unwrap();
    let replayed = second
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;

    assert!(matches!(
        replayed,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed { output: ToolOutput::Inline(ref output) },
            cached: true,
        } if output == &json!({ "result": "hello" })
    ));
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    let key = ToolEffectKey::new(RunId::new("run-1"), ToolCallId::new("call-1"));
    let projection = replay_tool_effect(&key, &journal.load_effect(&key).await.unwrap())
        .unwrap()
        .unwrap();
    assert!(matches!(
        projection.phase,
        ToolEffectPhase::Committed { .. }
    ));
}

#[tokio::test]
async fn oversized_output_is_spilled_verified_and_replayed_as_one_artifact_reference() {
    let mut bounds = policy(ApprovalPolicy::NotRequired);
    bounds.max_output_bytes = Some(64);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let artifacts =
        ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 4 * 1024, 80).unwrap();
    let first = runtime_with_artifacts(bounds.clone(), journal.clone(), artifacts.clone());
    let first_executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    first
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            first_executor.clone(),
        )
        .unwrap();
    let large = "x".repeat(512);
    let invocation = invocation(&large);
    let first_result = first
        .invoke(
            invocation.clone(),
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    let GuardedToolResult::Outcome {
        outcome:
            ToolOutcome::Completed {
                output: ToolOutput::Artifact(artifact),
            },
        cached: false,
    } = first_result
    else {
        panic!("oversized result must be replaced by one Artifact reference")
    };
    assert!(artifact.summary.contains("Preview:"));
    assert!(artifact.summary.len() < large.len());
    let bytes = artifacts.resolve(&artifact).await.unwrap();
    let resolved: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(resolved, json!({ "result": large }));
    assert_eq!(
        artifact.artifact.digest,
        orchestral_core::agent_protocol::wire::Digest::sha256(&bytes)
    );
    assert_eq!(first_executor.calls.load(Ordering::SeqCst), 1);

    let read_bounds = ToolPolicyBounds {
        allowed_effects: effects(&[EffectScope::ArtifactRead]),
        approval: ApprovalPolicy::NotRequired,
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(8 * 1024),
        ..ToolPolicyBounds::default()
    };
    let reader = runtime_with_artifacts(
        read_bounds.clone(),
        Arc::new(InMemoryToolEffectJournalStore::default()),
        artifacts.clone(),
    );
    reader
        .register(
            guarded_artifact_read_descriptor(ToolRestriction {
                bounds: read_bounds.clone(),
            }),
            Arc::new(GuardedArtifactReadExecutor::new(artifacts.clone())),
        )
        .unwrap();
    let read_result = reader
        .invoke(
            ToolInvocation {
                run_id: RunId::new("artifact-read-run"),
                call_id: ToolCallId::new("artifact-read-call"),
                tool_id: ToolId::new("orchestral/artifact_read/v1"),
                arguments: json!({
                    "artifact_ref": artifact.artifact.artifact_ref,
                    "digest": artifact.artifact.digest,
                    "media_type": artifact.media_type,
                    "byte_size": artifact.byte_size,
                    "max_bytes": 32,
                }),
            },
            RunToolGrant {
                bounds: read_bounds,
            },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        read_result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed {
                output: ToolOutput::Inline(ref output),
            },
            ..
        } if output["content"].as_str().is_some_and(|content| content.starts_with("{\"result\":\""))
            && output["complete"] == json!(false)
    ));
    drop(first);

    let second = runtime_with_artifacts(bounds.clone(), journal.clone(), artifacts.clone());
    let second_executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    second
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            second_executor.clone(),
        )
        .unwrap();
    let replayed = second
        .invoke(
            invocation,
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        replayed,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Completed {
                output: ToolOutput::Artifact(ref replayed),
            },
            cached: true,
        } if replayed == &artifact
    ));
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
    let key = ToolEffectKey::new(RunId::new("run-1"), ToolCallId::new("call-1"));
    let projection = replay_tool_effect(&key, &journal.load_effect(&key).await.unwrap())
        .unwrap()
        .unwrap();
    assert!(matches!(
        projection.phase,
        ToolEffectPhase::Committed {
            outcome: ToolOutcome::Completed {
                output: ToolOutput::Artifact(_),
            },
            ..
        }
    ));
}

#[tokio::test]
async fn artifact_put_commit_and_fail_hooks_are_fail_open_when_configured() {
    for fail_event in ["artifact.put", "artifact.commit"] {
        let (result, events) = invoke_oversized_with_artifact_hook(
            HookFailurePolicy::FailOpen,
            fail_event,
            Arc::new(InMemoryBlobStore::default()),
        )
        .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Completed {
                    output: ToolOutput::Artifact(_),
                },
                cached: false,
            }
        ));
        assert_eq!(events, ["artifact.put", "artifact.commit"]);
    }

    let (result, events) = invoke_oversized_with_artifact_hook(
        HookFailurePolicy::FailOpen,
        "artifact.fail",
        Arc::new(RejectingBlobStore),
    )
    .await;
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Failed {
                ref code,
                ref message,
                ..
            },
            cached: false,
        } if code == "artifact_persistence_failed"
            && message.contains("injected artifact write failure")
    ));
    assert_eq!(events, ["artifact.put", "artifact.fail"]);
}

#[tokio::test]
async fn artifact_put_commit_and_fail_hooks_are_fail_closed_when_configured() {
    let cases: [(&str, Arc<dyn BlobStore>, &[&str]); 3] = [
        (
            "artifact.put",
            Arc::new(InMemoryBlobStore::default()),
            &["artifact.put", "artifact.fail"],
        ),
        (
            "artifact.commit",
            Arc::new(InMemoryBlobStore::default()),
            &["artifact.put", "artifact.commit", "artifact.fail"],
        ),
        (
            "artifact.fail",
            Arc::new(RejectingBlobStore),
            &["artifact.put", "artifact.fail"],
        ),
    ];

    for (fail_event, store, expected_events) in cases {
        let (result, events) =
            invoke_oversized_with_artifact_hook(HookFailurePolicy::FailClosed, fail_event, store)
                .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Failed {
                    ref code,
                    ref message,
                    ..
                },
                cached: false,
            } if code == "artifact_persistence_failed" && message.contains(fail_event)
        ));
        assert_eq!(events, expected_events, "event mismatch for {fail_event}");
    }
}

#[tokio::test]
async fn invoked_without_observation_becomes_unknown_and_is_never_reexecuted() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let key = seed_effect_trace(&journal, &bounds, None).await;
    let runtime = runtime_with_effect_journal(bounds.clone(), journal.clone());
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            executor.clone(),
        )
        .unwrap();

    let result = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;

    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { .. },
            cached: true,
        }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    let projection = replay_tool_effect(&key, &journal.load_effect(&key).await.unwrap())
        .unwrap()
        .unwrap();
    assert!(matches!(
        projection.phase,
        ToolEffectPhase::UnknownEffect { .. }
    ));
}

#[tokio::test]
async fn observed_effect_is_committed_after_restart_without_reexecution() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let observed = ToolOutcome::Completed {
        output: json!({ "result": "hello" }).into(),
    };
    let key = seed_effect_trace(&journal, &bounds, Some(observed.clone())).await;
    let runtime = runtime_with_effect_journal(bounds.clone(), journal.clone());
    let executor = Arc::new(EchoExecutor {
        calls: AtomicUsize::new(0),
        delay: Duration::ZERO,
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
            executor.clone(),
        )
        .unwrap();

    let result = runtime
        .invoke(
            invocation("hello"),
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;

    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome,
            cached: true,
        } if outcome == observed
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    let projection = replay_tool_effect(&key, &journal.load_effect(&key).await.unwrap())
        .unwrap()
        .unwrap();
    assert!(matches!(
        projection.phase,
        ToolEffectPhase::Committed { .. }
    ));
}

#[tokio::test]
async fn four_thousand_crash_cuts_never_duplicate_or_blindly_replay_a_tool_effect() {
    const CASES_PER_CUT: usize = 500;

    let mut cuts = 0usize;
    let mut duplicate_executions = 0usize;
    let mut unknown_reexecutions = 0usize;
    for boundary in EffectCrashBoundary::ALL {
        for side in EffectCrashSide::ALL {
            for _ in 0..CASES_PER_CUT {
                let bounds = policy(ApprovalPolicy::NotRequired);
                let durable = Arc::new(InMemoryToolEffectJournalStore::default());
                let crash_journal =
                    Arc::new(CrashCutEffectJournal::new(durable.clone(), boundary, side));
                let first = runtime_with_effect_journal(bounds.clone(), crash_journal.clone());
                let first_executor = Arc::new(EchoExecutor {
                    calls: AtomicUsize::new(0),
                    delay: Duration::ZERO,
                });
                first
                    .register(
                        descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
                        first_executor.clone(),
                    )
                    .unwrap();
                let _first_result = first
                    .invoke(
                        invocation("hello"),
                        RunToolGrant {
                            bounds: bounds.clone(),
                        },
                        None,
                        CancellationToken::new(),
                    )
                    .await;
                assert!(crash_journal.fired(), "cut did not fire at {boundary:?}");
                let first_calls = first_executor.calls.load(Ordering::SeqCst);
                drop(first);

                let replacement = runtime_with_effect_journal(bounds.clone(), durable.clone());
                let replacement_executor = Arc::new(EchoExecutor {
                    calls: AtomicUsize::new(0),
                    delay: Duration::ZERO,
                });
                replacement
                    .register(
                        descriptor(bounds.clone(), ToolConcurrency::ParallelSafe),
                        replacement_executor.clone(),
                    )
                    .unwrap();
                let recovered = replacement
                    .invoke(
                        invocation("hello"),
                        RunToolGrant { bounds },
                        None,
                        CancellationToken::new(),
                    )
                    .await;
                let replacement_calls = replacement_executor.calls.load(Ordering::SeqCst);
                duplicate_executions += usize::from(first_calls + replacement_calls > 1);

                let key = ToolEffectKey::new(RunId::new("run-1"), ToolCallId::new("call-1"));
                let projection =
                    replay_tool_effect(&key, &durable.load_effect(&key).await.unwrap())
                        .unwrap()
                        .unwrap();
                let must_be_unknown = matches!(
                    (boundary, side),
                    (
                        EffectCrashBoundary::Invoked,
                        EffectCrashSide::AfterAppendBeforeAck
                    ) | (EffectCrashBoundary::Observed, EffectCrashSide::BeforeAppend)
                );
                if must_be_unknown {
                    unknown_reexecutions += replacement_calls;
                    assert!(matches!(
                        recovered,
                        GuardedToolResult::Outcome {
                            outcome: ToolOutcome::UnknownEffect { .. },
                            cached: true,
                        }
                    ));
                    assert!(matches!(
                        projection.phase,
                        ToolEffectPhase::UnknownEffect { .. }
                    ));
                } else {
                    assert!(matches!(
                        recovered,
                        GuardedToolResult::Outcome {
                            outcome: ToolOutcome::Completed { .. },
                            ..
                        }
                    ));
                    assert!(matches!(
                        projection.phase,
                        ToolEffectPhase::Committed { .. }
                    ));
                }
                cuts += 1;
            }
        }
    }

    assert_eq!(
        cuts,
        EffectCrashBoundary::ALL.len() * EffectCrashSide::ALL.len() * CASES_PER_CUT
    );
    assert_eq!(duplicate_executions, 0);
    assert_eq!(unknown_reexecutions, 0);
}

struct CancelExecutor {
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    started: Notify,
}

struct ActiveGuard(Arc<AtomicUsize>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl GuardedToolExecutor for CancelExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveGuard(self.active.clone());
        self.started.notify_one();
        execution.cancellation.cancelled().await;
        ToolOutcome::Cancelled
    }
}

#[tokio::test]
async fn run_cancellation_reaches_executor_and_closes_the_call() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let runtime = Arc::new(runtime(bounds.clone()));
    let active = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(CancelExecutor {
        calls: AtomicUsize::new(0),
        active: active.clone(),
        started: Notify::new(),
    });
    runtime
        .register(
            descriptor(bounds.clone(), ToolConcurrency::PerRunSerial),
            executor.clone(),
        )
        .unwrap();

    let root = CancellationToken::new();
    let task = {
        let runtime = runtime.clone();
        let cancellation = root.clone();
        tokio::spawn(async move {
            runtime
                .invoke(
                    invocation("wait"),
                    RunToolGrant { bounds },
                    None,
                    cancellation,
                )
                .await
        })
    };
    executor.started.notified().await;
    root.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cancellation should close the invocation")
        .unwrap();
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::Cancelled,
            cached: false,
        }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn post_dispatch_non_idempotent_cancellation_is_unknown_effect() {
    let bounds = policy(ApprovalPolicy::NotRequired);
    let runtime = Arc::new(runtime(bounds.clone()));
    let active = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(CancelExecutor {
        calls: AtomicUsize::new(0),
        active: active.clone(),
        started: Notify::new(),
    });
    let mut tool = descriptor(bounds.clone(), ToolConcurrency::PerRunSerial);
    tool.idempotency = ToolIdempotency::NonIdempotent;
    runtime.register(tool, executor.clone()).unwrap();

    let root = CancellationToken::new();
    let task = {
        let runtime = runtime.clone();
        let cancellation = root.clone();
        tokio::spawn(async move {
            runtime
                .invoke(
                    invocation("wait"),
                    RunToolGrant { bounds },
                    None,
                    cancellation,
                )
                .await
        })
    };
    executor.started.notified().await;
    root.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("cancellation should close the invocation")
        .unwrap();
    assert!(matches!(
        result,
        GuardedToolResult::Outcome {
            outcome: ToolOutcome::UnknownEffect { .. },
            cached: false,
        }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn two_thousand_five_hundred_model_sandbox_downgrades_reach_no_executor() {
    const MUTATIONS: usize = 2_500;
    let root = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
    let (bounds, executable) = strict_shell_security_bounds(&root);
    let runtime = runtime(bounds.clone());
    runtime
        .register(
            guarded_shell_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedShellExecutor::default()),
        )
        .unwrap();
    let fields = [
        "approval",
        "sandbox",
        "sandbox_mode",
        "network",
        "environment",
        "credential",
        "allowed_programs",
    ];
    for index in 0..MUTATIONS {
        let mut arguments = json!({ "command": executable });
        arguments.as_object_mut().unwrap().insert(
            fields[index % fields.len()].to_owned(),
            json!({ "required": false, "allow": "*", "index": index }),
        );
        let result = runtime
            .invoke(
                ToolInvocation {
                    run_id: RunId::new("sandbox-downgrade-run"),
                    call_id: ToolCallId::new(format!("sandbox-downgrade-{index}")),
                    tool_id: ToolId::new("orchestral/shell_exec/v1"),
                    arguments,
                },
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { ref code, .. },
                cached: false,
            } if code == "input_schema_violation"
        ));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn two_thousand_five_hundred_alternate_spawn_mutations_are_rejected_before_approval() {
    const MUTATIONS: usize = 2_500;
    let root = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
    let (bounds, _) = strict_shell_security_bounds(&root);
    let runtime = runtime(bounds.clone());
    runtime
        .register(
            guarded_shell_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedShellExecutor::default()),
        )
        .unwrap();
    for index in 0..MUTATIONS {
        let invocation = ToolInvocation {
            run_id: RunId::new("alternate-spawn-run"),
            call_id: ToolCallId::new(format!("alternate-spawn-{index}")),
            tool_id: ToolId::new("orchestral/shell_exec/v1"),
            arguments: json!({
                "command": format!("/orchestral-unapproved/bin-{index}"),
                "args": ["must-not-run"]
            }),
        };
        let result = runtime
            .invoke(
                invocation,
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { ref code, .. },
                cached: false,
            } if code == "input_schema_violation"
        ));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn two_thousand_five_hundred_symlink_escapes_read_zero_outside_bytes() {
    const MUTATIONS: usize = 2_500;
    let parent = std::fs::canonicalize(std::env::temp_dir())
        .unwrap()
        .join(format!("orchestral-symlink-gate-{}", uuid::Uuid::new_v4()));
    let workspace = parent.join("workspace");
    let outside = parent.join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let parent = std::fs::canonicalize(parent).unwrap();
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let outside = std::fs::canonicalize(outside).unwrap();
    let secret = outside.join("secret.txt");
    std::fs::write(&secret, "ORCHESTRAL_SYMLINK_SENTINEL").unwrap();
    let workspace_string = workspace.to_string_lossy().into_owned();
    let bounds = ToolPolicyBounds {
        allowed_effects: effects(&[EffectScope::FilesystemRead]),
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: false,
            allowed_profiles: strings(&["workspace_read"]),
        },
        process: ProcessPolicy::default(),
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([workspace_string]),
            writable_roots: BTreeSet::new(),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(4 * 1024),
    };
    let runtime = runtime(bounds.clone());
    runtime
        .register(
            guarded_file_read_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedFileReadExecutor::new(&workspace).unwrap()),
        )
        .unwrap();
    for index in 0..MUTATIONS {
        let link = workspace.join(format!("escape-{index}.txt"));
        std::os::unix::fs::symlink(&secret, &link).unwrap();
        let result = runtime
            .invoke(
                ToolInvocation {
                    run_id: RunId::new("symlink-escape-run"),
                    call_id: ToolCallId::new(format!("symlink-escape-{index}")),
                    tool_id: ToolId::new("orchestral/file_read/v3"),
                    arguments: json!({ "path": format!("escape-{index}.txt") }),
                },
                RunToolGrant {
                    bounds: bounds.clone(),
                },
                None,
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(
            result,
            GuardedToolResult::Outcome {
                outcome: ToolOutcome::Rejected { ref code, .. },
                cached: false,
            } if code == "workspace_path_escape"
        ));
    }
    std::fs::remove_dir_all(parent).unwrap();
}

#[tokio::test]
async fn one_thousand_each_retired_subprocess_and_http_secret_attempts_have_no_execution_route() {
    const ATTEMPTS: usize = 1_000;
    let bounds = policy(ApprovalPolicy::NotRequired);
    let runtime = runtime(bounds.clone());
    for tool_id in ["orchestral/subprocess/v1", "orchestral/http/v1"] {
        for index in 0..ATTEMPTS {
            let sentinel = format!("ORCHESTRAL_RETIRED_SENTINEL_{tool_id}_{index}");
            let result = runtime
                .invoke(
                    ToolInvocation {
                        run_id: RunId::new("retired-effect-route-run"),
                        call_id: ToolCallId::new(format!("{tool_id}-{index}")),
                        tool_id: ToolId::new(tool_id),
                        arguments: json!({
                            "command": "/bin/sh",
                            "url": "http://169.254.169.254/latest/meta-data",
                            "environment": {"SECRET": sentinel},
                            "body": sentinel
                        }),
                    },
                    RunToolGrant {
                        bounds: bounds.clone(),
                    },
                    None,
                    CancellationToken::new(),
                )
                .await;
            assert!(matches!(
                result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Rejected { ref code, .. },
                    cached: false,
                } if code == "tool_not_found"
            ));
        }
    }
}

#[tokio::test]
async fn guarded_file_read_uses_effective_roots_without_model_authority_fields() {
    let root =
        std::env::temp_dir().join(format!("orchestral-guarded-read-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let file = root.join("hello.txt");
    let expected = "alpha\n你好\nomega\n";
    std::fs::write(&file, expected).unwrap();
    let root = std::fs::canonicalize(&root).unwrap();
    let bounds = ToolPolicyBounds {
        allowed_effects: effects(&[EffectScope::FilesystemRead]),
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: false,
            allowed_profiles: strings(&["workspace_read"]),
        },
        process: ProcessPolicy::default(),
        filesystem: FilesystemPolicy {
            readable_roots: strings(&[root.to_string_lossy().as_ref()]),
            writable_roots: BTreeSet::new(),
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(64 * 1024),
    };
    let runtime = runtime(bounds.clone());
    let descriptor = guarded_file_read_descriptor(ToolRestriction {
        bounds: bounds.clone(),
    });
    assert!(descriptor.model_schema.input_schema["properties"]
        .get("approval")
        .is_none());
    assert!(descriptor.model_schema.input_schema["properties"]
        .get("sandbox_mode")
        .is_none());
    assert!(descriptor.model_schema.input_schema["properties"]
        .get("max_bytes")
        .is_none());
    assert!(descriptor.model_schema.input_schema["properties"]
        .get("offset")
        .is_some());
    runtime
        .register(
            descriptor,
            Arc::new(GuardedFileReadExecutor::new(&root).unwrap()),
        )
        .unwrap();

    let first = runtime
        .invoke(
            ToolInvocation {
                run_id: RunId::new("guarded-file-run"),
                call_id: ToolCallId::new("guarded-file-call-1"),
                tool_id: ToolId::new("orchestral/file_read/v3"),
                arguments: json!({ "path": "hello.txt", "limit": 2 }),
            },
            RunToolGrant {
                bounds: bounds.clone(),
            },
            None,
            CancellationToken::new(),
        )
        .await;
    let first = match first {
        GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(output),
                },
            ..
        } => output,
        other => panic!("first bounded read failed: {other:?}"),
    };
    assert_eq!(first["content"], json!("alpha\n你好\n"));
    assert_eq!(first["start_line"], json!(1));
    assert_eq!(first["end_line"], json!(2));
    assert_eq!(first["next_offset"], json!(3));
    assert_eq!(first["eof"], json!(false));
    assert_eq!(first["truncated"], json!(true));

    let second = runtime
        .invoke(
            ToolInvocation {
                run_id: RunId::new("guarded-file-run"),
                call_id: ToolCallId::new("guarded-file-call-2"),
                tool_id: ToolId::new("orchestral/file_read/v3"),
                arguments: json!({
                    "path": "hello.txt",
                    "offset": first["next_offset"],
                    "limit": 2
                }),
            },
            RunToolGrant { bounds },
            None,
            CancellationToken::new(),
        )
        .await;
    let second = match second {
        GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(output),
                },
            ..
        } => output,
        other => panic!("second bounded read failed: {other:?}"),
    };
    assert_eq!(
        format!(
            "{}{}",
            first["content"].as_str().unwrap(),
            second["content"].as_str().unwrap()
        ),
        expected
    );
    assert_eq!(second["truncated"], json!(false));
    assert_eq!(second["eof"], json!(true));
    assert_eq!(second["next_offset"], json!(4));
    std::fs::remove_dir_all(root).unwrap();
}
