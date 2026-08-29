use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::executor::{
    ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, Executor,
};
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::tool_effect::{
    InMemoryToolEffectJournalStore, PreparedToolEffect, ToolAuthorizationEvidence,
    ToolEffectAttemptId, ToolEffectEvent, ToolEffectEventDraft, ToolEffectEventId,
    ToolEffectJournalStore, ToolEffectKey,
};
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EffectiveToolPolicy, EnvironmentPolicy, FilesystemPolicy,
    HostApprovalVerifier, HostToolPolicy, InMemoryApprovalCapabilityStore,
    InteractiveCommandPolicy, ModelToolSchema, NetworkPolicy, ProcessPolicy, RunToolGrant,
    SandboxPolicy, ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency, ToolInvocation,
    ToolOutcome, ToolPolicyBounds, ToolRestriction, TransportLaunchPolicy,
};
use orchestral_core::types::{Plan, Step, WorkflowId};
use orchestral_runtime::{
    tool_permission_decision_digest, workflow_plan_digest, workflow_step_call_id,
    DescriptorPermissionPolicy, GuardedToolExecution, GuardedToolExecutor, GuardedToolRuntime,
    HookDispatchMode, HookError, HookExecutionPolicy, HookFailurePolicy, HookRegistry, RuntimeHook,
    RuntimeHookContext, RuntimeHookEventEnvelope, ToolPermissionDecision, WorkflowExecutionError,
    WorkflowExecutionRequest, WorkflowExecutionStrategy,
};
use serde_json::json;

const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

#[derive(Default)]
struct RecordedProgress {
    events: tokio::sync::Mutex<Vec<ExecutionProgressEvent>>,
}

#[async_trait]
impl ExecutionProgressReporter for RecordedProgress {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
        self.events.lock().await.push(event);
        Ok(())
    }
}

fn strings(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn effects(values: &[EffectScope]) -> BTreeSet<EffectScope> {
    values.iter().copied().collect()
}

fn bounds(approval: ApprovalPolicy) -> ToolPolicyBounds {
    ToolPolicyBounds {
        allowed_effects: effects(&[EffectScope::Process]),
        approval,
        sandbox: SandboxPolicy {
            required: false,
            allowed_profiles: strings(&["strict"]),
        },
        process: ProcessPolicy {
            interactive: InteractiveCommandPolicy {
                enabled: true,
                command_shells: strings(&["echo"]),
                allow_child_processes: false,
            },
            transport: TransportLaunchPolicy::default(),
        },
        filesystem: FilesystemPolicy::default(),
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy::default(),
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(1_000),
        max_output_bytes: Some(1_024),
    }
}

struct GuardedEcho {
    calls: AtomicUsize,
}

struct RetryOnceEcho {
    calls: AtomicUsize,
}

#[async_trait]
impl GuardedToolExecutor for GuardedEcho {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
        }
    }
}

#[async_trait]
impl GuardedToolExecutor for RetryOnceEcho {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            ToolOutcome::Failed {
                code: "retry_once".to_owned(),
                message: "retry the logical Step once".to_owned(),
                retryable: true,
            }
        } else {
            ToolOutcome::Completed {
                output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
            }
        }
    }
}

struct StepHookProbe {
    fail_event: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl RuntimeHook for StepHookProbe {
    fn id(&self) -> &'static str {
        "step_hook_probe"
    }

    async fn on_event(
        &self,
        event: &RuntimeHookEventEnvelope,
        _context: &RuntimeHookContext,
    ) -> Result<(), HookError> {
        self.events
            .lock()
            .expect("step hook events lock")
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

struct HookOutcomeTool {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

#[async_trait]
impl GuardedToolExecutor for HookOutcomeTool {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            ToolOutcome::Failed {
                code: "injected_tool_failure".to_owned(),
                message: "injected guarded Tool failure".to_owned(),
                retryable: false,
            }
        } else {
            ToolOutcome::Completed {
                output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
            }
        }
    }
}

async fn run_step_hook_case(
    failure_policy: HookFailurePolicy,
    fail_event: &'static str,
    tool_fails: bool,
) -> (ExecutionResult, Vec<String>, usize) {
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
        .register(Arc::new(StepHookProbe {
            fail_event,
            events: events.clone(),
        }))
        .await;

    let host_bounds = bounds(ApprovalPolicy::NotRequired);
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
            .expect("valid signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: host_bounds.clone(),
            },
            verifier,
        )
        .expect("valid Host Tool runtime"),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    runtime
        .register(
            ToolDescriptor {
                tool_id: ToolId::new("test/hook-tool"),
                model_schema: ModelToolSchema {
                    name: "hook_tool".to_owned(),
                    description: "Exercise Step hooks".to_owned(),
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
                restriction: ToolRestriction {
                    bounds: host_bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            Arc::new(HookOutcomeTool {
                calls: calls.clone(),
                fail: tool_fails,
            }),
        )
        .expect("Tool registers");
    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action("hook_tool");
    let strategy =
        WorkflowExecutionStrategy::new(Arc::new(normalizer), Arc::new(Executor::new()), runtime)
            .with_hooks(hooks);
    let snapshot = strategy
        .execute(
            WorkflowExecutionRequest::new(
                RunId::new(format!("hook-{fail_event}")),
                WorkflowId::new("hook-task"),
                Plan::new(
                    "hook workflow",
                    vec![Step::action("hook-step", "hook_tool")
                        .with_params(json!({ "value": "hello" }))
                        .with_exports(vec!["result".to_owned()])],
                ),
                RunToolGrant {
                    bounds: host_bounds,
                },
            )
            .with_progress_reporter(Arc::new(RecordedProgress::default())),
        )
        .await
        .expect("hook workflow is structurally valid");
    let recorded = events.lock().expect("step hook events lock").clone();
    (snapshot.result, recorded, calls.load(Ordering::SeqCst))
}

#[tokio::test]
async fn workflow_dag_uses_guarded_tools_as_its_only_execution_boundary() {
    let host_bounds = bounds(ApprovalPolicy::NotRequired);
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
            .expect("valid signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new(
            HostToolPolicy {
                bounds: host_bounds.clone(),
            },
            verifier,
        )
        .expect("valid Host Tool runtime"),
    );
    let guarded = Arc::new(GuardedEcho {
        calls: AtomicUsize::new(0),
    });
    runtime
        .register(
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
                restriction: ToolRestriction {
                    bounds: host_bounds.clone(),
                },
                idempotency: ToolIdempotency::IdempotentWithKey,
                concurrency: ToolConcurrency::ParallelSafe,
            },
            guarded.clone(),
        )
        .expect("Tool registers");

    let executor = Arc::new(Executor::new());
    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action("echo");
    let strategy = WorkflowExecutionStrategy::new(Arc::new(normalizer), executor, runtime);
    let denied_progress = Arc::new(RecordedProgress::default());
    let allowed_progress = Arc::new(RecordedProgress::default());
    let plan = Plan::new(
        "echo through a guarded DAG",
        vec![Step::action("echo-step", "echo")
            .with_params(json!({ "value": "hello" }))
            .with_exports(vec!["result".to_owned()])],
    );

    let denied = strategy
        .execute(
            WorkflowExecutionRequest::new(
                RunId::new("workflow-denied"),
                WorkflowId::new("task-denied"),
                plan.clone(),
                RunToolGrant {
                    bounds: bounds(ApprovalPolicy::Deny),
                },
            )
            .with_progress_reporter(denied_progress),
        )
        .await
        .expect("denial is an execution result, not a strategy crash");
    assert!(matches!(denied.result, ExecutionResult::Failed { .. }));
    assert_eq!(denied.tool_calls, 1);
    assert_eq!(guarded.calls.load(Ordering::SeqCst), 0);

    let allowed = strategy
        .execute(
            WorkflowExecutionRequest::new(
                RunId::new("workflow-allowed"),
                WorkflowId::new("task-allowed"),
                plan,
                RunToolGrant {
                    bounds: bounds(ApprovalPolicy::NotRequired),
                },
            )
            .with_progress_reporter(allowed_progress.clone()),
        )
        .await
        .expect("allowed guarded workflow executes");
    assert!(matches!(allowed.result, ExecutionResult::Completed));
    assert_eq!(allowed.tool_calls, 1);
    assert_eq!(allowed.working_set.get("result"), Some(&json!("hello")));
    assert_eq!(guarded.calls.load(Ordering::SeqCst), 1);
    let phases = allowed_progress
        .events
        .lock()
        .await
        .iter()
        .map(|event| event.phase.clone())
        .collect::<Vec<_>>();
    assert!(phases.iter().any(|phase| phase == "step_started"));
    assert!(phases.iter().any(|phase| phase == "step_completed"));
    assert!(phases.iter().any(|phase| phase == "workflow_completed"));
}

#[tokio::test]
async fn step_lifecycle_hooks_are_fail_open_when_configured() {
    for fail_event in ["before_step", "after_step"] {
        let (result, events, calls) =
            run_step_hook_case(HookFailurePolicy::FailOpen, fail_event, false).await;
        assert!(matches!(result, ExecutionResult::Completed));
        assert_eq!(events, ["before_step", "after_step"]);
        assert_eq!(calls, 1);
    }

    let (result, events, calls) =
        run_step_hook_case(HookFailurePolicy::FailOpen, "on_step_error", true).await;
    assert!(matches!(result, ExecutionResult::Failed { .. }));
    assert_eq!(events, ["before_step", "after_step", "on_step_error"]);
    assert_eq!(calls, 1);
}

#[tokio::test]
async fn step_lifecycle_hooks_are_fail_closed_when_configured() {
    let (result, events, calls) =
        run_step_hook_case(HookFailurePolicy::FailClosed, "before_step", false).await;
    assert!(matches!(
        result,
        ExecutionResult::Failed { ref error, .. } if error.contains("before_step")
    ));
    assert_eq!(events, ["before_step", "on_step_error"]);
    assert_eq!(calls, 0, "before_step fail-closed must fence the Tool");

    let (result, events, calls) =
        run_step_hook_case(HookFailurePolicy::FailClosed, "after_step", false).await;
    assert!(matches!(
        result,
        ExecutionResult::Failed { ref error, .. } if error.contains("after_step")
    ));
    assert_eq!(events, ["before_step", "after_step", "on_step_error"]);
    assert_eq!(calls, 1);

    let (result, events, calls) =
        run_step_hook_case(HookFailurePolicy::FailClosed, "on_step_error", true).await;
    assert!(matches!(
        result,
        ExecutionResult::Failed { ref error, .. } if error.contains("on_step_error")
    ));
    assert_eq!(events, ["before_step", "after_step", "on_step_error"]);
    assert_eq!(calls, 1);
}

fn durable_echo_runtime(
    host_bounds: &ToolPolicyBounds,
    journal: Arc<InMemoryToolEffectJournalStore>,
    executor: Arc<dyn GuardedToolExecutor>,
) -> (
    Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>>,
    ToolDescriptor,
) {
    let verifier =
        HostApprovalVerifier::new(SIGNING_KEY, InMemoryApprovalCapabilityStore::default())
            .expect("valid signing key");
    let runtime = Arc::new(
        GuardedToolRuntime::new_with_effect_journal(
            HostToolPolicy {
                bounds: host_bounds.clone(),
            },
            verifier,
            journal,
        )
        .expect("durable Tool runtime is valid"),
    );
    let descriptor = ToolDescriptor {
        tool_id: ToolId::new("test/durable-echo"),
        model_schema: ModelToolSchema {
            name: "echo".to_owned(),
            description: "Echo one durable value".to_owned(),
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
        restriction: ToolRestriction {
            bounds: host_bounds.clone(),
        },
        idempotency: ToolIdempotency::IdempotentWithKey,
        concurrency: ToolConcurrency::ParallelSafe,
    };
    runtime
        .register(descriptor.clone(), executor)
        .expect("durable echo Tool registers");
    (runtime, descriptor)
}

fn durable_workflow_strategy(
    runtime: Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>>,
) -> WorkflowExecutionStrategy {
    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action("echo");
    WorkflowExecutionStrategy::new(
        Arc::new(normalizer),
        Arc::new(Executor::new().with_retry_policy(
            3,
            std::time::Duration::ZERO,
            std::time::Duration::ZERO,
        )),
        runtime,
    )
}

#[tokio::test]
async fn recovery_replay_reuses_committed_step_effects_with_stable_call_ids() {
    let host_bounds = bounds(ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let first_executor = Arc::new(GuardedEcho {
        calls: AtomicUsize::new(0),
    });
    let (first_runtime, _) =
        durable_echo_runtime(&host_bounds, journal.clone(), first_executor.clone());
    let plan = Plan::new(
        "durable replay",
        vec![Step::action("echo-step", "echo")
            .with_params(json!({ "value": "stable" }))
            .with_exports(vec!["result".to_owned()])],
    );
    let run_id = RunId::new("durable-workflow-run");
    let workflow_id = WorkflowId::new("durable-workflow");
    let first = durable_workflow_strategy(first_runtime)
        .execute(
            WorkflowExecutionRequest::new(
                run_id.clone(),
                workflow_id.clone(),
                plan.clone(),
                RunToolGrant {
                    bounds: host_bounds.clone(),
                },
            )
            .with_progress_reporter(Arc::new(RecordedProgress::default())),
        )
        .await
        .expect("first Workflow execution completes");
    assert!(matches!(first.result, ExecutionResult::Completed));
    assert_eq!(first_executor.calls.load(Ordering::SeqCst), 1);

    let replacement_executor = Arc::new(GuardedEcho {
        calls: AtomicUsize::new(0),
    });
    let (replacement_runtime, _) =
        durable_echo_runtime(&host_bounds, journal, replacement_executor.clone());
    let replayed = durable_workflow_strategy(replacement_runtime)
        .execute(
            WorkflowExecutionRequest::new(
                run_id,
                workflow_id,
                plan,
                RunToolGrant {
                    bounds: host_bounds,
                },
            )
            .with_progress_reporter(Arc::new(RecordedProgress::default()))
            .with_recovery_replay(),
        )
        .await
        .expect("committed Workflow effects replay");
    assert!(matches!(replayed.result, ExecutionResult::Completed));
    assert_eq!(replayed.tool_calls, 1);
    assert_eq!(replacement_executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(replayed.working_set.get("result"), Some(&json!("stable")));
}

#[tokio::test]
async fn recovery_replay_preserves_logical_retry_attempt_identities() {
    let host_bounds = bounds(ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let first_executor = Arc::new(RetryOnceEcho {
        calls: AtomicUsize::new(0),
    });
    let (first_runtime, _) =
        durable_echo_runtime(&host_bounds, journal.clone(), first_executor.clone());
    let plan = Plan::new(
        "durable retry replay",
        vec![Step::action("retry-step", "echo")
            .with_params(json!({ "value": "after-retry" }))
            .with_exports(vec!["result".to_owned()])],
    );
    let run_id = RunId::new("durable-retry-run");
    let workflow_id = WorkflowId::new("durable-retry-workflow");
    let first = durable_workflow_strategy(first_runtime)
        .execute(
            WorkflowExecutionRequest::new(
                run_id.clone(),
                workflow_id.clone(),
                plan.clone(),
                RunToolGrant {
                    bounds: host_bounds.clone(),
                },
            )
            .with_progress_reporter(Arc::new(RecordedProgress::default())),
        )
        .await
        .expect("retrying Workflow completes");
    assert!(matches!(first.result, ExecutionResult::Completed));
    assert_eq!(first.tool_calls, 2);
    assert_eq!(first_executor.calls.load(Ordering::SeqCst), 2);

    let replacement_executor = Arc::new(GuardedEcho {
        calls: AtomicUsize::new(0),
    });
    let (replacement_runtime, _) =
        durable_echo_runtime(&host_bounds, journal, replacement_executor.clone());
    let replayed = durable_workflow_strategy(replacement_runtime)
        .execute(
            WorkflowExecutionRequest::new(
                run_id,
                workflow_id,
                plan,
                RunToolGrant {
                    bounds: host_bounds,
                },
            )
            .with_progress_reporter(Arc::new(RecordedProgress::default()))
            .with_recovery_replay(),
        )
        .await
        .expect("both durable retry attempts replay");
    assert!(matches!(replayed.result, ExecutionResult::Completed));
    assert_eq!(replayed.tool_calls, 2);
    assert_eq!(replacement_executor.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replayed.working_set.get("result"),
        Some(&json!("after-retry"))
    );
}

#[tokio::test]
async fn recovery_preflight_blocks_all_siblings_when_one_effect_is_unresolved() {
    let host_bounds = bounds(ApprovalPolicy::NotRequired);
    let run_grant = RunToolGrant {
        bounds: host_bounds.clone(),
    };
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let executor = Arc::new(GuardedEcho {
        calls: AtomicUsize::new(0),
    });
    let (runtime, descriptor) =
        durable_echo_runtime(&host_bounds, journal.clone(), executor.clone());
    let run_id = RunId::new("unknown-workflow-run");
    let workflow_id = WorkflowId::new("unknown-workflow");
    let step_a = Step::action("a", "echo")
        .with_params(json!({ "value": "a" }))
        .with_exports(vec!["result".to_owned()]);
    let step_b = Step::action("b", "echo")
        .with_params(json!({ "value": "b" }))
        .with_exports(vec!["result".to_owned()]);
    let plan = Plan::new("unknown recovery", vec![step_a.clone(), step_b.clone()]);
    let plan_digest = workflow_plan_digest(&plan).expect("normalized Plan digest");
    let call_a = workflow_step_call_id(&run_id, &workflow_id, &plan_digest, &step_a.id, 1);
    let invocation = ToolInvocation {
        run_id: run_id.clone(),
        call_id: call_a.clone(),
        tool_id: descriptor.tool_id.clone(),
        arguments: step_a.params.clone(),
    };
    let effective_policy = EffectiveToolPolicy::resolve(
        &HostToolPolicy {
            bounds: host_bounds.clone(),
        },
        &run_grant,
        &descriptor.restriction,
    )
    .expect("effective policy is valid");
    let operation = executor
        .plan_operation(&invocation, &descriptor, &effective_policy)
        .expect("operation plan is valid");
    let prepared = PreparedToolEffect {
        args_digest: invocation.args_digest().expect("arguments digest"),
        operation_digest: operation.digest().expect("operation digest"),
        permission_digest: tool_permission_decision_digest(
            &DescriptorPermissionPolicy,
            &ToolPermissionDecision::Allow,
        )
        .expect("permission digest"),
        policy_digest: effective_policy.digest().expect("policy digest"),
        descriptor_digest: descriptor.digest().expect("descriptor digest"),
        idempotency: descriptor.idempotency,
        effect_scopes: operation.required_capabilities.effects,
        invocation,
    };
    let key_a = ToolEffectKey::new(run_id.clone(), call_a);
    journal
        .append(
            0,
            ToolEffectEventDraft {
                event_id: ToolEffectEventId::new("unknown-a-prepared"),
                key: key_a.clone(),
                payload: ToolEffectEvent::Prepared { effect: prepared },
            },
        )
        .await
        .expect("Prepared effect is durable");
    journal
        .append(
            1,
            ToolEffectEventDraft {
                event_id: ToolEffectEventId::new("unknown-a-invoked"),
                key: key_a,
                payload: ToolEffectEvent::Invoked {
                    attempt_id: ToolEffectAttemptId::new("unknown-a-attempt"),
                    authorization: ToolAuthorizationEvidence::Policy,
                },
            },
        )
        .await
        .expect("Invoked effect is durable");

    let error = durable_workflow_strategy(runtime)
        .execute(
            WorkflowExecutionRequest::new(run_id.clone(), workflow_id.clone(), plan, run_grant)
                .with_progress_reporter(Arc::new(RecordedProgress::default()))
                .with_recovery_replay(),
        )
        .await
        .expect_err("an unresolved effect blocks the whole Workflow replay");
    assert!(matches!(
        error,
        WorkflowExecutionError::UnknownEffect { .. }
    ));
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    let sibling_call = workflow_step_call_id(&run_id, &workflow_id, &plan_digest, &step_b.id, 1);
    let sibling_key = ToolEffectKey::new(run_id, sibling_call);
    assert!(journal
        .load_effect(&sibling_key)
        .await
        .expect("sibling effect lookup succeeds")
        .is_empty());
}
