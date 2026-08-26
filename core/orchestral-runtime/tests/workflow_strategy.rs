use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::executor::{
    ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, Executor,
};
use orchestral_core::normalizer::PlanNormalizer;
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalVerifier,
    HostToolPolicy, InMemoryApprovalCapabilityStore, ModelToolSchema, NetworkPolicy, ProcessPolicy,
    RunToolGrant, SandboxPolicy, ToolConcurrency, ToolDescriptor, ToolId, ToolIdempotency,
    ToolOutcome, ToolPolicyBounds, ToolRestriction,
};
use orchestral_core::types::{Plan, Step, WorkflowId};
use orchestral_runtime::{
    GuardedToolExecution, GuardedToolExecutor, GuardedToolRuntime, HookDispatchMode, HookError,
    HookExecutionPolicy, HookFailurePolicy, HookRegistry, RuntimeHook, RuntimeHookContext,
    RuntimeHookEventEnvelope, WorkflowExecutionRequest, WorkflowExecutionStrategy,
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
            allowed_programs: strings(&["echo"]),
            allow_shell_expression: false,
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

#[async_trait]
impl GuardedToolExecutor for GuardedEcho {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ToolOutcome::Completed {
            output: json!({ "result": execution.invocation.arguments["value"].clone() }).into(),
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
