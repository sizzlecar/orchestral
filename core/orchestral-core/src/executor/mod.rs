//! Executor module
//!
//! The Executor is responsible for:
//! - DAG-based topological scheduling
//! - Parallel execution of ready nodes
//! - State-driven execution

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

mod action_runner;
mod dag;
mod logging;
mod progress;
mod run;
mod schema;
mod step_support;
mod types;

pub use self::action_runner::{
    execute_action_with_registry, execute_action_with_registry_with_options,
};
pub use self::dag::{DagNode, ExecutionDag, NodeState};
pub use self::types::{
    ActionExecutionOptions, ActionPreflightHook, ActionRegistry, AgentStepExecutor,
    ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, ExecutorContext,
};

use self::logging::{
    truncate_for_log, truncate_json_for_log, MAX_LOG_JSON_CHARS, MAX_LOG_TEXT_CHARS,
};
use self::progress::{
    build_step_completion_metadata, build_step_start_metadata, choose_terminal_result,
    report_progress,
};
use self::step_support::{bind_param_value, resolve_param_templates, validate_declared_exports};

const DEFAULT_MAX_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

/// The executor - orchestrates DAG execution
pub struct Executor {
    /// Action registry
    pub action_registry: Arc<RwLock<ActionRegistry>>,
    /// Maximum parallel executions
    pub max_parallel: usize,
    /// Max retries for ActionResult::RetryableError (excluding initial attempt).
    pub max_retry_attempts: u32,
    /// Base delay for exponential backoff when action does not provide retry_after.
    pub retry_base_delay: Duration,
    /// Cap for exponential backoff delay.
    pub retry_max_delay: Duration,
    /// Whether declared exports are required at runtime.
    pub strict_exports: bool,
    /// Optional runtime hook for handling agent steps.
    pub agent_step_executor: Option<Arc<dyn AgentStepExecutor>>,
    /// Optional execution options applied to all non-agent action executions.
    pub action_execution_options: ActionExecutionOptions,
}

impl Executor {
    /// Create a new executor
    pub fn new(action_registry: ActionRegistry) -> Self {
        Self::with_registry(Arc::new(RwLock::new(action_registry)))
    }

    /// Create a new executor with a shared registry
    pub fn with_registry(action_registry: Arc<RwLock<ActionRegistry>>) -> Self {
        Self {
            action_registry,
            max_parallel: 4,
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            retry_base_delay: DEFAULT_RETRY_BASE_DELAY,
            retry_max_delay: DEFAULT_RETRY_MAX_DELAY,
            strict_exports: true,
            agent_step_executor: None,
            action_execution_options: ActionExecutionOptions::default(),
        }
    }

    /// Set maximum parallel executions
    pub fn with_max_parallel(mut self, max: usize) -> Self {
        self.max_parallel = max;
        self
    }

    /// Configure retry policy for retryable action errors.
    pub fn with_retry_policy(
        mut self,
        max_retry_attempts: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Self {
        self.max_retry_attempts = max_retry_attempts;
        self.retry_base_delay = retry_base_delay;
        self.retry_max_delay = retry_max_delay.max(retry_base_delay);
        self
    }

    /// Configure strict runtime checks for step exports.
    pub fn with_export_contract(mut self, strict_exports: bool) -> Self {
        self.strict_exports = strict_exports;
        self
    }

    /// Configure runtime-provided executor for `StepKind::Agent`.
    pub fn with_agent_step_executor(mut self, agent_executor: Arc<dyn AgentStepExecutor>) -> Self {
        self.agent_step_executor = Some(agent_executor);
        self
    }

    /// Configure common action execution options.
    pub fn with_action_execution_options(mut self, options: ActionExecutionOptions) -> Self {
        self.action_execution_options = options;
        self
    }

    /// Configure a preflight hook for non-agent action execution.
    pub fn with_action_preflight_hook(mut self, hook: ActionPreflightHook) -> Self {
        self.action_execution_options.preflight_hook = Some(hook);
        self
    }

    /// Execute a DAG
    pub async fn execute(&self, dag: &mut ExecutionDag, ctx: &ExecutorContext) -> ExecutionResult {
        loop {
            let ready: Vec<_> = dag.ready_nodes.clone();
            if ready.is_empty() {
                return self.resolve_no_ready_nodes(dag, ctx).await;
            }

            let batch: Vec<_> = ready.into_iter().take(self.max_parallel).collect();
            if let Some(waiting_result) = self.handle_wait_steps(dag, &batch, ctx).await {
                return waiting_result;
            }

            if let Some(result) = self.execute_batch(dag, batch, ctx).await {
                return result;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tokio::time::{sleep, Duration};

    use crate::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
    use crate::store::WorkingSet;
    use crate::types::{Plan, Step, StepId, StepIoBinding};

    struct CollectProgressReporter {
        events: Arc<RwLock<Vec<ExecutionProgressEvent>>>,
    }

    impl CollectProgressReporter {
        fn new() -> Self {
            Self {
                events: Arc::new(RwLock::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl ExecutionProgressReporter for CollectProgressReporter {
        async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
            self.events.write().await.push(event);
            Ok(())
        }
    }

    struct StaticAction {
        name: String,
        result: ActionResult,
        metadata: Option<ActionMeta>,
    }

    impl StaticAction {
        fn new(name: &str, result: ActionResult) -> Self {
            Self {
                name: name.to_string(),
                result,
                metadata: None,
            }
        }

        fn with_metadata(mut self, metadata: ActionMeta) -> Self {
            self.metadata = Some(metadata);
            self
        }
    }

    #[async_trait]
    impl Action for StaticAction {
        fn name(&self) -> &str {
            &self.name
        }

        fn description(&self) -> &str {
            "test action"
        }

        fn metadata(&self) -> ActionMeta {
            self.metadata
                .clone()
                .unwrap_or_else(|| ActionMeta::new(self.name(), self.description()))
        }

        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            self.result.clone()
        }
    }

    struct ConsumeContentAction;

    #[async_trait]
    impl Action for ConsumeContentAction {
        fn name(&self) -> &str {
            "consume_content"
        }

        fn description(&self) -> &str {
            "consume bound content"
        }

        async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
            match input.params.get("content").and_then(|v| v.as_str()) {
                Some(content) => {
                    ActionResult::success_with_one("written", Value::String(content.to_string()))
                }
                None => ActionResult::error("content binding not applied"),
            }
        }
    }

    struct EchoParamsAction;

    #[async_trait]
    impl Action for EchoParamsAction {
        fn name(&self) -> &str {
            "echo_params"
        }

        fn description(&self) -> &str {
            "echo params"
        }

        async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
            ActionResult::success_with_one("params", input.params.clone())
        }
    }

    struct SlowAction {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        delay_ms: u64,
    }

    impl SlowAction {
        fn new(active: Arc<AtomicUsize>, peak: Arc<AtomicUsize>, delay_ms: u64) -> Self {
            Self {
                active,
                peak,
                delay_ms,
            }
        }
    }

    struct FlakyRetryAction {
        failures_left: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
    }

    impl FlakyRetryAction {
        fn new(failures_left: Arc<AtomicUsize>, calls: Arc<AtomicUsize>) -> Self {
            Self {
                failures_left,
                calls,
            }
        }
    }

    struct StaticAgentExecutor {
        result: ActionResult,
    }

    #[async_trait]
    impl AgentStepExecutor for StaticAgentExecutor {
        async fn execute_agent_step(
            &self,
            _step: &Step,
            _resolved_params: Value,
            _execution_id: &str,
            _ctx: &ExecutorContext,
            _action_registry: Arc<RwLock<ActionRegistry>>,
        ) -> ActionResult {
            self.result.clone()
        }
    }

    #[async_trait]
    impl Action for FlakyRetryAction {
        fn name(&self) -> &str {
            "flaky_retry"
        }

        fn description(&self) -> &str {
            "returns retryable errors before succeeding"
        }

        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            let current_call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let left = self.failures_left.load(Ordering::SeqCst);
            if left > 0 {
                self.failures_left.fetch_sub(1, Ordering::SeqCst);
                return ActionResult::retryable("temporary failure", None, current_call as u32);
            }
            ActionResult::success_with_one("ok", json!(current_call))
        }
    }

    #[async_trait]
    impl Action for SlowAction {
        fn name(&self) -> &str {
            "slow"
        }

        fn description(&self) -> &str {
            "slow action for parallelism tests"
        }

        async fn run(&self, _input: ActionInput, _ctx: ActionContext) -> ActionResult {
            let in_flight = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let mut peak = self.peak.load(Ordering::SeqCst);
            while in_flight > peak {
                match self.peak.compare_exchange(
                    peak,
                    in_flight,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }
            sleep(Duration::from_millis(self.delay_ms)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            ActionResult::success()
        }
    }

    #[test]
    fn test_io_binding_injects_input_for_downstream_step() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success_with_one("guide_markdown", json!("hello world")),
            )));
            registry.register(Arc::new(ConsumeContentAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "binding flow",
                vec![
                    Step::action("s1", "produce").with_exports(vec!["guide_markdown".to_string()]),
                    Step::action("s2", "consume_content")
                        .with_depends_on(vec![StepId::from("s1")])
                        .with_io_bindings(vec![StepIoBinding::required(
                            "s1.guide_markdown",
                            "content",
                        )])
                        .with_exports(vec!["written".to_string()]),
                ],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let working_set = Arc::new(RwLock::new(WorkingSet::new()));
            let ctx = ExecutorContext::new("task-1", working_set.clone());

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));

            let ws = working_set.read().await;
            assert_eq!(ws.get_task("s2.written"), Some(&json!("hello world")));
        });
    }

    #[test]
    fn test_missing_required_binding_fails_step() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(ConsumeContentAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "missing binding",
                vec![Step::action("s1", "consume_content")
                    .with_io_bindings(vec![StepIoBinding::required(
                        "missing.step_output",
                        "content",
                    )])
                    .with_exports(vec!["written".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("Missing required io binding"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_executor_resolves_param_templates_from_working_set() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(EchoParamsAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "template params",
                vec![Step::action("s1", "echo_params")
                    .with_params(json!({
                        "path": "{{artifact.path}}",
                        "message": "open {{artifact.path}}",
                        "payload": "{{artifact.meta}}",
                        "paths": "{{artifact.paths}}"
                    }))
                    .with_exports(vec!["params".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ws = Arc::new(RwLock::new(WorkingSet::new()));
            {
                let mut guard = ws.write().await;
                guard.set_task("artifact.path", json!("docs/sample.xlsx"));
                guard.set_task("artifact.meta", json!({"rows": 3}));
                guard.set_task("artifact.paths", json!(["docs/a.md", "docs/b.md"]));
            }
            let ctx = ExecutorContext::new("task-1", ws.clone());

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));

            let ws = ws.read().await;
            let params = ws.get_task("s1.params").expect("params export");
            assert_eq!(params["path"], json!("docs/sample.xlsx"));
            assert_eq!(params["message"], json!("open docs/sample.xlsx"));
            assert_eq!(params["payload"], json!({"rows": 3}));
            assert_eq!(params["paths"], json!(["docs/a.md", "docs/b.md"]));
        });
    }

    #[test]
    fn test_executor_resolves_param_templates_from_io_bound_params() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(EchoParamsAction));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "io-bound template params",
                vec![Step::action("s1", "echo_params")
                    .with_io_bindings(vec![
                        StepIoBinding::required("artifact.path", "file_path"),
                        StepIoBinding::required("artifact.paths", "source_paths"),
                    ])
                    .with_params(json!({
                        "path": "{{file_path}}",
                        "message": "open {{file_path}}",
                        "sources": "{{source_paths}}"
                    }))
                    .with_exports(vec!["params".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ws = Arc::new(RwLock::new(WorkingSet::new()));
            {
                let mut guard = ws.write().await;
                guard.set_task("artifact.path", json!("docs/sample.xlsx"));
                guard.set_task("artifact.paths", json!(["docs/a.md", "docs/b.md"]));
            }
            let ctx = ExecutorContext::new("task-1", ws.clone());

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));

            let ws = ws.read().await;
            let params = ws.get_task("s1.params").expect("params export");
            assert_eq!(params["path"], json!("docs/sample.xlsx"));
            assert_eq!(params["message"], json!("open docs/sample.xlsx"));
            assert_eq!(params["sources"], json!(["docs/a.md", "docs/b.md"]));
        });
    }

    #[test]
    fn test_missing_declared_export_fails_step_when_strict() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success(),
            )));
            let executor = Executor::new(registry).with_export_contract(true);

            let plan = Plan::new(
                "strict exports",
                vec![Step::action("s1", "produce").with_exports(vec!["result".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("missing declared export"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_input_schema_validation_fails_on_wrong_type() {
        tokio_test::block_on(async {
            let action = StaticAction::new("typed_input", ActionResult::success()).with_metadata(
                ActionMeta::new("typed_input", "typed input").with_input_schema(json!({
                    "type": "object",
                    "properties": {
                        "count": { "type": "integer" }
                    },
                    "required": ["count"]
                })),
            );

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(action));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "typed input fail",
                vec![Step::action("s1", "typed_input").with_params(json!({
                    "count": "not-integer"
                }))],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("input schema validation failed"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_action_preflight_hook_rejects_step_before_action_run() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new("noop", ActionResult::success())));
            let executor = Executor::new(registry).with_action_preflight_hook(Arc::new(
                |action_name: &str, _params: &Value| {
                    if action_name == "noop" {
                        Some("blocked by preflight policy".to_string())
                    } else {
                        None
                    }
                },
            ));

            let plan = Plan::new("preflight deny", vec![Step::action("s1", "noop")]);
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("failed preflight"));
                    assert!(error.contains("blocked by preflight policy"));
                }
                other => panic!("expected failed result, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_output_schema_validation_fails_on_wrong_type() {
        tokio_test::block_on(async {
            let action = StaticAction::new(
                "typed_output",
                ActionResult::success_with_one("bytes", json!("wrong")),
            )
            .with_metadata(
                ActionMeta::new("typed_output", "typed output").with_output_schema(json!({
                    "type": "object",
                    "properties": {
                        "bytes": { "type": "integer" }
                    },
                    "required": ["bytes"]
                })),
            );

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(action));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "typed output fail",
                vec![Step::action("s1", "typed_output")],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("output schema validation failed"));
                }
                _ => panic!("expected failed result"),
            }
        });
    }

    #[test]
    fn test_progress_reporter_receives_step_lifecycle_events() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new(
                "produce",
                ActionResult::success_with_one("result", json!("ok")),
            )));
            let executor = Executor::new(registry);

            let plan = Plan::new(
                "progress flow",
                vec![Step::action("s1", "produce").with_exports(vec!["result".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let reporter = Arc::new(CollectProgressReporter::new());
            let events_ref = reporter.events.clone();
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())))
                .with_progress_reporter(reporter);

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));

            let events = events_ref.read().await.clone();
            let phases: Vec<String> = events.into_iter().map(|e| e.phase).collect();
            assert!(phases.iter().any(|p| p == "step_started"));
            assert!(phases.iter().any(|p| p == "step_completed"));
            assert!(phases.iter().any(|p| p == "task_completed"));
        });
    }

    #[test]
    fn test_step_completion_metadata_uses_target_path_when_present() {
        let mut exports = HashMap::new();
        exports.insert("target_path".to_string(), json!("output/统计人数.md"));
        let metadata = build_step_completion_metadata("doc_transform", &exports);
        assert_eq!(
            metadata.get("path").and_then(|v| v.as_str()),
            Some("output/统计人数.md")
        );
    }

    #[test]
    fn test_execute_with_max_parallel_one_keeps_remaining_ready_nodes() {
        tokio_test::block_on(async {
            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(StaticAction::new("noop", ActionResult::success())));
            let executor = Executor::new(registry).with_max_parallel(1);

            let plan = Plan::new(
                "fan-out",
                vec![
                    Step::action("s1", "noop"),
                    Step::action("s2", "noop"),
                    Step::action("s3", "noop"),
                ],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));
            assert_eq!(dag.completed_nodes().len(), 3);
        });
    }

    #[test]
    fn test_execute_runs_ready_steps_in_parallel_batches() {
        tokio_test::block_on(async {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(SlowAction::new(active.clone(), peak.clone(), 40)));
            let executor = Executor::new(registry).with_max_parallel(2);

            let plan = Plan::new(
                "parallel fan-out",
                vec![
                    Step::action("s1", "slow"),
                    Step::action("s2", "slow"),
                    Step::action("s3", "slow"),
                ],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));
            assert!(peak.load(Ordering::SeqCst) >= 2);
        });
    }

    #[test]
    fn test_retryable_error_retries_and_then_succeeds() {
        tokio_test::block_on(async {
            let failures_left = Arc::new(AtomicUsize::new(2));
            let calls = Arc::new(AtomicUsize::new(0));

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(FlakyRetryAction::new(
                failures_left.clone(),
                calls.clone(),
            )));
            let executor = Executor::new(registry).with_retry_policy(
                3,
                Duration::from_millis(0),
                Duration::from_millis(0),
            );

            let plan = Plan::new(
                "retry success",
                vec![Step::action("s1", "flaky_retry").with_exports(vec!["ok".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let working_set = Arc::new(RwLock::new(WorkingSet::new()));
            let ctx = ExecutorContext::new("task-1", working_set.clone());

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));
            assert_eq!(calls.load(Ordering::SeqCst), 3);

            let ws = working_set.read().await;
            assert_eq!(ws.get_task("s1.ok"), Some(&json!(3)));
        });
    }

    #[test]
    fn test_retryable_error_exhausts_and_fails() {
        tokio_test::block_on(async {
            let failures_left = Arc::new(AtomicUsize::new(10));
            let calls = Arc::new(AtomicUsize::new(0));

            let mut registry = ActionRegistry::new();
            registry.register(Arc::new(FlakyRetryAction::new(
                failures_left.clone(),
                calls.clone(),
            )));
            let executor = Executor::new(registry).with_retry_policy(
                2,
                Duration::from_millis(0),
                Duration::from_millis(0),
            );

            let plan = Plan::new(
                "retry failure",
                vec![Step::action("s1", "flaky_retry").with_exports(vec!["ok".to_string()])],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("retry exhausted"));
                }
                other => panic!("expected failed result, got {:?}", other),
            }
            // initial attempt + 2 retries
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        });
    }

    #[test]
    fn test_agent_step_requires_executor_or_fails() {
        tokio_test::block_on(async {
            let executor = Executor::new(ActionRegistry::new());
            let plan = Plan::new(
                "agent missing",
                vec![Step::agent("s1").with_params(json!({
                    "goal":"inspect",
                    "allowed_actions":["echo"],
                    "max_iterations": 2,
                    "output_keys":["summary"]
                }))],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ctx = ExecutorContext::new("task-1", Arc::new(RwLock::new(WorkingSet::new())));

            let result = executor.execute(&mut dag, &ctx).await;
            match result {
                ExecutionResult::Failed { error, .. } => {
                    assert!(error.contains("missing agent executor"));
                }
                other => panic!("expected failed result, got {:?}", other),
            }
        });
    }

    #[test]
    fn test_agent_step_uses_configured_agent_executor() {
        tokio_test::block_on(async {
            let agent_executor = Arc::new(StaticAgentExecutor {
                result: ActionResult::success_with_one("summary", json!("ok")),
            });
            let executor =
                Executor::new(ActionRegistry::new()).with_agent_step_executor(agent_executor);

            let plan = Plan::new(
                "agent success",
                vec![Step::agent("s1").with_params(json!({
                    "goal":"inspect",
                    "allowed_actions":["echo"],
                    "max_iterations": 2,
                    "output_keys":["summary"]
                }))],
            );
            let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
            let ws = Arc::new(RwLock::new(WorkingSet::new()));
            let ctx = ExecutorContext::new("task-1", ws.clone());

            let result = executor.execute(&mut dag, &ctx).await;
            assert!(matches!(result, ExecutionResult::Completed));
            assert_eq!(ws.read().await.get_task("summary"), Some(&json!("ok")));
            assert_eq!(ws.read().await.get_task("s1.summary"), Some(&json!("ok")));
        });
    }
}
