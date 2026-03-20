use std::collections::VecDeque;
use std::fs;
use std::sync::Mutex;

use super::output::resolve_plan_response_template;
use super::recovery::{
    affected_subgraph_steps, build_normalization_repair_intent, locate_normalization_repair_target,
    locate_recovery_cut_step, NormalizeRepairMode, NormalizeRepairTarget,
};
use super::*;
use crate::action::{ActionFactory, DefaultActionFactory};
use crate::thread::Thread;
use async_trait::async_trait;
use orchestral_core::executor::{ExecutionDag, ExecutionProgressEvent, ExecutionProgressReporter};
use orchestral_core::normalizer::ValidationError;
use orchestral_core::planner::{
    PlanError, Planner, PlannerLoopContext, PlannerOutput, SingleAction,
};
use orchestral_core::store::{
    BroadcastEventBus, EventBus, EventStore, InMemoryEventStore, InMemoryTaskStore, TaskStore,
};
use orchestral_core::types::{Plan, Step, StepId, StepIoBinding, StepKind};
use orchestral_core::{action::extract_meta, config::ActionSpec};
use serde_json::json;

#[test]
fn test_complete_wait_step_for_resume_marks_wait_user() {
    let plan = Plan::new(
        "goal",
        vec![
            Step::wait_user("wait"),
            Step::action("next", "noop").with_depends_on(vec![StepId::from("wait")]),
        ],
    );
    let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
    let event = Event::user_input("thread-1", "int-1", json!({"text":"ok"}));

    complete_wait_step_for_resume(&mut dag, &plan, &[], &event);

    assert!(dag.completed_nodes().contains(&"wait"));
}

#[test]
fn test_complete_wait_step_for_resume_matches_wait_event_kind() {
    let mut wait_timer = Step::action("wait_timer", "wait_event");
    wait_timer.kind = StepKind::WaitEvent;
    wait_timer.params = json!({"event_type":"timer"});

    let mut wait_webhook = Step::action("wait_webhook", "wait_event");
    wait_webhook.kind = StepKind::WaitEvent;
    wait_webhook.params = json!({"event_type":"webhook"});

    let plan = Plan::new(
        "goal",
        vec![
            wait_timer,
            Step::action("prep", "noop"),
            wait_webhook.with_depends_on(vec![StepId::from("prep")]),
        ],
    );
    let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
    dag.mark_completed("prep");
    let event = Event::external("thread-1", "webhook", json!({"ok":true}));

    complete_wait_step_for_resume(&mut dag, &plan, &[StepId::from("prep")], &event);

    assert!(dag.completed_nodes().contains(&"wait_webhook"));
    assert!(!dag.completed_nodes().contains(&"wait_timer"));
}

#[test]
fn test_apply_resume_event_to_working_set_for_external_event() {
    let mut ws = WorkingSet::new();
    let event = Event::external("thread-1", "timer", json!({"due":true}));

    apply_resume_event_to_working_set(&mut ws, &event);

    assert_eq!(
        ws.get_task("resume_external_event"),
        Some(&json!({"due":true}))
    );
    assert_eq!(
        ws.get_task("last_event_payload"),
        Some(&json!({"due":true}))
    );
    assert_eq!(ws.get_task("last_event_kind"), Some(&json!("timer")));
}

#[test]
fn test_runtime_progress_reporter_persists_and_publishes() {
    tokio_test::block_on(async {
        let event_store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());
        let event_bus = Arc::new(BroadcastEventBus::new(16));
        let mut sub = event_bus.subscribe();
        let reporter = RuntimeProgressReporter::new(
            "thread-1".into(),
            "int-1".into(),
            event_store.clone(),
            event_bus.clone(),
            Arc::new(HookRegistry::new()),
        );

        reporter
            .report(
                ExecutionProgressEvent::new(
                    "task-1",
                    Some(StepId::from("s1")),
                    Some("echo".to_string()),
                    "step_started",
                )
                .with_message("running"),
            )
            .await
            .expect("report");

        let events = event_store
            .query_by_thread("thread-1")
            .await
            .expect("query");
        assert_eq!(events.len(), 1);
        match &events[0] {
            Event::SystemTrace { payload, .. } => {
                assert_eq!(payload["category"], "execution_progress");
                assert_eq!(payload["interaction_id"], "int-1");
                assert_eq!(payload["task_id"], "task-1");
                assert_eq!(payload["phase"], "step_started");
            }
            _ => panic!("expected system trace"),
        }

        let bus_event = sub.recv().await.expect("bus event");
        assert!(matches!(bus_event, Event::SystemTrace { .. }));
    });
}

#[test]
fn test_locate_recovery_cut_step_backtracks_to_upstream_on_data_contract_error() {
    let plan = Plan::new(
        "goal",
        vec![
            Step::action("A", "file_read"),
            Step::action("B", "doc_parse")
                .with_depends_on(vec![StepId::from("A")])
                .with_io_bindings(vec![StepIoBinding::required("A.content", "content")]),
            Step::action("C", "summarize").with_depends_on(vec![StepId::from("B")]),
        ],
    );
    let completed = vec![StepId::from("A")];
    let cut = locate_recovery_cut_step(
        &plan,
        "B",
        "input schema validation failed at $.content",
        &completed,
        &HashMap::new(),
    )
    .expect("cut");
    assert_eq!(cut.cut_step_id, "A");
    let affected = affected_subgraph_steps(&plan, &cut.cut_step_id);
    let affected_set: std::collections::HashSet<_> = affected.into_iter().collect();
    assert!(affected_set.contains("A"));
    assert!(affected_set.contains("B"));
    assert!(affected_set.contains("C"));
}

#[test]
fn test_locate_recovery_cut_step_keeps_failed_step_on_runtime_error() {
    let plan = Plan::new(
        "goal",
        vec![
            Step::action("A", "file_read"),
            Step::action("B", "http").with_depends_on(vec![StepId::from("A")]),
        ],
    );
    let cut = locate_recovery_cut_step(
        &plan,
        "B",
        "request timeout after 10s",
        &[StepId::from("A")],
        &HashMap::new(),
    )
    .expect("cut");
    assert_eq!(cut.cut_step_id, "B");
}

#[test]
fn test_locate_recovery_cut_step_does_not_backtrack_to_unfinished_dependency() {
    let plan = Plan::new(
        "goal",
        vec![
            Step::action("A", "file_read"),
            Step::action("B", "doc_parse")
                .with_depends_on(vec![StepId::from("A")])
                .with_io_bindings(vec![StepIoBinding::required("A.content", "content")]),
        ],
    );
    let cut = locate_recovery_cut_step(
        &plan,
        "B",
        "missing required io binding 'content'",
        &[],
        &HashMap::new(),
    )
    .expect("cut");
    assert_eq!(cut.cut_step_id, "B");
}

#[test]
fn test_locate_recovery_cut_step_prefers_error_referenced_dependency() {
    let plan = Plan::new(
        "goal",
        vec![
            Step::action("A1", "fetch_profile"),
            Step::action("A2", "fetch_policy"),
            Step::action("B", "merge")
                .with_depends_on(vec![StepId::from("A1"), StepId::from("A2")])
                .with_io_bindings(vec![
                    StepIoBinding::required("A1.result", "profile"),
                    StepIoBinding::required("A2.result", "policy"),
                ]),
        ],
    );
    let cut = locate_recovery_cut_step(
        &plan,
        "B",
        "schema validation failed: dependency A2 returned invalid format",
        &[StepId::from("A1"), StepId::from("A2")],
        &HashMap::new(),
    )
    .expect("cut");
    assert_eq!(cut.cut_step_id, "A2");
}

#[test]
fn test_locate_normalization_repair_target_prefers_full_rewrite_without_checkpoint() {
    let plan = Plan::new(
        "goal",
        vec![
            Step::action("s1", "shell"),
            Step::agent("s2")
                .with_depends_on(vec![StepId::from("s1")])
                .with_params(json!({
                    "goal": "inspect and update",
                    "allowed_actions": ["shell", "file_write"],
                    "max_iterations": 5,
                    "output_keys": ["updated_file_path"],
                    "output_rules": {
                        "updated_file_path": {
                            "candidates": [
                                {
                                    "slot": "fill_result",
                                    "path": "updated_file_path",
                                    "requires": { "action": "file_write" }
                                }
                            ]
                        }
                    }
                })),
        ],
    );
    let error = NormalizeError::Validation(ValidationError::InvalidAgentParams(
        "s2".to_string(),
        "explore agent may not own side-effect-sensitive outputs".to_string(),
    ));

    let target = locate_normalization_repair_target(&plan, &error, &[]).expect("target");

    assert_eq!(target.mode_label(), "full_plan");
    assert_eq!(target.invalid_step_id.as_deref(), Some("s2"));
    assert_eq!(target.affected_steps, vec!["s2".to_string()]);
}

#[test]
fn test_locate_normalization_repair_target_patches_only_unfinished_subgraph() {
    let plan = Plan::new(
        "goal",
        vec![
            Step::action("s1", "shell"),
            Step::agent("s2")
                .with_depends_on(vec![StepId::from("s1")])
                .with_params(json!({
                    "goal": "inspect and update",
                    "allowed_actions": ["shell", "file_write"],
                    "max_iterations": 5,
                    "output_keys": ["updated_file_path"],
                    "output_rules": {
                        "updated_file_path": {
                            "candidates": [
                                {
                                    "slot": "fill_result",
                                    "path": "updated_file_path",
                                    "requires": { "action": "file_write" }
                                }
                            ]
                        }
                    }
                })),
            Step::action("s3", "notify").with_depends_on(vec![StepId::from("s2")]),
        ],
    );
    let error = NormalizeError::Validation(ValidationError::InvalidAgentParams(
        "s2".to_string(),
        "explore agent may not own side-effect-sensitive outputs".to_string(),
    ));

    let target =
        locate_normalization_repair_target(&plan, &error, &[StepId::from("s1")]).expect("target");

    assert_eq!(target.mode_label(), "subgraph_patch");
    assert_eq!(target.cut_step_id(), Some("s2"));
    assert_eq!(target.invalid_step_id.as_deref(), Some("s2"));
    let affected: std::collections::HashSet<_> = target.affected_steps.into_iter().collect();
    assert_eq!(
        affected,
        std::collections::HashSet::from(["s2".to_string(), "s3".to_string(),])
    );
}

#[test]
fn test_build_normalization_repair_intent_adds_hard_rules() {
    let plan = Plan::new(
        "goal",
        vec![Step::agent("s2").with_params(json!({
            "goal": "inspect and update",
            "allowed_actions": ["shell", "file_write"],
            "max_iterations": 5,
            "output_keys": ["updated_file_path"]
        }))],
    );
    let target = NormalizeRepairTarget {
        mode: NormalizeRepairMode::FullPlan,
        invalid_step_id: Some("s2".to_string()),
        affected_steps: vec!["s2".to_string()],
    };
    let error = NormalizeError::Validation(ValidationError::InvalidAgentParams(
        "s2".to_string(),
        "bad agent".to_string(),
    ));
    let intent = build_normalization_repair_intent(
        &Intent::new("fill the workbook"),
        &plan,
        &error,
        &target,
        &[],
    );

    assert!(intent.content.contains("Normalization repair request."));
    assert!(intent.content.contains("Repair mode: full_plan"));
    assert!(intent
        .content
        .contains("Never return an explore agent that owns outputs"));
    assert!(intent
        .content
        .contains("inspect/collect -> derive leaf -> apply/emit -> verify"));
}

#[test]
fn test_summarize_working_set_applies_semantic_limits() {
    let mut snapshot = HashMap::new();
    snapshot.insert("stdout".to_string(), json!("s".repeat(5_000)));
    snapshot.insert("stderr".to_string(), json!("e".repeat(1_500)));
    snapshot.insert("path".to_string(), json!("p".repeat(500)));
    snapshot.insert("reader.content".to_string(), json!("c".repeat(5_000)));

    let summary = summarize_working_set(&snapshot);

    let stdout_line = summary
        .lines()
        .find(|line| line.starts_with("  stdout: \""))
        .expect("stdout line");
    let stdout_value = stdout_line
        .strip_prefix("  stdout: \"")
        .and_then(|line| line.strip_suffix('"'))
        .expect("stdout quoted value");
    assert_eq!(stdout_value.chars().count(), 4_000);

    let stderr_line = summary
        .lines()
        .find(|line| line.starts_with("  stderr: \""))
        .expect("stderr line");
    let stderr_value = stderr_line
        .strip_prefix("  stderr: \"")
        .and_then(|line| line.strip_suffix('"'))
        .expect("stderr quoted value");
    assert_eq!(stderr_value.chars().count(), 1_000);

    let path_line = summary
        .lines()
        .find(|line| line.starts_with("  path: \""))
        .expect("path line");
    let path_value = path_line
        .strip_prefix("  path: \"")
        .and_then(|line| line.strip_suffix('"'))
        .expect("path quoted value");
    assert_eq!(path_value.chars().count(), 200);

    let content_line = summary
        .lines()
        .find(|line| line.starts_with("  reader.content: \""))
        .expect("content line");
    let content_value = content_line
        .strip_prefix("  reader.content: \"")
        .and_then(|line| line.strip_suffix('"'))
        .expect("content quoted value");
    assert_eq!(content_value.chars().count(), 4_000);
}

#[test]
fn test_summarize_working_set_is_utf8_safe() {
    let mut snapshot = HashMap::new();
    snapshot.insert("note".to_string(), json!("你好".repeat(260)));

    let summary = summarize_working_set(&snapshot);
    let note_line = summary
        .lines()
        .find(|line| line.starts_with("  note: \""))
        .expect("note line");
    let note_value = note_line
        .strip_prefix("  note: \"")
        .and_then(|line| line.strip_suffix('"'))
        .expect("note quoted value");

    assert_eq!(note_value.chars().count(), 200);
    assert!(note_value.ends_with("..."));
}

#[test]
fn test_summarize_working_set_prioritizes_stdout_content_and_stderr() {
    let mut snapshot = HashMap::new();
    snapshot.insert("stdout".to_string(), json!("s".repeat(10_000)));
    snapshot.insert("stderr".to_string(), json!("e".repeat(3_000)));
    snapshot.insert("reader.content".to_string(), json!("c".repeat(10_000)));
    for idx in 0..120 {
        snapshot.insert(format!("low_{idx:03}"), json!("l".repeat(400)));
    }

    let summary = summarize_working_set(&snapshot);

    assert!(summary.contains("  stdout: \""));
    assert!(summary.contains("  reader.content: \""));
    assert!(summary.contains("  stderr: \""));
    assert!(!summary.contains("  low_119: \""));
}

#[test]
fn test_resolve_plan_response_template_appends_summary_when_on_complete_is_static() {
    let mut plan = Plan::new("goal", vec![]);
    plan.on_complete = Some("Successfully found and summarized the Excel file.".to_string());
    let mut ws = HashMap::new();
    ws.insert("summary".to_string(), json!("Sheet 2025-Q2: 28 rows"));

    let resolved =
        resolve_plan_response_template(&plan, &ExecutionResult::Completed, &ws).expect("msg");

    assert_eq!(
        resolved,
        "Successfully found and summarized the Excel file.\n\nSheet 2025-Q2: 28 rows"
    );
}

#[test]
fn test_resolve_plan_response_template_does_not_duplicate_summary_when_placeholder_exists() {
    let mut plan = Plan::new("goal", vec![]);
    plan.on_complete = Some("Done:\n{{summary}}".to_string());
    let mut ws = HashMap::new();
    ws.insert("summary".to_string(), json!("Sheet 2025-Q2: 28 rows"));

    let resolved =
        resolve_plan_response_template(&plan, &ExecutionResult::Completed, &ws).expect("msg");

    assert_eq!(resolved, "Done:\nSheet 2025-Q2: 28 rows");
}

#[test]
fn test_resolve_plan_response_template_uses_scoped_summary_fallback() {
    let mut plan = Plan::new("goal", vec![]);
    plan.on_complete = Some("Done".to_string());
    let mut ws = HashMap::new();
    ws.insert(
        "summarize_excel_agent.summary".to_string(),
        json!("Sheet 2025-Q2: 28 rows"),
    );

    let resolved =
        resolve_plan_response_template(&plan, &ExecutionResult::Completed, &ws).expect("msg");

    assert_eq!(resolved, "Done\n\nSheet 2025-Q2: 28 rows");
}

#[derive(Debug)]
struct SequencePlanner {
    outputs: Mutex<VecDeque<PlannerOutput>>,
    seen_loop_contexts: Mutex<Vec<Option<PlannerLoopContext>>>,
}

impl SequencePlanner {
    fn new(outputs: Vec<PlannerOutput>) -> Self {
        Self {
            outputs: Mutex::new(outputs.into()),
            seen_loop_contexts: Mutex::new(Vec::new()),
        }
    }

    fn seen_loop_contexts(&self) -> Vec<Option<PlannerLoopContext>> {
        self.seen_loop_contexts
            .lock()
            .expect("loop contexts")
            .clone()
    }
}

#[async_trait]
impl Planner for SequencePlanner {
    async fn plan(
        &self,
        _intent: &Intent,
        context: &PlannerContext,
    ) -> Result<PlannerOutput, PlanError> {
        self.seen_loop_contexts
            .lock()
            .expect("loop contexts")
            .push(context.loop_context.clone());
        self.outputs
            .lock()
            .expect("outputs")
            .pop_front()
            .ok_or_else(|| PlanError::Generation("planner output queue exhausted".to_string()))
    }
}

fn build_test_orchestrator(
    planner: Arc<SequencePlanner>,
    max_planner_iterations: usize,
) -> (
    Orchestrator,
    Arc<InMemoryTaskStore>,
    Arc<InMemoryEventStore>,
) {
    let event_store = Arc::new(InMemoryEventStore::new());
    let task_store = Arc::new(InMemoryTaskStore::new());
    let thread_runtime = ThreadRuntime::new(Thread::new(), event_store.clone());

    let action_spec = ActionSpec {
        name: "file_read".to_string(),
        kind: "file_read".to_string(),
        description: Some("Read a file".to_string()),
        category: None,
        config: json!({}),
        interface: None,
    };
    let action = DefaultActionFactory::new()
        .build(&action_spec)
        .expect("build file_read");
    let action_meta = extract_meta(action.as_ref());

    let mut inner_registry = orchestral_core::executor::ActionRegistry::new();
    inner_registry.register(action);
    let registry = Arc::new(RwLock::new(inner_registry));
    let executor = Executor::with_registry(registry);

    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action_meta(&action_meta);

    let orchestrator = Orchestrator::with_config(
        thread_runtime,
        planner,
        normalizer,
        executor,
        task_store.clone() as Arc<dyn TaskStore>,
        OrchestratorConfig {
            max_planner_iterations,
            ..OrchestratorConfig::default()
        },
    );
    (orchestrator, task_store, event_store)
}

fn write_test_file(name: &str, content: &str) -> std::path::PathBuf {
    let root = std::env::current_dir()
        .expect("cwd")
        .join("target")
        .join("agent-loop-tests");
    fs::create_dir_all(&root).expect("create test dir");
    let path = root.join(format!("{}-{}.txt", name, uuid::Uuid::new_v4()));
    fs::write(&path, content).expect("write test file");
    path
}

#[tokio::test]
async fn test_orchestrator_agent_loop_replans_after_completed_single_action() {
    let path = write_test_file("agent-loop-complete", "hello from loop\n");
    let planner = Arc::new(SequencePlanner::new(vec![
        PlannerOutput::SingleAction(SingleAction {
            action: "file_read".to_string(),
            params: json!({"path": path.to_string_lossy()}),
            reason: Some("read file".to_string()),
        }),
        PlannerOutput::Done("final answer".to_string()),
    ]));
    let (orchestrator, task_store, event_store) = build_test_orchestrator(planner.clone(), 4);
    let thread_id = orchestrator.thread_runtime.thread_id().await;

    let result = orchestrator
        .handle_event(Event::user_input(
            thread_id.as_str(),
            "int-1",
            json!({"text":"读取文件后给出结论"}),
        ))
        .await
        .expect("handle event");

    let (task_id, execution_result) = match result {
        OrchestratorResult::Started {
            task_id, result, ..
        } => (task_id, result),
        other => panic!("unexpected orchestrator result: {:?}", other),
    };
    assert!(matches!(execution_result, ExecutionResult::Completed));

    let task = task_store
        .load(task_id.as_str())
        .await
        .expect("load task")
        .expect("task exists");
    assert!(matches!(task.state, TaskState::Done));
    assert_eq!(
        task.working_set_snapshot.get("single_action.content"),
        Some(&json!("hello from loop\n"))
    );

    let loop_contexts = planner.seen_loop_contexts();
    assert_eq!(loop_contexts.len(), 2);
    assert!(loop_contexts[0].is_none());
    let second = loop_contexts[1].clone().expect("second loop context");
    assert_eq!(second.iteration, 2);
    assert!(second
        .recent_observations
        .iter()
        .any(|item| item.contains("result=completed")));
    assert!(second
        .working_set_preview
        .as_deref()
        .is_some_and(|preview| preview.contains("single_action.content")));

    let events = event_store
        .query_by_thread(thread_id.as_str())
        .await
        .expect("query events");
    let assistant_messages = events
        .iter()
        .filter_map(|event| match event {
            Event::AssistantOutput { payload, .. } => {
                payload.get("message").and_then(Value::as_str)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(assistant_messages, vec!["final answer"]);

    let _ = fs::remove_file(path);
}

#[tokio::test]
async fn test_orchestrator_agent_loop_replans_after_failed_action_and_returns_need_input() {
    let missing_path = std::env::current_dir()
        .expect("cwd")
        .join("target")
        .join("agent-loop-tests")
        .join(format!("missing-{}.txt", uuid::Uuid::new_v4()));
    let planner = Arc::new(SequencePlanner::new(vec![
        PlannerOutput::SingleAction(SingleAction {
            action: "file_read".to_string(),
            params: json!({"path": missing_path.to_string_lossy()}),
            reason: Some("try missing file".to_string()),
        }),
        PlannerOutput::NeedInput("请提供正确的文件路径".to_string()),
    ]));
    let (orchestrator, task_store, _event_store) = build_test_orchestrator(planner.clone(), 4);
    let thread_id = orchestrator.thread_runtime.thread_id().await;

    let result = orchestrator
        .handle_event(Event::user_input(
            thread_id.as_str(),
            "int-1",
            json!({"text":"先尝试读文件，失败后问我要路径"}),
        ))
        .await
        .expect("handle event");

    let (task_id, execution_result) = match result {
        OrchestratorResult::Started {
            task_id, result, ..
        } => (task_id, result),
        other => panic!("unexpected orchestrator result: {:?}", other),
    };
    assert!(matches!(
        execution_result,
        ExecutionResult::WaitingUser { .. }
    ));

    let task = task_store
        .load(task_id.as_str())
        .await
        .expect("load task")
        .expect("task exists");
    assert!(matches!(task.state, TaskState::WaitingUser { .. }));
    assert!(task.plan.is_none());

    let loop_contexts = planner.seen_loop_contexts();
    assert_eq!(loop_contexts.len(), 2);
    let second = loop_contexts[1].clone().expect("second loop context");
    assert!(second
        .recent_observations
        .iter()
        .any(|item| item.contains("result=failed")));
}

#[tokio::test]
async fn test_orchestrator_agent_loop_enforces_iteration_limit_after_completed_execution() {
    let path = write_test_file("agent-loop-limit", "limit\n");
    let planner = Arc::new(SequencePlanner::new(vec![PlannerOutput::SingleAction(
        SingleAction {
            action: "file_read".to_string(),
            params: json!({"path": path.to_string_lossy()}),
            reason: Some("read file once".to_string()),
        },
    )]));
    let (orchestrator, task_store, _event_store) = build_test_orchestrator(planner, 1);
    let thread_id = orchestrator.thread_runtime.thread_id().await;

    let result = orchestrator
        .handle_event(Event::user_input(
            thread_id.as_str(),
            "int-1",
            json!({"text":"读完后如果没结束就报错"}),
        ))
        .await
        .expect("handle event");

    let (task_id, execution_result) = match result {
        OrchestratorResult::Started {
            task_id, result, ..
        } => (task_id, result),
        other => panic!("unexpected orchestrator result: {:?}", other),
    };
    match execution_result {
        ExecutionResult::Failed { step_id, error } => {
            assert_eq!(step_id.as_str(), "planner");
            assert!(error.contains("planner iteration limit reached"));
        }
        other => panic!("expected failed result, got {:?}", other),
    }

    let task = task_store
        .load(task_id.as_str())
        .await
        .expect("load task")
        .expect("task exists");
    assert!(matches!(
        task.state,
        TaskState::Failed {
            recoverable: false,
            ..
        }
    ));

    let _ = fs::remove_file(path);
}
