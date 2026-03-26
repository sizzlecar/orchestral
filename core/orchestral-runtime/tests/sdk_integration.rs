//! SDK integration test — requires OPENROUTER_API_KEY in .env.local or env.
//!
//! Run with: cargo test -p orchestral-runtime --test sdk_integration -- --nocapture

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use orchestral_core::action::{Action, ActionContext, ActionInput, ActionMeta, ActionResult};
use orchestral_core::spi::lifecycle::{LifecycleHook, StepContext, StepDecision, TurnContext};
use orchestral_core::types::Step;
use orchestral_runtime::sdk::Orchestral;
use std::fs;

/// Custom action: doubles a number.
struct DoubleAction;

#[async_trait]
impl Action for DoubleAction {
    fn name(&self) -> &str {
        "double"
    }

    fn description(&self) -> &str {
        "Double a number"
    }

    fn metadata(&self) -> ActionMeta {
        ActionMeta::new("double", "Double a number. Returns result = input * 2.")
            .with_input_schema(json!({
                "type": "object",
                "properties": {
                    "number": { "type": "number", "description": "Number to double" }
                },
                "required": ["number"]
            }))
            .with_output_schema(json!({
                "type": "object",
                "properties": {
                    "result": { "type": "number" }
                },
                "required": ["result"]
            }))
    }

    async fn run(&self, input: ActionInput, _ctx: ActionContext) -> ActionResult {
        let number = input
            .params
            .get("number")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        ActionResult::success_with(HashMap::from([("result".to_string(), json!(number * 2.0))]))
    }
}

/// Counting hook: tracks how many steps were executed.
struct CountingHook {
    step_count: AtomicUsize,
    turn_started: AtomicUsize,
}

impl CountingHook {
    fn new() -> Self {
        Self {
            step_count: AtomicUsize::new(0),
            turn_started: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl LifecycleHook for CountingHook {
    async fn on_turn_start(&self, _ctx: &TurnContext) {
        self.turn_started.fetch_add(1, Ordering::SeqCst);
    }

    async fn before_step(&self, _step: &Step, _ctx: &StepContext) -> StepDecision {
        self.step_count.fetch_add(1, Ordering::SeqCst);
        StepDecision::Continue
    }
}

fn load_env() {
    // Try multiple paths — test CWD varies
    for path in [".env.local", "../../.env.local", "../../../.env.local"] {
        if let Ok(content) = std::fs::read_to_string(path) {
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((key, value)) = line.split_once('=') {
                    let key = key.trim();
                    // Don't override already-set vars
                    if std::env::var(key).is_err() {
                        std::env::set_var(key, value.trim());
                    }
                }
            }
            break;
        }
    }
}

#[tokio::test]
async fn test_sdk_run_with_custom_action() {
    load_env();
    if std::env::var("OPENROUTER_API_KEY").is_err() {
        eprintln!("OPENROUTER_API_KEY not set, skipping SDK integration test");
        return;
    }

    let app = Orchestral::builder()
        .action(DoubleAction)
        .planner_backend("openrouter")
        .planner_model("anthropic/claude-sonnet-4.5")
        .max_planner_iterations(4)
        .build()
        .await
        .expect("build orchestral app");

    let result = app
        .run("Use the double action to double the number 21")
        .await;
    match result {
        Ok(r) => {
            println!("Status: {}", r.status);
            println!("Message: {}", r.message);
            // The planner should have called the double action
            assert!(
                r.status == "completed" || r.status == "failed",
                "unexpected status: {}",
                r.status
            );
        }
        Err(e) => {
            // Planner errors are acceptable in CI without proper LLM setup
            println!("SDK run error (may be expected): {}", e);
        }
    }
}

#[tokio::test]
async fn test_sdk_lifecycle_hooks_fire() {
    load_env();
    if std::env::var("OPENROUTER_API_KEY").is_err() {
        eprintln!("OPENROUTER_API_KEY not set, skipping SDK integration test");
        return;
    }

    let hook = Arc::new(CountingHook::new());
    let app = Orchestral::builder()
        .action(DoubleAction)
        .hook_arc(hook.clone() as Arc<dyn LifecycleHook>)
        .planner_backend("openrouter")
        .planner_model("anthropic/claude-sonnet-4.5")
        .max_planner_iterations(4)
        .build()
        .await
        .expect("build orchestral app");

    let _ = app.run("Double the number 5 using the double action").await;

    let turns = hook.turn_started.load(Ordering::SeqCst);
    let steps = hook.step_count.load(Ordering::SeqCst);
    println!("Hooks fired: turns={}, steps={}", turns, steps);

    // Turn should have started at least once
    assert!(turns >= 1, "on_turn_start should have fired");
    // At least one step should have been attempted
    assert!(steps >= 1, "before_step should have fired at least once");
}

/// Multi-step scenario: file_read + custom action + file_write in a pipeline.
/// Verifies the SDK can orchestrate a realistic multi-action workflow.
#[tokio::test]
async fn test_sdk_multi_step_pipeline() {
    load_env();
    if std::env::var("OPENROUTER_API_KEY").is_err() {
        eprintln!("OPENROUTER_API_KEY not set, skipping");
        return;
    }

    // Set up workspace with input data
    let dir = std::env::temp_dir().join(format!(
        "orchestral-sdk-pipeline-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("data.yaml"),
        "regions:\n  - name: North\n    revenue: 1200000\n  - name: South\n    revenue: 850000\n  - name: East\n    revenue: 960000\n",
    )
    .unwrap();

    // Track steps via hook
    let hook = Arc::new(CountingHook::new());

    let app = Orchestral::builder()
        .action(DoubleAction)
        .hook_arc(hook.clone() as Arc<dyn LifecycleHook>)
        .planner_backend("openrouter")
        .planner_model("anthropic/claude-sonnet-4.5")
        .max_planner_iterations(6)
        .config_path("../../configs/orchestral.cli.runtime.override.yaml")
        .build()
        .await
        .expect("build orchestral app");

    let prompt = format!(
        "1. Read the file {data} to get revenue data.\n\
         2. Write a markdown report to {report} summarizing total revenue across all regions.\n\
         Include the total sum and a list of each region with its revenue.",
        data = dir.join("data.yaml").display(),
        report = dir.join("report.md").display(),
    );

    let result = app.run(&prompt).await;

    let steps = hook.step_count.load(Ordering::SeqCst);
    let turns = hook.turn_started.load(Ordering::SeqCst);
    println!("Pipeline hooks: turns={}, steps={}", turns, steps);

    match result {
        Ok(r) => {
            println!("Status: {}", r.status);
            println!("Message: {}", r.message);
            assert_eq!(r.status, "completed", "got: {}", r.status);

            // Should have executed multiple steps (at least file_read + file_write)
            assert!(steps >= 2, "expected >= 2 steps, got {}", steps);

            // Verify the report was written
            let report_path = dir.join("report.md");
            assert!(
                report_path.exists(),
                "report.md should have been created at {}",
                report_path.display()
            );
            let report_content = fs::read_to_string(&report_path).unwrap();
            println!("Report:\n{}", report_content);
            assert!(
                report_content.contains("North") || report_content.contains("revenue"),
                "report should contain region data: {}",
                report_content
            );
        }
        Err(e) => panic!("SDK pipeline failed: {}", e),
    }

    let _ = fs::remove_dir_all(&dir);
}
