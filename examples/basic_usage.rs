//! Basic usage example for Orchestral
//!
//! This example demonstrates:
//! - Creating a Thread and Interaction
//! - Using the ThreadRuntime with ConcurrencyPolicy
//! - Creating a simple Action and executing it
//! - Using the EventStore for event tracking

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::RwLock;

// Import from orchestral crates
use orchestral_core::prelude::*;
use orchestral_runtime::{
    ConcurrencyDecision, ConcurrencyPolicy, HandleEventResult, RunningState, Thread,
    ThreadRuntime,
};
use orchestral_stores::{Event, EventStore, InMemoryEventStore, InMemoryReferenceStore};

/// A simple echo action that returns the input as output
struct EchoAction;

#[async_trait]
impl Action for EchoAction {
    fn name(&self) -> &str {
        "echo"
    }

    fn description(&self) -> &str {
        "Echoes the input back as output"
    }

    async fn run(&self, input: ActionInput, ctx: ActionContext) -> ActionResult {
        // Check if cancelled
        if ctx.is_cancelled() {
            return ActionResult::error("Action was cancelled");
        }

        // Get the message from params
        let message = input
            .params
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("No message provided");

        // Return the message as output
        ActionResult::success_with_one("result", json!(format!("Echo: {}", message)))
    }
}

/// Custom concurrency policy that allows up to 2 parallel interactions
struct LimitedParallelPolicy;

impl ConcurrencyPolicy for LimitedParallelPolicy {
    fn decide(&self, running: &RunningState, _new_event: &Event) -> ConcurrencyDecision {
        if running.active_count >= 2 {
            ConcurrencyDecision::reject("Maximum 2 parallel interactions allowed")
        } else {
            ConcurrencyDecision::Parallel
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Orchestral Basic Usage Example ===\n");

    // Create stores
    let event_store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());
    let reference_store: Arc<dyn ReferenceStore> = Arc::new(InMemoryReferenceStore::new());

    // Create a Thread (context world)
    let thread = Thread::new();
    println!("Created Thread: {}", thread.id);

    // Create ThreadRuntime with custom policy
    let runtime = ThreadRuntime::with_policy(
        thread,
        event_store.clone(),
        Arc::new(LimitedParallelPolicy),
    );

    // Handle first event
    let event1 = Event::user_input(
        runtime.thread_id().await,
        "interaction-1",
        json!({"message": "Hello, Orchestral!"}),
    );

    let result1 = runtime.handle_event(event1).await?;
    match &result1 {
        HandleEventResult::Started { interaction_id } => {
            println!("Started interaction: {}", interaction_id);
        }
        _ => println!("Unexpected result: {:?}", result1),
    }

    // Handle second event (should run in parallel)
    let event2 = Event::user_input(
        runtime.thread_id().await,
        "interaction-2",
        json!({"message": "Second message"}),
    );

    let result2 = runtime.handle_event(event2).await?;
    match &result2 {
        HandleEventResult::Started { interaction_id } => {
            println!("Started parallel interaction: {}", interaction_id);
        }
        _ => println!("Unexpected result: {:?}", result2),
    }

    // Try third event (should be rejected due to policy)
    let event3 = Event::user_input(
        runtime.thread_id().await,
        "interaction-3",
        json!({"message": "Third message"}),
    );

    let result3 = runtime.handle_event(event3).await?;
    match &result3 {
        HandleEventResult::Rejected { reason } => {
            println!("Event rejected: {}", reason);
        }
        _ => println!("Unexpected result: {:?}", result3),
    }

    println!("\n--- Creating and executing a Plan ---\n");

    // Create an Intent
    let intent = Intent::new("Echo a message");
    println!("Created Intent: {}", intent.content);

    // Create a Plan manually (in real use, this would come from a Planner)
    let plan = Plan::new(
        "Echo the user's message",
        vec![Step::action("s1", "echo").with_params(json!({"message": "Hello from Plan!"}))],
    );
    println!("Created Plan: {}", plan.goal);

    // Create a PlanNormalizer and register our action
    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action("echo");

    // Normalize the plan
    let normalized = normalizer.normalize(plan)?;
    println!("Plan normalized successfully");

    // Create an Executor with our action
    let mut registry = ActionRegistry::new();
    registry.register(Arc::new(EchoAction));

    let executor = Executor::new(registry);

    // Create executor context
    let working_set = Arc::new(RwLock::new(WorkingSet::new()));
    let exec_ctx = ExecutorContext::new("task-1", working_set.clone(), reference_store);

    // Execute the plan
    let mut dag = normalized.dag;
    let result = executor.execute(&mut dag, &exec_ctx).await;

    match result {
        ExecutionResult::Completed => {
            println!("Execution completed!");

            // Read the result from working set
            let ws = working_set.read().await;
            if let Some(result) = ws.get_task("result") {
                println!("Result: {}", result);
            }
        }
        ExecutionResult::Failed { step_id, error } => {
            println!("Execution failed at step {}: {}", step_id, error);
        }
        _ => println!("Unexpected execution result"),
    }

    println!("\n=== Example Complete ===");
    Ok(())
}
