//! Runtime pipeline example for Orchestral
//!
//! Demonstrates: Event → Intent → Plan → Normalize → Execute

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use orchestral_core::prelude::*;
use orchestral_actions::{ActionRegistryManager, DefaultActionFactory};
use orchestral_context::BasicContextBuilder;
use orchestral_runtime::{Orchestrator, OrchestratorResult, Thread, ThreadRuntime};
use orchestral_stores::{
    Event, EventStore, InMemoryEventStore, InMemoryReferenceStore, InMemoryTaskStore,
};

/// Minimal planner that maps any intent to a single echo action
struct SimplePlanner;

#[async_trait]
impl Planner for SimplePlanner {
    async fn plan(&self, intent: &Intent, _context: &PlannerContext) -> Result<Plan, PlanError> {
        Ok(Plan::new(
            "Echo user input",
            vec![Step::action("s1", "echo").with_params(json!({
                "message": intent.content
            }))],
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Orchestral Runtime Pipeline Example ===\n");

    let event_store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());
    let reference_store: Arc<dyn ReferenceStore> = Arc::new(InMemoryReferenceStore::new());
    let task_store: Arc<dyn TaskStore> = Arc::new(InMemoryTaskStore::new());

    let thread = Thread::new();
    let runtime = ThreadRuntime::new(thread, event_store.clone());

    let factory = Arc::new(DefaultActionFactory::new());
    let manager = Arc::new(ActionRegistryManager::new("config/actions.yaml", factory));
    manager.load().await?;
    let _watcher = manager.start_watching()?;

    let planner = Arc::new(SimplePlanner);
    let mut normalizer = PlanNormalizer::new();
    normalizer.register_action("echo");

    let executor = Executor::with_registry(manager.registry());

    let context_builder = Arc::new(BasicContextBuilder::new(
        event_store.clone(),
        reference_store.clone(),
    ));

    let orchestrator = Orchestrator::new(
        runtime,
        planner,
        normalizer,
        executor,
        task_store,
        reference_store,
    )
    .with_context_builder(context_builder);

    let event = Event::user_input(
        orchestrator.thread_runtime.thread_id().await,
        "interaction-1",
        json!({ "message": "Hello from the pipeline!" }),
    );

    let result = orchestrator.handle_event(event).await?;
    match result {
        OrchestratorResult::Started {
            interaction_id,
            task_id,
            result,
        } => {
            println!("Started interaction: {}", interaction_id);
            println!("Task: {}", task_id);
            println!("Execution result: {:?}", result);
        }
        OrchestratorResult::Merged {
            interaction_id,
            task_id,
            result,
        } => {
            println!("Merged into interaction: {}", interaction_id);
            println!("Task: {}", task_id);
            println!("Execution result: {:?}", result);
        }
        OrchestratorResult::Rejected { reason } => {
            println!("Event rejected: {}", reason);
        }
        OrchestratorResult::Queued => {
            println!("Event queued");
        }
    }

    println!("\n=== Example Complete ===");
    Ok(())
}
