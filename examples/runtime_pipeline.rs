//! Runtime pipeline example for Orchestral
//!
//! Demonstrates startup from a single `config/orchestral.yaml`.

use serde_json::json;

use orchestral_runtime::{OrchestratorResult, RuntimeApp};
use orchestral_stores::Event;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Orchestral Runtime Pipeline Example ===\n");

    let app = RuntimeApp::from_config_path("config/orchestral.yaml").await?;
    let thread_id = app.orchestrator.thread_runtime.thread_id().await;

    let event = Event::user_input(
        thread_id,
        "interaction-1",
        json!({ "message": "Hello from the pipeline!" }),
    );

    let result = app.orchestrator.handle_event(event).await?;
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
