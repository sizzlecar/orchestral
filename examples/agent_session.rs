use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
use orchestral_core::model_protocol::{
    ModelBackend, ModelCapabilities, ModelDescriptor, ModelError, ModelEvent, ModelEventId,
    ModelFinishReason, ModelRequest, ModelStream, ModelStreamEvent,
};
use orchestral_runtime::{
    AgentClient, AgentController, GenericAgentConfig, InternalGenericAgentProvider,
};
use tokio_util::sync::CancellationToken;

struct DemoModel;

#[async_trait]
impl ModelBackend for DemoModel {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "example/demo-model".to_owned(),
            capabilities: ModelCapabilities::default(),
            extensions: Default::default(),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        let request_id = request.request_id;
        Ok(stream::iter([
            Ok(ModelStreamEvent {
                request_id: request_id.clone(),
                event_id: ModelEventId::new("demo-text"),
                sequence: 1,
                payload: ModelEvent::TextDelta {
                    delta: "Hello from the provider-neutral Generic Agent.".to_owned(),
                },
            }),
            Ok(ModelStreamEvent {
                request_id,
                event_id: ModelEventId::new("demo-finish"),
                sequence: 2,
                payload: ModelEvent::Finish {
                    reason: ModelFinishReason::Stop,
                },
            }),
        ])
        .boxed())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(InternalGenericAgentProvider::new(
        Arc::new(DemoModel),
        GenericAgentConfig::new("example/provider", "generic-agent"),
    )?);
    let controller = Arc::new(AgentController::new(
        provider,
        ProviderBindingRef::new("example/local"),
    )?);
    let client = AgentClient::new(controller, AgentSessionId::new("example-session"));
    let turn = client.run_text("Say hello").await?;
    println!("{}", turn.final_text().unwrap_or("<non-text delivery>"));
    Ok(())
}
