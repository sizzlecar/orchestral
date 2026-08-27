use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use orchestral_core::model_protocol::{
    ModelBackend, ModelCapabilities, ModelDescriptor, ModelError, ModelErrorCode, ModelEvent,
    ModelEventId, ModelFinishReason, ModelRequest, ModelStream, ModelStreamEvent, ModelToolCallId,
    ModelUsage,
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::suite::{ModelFixtureFactory, ModelFixtureResponse, ModelFixtureScenario};

/// Deterministic third protocol family used to prove that the suite is not
/// coupled to either HTTP adapter.
pub struct ScriptedModelFixture;

impl ModelFixtureFactory for ScriptedModelFixture {
    fn adapter_name(&self) -> &'static str {
        "scripted-fake"
    }

    fn backend(
        &self,
        scenario: ModelFixtureScenario,
        _endpoint: &str,
    ) -> Result<Arc<dyn ModelBackend>, ModelError> {
        Ok(Arc::new(ScriptedModelBackend { scenario }))
    }

    fn response(&self, _scenario: ModelFixtureScenario) -> ModelFixtureResponse {
        ModelFixtureResponse::Complete(Vec::new())
    }
}

struct ScriptedModelBackend {
    scenario: ModelFixtureScenario,
}

#[async_trait]
impl ModelBackend for ScriptedModelBackend {
    fn descriptor(&self) -> ModelDescriptor {
        ModelDescriptor {
            backend_id: "scripted-conformance-model".to_owned(),
            capabilities: ModelCapabilities {
                streaming: true,
                tool_calls: true,
                parallel_tool_calls: false,
                structured_output: true,
                max_context_tokens: Some(8_192),
            },
            extensions: BTreeMap::from([(
                "testkit/family".to_owned(),
                Value::String("scripted".to_owned()),
            )]),
        }
    }

    async fn start(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        request.validate()?;
        self.descriptor().validate()?;
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        let stream = match self.scenario {
            ModelFixtureScenario::Text => stream::iter(sequence(
                &request,
                vec![
                    ModelEvent::TextDelta {
                        delta: "hello ".to_owned(),
                    },
                    ModelEvent::TextDelta {
                        delta: "world".to_owned(),
                    },
                    ModelEvent::Usage {
                        usage: ModelUsage {
                            input_tokens: Some(7),
                            output_tokens: Some(2),
                        },
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::Stop,
                    },
                ],
            ))
            .boxed(),
            ModelFixtureScenario::Tool => stream::iter(sequence(
                &request,
                vec![
                    ModelEvent::ToolCallStart {
                        call_id: ModelToolCallId::new("call-1"),
                        name: "echo".to_owned(),
                        extensions: BTreeMap::new(),
                    },
                    ModelEvent::ToolCallArgumentsDelta {
                        call_id: ModelToolCallId::new("call-1"),
                        delta: r#"{"value":"hello"}"#.to_owned(),
                    },
                    ModelEvent::ToolCallEnd {
                        call_id: ModelToolCallId::new("call-1"),
                    },
                    ModelEvent::Usage {
                        usage: ModelUsage {
                            input_tokens: Some(8),
                            output_tokens: Some(3),
                        },
                    },
                    ModelEvent::Finish {
                        reason: ModelFinishReason::ToolCalls,
                    },
                ],
            ))
            .boxed(),
            ModelFixtureScenario::Malformed => {
                stream::once(async { Err(ModelError::protocol("scripted malformed stream")) })
                    .boxed()
            }
            ModelFixtureScenario::Stalled => stream::once(async move {
                cancellation.cancelled().await;
                Err(cancelled_error())
            })
            .boxed(),
        };
        Ok(stream)
    }
}

fn sequence(
    request: &ModelRequest,
    payloads: Vec<ModelEvent>,
) -> Vec<Result<ModelStreamEvent, ModelError>> {
    payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let sequence = index as u64 + 1;
            Ok(ModelStreamEvent {
                request_id: request.request_id.clone(),
                event_id: ModelEventId::new(format!(
                    "{}-scripted-{sequence}",
                    request.request_id.as_str()
                )),
                sequence,
                payload,
            })
        })
        .collect()
}

fn cancelled_error() -> ModelError {
    ModelError::new(ModelErrorCode::Cancelled, "model request cancelled")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModelConformanceSuite;

    #[tokio::test]
    async fn scripted_fake_passes_the_shared_suite() {
        let report = ModelConformanceSuite::default()
            .run(&ScriptedModelFixture)
            .await;
        assert!(report.is_conformant(), "{:#?}", report.results());
    }
}
