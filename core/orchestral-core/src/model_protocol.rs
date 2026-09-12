//! Provider-neutral model request and streaming contracts.
//!
//! A model backend performs one model request. It is intentionally smaller
//! than an Agent: it has no Run lifecycle, Session journal, tools execution,
//! approvals, or goal semantics.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::agent_protocol::wire::Digest;

pub type ModelStream = BoxStream<'static, Result<ModelStreamEvent, ModelError>>;

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_empty(&self) -> bool {
                self.0.trim().is_empty()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(ModelRequestId);
string_id!(ModelEventId);
string_id!(ModelToolCallId);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCapabilities {
    pub streaming: bool,
    pub tool_calls: bool,
    pub parallel_tool_calls: bool,
    pub structured_output: bool,
    #[serde(default)]
    pub max_context_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDescriptor {
    pub backend_id: String,
    pub capabilities: ModelCapabilities,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
}

/// Strength of the token count exposed by a model-family adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelTokenAccounting {
    /// The adapter uses the provider's exact tokenizer/counting contract.
    Exact,
    /// The adapter deliberately over-counts the provider wire representation.
    ConservativeUpperBound,
    /// A context-planning estimate, never a hard token or cost reservation.
    Estimated,
}

/// Planning information kept separate from the meter's hard input bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelContextEstimate {
    pub tokens: u64,
    pub accounting: ModelTokenAccounting,
}

/// Immutable identity of the token accounting strategy used to build model
/// context. The descriptor is bound into the Generic Agent configuration
/// digest so recovery cannot silently select history with a different meter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenMeterDescriptor {
    pub strategy: String,
    pub version: String,
    pub accounting: ModelTokenAccounting,
    pub config_digest: Digest,
}

impl ModelTokenMeterDescriptor {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.strategy.trim().is_empty()
            || self.version.trim().is_empty()
            || !self.config_digest.is_sha256()
            || self.accounting == ModelTokenAccounting::Estimated
        {
            return Err(ModelError::invalid_request(
                "invalid model token meter descriptor",
            ));
        }
        Ok(())
    }
}

/// Model-family token accounting boundary. A production adapter must provide
/// either its exact tokenizer or a documented conservative upper bound over
/// the provider-specific serialized request input.
pub trait ModelTokenMeter: Send + Sync {
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor;

    fn count_request_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<u64, ModelError>;

    /// Estimate space for context selection when no hard input/cost ceiling
    /// applies. The default retains the existing certified count. Adapters
    /// overriding this method must bind its algorithm into their descriptor's
    /// immutable identity. Estimates must not replace `count_request_input`
    /// for resource reservations or validation of observed usage.
    fn estimate_context_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<ModelContextEstimate, ModelError> {
        Ok(ModelContextEstimate {
            tokens: self.count_request_input(messages, tools)?,
            accounting: self.meter_descriptor().accounting,
        })
    }
}

impl ModelDescriptor {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.backend_id.trim().is_empty()
            || self.capabilities.max_context_tokens == Some(0)
            || (self.capabilities.parallel_tool_calls && !self.capabilities.tool_calls)
        {
            return Err(ModelError::invalid_request(
                "invalid backend identity or capability combination",
            ));
        }
        validate_extensions(&self.extensions)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    pub request_id: ModelRequestId,
    pub messages: Vec<ModelMessage>,
    #[serde(default)]
    pub tools: Vec<ModelToolDefinition>,
    #[serde(default)]
    pub output_schema: Option<Value>,
    #[serde(default)]
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
}

impl ModelRequest {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.request_id.is_empty()
            || self.messages.is_empty()
            || self.max_output_tokens == Some(0)
        {
            return Err(ModelError::invalid_request(
                "model request requires an identity, messages, and positive limits",
            ));
        }
        for message in &self.messages {
            message.validate()?;
        }
        let mut tool_names = BTreeSet::new();
        for tool in &self.tools {
            tool.validate()?;
            if !tool_names.insert(tool.name.as_str()) {
                return Err(ModelError::invalid_request(
                    "model tool names must be unique",
                ));
            }
        }
        if self
            .output_schema
            .as_ref()
            .is_some_and(|schema| !schema.is_object())
        {
            return Err(ModelError::invalid_request(
                "model output schema must be a JSON object",
            ));
        }
        validate_extensions(&self.extensions)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ModelRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelMessage {
    pub role: ModelRole,
    pub content: Vec<ModelContent>,
}

impl ModelMessage {
    pub fn text(role: ModelRole, text: impl Into<String>) -> Self {
        Self {
            role,
            content: vec![ModelContent::Text { text: text.into() }],
        }
    }

    pub fn validate(&self) -> Result<(), ModelError> {
        if self.content.is_empty() {
            return Err(ModelError::invalid_request(
                "model message content must not be empty",
            ));
        }
        let mut continuations = BTreeSet::new();
        for content in &self.content {
            content.validate()?;
            if let ModelContent::Continuation { namespace, .. } = content {
                if self.role != ModelRole::Assistant || !continuations.insert(namespace) {
                    return Err(ModelError::invalid_request(
                        "continuation requires the Assistant role and a unique namespace",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum ModelContent {
    Text {
        text: String,
    },
    Json {
        value: Value,
    },
    Data {
        media_type: String,
        value: Value,
    },
    /// Opaque provider-owned state belonging to this entire Assistant message,
    /// including all its parallel Tool calls. Retained in canonical history for
    /// native continuation; never rendered as public text or a summary and never
    /// passed to a Tool. A provider that cannot consume it must reject it.
    Continuation {
        namespace: String,
        value: Value,
    },
    ToolCall {
        call_id: ModelToolCallId,
        name: String,
        arguments: Value,
        /// Provider-owned continuation state that survives Tool execution and
        /// recovery but is never passed to the Tool itself.
        #[serde(default)]
        extensions: BTreeMap<String, Value>,
    },
    ToolResult {
        call_id: ModelToolCallId,
        result: Value,
        is_error: bool,
    },
}

impl ModelContent {
    pub fn validate(&self) -> Result<(), ModelError> {
        match self {
            Self::Text { text } if text.is_empty() => {
                Err(ModelError::invalid_request("model text must not be empty"))
            }
            Self::Data { media_type, .. } if media_type.trim().is_empty() => Err(
                ModelError::invalid_request("model data requires a media type"),
            ),
            Self::Continuation { namespace, .. } if namespace.trim().is_empty() => Err(
                ModelError::invalid_request("model continuation requires a namespace"),
            ),
            Self::ToolCall {
                call_id,
                name,
                extensions,
                ..
            } => {
                if call_id.is_empty() || name.trim().is_empty() {
                    return Err(ModelError::invalid_request(
                        "model tool call requires call_id and name",
                    ));
                }
                validate_extensions(extensions)
            }
            Self::ToolResult { call_id, .. } if call_id.is_empty() => Err(
                ModelError::invalid_request("model tool result requires call_id"),
            ),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

impl ModelToolDefinition {
    pub fn validate(&self) -> Result<(), ModelError> {
        if self.name.trim().is_empty()
            || self.description.trim().is_empty()
            || !self.input_schema.is_object()
        {
            return Err(ModelError::invalid_request(
                "model tool requires name, description, and an object input schema",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelStreamEvent {
    pub request_id: ModelRequestId,
    pub event_id: ModelEventId,
    pub sequence: u64,
    pub payload: ModelEvent,
}

impl ModelStreamEvent {
    pub fn validate_for(
        &self,
        request_id: &ModelRequestId,
        expected_sequence: u64,
    ) -> Result<(), ModelError> {
        if self.request_id != *request_id
            || self.event_id.is_empty()
            || self.sequence != expected_sequence
            || expected_sequence == 0
        {
            return Err(ModelError::new(
                ModelErrorCode::Protocol,
                "model event identity or sequence is invalid",
            ));
        }
        self.payload.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum ModelEvent {
    TextDelta {
        delta: String,
    },
    /// Complete opaque state for the current Assistant message, once per
    /// namespace. The consumer commits it only with a terminal Finish event.
    /// This is not an output delta or a Tool argument fragment.
    Continuation {
        namespace: String,
        value: Value,
    },
    ToolCallStart {
        call_id: ModelToolCallId,
        name: String,
        /// Provider-owned state associated with the exact Tool-call part.
        #[serde(default)]
        extensions: BTreeMap<String, Value>,
    },
    ToolCallArgumentsDelta {
        call_id: ModelToolCallId,
        delta: String,
    },
    ToolCallEnd {
        call_id: ModelToolCallId,
    },
    Usage {
        usage: ModelUsage,
    },
    Finish {
        reason: ModelFinishReason,
    },
}

impl ModelEvent {
    pub fn validate(&self) -> Result<(), ModelError> {
        match self {
            Self::TextDelta { delta } if delta.is_empty() => {
                Err(ModelError::protocol("text delta must not be empty"))
            }
            Self::Continuation { namespace, .. } if namespace.trim().is_empty() => Err(
                ModelError::protocol("model continuation requires a namespace"),
            ),
            Self::ToolCallStart {
                call_id,
                name,
                extensions,
            } => {
                if call_id.is_empty() || name.trim().is_empty() {
                    return Err(ModelError::protocol(
                        "tool-call start requires call_id and name",
                    ));
                }
                validate_extensions(extensions)
            }
            Self::ToolCallArgumentsDelta { call_id, delta }
                if call_id.is_empty() || delta.is_empty() =>
            {
                Err(ModelError::protocol(
                    "tool-call arguments delta requires call_id and content",
                ))
            }
            Self::ToolCallEnd { call_id } if call_id.is_empty() => {
                Err(ModelError::protocol("tool-call end requires call_id"))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsage {
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ModelFinishReason {
    Stop,
    Length,
    ToolCalls,
    Cancelled,
    ContentFilter,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ModelErrorCode {
    InvalidRequest,
    Unsupported,
    Unavailable,
    RateLimited,
    Authentication,
    Protocol,
    Cancelled,
    Internal,
}

#[derive(Debug, Clone, Error, PartialEq, Serialize, Deserialize)]
#[error("{code:?}: {message}")]
#[serde(deny_unknown_fields)]
pub struct ModelError {
    pub code: ModelErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

impl ModelError {
    pub fn new(code: ModelErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            details: Value::Null,
        }
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ModelErrorCode::InvalidRequest, message)
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(ModelErrorCode::Protocol, message)
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

#[async_trait]
pub trait ModelBackend: Send + Sync {
    fn descriptor(&self) -> ModelDescriptor;

    /// Establishes the pull-based event stream before returning. Dropping the
    /// stream does not replace cancellation; adapters must observe the supplied
    /// token and stop their underlying request.
    async fn start(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError>;
}

#[async_trait]
impl ModelBackend for Arc<dyn ModelBackend> {
    fn descriptor(&self) -> ModelDescriptor {
        (**self).descriptor()
    }

    async fn start(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelStream, ModelError> {
        (**self).start(request, cancellation).await
    }
}

fn validate_extensions(extensions: &BTreeMap<String, Value>) -> Result<(), ModelError> {
    if extensions.keys().any(|key| {
        !key.split_once('/')
            .is_some_and(|(namespace, name)| !namespace.is_empty() && !name.is_empty())
    }) {
        return Err(ModelError::invalid_request(
            "model extension keys must be namespaced",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_is_message_owned_and_never_valid_as_user_or_tool_content() {
        let state = ModelContent::Continuation {
            namespace: "provider/native/v1".to_owned(),
            value: serde_json::json!({"opaque": " Ω\n"}),
        };
        let mut message = ModelMessage {
            role: ModelRole::Assistant,
            content: vec![state.clone()],
        };
        message.validate().unwrap();
        let restored: ModelMessage =
            serde_json::from_slice(&serde_json::to_vec(&message).unwrap()).unwrap();
        assert_eq!(restored, message);
        for role in [ModelRole::System, ModelRole::User, ModelRole::Tool] {
            message.role = role;
            assert!(message.validate().is_err());
        }
        message.role = ModelRole::Assistant;
        message.content.push(state);
        assert!(
            message.validate().is_err(),
            "one state per namespace, not one per parallel call"
        );
    }

    #[test]
    fn request_requires_unique_tools_and_namespaced_extensions() {
        let mut request = ModelRequest {
            request_id: ModelRequestId::new("request-1"),
            messages: vec![ModelMessage::text(ModelRole::User, "hello")],
            tools: Vec::new(),
            output_schema: None,
            max_output_tokens: None,
            extensions: BTreeMap::new(),
        };
        assert!(request.validate().is_ok());
        request.extensions.insert("invalid".to_owned(), Value::Null);
        assert_eq!(
            request.validate().expect_err("invalid namespace").code,
            ModelErrorCode::InvalidRequest
        );
    }

    #[test]
    fn event_sequence_and_request_identity_are_explicit() {
        let request_id = ModelRequestId::new("request-1");
        let event = ModelStreamEvent {
            request_id: request_id.clone(),
            event_id: ModelEventId::new("event-1"),
            sequence: 1,
            payload: ModelEvent::TextDelta {
                delta: "hello".to_owned(),
            },
        };
        assert!(event.validate_for(&request_id, 1).is_ok());
        assert_eq!(
            event.validate_for(&request_id, 2).expect_err("gap").code,
            ModelErrorCode::Protocol
        );
    }
}
