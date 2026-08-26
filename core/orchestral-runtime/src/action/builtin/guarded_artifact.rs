//! Guarded model access to Host-persisted Tool result Artifacts.

use std::collections::BTreeSet;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::{ArtifactRef, ArtifactRefWithDigest, Digest};
use orchestral_core::tool_protocol::{
    EffectScope, ModelToolSchema, ToolArtifact, ToolConcurrency, ToolDescriptor, ToolId,
    ToolIdempotency, ToolOutcome, ToolRestriction,
};
use serde_json::{json, Value};

use crate::tool_runtime::{GuardedToolExecution, GuardedToolExecutor, ToolArtifactStore};

const DEFAULT_READ_BYTES: u64 = 32 * 1024;
const HARD_MAX_READ_BYTES: u64 = 64 * 1024;

#[derive(Clone)]
pub struct GuardedArtifactReadExecutor {
    artifacts: ToolArtifactStore,
}

impl GuardedArtifactReadExecutor {
    pub fn new(artifacts: ToolArtifactStore) -> Self {
        Self { artifacts }
    }
}

#[async_trait]
impl GuardedToolExecutor for GuardedArtifactReadExecutor {
    async fn execute(&self, execution: GuardedToolExecution) -> ToolOutcome {
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let arguments = &execution.invocation.arguments;
        let artifact_ref = match required_string(arguments, "artifact_ref") {
            Ok(value) => value,
            Err(outcome) => return outcome,
        };
        let digest = match required_string(arguments, "digest") {
            Ok(value) => Digest::new(value),
            Err(outcome) => return outcome,
        };
        let media_type = match required_string(arguments, "media_type") {
            Ok(value) => value,
            Err(outcome) => return outcome,
        };
        if media_type != "application/json" {
            return rejected(
                "artifact_media_type_unsupported",
                "artifact_read v1 supports application/json Tool results only",
            );
        }
        let byte_size = match required_u64(arguments, "byte_size") {
            Ok(value) if value > 0 => value,
            _ => return rejected("artifact_shape_invalid", "byte_size must be positive"),
        };
        let offset = arguments.get("offset").and_then(Value::as_u64).unwrap_or(0);
        let policy_max = execution
            .effective_policy
            .bounds()
            .max_output_bytes
            .unwrap_or(HARD_MAX_READ_BYTES)
            .saturating_sub(4 * 1024)
            .max(1);
        let max_bytes = arguments
            .get("max_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_READ_BYTES)
            .min(HARD_MAX_READ_BYTES)
            .min(policy_max)
            .max(1);
        let artifact = ToolArtifact {
            artifact: ArtifactRefWithDigest {
                artifact_ref: ArtifactRef::new(artifact_ref),
                digest,
            },
            media_type,
            byte_size,
            summary: "Artifact read request".to_owned(),
        };
        let bytes = match self.artifacts.resolve(&artifact).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return ToolOutcome::Failed {
                    code: "artifact_resolve_failed".to_owned(),
                    message: error.to_string(),
                    retryable: false,
                }
            }
        };
        if execution.cancellation.is_cancelled() {
            return ToolOutcome::Cancelled;
        }
        let content = match std::str::from_utf8(&bytes) {
            Ok(content) => content,
            Err(error) => {
                return ToolOutcome::Failed {
                    code: "artifact_encoding_invalid".to_owned(),
                    message: error.to_string(),
                    retryable: false,
                }
            }
        };
        let start = match usize::try_from(offset) {
            Ok(start) if start <= content.len() && content.is_char_boundary(start) => start,
            _ => {
                return rejected(
                    "artifact_offset_invalid",
                    "offset must identify a UTF-8 boundary within the Artifact",
                )
            }
        };
        let requested_end = start
            .saturating_add(usize::try_from(max_bytes).unwrap_or(usize::MAX))
            .min(content.len());
        let mut end = requested_end;
        while end > start && !content.is_char_boundary(end) {
            end -= 1;
        }
        if end == start && start < content.len() {
            end = (start + 1..=content.len())
                .find(|candidate| content.is_char_boundary(*candidate))
                .unwrap_or(content.len());
        }
        ToolOutcome::Completed {
            output: json!({
                "artifact_ref": artifact.artifact.artifact_ref,
                "digest": artifact.artifact.digest,
                "offset": offset,
                "next_offset": end as u64,
                "bytes_read": (end - start) as u64,
                "total_bytes": byte_size,
                "complete": end == content.len(),
                "content": &content[start..end],
            })
            .into(),
        }
    }
}

pub fn guarded_artifact_read_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/artifact_read/v1"),
        model_schema: ModelToolSchema {
            name: "artifact_read".to_owned(),
            description: "Read a verified chunk from a large Tool result Artifact".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["artifact_ref", "digest", "media_type", "byte_size"],
                "properties": {
                    "artifact_ref": { "type": "string" },
                    "digest": { "type": "string" },
                    "media_type": { "type": "string" },
                    "byte_size": { "type": "integer" },
                    "offset": { "type": "integer" },
                    "max_bytes": { "type": "integer" }
                },
                "additionalProperties": false
            }),
        },
        output_schema: json!({
            "type": "object",
            "required": [
                "artifact_ref", "digest", "offset", "next_offset", "bytes_read",
                "total_bytes", "complete", "content"
            ],
            "properties": {
                "artifact_ref": { "type": "string" },
                "digest": { "type": "string" },
                "offset": { "type": "integer" },
                "next_offset": { "type": "integer" },
                "bytes_read": { "type": "integer" },
                "total_bytes": { "type": "integer" },
                "complete": { "type": "boolean" },
                "content": { "type": "string" }
            },
            "additionalProperties": false
        }),
        effect_scopes: BTreeSet::from([EffectScope::ArtifactRead]),
        restriction,
        idempotency: ToolIdempotency::Pure,
        concurrency: ToolConcurrency::ParallelSafe,
    }
}

fn required_string(arguments: &Value, name: &str) -> Result<String, ToolOutcome> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| rejected("artifact_shape_invalid", format!("{name} must be a string")))
}

fn required_u64(arguments: &Value, name: &str) -> Result<u64, ToolOutcome> {
    arguments.get(name).and_then(Value::as_u64).ok_or_else(|| {
        rejected(
            "artifact_shape_invalid",
            format!("{name} must be an integer"),
        )
    })
}

fn rejected(code: impl Into<String>, message: impl Into<String>) -> ToolOutcome {
    ToolOutcome::Rejected {
        code: code.into(),
        message: message.into(),
    }
}
