//! Guarded model access to Host-persisted Tool result Artifacts.

use std::collections::BTreeSet;

use async_trait::async_trait;
use orchestral_core::agent_protocol::wire::{ArtifactRef, ArtifactRefWithDigest, Digest};
use orchestral_core::tool_protocol::{
    EffectScope, ModelToolSchema, ToolArtifact, ToolConcurrency, ToolDescriptor, ToolId,
    ToolIdempotency, ToolInvocation, ToolOutcome, ToolRestriction,
};
use serde_json::{json, Value};

use crate::tool_runtime::{
    ArtifactReadObservation, GuardedToolExecution, GuardedToolExecutor, ToolArtifactStore,
};

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
    fn planning_contract(&self) -> Value {
        json!({
            "contract": "orchestral.artifact-read/page-envelope/v2",
            "inline_output_limit": self.artifacts.inline_output_limit(),
            "read_observation": "complete-json-pages/v1",
        })
    }

    fn artifact_read_observation(
        &self,
        invocation: &ToolInvocation,
        output: &Value,
    ) -> Option<ArtifactReadObservation> {
        let arguments = &invocation.arguments;
        let offset = output.get("offset")?.as_u64()?;
        let content = output.get("content")?.as_str()?;
        let byte_size = arguments.get("byte_size")?.as_u64()?;
        let end = offset.checked_add(content.len() as u64)?;
        if output.get("artifact_ref")? != arguments.get("artifact_ref")?
            || output.get("digest")? != arguments.get("digest")?
            || output.get("total_bytes")?.as_u64()? != byte_size
            || output.get("bytes_read")?.as_u64()? != content.len() as u64
            || output.get("next_offset")?.as_u64()? != end
            || output.get("complete")?.as_bool()? != (end == byte_size)
            || offset != arguments.get("offset").and_then(Value::as_u64).unwrap_or(0)
            || end > byte_size
        {
            return None;
        }
        Some(ArtifactReadObservation {
            artifact: ArtifactRefWithDigest {
                artifact_ref: ArtifactRef::new(arguments.get("artifact_ref")?.as_str()?),
                digest: Digest::new(arguments.get("digest")?.as_str()?),
            },
            media_type: arguments.get("media_type")?.as_str()?.to_owned(),
            byte_size,
            offset,
            content: content.to_owned(),
        })
    }

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
        let offset = match arguments.get("offset") {
            None => 0,
            Some(value) => match value.as_u64() {
                Some(offset) => offset,
                None => {
                    return rejected(
                        "artifact_shape_invalid",
                        "offset must be a nonnegative integer",
                    )
                }
            },
        };
        let policy_max = execution
            .effective_policy
            .bounds()
            .max_output_bytes
            .unwrap_or(HARD_MAX_READ_BYTES)
            .max(1);
        let inline_limit = self
            .artifacts
            .inline_output_limit()
            .map_or(policy_max, |limit| limit.min(policy_max));
        let max_bytes = match arguments.get("max_bytes") {
            None => DEFAULT_READ_BYTES,
            Some(value) => match value.as_u64() {
                Some(bytes) if bytes > 0 => bytes,
                _ => {
                    return rejected(
                        "artifact_shape_invalid",
                        "max_bytes must be a positive integer",
                    )
                }
            },
        }
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
        match bounded_page(&artifact, content, start, end, inline_limit) {
            Ok(output) => ToolOutcome::Completed {
                output: output.into(),
            },
            Err(outcome) => outcome,
        }
    }
}

// Fit the complete serialized envelope, including escaping and continuation
// metadata. Otherwise a page can spill into another Artifact indefinitely.
fn bounded_page(
    artifact: &ToolArtifact,
    content: &str,
    start: usize,
    end: usize,
    inline_limit: u64,
) -> Result<Value, ToolOutcome> {
    let page = |end: usize| {
        json!({
            "artifact_ref": artifact.artifact.artifact_ref,
            "digest": artifact.artifact.digest,
            "offset": start as u64,
            "next_offset": end as u64,
            "bytes_read": (end - start) as u64,
            "total_bytes": artifact.byte_size,
            "complete": end == content.len(),
            "content": &content[start..end],
        })
    };
    let fits = |value: &Value| {
        serde_jcs::to_vec(value).is_ok_and(|bytes| bytes.len() as u64 <= inline_limit)
    };
    let requested = page(end);
    if fits(&requested) {
        return Ok(requested);
    }
    let boundaries = std::iter::once(start)
        .chain(
            content[start..end]
                .char_indices()
                .map(|(offset, ch)| start + offset + ch.len_utf8()),
        )
        .collect::<Vec<_>>();
    let mut low = 0;
    let mut high = boundaries.len() - 1;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if fits(&page(boundaries[middle])) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let selected = page(boundaries[low]);
    if !fits(&selected) || (boundaries[low] == start && start < content.len()) {
        return Err(rejected(
            "artifact_inline_limit_too_small",
            "Host inline output budget cannot fit the Artifact page metadata and one UTF-8 character",
        ));
    }
    Ok(selected)
}

pub fn guarded_artifact_read_descriptor(restriction: ToolRestriction) -> ToolDescriptor {
    ToolDescriptor {
        tool_id: ToolId::new("orchestral/artifact_read/v1"),
        model_schema: ModelToolSchema {
            name: "artifact_read".to_owned(),
            description: "Read a stored large Tool result without rerunning the Tool. Copy its artifact reference, digest, media_type and byte_size. Content is canonical JSON text; offset is a UTF-8 byte position. Continue at returned next_offset until complete=true when the full result is needed. Pages fit the Host output budget.".to_owned(),
            input_schema: json!({
                "type": "object",
                "required": ["artifact_ref", "digest", "media_type", "byte_size"],
                "properties": {
                    "artifact_ref": { "type": "string", "minLength": 1 },
                    "digest": { "type": "string", "minLength": 1 },
                    "media_type": { "type": "string", "enum": ["application/json"] },
                    "byte_size": { "type": "integer", "minimum": 1 },
                    "offset": { "type": "integer", "minimum": 0, "description": "Byte offset; omit for the first page, then use next_offset." },
                    "max_bytes": { "type": "integer", "minimum": 1, "description": "Requested content bytes; the Host may return a smaller page." }
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

#[cfg(test)]
mod page_tests {
    use super::*;

    #[test]
    fn insufficient_envelope_budget_fails_instead_of_a_nonprogressing_page() {
        let content = "\"你好🦀\"";
        let artifact = ToolArtifact {
            artifact: ArtifactRefWithDigest {
                artifact_ref: ArtifactRef::new("stored-output"),
                digest: Digest::sha256(content.as_bytes()),
            },
            media_type: "application/json".to_owned(),
            byte_size: content.len() as u64,
            summary: String::new(),
        };
        assert!(matches!(
            bounded_page(&artifact, content, 0, content.len(), 1),
            Err(ToolOutcome::Rejected { .. })
        ));
        let full = bounded_page(&artifact, content, 0, content.len(), 1024).unwrap();
        assert_eq!(full["content"], content);
        assert_eq!(full["complete"], true);
        let empty = bounded_page(&artifact, content, content.len(), content.len(), 1024).unwrap();
        assert_eq!(empty["bytes_read"], 0);
        assert_eq!(empty["complete"], true);
    }
}
