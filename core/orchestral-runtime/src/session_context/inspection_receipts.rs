//! Bounded, atomic inspection facts reconstructed from original exchanges.
//!
//! These receipts describe observations, not file contents or current filesystem
//! state. Recompaction must supply the original groups (as the compactor does),
//! rather than parsing facts out of an earlier prose summary.

use std::collections::{BTreeMap, BTreeSet};

use orchestral_core::agent_protocol::wire::ArtifactRefWithDigest;
use orchestral_core::model_protocol::ModelContent;
use serde::Serialize;
use serde_json::Value;

use super::{SessionCompactionGroup, SessionContextError};

type Range = [u64; 2];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Key {
    Read(String, String, String),
    Failed(Option<String>, String, String, Option<u64>),
}

#[derive(Debug, Clone, Serialize)]
struct Receipt {
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace: Option<String>,
    path: String,
    #[serde(flatten)]
    fact: Fact,
    sources: Vec<Range>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Fact {
    Read {
        revision: String,
        /// Only completely returned lines, excluding clipped lines.
        ranges: Vec<Range>,
        next_offset: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        eof_line: Option<u64>,
        truncated: bool,
    },
    Failed {
        code: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        requested_offset: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactReceipt {
    artifact_ref: String,
    #[serde(flatten)]
    fact: ArtifactFact,
    sources: Vec<Range>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ArtifactFact {
    Retained {
        digest: String,
    },
    Page {
        #[serde(skip_serializing_if = "Option::is_none")]
        digest: Option<String>,
        offset: u64,
        next_offset: u64,
        total_bytes: u64,
        complete: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ArtifactKey {
    Retained(String, String),
    Page(String, Option<String>),
}

fn artifact_receipt(result: &Value, source: Range) -> Option<(ArtifactKey, ArtifactReceipt)> {
    let (key, artifact_ref, fact) =
        if result.get("kind").and_then(Value::as_str) == Some("artifact") {
            let artifact =
                serde_json::from_value::<ArtifactRefWithDigest>(result.get("artifact")?.clone())
                    .ok()?;
            if artifact.artifact_ref.as_str().is_empty() || !artifact.digest.is_sha256() {
                return None;
            }
            let reference = artifact.artifact_ref.to_string();
            let digest = artifact.digest.to_string();
            (
                ArtifactKey::Retained(reference.clone(), digest.clone()),
                reference,
                ArtifactFact::Retained { digest },
            )
        } else {
            let reference = nonempty_string(result, "artifact_ref")?;
            let offset = result.get("offset")?.as_u64()?;
            let next_offset = result.get("next_offset")?.as_u64()?;
            let total_bytes = result.get("total_bytes")?.as_u64()?;
            let complete = result.get("complete")?.as_bool()?;
            let bytes = result.get("bytes_read")?.as_u64()?;
            let content = result.get("content")?.as_str()?;
            if next_offset > total_bytes
                || next_offset.checked_sub(offset) != Some(bytes)
                || bytes != content.len() as u64
                || complete != (next_offset == total_bytes)
                || (!complete && bytes == 0)
            {
                return None;
            }
            let digest = match result.get("digest") {
                Some(value) => Some(value.as_str()?.to_owned()),
                None => None,
            };
            (
                ArtifactKey::Page(reference.clone(), digest.clone()),
                reference,
                ArtifactFact::Page {
                    digest,
                    offset,
                    next_offset,
                    total_bytes,
                    complete,
                },
            )
        };
    Some((
        key,
        ArtifactReceipt {
            artifact_ref,
            fact,
            sources: vec![source],
        },
    ))
}

#[derive(Serialize)]
struct Block<'a> {
    file_inspections_v1: &'a [Receipt],
    #[serde(skip_serializing_if = "<[ArtifactReceipt]>::is_empty")]
    artifact_observations_v1: &'a [ArtifactReceipt],
    #[serde(skip_serializing_if = "Vec::is_empty")]
    omitted_sources: Vec<Range>,
    #[serde(skip_serializing_if = "is_zero")]
    omitted_receipts: usize,
    /// When true, sources are a query envelope, not an exact omission list.
    #[serde(skip_serializing_if = "is_false")]
    omitted_sources_envelope: bool,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn merge_ranges(ranges: &mut Vec<Range>) {
    ranges.sort_unstable();
    let mut merged: Vec<Range> = Vec::new();
    for range in ranges.drain(..) {
        if let Some(last) = merged.last_mut() {
            if range[0] <= last[1].saturating_add(1) {
                last[1] = last[1].max(range[1]);
                continue;
            }
        }
        merged.push(range);
    }
    *ranges = merged;
}

fn nonempty_string(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn read_receipt(result: &Value, source: Range) -> Option<(Key, Receipt)> {
    let workspace = nonempty_string(result, "workspace")?;
    let path = nonempty_string(result, "path")?;
    let revision = nonempty_string(result, "revision")?;
    let start = result.get("start_line")?.as_u64()?;
    let end = result.get("end_line")?.as_u64()?;
    let next_offset = result.get("next_offset")?.as_u64()?;
    let eof = result.get("eof")?.as_bool()?;
    let truncated = result.get("truncated")?.as_bool()?;
    let content = result.get("content")?.as_str()?;
    let mut clipped = result
        .get("truncated_line_numbers")?
        .as_array()?
        .iter()
        .map(Value::as_u64)
        .collect::<Option<BTreeSet<_>>>()?;
    let reasons = result.get("truncation_reasons")?.as_array()?;
    if reasons.iter().any(|reason| !reason.is_string()) || truncated == reasons.is_empty() {
        return None;
    }
    let empty_file = start == 1 && end == 0 && eof && content.is_empty() && !truncated;
    if start == 0
        || next_offset != end.checked_add(1)?
        || (!empty_file && end < start)
        || (!truncated && !clipped.is_empty())
        || clipped.iter().any(|line| *line < start || *line > end)
    {
        return None;
    }

    // The byte cap can return a prefix of the very first line without listing
    // it in truncated_line_numbers. Treat this ambiguous single line as partial.
    if start == end && reasons.iter().any(|reason| reason == "byte_limit") {
        clipped.insert(start);
    }

    // A long-line excerpt is not evidence that the complete line was read.
    let mut ranges = Vec::new();
    if !empty_file {
        let mut cursor = start;
        for line in clipped {
            if cursor < line {
                ranges.push([cursor, line - 1]);
            }
            cursor = line.checked_add(1)?;
        }
        if cursor <= end {
            ranges.push([cursor, end]);
        }
    }
    let key = Key::Read(workspace.clone(), path.clone(), revision.clone());
    Some((
        key,
        Receipt {
            workspace: Some(workspace),
            path,
            fact: Fact::Read {
                revision,
                ranges,
                next_offset,
                eof_line: eof.then_some(end),
                truncated,
            },
            sources: vec![source],
        },
    ))
}

fn failed_receipt(arguments: &Value, result: &Value, source: Range) -> Option<(Key, Receipt)> {
    let path = nonempty_string(arguments, "path")?;
    let workspace = nonempty_string(arguments, "workspace");
    let code = nonempty_string(result, "code")?;
    let requested_offset = arguments.get("offset").and_then(Value::as_u64);
    Some((
        Key::Failed(
            workspace.clone(),
            path.clone(),
            code.clone(),
            requested_offset,
        ),
        Receipt {
            workspace,
            path,
            fact: Fact::Failed {
                code,
                requested_offset,
            },
            sources: vec![source],
        },
    ))
}

fn combine(existing: &mut Receipt, incoming: Receipt) {
    existing.sources.extend(incoming.sources);
    merge_ranges(&mut existing.sources);
    if let (
        Fact::Read {
            ranges,
            next_offset,
            eof_line,
            truncated,
            ..
        },
        Fact::Read {
            ranges: more,
            next_offset: next,
            eof_line: known_end,
            truncated: clipped,
            ..
        },
    ) = (&mut existing.fact, incoming.fact)
    {
        ranges.extend(more);
        merge_ranges(ranges);
        *next_offset = next;
        *eof_line = known_end.or(*eof_line);
        *truncated |= clipped;
    }
}

fn render(
    selected: &[Receipt],
    artifacts: &[ArtifactReceipt],
    mut omitted_sources: Vec<Range>,
    omitted_receipts: usize,
    envelope: bool,
) -> Result<String, SessionContextError> {
    merge_ranges(&mut omitted_sources);
    if envelope && omitted_sources.len() > 1 {
        omitted_sources = vec![[
            omitted_sources.first().unwrap()[0],
            omitted_sources.last().unwrap()[1],
        ]];
    }
    serde_json::to_string(&Block {
        file_inspections_v1: selected,
        artifact_observations_v1: artifacts,
        omitted_sources,
        omitted_receipts,
        omitted_sources_envelope: envelope,
    })
    .map_err(|error| SessionContextError::Compaction(error.to_string()))
}

/// Build a complete JSON receipt block within `max_chars`.
///
/// `None` means there were no file-read calls or Artifact outputs. Missing legacy metadata, malformed
/// results and budget exclusions are represented by retrievable source ranges.
/// An insufficient budget for even that omission notice returns an error; callers
/// can preserve the originals instead of committing a silently incomplete summary.
pub(super) fn inspection_receipts<'a>(
    groups: impl IntoIterator<Item = &'a SessionCompactionGroup>,
    max_chars: usize,
) -> Result<Option<String>, SessionContextError> {
    let mut receipts = BTreeMap::new();
    let mut artifacts = BTreeMap::<ArtifactKey, ArtifactReceipt>::new();
    let mut omitted = Vec::new();
    let mut omitted_count = 0;
    let mut has_inspections = false;
    let mut ordered = groups.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|group| group.source.last_session_seq);
    for group in ordered {
        let source = [
            group.source.first_session_seq,
            group.source.last_session_seq,
        ];
        let content = group.messages.iter().flat_map(|message| &message.content);
        for call in content.clone() {
            let ModelContent::ToolCall {
                call_id,
                name,
                arguments,
                ..
            } = call
            else {
                continue;
            };
            let duplicate_calls = content
                .clone()
                .filter(|item| {
                    matches!(item,
                        ModelContent::ToolCall { call_id: other, .. } if other == call_id
                    )
                })
                .count()
                != 1;
            let mut results = content.clone().filter_map(|item| match item {
                ModelContent::ToolResult {
                    call_id: other,
                    result,
                    is_error,
                } if other == call_id => Some((result, *is_error)),
                _ => None,
            });
            let first = results.next();
            let duplicate_results = results.next().is_some();
            let artifact_output = first.is_some_and(|(value, error)| {
                !error
                    && (value.get("kind").and_then(Value::as_str) == Some("artifact")
                        || value.get("artifact_ref").is_some())
            });
            if name != "file_read" && !artifact_output {
                continue;
            }
            has_inspections = true;
            if artifact_output {
                // This is a retrievable observation, never an authorization
                // grant. Artifact reads still verify the durable producer and
                // committed Tool Effect independently of context projection.
                let artifact = first
                    .filter(|_| !duplicate_calls && !duplicate_results)
                    .and_then(|(value, _)| artifact_receipt(value, source));
                if let Some((key, mut receipt)) = artifact {
                    if let Some(existing) = artifacts.get_mut(&key) {
                        existing.sources.append(&mut receipt.sources);
                        merge_ranges(&mut existing.sources);
                        existing.fact = receipt.fact;
                    } else {
                        artifacts.insert(key, receipt);
                    }
                } else {
                    omitted.push(source);
                    omitted_count += 1;
                }
                continue;
            }
            let receipt = if duplicate_calls || duplicate_results {
                None
            } else {
                first.and_then(|(result, is_error)| {
                    if is_error {
                        failed_receipt(arguments, result, source)
                    } else {
                        read_receipt(result, source)
                    }
                })
            };
            if let Some((key, receipt)) = receipt {
                if let Some(existing) = receipts.get_mut(&key) {
                    combine(existing, receipt);
                } else {
                    receipts.insert(key, receipt);
                }
            } else {
                omitted.push(source);
                omitted_count += 1;
            }
        }
    }
    if !has_inspections {
        return Ok(None);
    }
    let mut selected = receipts.into_values().collect::<Vec<_>>();
    selected.sort_by_key(|receipt| receipt.sources.last().map(|source| source[1]));
    let mut selected_artifacts = artifacts.into_values().collect::<Vec<_>>();
    selected_artifacts.sort_by_key(|receipt| receipt.sources.last().map(|source| source[1]));
    loop {
        let exact = render(
            &selected,
            &selected_artifacts,
            omitted.clone(),
            omitted_count,
            false,
        )?;
        if exact.chars().count() <= max_chars {
            return Ok(Some(exact));
        }
        // Compress the omission index itself, while declaring that it is a
        // conservative query envelope. No receipt or JSON value is sliced.
        if !omitted.is_empty() {
            let compact = render(
                &selected,
                &selected_artifacts,
                omitted.clone(),
                omitted_count,
                true,
            )?;
            if compact.chars().count() <= max_chars {
                return Ok(Some(compact));
            }
        }
        if selected.is_empty() && selected_artifacts.is_empty() {
            return Err(SessionContextError::Compaction(
                "inspection receipt budget cannot preserve omission sources".to_owned(),
            ));
        }
        let read_seq = selected
            .first()
            .and_then(|receipt| receipt.sources.last())
            .map(|source| source[1]);
        let artifact_seq = selected_artifacts
            .first()
            .and_then(|receipt| receipt.sources.last())
            .map(|source| source[1]);
        if artifact_seq.is_some_and(|seq| read_seq.is_none_or(|read| seq < read)) {
            omitted.extend(selected_artifacts.remove(0).sources);
        } else {
            omitted.extend(selected.remove(0).sources);
        }
        omitted_count += 1;
    }
}

#[cfg(test)]
mod tests;
