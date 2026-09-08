//! Bounded access to original Session records, including compacted history.

use crate::agent_protocol::wire::{Digest, RunId};
use serde::{Deserialize, Serialize};

/// Query the invoking Run's Session. No caller-selected Session is accepted.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SessionReadRequest {
    /// Exclusive sequence cursor for a chronological search/list page.
    #[serde(default)]
    pub after_seq: u64,
    /// Inclusive immutable upper cursor; reuse the returned value across pages.
    pub through_seq: Option<u64>,
    /// Case-insensitive literal search over original record JSON.
    pub query: Option<String>,
    /// Select one original record for exact, paginated reading.
    pub session_seq: Option<u64>,
    /// RFC 6901 pointer into that record; omitted means the complete record.
    pub json_pointer: Option<String>,
    /// UTF-8 byte offset into the selected value's canonical JSON.
    #[serde(default)]
    pub offset: u64,
    /// Requested chunk size, capped by the Host.
    pub max_bytes: Option<u64>,
    /// Requested search page length, capped by the Host.
    pub limit: Option<usize>,
}

/// One search hit. Preview text is explicitly incomplete; use its sequence
/// with `session_read` to inspect original fields and full content.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionReadHit {
    pub session_seq: u64,
    pub run_id: RunId,
    pub kind: String,
    pub digest: Digest,
    pub preview: String,
    pub truncated: bool,
}

/// Chronological page over a fixed prefix of the Session journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionReadPage {
    pub through_seq: u64,
    pub next_after_seq: u64,
    pub complete: bool,
    pub records: Vec<SessionReadHit>,
}

/// Exact UTF-8 chunk of canonical JSON, suitable for reassembly. The digest
/// identifies the entire original record, even when selecting one JSON field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionReadChunk {
    pub through_seq: u64,
    pub session_seq: u64,
    pub digest: Digest,
    pub json_pointer: String,
    pub offset: u64,
    pub next_offset: u64,
    pub total_bytes: u64,
    pub complete: bool,
    pub content: String,
}
