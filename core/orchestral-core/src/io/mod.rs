//! Blob IO abstractions.
//!
//! This module defines storage-neutral, streaming blob contracts.
//! Implementations should live in dedicated crates (for example
//! `orchestral-files`) while callers depend on `BlobId` and `BlobStore`.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Strongly-typed blob id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct BlobId(pub String);

impl BlobId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for BlobId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for BlobId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&BlobId> for BlobId {
    fn from(value: &BlobId) -> Self {
        value.clone()
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for BlobId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Streamed blob chunk type.
pub type BlobChunk = Bytes;

/// Streaming blob body.
pub type BlobStream = BoxStream<'static, Result<BlobChunk, BlobIoError>>;

/// Blob metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMeta {
    pub id: BlobId,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    pub byte_size: u64,
    #[serde(default)]
    pub checksum_sha256: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Blob metadata fetched from physical backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobHead {
    pub byte_size: u64,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default)]
    pub last_modified: Option<DateTime<Utc>>,
}

/// Write request.
pub struct BlobWriteRequest {
    pub body: BlobStream,
    pub file_name: Option<String>,
    pub mime_type: Option<String>,
    pub metadata: Value,
}

impl BlobWriteRequest {
    pub fn new(body: BlobStream) -> Self {
        Self {
            body,
            file_name: None,
            mime_type: None,
            metadata: Value::Null,
        }
    }

    pub fn with_file_name(mut self, file_name: Option<String>) -> Self {
        self.file_name = file_name;
        self
    }

    pub fn with_mime_type(mut self, mime_type: Option<String>) -> Self {
        self.mime_type = mime_type;
        self
    }

    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Read result.
pub struct BlobRead {
    pub meta: BlobMeta,
    pub body: BlobStream,
}

/// Storage-neutral blob errors.
#[derive(Debug, thiserror::Error)]
pub enum BlobIoError {
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
    #[error("path outside configured root: {0}")]
    PathOutsideRoot(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for BlobIoError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<serde_json::Error> for BlobIoError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}

/// Blob store abstraction.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Stream-write a blob and return metadata.
    async fn write(&self, request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError>;

    /// Stream-read a blob by id.
    async fn read(&self, blob_id: &BlobId) -> Result<BlobRead, BlobIoError>;

    /// Read blob metadata only.
    async fn head(&self, blob_id: &BlobId) -> Result<BlobHead, BlobIoError>;

    /// Delete a blob by id.
    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError>;
}
