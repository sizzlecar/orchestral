//! File IO abstractions.
//!
//! This module defines storage-neutral contracts. Implementations should live in
//! dedicated crates (e.g. `orchestral-files`), while callers interact through
//! file ids and metadata only.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Strongly-typed file id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct FileId(pub String);

impl FileId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for FileId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for FileId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&FileId> for FileId {
    fn from(value: &FileId) -> Self {
        value.clone()
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for FileId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Physical backend kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    Local,
    S3,
    Custom(String),
}

/// Physical object address returned by backend upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StoredAt {
    #[serde(default)]
    pub local_path: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub object_key: Option<String>,
}

/// File lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    Active,
    Deleted,
}

/// Stored file metadata row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecord {
    pub id: FileId,
    pub backend: StorageBackend,
    #[serde(default)]
    pub local_path: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub object_key: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    pub byte_size: u64,
    #[serde(default)]
    pub checksum_sha256: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    pub status: FileStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub deleted_at: Option<DateTime<Utc>>,
}

/// Upload payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRequest {
    pub bytes: Vec<u8>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

/// Download payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePayload {
    pub bytes: Vec<u8>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

/// File metadata fetched from physical backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileHead {
    pub byte_size: u64,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default)]
    pub last_modified: Option<DateTime<Utc>>,
}

/// Storage-neutral file errors.
#[derive(Debug, thiserror::Error)]
pub enum FileIoError {
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

impl From<std::io::Error> for FileIoError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<serde_json::Error> for FileIoError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}

/// Physical backend adapter.
#[async_trait]
pub trait FileBackend: Send + Sync {
    fn backend_kind(&self) -> StorageBackend;

    async fn upload(
        &self,
        file_id: &FileId,
        request: &UploadRequest,
    ) -> Result<StoredAt, FileIoError>;

    async fn download(&self, record: &FileRecord) -> Result<FilePayload, FileIoError>;

    async fn delete(&self, record: &FileRecord) -> Result<(), FileIoError>;

    async fn head(&self, record: &FileRecord) -> Result<FileHead, FileIoError>;
}

/// Metadata catalog.
#[async_trait]
pub trait FileCatalog: Send + Sync {
    async fn upsert(&self, record: &FileRecord) -> Result<(), FileIoError>;

    async fn get(&self, file_id: &FileId) -> Result<Option<FileRecord>, FileIoError>;

    async fn mark_deleted(
        &self,
        file_id: &FileId,
        deleted_at: DateTime<Utc>,
    ) -> Result<bool, FileIoError>;

    async fn list_recent(
        &self,
        limit: usize,
        include_deleted: bool,
    ) -> Result<Vec<FileRecord>, FileIoError>;
}

/// High-level file service used by other modules.
#[async_trait]
pub trait FileService: Send + Sync {
    async fn upload(&self, request: UploadRequest) -> Result<FileRecord, FileIoError>;

    async fn download(&self, file_id: &FileId) -> Result<FilePayload, FileIoError>;

    async fn delete(&self, file_id: &FileId) -> Result<bool, FileIoError>;

    async fn head(&self, file_id: &FileId) -> Result<FileHead, FileIoError>;

    async fn resolve(&self, file_id: &FileId) -> Result<Option<FileRecord>, FileIoError>;
}
