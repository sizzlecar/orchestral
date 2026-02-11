//! File module implementations.
//!
//! This crate implements core file IO abstractions:
//! - physical backends (local / s3 adapter)
//! - metadata catalogs (in-memory / postgres)
//! - high-level file service

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orchestral_core::io::{
    FileBackend, FileCatalog, FileHead, FileId, FileIoError, FilePayload, FileRecord, FileService,
    FileStatus, StorageBackend, StoredAt, UploadRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// File service mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileMode {
    Local,
    S3,
    Hybrid,
}

/// Service-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileServiceConfig {
    pub mode: FileMode,
    #[serde(default)]
    pub hybrid_write_to: Option<StorageBackend>,
}

impl Default for FileServiceConfig {
    fn default() -> Self {
        Self {
            mode: FileMode::Local,
            hybrid_write_to: None,
        }
    }
}

/// Local backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalFileBackendConfig {
    pub root_dir: String,
}

impl Default for LocalFileBackendConfig {
    fn default() -> Self {
        Self {
            root_dir: ".orchestral/files".to_string(),
        }
    }
}

/// S3 backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3FileBackendConfig {
    pub bucket: String,
    #[serde(default)]
    pub key_prefix: String,
}

/// In-memory metadata catalog.
pub struct InMemoryFileCatalog {
    records: RwLock<HashMap<String, FileRecord>>,
}

impl InMemoryFileCatalog {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryFileCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FileCatalog for InMemoryFileCatalog {
    async fn upsert(&self, record: &FileRecord) -> Result<(), FileIoError> {
        let mut guard = self.records.write().await;
        guard.insert(record.id.to_string(), record.clone());
        Ok(())
    }

    async fn get(&self, file_id: &FileId) -> Result<Option<FileRecord>, FileIoError> {
        let guard = self.records.read().await;
        Ok(guard.get(file_id.as_str()).cloned())
    }

    async fn mark_deleted(
        &self,
        file_id: &FileId,
        deleted_at: DateTime<Utc>,
    ) -> Result<bool, FileIoError> {
        let mut guard = self.records.write().await;
        let Some(record) = guard.get_mut(file_id.as_str()) else {
            return Ok(false);
        };
        record.status = FileStatus::Deleted;
        record.deleted_at = Some(deleted_at);
        record.updated_at = deleted_at;
        Ok(true)
    }

    async fn list_recent(
        &self,
        limit: usize,
        include_deleted: bool,
    ) -> Result<Vec<FileRecord>, FileIoError> {
        let guard = self.records.read().await;
        let mut rows: Vec<FileRecord> = guard.values().cloned().collect();
        if !include_deleted {
            rows.retain(|r| r.status == FileStatus::Active);
        }
        rows.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(rows.into_iter().take(limit).collect())
    }
}

/// Postgres metadata catalog.
pub struct PostgresFileCatalog {
    pool: PgPool,
    table_name: String,
}

impl PostgresFileCatalog {
    pub async fn new(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, FileIoError> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(connection_url)
            .await
            .map_err(|e| FileIoError::Io(e.to_string()))?;
        let prefix = normalize_table_prefix(&table_prefix.into());
        let table_name = format!("{}_file", prefix);
        let this = Self { pool, table_name };
        this.init_schema().await?;
        Ok(this)
    }

    async fn init_schema(&self) -> Result<(), FileIoError> {
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                backend TEXT NOT NULL,
                local_path TEXT NULL,
                bucket TEXT NULL,
                object_key TEXT NULL,
                file_name TEXT NULL,
                mime_type TEXT NULL,
                byte_size BIGINT NOT NULL,
                checksum_sha256 TEXT NULL,
                status TEXT NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                deleted_at TIMESTAMPTZ NULL,
                record_json JSONB NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| FileIoError::Io(e.to_string()))?;

        let idx_status = format!(
            "CREATE INDEX IF NOT EXISTS {0}_status_updated_idx ON {1} (status, updated_at DESC)",
            self.table_name, self.table_name
        );
        let idx_created = format!(
            "CREATE INDEX IF NOT EXISTS {0}_created_idx ON {1} (created_at DESC)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_status)
            .execute(&self.pool)
            .await
            .map_err(|e| FileIoError::Io(e.to_string()))?;
        sqlx::query(&idx_created)
            .execute(&self.pool)
            .await
            .map_err(|e| FileIoError::Io(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl FileCatalog for PostgresFileCatalog {
    async fn upsert(&self, record: &FileRecord) -> Result<(), FileIoError> {
        let byte_size = i64::try_from(record.byte_size)
            .map_err(|_| FileIoError::Serialization("byte_size exceeds i64 range".to_string()))?;
        let status = file_status_label(&record.status);
        let record_json = serde_json::to_value(record)?;
        let sql = format!(
            "INSERT INTO {} (
                id, backend, local_path, bucket, object_key, file_name, mime_type, byte_size, checksum_sha256,
                status, metadata, created_at, updated_at, deleted_at, record_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15
            )
            ON CONFLICT (id) DO UPDATE SET
                backend = EXCLUDED.backend,
                local_path = EXCLUDED.local_path,
                bucket = EXCLUDED.bucket,
                object_key = EXCLUDED.object_key,
                file_name = EXCLUDED.file_name,
                mime_type = EXCLUDED.mime_type,
                byte_size = EXCLUDED.byte_size,
                checksum_sha256 = EXCLUDED.checksum_sha256,
                status = EXCLUDED.status,
                metadata = EXCLUDED.metadata,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                deleted_at = EXCLUDED.deleted_at,
                record_json = EXCLUDED.record_json",
            self.table_name
        );
        sqlx::query(&sql)
            .bind(record.id.as_str())
            .bind(storage_backend_label(&record.backend))
            .bind(&record.local_path)
            .bind(&record.bucket)
            .bind(&record.object_key)
            .bind(&record.file_name)
            .bind(&record.mime_type)
            .bind(byte_size)
            .bind(&record.checksum_sha256)
            .bind(status)
            .bind(&record.metadata)
            .bind(record.created_at)
            .bind(record.updated_at)
            .bind(record.deleted_at)
            .bind(record_json)
            .execute(&self.pool)
            .await
            .map_err(|e| FileIoError::Io(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, file_id: &FileId) -> Result<Option<FileRecord>, FileIoError> {
        let sql = format!("SELECT record_json FROM {} WHERE id = $1", self.table_name);
        let row = sqlx::query(&sql)
            .bind(file_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FileIoError::Io(e.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let value: Value = row
            .try_get("record_json")
            .map_err(|e| FileIoError::Serialization(e.to_string()))?;
        let record: FileRecord = serde_json::from_value(value)?;
        Ok(Some(record))
    }

    async fn mark_deleted(
        &self,
        file_id: &FileId,
        deleted_at: DateTime<Utc>,
    ) -> Result<bool, FileIoError> {
        let Some(mut record) = self.get(file_id).await? else {
            return Ok(false);
        };
        record.status = FileStatus::Deleted;
        record.deleted_at = Some(deleted_at);
        record.updated_at = deleted_at;
        self.upsert(&record).await?;
        Ok(true)
    }

    async fn list_recent(
        &self,
        limit: usize,
        include_deleted: bool,
    ) -> Result<Vec<FileRecord>, FileIoError> {
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| FileIoError::Invalid("limit exceeds i64 range".to_string()))?;
        let sql = if include_deleted {
            format!(
                "SELECT record_json FROM {} ORDER BY created_at DESC LIMIT $1",
                self.table_name
            )
        } else {
            format!(
                "SELECT record_json FROM {} WHERE status = 'active' ORDER BY created_at DESC LIMIT $1",
                self.table_name
            )
        };
        let rows = sqlx::query(&sql)
            .bind(limit_i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FileIoError::Io(e.to_string()))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let value: Value = row
                .try_get("record_json")
                .map_err(|e| FileIoError::Serialization(e.to_string()))?;
            out.push(serde_json::from_value(value)?);
        }
        Ok(out)
    }
}

/// Local filesystem backend.
pub struct LocalFileBackend {
    root_dir: PathBuf,
}

impl LocalFileBackend {
    pub fn new(config: LocalFileBackendConfig) -> Self {
        Self {
            root_dir: PathBuf::from(config.root_dir),
        }
    }

    fn ensure_safe_relative(path: &str) -> Result<PathBuf, FileIoError> {
        let path_buf = PathBuf::from(path);
        if path_buf.is_absolute() {
            return Err(FileIoError::PathOutsideRoot(path.to_string()));
        }
        for component in path_buf.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                _ => return Err(FileIoError::PathOutsideRoot(path.to_string())),
            }
        }
        Ok(path_buf)
    }

    fn file_name_for_upload(file_id: &FileId, input_name: Option<&str>) -> String {
        let raw = input_name.unwrap_or("blob.bin");
        let safe = raw
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                    ch
                } else {
                    '_'
                }
            })
            .collect::<String>();
        format!("{}/{}", file_id.as_str(), safe)
    }
}

#[async_trait]
impl FileBackend for LocalFileBackend {
    fn backend_kind(&self) -> StorageBackend {
        StorageBackend::Local
    }

    async fn upload(
        &self,
        file_id: &FileId,
        request: &UploadRequest,
    ) -> Result<StoredAt, FileIoError> {
        if request.bytes.is_empty() {
            return Err(FileIoError::Invalid("empty file payload".to_string()));
        }
        let relative = Self::file_name_for_upload(file_id, request.file_name.as_deref());
        let full_path = self.root_dir.join(&relative);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&full_path, &request.bytes).await?;
        Ok(StoredAt {
            local_path: Some(relative),
            bucket: None,
            object_key: None,
        })
    }

    async fn download(&self, record: &FileRecord) -> Result<FilePayload, FileIoError> {
        let Some(path) = &record.local_path else {
            return Err(FileIoError::Invalid(
                "local_path missing for local backend file".to_string(),
            ));
        };
        let relative = Self::ensure_safe_relative(path)?;
        let full_path = self.root_dir.join(relative);
        let bytes = tokio::fs::read(&full_path).await?;
        let file_name = full_path
            .file_name()
            .and_then(|s| s.to_str())
            .map(ToString::to_string);
        Ok(FilePayload {
            bytes,
            file_name,
            mime_type: detect_mime(&full_path),
        })
    }

    async fn delete(&self, record: &FileRecord) -> Result<(), FileIoError> {
        let Some(path) = &record.local_path else {
            return Err(FileIoError::Invalid(
                "local_path missing for local backend file".to_string(),
            ));
        };
        let relative = Self::ensure_safe_relative(path)?;
        let full_path = self.root_dir.join(relative);
        match tokio::fs::remove_file(full_path).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn head(&self, record: &FileRecord) -> Result<FileHead, FileIoError> {
        let Some(path) = &record.local_path else {
            return Err(FileIoError::Invalid(
                "local_path missing for local backend file".to_string(),
            ));
        };
        let relative = Self::ensure_safe_relative(path)?;
        let full_path = self.root_dir.join(relative);
        let metadata = tokio::fs::metadata(full_path).await?;
        let last_modified = metadata.modified().ok().map(DateTime::<Utc>::from);
        Ok(FileHead {
            byte_size: metadata.len(),
            etag: None,
            last_modified,
        })
    }
}

/// Pluggable S3 client adapter.
#[async_trait]
pub trait S3FileClient: Send + Sync {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), FileIoError>;

    async fn get_object(&self, bucket: &str, key: &str) -> Result<FilePayload, FileIoError>;

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), FileIoError>;

    async fn head_object(&self, bucket: &str, key: &str) -> Result<FileHead, FileIoError>;
}

/// S3 backend using pluggable client adapter.
pub struct S3FileBackend {
    bucket: String,
    key_prefix: String,
    client: Arc<dyn S3FileClient>,
}

impl S3FileBackend {
    pub fn new(config: S3FileBackendConfig, client: Arc<dyn S3FileClient>) -> Self {
        Self {
            bucket: config.bucket,
            key_prefix: config.key_prefix,
            client,
        }
    }

    fn object_key(&self, file_id: &FileId, file_name: Option<&str>) -> String {
        let file_name = file_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("blob.bin");
        let mut key = String::new();
        if !self.key_prefix.trim().is_empty() {
            key.push_str(self.key_prefix.trim_matches('/'));
            key.push('/');
        }
        key.push_str(file_id.as_str());
        key.push('/');
        key.push_str(file_name);
        key
    }
}

#[async_trait]
impl FileBackend for S3FileBackend {
    fn backend_kind(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn upload(
        &self,
        file_id: &FileId,
        request: &UploadRequest,
    ) -> Result<StoredAt, FileIoError> {
        if request.bytes.is_empty() {
            return Err(FileIoError::Invalid("empty file payload".to_string()));
        }
        let key = self.object_key(file_id, request.file_name.as_deref());
        self.client
            .put_object(
                &self.bucket,
                &key,
                request.bytes.clone(),
                request.mime_type.as_deref(),
            )
            .await?;
        Ok(StoredAt {
            local_path: None,
            bucket: Some(self.bucket.clone()),
            object_key: Some(key),
        })
    }

    async fn download(&self, record: &FileRecord) -> Result<FilePayload, FileIoError> {
        let Some(bucket) = &record.bucket else {
            return Err(FileIoError::Invalid(
                "bucket missing for s3 backend file".to_string(),
            ));
        };
        let Some(key) = &record.object_key else {
            return Err(FileIoError::Invalid(
                "object_key missing for s3 backend file".to_string(),
            ));
        };
        self.client.get_object(bucket, key).await
    }

    async fn delete(&self, record: &FileRecord) -> Result<(), FileIoError> {
        let Some(bucket) = &record.bucket else {
            return Err(FileIoError::Invalid(
                "bucket missing for s3 backend file".to_string(),
            ));
        };
        let Some(key) = &record.object_key else {
            return Err(FileIoError::Invalid(
                "object_key missing for s3 backend file".to_string(),
            ));
        };
        self.client.delete_object(bucket, key).await
    }

    async fn head(&self, record: &FileRecord) -> Result<FileHead, FileIoError> {
        let Some(bucket) = &record.bucket else {
            return Err(FileIoError::Invalid(
                "bucket missing for s3 backend file".to_string(),
            ));
        };
        let Some(key) = &record.object_key else {
            return Err(FileIoError::Invalid(
                "object_key missing for s3 backend file".to_string(),
            ));
        };
        self.client.head_object(bucket, key).await
    }
}

/// High-level file service implementation.
pub struct ManagedFileService {
    config: FileServiceConfig,
    catalog: Arc<dyn FileCatalog>,
    local_backend: Option<Arc<dyn FileBackend>>,
    s3_backend: Option<Arc<dyn FileBackend>>,
}

impl ManagedFileService {
    pub fn new(config: FileServiceConfig, catalog: Arc<dyn FileCatalog>) -> Self {
        Self {
            config,
            catalog,
            local_backend: None,
            s3_backend: None,
        }
    }

    pub fn with_local_backend(mut self, backend: Arc<dyn FileBackend>) -> Self {
        self.local_backend = Some(backend);
        self
    }

    pub fn with_s3_backend(mut self, backend: Arc<dyn FileBackend>) -> Self {
        self.s3_backend = Some(backend);
        self
    }

    fn upload_target(&self) -> Result<StorageBackend, FileIoError> {
        match self.config.mode {
            FileMode::Local => Ok(StorageBackend::Local),
            FileMode::S3 => Ok(StorageBackend::S3),
            FileMode::Hybrid => {
                if let Some(target) = &self.config.hybrid_write_to {
                    return Ok(target.clone());
                }
                if self.s3_backend.is_some() {
                    Ok(StorageBackend::S3)
                } else if self.local_backend.is_some() {
                    Ok(StorageBackend::Local)
                } else {
                    Err(FileIoError::Invalid(
                        "hybrid mode requires at least one backend".to_string(),
                    ))
                }
            }
        }
    }

    fn backend_for_kind(&self, kind: &StorageBackend) -> Result<Arc<dyn FileBackend>, FileIoError> {
        match kind {
            StorageBackend::Local => self
                .local_backend
                .clone()
                .ok_or_else(|| FileIoError::Invalid("local backend is not configured".to_string())),
            StorageBackend::S3 => self
                .s3_backend
                .clone()
                .ok_or_else(|| FileIoError::Invalid("s3 backend is not configured".to_string())),
            StorageBackend::Custom(label) => Err(FileIoError::Unsupported(format!(
                "custom backend '{}' is not configured",
                label
            ))),
        }
    }
}

#[async_trait]
impl FileService for ManagedFileService {
    async fn upload(&self, request: UploadRequest) -> Result<FileRecord, FileIoError> {
        let id = FileId::from(uuid::Uuid::new_v4().to_string());
        let backend_kind = self.upload_target()?;
        let backend = self.backend_for_kind(&backend_kind)?;
        let stored_at = backend.upload(&id, &request).await?;
        let now = Utc::now();
        let record = FileRecord {
            id: id.clone(),
            backend: backend_kind,
            local_path: stored_at.local_path,
            bucket: stored_at.bucket,
            object_key: stored_at.object_key,
            file_name: request.file_name,
            mime_type: request.mime_type,
            byte_size: request.bytes.len() as u64,
            checksum_sha256: None,
            metadata: if request.metadata.is_null() {
                Value::Object(Default::default())
            } else {
                request.metadata
            },
            status: FileStatus::Active,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };
        self.catalog.upsert(&record).await?;
        Ok(record)
    }

    async fn download(&self, file_id: &FileId) -> Result<FilePayload, FileIoError> {
        let Some(record) = self.catalog.get(file_id).await? else {
            return Err(FileIoError::NotFound(file_id.to_string()));
        };
        if record.status == FileStatus::Deleted {
            return Err(FileIoError::NotFound(file_id.to_string()));
        }
        let backend = self.backend_for_kind(&record.backend)?;
        backend.download(&record).await
    }

    async fn delete(&self, file_id: &FileId) -> Result<bool, FileIoError> {
        let Some(record) = self.catalog.get(file_id).await? else {
            return Ok(false);
        };
        let backend = self.backend_for_kind(&record.backend)?;
        backend.delete(&record).await?;
        self.catalog.mark_deleted(file_id, Utc::now()).await
    }

    async fn head(&self, file_id: &FileId) -> Result<FileHead, FileIoError> {
        let Some(record) = self.catalog.get(file_id).await? else {
            return Err(FileIoError::NotFound(file_id.to_string()));
        };
        if record.status == FileStatus::Deleted {
            return Err(FileIoError::NotFound(file_id.to_string()));
        }
        let backend = self.backend_for_kind(&record.backend)?;
        backend.head(&record).await
    }

    async fn resolve(&self, file_id: &FileId) -> Result<Option<FileRecord>, FileIoError> {
        self.catalog.get(file_id).await
    }
}

fn normalize_table_prefix(raw: &str) -> String {
    let candidate = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if candidate.is_empty() {
        "orchestral".to_string()
    } else {
        candidate
    }
}

fn storage_backend_label(backend: &StorageBackend) -> String {
    match backend {
        StorageBackend::Local => "local".to_string(),
        StorageBackend::S3 => "s3".to_string(),
        StorageBackend::Custom(label) => format!("custom:{}", label),
    }
}

fn file_status_label(status: &FileStatus) -> &'static str {
    match status {
        FileStatus::Active => "active",
        FileStatus::Deleted => "deleted",
    }
}

fn detect_mime(path: &Path) -> Option<String> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    let mime = match ext.as_str() {
        "txt" | "md" | "log" => "text/plain",
        "json" => "application/json",
        "yaml" | "yml" => "application/yaml",
        "html" | "htm" => "text/html",
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "mp4" => "video/mp4",
        "csv" => "text/csv",
        _ => return None,
    };
    Some(mime.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_managed_file_service_local_upload_download_delete() {
        tokio_test::block_on(async {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos();
            let root = std::env::temp_dir().join(format!("orchestral-files-{}", suffix));
            tokio::fs::create_dir_all(&root).await.expect("mkdir");

            let catalog: Arc<dyn FileCatalog> = Arc::new(InMemoryFileCatalog::new());
            let local = Arc::new(LocalFileBackend::new(LocalFileBackendConfig {
                root_dir: root.display().to_string(),
            }));
            let service = ManagedFileService::new(FileServiceConfig::default(), catalog)
                .with_local_backend(local);

            let uploaded = service
                .upload(UploadRequest {
                    bytes: b"hello-file-service".to_vec(),
                    file_name: Some("a.txt".to_string()),
                    mime_type: Some("text/plain".to_string()),
                    metadata: serde_json::json!({"source":"test"}),
                })
                .await
                .expect("upload");
            assert_eq!(uploaded.status, FileStatus::Active);
            assert_eq!(uploaded.byte_size, 18);

            let payload = service.download(&uploaded.id).await.expect("download");
            assert_eq!(payload.bytes, b"hello-file-service");

            let head = service.head(&uploaded.id).await.expect("head");
            assert_eq!(head.byte_size, 18);

            assert!(service.delete(&uploaded.id).await.expect("delete"));
            assert!(matches!(
                service.download(&uploaded.id).await,
                Err(FileIoError::NotFound(_))
            ));

            let _ = tokio::fs::remove_dir_all(root).await;
        });
    }

    #[test]
    fn test_local_backend_rejects_unsafe_path() {
        tokio_test::block_on(async {
            let root = std::env::temp_dir().join("orchestral-files-unsafe-path");
            tokio::fs::create_dir_all(&root).await.expect("mkdir");
            let backend = LocalFileBackend::new(LocalFileBackendConfig {
                root_dir: root.display().to_string(),
            });
            let record = FileRecord {
                id: "f-unsafe".into(),
                backend: StorageBackend::Local,
                local_path: Some("../etc/passwd".to_string()),
                bucket: None,
                object_key: None,
                file_name: None,
                mime_type: None,
                byte_size: 0,
                checksum_sha256: None,
                metadata: Value::Null,
                status: FileStatus::Active,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                deleted_at: None,
            };
            let result = backend.download(&record).await;
            assert!(matches!(result, Err(FileIoError::PathOutsideRoot(_))));
            let _ = tokio::fs::remove_dir_all(root).await;
        });
    }
}
