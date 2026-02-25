//! Blob service implementations.
//!
//! This crate provides a `BlobStore` implementation with local/s3/hybrid
//! physical storage and in-memory/postgres metadata catalogs.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use orchestral_core::io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobStream, BlobWriteRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::HashMap;
use std::path::{Component, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

/// Blob store mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlobMode {
    Local,
    S3,
    Hybrid,
}

/// Service-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobServiceConfig {
    pub mode: BlobMode,
    #[serde(default)]
    pub hybrid_write_to: Option<String>,
}

impl Default for BlobServiceConfig {
    fn default() -> Self {
        Self {
            mode: BlobMode::Local,
            hybrid_write_to: None,
        }
    }
}

/// Local backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalBlobStoreConfig {
    pub root_dir: String,
}

impl Default for LocalBlobStoreConfig {
    fn default() -> Self {
        Self {
            root_dir: ".orchestral/blobs".to_string(),
        }
    }
}

/// S3 backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BlobStoreConfig {
    pub bucket: String,
    #[serde(default)]
    pub key_prefix: String,
}

const LOCAL_STORE_KEY: &str = "local";
const S3_STORE_KEY: &str = "s3";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlobRecord {
    pub id: BlobId,
    pub store: String,
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl BlobRecord {
    fn meta(&self) -> BlobMeta {
        BlobMeta {
            id: self.id.clone(),
            file_name: self.file_name.clone(),
            mime_type: self.mime_type.clone(),
            byte_size: self.byte_size,
            checksum_sha256: self.checksum_sha256.clone(),
            metadata: self.metadata.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct BlobLocation {
    local_path: Option<String>,
    bucket: Option<String>,
    object_key: Option<String>,
}

#[derive(Debug, Clone)]
struct DriverWriteResult {
    location: BlobLocation,
    byte_size: u64,
    checksum_sha256: Option<String>,
}

#[async_trait]
trait BlobCatalog: Send + Sync {
    async fn upsert(&self, record: &BlobRecord) -> Result<(), BlobIoError>;

    async fn get(&self, blob_id: &BlobId) -> Result<Option<BlobRecord>, BlobIoError>;

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError>;
}

/// In-memory blob metadata catalog.
struct InMemoryBlobCatalog {
    records: RwLock<HashMap<String, BlobRecord>>,
}

impl InMemoryBlobCatalog {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for InMemoryBlobCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BlobCatalog for InMemoryBlobCatalog {
    async fn upsert(&self, record: &BlobRecord) -> Result<(), BlobIoError> {
        let mut guard = self.records.write().await;
        guard.insert(record.id.to_string(), record.clone());
        Ok(())
    }

    async fn get(&self, blob_id: &BlobId) -> Result<Option<BlobRecord>, BlobIoError> {
        let guard = self.records.read().await;
        Ok(guard.get(blob_id.as_str()).cloned())
    }

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError> {
        let mut guard = self.records.write().await;
        Ok(guard.remove(blob_id.as_str()).is_some())
    }
}

/// Postgres blob metadata catalog.
struct PostgresBlobCatalog {
    pool: PgPool,
    table_name: String,
}

impl PostgresBlobCatalog {
    pub async fn new(
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, BlobIoError> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(connection_url)
            .await
            .map_err(|e| BlobIoError::Io(e.to_string()))?;
        let prefix = normalize_table_prefix(&table_prefix.into());
        let table_name = format!("{}_blob", prefix);
        let this = Self { pool, table_name };
        this.init_schema().await?;
        Ok(this)
    }

    async fn init_schema(&self) -> Result<(), BlobIoError> {
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id TEXT PRIMARY KEY,
                store TEXT NOT NULL,
                local_path TEXT NULL,
                bucket TEXT NULL,
                object_key TEXT NULL,
                file_name TEXT NULL,
                mime_type TEXT NULL,
                byte_size BIGINT NOT NULL,
                checksum_sha256 TEXT NULL,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                record_json JSONB NOT NULL
            )",
            self.table_name
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| BlobIoError::Io(e.to_string()))?;

        let idx_store_updated = format!(
            "CREATE INDEX IF NOT EXISTS {0}_store_updated_idx ON {1} (store, updated_at DESC)",
            self.table_name, self.table_name
        );
        let idx_created = format!(
            "CREATE INDEX IF NOT EXISTS {0}_created_idx ON {1} (created_at DESC)",
            self.table_name, self.table_name
        );
        sqlx::query(&idx_store_updated)
            .execute(&self.pool)
            .await
            .map_err(|e| BlobIoError::Io(e.to_string()))?;
        sqlx::query(&idx_created)
            .execute(&self.pool)
            .await
            .map_err(|e| BlobIoError::Io(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl BlobCatalog for PostgresBlobCatalog {
    async fn upsert(&self, record: &BlobRecord) -> Result<(), BlobIoError> {
        let byte_size = i64::try_from(record.byte_size)
            .map_err(|_| BlobIoError::Serialization("byte_size exceeds i64 range".to_string()))?;
        let record_json = serde_json::to_value(record)?;
        let sql = format!(
            "INSERT INTO {} (
                id, store, local_path, bucket, object_key, file_name, mime_type,
                byte_size, checksum_sha256, metadata, created_at, updated_at, record_json
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (id) DO UPDATE SET
                store = EXCLUDED.store,
                local_path = EXCLUDED.local_path,
                bucket = EXCLUDED.bucket,
                object_key = EXCLUDED.object_key,
                file_name = EXCLUDED.file_name,
                mime_type = EXCLUDED.mime_type,
                byte_size = EXCLUDED.byte_size,
                checksum_sha256 = EXCLUDED.checksum_sha256,
                metadata = EXCLUDED.metadata,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                record_json = EXCLUDED.record_json",
            self.table_name
        );
        sqlx::query(&sql)
            .bind(record.id.as_str())
            .bind(normalize_store_key(record.store.as_str()))
            .bind(&record.local_path)
            .bind(&record.bucket)
            .bind(&record.object_key)
            .bind(&record.file_name)
            .bind(&record.mime_type)
            .bind(byte_size)
            .bind(&record.checksum_sha256)
            .bind(&record.metadata)
            .bind(record.created_at)
            .bind(record.updated_at)
            .bind(record_json)
            .execute(&self.pool)
            .await
            .map_err(|e| BlobIoError::Io(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, blob_id: &BlobId) -> Result<Option<BlobRecord>, BlobIoError> {
        let sql = format!("SELECT record_json FROM {} WHERE id = $1", self.table_name);
        let row = sqlx::query(&sql)
            .bind(blob_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| BlobIoError::Io(e.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let value: Value = row
            .try_get("record_json")
            .map_err(|e| BlobIoError::Serialization(e.to_string()))?;
        Ok(Some(serde_json::from_value(value)?))
    }

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError> {
        let sql = format!("DELETE FROM {} WHERE id = $1", self.table_name);
        let result = sqlx::query(&sql)
            .bind(blob_id.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| BlobIoError::Io(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }
}

#[async_trait]
trait BlobDriver: Send + Sync {
    fn store_key(&self) -> &str;

    async fn write(
        &self,
        blob_id: &BlobId,
        body: BlobStream,
        file_name: Option<&str>,
        mime_type: Option<&str>,
    ) -> Result<DriverWriteResult, BlobIoError>;

    async fn read(&self, record: &BlobRecord) -> Result<BlobStream, BlobIoError>;

    async fn delete(&self, record: &BlobRecord) -> Result<(), BlobIoError>;

    async fn head(&self, record: &BlobRecord) -> Result<BlobHead, BlobIoError>;
}

/// Local filesystem blob driver.
struct LocalBlobStore {
    root_dir: PathBuf,
}

impl LocalBlobStore {
    pub fn new(config: LocalBlobStoreConfig) -> Self {
        Self {
            root_dir: PathBuf::from(config.root_dir),
        }
    }

    fn ensure_safe_relative(path: &str) -> Result<PathBuf, BlobIoError> {
        let path_buf = PathBuf::from(path);
        if path_buf.is_absolute() {
            return Err(BlobIoError::PathOutsideRoot(path.to_string()));
        }
        for component in path_buf.components() {
            match component {
                Component::Normal(_) | Component::CurDir => {}
                _ => return Err(BlobIoError::PathOutsideRoot(path.to_string())),
            }
        }
        Ok(path_buf)
    }

    fn file_name_for_write(blob_id: &BlobId, input_name: Option<&str>) -> String {
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
        format!("{}/{}", blob_id.as_str(), safe)
    }

    fn local_path(record: &BlobRecord) -> Result<&str, BlobIoError> {
        record
            .local_path
            .as_deref()
            .ok_or_else(|| BlobIoError::Invalid("local_path missing for local blob".to_string()))
    }
}

#[async_trait]
impl BlobDriver for LocalBlobStore {
    fn store_key(&self) -> &str {
        LOCAL_STORE_KEY
    }

    async fn write(
        &self,
        blob_id: &BlobId,
        mut body: BlobStream,
        file_name: Option<&str>,
        _mime_type: Option<&str>,
    ) -> Result<DriverWriteResult, BlobIoError> {
        let relative = Self::file_name_for_write(blob_id, file_name);
        let full_path = self.root_dir.join(&relative);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::File::create(&full_path).await?;
        let mut byte_size: u64 = 0;
        let mut has_any_chunk = false;

        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            if !chunk.is_empty() {
                has_any_chunk = true;
            }
            byte_size = byte_size.saturating_add(chunk.len() as u64);
            file.write_all(&chunk).await?;
        }
        file.flush().await?;

        if !has_any_chunk {
            let _ = tokio::fs::remove_file(&full_path).await;
            return Err(BlobIoError::Invalid("empty blob payload".to_string()));
        }

        Ok(DriverWriteResult {
            location: BlobLocation {
                local_path: Some(relative),
                bucket: None,
                object_key: None,
            },
            byte_size,
            checksum_sha256: None,
        })
    }

    async fn read(&self, record: &BlobRecord) -> Result<BlobStream, BlobIoError> {
        let path = Self::local_path(record)?;
        let relative = Self::ensure_safe_relative(path)?;
        let full_path = self.root_dir.join(relative);
        let file = tokio::fs::File::open(full_path).await?;

        let stream = futures_util::stream::try_unfold(file, |mut file| async move {
            let mut buf = vec![0_u8; 8 * 1024];
            let read = file
                .read(&mut buf)
                .await
                .map_err(|e| BlobIoError::Io(e.to_string()))?;
            if read == 0 {
                Ok(None)
            } else {
                buf.truncate(read);
                Ok(Some((Bytes::from(buf), file)))
            }
        });

        Ok(Box::pin(stream))
    }

    async fn delete(&self, record: &BlobRecord) -> Result<(), BlobIoError> {
        let path = Self::local_path(record)?;
        let relative = Self::ensure_safe_relative(path)?;
        let full_path = self.root_dir.join(relative);
        match tokio::fs::remove_file(full_path).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn head(&self, record: &BlobRecord) -> Result<BlobHead, BlobIoError> {
        let path = Self::local_path(record)?;
        let relative = Self::ensure_safe_relative(path)?;
        let full_path = self.root_dir.join(relative);
        let metadata = tokio::fs::metadata(full_path).await?;
        let last_modified = metadata.modified().ok().map(DateTime::<Utc>::from);
        Ok(BlobHead {
            byte_size: metadata.len(),
            etag: None,
            last_modified,
        })
    }
}

/// Streaming object read result for S3 clients.
pub struct S3GetObjectResult {
    pub body: BlobStream,
    pub byte_size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
}

/// Streaming object write result for S3 clients.
pub struct S3PutObjectResult {
    pub byte_size: u64,
    pub etag: Option<String>,
}

/// Pluggable S3 client adapter.
#[async_trait]
pub trait S3BlobClient: Send + Sync {
    async fn put_object_stream(
        &self,
        bucket: &str,
        key: &str,
        body: BlobStream,
        content_type: Option<&str>,
    ) -> Result<S3PutObjectResult, BlobIoError>;

    async fn get_object_stream(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<S3GetObjectResult, BlobIoError>;

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), BlobIoError>;

    async fn head_object(&self, bucket: &str, key: &str) -> Result<BlobHead, BlobIoError>;
}

/// S3 blob driver.
struct S3BlobStore {
    bucket: String,
    key_prefix: String,
    client: Arc<dyn S3BlobClient>,
}

impl S3BlobStore {
    pub fn new(config: S3BlobStoreConfig, client: Arc<dyn S3BlobClient>) -> Self {
        Self {
            bucket: config.bucket,
            key_prefix: config.key_prefix,
            client,
        }
    }

    fn object_key(&self, blob_id: &BlobId, file_name: Option<&str>) -> String {
        let file_name = file_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("blob.bin");
        let mut key = String::new();
        if !self.key_prefix.trim().is_empty() {
            key.push_str(self.key_prefix.trim_matches('/'));
            key.push('/');
        }
        key.push_str(blob_id.as_str());
        key.push('/');
        key.push_str(file_name);
        key
    }

    fn bucket_and_key(record: &BlobRecord) -> Result<(&str, &str), BlobIoError> {
        let bucket = record
            .bucket
            .as_deref()
            .ok_or_else(|| BlobIoError::Invalid("bucket missing for s3 blob".to_string()))?;
        let key = record
            .object_key
            .as_deref()
            .ok_or_else(|| BlobIoError::Invalid("object_key missing for s3 blob".to_string()))?;
        Ok((bucket, key))
    }
}

#[async_trait]
impl BlobDriver for S3BlobStore {
    fn store_key(&self) -> &str {
        S3_STORE_KEY
    }

    async fn write(
        &self,
        blob_id: &BlobId,
        body: BlobStream,
        file_name: Option<&str>,
        mime_type: Option<&str>,
    ) -> Result<DriverWriteResult, BlobIoError> {
        let key = self.object_key(blob_id, file_name);
        let put = self
            .client
            .put_object_stream(&self.bucket, &key, body, mime_type)
            .await?;

        Ok(DriverWriteResult {
            location: BlobLocation {
                local_path: None,
                bucket: Some(self.bucket.clone()),
                object_key: Some(key),
            },
            byte_size: put.byte_size,
            checksum_sha256: None,
        })
    }

    async fn read(&self, record: &BlobRecord) -> Result<BlobStream, BlobIoError> {
        let (bucket, key) = Self::bucket_and_key(record)?;
        let got = self.client.get_object_stream(bucket, key).await?;
        Ok(got.body)
    }

    async fn delete(&self, record: &BlobRecord) -> Result<(), BlobIoError> {
        let (bucket, key) = Self::bucket_and_key(record)?;
        self.client.delete_object(bucket, key).await
    }

    async fn head(&self, record: &BlobRecord) -> Result<BlobHead, BlobIoError> {
        let (bucket, key) = Self::bucket_and_key(record)?;
        self.client.head_object(bucket, key).await
    }
}

/// File service implementing the core `BlobStore` abstraction.
pub struct FileService {
    config: BlobServiceConfig,
    catalog: Arc<dyn BlobCatalog>,
    local_store: Option<Arc<dyn BlobDriver>>,
    s3_store: Option<Arc<dyn BlobDriver>>,
}

impl FileService {
    fn new(config: BlobServiceConfig, catalog: Arc<dyn BlobCatalog>) -> Self {
        Self {
            config,
            catalog,
            local_store: None,
            s3_store: None,
        }
    }

    pub fn with_in_memory_catalog(config: BlobServiceConfig) -> Self {
        Self::new(config, Arc::new(InMemoryBlobCatalog::new()))
    }

    pub async fn with_postgres_catalog(
        config: BlobServiceConfig,
        connection_url: &str,
        table_prefix: impl Into<String>,
    ) -> Result<Self, BlobIoError> {
        let catalog = PostgresBlobCatalog::new(connection_url, table_prefix).await?;
        Ok(Self::new(config, Arc::new(catalog)))
    }

    pub fn local_default() -> Self {
        Self::with_in_memory_catalog(BlobServiceConfig::default())
            .with_local_defaults(LocalBlobStoreConfig::default())
    }

    pub fn with_local_defaults(mut self, config: LocalBlobStoreConfig) -> Self {
        self.local_store = Some(Arc::new(LocalBlobStore::new(config)));
        self
    }

    pub fn with_s3_client(
        mut self,
        config: S3BlobStoreConfig,
        client: Arc<dyn S3BlobClient>,
    ) -> Self {
        self.s3_store = Some(Arc::new(S3BlobStore::new(config, client)));
        self
    }

    fn write_target_store(&self) -> Result<String, BlobIoError> {
        match self.config.mode {
            BlobMode::Local => Ok(LOCAL_STORE_KEY.to_string()),
            BlobMode::S3 => Ok(S3_STORE_KEY.to_string()),
            BlobMode::Hybrid => {
                if let Some(target) = &self.config.hybrid_write_to {
                    return Ok(normalize_store_key(target));
                }
                if self.s3_store.is_some() {
                    Ok(S3_STORE_KEY.to_string())
                } else if self.local_store.is_some() {
                    Ok(LOCAL_STORE_KEY.to_string())
                } else {
                    Err(BlobIoError::Invalid(
                        "hybrid mode requires at least one blob store".to_string(),
                    ))
                }
            }
        }
    }

    fn store_for_key(&self, key: &str) -> Result<Arc<dyn BlobDriver>, BlobIoError> {
        match normalize_store_key(key).as_str() {
            LOCAL_STORE_KEY => self.local_store.clone().ok_or_else(|| {
                BlobIoError::Invalid("local blob store is not configured".to_string())
            }),
            S3_STORE_KEY => self
                .s3_store
                .clone()
                .ok_or_else(|| BlobIoError::Invalid("s3 blob store is not configured".to_string())),
            other => Err(BlobIoError::Unsupported(format!(
                "blob store '{}' is not configured",
                other
            ))),
        }
    }

    async fn resolve_record(&self, blob_id: &BlobId) -> Result<BlobRecord, BlobIoError> {
        self.catalog
            .get(blob_id)
            .await?
            .ok_or_else(|| BlobIoError::NotFound(blob_id.to_string()))
    }
}

#[async_trait]
impl BlobStore for FileService {
    async fn write(&self, request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
        let blob_id = BlobId::from(uuid::Uuid::new_v4().to_string());
        let target_store = self.write_target_store()?;
        let store = self.store_for_key(target_store.as_str())?;

        let file_name = request.file_name;
        let mime_type = request.mime_type;
        let metadata = if request.metadata.is_null() {
            Value::Object(Default::default())
        } else {
            request.metadata
        };

        let write_result = store
            .write(
                &blob_id,
                request.body,
                file_name.as_deref(),
                mime_type.as_deref(),
            )
            .await?;

        let now = Utc::now();
        let record = BlobRecord {
            id: blob_id,
            store: store.store_key().to_string(),
            local_path: write_result.location.local_path,
            bucket: write_result.location.bucket,
            object_key: write_result.location.object_key,
            file_name,
            mime_type,
            byte_size: write_result.byte_size,
            checksum_sha256: write_result.checksum_sha256,
            metadata,
            created_at: now,
            updated_at: now,
        };
        self.catalog.upsert(&record).await?;
        Ok(record.meta())
    }

    async fn read(&self, blob_id: &BlobId) -> Result<BlobRead, BlobIoError> {
        let record = self.resolve_record(blob_id).await?;
        let store = self.store_for_key(record.store.as_str())?;
        let body = store.read(&record).await?;
        Ok(BlobRead {
            meta: record.meta(),
            body,
        })
    }

    async fn head(&self, blob_id: &BlobId) -> Result<BlobHead, BlobIoError> {
        let record = self.resolve_record(blob_id).await?;
        let store = self.store_for_key(record.store.as_str())?;
        store.head(&record).await
    }

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError> {
        let Some(record) = self.catalog.get(blob_id).await? else {
            return Ok(false);
        };
        let store = self.store_for_key(record.store.as_str())?;
        store.delete(&record).await?;
        self.catalog.delete(blob_id).await
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

fn normalize_store_key(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn one_chunk_stream(bytes: &'static [u8]) -> BlobStream {
        Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(bytes.to_vec()))
        }))
    }

    #[test]
    fn test_file_service_local_write_read_delete() {
        tokio_test::block_on(async {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos();
            let root = std::env::temp_dir().join(format!("orchestral-blobs-{}", suffix));
            tokio::fs::create_dir_all(&root).await.expect("mkdir");

            let service = FileService::with_in_memory_catalog(BlobServiceConfig::default())
                .with_local_defaults(LocalBlobStoreConfig {
                    root_dir: root.display().to_string(),
                });

            let written = service
                .write(
                    BlobWriteRequest::new(one_chunk_stream(b"hello-blob-stream"))
                        .with_file_name(Some("a.txt".to_string()))
                        .with_mime_type(Some("text/plain".to_string()))
                        .with_metadata(serde_json::json!({"source":"test"})),
                )
                .await
                .expect("write");
            assert_eq!(written.byte_size, 17);

            let read = service.read(&written.id).await.expect("read");
            let bytes = read
                .body
                .try_fold(Vec::new(), |mut acc, chunk| async move {
                    acc.extend_from_slice(&chunk);
                    Ok(acc)
                })
                .await
                .expect("collect");
            assert_eq!(bytes, b"hello-blob-stream");

            let head = service.head(&written.id).await.expect("head");
            assert_eq!(head.byte_size, 17);

            assert!(service.delete(&written.id).await.expect("delete"));
            assert!(matches!(
                service.read(&written.id).await,
                Err(BlobIoError::NotFound(_))
            ));

            let _ = tokio::fs::remove_dir_all(root).await;
        });
    }

    #[test]
    fn test_local_store_rejects_unsafe_path() {
        tokio_test::block_on(async {
            let root = std::env::temp_dir().join("orchestral-blobs-unsafe-path");
            tokio::fs::create_dir_all(&root).await.expect("mkdir");
            let local = LocalBlobStore::new(LocalBlobStoreConfig {
                root_dir: root.display().to_string(),
            });

            let record = BlobRecord {
                id: BlobId::from("b-unsafe"),
                store: LOCAL_STORE_KEY.to_string(),
                local_path: Some("../etc/passwd".to_string()),
                bucket: None,
                object_key: None,
                file_name: None,
                mime_type: None,
                byte_size: 0,
                checksum_sha256: None,
                metadata: Value::Null,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            let result = local.read(&record).await;
            assert!(matches!(result, Err(BlobIoError::PathOutsideRoot(_))));
            let _ = tokio::fs::remove_dir_all(root).await;
        });
    }
}
