//! Durable, content-addressed filesystem implementation of `BlobStore`.
//!
//! Blob identifiers are the SHA-256 digest of their bytes. Data and metadata
//! are committed with same-directory temporary files, `fsync`, and atomic
//! rename. A process crash may leave an unreferenced temporary/orphan data
//! file, but never a committed metadata record that names incomplete bytes.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobWriteRequest,
};

const DEFAULT_MAX_BLOB_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct FileBlobStore {
    root: Arc<PathBuf>,
    writer_gate: Arc<Mutex<()>>,
    max_blob_bytes: u64,
}

impl FileBlobStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, BlobIoError> {
        Self::open_with_limit(root, DEFAULT_MAX_BLOB_BYTES)
    }

    pub fn open_with_limit(
        root: impl Into<PathBuf>,
        max_blob_bytes: u64,
    ) -> Result<Self, BlobIoError> {
        if max_blob_bytes == 0 {
            return Err(BlobIoError::Invalid(
                "filesystem BlobStore byte limit must be positive".to_owned(),
            ));
        }
        let root = root.into();
        fs::create_dir_all(&root)?;
        let root = fs::canonicalize(root)?;
        if !root.is_dir() {
            return Err(BlobIoError::Invalid(format!(
                "BlobStore root is not a directory: {}",
                root.display()
            )));
        }
        Ok(Self {
            root: Arc::new(root),
            writer_gate: Arc::new(Mutex::new(())),
            max_blob_bytes,
        })
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    pub fn max_blob_bytes(&self) -> u64 {
        self.max_blob_bytes
    }

    fn validate_id(blob_id: &BlobId) -> Result<(), BlobIoError> {
        if blob_id.as_str().len() != 64
            || !blob_id
                .as_str()
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(BlobIoError::Invalid(
                "filesystem blob id must be a SHA-256 digest".to_owned(),
            ));
        }
        Ok(())
    }

    fn data_path(&self, blob_id: &BlobId) -> PathBuf {
        self.root.join(format!("blob-{}.data", blob_id.as_str()))
    }

    fn meta_path(&self, blob_id: &BlobId) -> PathBuf {
        self.root.join(format!("blob-{}.json", blob_id.as_str()))
    }

    fn read_sync(&self, blob_id: &BlobId) -> Result<(BlobMeta, Vec<u8>), BlobIoError> {
        Self::validate_id(blob_id)?;
        let meta_path = self.meta_path(blob_id);
        let data_path = self.data_path(blob_id);
        let meta_bytes = fs::read(&meta_path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => BlobIoError::NotFound(blob_id.to_string()),
            _ => BlobIoError::Io(error.to_string()),
        })?;
        let meta = serde_json::from_slice::<BlobMeta>(&meta_bytes)?;
        let bytes = fs::read(&data_path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => BlobIoError::Internal(format!(
                "committed metadata has no data file: {}",
                blob_id.as_str()
            )),
            _ => BlobIoError::Io(error.to_string()),
        })?;
        let digest = Digest::sha256(&bytes);
        if meta.id != *blob_id
            || meta.byte_size != bytes.len() as u64
            || meta.checksum_sha256.as_deref() != Some(digest.as_str())
            || digest.as_str() != blob_id.as_str()
        {
            return Err(BlobIoError::Internal(format!(
                "blob integrity verification failed: {}",
                blob_id.as_str()
            )));
        }
        Ok((meta, bytes))
    }

    fn write_sync(&self, meta: &BlobMeta, bytes: &[u8]) -> Result<BlobMeta, BlobIoError> {
        let _gate = self
            .writer_gate
            .lock()
            .map_err(|_| BlobIoError::Internal("BlobStore writer lock poisoned".to_owned()))?;
        let data_path = self.data_path(&meta.id);
        let meta_path = self.meta_path(&meta.id);
        if meta_path.exists() {
            return self.read_sync(&meta.id).map(|(stored, _)| stored);
        }
        if data_path.exists() {
            let existing = fs::read(&data_path)?;
            if Digest::sha256(&existing).as_str() != meta.id.as_str() {
                return Err(BlobIoError::Conflict(format!(
                    "orphan blob data conflicts with content address {}",
                    meta.id.as_str()
                )));
            }
        } else {
            atomic_write(&self.root, &data_path, "blob-data", bytes)?;
        }
        let encoded = serde_json::to_vec(meta)?;
        atomic_write(&self.root, &meta_path, "blob-meta", &encoded)?;
        Ok(meta.clone())
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, BlobIoError>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Result<T, BlobIoError> + Send + 'static,
    {
        let store = self.clone();
        tokio::task::spawn_blocking(move || operation(store))
            .await
            .map_err(|error| BlobIoError::Internal(format!("BlobStore worker failed: {error}")))?
    }
}

#[async_trait]
impl BlobStore for FileBlobStore {
    async fn write(&self, mut request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = request.body.next().await {
            let chunk = chunk?;
            let next_size = bytes.len().saturating_add(chunk.len()) as u64;
            if next_size > self.max_blob_bytes {
                return Err(BlobIoError::Invalid(format!(
                    "blob exceeds filesystem store limit of {} bytes",
                    self.max_blob_bytes
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(BlobIoError::Invalid("empty blob payload".to_owned()));
        }
        let digest = Digest::sha256(&bytes);
        let now = chrono::Utc::now();
        let meta = BlobMeta {
            id: BlobId::new(digest.as_str()),
            file_name: request.file_name.take(),
            mime_type: request.mime_type.take(),
            byte_size: bytes.len() as u64,
            checksum_sha256: Some(digest.to_string()),
            metadata: if request.metadata.is_null() {
                serde_json::json!({})
            } else {
                request.metadata
            },
            created_at: now,
            updated_at: now,
        };
        self.blocking(move |store| store.write_sync(&meta, &bytes))
            .await
    }

    async fn read(&self, blob_id: &BlobId) -> Result<BlobRead, BlobIoError> {
        let blob_id = blob_id.clone();
        let (meta, bytes) = self
            .blocking(move |store| store.read_sync(&blob_id))
            .await?;
        Ok(BlobRead {
            meta,
            body: Box::pin(futures_util::stream::once(
                async move { Ok(Bytes::from(bytes)) },
            )),
        })
    }

    async fn head(&self, blob_id: &BlobId) -> Result<BlobHead, BlobIoError> {
        let blob_id = blob_id.clone();
        let (meta, _) = self
            .blocking(move |store| store.read_sync(&blob_id))
            .await?;
        Ok(BlobHead {
            byte_size: meta.byte_size,
            etag: meta.checksum_sha256,
            last_modified: Some(meta.updated_at),
        })
    }

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError> {
        Self::validate_id(blob_id)?;
        let blob_id = blob_id.clone();
        self.blocking(move |store| {
            let _gate = store
                .writer_gate
                .lock()
                .map_err(|_| BlobIoError::Internal("BlobStore writer lock poisoned".to_owned()))?;
            let meta_path = store.meta_path(&blob_id);
            let data_path = store.data_path(&blob_id);
            let existed = meta_path.exists() || data_path.exists();
            remove_if_exists(&meta_path)?;
            remove_if_exists(&data_path)?;
            if existed {
                File::open(store.root.as_path())?.sync_all()?;
            }
            Ok(existed)
        })
        .await
    }
}

fn atomic_write(
    root: &Path,
    destination: &Path,
    prefix: &str,
    bytes: &[u8],
) -> Result<(), BlobIoError> {
    let temporary = root.join(format!(".{prefix}-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, destination)?;
        File::open(root)?.sync_all()?;
        Ok::<(), std::io::Error>(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(BlobIoError::Io(error.to_string()));
    }
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<(), BlobIoError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(BlobIoError::Io(error.to_string())),
    }
}
