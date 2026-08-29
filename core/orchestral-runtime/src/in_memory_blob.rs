//! Minimal in-memory BlobStore for embedded composition and deterministic tests.

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::sync::RwLock;

use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::io::{
    BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore, BlobWriteRequest,
};

#[derive(Clone)]
struct InMemoryBlobObject {
    meta: BlobMeta,
    bytes: Vec<u8>,
}

#[derive(Default)]
pub struct InMemoryBlobStore {
    objects: RwLock<HashMap<String, InMemoryBlobObject>>,
}

#[async_trait]
impl BlobStore for InMemoryBlobStore {
    async fn write(&self, mut request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
        let blob_id = BlobId::from(uuid::Uuid::new_v4().to_string());
        let mut data = Vec::new();
        while let Some(chunk) = request.body.next().await {
            data.extend_from_slice(&chunk?);
        }
        if data.is_empty() {
            return Err(BlobIoError::Invalid("empty blob payload".to_owned()));
        }
        let now = chrono::Utc::now();
        let meta = BlobMeta {
            id: blob_id,
            file_name: request.file_name.take(),
            mime_type: request.mime_type.take(),
            byte_size: data.len() as u64,
            checksum_sha256: Some(Digest::sha256(&data).to_string()),
            metadata: if request.metadata.is_null() {
                serde_json::json!({})
            } else {
                request.metadata
            },
            created_at: now,
            updated_at: now,
        };
        self.objects.write().await.insert(
            meta.id.to_string(),
            InMemoryBlobObject {
                meta: meta.clone(),
                bytes: data,
            },
        );
        Ok(meta)
    }

    async fn read(&self, blob_id: &BlobId) -> Result<BlobRead, BlobIoError> {
        let object = self
            .objects
            .read()
            .await
            .get(blob_id.as_str())
            .cloned()
            .ok_or_else(|| BlobIoError::NotFound(blob_id.to_string()))?;
        let body = Box::pin(futures_util::stream::once(async move {
            Ok(Bytes::from(object.bytes))
        }));
        Ok(BlobRead {
            meta: object.meta,
            body,
        })
    }

    async fn head(&self, blob_id: &BlobId) -> Result<BlobHead, BlobIoError> {
        let object = self
            .objects
            .read()
            .await
            .get(blob_id.as_str())
            .cloned()
            .ok_or_else(|| BlobIoError::NotFound(blob_id.to_string()))?;
        Ok(BlobHead {
            byte_size: object.meta.byte_size,
            etag: None,
            last_modified: Some(object.meta.updated_at),
        })
    }

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError> {
        Ok(self
            .objects
            .write()
            .await
            .remove(blob_id.as_str())
            .is_some())
    }
}
