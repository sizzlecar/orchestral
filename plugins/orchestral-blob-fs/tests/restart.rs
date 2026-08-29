use bytes::Bytes;
use futures_util::StreamExt;
use orchestral_blob_fs::FileBlobStore;
use orchestral_core::io::{BlobStore, BlobWriteRequest};

#[tokio::test]
async fn content_address_survives_restart_and_detects_tampering() {
    let root =
        std::env::temp_dir().join(format!("orchestral-blob-fs-test-{}", uuid::Uuid::new_v4()));
    let payload = br#"{"result":"durable artifact"}"#.to_vec();
    let store = FileBlobStore::open(&root).unwrap();
    let meta = store
        .write(BlobWriteRequest::new(Box::pin(futures_util::stream::once(
            {
                let payload = payload.clone();
                async move { Ok(Bytes::from(payload)) }
            },
        ))))
        .await
        .unwrap();
    drop(store);

    let reopened = FileBlobStore::open(&root).unwrap();
    let mut read = reopened.read(&meta.id).await.unwrap();
    let mut observed = Vec::new();
    while let Some(chunk) = read.body.next().await {
        observed.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(observed, payload);
    assert_eq!(read.meta.checksum_sha256, meta.checksum_sha256);

    let data_path = root.join(format!("blob-{}.data", meta.id.as_str()));
    std::fs::write(data_path, b"tampered").unwrap();
    assert!(reopened.read(&meta.id).await.is_err());
    std::fs::remove_dir_all(root).unwrap();
}
