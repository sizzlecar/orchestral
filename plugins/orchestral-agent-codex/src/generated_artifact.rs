use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use bytes::Bytes;
use futures_util::stream;
use orchestral_core::agent_connector::{
    AgentSessionActivity, AgentSessionChangeKind, AgentSessionDetail,
};
use orchestral_core::agent_protocol::wire::{
    ArtifactRef, ArtifactRefWithDigest, Content, ContentBody, Digest,
};
use orchestral_core::io::{BlobId, BlobStore, BlobWriteRequest};
use serde_json::{json, Value};
use tokio::sync::Mutex;

const MAX_GENERATED_IMAGE_BYTES: usize = 64 * 1024 * 1024;
const MAX_GENERATED_IMAGE_BASE64_BYTES: usize =
    MAX_GENERATED_IMAGE_BYTES.div_ceil(3).saturating_mul(4) + 4;

#[derive(Clone)]
pub(crate) struct GeneratedArtifactProjection {
    cache: Arc<Mutex<BTreeMap<String, PublishedGeneratedArtifact>>>,
}

impl Default for GeneratedArtifactProjection {
    fn default() -> Self {
        Self {
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[derive(Debug, Clone)]
struct PublishedGeneratedArtifact {
    artifact: ArtifactRefWithDigest,
    file_name: String,
    media_type: String,
    byte_size: u64,
}

#[derive(Debug)]
struct GeneratedImage<'a> {
    encoded: &'a str,
    file_name: String,
}

impl GeneratedArtifactProjection {
    pub(crate) async fn enrich_detail(
        &self,
        store: Option<&Arc<dyn BlobStore>>,
        session_id: &str,
        native_page: &Value,
        detail: &mut AgentSessionDetail,
    ) {
        let items = native_page
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get("item"))
            .filter_map(|item| {
                item.get("id")
                    .and_then(Value::as_str)
                    .map(|id| (id.to_owned(), item))
            })
            .collect::<BTreeMap<_, _>>();

        for activity in detail
            .turns
            .iter_mut()
            .flat_map(|turn| turn.activities.iter_mut())
        {
            let Some(item) = items.get(activity.activity_id.as_str()) else {
                continue;
            };
            self.enrich_activity(store, session_id, item, activity)
                .await;
        }
    }

    pub(crate) async fn enrich_change(
        &self,
        store: Option<&Arc<dyn BlobStore>>,
        session_id: &str,
        native_message: &Value,
        change: &mut AgentSessionChangeKind,
    ) {
        let AgentSessionChangeKind::ActivityUpsert { activity, .. } = change else {
            return;
        };
        let Some(item) = native_message.pointer("/params/item") else {
            return;
        };
        self.enrich_activity(store, session_id, item, activity)
            .await;
    }

    async fn enrich_activity(
        &self,
        store: Option<&Arc<dyn BlobStore>>,
        session_id: &str,
        item: &Value,
        activity: &mut AgentSessionActivity,
    ) {
        let Some(generated) = generated_image(item) else {
            return;
        };
        let Some(store) = store else {
            activity.content = vec![Content::text(
                "图片已生成，但此 Host 尚未配置 Artifact 存储。",
            )];
            return;
        };
        match self
            .publish(store, session_id, activity.activity_id.as_str(), generated)
            .await
        {
            Ok(published) => {
                activity.content = vec![
                    Content::text(format!("已生成图片：{}", published.file_name)),
                    Content {
                        media_type: published.media_type.clone(),
                        schema_id: None,
                        body: ContentBody::Artifact(published.artifact.clone()),
                    },
                ];
                activity.details["artifactFileName"] = json!(published.file_name);
                activity.details["artifactByteSize"] = json!(published.byte_size);
                activity.details["artifactSha256"] = json!(published.artifact.digest.as_str());
            }
            Err(error) => {
                tracing::warn!(
                    session_id,
                    activity_id = %activity.activity_id,
                    %error,
                    "could not publish generated Agent image"
                );
                activity.content = vec![Content::text(
                    "图片已生成，但下载地址暂时无法发布；Host 将在下次同步时重试。",
                )];
            }
        }
    }

    async fn publish(
        &self,
        store: &Arc<dyn BlobStore>,
        session_id: &str,
        activity_id: &str,
        generated: GeneratedImage<'_>,
    ) -> Result<PublishedGeneratedArtifact, String> {
        let encoded = strip_data_url(generated.encoded)?;
        if encoded.is_empty() || encoded.len() > MAX_GENERATED_IMAGE_BASE64_BYTES {
            return Err("generated image payload is empty or exceeds 64 MiB".to_owned());
        }
        let bytes = BASE64_STANDARD
            .decode(encoded)
            .map_err(|error| format!("generated image payload is not valid base64: {error}"))?;
        if bytes.is_empty() || bytes.len() > MAX_GENERATED_IMAGE_BYTES {
            return Err("generated image payload is empty or exceeds 64 MiB".to_owned());
        }
        let (media_type, extension) = image_format(&bytes)
            .ok_or_else(|| "generated image payload has an unsupported signature".to_owned())?;
        let digest = Digest::sha256(&bytes);
        let cache_key = digest.to_string();

        // Keep the lock across publication. Session snapshots and each live
        // subscriber can observe the same native item concurrently; one
        // content digest must result in at most one outbound upload per Host.
        let mut cache = self.cache.lock().await;
        if let Some(published) = cache.get(&cache_key) {
            return Ok(published.clone());
        }
        let file_name = safe_file_name(&generated.file_name, activity_id, extension);
        let byte_size = bytes.len() as u64;
        if let Ok(head) = store.head(&BlobId::new(digest.as_str())).await {
            if head.byte_size == byte_size {
                let published = PublishedGeneratedArtifact {
                    artifact: ArtifactRefWithDigest {
                        artifact_ref: ArtifactRef::new(digest.as_str()),
                        digest,
                    },
                    file_name,
                    media_type: media_type.to_owned(),
                    byte_size,
                };
                cache.insert(cache_key, published.clone());
                return Ok(published);
            }
        }
        let meta = store
            .write(
                BlobWriteRequest::new(Box::pin(stream::once(
                    async move { Ok(Bytes::from(bytes)) },
                )))
                .with_file_name(Some(file_name.clone()))
                .with_mime_type(Some(media_type.to_owned()))
                .with_metadata(json!({
                    "source": "agent_generated_output",
                    "provider": "codex",
                    "session_id": session_id,
                    "activity_id": activity_id,
                })),
            )
            .await
            .map_err(|error| error.to_string())?;
        if meta.byte_size != byte_size || meta.checksum_sha256.as_deref() != Some(digest.as_str()) {
            return Err("Artifact store returned inconsistent image metadata".to_owned());
        }
        let published = PublishedGeneratedArtifact {
            artifact: ArtifactRefWithDigest {
                artifact_ref: ArtifactRef::new(meta.id.as_str()),
                digest,
            },
            file_name,
            media_type: media_type.to_owned(),
            byte_size,
        };
        published
            .artifact
            .validate_integrity()
            .map_err(|error| error.to_string())?;
        tracing::info!(
            session_id,
            activity_id,
            artifact_ref = %published.artifact.artifact_ref,
            byte_size,
            media_type,
            "published generated Agent image"
        );
        cache.insert(cache_key, published.clone());
        Ok(published)
    }
}

pub(crate) fn is_generated_image_item(item: &Value) -> bool {
    matches!(
        item.get("type").and_then(Value::as_str),
        Some("Extension" | "extension")
    ) && item.get("kind").and_then(Value::as_str) == Some("image_gen.generation")
}

fn generated_image(item: &Value) -> Option<GeneratedImage<'_>> {
    if !is_generated_image_item(item)
        || item.get("status").and_then(Value::as_str) != Some("completed")
        || item
            .get("failure")
            .is_some_and(|failure| !failure.is_null())
    {
        return None;
    }
    let encoded = item.get("result").and_then(Value::as_str)?;
    let fallback = item
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("generated-image");
    let file_name = item
        .get("savedPath")
        .and_then(Value::as_str)
        .and_then(|path| Path::new(path).file_name())
        .and_then(|name| name.to_str())
        .unwrap_or(fallback)
        .to_owned();
    Some(GeneratedImage { encoded, file_name })
}

fn strip_data_url(encoded: &str) -> Result<&str, String> {
    if !encoded.starts_with("data:") {
        return Ok(encoded);
    }
    let (header, body) = encoded
        .split_once(',')
        .ok_or_else(|| "generated image data URL omitted its payload".to_owned())?;
    if !header.starts_with("data:image/") || !header.ends_with(";base64") {
        return Err("generated image data URL is not a base64 image".to_owned());
    }
    Ok(body)
}

fn image_format(bytes: &[u8]) -> Option<(&'static str, &'static str)> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some(("image/png", "png"))
    } else if bytes.starts_with(b"\xff\xd8\xff") {
        Some(("image/jpeg", "jpg"))
    } else if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some(("image/webp", "webp"))
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some(("image/gif", "gif"))
    } else {
        None
    }
}

fn safe_file_name(candidate: &str, activity_id: &str, extension: &str) -> String {
    let valid = candidate.len() <= 255
        && !candidate.trim().is_empty()
        && !candidate.chars().any(char::is_control)
        && Path::new(candidate)
            .file_name()
            .and_then(|name| name.to_str())
            == Some(candidate);
    if valid {
        candidate.to_owned()
    } else {
        let stem = activity_id
            .chars()
            .filter(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_'))
            .take(120)
            .collect::<String>();
        format!(
            "{}.{}",
            if stem.is_empty() {
                "generated-image"
            } else {
                &stem
            },
            extension
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::Utc;
    use orchestral_core::agent_connector::{
        AgentSessionActivityId, AgentSessionActivityKind, AgentSessionActivityStatus,
    };
    use orchestral_core::io::{
        BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobWriteRequest,
    };

    use super::*;

    const ONE_PIXEL_PNG: &str =
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

    struct RecordingBlobStore {
        writes: AtomicUsize,
        heads: AtomicUsize,
        existing_byte_size: Option<u64>,
    }

    #[async_trait]
    impl BlobStore for RecordingBlobStore {
        async fn write(&self, mut request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            let mut bytes = Vec::new();
            use futures_util::StreamExt as _;
            while let Some(chunk) = request.body.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            let digest = Digest::sha256(&bytes).to_string();
            let now = Utc::now();
            Ok(BlobMeta {
                id: BlobId::new(&digest),
                file_name: request.file_name,
                mime_type: request.mime_type,
                byte_size: bytes.len() as u64,
                checksum_sha256: Some(digest),
                metadata: request.metadata,
                created_at: now,
                updated_at: now,
            })
        }

        async fn read(&self, _: &BlobId) -> Result<BlobRead, BlobIoError> {
            Err(BlobIoError::Unsupported("unused".to_owned()))
        }

        async fn head(&self, _: &BlobId) -> Result<BlobHead, BlobIoError> {
            self.heads.fetch_add(1, Ordering::SeqCst);
            self.existing_byte_size
                .map(|byte_size| BlobHead {
                    byte_size,
                    etag: None,
                    last_modified: None,
                })
                .ok_or_else(|| BlobIoError::Unsupported("not present".to_owned()))
        }

        async fn delete(&self, _: &BlobId) -> Result<bool, BlobIoError> {
            Err(BlobIoError::Unsupported("unused".to_owned()))
        }
    }

    fn generated_item(result: &str) -> Value {
        json!({
            "type": "Extension",
            "kind": "image_gen.generation",
            "id": "image-1",
            "status": "completed",
            "result": result,
            "savedPath": "/private/generated/image-1.png",
            "failure": null
        })
    }

    fn activity() -> AgentSessionActivity {
        AgentSessionActivity {
            activity_id: AgentSessionActivityId::new("image-1"),
            kind: AgentSessionActivityKind::AgentMessage,
            status: AgentSessionActivityStatus::Completed,
            title: Some("Generated image".to_owned()),
            content: Vec::new(),
            details: json!({"type": "Extension"}),
        }
    }

    #[tokio::test]
    async fn generated_image_is_uploaded_once_and_projected_as_an_artifact() {
        let store = Arc::new(RecordingBlobStore {
            writes: AtomicUsize::new(0),
            heads: AtomicUsize::new(0),
            existing_byte_size: None,
        });
        let store_trait: Arc<dyn BlobStore> = store.clone();
        let projection = GeneratedArtifactProjection::default();
        let item = generated_item(ONE_PIXEL_PNG);
        let mut first = activity();
        let mut second = activity();

        projection
            .enrich_activity(Some(&store_trait), "thread-1", &item, &mut first)
            .await;
        projection
            .enrich_activity(Some(&store_trait), "thread-1", &item, &mut second)
            .await;

        assert_eq!(store.writes.load(Ordering::SeqCst), 1);
        assert_eq!(first.content, second.content);
        assert_eq!(first.content.len(), 2);
        assert!(matches!(first.content[1].body, ContentBody::Artifact(_)));
        assert_eq!(first.content[1].media_type, "image/png");
        assert_eq!(first.details["artifactFileName"], "image-1.png");
    }

    #[tokio::test]
    async fn invalid_or_non_image_payload_is_never_written() {
        let store = Arc::new(RecordingBlobStore {
            writes: AtomicUsize::new(0),
            heads: AtomicUsize::new(0),
            existing_byte_size: None,
        });
        let store_trait: Arc<dyn BlobStore> = store.clone();
        let projection = GeneratedArtifactProjection::default();
        let mut target = activity();

        projection
            .enrich_activity(
                Some(&store_trait),
                "thread-1",
                &generated_item(&BASE64_STANDARD.encode(b"not an image")),
                &mut target,
            )
            .await;

        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        assert!(matches!(target.content[0].body, ContentBody::Inline(_)));
        assert!(serde_json::to_string(&target.content)
            .unwrap()
            .contains("下次同步时重试"));
    }

    #[tokio::test]
    async fn existing_content_addressed_image_is_reused_without_upload() {
        let expected_size = BASE64_STANDARD.decode(ONE_PIXEL_PNG).unwrap().len() as u64;
        let store = Arc::new(RecordingBlobStore {
            writes: AtomicUsize::new(0),
            heads: AtomicUsize::new(0),
            existing_byte_size: Some(expected_size),
        });
        let store_trait: Arc<dyn BlobStore> = store.clone();
        let projection = GeneratedArtifactProjection::default();
        let mut target = activity();

        projection
            .enrich_activity(
                Some(&store_trait),
                "thread-1",
                &generated_item(ONE_PIXEL_PNG),
                &mut target,
            )
            .await;

        assert_eq!(store.heads.load(Ordering::SeqCst), 1);
        assert_eq!(store.writes.load(Ordering::SeqCst), 0);
        assert!(matches!(target.content[1].body, ContentBody::Artifact(_)));
    }

    #[test]
    fn only_native_completed_image_generation_items_are_accepted() {
        let mut failed = generated_item(ONE_PIXEL_PNG);
        failed["failure"] = json!({"message": "failed"});
        assert!(generated_image(&failed).is_none());
        assert!(generated_image(&json!({
            "type": "Extension",
            "kind": "some.other.extension",
            "status": "completed",
            "result": ONE_PIXEL_PNG
        }))
        .is_none());
    }
}
