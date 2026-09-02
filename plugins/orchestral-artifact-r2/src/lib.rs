//! Cloudflare R2-backed Artifact storage and access resolution.
//!
//! The plugin talks only to a narrowly scoped Worker control API. Permanent R2
//! credentials stay inside Cloudflare; Agent adapters receive short-lived read
//! URIs through the provider-neutral [`ArtifactResolver`] contract.

use std::collections::BTreeMap;
use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use orchestral_core::agent_protocol::wire::{ArtifactRefWithDigest, Digest};
use orchestral_core::io::{
    ArtifactPublishError, ArtifactPublishRequest, ArtifactPublisher, ArtifactResolveError,
    ArtifactResolver, BlobHead, BlobId, BlobIoError, BlobMeta, BlobRead, BlobStore,
    BlobWriteRequest, ResolvedArtifact,
};
use reqwest::header::{HeaderValue, AUTHORIZATION};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct R2ArtifactStore {
    client: Client,
    internal_url: Url,
    authorization: HeaderValue,
    max_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct WorkerArtifact {
    #[serde(alias = "attachment_id")]
    artifact_ref: String,
    #[serde(default, alias = "sha256")]
    digest: Option<String>,
    #[serde(default)]
    file_name: Option<String>,
    media_type: String,
    byte_size: u64,
    #[serde(default, alias = "agent_url")]
    uri: Option<String>,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
}

impl R2ArtifactStore {
    pub fn from_env_file(path: &Path) -> Result<Self, BlobIoError> {
        let raw = std::fs::read_to_string(path).map_err(BlobIoError::from)?;
        let values = parse_env(&raw)?;
        let internal_url = required(&values, "R2_ARTIFACT_INTERNAL_URL")?;
        let token = required(&values, "R2_ARTIFACT_INTERNAL_TOKEN")?;
        Self::new(internal_url, token, DEFAULT_MAX_BYTES)
    }

    /// Builds the store with a bearer token held in the current user's macOS
    /// Keychain. Only the non-secret service and account names belong in the
    /// process environment or service definition.
    pub fn from_macos_keychain(
        internal_url: &str,
        service: &str,
        account: &str,
    ) -> Result<Self, BlobIoError> {
        #[cfg(target_os = "macos")]
        {
            // Query through Apple's signed `security` client. Calling
            // SecItemCopyMatching directly binds the generic-password ACL to
            // the release binary's changing code requirement and can leave a
            // headless LaunchAgent blocked forever on an invisible Security
            // prompt after deployment. The token remains in the child pipe
            // and this process memory; it is never placed in argv or logs.
            let output = std::process::Command::new("/usr/bin/security")
                .args(["find-generic-password", "-s", service, "-a", account, "-w"])
                .output()
                .map_err(|error| {
                    BlobIoError::Invalid(format!(
                        "cannot invoke macOS Keychain client for R2 Artifact token: {error}"
                    ))
                })?;
            if !output.status.success() {
                let detail = String::from_utf8_lossy(&output.stderr);
                return Err(BlobIoError::Invalid(format!(
                    "cannot read R2 Artifact token from macOS Keychain: {}",
                    detail.trim()
                )));
            }
            let token = String::from_utf8(output.stdout).map_err(|_| {
                BlobIoError::Invalid(
                    "R2 Artifact token in macOS Keychain is not valid UTF-8".to_owned(),
                )
            })?;
            Self::new(
                internal_url,
                token.trim_end_matches(['\r', '\n']),
                DEFAULT_MAX_BYTES,
            )
        }

        #[cfg(not(target_os = "macos"))]
        {
            let _ = (internal_url, service, account);
            Err(BlobIoError::Invalid(
                "macOS Keychain Artifact configuration is only available on macOS".to_owned(),
            ))
        }
    }

    pub fn new(internal_url: &str, token: &str, max_bytes: u64) -> Result<Self, BlobIoError> {
        if max_bytes == 0 {
            return Err(BlobIoError::Invalid(
                "R2 Artifact byte limit must be positive".to_owned(),
            ));
        }
        if token.is_empty() {
            return Err(BlobIoError::Invalid(
                "R2 Artifact Worker token must not be empty".to_owned(),
            ));
        }
        let internal_url = Url::parse(internal_url)
            .map_err(|error| BlobIoError::Invalid(format!("invalid Worker URL: {error}")))?;
        if internal_url.scheme() != "https" || internal_url.host_str().is_none() {
            return Err(BlobIoError::Invalid(
                "R2 Artifact Worker URL must be absolute HTTPS".to_owned(),
            ));
        }
        let authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| BlobIoError::Invalid("invalid Worker API token".to_owned()))?;
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .map_err(|error| BlobIoError::Internal(error.to_string()))?;
        Ok(Self {
            client,
            internal_url,
            authorization,
            max_bytes,
        })
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// HTTPS origin used by browser-visible capability URLs from this Worker.
    /// The composition root uses it to keep the PWA CSP narrow while allowing
    /// direct R2-backed previews.
    pub fn access_origin(&self) -> String {
        self.internal_url.origin().ascii_serialization()
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url, BlobIoError> {
        let mut url = self.internal_url.clone();
        let mut path = url.path_segments_mut().map_err(|_| {
            BlobIoError::Invalid("Worker URL cannot accept path segments".to_owned())
        })?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }

    async fn resolve_worker(&self, id: &str) -> Result<WorkerArtifact, ArtifactResolveError> {
        validate_digest(id).map_err(ArtifactResolveError::Invalid)?;
        let url = self
            .endpoint(&[id, "resolve"])
            .map_err(|error| ArtifactResolveError::Invalid(error.to_string()))?;
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .send()
            .await
            .map_err(|error| ArtifactResolveError::Unavailable(error.to_string()))?;
        decode_resolve_response(response).await
    }
}

#[async_trait]
impl ArtifactResolver for R2ArtifactStore {
    async fn resolve(
        &self,
        artifact: &ArtifactRefWithDigest,
    ) -> Result<ResolvedArtifact, ArtifactResolveError> {
        artifact
            .validate_integrity()
            .map_err(|error| ArtifactResolveError::Invalid(error.to_string()))?;
        let id = artifact.artifact_ref.as_str();
        if artifact.digest.as_str() != id {
            return Err(ArtifactResolveError::Integrity(
                "R2 Artifact reference must equal its content digest".to_owned(),
            ));
        }
        let resolved = self.resolve_worker(id).await?;
        let digest = resolved.digest.as_deref().unwrap_or(&resolved.artifact_ref);
        if resolved.artifact_ref != id || digest != artifact.digest.as_str() {
            return Err(ArtifactResolveError::Integrity(
                "Worker resolved a different Artifact identity".to_owned(),
            ));
        }
        let uri = resolved.uri.ok_or_else(|| {
            ArtifactResolveError::Internal("Worker resolution omitted the read URI".to_owned())
        })?;
        let parsed =
            Url::parse(&uri).map_err(|error| ArtifactResolveError::Internal(error.to_string()))?;
        if parsed.scheme() != "https" || parsed.host_str().is_none() {
            return Err(ArtifactResolveError::Integrity(
                "Worker returned a non-HTTPS Artifact URI".to_owned(),
            ));
        }
        Ok(ResolvedArtifact {
            artifact: artifact.clone(),
            uri,
            file_name: resolved.file_name,
            media_type: resolved.media_type,
            byte_size: resolved.byte_size,
            expires_at: resolved.expires_at,
        })
    }
}

#[async_trait]
impl BlobStore for R2ArtifactStore {
    async fn write(&self, mut request: BlobWriteRequest) -> Result<BlobMeta, BlobIoError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = request.body.next().await {
            let chunk = chunk?;
            let next_size = bytes.len().saturating_add(chunk.len()) as u64;
            if next_size > self.max_bytes {
                return Err(BlobIoError::Invalid(format!(
                    "blob exceeds R2 Artifact limit of {} bytes",
                    self.max_bytes
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(BlobIoError::Invalid("empty blob payload".to_owned()));
        }
        let digest = hex::encode(Sha256::digest(&bytes));
        let file_name = request
            .file_name
            .take()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "artifact.bin".to_owned());
        let media_type = request
            .mime_type
            .take()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_owned());
        let mut url = self.endpoint(&[])?;
        url.query_pairs_mut()
            .append_pair("file_name", &file_name)
            .append_pair("media_type", &media_type);
        let response = self
            .client
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("x-file-size", bytes.len())
            .header("x-file-sha256", &digest)
            .body(bytes)
            .send()
            .await
            .map_err(|error| BlobIoError::Io(error.to_string()))?;
        let uploaded = decode_blob_response(response).await?;
        if uploaded.artifact_ref != digest
            || uploaded.digest.as_deref().unwrap_or(&uploaded.artifact_ref) != digest
            || uploaded.byte_size == 0
        {
            return Err(BlobIoError::Internal(
                "Worker returned inconsistent Artifact metadata".to_owned(),
            ));
        }
        let now = Utc::now();
        Ok(BlobMeta {
            id: BlobId::new(digest.clone()),
            file_name: Some(file_name),
            mime_type: Some(media_type),
            byte_size: uploaded.byte_size,
            checksum_sha256: Some(digest),
            metadata: if request.metadata.is_null() {
                serde_json::json!({})
            } else {
                request.metadata
            },
            created_at: now,
            updated_at: now,
        })
    }

    async fn read(&self, blob_id: &BlobId) -> Result<BlobRead, BlobIoError> {
        let artifact = ArtifactRefWithDigest {
            artifact_ref: orchestral_core::agent_protocol::wire::ArtifactRef::new(blob_id.as_str()),
            digest: Digest::new(blob_id.as_str()),
        };
        let resolved = self
            .resolve(&artifact)
            .await
            .map_err(resolve_to_blob_error)?;
        let response = self
            .client
            .get(&resolved.uri)
            .send()
            .await
            .map_err(|error| BlobIoError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(status_to_blob_error(response.status(), blob_id.as_str()));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| BlobIoError::Io(error.to_string()))?;
        if Digest::sha256(&bytes).as_str() != blob_id.as_str() {
            return Err(BlobIoError::Internal(
                "downloaded R2 Artifact failed digest verification".to_owned(),
            ));
        }
        let now = Utc::now();
        Ok(BlobRead {
            meta: BlobMeta {
                id: blob_id.clone(),
                file_name: resolved.file_name,
                mime_type: Some(resolved.media_type),
                byte_size: resolved.byte_size,
                checksum_sha256: Some(blob_id.to_string()),
                metadata: serde_json::json!({}),
                created_at: now,
                updated_at: now,
            },
            body: Box::pin(futures_util::stream::once(async move { Ok(bytes) })),
        })
    }

    async fn head(&self, blob_id: &BlobId) -> Result<BlobHead, BlobIoError> {
        let artifact = ArtifactRefWithDigest {
            artifact_ref: orchestral_core::agent_protocol::wire::ArtifactRef::new(blob_id.as_str()),
            digest: Digest::new(blob_id.as_str()),
        };
        let resolved = self
            .resolve(&artifact)
            .await
            .map_err(resolve_to_blob_error)?;
        Ok(BlobHead {
            byte_size: resolved.byte_size,
            etag: Some(blob_id.to_string()),
            last_modified: None,
        })
    }

    async fn delete(&self, blob_id: &BlobId) -> Result<bool, BlobIoError> {
        validate_digest(blob_id.as_str()).map_err(BlobIoError::Invalid)?;
        let response = self
            .client
            .delete(self.endpoint(&[blob_id.as_str()])?)
            .header(AUTHORIZATION, self.authorization.clone())
            .send()
            .await
            .map_err(|error| BlobIoError::Io(error.to_string()))?;
        if !response.status().is_success() {
            return Err(status_to_blob_error(response.status(), blob_id.as_str()));
        }
        #[derive(Deserialize)]
        struct Deleted {
            deleted: bool,
        }
        Ok(response
            .json::<Deleted>()
            .await
            .map_err(|error| BlobIoError::Serialization(error.to_string()))?
            .deleted)
    }
}

#[async_trait]
impl ArtifactPublisher for R2ArtifactStore {
    async fn publish(
        &self,
        request: ArtifactPublishRequest,
    ) -> Result<ResolvedArtifact, ArtifactPublishError> {
        if request.workspace_root.as_os_str().is_empty()
            || request.source_path.as_os_str().is_empty()
        {
            return Err(ArtifactPublishError::Invalid(
                "workspace root and source path are required".to_owned(),
            ));
        }
        let workspace_root = tokio::fs::canonicalize(&request.workspace_root)
            .await
            .map_err(|error| publish_path_error(&request.workspace_root, error))?;
        let candidate = if request.source_path.is_absolute() {
            request.source_path.clone()
        } else {
            workspace_root.join(&request.source_path)
        };
        let link_metadata = tokio::fs::symlink_metadata(&candidate)
            .await
            .map_err(|error| publish_path_error(&candidate, error))?;
        if link_metadata.file_type().is_symlink() {
            return Err(ArtifactPublishError::Invalid(
                "symbolic-link artifacts are not allowed".to_owned(),
            ));
        }
        let source_path = tokio::fs::canonicalize(&candidate)
            .await
            .map_err(|error| publish_path_error(&candidate, error))?;
        if !source_path.starts_with(&workspace_root) {
            return Err(ArtifactPublishError::OutsideWorkspace(
                source_path.display().to_string(),
            ));
        }
        let metadata = tokio::fs::metadata(&source_path)
            .await
            .map_err(|error| publish_path_error(&source_path, error))?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > self.max_bytes {
            return Err(ArtifactPublishError::Invalid(format!(
                "artifact must be a regular file between 1 and {} bytes",
                self.max_bytes
            )));
        }
        let file_name = request
            .file_name
            .or_else(|| {
                source_path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
            })
            .ok_or_else(|| ArtifactPublishError::Invalid("file name is required".to_owned()))?;
        if file_name.len() > 255
            || file_name.trim().is_empty()
            || file_name.chars().any(char::is_control)
            || Path::new(&file_name)
                .file_name()
                .and_then(|value| value.to_str())
                != Some(file_name.as_str())
        {
            return Err(ArtifactPublishError::Invalid(
                "file name must be a plain, non-empty name".to_owned(),
            ));
        }
        let media_type = request
            .media_type
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| media_type_for_path(&source_path).to_owned());
        if media_type.len() > 160 || media_type.chars().any(char::is_control) {
            return Err(ArtifactPublishError::Invalid(
                "media type is invalid".to_owned(),
            ));
        }
        let bytes = tokio::fs::read(&source_path)
            .await
            .map_err(|error| ArtifactPublishError::Internal(error.to_string()))?;
        let meta = self
            .write(
                BlobWriteRequest::new(Box::pin(futures_util::stream::once(async move {
                    Ok(Bytes::from(bytes))
                })))
                .with_file_name(Some(file_name))
                .with_mime_type(Some(media_type))
                .with_metadata(serde_json::json!({"source": "agent_workspace"})),
            )
            .await
            .map_err(blob_to_publish_error)?;
        let digest = meta
            .checksum_sha256
            .ok_or_else(|| ArtifactPublishError::Internal("upload omitted digest".to_owned()))?;
        self.resolve(&ArtifactRefWithDigest {
            artifact_ref: orchestral_core::agent_protocol::wire::ArtifactRef::new(&digest),
            digest: Digest::new(digest),
        })
        .await
        .map_err(resolve_to_publish_error)
    }
}

fn publish_path_error(path: &Path, error: std::io::Error) -> ArtifactPublishError {
    if error.kind() == std::io::ErrorKind::NotFound {
        ArtifactPublishError::NotFound(path.display().to_string())
    } else {
        ArtifactPublishError::Internal(error.to_string())
    }
}

fn blob_to_publish_error(error: BlobIoError) -> ArtifactPublishError {
    match error {
        BlobIoError::Invalid(message) => ArtifactPublishError::Invalid(message),
        BlobIoError::NotFound(message) => ArtifactPublishError::NotFound(message),
        BlobIoError::PathOutsideRoot(message) => ArtifactPublishError::OutsideWorkspace(message),
        BlobIoError::Io(message) | BlobIoError::Unsupported(message) => {
            ArtifactPublishError::Unavailable(message)
        }
        BlobIoError::Serialization(message)
        | BlobIoError::Conflict(message)
        | BlobIoError::Internal(message) => ArtifactPublishError::Internal(message),
    }
}

fn resolve_to_publish_error(error: ArtifactResolveError) -> ArtifactPublishError {
    match error {
        ArtifactResolveError::Invalid(message) => ArtifactPublishError::Invalid(message),
        ArtifactResolveError::NotFound(message) => ArtifactPublishError::NotFound(message),
        ArtifactResolveError::Unavailable(message) => ArtifactPublishError::Unavailable(message),
        ArtifactResolveError::Integrity(message) | ArtifactResolveError::Internal(message) => {
            ArtifactPublishError::Internal(message)
        }
    }
}

fn media_type_for_path(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "txt" | "md" | "log" => "text/plain",
        "json" => "application/json",
        "csv" => "text/csv",
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "zip" => "application/zip",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        _ => "application/octet-stream",
    }
}

fn validate_digest(value: &str) -> Result<(), String> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("Artifact id must be a SHA-256 digest".to_owned());
    }
    Ok(())
}

async fn decode_blob_response(response: reqwest::Response) -> Result<WorkerArtifact, BlobIoError> {
    let status = response.status();
    if !status.is_success() {
        return Err(status_to_blob_error(status, "Worker request"));
    }
    response
        .json()
        .await
        .map_err(|error| BlobIoError::Serialization(error.to_string()))
}

async fn decode_resolve_response(
    response: reqwest::Response,
) -> Result<WorkerArtifact, ArtifactResolveError> {
    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Err(ArtifactResolveError::NotFound("R2 Artifact".to_owned()));
    }
    if !status.is_success() {
        return Err(ArtifactResolveError::Unavailable(format!(
            "Worker returned HTTP {status}"
        )));
    }
    response
        .json()
        .await
        .map_err(|error| ArtifactResolveError::Internal(error.to_string()))
}

fn status_to_blob_error(status: StatusCode, subject: &str) -> BlobIoError {
    match status {
        StatusCode::NOT_FOUND => BlobIoError::NotFound(subject.to_owned()),
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
            BlobIoError::Invalid(format!("Worker rejected {subject}"))
        }
        StatusCode::CONFLICT => BlobIoError::Conflict(subject.to_owned()),
        _ => BlobIoError::Io(format!("R2 Artifact Worker returned HTTP {status}")),
    }
}

fn resolve_to_blob_error(error: ArtifactResolveError) -> BlobIoError {
    match error {
        ArtifactResolveError::Invalid(message) => BlobIoError::Invalid(message),
        ArtifactResolveError::NotFound(message) => BlobIoError::NotFound(message),
        ArtifactResolveError::Integrity(message) => BlobIoError::Internal(message),
        ArtifactResolveError::Unavailable(message) => BlobIoError::Io(message),
        ArtifactResolveError::Internal(message) => BlobIoError::Internal(message),
    }
}

fn parse_env(raw: &str) -> Result<BTreeMap<String, String>, BlobIoError> {
    let mut values = BTreeMap::new();
    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or_else(|| {
            BlobIoError::Invalid(format!("invalid Artifact config line {}", index + 1))
        })?;
        values.insert(
            key.trim().to_owned(),
            value
                .trim()
                .trim_matches(|character| character == '\'' || character == '"')
                .to_owned(),
        );
    }
    Ok(values)
}

fn required<'a>(values: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str, BlobIoError> {
    values
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| BlobIoError::Invalid(format!("Artifact config is missing {key}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_https_and_a_non_empty_secret() {
        assert!(R2ArtifactStore::new("http://example.test/v1/internal/blobs", "x", 1).is_err());
        assert!(R2ArtifactStore::new("https://example.test/v1/internal/blobs", "", 1).is_err());
    }

    #[test]
    fn exposes_only_the_validated_worker_origin_for_browser_policy() {
        let store = R2ArtifactStore::new(
            "https://files.example.test/v1/internal/blobs",
            "test-token",
            DEFAULT_MAX_BYTES,
        )
        .unwrap();

        assert_eq!(store.access_origin(), "https://files.example.test");
    }

    #[test]
    fn validates_content_addressed_ids() {
        assert!(validate_digest(&"a".repeat(64)).is_ok());
        assert!(validate_digest("uuid-is-not-a-digest").is_err());
    }

    #[test]
    fn infers_common_download_media_types() {
        assert_eq!(
            media_type_for_path(Path::new("report.xlsx")),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        assert_eq!(media_type_for_path(Path::new("README.md")), "text/plain");
        assert_eq!(
            media_type_for_path(Path::new("artifact.unknown")),
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn publisher_rejects_a_file_outside_the_workspace_before_upload() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(outside.path(), b"private").unwrap();
        let store = R2ArtifactStore::new(
            "https://artifacts.example.test/v1/internal/blobs",
            "test-token",
            DEFAULT_MAX_BYTES,
        )
        .unwrap();

        let error = store
            .publish(ArtifactPublishRequest {
                workspace_root: workspace.path().to_owned(),
                source_path: outside.path().to_owned(),
                file_name: None,
                media_type: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ArtifactPublishError::OutsideWorkspace(_)));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn publisher_rejects_a_symbolic_link() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().unwrap();
        let target = workspace.path().join("target.txt");
        let link = workspace.path().join("link.txt");
        std::fs::write(&target, b"content").unwrap();
        symlink(&target, &link).unwrap();
        let store = R2ArtifactStore::new(
            "https://artifacts.example.test/v1/internal/blobs",
            "test-token",
            DEFAULT_MAX_BYTES,
        )
        .unwrap();

        let error = store
            .publish(ArtifactPublishRequest {
                workspace_root: workspace.path().to_owned(),
                source_path: link,
                file_name: None,
                media_type: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(error, ArtifactPublishError::Invalid(_)));
    }
}
