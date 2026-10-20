use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use google_cloud_auth::credentials::service_account::{
    AccessSpecifier, Builder as ServiceAccountBuilder,
};
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder as GoogleCredentialsBuilder};
use orchestral_core::model_protocol::{ModelError, ModelErrorCode};
use reqwest::RequestBuilder;
use tokio_util::sync::CancellationToken;

use super::cancelled_error;

const GOOGLE_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Supplies dynamic bearer tokens for Gemini-compatible endpoints.
#[async_trait]
pub trait GeminiAccessTokenProvider: Send + Sync {
    /// Obtain a token, using the provider's normal caching/refresh policy.
    async fn access_token(&self) -> Result<String, ModelError>;

    /// Recover after the endpoint explicitly rejected `rejected_token` with 401.
    /// Return a fresh token to authorize one retry, or `None` when unsupported.
    /// Implementations should preserve credentials refreshed by another request.
    async fn refresh_after_rejection(
        &self,
        _rejected_token: &str,
    ) -> Result<Option<String>, ModelError> {
        Ok(None)
    }
}

#[derive(Clone)]
pub enum GeminiAuthentication {
    ApiKey(String),
    BearerToken(String),
    AccessTokenProvider(Arc<dyn GeminiAccessTokenProvider>),
}

impl GeminiAuthentication {
    pub(crate) fn is_valid(&self) -> bool {
        match self {
            Self::ApiKey(value) | Self::BearerToken(value) => !value.trim().is_empty(),
            Self::AccessTokenProvider(_) => true,
        }
    }

    pub(crate) async fn apply(
        &self,
        request: RequestBuilder,
        cancellation: &CancellationToken,
    ) -> Result<RequestBuilder, ModelError> {
        self.prepare(request, cancellation)
            .await
            .map(|(request, _)| request)
    }

    pub(crate) async fn prepare(
        &self,
        request: RequestBuilder,
        cancellation: &CancellationToken,
    ) -> Result<(RequestBuilder, Option<String>), ModelError> {
        match self {
            Self::ApiKey(value) => Ok((request.header("x-goog-api-key", value), None)),
            Self::BearerToken(value) => Ok((request.bearer_auth(value), None)),
            Self::AccessTokenProvider(provider) => {
                let token = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(cancelled_error()),
                    token = provider.access_token() => token?,
                };
                validate_access_token(&token)?;
                Ok((request.bearer_auth(&token), Some(token)))
            }
        }
    }
}

pub(crate) fn validate_access_token(token: &str) -> Result<(), ModelError> {
    if token.trim().is_empty() {
        return Err(ModelError::new(
            ModelErrorCode::Authentication,
            "Google access-token provider returned an empty token",
        ));
    }
    Ok(())
}

type CredentialsFactory = dyn Fn() -> Result<AccessTokenCredentials, ModelError> + Send + Sync;

struct GoogleCredentialState {
    credentials: AccessTokenCredentials,
    last_token: Option<String>,
}

/// Google ADC/service-account bridge for Gemini's dynamic bearer-token mode.
///
/// The Google SDK owns normal token caching and refresh. An explicit 401 rebuilds
/// that cache from the original credential source, including when system suspend
/// or a clock adjustment leaves the SDK holding a token the server has rejected.
#[derive(Clone)]
pub struct GoogleCloudAccessTokenProvider {
    factory: Arc<CredentialsFactory>,
    state: Arc<tokio::sync::Mutex<GoogleCredentialState>>,
}

impl std::fmt::Debug for GoogleCloudAccessTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GoogleCloudAccessTokenProvider")
            .finish_non_exhaustive()
    }
}

impl GoogleCloudAccessTokenProvider {
    fn with_factory(factory: Arc<CredentialsFactory>) -> Result<Self, ModelError> {
        let credentials = factory()?;
        Ok(Self {
            factory,
            state: Arc::new(tokio::sync::Mutex::new(GoogleCredentialState {
                credentials,
                last_token: None,
            })),
        })
    }

    /// Resolve Google's standard Application Default Credentials chain.
    pub fn application_default() -> Result<Self, ModelError> {
        Self::with_factory(Arc::new(|| {
            GoogleCredentialsBuilder::default()
                .with_scopes([GOOGLE_CLOUD_PLATFORM_SCOPE])
                .build_access_token_credentials()
                .map_err(|error| {
                    google_authentication_error("resolve Application Default Credentials", error)
                })
        }))
    }

    /// Load an explicitly supplied service-account key without changing process
    /// environment variables or exposing the key outside this provider.
    pub fn from_service_account_file(path: impl AsRef<Path>) -> Result<Self, ModelError> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|error| {
            google_authentication_error(
                &format!("read Google credential file '{}'", path.display()),
                error,
            )
        })?;
        let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
            google_authentication_error(
                &format!("parse Google credential file '{}'", path.display()),
                error,
            )
        })?;
        // Keep the validated source private in memory; a refresh does not require
        // the key file to remain present or read any new environment variables.
        let context = format!("load service-account credential '{}'", path.display());
        Self::with_factory(Arc::new(move || {
            ServiceAccountBuilder::new(value.clone())
                .with_access_specifier(AccessSpecifier::from_scopes([GOOGLE_CLOUD_PLATFORM_SCOPE]))
                .build_access_token_credentials()
                .map_err(|error| google_authentication_error(&context, error))
        }))
    }
}

#[async_trait]
impl GeminiAccessTokenProvider for GoogleCloudAccessTokenProvider {
    async fn access_token(&self) -> Result<String, ModelError> {
        let mut state = self.state.lock().await;
        let token = state
            .credentials
            .access_token()
            .await
            .map(|token| token.token)
            .map_err(|error| google_authentication_error("acquire Google access token", error))?;
        state.last_token = Some(token.clone());
        Ok(token)
    }

    async fn refresh_after_rejection(
        &self,
        rejected_token: &str,
    ) -> Result<Option<String>, ModelError> {
        // Serialize refresh and issuance across clones. A late rejection must not
        // discard credentials another request already refreshed successfully.
        let mut state = self.state.lock().await;
        if state.last_token.as_deref() == Some(rejected_token) {
            state.credentials = (self.factory)()?;
            state.last_token = None;
        }
        let token = state
            .credentials
            .access_token()
            .await
            .map(|token| token.token)
            .map_err(|error| google_authentication_error("refresh Google access token", error))?;
        state.last_token = Some(token.clone());
        Ok(Some(token))
    }
}

fn google_authentication_error(context: &str, error: impl std::fmt::Display) -> ModelError {
    ModelError::new(
        ModelErrorCode::Authentication,
        format!("{context}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Extensions, HeaderMap};
    use google_cloud_auth::credentials::{
        AccessToken, AccessTokenCredentialsProvider, CacheableResource, CredentialsProvider,
    };
    use google_cloud_auth::errors::CredentialsError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct CachedCredentials(String);

    impl CredentialsProvider for CachedCredentials {
        async fn headers(
            &self,
            _: Extensions,
        ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
            unreachable!("the adapter requests raw tokens")
        }

        async fn universe_domain(&self) -> Option<String> {
            None
        }
    }

    impl AccessTokenCredentialsProvider for CachedCredentials {
        async fn access_token(&self) -> Result<AccessToken, CredentialsError> {
            Ok(AccessToken {
                token: self.0.clone(),
            })
        }
    }

    fn provider() -> (GoogleCloudAccessTokenProvider, Arc<AtomicUsize>) {
        let builds = Arc::new(AtomicUsize::new(0));
        let calls = builds.clone();
        let provider = GoogleCloudAccessTokenProvider::with_factory(Arc::new(move || {
            let generation = calls.fetch_add(1, Ordering::SeqCst);
            Ok(CachedCredentials(format!("private-cached-token-{generation}")).into())
        }))
        .unwrap();
        (provider, builds)
    }

    #[tokio::test]
    async fn rejection_rebuilds_the_sdk_cache_and_clones_share_the_new_credentials() {
        let (provider, builds) = provider();
        let clone = provider.clone();
        let rejected = provider.access_token().await.unwrap();
        assert_eq!(clone.access_token().await.unwrap(), rejected);
        assert_eq!(builds.load(Ordering::SeqCst), 1);

        let fresh = provider
            .refresh_after_rejection(&rejected)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(fresh, rejected);
        assert_eq!(clone.access_token().await.unwrap(), fresh);
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert!(!format!("{provider:?}").contains("private-cached-token"));
    }

    #[tokio::test]
    async fn concurrent_and_late_rejections_preserve_already_refreshed_credentials() {
        let (provider, builds) = provider();
        let clone = provider.clone();
        let rejected = provider.access_token().await.unwrap();
        let (first, second) = tokio::join!(
            provider.refresh_after_rejection(&rejected),
            clone.refresh_after_rejection(&rejected),
        );
        assert_eq!(first.as_ref().unwrap(), second.as_ref().unwrap());
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(
            provider.refresh_after_rejection(&rejected).await.unwrap(),
            first.unwrap()
        );
        assert_eq!(builds.load(Ordering::SeqCst), 2);
    }
}
