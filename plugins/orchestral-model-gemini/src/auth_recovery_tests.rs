use super::*;
use axum::{http::HeaderMap, response::IntoResponse, routing::post, Router};
use futures_util::TryStreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const OLD_TOKEN: &str = "private-rejected-fixture-token";
const FRESH_TOKEN: &str = "private-refreshed-fixture-token";
const SUCCESS: &str = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"recovered\"}]},\"finishReason\":\"STOP\"}]}\n\n";

#[derive(Clone, Copy)]
enum Refresh {
    Fresh,
    Same,
    Empty,
    Fail,
    Pending,
}

struct Tokens {
    calls: AtomicUsize,
    refreshes: AtomicUsize,
    refresh: Refresh,
    entered: tokio::sync::Notify,
}

impl Tokens {
    fn new(refresh: Refresh) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            refreshes: AtomicUsize::new(0),
            refresh,
            entered: tokio::sync::Notify::new(),
        })
    }
}

#[async_trait]
impl GeminiAccessTokenProvider for Tokens {
    async fn access_token(&self) -> Result<String, ModelError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if self.refreshes.load(Ordering::SeqCst) > 0 {
            FRESH_TOKEN
        } else {
            OLD_TOKEN
        }
        .to_owned())
    }

    async fn refresh_after_rejection(&self, rejected: &str) -> Result<Option<String>, ModelError> {
        assert_eq!(rejected, OLD_TOKEN);
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        match self.refresh {
            Refresh::Fresh => Ok(Some(FRESH_TOKEN.to_owned())),
            Refresh::Same => Ok(Some(OLD_TOKEN.to_owned())),
            Refresh::Empty => Ok(Some("  ".to_owned())),
            Refresh::Fail => Err(ModelError::new(
                ModelErrorCode::Authentication,
                "credential refresh failed",
            )),
            Refresh::Pending => std::future::pending().await,
        }
    }
}

#[derive(Clone, Copy)]
enum Reply {
    Recover,
    Reject(StatusCode),
    BrokenStream,
}

struct Received {
    authorization: Option<String>,
    body: Bytes,
}

struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<Received>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(reply: Reply) -> Self {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = requests.clone();
        let app = Router::new().route(
            "/{*path}",
            post(move |headers: HeaderMap, body: Bytes| {
                let received = received.clone();
                async move {
                    let authorization = headers
                        .get("authorization")
                        .map(|value| value.to_str().unwrap().to_owned());
                    let fresh = authorization.as_deref() == Some(&format!("Bearer {FRESH_TOKEN}"));
                    received.lock().unwrap().push(Received {
                        authorization,
                        body,
                    });
                    match reply {
                        Reply::Recover if fresh => SUCCESS.into_response(),
                        Reply::BrokenStream => "data: invalid-json\n\n".into_response(),
                        Reply::Recover => (StatusCode::UNAUTHORIZED, "rejected").into_response(),
                        Reply::Reject(status) => (status, "rejected").into_response(),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            endpoint,
            requests,
            server,
        }
    }

    fn backend(&self, authentication: GeminiAuthentication) -> GeminiModelBackend {
        GeminiModelBackend::new(GeminiModelConfig {
            backend_id: "auth-recovery-fixture".into(),
            endpoint: self.endpoint.clone(),
            authentication,
            model: "fixture".into(),
            temperature: 0.0,
            thinking_level: None,
            default_max_output_tokens: 128,
            max_context_tokens: None,
            timeout: Duration::from_secs(2),
            max_buffered_events: 16,
        })
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn request() -> ModelRequest {
    ModelRequest {
        request_id: ModelRequestId::new("auth-recovery"),
        messages: vec![ModelMessage::text(
            ModelRole::User,
            "private-fixture-prompt",
        )],
        tools: Vec::new(),
        output_schema: None,
        max_output_tokens: Some(128),
        extensions: BTreeMap::new(),
    }
}

#[tokio::test]
async fn unauthorized_dynamic_token_refreshes_once_and_preserves_the_request_and_session() {
    let fixture = Fixture::new(Reply::Recover).await;
    let tokens = Tokens::new(Refresh::Fresh);
    let backend = fixture.backend(GeminiAuthentication::AccessTokenProvider(tokens.clone()));
    for _ in 0..2 {
        let events = backend
            .start(request(), CancellationToken::new())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(events.iter().any(|event| matches!(&event.payload,
            ModelEvent::TextDelta { delta } if delta == "recovered")));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.payload, ModelEvent::Finish { .. }))
                .count(),
            1
        );
        assert!(events
            .iter()
            .all(|event| event.request_id == request().request_id));
    }
    assert_eq!(tokens.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(tokens.calls.load(Ordering::SeqCst), 2);
    let received = fixture.requests.lock().unwrap();
    assert_eq!(received.len(), 3);
    assert_eq!(
        received[0].authorization.as_deref(),
        Some(format!("Bearer {OLD_TOKEN}").as_str())
    );
    assert_eq!(
        received[1].authorization.as_deref(),
        Some(format!("Bearer {FRESH_TOKEN}").as_str())
    );
    assert_eq!(received[0].body, received[1].body);
    assert_eq!(received[1].body, received[2].body);
}

#[tokio::test]
async fn repeated_unauthorized_is_terminal_even_when_refresh_returns_the_same_token() {
    for refresh in [Refresh::Fresh, Refresh::Same] {
        let fixture = Fixture::new(Reply::Reject(StatusCode::UNAUTHORIZED)).await;
        let tokens = Tokens::new(refresh);
        let backend = fixture.backend(GeminiAuthentication::AccessTokenProvider(tokens.clone()));
        let error = backend
            .start(request(), CancellationToken::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, ModelErrorCode::Authentication);
        assert!(!error.retryable);
        assert_eq!(error.details["auth_refresh_attempts"], 1);
        assert_eq!(tokens.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.requests.lock().unwrap().len(), 2);
        let diagnostic = serde_json::to_string(&error).unwrap();
        for private in [OLD_TOKEN, FRESH_TOKEN, "private-fixture-prompt"] {
            assert!(!diagnostic.contains(private));
        }
    }
}

#[tokio::test]
async fn static_credentials_and_providers_without_refresh_do_not_retry_unauthorized() {
    struct NoRefresh;
    #[async_trait]
    impl GeminiAccessTokenProvider for NoRefresh {
        async fn access_token(&self) -> Result<String, ModelError> {
            Ok(OLD_TOKEN.to_owned())
        }
    }
    for authentication in [
        GeminiAuthentication::ApiKey("fixture-key".into()),
        GeminiAuthentication::BearerToken(OLD_TOKEN.into()),
        GeminiAuthentication::AccessTokenProvider(Arc::new(NoRefresh)),
    ] {
        let fixture = Fixture::new(Reply::Reject(StatusCode::UNAUTHORIZED)).await;
        let error = fixture
            .backend(authentication)
            .start(request(), CancellationToken::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, ModelErrorCode::Authentication);
        assert!(!error.retryable);
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn permission_rate_limit_and_server_errors_do_not_refresh_credentials() {
    for (status, code, retryable) in [
        (StatusCode::FORBIDDEN, ModelErrorCode::Authentication, false),
        (
            StatusCode::TOO_MANY_REQUESTS,
            ModelErrorCode::RateLimited,
            true,
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            ModelErrorCode::Unavailable,
            true,
        ),
    ] {
        let fixture = Fixture::new(Reply::Reject(status)).await;
        let tokens = Tokens::new(Refresh::Fresh);
        let error = fixture
            .backend(GeminiAuthentication::AccessTokenProvider(tokens.clone()))
            .start(request(), CancellationToken::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, code);
        assert_eq!(error.retryable, retryable);
        assert_eq!(tokens.refreshes.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn refresh_errors_empty_tokens_and_timeouts_do_not_resend() {
    for refresh in [Refresh::Empty, Refresh::Fail, Refresh::Pending] {
        let fixture = Fixture::new(Reply::Recover).await;
        let tokens = Tokens::new(refresh);
        let backend = fixture
            .backend(GeminiAuthentication::AccessTokenProvider(tokens.clone()))
            .with_response_header_timeout(Duration::from_millis(100))
            .unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(3),
            backend.start(request(), CancellationToken::new()),
        )
        .await
        .unwrap()
        .err()
        .unwrap();
        assert_eq!(error.code, ModelErrorCode::Authentication);
        assert_eq!(error.details["phase"], "authentication_refresh");
        assert_eq!(error.details["auth_refresh_attempts"], 1);
        if matches!(refresh, Refresh::Pending) {
            assert_eq!(error.details["timeout"], true);
        }
        assert_eq!(tokens.refreshes.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn cancellation_interrupts_refresh_without_resending() {
    let fixture = Fixture::new(Reply::Recover).await;
    let tokens = Tokens::new(Refresh::Pending);
    let backend = fixture.backend(GeminiAuthentication::AccessTokenProvider(tokens.clone()));
    let cancellation = CancellationToken::new();
    let pending = backend.start(request(), cancellation.clone());
    let cancel = async {
        tokens.entered.notified().await;
        cancellation.cancel();
    };
    let (result, _) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(pending, cancel)
    })
    .await
    .unwrap();
    let error = result.err().unwrap();
    assert_eq!(error.code, ModelErrorCode::Cancelled);
    assert_eq!(error.details["phase"], "authentication_refresh");
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn successful_response_with_broken_stream_is_not_replayed_or_refreshed() {
    let fixture = Fixture::new(Reply::BrokenStream).await;
    let tokens = Tokens::new(Refresh::Fresh);
    let backend = fixture.backend(GeminiAuthentication::AccessTokenProvider(tokens.clone()));
    let error = backend
        .start(request(), CancellationToken::new())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert_eq!(error.code, ModelErrorCode::Protocol);
    assert_eq!(tokens.refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.requests.lock().unwrap().len(), 1);
}
