use std::future::Future;

use futures_util::StreamExt;
use gloo_net::http::{Request, RequestBuilder, Response};
use js_sys::Uint8Array;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use thiserror::Error;
use wasm_bindgen::JsValue;

use crate::browser::platform::new_uuid;
use crate::model::{
    AgentConnectorView, AgentSessionActionOutcome, AgentSessionChangeKindView,
    AgentSessionChangeView, AgentSessionDetail, AgentSessionPage, AgentSessionSummary, DeviceView,
    EventsResponse, PairingClaim, SessionView, StreamEvent, UploadedArtifact,
};
use crate::sse::{SessionSequenceDisposition, SessionSequenceGuard, SseParser};

const API_BASE: &str = "/api/v1";

pub struct AgentInputRequest<'a> {
    pub text: &'a str,
    pub attachments: &'a [UploadedArtifact],
    pub after_activity_id: Option<&'a str>,
}

#[derive(Debug, Clone, Error, PartialEq)]
#[error("{message}")]
pub struct ApiError {
    pub message: String,
    pub status: u16,
    pub code: String,
    pub details: Option<Value>,
}

impl ApiError {
    fn transport(error: impl std::fmt::Display) -> Self {
        Self {
            message: error.to_string(),
            status: 0,
            code: "network_error".to_owned(),
            details: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ApiClient;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiCredential {
    DeviceToken(String),
    GatewaySession,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AgentSessionObservation {
    NotModified {
        etag: Option<String>,
    },
    Modified {
        detail: Box<AgentSessionDetail>,
        etag: Option<String>,
    },
}

impl ApiClient {
    pub async fn claim_pairing(
        &self,
        secret: &str,
        device_name: &str,
    ) -> Result<PairingClaim, ApiError> {
        self.post_public(
            "/pairing/claim",
            &json!({ "secret": secret, "device_name": device_name }),
        )
        .await
    }

    pub async fn me(&self, credential: &ApiCredential) -> Result<Value, ApiError> {
        self.get("/me", credential).await
    }

    pub async fn devices(&self, credential: &ApiCredential) -> Result<Vec<DeviceView>, ApiError> {
        self.get("/devices", credential).await
    }

    pub async fn revoke_device(
        &self,
        credential: &ApiCredential,
        device_id: &str,
    ) -> Result<(), ApiError> {
        let response = self
            .authenticated(
                Request::delete(&format!("{API_BASE}/devices/{}", encode(device_id))),
                credential,
            )
            .send()
            .await
            .map_err(ApiError::transport)?;
        expect_empty(response).await
    }

    pub async fn sessions(&self, credential: &ApiCredential) -> Result<Vec<SessionView>, ApiError> {
        self.get("/sessions", credential).await
    }

    pub async fn create_session(
        &self,
        credential: &ApiCredential,
    ) -> Result<SessionView, ApiError> {
        self.post(
            "/sessions",
            credential,
            &json!({ "session_id": new_uuid()? }),
        )
        .await
    }

    pub async fn agent_connectors(
        &self,
        credential: &ApiCredential,
    ) -> Result<Vec<AgentConnectorView>, ApiError> {
        self.get("/agent-connectors", credential).await
    }

    pub async fn agent_sessions(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentSessionPage, ApiError> {
        let mut path = format!(
            "/agent-sessions?connector_id={}&limit={limit}",
            encode(connector_id)
        );
        if let Some(cursor) = cursor {
            path.push_str("&cursor=");
            path.push_str(&encode(cursor));
        }
        self.get(&path, credential).await
    }

    pub async fn create_agent_session(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        cwd: Option<&str>,
        title: Option<&str>,
        options: Value,
    ) -> Result<AgentSessionSummary, ApiError> {
        self.post(
            "/agent-sessions",
            credential,
            &json!({
                "connector_id": connector_id,
                "cwd": cwd,
                "title": title,
                "options": options,
            }),
        )
        .await
    }

    pub async fn agent_session(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentSessionDetail, ApiError> {
        let path = agent_session_path(connector_id, session_id, cursor, limit);
        self.get(&path, credential).await
    }

    /// Fetches an Agent session snapshot that can be interrupted when the
    /// selected session or connection mode changes.
    pub async fn observe_agent_session(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        limit: u32,
        signal: &web_sys::AbortSignal,
        etag: Option<&str>,
    ) -> Result<AgentSessionObservation, ApiError> {
        let path = agent_session_path(connector_id, session_id, None, limit);
        let mut request = Request::get(&format!("{API_BASE}{path}")).abort_signal(Some(signal));
        if let Some(etag) = etag {
            request = request.header("If-None-Match", etag);
        }
        let response = self
            .authenticated(request, credential)
            .send()
            .await
            .map_err(ApiError::transport)?;
        let response_etag = response.headers().get("ETag");
        if response.status() == 304 {
            return Ok(AgentSessionObservation::NotModified {
                etag: response_etag.or_else(|| etag.map(str::to_owned)),
            });
        }
        if !response.ok() {
            return Err(error_response(response).await);
        }
        let detail = response.json().await.map_err(ApiError::transport)?;
        Ok(AgentSessionObservation::Modified {
            detail: Box::new(detail),
            etag: response_etag,
        })
    }

    pub async fn invoke_agent_session_action(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        action_id: &str,
        arguments: Value,
        run_id: Option<&str>,
    ) -> Result<AgentSessionActionOutcome, ApiError> {
        self.post(
            "/agent-session/actions",
            credential,
            &json!({
                "connector_id": connector_id,
                "session_id": session_id,
                "action_id": action_id,
                "arguments": arguments,
                "run_id": run_id,
            }),
        )
        .await
    }

    pub async fn get_run(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        connector_id: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.get(
            &with_connector(&format!("/runs/{}", encode(run_id)), connector_id),
            credential,
        )
        .await
    }

    pub async fn events(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        after: u64,
        connector_id: Option<&str>,
    ) -> Result<EventsResponse, ApiError> {
        self.get(
            &with_connector(
                &format!("/runs/{}/events?after={after}", encode(run_id)),
                connector_id,
            ),
            credential,
        )
        .await
    }

    pub async fn start_run(
        &self,
        credential: &ApiCredential,
        session_id: &str,
        run_id: &str,
        input: &str,
        attachments: &[UploadedArtifact],
    ) -> Result<Value, ApiError> {
        self.post(
            &format!("/sessions/{}/runs", encode(session_id)),
            credential,
            &json!({
                "run_id": run_id,
                "input": input,
                "attachments": attachment_values(attachments),
            }),
        )
        .await
    }

    pub async fn start_agent_run(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        run_id: &str,
        input: AgentInputRequest<'_>,
    ) -> Result<Value, ApiError> {
        self.post(
            "/agent-runs",
            credential,
            &json!({
                "connector_id": connector_id,
                "session_id": session_id,
                "run_id": run_id,
                "input": input.text,
                "attachments": attachment_values(input.attachments),
                "after_activity_id": input.after_activity_id,
            }),
        )
        .await
    }

    pub async fn steer(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        command_id: &str,
        connector_id: Option<&str>,
        input: AgentInputRequest<'_>,
    ) -> Result<Value, ApiError> {
        self.post(
            &with_connector(&format!("/runs/{}/steer", encode(run_id)), connector_id),
            credential,
            &json!({
                "command_id": command_id,
                "text": input.text,
                "attachments": attachment_values(input.attachments),
                "after_activity_id": input.after_activity_id,
            }),
        )
        .await
    }

    pub async fn upload_artifact(
        &self,
        credential: &ApiCredential,
        file_name: &str,
        media_type: &str,
        bytes: &[u8],
        sha256: &str,
    ) -> Result<UploadedArtifact, ApiError> {
        let path = format!(
            "{API_BASE}/attachments?file_name={}&media_type={}",
            encode(file_name),
            encode(media_type)
        );
        let body = Uint8Array::from(bytes);
        let request = self
            .authenticated(Request::post(&path), credential)
            .header("Content-Type", media_type)
            .header("X-File-Size", &bytes.len().to_string())
            .header("X-File-Sha256", sha256)
            .body(body)
            .map_err(ApiError::transport)?;
        let response = request.send().await.map_err(ApiError::transport)?;
        decode(response).await
    }

    pub async fn cancel(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        reason: &str,
        connector_id: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.command(
            credential,
            &with_connector(&format!("/runs/{}/cancel", encode(run_id)), connector_id),
            json!({ "reason": reason }),
        )
        .await
    }

    pub async fn recover(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        connector_id: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.post(
            &with_connector(&format!("/runs/{}/recover", encode(run_id)), connector_id),
            credential,
            &json!({}),
        )
        .await
    }

    pub async fn resolve_input(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        request_id: &str,
        text: &str,
        connector_id: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.command(
            credential,
            &with_connector(
                &format!(
                    "/runs/{}/requests/{}/input",
                    encode(run_id),
                    encode(request_id)
                ),
                connector_id,
            ),
            json!({ "text": text }),
        )
        .await
    }

    pub async fn resolve_approval(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        request_id: &str,
        decision: &str,
        connector_id: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.command(
            credential,
            &with_connector(
                &format!(
                    "/runs/{}/requests/{}/approval",
                    encode(run_id),
                    encode(request_id)
                ),
                connector_id,
            ),
            json!({ "decision": decision }),
        )
        .await
    }

    pub async fn resolve_session_input(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        request_id: &str,
        text: &str,
    ) -> Result<Value, ApiError> {
        self.command(
            credential,
            &format!(
                "/agent-session/requests/{}/input?connector_id={}&session_id={}",
                encode(request_id),
                encode(connector_id),
                encode(session_id)
            ),
            json!({ "text": text }),
        )
        .await
    }

    pub async fn resolve_session_approval(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        request_id: &str,
        decision: &str,
    ) -> Result<Value, ApiError> {
        self.command(
            credential,
            &format!(
                "/agent-session/requests/{}/approval?connector_id={}&session_id={}",
                encode(request_id),
                encode(connector_id),
                encode(session_id)
            ),
            json!({ "decision": decision }),
        )
        .await
    }

    pub async fn stream<F>(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        after: u64,
        connector_id: Option<&str>,
        signal: &web_sys::AbortSignal,
        mut on_event: F,
    ) -> Result<(), ApiError>
    where
        F: FnMut(StreamEvent) + 'static,
    {
        let mut request = self.authenticated(
            Request::get(&format!(
                "{API_BASE}{}",
                with_connector(
                    &format!("/runs/{}/stream?after={after}", encode(run_id)),
                    connector_id
                )
            ))
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .abort_signal(Some(signal)),
            credential,
        );
        if after > 0 {
            request = request.header("Last-Event-ID", &after.to_string());
        }
        let response = request.send().await.map_err(ApiError::transport)?;
        if !response.ok() {
            return Err(error_response(response).await);
        }
        let body = response.body().ok_or_else(|| ApiError {
            message: "Streaming is not supported by this browser or proxy".to_owned(),
            status: 0,
            code: "stream_unavailable".to_owned(),
            details: None,
        })?;
        let mut stream = wasm_streams::ReadableStream::from_raw(body).into_stream();
        let mut parser = SseParser::default();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| ApiError::transport(js_message(&error)))?;
            let bytes = Uint8Array::new(&chunk).to_vec();
            for event in parser.push(&bytes) {
                on_event(event);
            }
        }
        if let Some(event) = parser.finish() {
            on_event(event);
        }
        Ok(())
    }

    pub async fn stream_agent_session<F, Fut>(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        after: u64,
        signal: &web_sys::AbortSignal,
        mut on_change: F,
    ) -> Result<(), ApiError>
    where
        F: FnMut(AgentSessionChangeView) -> Fut,
        Fut: Future<Output = Result<(), ApiError>>,
    {
        let path = format!(
            "/agent-session/stream?connector_id={}&session_id={}&after={after}",
            encode(connector_id),
            encode(session_id)
        );
        let mut request = Request::get(&format!("{API_BASE}{path}"))
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .abort_signal(Some(signal));
        if after > 0 {
            request = request.header("Last-Event-ID", &after.to_string());
        }
        let response = self
            .authenticated(request, credential)
            .send()
            .await
            .map_err(ApiError::transport)?;
        if !response.ok() {
            return Err(error_response(response).await);
        }
        let body = response.body().ok_or_else(|| ApiError {
            message: "Streaming is not supported by this browser or proxy".to_owned(),
            status: 0,
            code: "stream_unavailable".to_owned(),
            details: None,
        })?;
        let mut stream = wasm_streams::ReadableStream::from_raw(body).into_stream();
        let mut parser = SseParser::default();
        let mut sequence_guard = SessionSequenceGuard::with_cursor(after);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| ApiError::transport(js_message(&error)))?;
            let bytes = Uint8Array::new(&chunk).to_vec();
            for event in parser.push(&bytes) {
                match event {
                    StreamEvent::SessionChanged { data, .. } => {
                        let change: AgentSessionChangeView =
                            serde_json::from_str(&data).map_err(|error| ApiError {
                                message: format!("Invalid Agent session change: {error}"),
                                status: 0,
                                code: "invalid_session_change".to_owned(),
                                details: None,
                            })?;
                        match sequence_guard.observe(change.sequence) {
                            SessionSequenceDisposition::Apply => on_change(change).await?,
                            SessionSequenceDisposition::IgnoreDuplicate => {}
                            SessionSequenceDisposition::RefreshSnapshot => {
                                on_change(AgentSessionChangeView {
                                    connector_id: change.connector_id,
                                    session_id: change.session_id,
                                    sequence: change.sequence,
                                    change: AgentSessionChangeKindView::RefreshRequired {
                                        reason: "browser_sequence_gap".to_owned(),
                                    },
                                })
                                .await?;
                            }
                        }
                    }
                    StreamEvent::Error { data } => {
                        return Err(ApiError {
                            message: data,
                            status: 0,
                            code: "session_stream_failed".to_owned(),
                            details: None,
                        });
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn command(
        &self,
        credential: &ApiCredential,
        path: &str,
        mut payload: Value,
    ) -> Result<Value, ApiError> {
        payload["command_id"] = Value::String(new_uuid()?);
        self.post(path, credential, &payload).await
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        credential: &ApiCredential,
    ) -> Result<T, ApiError> {
        self.get_with_abort(path, credential, None).await
    }

    async fn get_with_abort<T: DeserializeOwned>(
        &self,
        path: &str,
        credential: &ApiCredential,
        signal: Option<&web_sys::AbortSignal>,
    ) -> Result<T, ApiError> {
        let request = Request::get(&format!("{API_BASE}{path}")).abort_signal(signal);
        let response = self
            .authenticated(request, credential)
            .send()
            .await
            .map_err(ApiError::transport)?;
        decode(response).await
    }

    async fn post<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        path: &str,
        credential: &ApiCredential,
        body: &B,
    ) -> Result<T, ApiError> {
        let request = self
            .authenticated(Request::post(&format!("{API_BASE}{path}")), credential)
            .json(body)
            .map_err(ApiError::transport)?;
        let response = request.send().await.map_err(ApiError::transport)?;
        decode(response).await
    }

    async fn post_public<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ApiError> {
        let response = Request::post(&format!("{API_BASE}{path}"))
            .header("Accept", "application/json")
            .cache(web_sys::RequestCache::NoStore)
            .credentials(web_sys::RequestCredentials::SameOrigin)
            .referrer_policy(web_sys::ReferrerPolicy::NoReferrer)
            .json(body)
            .map_err(ApiError::transport)?
            .send()
            .await
            .map_err(ApiError::transport)?;
        decode(response).await
    }

    fn authenticated(&self, request: RequestBuilder, credential: &ApiCredential) -> RequestBuilder {
        let request = request
            .header("Accept", "application/json")
            .cache(web_sys::RequestCache::NoStore)
            .credentials(web_sys::RequestCredentials::SameOrigin)
            .referrer_policy(web_sys::ReferrerPolicy::NoReferrer);
        match credential {
            ApiCredential::DeviceToken(token) => {
                request.header("Authorization", &format!("Bearer {token}"))
            }
            ApiCredential::GatewaySession => request,
        }
    }
}

async fn decode<T: DeserializeOwned>(response: Response) -> Result<T, ApiError> {
    if !response.ok() {
        return Err(error_response(response).await);
    }
    response.json().await.map_err(ApiError::transport)
}

async fn expect_empty(response: Response) -> Result<(), ApiError> {
    if response.ok() {
        Ok(())
    } else {
        Err(error_response(response).await)
    }
}

async fn error_response(response: Response) -> ApiError {
    let status = response.status();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    ApiError {
        message: body
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("Request failed ({status})")),
        status,
        code: body
            .get("code")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("http_{status}")),
        details: body.get("details").cloned(),
    }
}

fn encode(value: &str) -> String {
    js_sys::encode_uri_component(value)
        .as_string()
        .unwrap_or_default()
}

fn attachment_values(attachments: &[UploadedArtifact]) -> Vec<Value> {
    attachments
        .iter()
        .map(UploadedArtifact::command_value)
        .collect()
}

fn agent_session_path(
    connector_id: &str,
    session_id: &str,
    cursor: Option<&str>,
    limit: u32,
) -> String {
    let mut path = format!(
        "/agent-session?connector_id={}&session_id={}&limit={limit}",
        encode(connector_id),
        encode(session_id)
    );
    if let Some(cursor) = cursor {
        path.push_str("&cursor=");
        path.push_str(&encode(cursor));
    }
    path
}

fn with_connector(path: &str, connector_id: Option<&str>) -> String {
    let Some(connector_id) = connector_id else {
        return path.to_owned();
    };
    let separator = if path.contains('?') { '&' } else { '?' };
    format!("{path}{separator}connector_id={}", encode(connector_id))
}

fn js_message(value: &JsValue) -> String {
    value
        .as_string()
        .unwrap_or_else(|| "The live connection was interrupted".to_owned())
}
