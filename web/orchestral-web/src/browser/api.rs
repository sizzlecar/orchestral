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
    AgentConnectorView, AgentSessionDetail, AgentSessionPage, DeviceView, EventsResponse,
    PairingClaim, SessionView, StreamEvent,
};
use crate::sse::SseParser;

const API_BASE: &str = "/api/v1";

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
    ) -> Result<AgentSessionPage, ApiError> {
        self.get(
            &format!(
                "/agent-sessions?connector_id={}&limit=100",
                encode(connector_id)
            ),
            credential,
        )
        .await
    }

    pub async fn agent_session(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
    ) -> Result<AgentSessionDetail, ApiError> {
        self.get(
            &format!(
                "/agent-session?connector_id={}&session_id={}",
                encode(connector_id),
                encode(session_id)
            ),
            credential,
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
    ) -> Result<Value, ApiError> {
        self.post(
            &format!("/sessions/{}/runs", encode(session_id)),
            credential,
            &json!({ "run_id": run_id, "input": input }),
        )
        .await
    }

    pub async fn start_agent_run(
        &self,
        credential: &ApiCredential,
        connector_id: &str,
        session_id: &str,
        run_id: &str,
        input: &str,
    ) -> Result<Value, ApiError> {
        self.post(
            "/agent-runs",
            credential,
            &json!({
                "connector_id": connector_id,
                "session_id": session_id,
                "run_id": run_id,
                "input": input
            }),
        )
        .await
    }

    pub async fn steer(
        &self,
        credential: &ApiCredential,
        run_id: &str,
        text: &str,
        connector_id: Option<&str>,
    ) -> Result<Value, ApiError> {
        self.command(
            credential,
            &with_connector(&format!("/runs/{}/steer", encode(run_id)), connector_id),
            json!({ "text": text }),
        )
        .await
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
        let response = self
            .authenticated(Request::get(&format!("{API_BASE}{path}")), credential)
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
