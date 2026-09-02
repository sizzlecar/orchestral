use std::collections::BTreeMap;

use dioxus::html::FileData;
use dioxus::prelude::{spawn, ReadableExt, Signal, WritableExt};
use futures_util::{stream, StreamExt};
use gloo_timers::future::TimeoutFuture;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use wasm_bindgen::{closure::Closure, JsValue};

use crate::browser::api::{AgentSessionObservation, ApiClient, ApiCredential, ApiError};
use crate::browser::{platform, storage};
use crate::model::{
    AgentConnectorView, AgentSessionActionStatusView, AgentSessionChangeKindView,
    AgentSessionChangeView, OutboxEntry, OutboxOperation, SessionView, StreamEvent,
    UploadedArtifact,
};
use crate::state::{
    is_terminal, AgentSessionListState, AppState, AuthStatus, ConnectorsState, LoadStatus, Notice,
    SessionsState,
};

const AGENT_HISTORY_PAGE_LIMIT: u32 = 100;
const AGENT_SESSION_LIST_PAGE_LIMIT: u32 = 25;
const AGENT_OBSERVER_ACTIVE_INTERVAL_MS: u32 = 1_500;
const AGENT_OBSERVER_IDLE_INTERVAL_MS: u32 = 2_000;
const AGENT_OBSERVER_BACKGROUND_INTERVAL_MS: u32 = 15_000;
const AGENT_OBSERVER_MAX_BACKOFF_MS: u32 = 30_000;
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentSessionObservationTarget {
    session_key: String,
    connector_id: String,
    session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveTransportPlan {
    agent_session: Option<AgentSessionObservationTarget>,
    run_id: Option<String>,
}

fn selected_agent_observation_target(state: &AppState) -> Option<AgentSessionObservationTarget> {
    if !state.connection.online {
        return None;
    }
    let session = state.selected_session()?;
    Some(AgentSessionObservationTarget {
        session_key: session.key(),
        connector_id: session.connector_id.clone()?,
        session_id: session.id.clone(),
    })
}

fn live_transport_plan(state: &AppState) -> LiveTransportPlan {
    LiveTransportPlan {
        agent_session: selected_agent_observation_target(state),
        run_id: state.observable_run().map(|run| run.id.clone()),
    }
}

fn agent_observation_is_current(state: &AppState, target: &AgentSessionObservationTarget) -> bool {
    selected_agent_observation_target(state).as_ref() == Some(target)
}

fn agent_observation_interval_ms(session_state: &str, document_visible: bool) -> u32 {
    if !document_visible {
        return AGENT_OBSERVER_BACKGROUND_INTERVAL_MS;
    }
    match session_state {
        "active" | "running" | "waiting" | "waiting_input" | "waiting_approval"
        | "busy_elsewhere" => AGENT_OBSERVER_ACTIVE_INTERVAL_MS,
        _ => AGENT_OBSERVER_IDLE_INTERVAL_MS,
    }
}

fn agent_observation_backoff_ms(consecutive_errors: u32) -> u32 {
    let exponent = consecutive_errors.saturating_sub(1).min(5);
    (1_000_u32.saturating_mul(2_u32.saturating_pow(exponent))).min(AGENT_OBSERVER_MAX_BACKOFF_MS)
}

fn mark_observation_healthy(state: &mut AppState, mode: &str) {
    if state.connection.stream != mode
        || state.connection.attempt != 0
        || state.connection.error.is_some()
    {
        state.connection.stream = mode.to_owned();
        state.connection.attempt = 0;
        state.connection.error = None;
        state.connection.last_connected_at = Some(platform::now());
    }
}

fn optimistic_artifact_message(input: &str, attachments: &[UploadedArtifact]) -> String {
    let mut message = if input.is_empty() {
        "请查看并处理随消息附上的文件。".to_owned()
    } else {
        input.to_owned()
    };
    for attachment in attachments {
        message.push_str(&format!(
            "\n\n[附件：{}]({}) · {} · {} bytes",
            attachment.file_name,
            attachment.download_url,
            attachment.media_type,
            attachment.byte_size
        ));
    }
    message
}

#[derive(Clone, Copy)]
pub struct LiveTransportControls {
    pub run_abort: Signal<Option<web_sys::AbortController>>,
    pub run_generation: Signal<u64>,
    pub agent_session_abort: Signal<Option<web_sys::AbortController>>,
    pub agent_session_generation: Signal<u64>,
}

#[derive(Clone, Copy)]
pub struct AppController {
    pub state: Signal<AppState>,
    pub token: Signal<Option<ApiCredential>>,
    pub pairing_secret: Signal<Option<String>>,
    pub preferences: Signal<storage::Preferences>,
    pub stream_abort: Signal<Option<web_sys::AbortController>>,
    pub stream_generation: Signal<u64>,
    pub agent_session_stream_abort: Signal<Option<web_sys::AbortController>>,
    pub agent_session_stream_generation: Signal<u64>,
    pub install_event: Signal<Option<JsValue>>,
    api: ApiClient,
}

impl AppController {
    pub fn new(
        state: Signal<AppState>,
        token: Signal<Option<ApiCredential>>,
        pairing_secret: Signal<Option<String>>,
        preferences: Signal<storage::Preferences>,
        streams: LiveTransportControls,
        install_event: Signal<Option<JsValue>>,
    ) -> Self {
        Self {
            state,
            token,
            pairing_secret,
            preferences,
            stream_abort: streams.run_abort,
            stream_generation: streams.run_generation,
            agent_session_stream_abort: streams.agent_session_abort,
            agent_session_stream_generation: streams.agent_session_generation,
            install_event,
            api: ApiClient,
        }
    }

    pub async fn bootstrap(mut self) {
        let _ = platform::register_service_worker().await;
        platform::apply_theme(&self.preferences.read().theme);
        if self.pairing_secret.read().is_some() {
            self.claim_pairing().await;
            return;
        }

        let gateway = ApiCredential::GatewaySession;
        match self.api.me(&gateway).await {
            Ok(me) if me.get("auth_mode").and_then(Value::as_str) == Some("gateway_jwt") => {
                self.token.set(Some(gateway));
                let mut state = self.state.write();
                state.auth.status = AuthStatus::Authenticated;
                state.auth.me = Some(me);
                state.auth.device = None;
                state.auth.error = None;
                drop(state);
                self.load_workspace().await;
                return;
            }
            Ok(_) => {}
            Err(error) if error.code.starts_with("gateway_authentication_") => {
                self.set_auth_error(error.message);
                return;
            }
            Err(ApiError { status: 401, .. }) => {}
            Err(error) => {
                let mut state = self.state.write();
                state.connection.error = Some(error.message);
            }
        }

        let token = match storage::load_token().await {
            Ok(token) => token,
            Err(error) => {
                self.clear_auth(Some(error)).await;
                return;
            }
        };
        let Some(token) = token else {
            self.clear_auth(None).await;
            return;
        };
        let credential = ApiCredential::DeviceToken(token);
        self.token.set(Some(credential.clone()));
        match self.api.me(&credential).await {
            Ok(me) => {
                let mut state = self.state.write();
                state.auth.status = AuthStatus::Authenticated;
                state.auth.me = Some(me);
                state.auth.error = None;
            }
            Err(error) if error.status == 401 => {
                self.clear_auth(Some(error.message)).await;
                return;
            }
            Err(error) => {
                let mut state = self.state.write();
                state.auth.status = AuthStatus::Authenticated;
                state.connection.stream = if platform::is_online() {
                    "error"
                } else {
                    "offline"
                }
                .to_owned();
                state.connection.error = Some(error.message);
            }
        }
        self.load_workspace().await;
    }

    pub async fn claim_pairing(mut self) {
        let Some(secret) = self.pairing_secret.read().clone() else {
            self.clear_auth(Some("配对链接已失效，请从主机生成新的二维码。".to_owned()))
                .await;
            return;
        };
        {
            let mut state = self.state.write();
            state.auth.status = AuthStatus::Pairing;
            state.auth.error = None;
        }
        let device_name = platform::default_device_name(&self.preferences.read().device_name);
        match self.api.claim_pairing(&secret, &device_name).await {
            Ok(claim) => {
                if let Err(error) = storage::save_token(&claim.token).await {
                    self.set_auth_error(error);
                    return;
                }
                self.token
                    .set(Some(ApiCredential::DeviceToken(claim.token)));
                self.pairing_secret.set(None);
                {
                    let mut preferences = self.preferences.write();
                    preferences.device_name = claim.device.name.clone();
                    storage::save_preferences(&preferences);
                }
                {
                    let mut state = self.state.write();
                    state.auth.status = AuthStatus::Authenticated;
                    state.auth.me = Some(json!({ "device_id": claim.device.id }));
                    state.auth.device = Some(claim.device);
                    state.auth.error = None;
                }
                self.notice("设备配对成功", "success");
                self.load_workspace().await;
            }
            Err(error) => self.set_auth_error(error.message),
        }
    }

    pub async fn clear_auth(mut self, message: Option<String>) {
        self.stop_stream();
        self.token.set(None);
        let _ = storage::clear_token().await;
        let online = self.state.read().connection.online;
        let mut replacement = AppState::new(online);
        replacement.auth.status = AuthStatus::Unpaired;
        replacement.auth.error = message;
        self.state.set(replacement);
    }

    fn set_auth_error(mut self, message: String) {
        let mut state = self.state.write();
        state.auth.status = AuthStatus::Error;
        state.auth.error = Some(message);
    }

    pub async fn load_workspace(self) {
        self.refresh_devices().await;
        self.refresh_sessions(true).await;
        self.flush_outbox().await;
    }

    async fn flush_outbox(mut self) {
        if !platform::is_online() {
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let entries = match storage::load_outbox().await {
            Ok(entries) => entries,
            Err(error) => {
                self.notice(&format!("无法读取本地 Outbox：{error}"), "warning");
                return;
            }
        };
        let mut delivered = false;
        for entry in entries {
            let result = match &entry.operation {
                OutboxOperation::Start { run_id } => match entry.connector_id.as_deref() {
                    Some(connector_id) => {
                        self.api
                            .start_agent_run(
                                &token,
                                connector_id,
                                &entry.session_id,
                                run_id,
                                &entry.input,
                                &entry.attachments,
                            )
                            .await
                    }
                    None => {
                        self.api
                            .start_run(
                                &token,
                                &entry.session_id,
                                run_id,
                                &entry.input,
                                &entry.attachments,
                            )
                            .await
                    }
                },
                OutboxOperation::Steer { run_id, command_id } => {
                    self.api
                        .steer(
                            &token,
                            run_id,
                            command_id,
                            &entry.input,
                            entry.connector_id.as_deref(),
                            &entry.attachments,
                        )
                        .await
                }
            };
            match result {
                Ok(response) => {
                    if matches!(&entry.operation, OutboxOperation::Steer { .. }) {
                        if let Err(error) = check_ack(&response, "Outbox 重放") {
                            let _ = storage::delete_outbox(&entry.id).await;
                            self.notice(&error.message, "error");
                            continue;
                        }
                    }
                    match storage::delete_outbox(&entry.id).await {
                        Ok(()) => delivered = true,
                        Err(error) => self.notice(
                            &format!("Outbox 已送达，但本地确认写入失败：{error}"),
                            "warning",
                        ),
                    }
                }
                Err(error) if error.status == 401 => {
                    self.clear_auth(Some(error.message)).await;
                    return;
                }
                Err(error) if error.status == 0 => {
                    let mut state = self.state.write();
                    state.connection.error = Some("Outbox 等待网络恢复后重试".to_owned());
                    return;
                }
                Err(error) => {
                    // A definitive HTTP rejection cannot become successful by
                    // replaying the same immutable identity forever. Retain
                    // the user's original composer draft in the immediate
                    // path, and retire this failed durable operation.
                    let _ = storage::delete_outbox(&entry.id).await;
                    self.notice(&presented_api_error(&error), "error");
                }
            }
        }
        if delivered {
            self.refresh_sessions(true).await;
        }
    }

    pub async fn refresh_devices(mut self) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        self.state.write().devices.status = LoadStatus::Loading;
        match self.api.devices(&token).await {
            Ok(devices) => {
                let mut state = self.state.write();
                state.devices.status = LoadStatus::Ready;
                state.devices.items = devices;
                state.devices.error = None;
            }
            Err(error) if error.status == 401 => self.clear_auth(Some(error.message)).await,
            Err(error) => {
                let mut state = self.state.write();
                state.devices.status = LoadStatus::Error;
                state.devices.error = Some(error.message);
            }
        }
    }

    pub async fn refresh_sessions(mut self, load_selection: bool) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let (cached_sessions, previous_selected_id) = {
            let state = self.state.read();
            (
                state.sessions.items.clone(),
                state.sessions.selected_id.clone(),
            )
        };
        self.state.write().sessions.status = LoadStatus::Loading;
        match self.api.sessions(&token).await {
            Ok(mut sessions) => {
                let connectors = match self.api.agent_connectors(&token).await {
                    Ok(connectors) => {
                        self.state.write().connectors = ConnectorsState {
                            status: LoadStatus::Ready,
                            items: connectors.clone(),
                            error: None,
                        };
                        connectors
                    }
                    Err(error) if error.status == 401 => {
                        self.clear_auth(Some(error.message)).await;
                        return;
                    }
                    Err(error) => {
                        self.state.write().connectors = ConnectorsState {
                            status: LoadStatus::Error,
                            items: Vec::new(),
                            error: Some(error.message),
                        };
                        Vec::new()
                    }
                };
                let mut connector_pages = BTreeMap::new();
                for connector in connectors {
                    if connector.capabilities.list {
                        match self
                            .api
                            .agent_sessions(
                                &token,
                                &connector.connector_id,
                                None,
                                AGENT_SESSION_LIST_PAGE_LIMIT,
                            )
                            .await
                        {
                            Ok(page) => {
                                let next_cursor = page.next_cursor;
                                sessions.extend(
                                    page.sessions
                                        .into_iter()
                                        .map(|session| session.into_session()),
                                );
                                connector_pages.insert(
                                    connector.connector_id,
                                    AgentSessionListState {
                                        next_cursor,
                                        loading_more: false,
                                        error: None,
                                    },
                                );
                            }
                            Err(error) if error.status == 401 => {
                                self.clear_auth(Some(error.message)).await;
                                return;
                            }
                            Err(error) => {
                                sessions.extend(
                                    cached_sessions
                                        .iter()
                                        .filter(|session| {
                                            session.connector_id.as_deref()
                                                == Some(connector.connector_id.as_str())
                                        })
                                        .cloned(),
                                );
                                connector_pages.insert(
                                    connector.connector_id,
                                    AgentSessionListState {
                                        next_cursor: None,
                                        loading_more: false,
                                        error: Some(error.message),
                                    },
                                );
                            }
                        }
                    }
                }
                let (latest_sessions, current_selected_id) = {
                    let state = self.state.read();
                    (
                        state.sessions.items.clone(),
                        state.sessions.selected_id.clone(),
                    )
                };
                let selection_changed_during_refresh = current_selected_id != previous_selected_id;
                sessions = merge_sessions(&latest_sessions, sessions);
                if let Some(selected) = current_selected_id.as_deref().and_then(|selected_id| {
                    latest_sessions
                        .iter()
                        .find(|session| session.key() == selected_id)
                }) {
                    let selected_key = selected.key();
                    if !sessions.iter().any(|session| session.key() == selected_key)
                        && selected_session_may_be_on_later_page(selected, &connector_pages)
                    {
                        sessions.push(selected.clone());
                    }
                }
                sessions.sort_by_key(|session| std::cmp::Reverse(session.updated_at_unix_ms));
                let selected_id = retained_selection(current_selected_id.as_deref(), &sessions);
                {
                    let mut state = self.state.write();
                    let stream_cursors = std::mem::take(&mut state.sessions.stream_cursors);
                    state.sessions = SessionsState {
                        status: LoadStatus::Ready,
                        items: sessions,
                        selected_id: selected_id.clone(),
                        connector_pages,
                        stream_cursors,
                        error: None,
                    };
                }
                if load_selection && !selection_changed_during_refresh {
                    if let Some(selected_id) = selected_id.clone() {
                        self.load_session(selected_id).await;
                    }
                }
                if selected_id.is_none() && current_selected_id.is_some() {
                    self.stop_stream_and_set_idle();
                }
            }
            Err(error) if error.status == 401 => self.clear_auth(Some(error.message)).await,
            Err(error) => {
                let mut state = self.state.write();
                state.sessions.status = LoadStatus::Error;
                state.sessions.error = Some(error.message.clone());
                state.connection.error = Some(error.message);
                state.connection.stream = if platform::is_online() {
                    "error"
                } else {
                    "offline"
                }
                .to_owned();
            }
        }
    }

    pub async fn load_more_agent_sessions(mut self, connector_id: String) {
        if !platform::is_online() {
            self.notice("当前离线，恢复连接后再加载会话", "warning");
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let Some(cursor) = ({
            let state = self.state.read();
            state
                .sessions
                .connector_pages
                .get(&connector_id)
                .and_then(|page| {
                    if page.loading_more || (page.next_cursor.is_none() && page.error.is_none()) {
                        None
                    } else {
                        Some(page.next_cursor.clone())
                    }
                })
        }) else {
            return;
        };
        {
            let mut state = self.state.write();
            let Some(page) = state.sessions.connector_pages.get_mut(&connector_id) else {
                return;
            };
            page.loading_more = true;
            page.error = None;
        }

        match self
            .api
            .agent_sessions(
                &token,
                &connector_id,
                cursor.as_deref(),
                AGENT_SESSION_LIST_PAGE_LIMIT,
            )
            .await
        {
            Ok(page) => {
                let next_cursor = advancing_cursor(cursor.as_deref(), page.next_cursor);
                let incoming = page
                    .sessions
                    .into_iter()
                    .map(|session| session.into_session())
                    .collect();
                let mut state = self.state.write();
                let response_is_current = state
                    .sessions
                    .connector_pages
                    .get(&connector_id)
                    .is_some_and(|page| page_request_is_current(page, cursor.as_deref()));
                if !response_is_current {
                    return;
                }
                merge_session_page(&mut state.sessions.items, incoming);
                if let Some(page) = state.sessions.connector_pages.get_mut(&connector_id) {
                    page.next_cursor = next_cursor;
                    page.loading_more = false;
                    page.error = None;
                }
            }
            Err(error) if error.status == 401 => self.clear_auth(Some(error.message)).await,
            Err(error) => {
                {
                    let mut state = self.state.write();
                    if let Some(page) = state.sessions.connector_pages.get_mut(&connector_id) {
                        if page_request_is_current(page, cursor.as_deref()) {
                            page.loading_more = false;
                            page.error = Some(error.message.clone());
                        }
                    }
                }
                self.notice(&error.message, "error");
            }
        }
    }

    /// Refreshes only the visible session source. Connector refreshes replace
    /// the first page in place while retaining the currently selected session
    /// if it lives on an older page; the conversation view is never reloaded.
    pub async fn refresh_session_group(mut self, connector_id: Option<String>) {
        let Some(connector_id) = connector_id else {
            self.refresh_sessions(false).await;
            self.state.write().ui.session_page = 0;
            return;
        };
        if !platform::is_online() {
            self.notice("当前离线，恢复连接后再刷新会话", "warning");
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        {
            let mut state = self.state.write();
            let page = state
                .sessions
                .connector_pages
                .entry(connector_id.clone())
                .or_default();
            if page.loading_more {
                return;
            }
            page.loading_more = true;
            page.error = None;
        }
        match self
            .api
            .agent_sessions(&token, &connector_id, None, AGENT_SESSION_LIST_PAGE_LIMIT)
            .await
        {
            Ok(page) => {
                let incoming = page
                    .sessions
                    .into_iter()
                    .map(|session| session.into_session())
                    .collect::<Vec<_>>();
                let mut state = self.state.write();
                let selected_id = state.sessions.selected_id.clone();
                state.sessions.items.retain(|session| {
                    session.connector_id.as_deref() != Some(connector_id.as_str())
                        || selected_id.as_deref() == Some(session.key().as_str())
                });
                merge_session_page(&mut state.sessions.items, incoming);
                if let Some(list_state) = state.sessions.connector_pages.get_mut(&connector_id) {
                    list_state.next_cursor = page.next_cursor;
                    list_state.loading_more = false;
                    list_state.error = None;
                }
                if state.ui.session_tab.as_deref() == Some(connector_id.as_str()) {
                    state.ui.session_page = 0;
                }
            }
            Err(error) if error.status == 401 => self.clear_auth(Some(error.message)).await,
            Err(error) => {
                if let Some(page) = self
                    .state
                    .write()
                    .sessions
                    .connector_pages
                    .get_mut(&connector_id)
                {
                    page.loading_more = false;
                    page.error = Some(error.message.clone());
                }
                self.notice(&error.message, "error");
            }
        }
    }

    pub async fn load_session(mut self, session_key: String) {
        self.state.write().ui.loading_session = Some(session_key.clone());
        self.load_session_inner(session_key.clone()).await;
        let still_selected = {
            let mut state = self.state.write();
            if state.ui.loading_session.as_deref() == Some(session_key.as_str()) {
                state.ui.loading_session = None;
            }
            state.sessions.selected_id.as_deref() == Some(session_key.as_str())
        };
        if still_selected {
            let controller = self;
            spawn(async move {
                // Let Dioxus commit the newly projected timeline before
                // measuring its scroll height.
                TimeoutFuture::new(0).await;
                if controller.state.read().sessions.selected_id.as_deref()
                    == Some(session_key.as_str())
                {
                    platform::scroll_timeline_to_end();
                }
            });
        }
    }

    async fn load_session_inner(mut self, session_key: String) {
        self.stop_stream();
        let Some(session) = self
            .state
            .read()
            .sessions
            .items
            .iter()
            .find(|session| session.key() == session_key)
            .cloned()
        else {
            self.notice("会话已不存在，请刷新列表", "warning");
            return;
        };
        {
            let mut state = self.state.write();
            state.sessions.selected_id = Some(session_key.clone());
            state.ui.drawer_open = false;
            state.ui.session_actions_open = false;
        }
        let mut initial_observer_errors = 0;
        if let Some(connector_id) = session.connector_id.as_deref() {
            let Some(token) = self.token.read().clone() else {
                return;
            };
            let result = self
                .api
                .agent_session(
                    &token,
                    connector_id,
                    &session.id,
                    None,
                    AGENT_HISTORY_PAGE_LIMIT,
                )
                .await;
            if self.state.read().sessions.selected_id.as_deref() != Some(session_key.as_str()) {
                if let Err(error) = result {
                    if error.status == 401 {
                        self.clear_auth(Some(error.message)).await;
                    }
                }
                return;
            }
            match result {
                Ok(detail) => {
                    self.state.write().project_agent_session(detail);
                }
                Err(error) if error.status == 401 => {
                    self.clear_auth(Some(error.message)).await;
                    return;
                }
                Err(error) if error.status == 404 => {
                    let removed_selected = {
                        let selected_key = session.key();
                        let mut state = self.state.write();
                        state
                            .sessions
                            .items
                            .retain(|item| item.key() != selected_key);
                        let was_selected =
                            state.sessions.selected_id.as_deref() == Some(selected_key.as_str());
                        if was_selected {
                            state.sessions.selected_id = None;
                        }
                        was_selected
                    };
                    if removed_selected {
                        self.stop_stream_and_set_idle();
                        self.notice("会话已不存在，已从列表移除", "warning");
                    }
                    return;
                }
                Err(error) => {
                    initial_observer_errors = 1;
                    self.state.write().connection.error = Some(error.message.clone());
                    self.notice(&error.message, "error");
                }
            }
        }
        let run_ids = session
            .run_ids
            .iter()
            .filter(|run_id| !run_id.starts_with("agent-history:"))
            .filter(|run_id| {
                self.state
                    .read()
                    .runs
                    .get(*run_id)
                    .is_none_or(|run| run.view.is_none() || !is_terminal(&run.status))
            })
            .cloned()
            .collect::<Vec<_>>();
        stream::iter(run_ids)
            .for_each_concurrent(4, |run_id| {
                let controller = self;
                let session_id = session.id.clone();
                let connector_id = session.connector_id.clone();
                let selected_key = session_key.clone();
                async move {
                    if controller.state.read().sessions.selected_id.as_deref()
                        == Some(selected_key.as_str())
                    {
                        controller
                            .load_run_snapshot(&run_id, &session_id, connector_id.as_deref())
                            .await;
                    }
                }
            })
            .await;
        if self.state.read().sessions.selected_id.as_deref() != Some(session_key.as_str()) {
            return;
        }
        self.resume_live_transport_for_selection(initial_observer_errors);
    }

    pub async fn load_earlier_agent_history(mut self) {
        if !platform::is_online() {
            self.notice("当前离线，恢复连接后再加载记录", "warning");
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let Some((session_key, connector_id, session_id, run_id, cursor)) = ({
            let state = self.state.read();
            state.selected_session().and_then(|session| {
                let connector_id = session.connector_id.clone()?;
                let run_id = session.history_run_id()?;
                let run = state.runs.get(&run_id)?;
                if run.history_loading_earlier {
                    return None;
                }
                Some((
                    session.key(),
                    connector_id,
                    session.id.clone(),
                    run_id,
                    run.history_next_cursor.clone()?,
                ))
            })
        }) else {
            return;
        };

        if let Some(run) = self.state.write().runs.get_mut(&run_id) {
            run.history_loading_earlier = true;
            run.history_pagination_started = true;
        }
        match self
            .api
            .agent_session(
                &token,
                &connector_id,
                &session_id,
                Some(&cursor),
                AGENT_HISTORY_PAGE_LIMIT,
            )
            .await
        {
            Ok(detail) => {
                let still_selected =
                    self.state.read().sessions.selected_id.as_deref() == Some(session_key.as_str());
                let anchor = still_selected
                    .then(platform::timeline_scroll_anchor)
                    .flatten();
                self.state.write().prepend_agent_session_history(detail);
                if let Some(anchor) = anchor {
                    spawn(async move {
                        TimeoutFuture::new(0).await;
                        platform::restore_timeline_scroll_anchor(anchor);
                    });
                }
            }
            Err(error) if error.status == 401 => self.clear_auth(Some(error.message)).await,
            Err(error) => {
                if let Some(run) = self.state.write().runs.get_mut(&run_id) {
                    run.history_loading_earlier = false;
                }
                self.notice(&error.message, "error");
            }
        }
    }

    async fn load_run_snapshot(
        mut self,
        run_id: &str,
        session_id: &str,
        connector_id: Option<&str>,
    ) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let cursor = {
            let mut state = self.state.write();
            state
                .ensure_run_source(
                    run_id,
                    Some(session_id.to_owned()),
                    connector_id.map(str::to_owned),
                )
                .cursor
        };
        let (events, view) = futures_util::join!(
            self.api.events(&token, run_id, cursor, connector_id),
            self.api.get_run(&token, run_id, connector_id)
        );
        if let Ok(page) = events {
            let now = platform::now();
            let mut state = self.state.write();
            let run = state.ensure_run_source(
                run_id,
                Some(session_id.to_owned()),
                connector_id.map(str::to_owned),
            );
            for record in page.records {
                run.project_durable(&record, now);
            }
            state.reconcile_request_actions(run_id);
        }
        match view {
            Ok(view) => {
                let mut state = self.state.write();
                state
                    .ensure_run_source(
                        run_id,
                        Some(session_id.to_owned()),
                        connector_id.map(str::to_owned),
                    )
                    .apply_view(view, platform::now());
                state.reconcile_request_actions(run_id);
            }
            Err(error) if error.status == 401 => self.clear_auth(Some(error.message)).await,
            Err(error) if error.status == 404 => {
                let mut state = self.state.write();
                state.runs.remove(run_id);
                state.run_order.retain(|id| id != run_id);
                for session in &mut state.sessions.items {
                    session.run_ids.retain(|id| id != run_id);
                }
            }
            Err(error) => {
                let mut state = self.state.write();
                let run = state.ensure_run_source(
                    run_id,
                    Some(session_id.to_owned()),
                    connector_id.map(str::to_owned),
                );
                run.error = Some(error.message);
                if run.status == "loading" {
                    run.status = "unknown".to_owned();
                }
            }
        }
    }

    pub async fn refresh_run(self, run_id: &str) {
        let (session_id, connector_id) = self
            .state
            .read()
            .runs
            .get(run_id)
            .map(|run| {
                (
                    run.session_id.clone().unwrap_or_default(),
                    run.connector_id.clone(),
                )
            })
            .unwrap_or_default();
        self.load_run_snapshot(run_id, &session_id, connector_id.as_deref())
            .await;
    }

    pub async fn create_session(mut self) -> Option<SessionView> {
        if !platform::is_online() {
            self.notice("离线时无法创建会话", "warning");
            return None;
        }
        let token = self.token.read().clone()?;
        self.set_busy(true);
        let result = self.api.create_session(&token).await;
        self.set_busy(false);
        match result {
            Ok(session) => {
                {
                    let mut state = self.state.write();
                    state.ui.session_tab = Some("orchestral".to_owned());
                    state.ui.session_page = 0;
                }
                self.upsert_session(session.clone(), true);
                self.load_session(session.key()).await;
                Some(session)
            }
            Err(error) => {
                self.handle_api_error(error).await;
                None
            }
        }
    }

    pub async fn create_agent_session(
        mut self,
        connector: AgentConnectorView,
        cwd: Option<String>,
        options: Value,
    ) -> Option<SessionView> {
        if !platform::is_online() {
            self.notice("离线时无法创建会话", "warning");
            return None;
        }
        if !connector.capabilities.create {
            self.notice("这个 Agent 不支持创建会话", "warning");
            return None;
        }
        let token = self.token.read().clone()?;
        self.set_busy(true);
        let result = self
            .api
            .create_agent_session(
                &token,
                &connector.connector_id,
                cwd.as_deref(),
                None,
                options,
            )
            .await;
        self.set_busy(false);
        match result {
            Ok(summary) => {
                let session = summary.into_session();
                let session_key = session.key();
                self.stop_stream();
                self.upsert_session(session.clone(), false);
                {
                    let mut state = self.state.write();
                    state.activate_created_agent_session(
                        connector.connector_id.clone(),
                        session_key,
                    );
                }
                // `thread/start` returns authoritative metadata and a new
                // thread has no persisted turns. Start live observation in
                // the background instead of showing the history-loading UI.
                self.resume_live_transport_for_selection(0);
                Some(session)
            }
            Err(error) => {
                self.handle_api_error(error).await;
                None
            }
        }
    }

    pub async fn invoke_session_action(
        mut self,
        connector_id: String,
        session_id: String,
        action_id: String,
        arguments: Value,
        run_action: bool,
    ) {
        if !platform::is_online() {
            self.notice("离线时无法执行会话操作", "warning");
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let operation_session_key = format!("{connector_id}\0{session_id}");
        let run_id = if run_action {
            match platform::new_uuid() {
                Ok(run_id) => Some(run_id),
                Err(error) => {
                    self.notice(&error.message, "error");
                    return;
                }
            }
        } else {
            None
        };
        if run_action
            && self.state.read().sessions.selected_id.as_deref()
                == Some(operation_session_key.as_str())
        {
            self.stop_stream();
        }
        self.set_busy(true);
        let result = self
            .api
            .invoke_agent_session_action(
                &token,
                &connector_id,
                &session_id,
                &action_id,
                arguments,
                run_id.as_deref(),
            )
            .await;
        self.set_busy(false);
        match result {
            Ok(outcome) => {
                self.state.write().ui.session_actions_open = false;
                if let Some(summary) = outcome.session {
                    let session = summary.into_session();
                    let still_selected = self.state.read().sessions.selected_id.as_deref()
                        == Some(operation_session_key.as_str());
                    self.upsert_session(session.clone(), still_selected);
                    if still_selected {
                        self.load_session(session.key()).await;
                    }
                }
                match outcome.status {
                    AgentSessionActionStatusView::Completed => {
                        if run_action
                            && self.state.read().sessions.selected_id.as_deref()
                                == Some(operation_session_key.as_str())
                        {
                            self.resume_live_transport_for_selection(0);
                        }
                        self.notice("会话操作已完成", "success");
                    }
                    AgentSessionActionStatusView::Running { run_id } => {
                        let session = self
                            .state
                            .read()
                            .sessions
                            .items
                            .iter()
                            .find(|session| {
                                session.id == session_id
                                    && session.connector_id.as_deref()
                                        == Some(connector_id.as_str())
                            })
                            .cloned();
                        if let Some(mut session) = session {
                            if !session.run_ids.contains(&run_id) {
                                session.run_ids.push(run_id.clone());
                            }
                            session.updated_at_unix_ms = platform::now() as i64;
                            let still_selected = self.state.read().sessions.selected_id.as_deref()
                                == Some(operation_session_key.as_str());
                            self.upsert_session(session, still_selected);
                        }
                        {
                            let mut state = self.state.write();
                            let run = state.ensure_run_source(
                                &run_id,
                                Some(session_id.clone()),
                                Some(connector_id.clone()),
                            );
                            run.status = "accepted".to_owned();
                            run.started_at = Some(platform::now());
                        }
                        let still_selected = self.state.read().sessions.selected_id.as_deref()
                            == Some(operation_session_key.as_str());
                        if still_selected {
                            self.stop_stream();
                        }
                        self.refresh_run(&run_id).await;
                        if self.state.read().sessions.selected_id.as_deref()
                            == Some(operation_session_key.as_str())
                        {
                            self.resume_live_transport_for_selection(0);
                        }
                        self.notice("会话操作已启动", "success");
                    }
                }
            }
            Err(error) => {
                self.handle_api_error(error).await;
                if run_action
                    && self.state.read().sessions.selected_id.as_deref()
                        == Some(operation_session_key.as_str())
                {
                    self.resume_live_transport_for_selection(0);
                }
            }
        }
    }

    pub async fn upload_artifact(self, file: FileData) -> Result<UploadedArtifact, String> {
        let Some(token) = self.token.read().clone() else {
            return Err("当前设备尚未登录".to_owned());
        };
        if file.size() == 0 || file.size() > MAX_ARTIFACT_BYTES {
            return Err("文件必须在 1 byte 到 64 MiB 之间".to_owned());
        }
        let file_name = file.name();
        let media_type = file
            .content_type()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_owned());
        let bytes = file
            .read_bytes()
            .await
            .map_err(|error| format!("读取文件失败：{error}"))?;
        let sha256 = hex::encode(Sha256::digest(&bytes));
        self.api
            .upload_artifact(&token, &file_name, &media_type, &bytes, &sha256)
            .await
            .map_err(|error| presented_api_error(&error))
    }

    /// Submits one composer operation and reports whether the caller may clear
    /// its draft. A transport failure is ambiguous because the Host can have
    /// committed the idempotent command before the response was lost; in that
    /// case the optimistic message stays visible and the draft may clear.
    pub async fn submit(mut self, text: String, attachments: Vec<UploadedArtifact>) -> bool {
        let input = text.trim().to_owned();
        if (input.is_empty() && attachments.is_empty()) || self.state.read().ui.composer_busy {
            return false;
        }
        let display_input = optimistic_artifact_message(&input, &attachments);
        if !platform::is_online() {
            self.notice("当前离线，恢复连接后再发送", "warning");
            return false;
        }
        if self.state.read().recoverable_run().is_some() {
            self.notice("Agent 状态正在自动恢复，请稍后再发送", "warning");
            return false;
        }
        let Some(token) = self.token.read().clone() else {
            return false;
        };
        self.set_busy(true);
        let session = match self.state.read().selected_session().cloned() {
            Some(session) => session,
            None => match self.api.create_session(&token).await {
                Ok(session) => {
                    self.upsert_session(session.clone(), true);
                    session
                }
                Err(error) => {
                    self.set_busy(false);
                    self.handle_api_error(error).await;
                    return false;
                }
            },
        };
        let operation_session_key = session.key();
        let native_anchor_id = self.state.read().selected_native_tail_id();

        let active_run_id = { self.state.read().active_run().map(|run| run.id.clone()) };
        if let Some(run_id) = active_run_id {
            let command_id = match platform::new_uuid() {
                Ok(command_id) => command_id,
                Err(error) => {
                    self.notice(&error.message, "error");
                    self.set_busy(false);
                    return false;
                }
            };
            let outbox = OutboxEntry {
                id: format!("steer:{command_id}"),
                connector_id: session.connector_id.clone(),
                session_id: session.id.clone(),
                input: input.clone(),
                attachments: attachments.clone(),
                native_anchor_id: native_anchor_id.clone(),
                created_at_unix_ms: platform::now() as i64,
                operation: OutboxOperation::Steer {
                    run_id: run_id.clone(),
                    command_id: command_id.clone(),
                },
            };
            if let Err(error) = storage::save_outbox(&outbox).await {
                self.notice(&format!("发送前无法保存本地 Outbox：{error}"), "error");
                self.set_busy(false);
                return false;
            }
            let optimistic_message_id = format!("steer-{command_id}");
            self.state
                .write()
                .ensure_run_source(
                    &run_id,
                    Some(session.id.clone()),
                    session.connector_id.clone(),
                )
                .optimistic_steer(
                    optimistic_message_id.clone(),
                    display_input.clone(),
                    platform::now(),
                    native_anchor_id,
                );
            spawn(async move {
                TimeoutFuture::new(0).await;
                platform::scroll_timeline_to_end();
            });
            let accepted = match self
                .api
                .steer(
                    &token,
                    &run_id,
                    &command_id,
                    &input,
                    session.connector_id.as_deref(),
                    &attachments,
                )
                .await
            {
                Ok(ack) => match check_ack(&ack, "引导") {
                    Ok(_) => {
                        let outbox_id = outbox.id.clone();
                        let controller = self;
                        spawn(async move {
                            if let Err(error) = storage::delete_outbox(&outbox_id).await {
                                controller.notice(
                                    &format!("已发送，但清理本地 Outbox 失败：{error}"),
                                    "warning",
                                );
                            }
                        });
                        true
                    }
                    Err(error) => {
                        if let Some(run) = self.state.write().runs.get_mut(&run_id) {
                            run.messages
                                .retain(|message| message.id != optimistic_message_id);
                        }
                        let _ = storage::delete_outbox(&outbox.id).await;
                        self.notice(&error.message, "error");
                        false
                    }
                },
                Err(error) => {
                    // A transport error is ambiguous: the Host may already
                    // have accepted this exact command id. Keep that local
                    // message for later reconciliation, while definitive
                    // rejections remove it immediately.
                    let ambiguous = error.status == 0;
                    if !ambiguous {
                        if let Some(run) = self.state.write().runs.get_mut(&run_id) {
                            run.messages
                                .retain(|message| message.id != optimistic_message_id);
                        }
                        let _ = storage::delete_outbox(&outbox.id).await;
                    }
                    self.handle_api_error(error).await;
                    ambiguous
                }
            };
            self.set_busy(false);
            return accepted;
        }

        let run_id = match platform::new_uuid() {
            Ok(run_id) => run_id,
            Err(error) => {
                self.notice(&error.message, "error");
                self.set_busy(false);
                return false;
            }
        };
        let outbox = OutboxEntry {
            id: format!("start:{run_id}"),
            connector_id: session.connector_id.clone(),
            session_id: session.id.clone(),
            input: input.clone(),
            attachments: attachments.clone(),
            native_anchor_id: native_anchor_id.clone(),
            created_at_unix_ms: platform::now() as i64,
            operation: OutboxOperation::Start {
                run_id: run_id.clone(),
            },
        };
        if let Err(error) = storage::save_outbox(&outbox).await {
            self.notice(&format!("发送前无法保存本地 Outbox：{error}"), "error");
            self.set_busy(false);
            return false;
        }
        let now = platform::now();
        let mut pending_session = session.clone();
        pending_session.updated_at_unix_ms = now as i64;
        if !pending_session.run_ids.iter().any(|id| id == &run_id) {
            pending_session.run_ids.push(run_id.clone());
        }
        self.upsert_session(pending_session, true);
        self.state
            .write()
            .ensure_run_source(
                &run_id,
                Some(session.id.clone()),
                session.connector_id.clone(),
            )
            .optimistic_start_input(display_input.clone(), now, native_anchor_id.clone());
        self.stop_stream();
        spawn(async move {
            TimeoutFuture::new(0).await;
            platform::scroll_timeline_to_end();
        });

        let start = match session.connector_id.as_deref() {
            Some(connector_id) => {
                self.api
                    .start_agent_run(
                        &token,
                        connector_id,
                        &session.id,
                        &run_id,
                        &input,
                        &attachments,
                    )
                    .await
            }
            None => {
                self.api
                    .start_run(&token, &session.id, &run_id, &input, &attachments)
                    .await
            }
        };
        let accepted = match start {
            Ok(response) => {
                let actual_run_id = response
                    .get("run_id")
                    .and_then(value_as_id)
                    .unwrap_or_else(|| run_id.clone());
                let operation = response
                    .get("operation")
                    .and_then(Value::as_str)
                    .unwrap_or("started");
                let command_id = response.get("command_id").and_then(value_as_id);
                if actual_run_id != run_id {
                    let mut state = self.state.write();
                    // Another tab may have won the session operation and the
                    // Host atomically converted this stale "start" into a
                    // steer on the existing Run. Never replace that Run with
                    // this tab's provisional local projection.
                    state.runs.remove(&run_id);
                    state.run_order.retain(|item| item != &run_id);
                }
                let mut updated = session.clone();
                updated.updated_at_unix_ms = platform::now() as i64;
                if !updated.run_ids.iter().any(|id| id == &actual_run_id) {
                    updated.run_ids.push(actual_run_id.clone());
                }
                let still_selected = self.state.read().sessions.selected_id.as_deref()
                    == Some(operation_session_key.as_str());
                self.upsert_session(updated, still_selected);
                {
                    let mut state = self.state.write();
                    let run = state.ensure_run_source(
                        &actual_run_id,
                        Some(session.id.clone()),
                        session.connector_id.clone(),
                    );
                    match operation {
                        "steered" => run.optimistic_steer(
                            format!("steer-{}", command_id.as_deref().unwrap_or(run_id.as_str())),
                            display_input,
                            platform::now(),
                            native_anchor_id,
                        ),
                        "replayed" => {}
                        _ => run.record_accepted_input(display_input, platform::now()),
                    }
                    if let Some(view) = response.get("view") {
                        run.apply_view(view.clone(), platform::now());
                    }
                }
                // The Host acknowledgement is the composer commit point.
                // Snapshot reconciliation, transport replacement and Outbox
                // cleanup are follow-up work and must not keep accepted text
                // or attachments visible in the input field.
                let outbox_id = outbox.id.clone();
                let controller = self;
                spawn(async move {
                    if let Err(error) = storage::delete_outbox(&outbox_id).await {
                        controller.notice(
                            &format!("已发送，但清理本地 Outbox 失败：{error}"),
                            "warning",
                        );
                    }
                    controller.refresh_run(&actual_run_id).await;
                    if controller.state.read().sessions.selected_id.as_deref()
                        == Some(operation_session_key.as_str())
                    {
                        controller.resume_live_transport_for_selection(0);
                    }
                });
                true
            }
            Err(error) => {
                let ambiguous = error.status == 0;
                let presented = presented_api_error(&error);
                if ambiguous {
                    self.notice(
                        "发送结果暂时未知，已保存在 Outbox 并将在重连后核对",
                        "warning",
                    );
                } else {
                    let _ = storage::delete_outbox(&outbox.id).await;
                    if let Some(run) = self.state.write().runs.get_mut(&run_id) {
                        run.reject_optimistic_start(presented.clone(), platform::now());
                    }
                    if error.code == "live_control_unavailable" {
                        self.notice(&presented, "warning");
                    } else {
                        self.handle_api_error(error).await;
                    }
                }
                if self.state.read().sessions.selected_id.as_deref()
                    == Some(operation_session_key.as_str())
                {
                    self.resume_live_transport_for_selection(0);
                }
                ambiguous
            }
        };
        self.set_busy(false);
        accepted
    }

    pub async fn cancel(self) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let Some((run_id, connector_id)) = self
            .state
            .read()
            .active_run()
            .map(|run| (run.id.clone(), run.connector_id.clone()))
        else {
            return;
        };
        self.set_busy(true);
        match self
            .api
            .cancel(
                &token,
                &run_id,
                "Cancelled from paired device",
                connector_id.as_deref(),
            )
            .await
        {
            Ok(ack) => match check_ack(&ack, "取消") {
                Ok(_) => self.notice("已请求停止任务", "info"),
                Err(error) => self.notice(&error.message, "error"),
            },
            Err(error) => self.handle_api_error(error).await,
        }
        self.set_busy(false);
    }

    pub async fn resolve_input(mut self, run_id: String, request_id: String, text: String) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        {
            let mut state = self.state.write();
            if state.ui.request_is_resolving(&run_id, &request_id) {
                return;
            }
            state.ui.set_request_resolving(&run_id, &request_id, true);
        }
        let connector_id = self
            .state
            .read()
            .runs
            .get(&run_id)
            .and_then(|run| run.connector_id.clone());
        match self
            .api
            .resolve_input(
                &token,
                &run_id,
                &request_id,
                text.trim(),
                connector_id.as_deref(),
            )
            .await
        {
            Ok(ack) => {
                if let Err(error) = check_ack(&ack, "回复") {
                    self.state
                        .write()
                        .ui
                        .set_request_resolving(&run_id, &request_id, false);
                    self.notice(&error.message, "error");
                } else {
                    self.refresh_run(&run_id).await;
                }
            }
            Err(error) if is_stale_request_error(&error) => {
                self.state
                    .write()
                    .remove_pending_request(&run_id, &request_id);
                self.notice("该请求已由其他客户端处理", "info");
                self.refresh_run(&run_id).await;
            }
            Err(error) => {
                self.state
                    .write()
                    .ui
                    .set_request_resolving(&run_id, &request_id, false);
                self.handle_api_error(error).await;
            }
        }
    }

    pub async fn resolve_approval(mut self, run_id: String, request_id: String, decision: String) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        {
            let mut state = self.state.write();
            if state.ui.request_is_resolving(&run_id, &request_id) {
                return;
            }
            state.ui.set_request_resolving(&run_id, &request_id, true);
        }
        let connector_id = self
            .state
            .read()
            .runs
            .get(&run_id)
            .and_then(|run| run.connector_id.clone());
        match self
            .api
            .resolve_approval(
                &token,
                &run_id,
                &request_id,
                &decision,
                connector_id.as_deref(),
            )
            .await
        {
            Ok(ack) => {
                if let Err(error) = check_ack(&ack, "批准") {
                    self.state
                        .write()
                        .ui
                        .set_request_resolving(&run_id, &request_id, false);
                    self.notice(&error.message, "error");
                } else {
                    self.refresh_run(&run_id).await;
                }
            }
            Err(error) if is_stale_request_error(&error) => {
                self.state
                    .write()
                    .remove_pending_request(&run_id, &request_id);
                self.notice("该审批已由其他客户端处理", "info");
                self.refresh_run(&run_id).await;
            }
            Err(error) => {
                self.state
                    .write()
                    .ui
                    .set_request_resolving(&run_id, &request_id, false);
                self.handle_api_error(error).await;
            }
        }
    }

    pub async fn revoke_device(mut self, device_id: String, current: bool) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        match self.api.revoke_device(&token, &device_id).await {
            Ok(()) if current => {
                self.clear_auth(Some("当前设备已撤销，请重新配对。".to_owned()))
                    .await;
            }
            Ok(()) => {
                self.state
                    .write()
                    .devices
                    .items
                    .retain(|device| device.id != device_id);
                self.notice("设备访问权已撤销", "success");
            }
            Err(error) => self.handle_api_error(error).await,
        }
    }

    fn resume_live_transport_for_selection(self, initial_observer_errors: u32) {
        let plan = live_transport_plan(&self.state.read());
        if plan.agent_session.is_some() {
            self.start_selected_agent_observer(initial_observer_errors);
        } else {
            let controller = self;
            controller.stop_agent_session_observer();
        }
        if let Some(run_id) = plan.run_id {
            self.start_stream(run_id);
        } else {
            let controller = self;
            controller.stop_run_stream();
            if plan.agent_session.is_none() {
                controller.stop_stream_and_set_idle();
            }
        }
    }

    fn start_selected_agent_observer(self, initial_errors: u32) {
        let selected = {
            let state = self.state.read();
            selected_agent_observation_target(&state).map(|target| {
                let session_state = state
                    .selected_session()
                    .and_then(|session| session.state.clone())
                    .unwrap_or_else(|| "idle".to_owned());
                (target, session_state)
            })
        };
        if let Some((target, session_state)) = selected {
            self.start_agent_session_observer(target, session_state, initial_errors);
        }
    }

    fn start_agent_session_observer(
        mut self,
        target: AgentSessionObservationTarget,
        session_state: String,
        initial_errors: u32,
    ) {
        self.stop_agent_session_observer();
        if !agent_observation_is_current(&self.state.read(), &target) {
            return;
        }
        let Ok(controller) = web_sys::AbortController::new() else {
            self.notice("浏览器无法启动 Agent 会话自动刷新", "error");
            return;
        };
        self.agent_session_stream_abort
            .set(Some(controller.clone()));
        let generation = self
            .agent_session_stream_generation
            .read()
            .saturating_add(1);
        self.agent_session_stream_generation.set(generation);
        {
            let mut state = self.state.write();
            state.connection.stream = if initial_errors == 0 {
                "observing"
            } else {
                "reconnecting"
            }
            .to_owned();
            state.connection.attempt = initial_errors;
            if initial_errors == 0 {
                state.connection.error = None;
            }
        }
        spawn(async move {
            self.follow_agent_session_observer(
                target,
                session_state,
                controller,
                generation,
                initial_errors,
            )
            .await;
        });
    }

    async fn follow_agent_session_observer(
        mut self,
        target: AgentSessionObservationTarget,
        session_state: String,
        controller: web_sys::AbortController,
        generation: u64,
        initial_errors: u32,
    ) {
        let mut consecutive_errors = initial_errors;
        loop {
            if controller.signal().aborted()
                || *self.agent_session_stream_generation.read() != generation
                || !agent_observation_is_current(&self.state.read(), &target)
            {
                return;
            }
            let Some(token) = self.token.read().clone() else {
                return;
            };
            {
                let mut state = self.state.write();
                state.connection.stream = if consecutive_errors == 0 {
                    "connecting"
                } else {
                    "reconnecting"
                }
                .to_owned();
                state.connection.attempt = consecutive_errors;
                state.connection.error = None;
            }
            let refresh_controller = self;
            let refresh_target = target.clone();
            let refresh_signal = controller.signal();
            let after = self
                .state
                .read()
                .sessions
                .stream_cursors
                .get(&target.session_key)
                .copied()
                .unwrap_or_default();
            let result = self
                .api
                .stream_agent_session(
                    &token,
                    &target.connector_id,
                    &target.session_id,
                    after,
                    &controller.signal(),
                    move |change| {
                        let controller = refresh_controller;
                        let target = refresh_target.clone();
                        let signal = refresh_signal.clone();
                        async move {
                            controller
                                .apply_agent_session_observation_change(
                                    target, generation, signal, change,
                                )
                                .await
                        }
                    },
                )
                .await;
            if controller.signal().aborted()
                || *self.agent_session_stream_generation.read() != generation
            {
                return;
            }
            match result {
                Err(error) if error.status == 501 => {
                    self.follow_agent_session_polling_observer(
                        target,
                        session_state,
                        controller,
                        generation,
                        consecutive_errors,
                    )
                    .await;
                    return;
                }
                Err(error) if error.status == 401 => {
                    if agent_observation_is_current(&self.state.read(), &target) {
                        self.clear_auth(Some(error.message)).await;
                    }
                    return;
                }
                Err(error) if error.status == 404 => {
                    let mut state = self.state.write();
                    if !agent_observation_is_current(&state, &target) {
                        return;
                    }
                    state
                        .sessions
                        .items
                        .retain(|session| session.key() != target.session_key);
                    state.sessions.selected_id = None;
                    drop(state);
                    self.stop_stream_and_set_idle();
                    self.notice("会话已不存在，已从列表移除", "warning");
                    return;
                }
                Ok(()) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    self.state.write().connection.error =
                        Some("Agent 会话实时连接已关闭，正在重连".to_owned());
                }
                Err(error) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    self.state.write().connection.error = Some(error.message);
                }
            }
            self.state.write().connection.attempt = consecutive_errors;
            TimeoutFuture::new(agent_observation_backoff_ms(consecutive_errors)).await;
        }
    }

    async fn apply_agent_session_observation_change(
        mut self,
        target: AgentSessionObservationTarget,
        generation: u64,
        signal: web_sys::AbortSignal,
        change: AgentSessionChangeView,
    ) -> Result<(), ApiError> {
        if signal.aborted()
            || *self.agent_session_stream_generation.read() != generation
            || !agent_observation_is_current(&self.state.read(), &target)
        {
            return Ok(());
        }
        if change.connector_id != target.connector_id || change.session_id != target.session_id {
            return Err(ApiError {
                message: "Agent session stream returned a change for another session".to_owned(),
                status: 0,
                code: "session_change_target_mismatch".to_owned(),
                details: None,
            });
        }
        if matches!(
            &change.change,
            AgentSessionChangeKindView::RefreshRequired { .. }
        ) {
            return self
                .refresh_agent_session_observation(target, generation, signal)
                .await;
        }

        let follow_timeline = platform::timeline_is_near_end();
        let current = self.state.read().clone();
        let mut next = current.clone();
        let tail_before = next.selected_timeline_tail_key();
        next.apply_agent_session_change(change);
        mark_observation_healthy(&mut next, "live");
        let tail_appended = tail_before != next.selected_timeline_tail_key();
        if next != current {
            self.state.set(next);
        }
        if tail_appended && follow_timeline {
            spawn(async move {
                TimeoutFuture::new(0).await;
                platform::scroll_timeline_to_end();
            });
        }
        Ok(())
    }

    async fn refresh_agent_session_observation(
        mut self,
        target: AgentSessionObservationTarget,
        generation: u64,
        signal: web_sys::AbortSignal,
    ) -> Result<(), ApiError> {
        if signal.aborted()
            || *self.agent_session_stream_generation.read() != generation
            || !agent_observation_is_current(&self.state.read(), &target)
        {
            return Ok(());
        }
        let Some(token) = self.token.read().clone() else {
            return Ok(());
        };
        let detail = self
            .api
            .agent_session(
                &token,
                &target.connector_id,
                &target.session_id,
                None,
                AGENT_HISTORY_PAGE_LIMIT,
            )
            .await?;
        if signal.aborted()
            || *self.agent_session_stream_generation.read() != generation
            || !agent_observation_is_current(&self.state.read(), &target)
        {
            return Ok(());
        }
        let follow_timeline = platform::timeline_is_near_end();
        let current = self.state.read().clone();
        let mut next = current.clone();
        let tail_before = next.selected_timeline_tail_key();
        next.project_agent_session(detail);
        mark_observation_healthy(&mut next, "live");
        let tail_appended = tail_before != next.selected_timeline_tail_key();
        if next != current {
            self.state.set(next);
        }
        if tail_appended && follow_timeline {
            spawn(async move {
                TimeoutFuture::new(0).await;
                platform::scroll_timeline_to_end();
            });
        }
        Ok(())
    }

    async fn follow_agent_session_polling_observer(
        mut self,
        target: AgentSessionObservationTarget,
        mut session_state: String,
        controller: web_sys::AbortController,
        generation: u64,
        initial_errors: u32,
    ) {
        let mut consecutive_errors = initial_errors;
        let mut etag = None;
        loop {
            if controller.signal().aborted()
                || *self.agent_session_stream_generation.read() != generation
                || !agent_observation_is_current(&self.state.read(), &target)
            {
                return;
            }
            let delay = if consecutive_errors == 0 {
                agent_observation_interval_ms(&session_state, platform::is_document_visible())
            } else {
                agent_observation_backoff_ms(consecutive_errors)
            };
            TimeoutFuture::new(delay).await;
            if controller.signal().aborted()
                || *self.agent_session_stream_generation.read() != generation
                || !agent_observation_is_current(&self.state.read(), &target)
            {
                return;
            }
            let Some(token) = self.token.read().clone() else {
                return;
            };
            let result = self
                .api
                .observe_agent_session(
                    &token,
                    &target.connector_id,
                    &target.session_id,
                    AGENT_HISTORY_PAGE_LIMIT,
                    &controller.signal(),
                    etag.as_deref(),
                )
                .await;
            if controller.signal().aborted()
                || *self.agent_session_stream_generation.read() != generation
            {
                return;
            }

            match result {
                Ok(AgentSessionObservation::NotModified { etag: next_etag }) => {
                    etag = next_etag;
                    let healthy = {
                        let state = self.state.read();
                        if !agent_observation_is_current(&state, &target) {
                            return;
                        }
                        state.connection.stream == "observing"
                            && state.connection.attempt == 0
                            && state.connection.error.is_none()
                    };
                    if !healthy {
                        let mut state = self.state.write();
                        mark_observation_healthy(&mut state, "observing");
                    }
                    if !agent_observation_is_current(&self.state.read(), &target) {
                        return;
                    }
                    consecutive_errors = 0;
                }
                Ok(AgentSessionObservation::Modified {
                    detail,
                    etag: next_etag,
                }) => {
                    etag = next_etag;
                    let next_session_state = detail.summary.state.clone();
                    let follow_timeline = platform::timeline_is_near_end();
                    let current = self.state.read().clone();
                    if !agent_observation_is_current(&current, &target) {
                        return;
                    }
                    let mut next = current.clone();
                    let tail_before = next.selected_timeline_tail_key();
                    next.project_agent_session(*detail);
                    mark_observation_healthy(&mut next, "observing");
                    let tail_appended = tail_before != next.selected_timeline_tail_key();
                    if next != current {
                        self.state.set(next);
                    }
                    if tail_appended && follow_timeline {
                        spawn(async move {
                            TimeoutFuture::new(0).await;
                            platform::scroll_timeline_to_end();
                        });
                    }
                    session_state = next_session_state;
                    consecutive_errors = 0;
                }
                Err(error) if error.status == 401 => {
                    if agent_observation_is_current(&self.state.read(), &target) {
                        self.clear_auth(Some(error.message)).await;
                    }
                    return;
                }
                Err(error) if error.status == 404 => {
                    {
                        let mut state = self.state.write();
                        if !agent_observation_is_current(&state, &target) {
                            return;
                        }
                        state
                            .sessions
                            .items
                            .retain(|session| session.key() != target.session_key);
                        state.sessions.selected_id = None;
                    }
                    self.stop_stream_and_set_idle();
                    self.notice("会话已不存在，已从列表移除", "warning");
                    return;
                }
                Err(error) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    let mut state = self.state.write();
                    if !agent_observation_is_current(&state, &target) {
                        return;
                    }
                    state.connection.stream = "reconnecting".to_owned();
                    state.connection.attempt = consecutive_errors;
                    state.connection.error = Some(error.message);
                }
            }
        }
    }

    pub fn start_stream(mut self, run_id: String) {
        self.stop_run_stream();
        let Ok(controller) = web_sys::AbortController::new() else {
            self.notice("浏览器无法建立可取消的实时连接", "error");
            return;
        };
        self.stream_abort.set(Some(controller.clone()));
        let generation = self.stream_generation.read().saturating_add(1);
        self.stream_generation.set(generation);
        spawn(async move {
            self.follow_stream(run_id, controller, generation).await;
        });
    }

    fn stop_run_stream(mut self) {
        if let Some(controller) = self.stream_abort.take() {
            controller.abort();
        }
        let generation = self.stream_generation.read().saturating_add(1);
        self.stream_generation.set(generation);
    }

    fn stop_agent_session_observer(mut self) {
        if let Some(controller) = self.agent_session_stream_abort.take() {
            controller.abort();
        }
        let generation = self
            .agent_session_stream_generation
            .read()
            .saturating_add(1);
        self.agent_session_stream_generation.set(generation);
    }

    fn stop_stream(self) {
        self.stop_run_stream();
        self.stop_agent_session_observer();
    }

    fn stop_stream_and_set_idle(mut self) {
        self.stop_stream();
        let mut state = self.state.write();
        state.connection.stream = if platform::is_online() {
            "idle"
        } else {
            "offline"
        }
        .to_owned();
        state.connection.attempt = 0;
        state.connection.error = None;
    }

    async fn follow_stream(
        mut self,
        run_id: String,
        controller: web_sys::AbortController,
        generation: u64,
    ) {
        let mut attempt = 0_u32;
        loop {
            if controller.signal().aborted() || *self.stream_generation.read() != generation {
                return;
            }
            if attempt > 0 {
                // A Host restart can close the SSE connection after recording
                // continuity loss and restore the Run before the replacement
                // stream is attached. Re-read the bounded Run snapshot so the
                // composer does not depend on a missed transition or a full
                // browser refresh to leave `unknown`.
                self.refresh_run(&run_id).await;
                if controller.signal().aborted() || *self.stream_generation.read() != generation {
                    return;
                }
                if self
                    .state
                    .read()
                    .runs
                    .get(&run_id)
                    .is_some_and(|run| is_terminal(&run.status))
                {
                    self.finish_terminal_stream(generation);
                    return;
                }
            }
            let Some(token) = self.token.read().clone() else {
                return;
            };
            let cursor = self
                .state
                .read()
                .runs
                .get(&run_id)
                .map(|run| run.cursor)
                .unwrap_or_default();
            let connector_id = self
                .state
                .read()
                .runs
                .get(&run_id)
                .and_then(|run| run.connector_id.clone());
            {
                let mut state = self.state.write();
                state.connection.stream = if attempt == 0 {
                    "connecting"
                } else {
                    "reconnecting"
                }
                .to_owned();
                state.connection.attempt = attempt;
                state.connection.error = None;
            }
            let event_controller = self;
            let event_run_id = run_id.clone();
            let result = self
                .api
                .stream(
                    &token,
                    &run_id,
                    cursor,
                    connector_id.as_deref(),
                    &controller.signal(),
                    move |event| {
                        event_controller.handle_stream_event(&event_run_id, generation, event)
                    },
                )
                .await;
            if controller.signal().aborted() || *self.stream_generation.read() != generation {
                return;
            }
            if self
                .state
                .read()
                .runs
                .get(&run_id)
                .is_some_and(|run| is_terminal(&run.status))
            {
                self.finish_terminal_stream(generation);
                return;
            }
            if let Err(error) = result {
                if error.status == 401 {
                    self.clear_auth(Some(error.message)).await;
                    return;
                }
                let mut state = self.state.write();
                state.connection.error = Some(error.message);
            }
            attempt = attempt.saturating_add(1);
            self.state.write().connection.attempt = attempt;
            let delay = (500_u32.saturating_mul(2_u32.saturating_pow(attempt.min(5)))).min(15_000);
            TimeoutFuture::new(delay).await;
        }
    }

    fn handle_stream_event(mut self, run_id: &str, generation: u64, event: StreamEvent) {
        if *self.stream_generation.read() != generation {
            return;
        }
        let follow_timeline = platform::timeline_is_near_end();
        let tail_before = self.state.read().selected_timeline_tail_key();
        let mut timeline_changed = false;
        match event {
            StreamEvent::Durable { data, .. } => {
                if let Ok(record) = serde_json::from_str::<Value>(&data) {
                    timeline_changed = true;
                    let terminal = {
                        let mut state = self.state.write();
                        state.connection.stream = "live".to_owned();
                        state.connection.attempt = 0;
                        state.connection.last_connected_at = Some(platform::now());
                        let run = state.ensure_run(run_id, None);
                        run.project_durable(&record, platform::now());
                        let terminal = is_terminal(&run.status);
                        state.reconcile_request_actions(run_id);
                        terminal
                    };
                    if terminal {
                        self.finish_terminal_stream(generation);
                    }
                }
            }
            StreamEvent::Telemetry { data } => {
                if let Ok(telemetry) = serde_json::from_str::<Value>(&data) {
                    timeline_changed = true;
                    let mut state = self.state.write();
                    state.connection.stream = "live".to_owned();
                    state.ensure_run(run_id, None).project_telemetry(&telemetry);
                }
            }
            StreamEvent::SessionChanged { .. } => {}
            StreamEvent::Error { data } => {
                let message = serde_json::from_str::<Value>(&data)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("message")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .unwrap_or(data);
                let mut state = self.state.write();
                state.connection.stream = "error".to_owned();
                state.connection.error = Some(message);
            }
            StreamEvent::KeepAlive => {
                self.state.write().connection.stream = "live".to_owned();
            }
        }
        let tail_appended =
            timeline_changed && tail_before != self.state.read().selected_timeline_tail_key();
        if tail_appended && follow_timeline {
            spawn(async move {
                TimeoutFuture::new(0).await;
                platform::scroll_timeline_to_end();
            });
        }
    }

    fn finish_terminal_stream(self, generation: u64) {
        if *self.stream_generation.read() != generation {
            return;
        }
        self.stop_run_stream();
        if self.agent_session_stream_abort.read().is_none()
            && selected_agent_observation_target(&self.state.read()).is_some()
        {
            self.start_selected_agent_observer(0);
        }
    }

    pub fn window_listeners(self) -> Vec<Closure<dyn FnMut(web_sys::Event)>> {
        let mut listeners = Vec::new();
        if let Ok(listener) = platform::install_visual_viewport_sync() {
            listeners.push(listener);
        }
        if let Ok(listener) = platform::add_window_listener("online", move |_| {
            let mut controller = self;
            controller.state.write().connection.online = true;
            let refresh = controller;
            spawn(async move { refresh.load_workspace().await });
        }) {
            listeners.push(listener);
        }
        if let Ok(listener) = platform::add_window_listener("offline", move |_| {
            let mut controller = self;
            controller.stop_stream();
            let mut state = controller.state.write();
            state.connection.online = false;
            state.connection.stream = "offline".to_owned();
            state.connection.attempt = 0;
            state.connection.error = None;
        }) {
            listeners.push(listener);
        }
        if let Ok(listener) = platform::add_window_listener("beforeinstallprompt", move |event| {
            event.prevent_default();
            let mut controller = self;
            controller.install_event.set(Some(event.into()));
            controller.state.write().ui.install_available = true;
        }) {
            listeners.push(listener);
        }
        if let Ok(listener) = platform::add_window_listener("appinstalled", move |_| {
            let mut controller = self;
            controller.install_event.set(None);
            controller.state.write().ui.install_available = false;
            controller.notice("Orchestral 已安装", "success");
        }) {
            listeners.push(listener);
        }
        listeners
    }

    pub fn install(mut self) {
        if let Some(event) = self.install_event.take() {
            platform::install_event_prompt(&event);
            self.state.write().ui.install_available = false;
        } else {
            self.notice("请在浏览器菜单中选择“安装应用”或“添加到主屏幕”", "info");
        }
    }

    pub fn set_theme(mut self, theme: String) {
        self.preferences.write().theme = theme.clone();
        storage::save_preferences(&self.preferences.read());
        platform::apply_theme(&theme);
    }

    pub fn notice(mut self, message: &str, tone: &str) {
        let id = platform::now() as u64;
        self.state.write().ui.show_notice(Notice {
            message: message.to_owned(),
            tone: tone.to_owned(),
            id,
        });
        let timeout_ms = if tone == "error" { 10_000 } else { 5_000 };
        let mut state = self.state;
        spawn(async move {
            TimeoutFuture::new(timeout_ms).await;
            state.write().ui.dismiss_notice(id);
        });
    }

    fn set_busy(mut self, busy: bool) {
        self.state.write().ui.composer_busy = busy;
    }

    fn upsert_session(mut self, session: SessionView, select: bool) {
        let mut state = self.state.write();
        let session_key = session.key();
        let session = merge_session(
            state
                .sessions
                .items
                .iter()
                .find(|item| item.key() == session_key),
            session,
        );
        state
            .sessions
            .items
            .retain(|item| item.key() != session_key);
        state.sessions.items.push(session.clone());
        state
            .sessions
            .items
            .sort_by_key(|item| std::cmp::Reverse(item.updated_at_unix_ms));
        state.sessions.status = LoadStatus::Ready;
        state.sessions.error = None;
        if select {
            state.sessions.selected_id = Some(session_key);
        }
    }

    async fn handle_api_error(self, error: ApiError) {
        if error.status == 401 {
            self.clear_auth(Some(error.message)).await;
        } else {
            self.notice(&error.message, "error");
        }
    }
}

fn merge_session(existing: Option<&SessionView>, mut incoming: SessionView) -> SessionView {
    if let Some(existing) = existing {
        for run_id in &existing.run_ids {
            if !incoming.run_ids.contains(run_id) {
                incoming.run_ids.push(run_id.clone());
            }
        }
    }
    incoming
}

fn presented_api_error(error: &ApiError) -> String {
    if error.code == "live_control_unavailable" {
        "这个会话由另一个 Codex 进程持有，当前无法实时 steer；请改用 shared daemon 托管的会话"
            .to_owned()
    } else {
        error.message.clone()
    }
}

fn merge_sessions(existing: &[SessionView], incoming: Vec<SessionView>) -> Vec<SessionView> {
    incoming
        .into_iter()
        .map(|session| {
            let key = session.key();
            merge_session(existing.iter().find(|item| item.key() == key), session)
        })
        .collect()
}

fn merge_session_page(existing: &mut Vec<SessionView>, incoming: Vec<SessionView>) {
    for session in incoming {
        let key = session.key();
        let merged = merge_session(existing.iter().find(|item| item.key() == key), session);
        existing.retain(|item| item.key() != key);
        existing.push(merged);
    }
    existing.sort_by_key(|session| std::cmp::Reverse(session.updated_at_unix_ms));
}

fn retained_selection(selected_id: Option<&str>, sessions: &[SessionView]) -> Option<String> {
    selected_id
        .filter(|selected| sessions.iter().any(|session| session.key() == *selected))
        .map(str::to_owned)
}

fn selected_session_may_be_on_later_page(
    selected: &SessionView,
    connector_pages: &BTreeMap<String, AgentSessionListState>,
) -> bool {
    let Some(connector_id) = selected.connector_id.as_deref() else {
        return false;
    };
    connector_pages
        .get(connector_id)
        .is_some_and(|page| page.next_cursor.is_some() || page.error.is_some() || page.loading_more)
}

fn page_request_is_current(page: &AgentSessionListState, requested_cursor: Option<&str>) -> bool {
    page.loading_more && page.next_cursor.as_deref() == requested_cursor
}

fn advancing_cursor(requested_cursor: Option<&str>, next_cursor: Option<String>) -> Option<String> {
    next_cursor.filter(|next| requested_cursor != Some(next.as_str()))
}

fn check_ack(ack: &Value, operation: &str) -> Result<String, ApiError> {
    let state = ack
        .get("state")
        .and_then(|state| state.get("state"))
        .and_then(Value::as_str);
    match state {
        Some("accepted" | "applied") => Ok(ack
            .get("command_id")
            .and_then(value_as_id)
            .unwrap_or_else(|| "command".to_owned())),
        Some("rejected") => Err(ApiError {
            message: ack
                .get("state")
                .and_then(|state| state.get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{operation}被拒绝")),
            status: 0,
            code: ack
                .get("state")
                .and_then(|state| state.get("code"))
                .and_then(Value::as_str)
                .unwrap_or("command_rejected")
                .to_owned(),
            details: None,
        }),
        Some("unsupported") => Err(ApiError {
            message: format!(
                "主机不支持此操作：{}",
                ack.get("state")
                    .and_then(|state| state.get("feature"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
            status: 0,
            code: "unsupported".to_owned(),
            details: None,
        }),
        _ => Err(ApiError {
            message: format!("{operation}没有返回有效确认"),
            status: 0,
            code: "invalid_ack".to_owned(),
            details: None,
        }),
    }
}

fn is_stale_request_error(error: &ApiError) -> bool {
    error.status == 409
        && matches!(
            error.code.as_str(),
            "request_not_pending" | "approval_unavailable"
        )
}

fn value_as_id(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.get("0").and_then(Value::as_str).map(str::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn native_session(id: &str, updated_at_unix_ms: i64) -> SessionView {
        SessionView {
            id: id.to_owned(),
            created_at_unix_ms: updated_at_unix_ms,
            updated_at_unix_ms,
            run_ids: Vec::new(),
            connector_id: None,
            title: None,
            preview: None,
            cwd: None,
            state: None,
        }
    }

    fn selected_external_state() -> AppState {
        let mut state = AppState::new(true);
        state.sessions.items.push(SessionView {
            id: "thread-1".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            run_ids: vec!["agent-history:codex/local:thread-1".to_owned()],
            connector_id: Some("codex/local".to_owned()),
            title: None,
            preview: None,
            cwd: None,
            state: Some("active".to_owned()),
        });
        state.sessions.selected_id = Some("codex/local\0thread-1".to_owned());
        state
            .ensure_run_source(
                "agent-history:codex/local:thread-1",
                Some("thread-1".to_owned()),
                Some("codex/local".to_owned()),
            )
            .status = "delivered".to_owned();
        state
    }

    #[test]
    fn agent_observation_uses_fast_active_polling_and_capped_error_backoff() {
        for status in [
            "active",
            "running",
            "waiting",
            "waiting_input",
            "waiting_approval",
            "busy_elsewhere",
        ] {
            assert_eq!(
                agent_observation_interval_ms(status, true),
                AGENT_OBSERVER_ACTIVE_INTERVAL_MS
            );
        }
        for status in ["idle", "detached", "unavailable", "unknown"] {
            assert_eq!(
                agent_observation_interval_ms(status, true),
                AGENT_OBSERVER_IDLE_INTERVAL_MS
            );
        }
        assert_eq!(
            agent_observation_interval_ms("active", false),
            AGENT_OBSERVER_BACKGROUND_INTERVAL_MS
        );
        assert_eq!(
            (1..=7)
                .map(agent_observation_backoff_ms)
                .collect::<Vec<_>>(),
            vec![1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]
        );
    }

    #[test]
    fn agent_observation_guard_tracks_selection_and_connectivity_during_controlled_runs() {
        let mut state = selected_external_state();
        let target = selected_agent_observation_target(&state).unwrap();
        assert!(agent_observation_is_current(&state, &target));

        state.connection.online = false;
        assert!(!agent_observation_is_current(&state, &target));
        state.connection.online = true;

        state.sessions.items[0]
            .run_ids
            .push("controlled-run".to_owned());
        state
            .ensure_run_source(
                "controlled-run",
                Some("thread-1".to_owned()),
                Some("codex/local".to_owned()),
            )
            .status = "submitting".to_owned();
        let submitting = live_transport_plan(&state);
        assert_eq!(submitting.agent_session, Some(target.clone()));
        assert_eq!(submitting.run_id, None);
        assert_eq!(
            selected_agent_observation_target(&state),
            Some(target.clone())
        );

        state.runs.get_mut("controlled-run").unwrap().status = "accepted".to_owned();
        let running = live_transport_plan(&state);
        assert_eq!(running.agent_session, Some(target.clone()));
        assert_eq!(running.run_id.as_deref(), Some("controlled-run"));

        state.runs.get_mut("controlled-run").unwrap().status = "delivered".to_owned();
        assert_eq!(
            selected_agent_observation_target(&state),
            Some(target.clone())
        );

        state.sessions.selected_id = None;
        assert!(!agent_observation_is_current(&state, &target));
    }

    #[test]
    fn refresh_does_not_select_the_first_session_without_an_explicit_selection() {
        let sessions = vec![native_session("newest", 20), native_session("older", 10)];

        assert_eq!(retained_selection(None, &sessions), None);
        assert_eq!(
            retained_selection(Some("older"), &sessions).as_deref(),
            Some("older")
        );
        assert_eq!(retained_selection(Some("deleted"), &sessions), None);
    }

    #[test]
    fn selected_external_session_is_pinned_only_while_more_pages_may_contain_it() {
        let selected = SessionView {
            id: "older-thread".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            run_ids: vec!["agent-history:codex/local:older-thread".to_owned()],
            connector_id: Some("codex/local".to_owned()),
            title: None,
            preview: None,
            cwd: None,
            state: None,
        };
        let mut pages = BTreeMap::from([(
            "codex/local".to_owned(),
            AgentSessionListState {
                next_cursor: Some("page-2".to_owned()),
                loading_more: false,
                error: None,
            },
        )]);

        assert!(selected_session_may_be_on_later_page(&selected, &pages));

        pages.get_mut("codex/local").unwrap().next_cursor = None;
        assert!(!selected_session_may_be_on_later_page(&selected, &pages));

        pages.get_mut("codex/local").unwrap().error = Some("offline".to_owned());
        assert!(selected_session_may_be_on_later_page(&selected, &pages));
        assert!(!selected_session_may_be_on_later_page(
            &native_session("native", 1),
            &pages
        ));
    }

    #[test]
    fn connector_pagination_rejects_stale_and_non_advancing_cursors() {
        let page = AgentSessionListState {
            next_cursor: Some("page-2".to_owned()),
            loading_more: true,
            error: None,
        };

        assert!(page_request_is_current(&page, Some("page-2")));
        assert!(!page_request_is_current(&page, Some("stale-page")));
        assert_eq!(
            advancing_cursor(Some("page-2"), Some("page-3".to_owned())).as_deref(),
            Some("page-3")
        );
        assert_eq!(
            advancing_cursor(Some("page-2"), Some("page-2".to_owned())),
            None
        );
    }

    #[test]
    fn refreshing_external_metadata_does_not_drop_controlled_runs() {
        let existing = SessionView {
            id: "thread-1".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            run_ids: vec![
                "agent-history:fixture/local:thread-1".to_owned(),
                "controlled-run".to_owned(),
            ],
            connector_id: Some("fixture/local".to_owned()),
            title: Some("Old".to_owned()),
            preview: None,
            cwd: None,
            state: Some("active".to_owned()),
        };
        let incoming = SessionView {
            title: Some("Renamed".to_owned()),
            run_ids: vec!["agent-history:fixture/local:thread-1".to_owned()],
            ..existing.clone()
        };

        let merged = merge_sessions(&[existing], vec![incoming]).remove(0);
        assert_eq!(merged.title.as_deref(), Some("Renamed"));
        assert!(merged.run_ids.iter().any(|run| run == "controlled-run"));
    }

    #[test]
    fn loading_another_connector_page_deduplicates_sessions_and_preserves_run_state() {
        let existing_external = SessionView {
            id: "thread-1".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            run_ids: vec![
                "agent-history:fixture/local:thread-1".to_owned(),
                "controlled-run".to_owned(),
            ],
            connector_id: Some("fixture/local".to_owned()),
            title: Some("Old title".to_owned()),
            preview: None,
            cwd: None,
            state: Some("active".to_owned()),
        };
        let incoming_external = SessionView {
            title: Some("New title".to_owned()),
            updated_at_unix_ms: 30,
            run_ids: vec!["agent-history:fixture/local:thread-1".to_owned()],
            ..existing_external.clone()
        };
        let mut sessions = vec![native_session("native", 10), existing_external];

        merge_session_page(
            &mut sessions,
            vec![incoming_external, native_session("native-2", 20)],
        );

        assert_eq!(sessions.len(), 3);
        assert_eq!(sessions[0].id, "thread-1");
        assert_eq!(sessions[0].title.as_deref(), Some("New title"));
        assert!(sessions[0]
            .run_ids
            .iter()
            .any(|run_id| run_id == "controlled-run"));
        assert_eq!(
            sessions
                .iter()
                .filter(|session| session.id == "thread-1")
                .count(),
            1
        );
    }

    #[test]
    fn accepted_ack_returns_command_id() {
        let ack = json!({ "command_id": "cmd-1", "state": { "state": "accepted" } });
        assert_eq!(check_ack(&ack, "test").unwrap(), "cmd-1");
    }

    #[test]
    fn only_authoritative_stale_request_conflicts_remove_cards() {
        for code in ["request_not_pending", "approval_unavailable"] {
            assert!(is_stale_request_error(&ApiError {
                message: "stale".to_owned(),
                status: 409,
                code: code.to_owned(),
                details: None,
            }));
        }
        assert!(!is_stale_request_error(&ApiError {
            message: "wrong approval choice".to_owned(),
            status: 409,
            code: "session_approval_unavailable".to_owned(),
            details: None,
        }));
    }

    #[test]
    fn rejected_ack_preserves_host_message() {
        let ack = json!({ "state": { "state": "rejected", "code": "no", "message": "denied" } });
        let error = check_ack(&ack, "test").unwrap_err();
        assert_eq!(error.code, "no");
        assert_eq!(error.message, "denied");
    }
}
