use std::collections::BTreeMap;

use dioxus::prelude::{spawn, ReadableExt, Signal, WritableExt};
use gloo_timers::future::TimeoutFuture;
use serde_json::{json, Value};
use wasm_bindgen::{closure::Closure, JsValue};

use crate::browser::api::{AgentSessionObservation, ApiClient, ApiCredential, ApiError};
use crate::browser::{platform, storage};
use crate::model::{AgentConnectorView, AgentSessionActionStatusView, SessionView, StreamEvent};
use crate::state::{
    is_terminal, AgentSessionListState, AppState, AuthStatus, ConnectorsState, LoadStatus, Notice,
    SessionsState,
};

const AGENT_HISTORY_PAGE_LIMIT: u32 = 100;
const AGENT_SESSION_LIST_PAGE_LIMIT: u32 = 25;
const AGENT_OBSERVER_ACTIVE_INTERVAL_MS: u32 = 1_500;
const AGENT_OBSERVER_IDLE_INTERVAL_MS: u32 = 12_000;
const AGENT_OBSERVER_MAX_BACKOFF_MS: u32 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct AgentSessionObservationTarget {
    session_key: String,
    connector_id: String,
    session_id: String,
}

fn selected_agent_observation_target(state: &AppState) -> Option<AgentSessionObservationTarget> {
    if !state.connection.online || orchestral_run_blocks_agent_observation(state) {
        return None;
    }
    let session = state.selected_session()?;
    Some(AgentSessionObservationTarget {
        session_key: session.key(),
        connector_id: session.connector_id.clone()?,
        session_id: session.id.clone(),
    })
}

fn orchestral_run_blocks_agent_observation(state: &AppState) -> bool {
    state.selected_session().is_some_and(|session| {
        session
            .run_ids
            .iter()
            .filter(|run_id| !run_id.starts_with("agent-history:"))
            .filter_map(|run_id| state.runs.get(run_id))
            .any(|run| {
                matches!(
                    run.status.as_str(),
                    "loading" | "submitting" | "accepted" | "running" | "waiting" | "stopping"
                )
            })
    })
}

fn agent_observation_is_current(state: &AppState, target: &AgentSessionObservationTarget) -> bool {
    selected_agent_observation_target(state).as_ref() == Some(target)
}

fn agent_observation_interval_ms(session_state: &str) -> u32 {
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

#[derive(Clone, Copy)]
pub struct AppController {
    pub state: Signal<AppState>,
    pub token: Signal<Option<ApiCredential>>,
    pub pairing_secret: Signal<Option<String>>,
    pub preferences: Signal<storage::Preferences>,
    pub stream_abort: Signal<Option<web_sys::AbortController>>,
    pub stream_generation: Signal<u64>,
    pub install_event: Signal<Option<JsValue>>,
    api: ApiClient,
}

impl AppController {
    pub fn new(
        state: Signal<AppState>,
        token: Signal<Option<ApiCredential>>,
        pairing_secret: Signal<Option<String>>,
        preferences: Signal<storage::Preferences>,
        stream_abort: Signal<Option<web_sys::AbortController>>,
        stream_generation: Signal<u64>,
        install_event: Signal<Option<JsValue>>,
    ) -> Self {
        Self {
            state,
            token,
            pairing_secret,
            preferences,
            stream_abort,
            stream_generation,
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
                    state.sessions = SessionsState {
                        status: LoadStatus::Ready,
                        items: sessions,
                        selected_id: selected_id.clone(),
                        connector_pages,
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

    pub async fn load_session(mut self, session_key: String) {
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
        for run_id in &session.run_ids {
            if self.state.read().sessions.selected_id.as_deref() != Some(session_key.as_str()) {
                return;
            }
            if run_id.starts_with("agent-history:") {
                continue;
            }
            self.load_run_snapshot(run_id, &session.id, session.connector_id.as_deref())
                .await;
        }
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
        let events = self.api.events(&token, run_id, cursor, connector_id).await;
        let view = self.api.get_run(&token, run_id, connector_id).await;
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
        }
        match view {
            Ok(view) => {
                self.state
                    .write()
                    .ensure_run_source(
                        run_id,
                        Some(session_id.to_owned()),
                        connector_id.map(str::to_owned),
                    )
                    .apply_view(view, platform::now());
            }
            Err(error) if error.status == 401 => self.clear_auth(Some(error.message)).await,
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

    pub async fn create_session(self) -> Option<SessionView> {
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
            .create_agent_session(&token, &connector.connector_id, None, None)
            .await;
        self.set_busy(false);
        match result {
            Ok(summary) => {
                let session = summary.into_session();
                self.state.write().ui.new_session_open = false;
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

    pub async fn submit(mut self, text: String) {
        let input = text.trim().to_owned();
        if input.is_empty() || self.state.read().ui.composer_busy {
            return;
        }
        if !platform::is_online() {
            self.notice("当前离线，恢复连接后再发送", "warning");
            return;
        }
        if self.state.read().recoverable_run().is_some() {
            self.notice("运行连接已中断，请先恢复连接", "warning");
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
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
                    return;
                }
            },
        };
        let operation_session_key = session.key();

        let active_run_id = { self.state.read().active_run().map(|run| run.id.clone()) };
        if let Some(run_id) = active_run_id {
            match self
                .api
                .steer(&token, &run_id, &input, session.connector_id.as_deref())
                .await
            {
                Ok(ack) => match check_ack(&ack, "引导") {
                    Ok(command_id) => {
                        self.state
                            .write()
                            .ensure_run_source(
                                &run_id,
                                Some(session.id.clone()),
                                session.connector_id.clone(),
                            )
                            .optimistic_steer(format!("steer-{command_id}"), input);
                    }
                    Err(error) => self.notice(&error.message, "error"),
                },
                Err(error) => self.handle_api_error(error).await,
            }
            self.set_busy(false);
            return;
        }

        let run_id = match platform::new_uuid() {
            Ok(run_id) => run_id,
            Err(error) => {
                self.notice(&error.message, "error");
                self.set_busy(false);
                return;
            }
        };
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
            .optimistic_start_input(input.clone(), now);
        self.stop_stream();
        spawn(async move {
            TimeoutFuture::new(0).await;
            platform::scroll_timeline_to_end();
        });

        let start = match session.connector_id.as_deref() {
            Some(connector_id) => {
                self.api
                    .start_agent_run(&token, connector_id, &session.id, &run_id, &input)
                    .await
            }
            None => {
                self.api
                    .start_run(&token, &session.id, &run_id, &input)
                    .await
            }
        };
        match start {
            Ok(response) => {
                let actual_run_id = response
                    .get("run_id")
                    .and_then(value_as_id)
                    .unwrap_or_else(|| run_id.clone());
                if actual_run_id != run_id {
                    let mut state = self.state.write();
                    if let Some(mut provisional) = state.runs.remove(&run_id) {
                        provisional.id = actual_run_id.clone();
                        provisional.session_id = Some(session.id.clone());
                        provisional.connector_id = session.connector_id.clone();
                        if let Some(initial) = provisional
                            .messages
                            .iter_mut()
                            .find(|message| message.role == "user" && !message.steering)
                        {
                            initial.id = format!("optimistic-input-{actual_run_id}");
                        }
                        state.runs.insert(actual_run_id.clone(), provisional);
                    }
                    for item in &mut state.run_order {
                        if item == &run_id {
                            *item = actual_run_id.clone();
                        }
                    }
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
                    run.record_started_input(input, platform::now());
                    if let Some(view) = response.get("view") {
                        run.apply_view(view.clone(), platform::now());
                    }
                }
                self.refresh_run(&actual_run_id).await;
                if self.state.read().sessions.selected_id.as_deref()
                    == Some(operation_session_key.as_str())
                {
                    self.resume_live_transport_for_selection(0);
                }
            }
            Err(error) => {
                if let Some(run) = self.state.write().runs.get_mut(&run_id) {
                    run.reject_optimistic_start(error.message.clone(), platform::now());
                }
                self.handle_api_error(error).await;
                if self.state.read().sessions.selected_id.as_deref()
                    == Some(operation_session_key.as_str())
                {
                    self.resume_live_transport_for_selection(0);
                }
            }
        }
        self.set_busy(false);
    }

    pub async fn recover_current_run(mut self) {
        if !platform::is_online() {
            self.notice("当前离线，恢复网络后再重连任务", "warning");
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let Some((run_id, connector_id, operation_session_key)) = ({
            let state = self.state.read();
            state.recoverable_run().map(|run| {
                (
                    run.id.clone(),
                    run.connector_id.clone(),
                    state.sessions.selected_id.clone().unwrap_or_default(),
                )
            })
        }) else {
            return;
        };
        self.set_busy(true);
        self.stop_stream();
        match self
            .api
            .recover(&token, &run_id, connector_id.as_deref())
            .await
        {
            Ok(view) => {
                self.state
                    .write()
                    .ensure_run_source(&run_id, None, connector_id)
                    .apply_view(view, platform::now());
                self.refresh_run(&run_id).await;
                if self.state.read().sessions.selected_id.as_deref()
                    == Some(operation_session_key.as_str())
                {
                    self.resume_live_transport_for_selection(0);
                }
            }
            Err(error) => {
                self.handle_api_error(error).await;
                if self.state.read().sessions.selected_id.as_deref()
                    == Some(operation_session_key.as_str())
                {
                    self.resume_live_transport_for_selection(0);
                }
            }
        }
        self.set_busy(false);
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

    pub async fn resolve_input(self, run_id: String, request_id: String, text: String) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let connector_id = self
            .state
            .read()
            .runs
            .get(&run_id)
            .and_then(|run| run.connector_id.clone());
        self.set_busy(true);
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
                    self.notice(&error.message, "error");
                } else {
                    self.refresh_run(&run_id).await;
                }
            }
            Err(error) => self.handle_api_error(error).await,
        }
        self.set_busy(false);
    }

    pub async fn resolve_approval(self, run_id: String, request_id: String, decision: String) {
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let connector_id = self
            .state
            .read()
            .runs
            .get(&run_id)
            .and_then(|run| run.connector_id.clone());
        self.set_busy(true);
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
                    self.notice(&error.message, "error");
                } else {
                    self.refresh_run(&run_id).await;
                }
            }
            Err(error) => self.handle_api_error(error).await,
        }
        self.set_busy(false);
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
        let active = self.state.read().active_run().map(|run| run.id.clone());
        if let Some(run_id) = active {
            self.start_stream(run_id);
        } else if selected_agent_observation_target(&self.state.read()).is_some() {
            self.start_selected_agent_observer(initial_observer_errors);
        } else {
            self.stop_stream_and_set_idle();
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
        self.stop_stream();
        if !agent_observation_is_current(&self.state.read(), &target) {
            return;
        }
        let Ok(controller) = web_sys::AbortController::new() else {
            self.notice("浏览器无法启动 Agent 会话自动刷新", "error");
            return;
        };
        self.stream_abort.set(Some(controller.clone()));
        let generation = self.stream_generation.read().saturating_add(1);
        self.stream_generation.set(generation);
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
        mut session_state: String,
        controller: web_sys::AbortController,
        generation: u64,
        initial_errors: u32,
    ) {
        let mut consecutive_errors = initial_errors;
        let mut etag = None;
        loop {
            if controller.signal().aborted()
                || *self.stream_generation.read() != generation
                || !agent_observation_is_current(&self.state.read(), &target)
            {
                return;
            }
            let delay = if consecutive_errors == 0 {
                agent_observation_interval_ms(&session_state)
            } else {
                agent_observation_backoff_ms(consecutive_errors)
            };
            TimeoutFuture::new(delay).await;
            if controller.signal().aborted()
                || *self.stream_generation.read() != generation
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
            if controller.signal().aborted() || *self.stream_generation.read() != generation {
                return;
            }

            match result {
                Ok(AgentSessionObservation::NotModified { etag: next_etag }) => {
                    etag = next_etag;
                    let mut state = self.state.write();
                    if !agent_observation_is_current(&state, &target) {
                        return;
                    }
                    state.connection.stream = "observing".to_owned();
                    state.connection.attempt = 0;
                    state.connection.error = None;
                    state.connection.last_connected_at = Some(platform::now());
                    consecutive_errors = 0;
                }
                Ok(AgentSessionObservation::Modified {
                    detail,
                    etag: next_etag,
                }) => {
                    etag = next_etag;
                    let next_session_state = detail.summary.state.clone();
                    let mut state = self.state.write();
                    if !agent_observation_is_current(&state, &target) {
                        return;
                    }
                    state.project_agent_session(detail);
                    state.connection.stream = "observing".to_owned();
                    state.connection.attempt = 0;
                    state.connection.error = None;
                    state.connection.last_connected_at = Some(platform::now());
                    drop(state);
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
        self.stop_stream();
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

    fn stop_stream(mut self) {
        if let Some(controller) = self.stream_abort.take() {
            controller.abort();
        }
        let generation = self.stream_generation.read().saturating_add(1);
        self.stream_generation.set(generation);
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
        match event {
            StreamEvent::Durable { data, .. } => {
                if let Ok(record) = serde_json::from_str::<Value>(&data) {
                    let terminal = {
                        let mut state = self.state.write();
                        state.connection.stream = "live".to_owned();
                        state.connection.attempt = 0;
                        state.connection.last_connected_at = Some(platform::now());
                        let run = state.ensure_run(run_id, None);
                        run.project_durable(&record, platform::now());
                        is_terminal(&run.status)
                    };
                    if terminal {
                        self.finish_terminal_stream(generation);
                    }
                }
            }
            StreamEvent::Telemetry { data } => {
                if let Ok(telemetry) = serde_json::from_str::<Value>(&data) {
                    let mut state = self.state.write();
                    state.connection.stream = "live".to_owned();
                    state.ensure_run(run_id, None).project_telemetry(&telemetry);
                }
            }
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
    }

    fn finish_terminal_stream(self, generation: u64) {
        if *self.stream_generation.read() != generation {
            return;
        }
        self.resume_live_transport_for_selection(0);
    }

    pub fn window_listeners(self) -> Vec<Closure<dyn FnMut(web_sys::Event)>> {
        let mut listeners = Vec::new();
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
                agent_observation_interval_ms(status),
                AGENT_OBSERVER_ACTIVE_INTERVAL_MS
            );
        }
        for status in ["idle", "detached", "unavailable", "unknown"] {
            assert_eq!(
                agent_observation_interval_ms(status),
                AGENT_OBSERVER_IDLE_INTERVAL_MS
            );
        }
        assert_eq!(
            (1..=7)
                .map(agent_observation_backoff_ms)
                .collect::<Vec<_>>(),
            vec![1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000]
        );
    }

    #[test]
    fn agent_observation_guard_tracks_selection_connectivity_and_controlled_runs() {
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
        assert!(selected_agent_observation_target(&state).is_none());

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
    fn rejected_ack_preserves_host_message() {
        let ack = json!({ "state": { "state": "rejected", "code": "no", "message": "denied" } });
        let error = check_ack(&ack, "test").unwrap_err();
        assert_eq!(error.code, "no");
        assert_eq!(error.message, "denied");
    }
}
