use dioxus::prelude::{spawn, ReadableExt, Signal, WritableExt};
use gloo_timers::future::TimeoutFuture;
use serde_json::{json, Value};
use wasm_bindgen::{closure::Closure, JsValue};

use crate::browser::api::{ApiClient, ApiCredential, ApiError};
use crate::browser::{platform, storage};
use crate::model::{AgentConnectorView, SessionView, StreamEvent};
use crate::state::{
    is_terminal, AppState, AuthStatus, ConnectorsState, LoadStatus, Notice, SessionsState,
};

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
                for connector in connectors {
                    if connector.capabilities.list {
                        if let Ok(page) = self
                            .api
                            .agent_sessions(&token, &connector.connector_id)
                            .await
                        {
                            sessions.extend(
                                page.sessions
                                    .into_iter()
                                    .map(|session| session.into_session()),
                            );
                        }
                    }
                }
                sessions = merge_sessions(&self.state.read().sessions.items, sessions);
                sessions.sort_by_key(|session| std::cmp::Reverse(session.updated_at_unix_ms));
                let selected_id = {
                    let state = self.state.read();
                    state
                        .sessions
                        .selected_id
                        .clone()
                        .filter(|selected| sessions.iter().any(|item| &item.key() == selected))
                        .or_else(|| sessions.first().map(SessionView::key))
                };
                {
                    let mut state = self.state.write();
                    state.sessions = SessionsState {
                        status: LoadStatus::Ready,
                        items: sessions,
                        selected_id: selected_id.clone(),
                        error: None,
                    };
                }
                if load_selection {
                    if let Some(selected_id) = selected_id {
                        self.load_session(selected_id).await;
                    }
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
            state.sessions.selected_id = Some(session_key);
            state.ui.drawer_open = false;
            state.ui.session_actions_open = false;
        }
        if let Some(connector_id) = session.connector_id.as_deref() {
            let Some(token) = self.token.read().clone() else {
                return;
            };
            match self
                .api
                .agent_session(&token, connector_id, &session.id)
                .await
            {
                Ok(detail) => self.state.write().project_agent_session(detail),
                Err(error) if error.status == 401 => {
                    self.clear_auth(Some(error.message)).await;
                    return;
                }
                Err(error) => self.notice(&error.message, "error"),
            }
        }
        for run_id in session.run_ids {
            if run_id.starts_with("agent-history:") {
                continue;
            }
            self.load_run_snapshot(&run_id, &session.id, session.connector_id.as_deref())
                .await;
        }
        let active = self.state.read().active_run().map(|run| run.id.clone());
        if let Some(run_id) = active {
            self.start_stream(run_id);
        } else {
            let mut state = self.state.write();
            state.connection.stream = if platform::is_online() {
                "idle"
            } else {
                "offline"
            }
            .to_owned();
            state.connection.attempt = 0;
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
    ) {
        if !platform::is_online() {
            self.notice("离线时无法执行会话操作", "warning");
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        self.set_busy(true);
        let result = self
            .api
            .invoke_agent_session_action(&token, &connector_id, &session_id, &action_id, arguments)
            .await;
        self.set_busy(false);
        match result {
            Ok(outcome) => {
                self.state.write().ui.session_actions_open = false;
                if let Some(summary) = outcome.session {
                    let session = summary.into_session();
                    self.upsert_session(session.clone(), true);
                    self.load_session(session.key()).await;
                }
                self.notice("会话操作已完成", "success");
            }
            Err(error) => self.handle_api_error(error).await,
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
                self.upsert_session(updated, true);
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
                if self
                    .state
                    .read()
                    .runs
                    .get(&actual_run_id)
                    .is_some_and(|run| !is_terminal(&run.status))
                {
                    self.start_stream(actual_run_id);
                }
            }
            Err(error) => {
                if let Some(run) = self.state.write().runs.get_mut(&run_id) {
                    run.reject_optimistic_start(error.message.clone(), platform::now());
                }
                self.handle_api_error(error).await;
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
        let Some((run_id, connector_id)) = self
            .state
            .read()
            .recoverable_run()
            .map(|run| (run.id.clone(), run.connector_id.clone()))
        else {
            return;
        };
        self.set_busy(true);
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
                let status = self
                    .state
                    .read()
                    .runs
                    .get(&run_id)
                    .map(|run| run.status.clone());
                if status
                    .as_deref()
                    .is_some_and(|status| !is_terminal(status) && status != "unknown")
                {
                    self.start_stream(run_id);
                }
            }
            Err(error) => self.handle_api_error(error).await,
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
                    move |event| event_controller.handle_stream_event(&event_run_id, event),
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
                let mut state = self.state.write();
                state.connection.stream = "idle".to_owned();
                state.connection.attempt = 0;
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

    fn handle_stream_event(mut self, run_id: &str, event: StreamEvent) {
        match event {
            StreamEvent::Durable { data, .. } => {
                if let Ok(record) = serde_json::from_str::<Value>(&data) {
                    let mut state = self.state.write();
                    state.connection.stream = "live".to_owned();
                    state.connection.attempt = 0;
                    state.connection.last_connected_at = Some(platform::now());
                    state
                        .ensure_run(run_id, None)
                        .project_durable(&record, platform::now());
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
            let mut state = controller.state.write();
            state.connection.online = false;
            state.connection.stream = "offline".to_owned();
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
        self.state.write().ui.notice = Some(Notice {
            message: message.to_owned(),
            tone: tone.to_owned(),
            id,
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
