use super::*;
use crate::model_controls;

impl AppController {
    pub async fn open_model_controls(mut self) {
        let Some(session) = self.state.read().selected_session().cloned() else {
            return;
        };
        let key = session.key();
        let generation = self.state.write().ui.model_controls.begin(key.clone());
        let Some(token) = self.token.read().clone() else {
            let mut state = self.state.write();
            state.ui.model_controls.loading = false;
            state.ui.model_controls.error = Some("请先连接主机。".into());
            return;
        };
        let result = if session.connector_id.is_some() {
            self.state
                .read()
                .selected_connector()
                .map(|connector| model_controls::from_connector(&session, connector))
                .ok_or_else(|| "Agent 连接暂不可用，请刷新后重试。".to_owned())
        } else {
            self.api
                .model_settings(&token, &session.id)
                .await
                .map_err(|error| error.message)
        };
        let mut state = self.state.write();
        if state.sessions.selected_id.as_deref() != Some(key.as_str())
            || !state.ui.model_controls.accepts(&key, generation)
        {
            return;
        }
        state.ui.model_controls.loading = false;
        match result {
            Ok(view) => {
                state.ui.model_controls.view = Some(view);
            }
            Err(error) => {
                state.ui.model_controls.error = Some(error);
            }
        }
    }

    pub async fn change_model_setting(mut self, reasoning: bool, value: String) {
        if !platform::is_online() {
            self.notice("离线时无法修改模型设置。", "warning");
            return;
        }
        let Some(session) = self.state.read().selected_session().cloned() else {
            return;
        };
        let key = session.key();
        let Some(view) = self.state.read().ui.model_controls.view.clone() else {
            return;
        };
        if self.state.read().ui.model_controls.session_key != key
            || self.state.read().ui.model_controls.busy
        {
            return;
        }
        let Some(token) = self.token.read().clone() else {
            return;
        };
        let field = if reasoning {
            "reasoning_effort"
        } else {
            "model"
        };
        self.state.write().ui.model_controls.busy = true;
        self.state.write().ui.model_controls.error = None;
        self.set_busy(true);
        let result = if let Some(connector) = session.connector_id.as_ref() {
            self.api
                .invoke_agent_session_action(
                    &token,
                    connector,
                    &session.id,
                    if reasoning {
                        model_controls::SET_REASONING
                    } else {
                        model_controls::SET_MODEL
                    },
                    json!({field:value}),
                    None,
                )
                .await
                .and_then(|outcome| {
                    outcome
                        .session
                        .map(|summary| summary.execution_profile)
                        .ok_or_else(|| ApiError {
                            status: 502,
                            code: "model_settings_unconfirmed".into(),
                            details: None,
                            message: "服务未确认模型设置，请刷新后检查。".into(),
                        })
                })
        } else {
            self.api
                .change_model_settings(
                    &token,
                    &session.id,
                    json!({field:value,"expected_model":view.profile.model.unwrap_or_default()}),
                )
                .await
        };
        self.set_busy(false);
        match result {
            Ok(profile) => {
                {
                    let mut state = self.state.write();
                    if let Some(session) = state
                        .sessions
                        .items
                        .iter_mut()
                        .find(|item| item.key() == key)
                    {
                        session.execution_profile = profile;
                    }
                    if state.ui.model_controls.session_key == key {
                        state.ui.model_controls.busy = false;
                    }
                }
                if self.state.read().sessions.selected_id.as_deref() == Some(key.as_str()) {
                    self.open_model_controls().await;
                    self.notice("设置已更新，后续请求将使用新设置。", "success");
                }
            }
            Err(error) => {
                let mut state = self.state.write();
                if state.ui.model_controls.session_key == key {
                    state.ui.model_controls.busy = false;
                    state.ui.model_controls.error = Some(error.message);
                }
            }
        }
    }
}
