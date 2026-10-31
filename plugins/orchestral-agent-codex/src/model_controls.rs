//! Native model selection stays in the Codex adapter; clients consume actions.

use super::*;

pub(super) fn descriptor(action: &str, catalog: &Value) -> AgentSessionActionDescriptor {
    let model = action == SESSION_SET_MODEL_ACTION;
    let field = if model { "model" } else { "reasoning_effort" };
    let mut schema = catalog["properties"][field].clone();
    schema
        .as_object_mut()
        .expect("typed selection schema")
        .remove("default");
    if !model {
        if let Some(options) = schema.get_mut("enum").and_then(Value::as_array_mut) {
            if !options.contains(&json!("default")) {
                options.insert(0, json!("default"));
            }
        }
    }
    AgentSessionActionDescriptor {
        action_id: AgentSessionActionId::new(action),
        title: if model {
            "切换模型"
        } else {
            "推理强度"
        }
        .into(),
        description: "应用到此会话的后续请求；活动任务结束后可修改。".into(),
        input_schema: Some(json!({"type":"object","additionalProperties":false,
            "required":[field],"properties":{field:schema}})),
        input_channel: false,
        execution: AgentSessionActionExecution::Immediate,
    }
}

#[derive(Clone, Default)]
pub(super) struct PendingSettings {
    model: Option<String>,
    effort: Option<String>,
}

impl PendingSettings {
    pub(super) fn enrich(&self, profile: &mut AgentSessionExecutionProfile) {
        if self.model.is_some() {
            profile.model.clone_from(&self.model);
        }
        if self.effort.is_some() {
            profile.reasoning_effort.clone_from(&self.effort);
        }
    }
    pub(super) fn parameters(&self, params: &mut Value) {
        if let Some(model) = &self.model {
            params["model"] = json!(model);
        }
        if let Some(effort) = &self.effort {
            params["effort"] = json!(effort);
        }
    }
}

impl CodexConnector {
    pub(crate) fn pending_model_selection(&self, id: &AgentSessionId) -> Option<PendingSettings> {
        self.pending_model_settings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id.as_str())
            .cloned()
    }
    pub(crate) fn commit_pending_model_selection(&self, id: &AgentSessionId) {
        let settings = self
            .pending_model_settings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id.as_str());
        if let Some(settings) = settings {
            self.remember_execution_profile(
                id,
                &json!({"model":settings.model,"reasoningEffort":settings.effort}),
            );
        }
    }
}

async fn declared_default(
    client: &ConnectedClient,
    model: &str,
) -> Result<String, AgentConnectorError> {
    let mut cursor: Option<String> = None;
    let mut seen = BTreeSet::new();
    for _ in 0..16 {
        let mut params = json!({"limit":100});
        if let Some(cursor) = &cursor {
            params["cursor"] = json!(cursor);
        }
        let result = client
            .rpc
            .request("model/list", params)
            .await
            .map_err(connector_transport_error)?;
        if let Some(value) = result
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|entry| {
                entry.get("model").and_then(Value::as_str) == Some(model)
                    || entry.get("id").and_then(Value::as_str) == Some(model)
            })
            .and_then(|entry| entry.get("defaultReasoningEffort"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            return Ok(value.into());
        }
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if let Some(next) = &cursor {
            if !seen.insert(next.clone()) {
                break;
            }
        } else {
            break;
        }
    }
    Err(AgentConnectorError::unsupported(
        "Codex 未声明当前模型的默认推理强度，请选择明确的强度。",
    ))
}

pub(super) async fn apply(
    connector: &CodexConnector,
    request: &InvokeAgentSessionActionRequest,
) -> Result<AgentSessionSummary, AgentConnectorError> {
    if request.run_id.is_some() {
        return Err(AgentConnectorError::invalid(
            "model settings are immediate session actions",
        ));
    }
    let model = request.action_id.as_str() == SESSION_SET_MODEL_ACTION;
    let field = if model { "model" } else { "reasoning_effort" };
    let object = request
        .arguments
        .as_object()
        .ok_or_else(|| AgentConnectorError::invalid("model settings require an object"))?;
    if object.len() != 1 || !object.contains_key(field) {
        return Err(AgentConnectorError::invalid(format!(
            "model settings require only {field}"
        )));
    }
    let mut value = required_action_string(&request.arguments, field)?;
    let client = connector.client().await?;
    let current = connector.read_summary(&client, &request.session_id).await?;
    if !matches!(
        current.state,
        orchestral_core::agent_connector::AgentSessionState::Idle
            | orchestral_core::agent_connector::AgentSessionState::Detached
    ) {
        return Err(AgentConnectorError::unsupported(
            "请等待当前任务结束后再切换模型或推理强度。",
        ));
    }
    if !model && value == "default" {
        let model = current.execution_profile.model.as_deref().ok_or_else(|| {
            AgentConnectorError::unsupported("当前模型元数据不可用，请先选择模型或明确的推理强度。")
        })?;
        value = declared_default(&client, model).await?;
    }
    let mut params = json!({"threadId":request.session_id.as_str(),"excludeTurns":true,"persistExtendedHistory":true});
    if model {
        params["model"] = json!(value);
    } else {
        params["config"] = json!({"model_reasoning_effort":value});
    }
    match client.rpc.request("thread/resume", params).await {
        Ok(response) => {
            connector
                .pending_model_settings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(request.session_id.as_str());
            connector.remember_execution_profile(&request.session_id, &response);
            connector
                .provider_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .mark_loaded(request.session_id.clone());
            let thread = response
                .get("thread")
                .ok_or_else(|| AgentConnectorError::protocol("thread/resume omitted thread"))?;
            Ok(connector.enrich_summary(normalize::session_summary(
                &connector.describe().connector_id,
                thread,
            )?))
        }
        Err(error) if provider::unmaterialized_resume_without_rollout(&error) => {
            // The native runtime has no rollout before its first user turn.
            // Confirm it is loaded, then send the same settings as supported
            // turn/start overrides. Do not fabricate or rewrite native history.
            if !provider::client_has_loaded_session(&client.rpc, &request.session_id).await {
                return Err(connector_transport_error(error));
            }
            let mut pending = connector
                .pending_model_settings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let settings = pending
                .entry(request.session_id.as_str().into())
                .or_default();
            if model {
                settings.model = Some(value);
            } else {
                settings.effort = Some(value);
            }
            drop(pending);
            Ok(connector.enrich_summary(current))
        }
        Err(error) => Err(connector_transport_error(error)),
    }
}
