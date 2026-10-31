//! Future SDK turns use explicit CLI settings; live native terminals retain
//! their own configuration and cannot be silently reconfigured through peers.
use super::*;
use orchestral_core::agent_connector::{
    AgentSessionActionStatus, SESSION_SET_MODEL_ACTION, SESSION_SET_REASONING_ACTION,
};
use serde_json::json;
use std::fs;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Settings {
    model: Option<String>,
    reasoning_effort: Option<String>,
}

pub(super) fn descriptors() -> [AgentSessionActionDescriptor; 2] {
    [SESSION_SET_MODEL_ACTION, SESSION_SET_REASONING_ACTION].map(|id| {
        let model = id == SESSION_SET_MODEL_ACTION;
        let field = if model { "model" } else { "reasoning_effort" };
        let mut value = json!({"type":"string","minLength":1});
        if !model { value["enum"] = json!(["default","low","medium","high","xhigh","max"]); }
        AgentSessionActionDescriptor {
            action_id: AgentSessionActionId::new(id), title: if model { "切换模型" } else { "推理强度" }.into(),
            description: "用于此会话的后续 SDK 请求；原 Claude 终端须在终端内修改。具体模型支持由 Claude 校验。".into(),
            input_schema: Some(json!({"type":"object","additionalProperties":false,"required":[field],"properties":{field:value}})),
            input_channel: false, execution: AgentSessionActionExecution::Immediate,
        }
    })
}

fn path(
    connector: &ClaudeCodeConnector,
    id: &AgentSessionId,
) -> Result<PathBuf, AgentConnectorError> {
    validate_session_id(id.as_str())?;
    Ok(connector
        .config
        .session_store_dir
        .join("model-settings")
        .join(format!("{id}.json")))
}

pub(super) fn load(
    connector: &ClaudeCodeConnector,
    id: &AgentSessionId,
) -> Result<Settings, AgentConnectorError> {
    let path = path(connector, id)?;
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| AgentConnectorError::protocol(error.to_string())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Settings::default()),
        Err(error) => Err(io_error(error)),
    }
}

impl Settings {
    pub(super) fn enrich(&self, profile: &mut AgentSessionExecutionProfile) {
        if self.model.is_some() {
            profile.model.clone_from(&self.model);
        }
        if self.reasoning_effort.is_some() {
            profile.reasoning_effort.clone_from(&self.reasoning_effort);
        }
    }
    pub(super) fn append_args(&self, args: &mut Vec<String>) {
        if let Some(model) = &self.model {
            args.extend(["--model".into(), model.clone()]);
        }
        if let Some(effort) = self
            .reasoning_effort
            .as_ref()
            .filter(|value| value.as_str() != "default")
        {
            args.extend(["--effort".into(), effort.clone()]);
        }
    }
}

pub(super) async fn apply(
    connector: &ClaudeCodeConnector,
    request: InvokeAgentSessionActionRequest,
) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
    let id = &request.session_id;
    if request.run_id.is_some() {
        return Err(AgentConnectorError::invalid(
            "model settings are immediate actions",
        ));
    }
    if connector.native_owner(id).await? {
        return Err(AgentConnectorError::unsupported(
            "此会话由原 Claude 终端拥有，请在终端中修改模型与推理强度。",
        ));
    }
    if connector
        .provider_state()
        .session_state(id)
        .is_some_and(|state| state != AgentSessionState::Idle)
    {
        return Err(AgentConnectorError::unsupported(
            "请等待当前任务结束后再修改模型设置。",
        ));
    }
    let detail = AgentConnector::read_session(connector, id).await?;
    let field = if request.action_id.as_str() == SESSION_SET_MODEL_ACTION {
        "model"
    } else {
        "reasoning_effort"
    };
    let object = request
        .arguments
        .as_object()
        .ok_or_else(|| AgentConnectorError::invalid("model settings require an object"))?;
    if object.len() != 1 {
        return Err(AgentConnectorError::invalid(
            "change one model setting at a time",
        ));
    }
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 1024)
        .ok_or_else(|| AgentConnectorError::invalid("model setting must be a nonempty string"))?;
    if field == "reasoning_effort"
        && !matches!(
            value,
            "default" | "low" | "medium" | "high" | "xhigh" | "max"
        )
    {
        return Err(AgentConnectorError::invalid(
            "unsupported Claude effort value",
        ));
    }
    let mut settings = load(connector, id)?;
    if field == "model" {
        settings.model = Some(value.into());
    } else {
        settings.reasoning_effort = Some(value.into());
    }
    let destination = path(connector, id)?;
    let directory = destination.parent().expect("settings parent");
    fs::create_dir_all(directory).map_err(io_error)?;
    let temporary = directory.join(format!("{}.pending", uuid::Uuid::new_v4()));
    let result = (|| {
        use std::io::Write;
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary).map_err(io_error)?;
        file.write_all(
            &serde_json::to_vec(&settings)
                .map_err(|error| AgentConnectorError::protocol(error.to_string()))?,
        )
        .map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        fs::rename(&temporary, destination).map_err(io_error)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result?;
    let mut summary = detail.summary;
    settings.enrich(&mut summary.execution_profile);
    Ok(AgentSessionActionOutcome {
        status: AgentSessionActionStatus::Completed,
        session: Some(summary),
        content: Vec::new(),
        details: Value::Null,
    })
}
