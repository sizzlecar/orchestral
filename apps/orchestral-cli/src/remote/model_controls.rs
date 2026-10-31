//! Session-scoped model controls. The Host owns configuration and new Runs pin
//! their own provenance; a browser never sends credentials or provider wire.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{bail, Context};
use orchestral_core::agent_connector::AgentSessionExecutionProfile;
use orchestral_core::agent_protocol::wire::{AgentSessionId, RunId};
use orchestral_core::config::ReasoningPreference;
use orchestral_core::session_history::SessionOrigin;
use orchestral_runtime::api::AgentApi;
use orchestral_runtime::AgentRunHandle;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::agent::{AgentHost, AgentRunOptions, HostMetadata};
use crate::model_controls::{apply_selection, Selection};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelChoice {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ModelSettingsView {
    pub session_id: String,
    pub profile: AgentSessionExecutionProfile,
    pub models: Vec<ModelChoice>,
    pub reasoning: Vec<ModelChoice>,
    pub reasoning_declared: bool,
    pub notice: Option<String>,
    pub model_supported: bool,
    pub reasoning_supported: bool,
    pub allow_custom_model: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModelChange {
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub expected_model: String,
}

impl ModelChange {
    pub(crate) fn selection(
        &self,
        metadata: &HostMetadata,
        current: &str,
    ) -> anyhow::Result<Selection> {
        anyhow::ensure!(
            self.expected_model == current,
            "模型设置已变化，请刷新后重试。"
        );
        match (&self.model, &self.reasoning_effort) {
            (Some(value), None) => {
                anyhow::ensure!(
                    !value.trim().is_empty() && value.len() <= 1024,
                    "invalid model selection"
                );
                let selection = if value.trim_start().starts_with('{') {
                    serde_json::from_str(value).context("invalid model selection")?
                } else {
                    Selection::ApiModel {
                        backend: metadata.model_backend.name.clone(),
                        id: value.trim().into(),
                    }
                };
                anyhow::ensure!(
                    matches!(
                        selection,
                        Selection::ApiModel { .. } | Selection::Profile { .. }
                    ),
                    "invalid model selection"
                );
                Ok(selection)
            }
            (None, Some(value)) => {
                let value = value
                    .parse::<ReasoningPreference>()
                    .map_err(anyhow::Error::msg)?;
                metadata
                    .model_controls
                    .validate_reasoning_preference(&value)?;
                if let Some(declared) = metadata
                    .model_controls
                    .reasoning_capabilities()
                    .supported_preferences
                {
                    anyhow::ensure!(
                        value == ReasoningPreference::Default || declared.contains(&value),
                        "当前模型不支持这个推理强度。"
                    );
                }
                Ok(Selection::Reasoning {
                    backend: metadata.model_backend.name.clone(),
                    model: current.into(),
                    value,
                })
            }
            _ => bail!("请一次修改模型或推理强度。"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ModelSnapshot {
    pub api: AgentApi,
    approvals: Arc<orchestral_runtime::InMemoryHostApprovalBroker>,
    metadata: HostMetadata,
    model: String,
    pub(crate) profile: AgentSessionExecutionProfile,
}

impl ModelSnapshot {
    pub(crate) fn from_host(host: &AgentHost) -> Self {
        Self {
            api: host.api.clone(),
            approvals: host.approvals.clone(),
            metadata: host.metadata.clone(),
            model: host.model.clone(),
            profile: host.execution_profile.clone(),
        }
    }

    async fn view(&self, session: &str) -> anyhow::Result<ModelSettingsView> {
        let mut models = Vec::new();
        let choice = |selection: Selection, label: String| ModelChoice {
            value: serde_json::to_string(&selection).expect("typed model selection"),
            label,
        };
        models.push(choice(
            Selection::ApiModel {
                backend: self.metadata.model_backend.name.clone(),
                id: self.model.clone(),
            },
            self.model.clone(),
        ));
        for profile in &self.metadata.models {
            models.push(choice(
                Selection::Profile {
                    name: profile.name.clone(),
                },
                format!("{} · {} / {}", profile.name, profile.backend, profile.model),
            ));
        }
        let mut notices = Vec::new();
        match tokio::time::timeout(
            Duration::from_secs(5),
            self.metadata.model_controls.list_models(),
        )
        .await
        {
            Ok(Ok(Some(ids))) => {
                for id in ids {
                    if id != self.model && !id.trim().is_empty() {
                        models.push(choice(
                            Selection::ApiModel {
                                backend: self.metadata.model_backend.name.clone(),
                                id: id.clone(),
                            },
                            id,
                        ));
                    }
                }
            }
            Ok(Ok(None)) => {}
            _ => notices.push("模型目录暂不可用，可继续选择已配置的模型。"),
        }
        let capabilities = match tokio::time::timeout(
            Duration::from_secs(5),
            self.metadata
                .model_controls
                .resolve_reasoning_capabilities(),
        )
        .await
        {
            Ok(Ok(capabilities)) => capabilities,
            _ => {
                notices.push("推理能力查询暂不可用。");
                self.metadata.model_controls.reasoning_capabilities()
            }
        };
        let declared = capabilities.supported_preferences.is_some();
        let mut reasoning = capabilities.choices();
        if !declared {
            for value in self
                .metadata
                .model_controls
                .reasoning_preference_candidates()
                .into_iter()
                .chain(std::iter::once(self.metadata.reasoning.clone()))
            {
                if !reasoning.contains(&value)
                    && self
                        .metadata
                        .model_controls
                        .validate_reasoning_preference(&value)
                        .is_ok()
                {
                    reasoning.push(value);
                }
            }
            notices.push("未声明的推理选项由服务校验。");
        }
        Ok(ModelSettingsView {
            model_supported: true,
            reasoning_supported: true,
            allow_custom_model: true,
            session_id: session.into(),
            profile: self.profile.clone(),
            models,
            reasoning: reasoning
                .into_iter()
                .map(|value| ModelChoice {
                    value: value.to_string(),
                    label: value.to_string(),
                })
                .collect(),
            reasoning_declared: declared,
            notice: (!notices.is_empty()).then(|| notices.join(" ")),
        })
    }
}

pub(crate) struct LocalModelRequest {
    pub change: ModelChange,
    pub reply: oneshot::Sender<Result<AgentSessionExecutionProfile, String>>,
}

pub(crate) struct LocalModelControls {
    snapshot: RwLock<ModelSnapshot>,
    changes: mpsc::Sender<LocalModelRequest>,
}

impl LocalModelControls {
    pub(crate) fn new(host: &AgentHost) -> (Arc<Self>, mpsc::Receiver<LocalModelRequest>) {
        let (changes, requests) = mpsc::channel(8);
        (
            Arc::new(Self {
                snapshot: RwLock::new(ModelSnapshot::from_host(host)),
                changes,
            }),
            requests,
        )
    }
    pub(crate) fn publish(&self, host: &AgentHost) {
        *self.snapshot.write().expect("model snapshot") = ModelSnapshot::from_host(host);
    }
    pub(crate) fn snapshot(&self) -> ModelSnapshot {
        self.snapshot.read().expect("model snapshot").clone()
    }
    pub(crate) async fn change(
        &self,
        change: ModelChange,
    ) -> anyhow::Result<AgentSessionExecutionProfile> {
        let (reply, result) = oneshot::channel();
        self.changes
            .try_send(LocalModelRequest { change, reply })
            .map_err(|_| anyhow::anyhow!("终端正在处理其他设置，请稍后重试。"))?;
        tokio::time::timeout(Duration::from_secs(30), result)
            .await
            .context("终端设置请求超时，请刷新确认当前设置。")?
            .context("终端已断开。")?
            .map_err(anyhow::Error::msg)
    }
}

pub(crate) enum NativeModelControls {
    Local(Arc<LocalModelControls>),
    Gateway(Box<Mutex<GatewayModels>>),
}

pub(crate) struct GatewayModels {
    base: Arc<AgentHost>,
    options: AgentRunOptions,
    sessions: BTreeMap<String, (Arc<AgentHost>, AgentRunOptions)>,
    runs: BTreeMap<String, ModelSnapshot>,
}

impl NativeModelControls {
    pub(crate) fn gateway(host: Arc<AgentHost>, options: AgentRunOptions) -> Arc<Self> {
        Arc::new(Self::Gateway(Box::new(Mutex::new(GatewayModels {
            base: host,
            options,
            sessions: BTreeMap::new(),
            runs: BTreeMap::new(),
        }))))
    }
    pub(crate) async fn view(&self, session: &str) -> anyhow::Result<ModelSettingsView> {
        let snapshot = match self {
            Self::Local(local) => local.snapshot(),
            Self::Gateway(models) => {
                let mut models = models.lock().await;
                let (host, _) = models.session_host(session).await?;
                ModelSnapshot::from_host(&host)
            }
        };
        snapshot.view(session).await
    }
    pub(crate) async fn profile(&self, session: &str) -> Option<AgentSessionExecutionProfile> {
        match self {
            Self::Local(local) => Some(local.snapshot().profile),
            Self::Gateway(models) => {
                let models = models.lock().await;
                models
                    .sessions
                    .get(session)
                    .map(|(host, _)| host.execution_profile.clone())
            }
        }
    }
    pub(crate) async fn change(
        &self,
        session: &str,
        change: ModelChange,
    ) -> anyhow::Result<AgentSessionExecutionProfile> {
        match self {
            Self::Local(local) => local.change(change).await,
            Self::Gateway(models) => {
                let mut models = models.lock().await;
                let (host, mut options) = models.session_host(session).await?;
                for run in host
                    .api
                    .catalog_runs()
                    .await?
                    .into_iter()
                    .filter(|run| run.session_id.as_str() == session)
                {
                    anyhow::ensure!(
                        host.api.inspect(&run.run_id).await?.state.is_terminal(),
                        "请等待当前任务结束后再切换模型或推理强度。"
                    );
                    models.runs.remove(run.run_id.as_str());
                }
                let selection = change.selection(&host.metadata, &host.model)?;
                apply_selection(
                    &host.metadata,
                    &host.model,
                    &mut options.model_overrides,
                    selection,
                )?;
                let next = Arc::new(host.reconfigure(&options).await?);
                let profile = next.execution_profile.clone();
                if !Arc::ptr_eq(&host, &models.base) {
                    host.shutdown().await;
                }
                models.sessions.insert(session.into(), (next, options));
                Ok(profile)
            }
        }
    }
    pub(crate) async fn api_for_run(&self, run: &RunId) -> anyhow::Result<AgentApi> {
        match self {
            Self::Local(local) => Ok(local.snapshot().api),
            Self::Gateway(models) => {
                let mut models = models.lock().await;
                if let Some(snapshot) = models.runs.get(run.as_str()) {
                    return Ok(snapshot.api.clone());
                }
                if !models.base.api.has_run(run).await? {
                    return Ok(models.base.api.clone());
                }
                if models.base.api.inspect(run).await?.state.is_terminal() {
                    return Ok(models.base.api.clone());
                }
                let origin =
                    SessionOrigin::from_extensions(&models.base.api.run_extensions(run).await?)?;
                if let Some(origin) = origin {
                    let options = models.origin_options(&origin);
                    let host = models.base.reconfigure(&options).await?;
                    let api = host.api.clone();
                    models
                        .runs
                        .insert(run.as_str().into(), ModelSnapshot::from_host(&host));
                    return Ok(api);
                }
                Ok(models.base.api.clone())
            }
        }
    }
    pub(crate) async fn approvals_for_run(
        &self,
        run: &RunId,
    ) -> anyhow::Result<Arc<orchestral_runtime::InMemoryHostApprovalBroker>> {
        match self {
            Self::Local(local) => Ok(local.snapshot().approvals),
            Self::Gateway(models) => {
                let models = models.lock().await;
                Ok(models
                    .runs
                    .get(run.as_str())
                    .map(|snapshot| snapshot.approvals.clone())
                    .unwrap_or_else(|| models.base.approvals.clone()))
            }
        }
    }
    pub(crate) async fn start(
        &self,
        session: &AgentSessionId,
        run: RunId,
        input: String,
    ) -> anyhow::Result<(AgentRunHandle, AgentApi)> {
        let Self::Gateway(models) = self else {
            bail!("local terminal starts its own Runs")
        };
        let mut models = models.lock().await;
        let snapshot = if models.base.api.has_run(&run).await? {
            models
                .runs
                .get(run.as_str())
                .cloned()
                .unwrap_or_else(|| ModelSnapshot::from_host(&models.base))
        } else {
            ModelSnapshot::from_host(&models.session_host(session.as_str()).await?.0)
        };
        let api = snapshot.api.clone();
        api.create_session(Some(session.clone())).await?;
        let handle = api.start_text(session, Some(run.clone()), input).await?;
        models.runs.insert(run.as_str().into(), snapshot);
        Ok((handle, api))
    }
}

impl GatewayModels {
    fn origin_options(&self, origin: &SessionOrigin) -> AgentRunOptions {
        let mut options = self.options.clone();
        let initial = self.base.origin();
        if origin.backend != initial.backend || origin.model_profile != initial.model_profile {
            options.model_overrides.backend = origin.backend.clone();
            options.model_overrides.model_profile = origin.model_profile.clone();
            options.model_overrides.base_url = None;
            options.model_overrides.api_key_env = None;
            options.model_overrides.no_auth = false;
        }
        options.model_overrides.model = Some(origin.model.clone());
        options.model_overrides.reasoning = origin.reasoning.clone();
        options
    }
    async fn last_origin(&self, session: &str) -> anyhow::Result<Option<SessionOrigin>> {
        let mut runs = self.base.api.catalog_runs().await?;
        runs.retain(|run| run.session_id.as_str() == session);
        runs.sort_by_key(|run| (run.created_at_unix_ms, run.run_id.clone()));
        match runs.last() {
            Some(run) => Ok(SessionOrigin::from_extensions(
                &self.base.api.run_extensions(&run.run_id).await?,
            )?),
            None => Ok(None),
        }
    }
    async fn session_host(
        &mut self,
        session: &str,
    ) -> anyhow::Result<(Arc<AgentHost>, AgentRunOptions)> {
        if let Some(entry) = self.sessions.get(session) {
            return Ok(entry.clone());
        }
        let entry = if let Some(origin) = self.last_origin(session).await? {
            let options = self.origin_options(&origin);
            (Arc::new(self.base.reconfigure(&options).await?), options)
        } else {
            (self.base.clone(), self.options.clone())
        };
        self.sessions.insert(session.into(), entry.clone());
        Ok(entry)
    }
}
