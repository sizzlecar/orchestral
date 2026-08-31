//! API composition surface backed by the same Agent SDK and Controller used by
//! embedded callers and the CLI.

use std::collections::BTreeMap;
use std::sync::Arc;

use orchestral_core::agent_protocol::wire::{
    AgentCommandEnvelope, AgentJournalRecord, AgentRunView, AgentSessionId, CommandAck, Content,
    ResourceBinding, RunId,
};
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use crate::{AgentClient, AgentControlEvent, AgentController, AgentRunHandle, AgentSdkError};

#[derive(Clone)]
pub struct AgentApi {
    controller: Arc<AgentController>,
    default_resources: Arc<Vec<ResourceBinding>>,
    sessions: Arc<RwLock<BTreeMap<AgentSessionId, AgentClient>>>,
}

impl AgentApi {
    pub fn new(controller: Arc<AgentController>) -> Self {
        Self::with_resources(controller, Vec::new())
    }

    /// Creates an API whose sessions inherit the same immutable resource
    /// bindings. Transports can therefore create sessions without knowing how
    /// a concrete Host discovered Skills or other resources.
    pub fn with_resources(
        controller: Arc<AgentController>,
        default_resources: Vec<ResourceBinding>,
    ) -> Self {
        Self {
            controller,
            default_resources: Arc::new(default_resources),
            sessions: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn create_session(
        &self,
        preferred_id: Option<AgentSessionId>,
    ) -> Result<AgentSessionId, AgentSdkError> {
        let session_id = preferred_id.unwrap_or_else(|| {
            AgentSessionId::new(format!("api-session-{}", uuid::Uuid::new_v4()))
        });
        if session_id.is_empty() {
            return Err(AgentSdkError::InvalidInput(
                "Agent session id must not be empty".to_owned(),
            ));
        }
        self.sessions
            .write()
            .await
            .entry(session_id.clone())
            .or_insert_with(|| {
                AgentClient::new(self.controller.clone(), session_id.clone())
                    .with_resources(self.default_resources.as_ref().clone())
            });
        Ok(session_id)
    }

    pub async fn start_text(
        &self,
        session_id: &AgentSessionId,
        run_id: Option<RunId>,
        input: impl Into<String>,
    ) -> Result<AgentRunHandle, AgentSdkError> {
        let client = self.session(session_id).await?;
        match run_id {
            Some(run_id) => {
                let input = input.into();
                if input.trim().is_empty() {
                    return Err(AgentSdkError::InvalidInput(
                        "Agent input must not be empty".to_owned(),
                    ));
                }
                client
                    .start_with_run_id(run_id, vec![Content::text(input)])
                    .await
            }
            None => client.start_text(input).await,
        }
    }

    pub async fn inspect(&self, run_id: &RunId) -> Result<AgentRunView, AgentSdkError> {
        Ok(self.controller.inspect(run_id).await?)
    }

    pub async fn initial_input(&self, run_id: &RunId) -> Result<Vec<Content>, AgentSdkError> {
        Ok(self.controller.initial_input(run_id).await?)
    }

    pub async fn events(
        &self,
        run_id: &RunId,
        after_run_seq: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentSdkError> {
        Ok(self.controller.events(run_id, after_run_seq).await?)
    }

    /// Subscribes to best-effort live events. Durable consumers must combine
    /// this with [`Self::events`] before rendering and after broadcast lag.
    pub async fn subscribe(
        &self,
        run_id: &RunId,
    ) -> Result<broadcast::Receiver<AgentControlEvent>, AgentSdkError> {
        Ok(self.controller.subscribe(run_id).await?)
    }

    pub async fn command(
        &self,
        command: AgentCommandEnvelope,
    ) -> Result<CommandAck, AgentSdkError> {
        Ok(self.controller.command(command).await?)
    }

    pub async fn cancel(
        &self,
        run_id: &RunId,
        reason: impl Into<String>,
    ) -> Result<CommandAck, AgentSdkError> {
        Ok(self.controller.cancel(run_id, reason).await?)
    }

    pub async fn recover(&self, run_id: &RunId) -> Result<AgentRunView, AgentSdkError> {
        Ok(self.controller.recover(run_id).await?)
    }

    async fn session(&self, session_id: &AgentSessionId) -> Result<AgentClient, AgentSdkError> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| {
                AgentSdkError::InvalidInput(format!(
                    "Agent session does not exist: {}",
                    session_id.as_str()
                ))
            })
    }
}
