//! API composition surface backed by the same Agent SDK and Controller used by
//! embedded callers and the CLI.

use std::collections::BTreeMap;
use std::sync::Arc;

use orchestral_core::agent_protocol::wire::{
    AgentCommandEnvelope, AgentJournalRecord, AgentRunView, AgentSessionId, CommandAck, Content,
    RunId,
};
use tokio::sync::RwLock;

use crate::{AgentClient, AgentController, AgentRunHandle, AgentSdkError};

#[derive(Clone)]
pub struct AgentApi {
    controller: Arc<AgentController>,
    sessions: Arc<RwLock<BTreeMap<AgentSessionId, AgentClient>>>,
}

impl AgentApi {
    pub fn new(controller: Arc<AgentController>) -> Self {
        Self {
            controller,
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
            .or_insert_with(|| AgentClient::new(self.controller.clone(), session_id.clone()));
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

    pub async fn events(
        &self,
        run_id: &RunId,
        after_run_seq: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentSdkError> {
        Ok(self.controller.events(run_id, after_run_seq).await?)
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
