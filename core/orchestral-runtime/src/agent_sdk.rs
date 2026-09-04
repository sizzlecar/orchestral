//! Provider-neutral SDK surface over the Agent Protocol control plane.
//!
//! This layer owns no planning or model loop. CLI, HTTP adapters, and embedded
//! applications can therefore share exactly the same `AgentController`
//! sequencing, commands, inspection, and durable event semantics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use orchestral_core::agent_connector::AgentSessionActionInvocation;
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    wire::{
        AgentCommand, AgentCommandEnvelope, AgentExecutionRef, AgentJournalRecord,
        AgentRunEnvelope, AgentRunView, AgentSessionId, CommandAck, CommandId, Content,
        ContentBody, Extensions, RequestId, RequestResolution, ResourceBinding, RunId,
    },
    AGENT_PROTOCOL_V1,
};
use tokio::sync::broadcast;

use crate::agent_control::{AgentControlError, AgentControlEvent, AgentController};

#[derive(Clone)]
pub struct AgentClient {
    controller: Arc<AgentController>,
    session_id: AgentSessionId,
    resources: Arc<Vec<ResourceBinding>>,
    next_run: Arc<AtomicU64>,
}

impl AgentClient {
    pub fn new(controller: Arc<AgentController>, session_id: AgentSessionId) -> Self {
        Self {
            controller,
            session_id,
            resources: Arc::new(Vec::new()),
            next_run: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_resources(mut self, resources: Vec<ResourceBinding>) -> Self {
        self.resources = Arc::new(resources);
        self
    }

    pub fn session_id(&self) -> &AgentSessionId {
        &self.session_id
    }

    pub fn controller(&self) -> &Arc<AgentController> {
        &self.controller
    }

    pub async fn start_text(
        &self,
        input: impl Into<String>,
    ) -> Result<AgentRunHandle, AgentSdkError> {
        let input = input.into();
        if input.trim().is_empty() {
            return Err(AgentSdkError::InvalidInput(
                "Agent input must not be empty".to_owned(),
            ));
        }
        let sequence = self.next_run.fetch_add(1, Ordering::SeqCst);
        self.start_with_run_id(
            RunId::new(format!(
                "sdk-{}-{sequence}-{}",
                self.session_id.as_str(),
                uuid::Uuid::new_v4()
            )),
            vec![Content::text(input)],
        )
        .await
    }

    pub async fn start_with_run_id(
        &self,
        run_id: RunId,
        input: Vec<Content>,
    ) -> Result<AgentRunHandle, AgentSdkError> {
        self.start_with_run_id_and_extensions(run_id, input, Extensions::new())
            .await
    }

    pub async fn start_with_run_id_and_extensions(
        &self,
        run_id: RunId,
        input: Vec<Content>,
        extensions: Extensions,
    ) -> Result<AgentRunHandle, AgentSdkError> {
        let mut run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            self.session_id.clone(),
            run_id.clone(),
            input,
        )?;
        run.spec.resources = self.resources.as_ref().clone();
        run.spec.extensions = extensions;
        let run = AgentRunEnvelope::seal(run.spec)?;
        let execution = self.controller.start(run).await?;
        Ok(AgentRunHandle {
            controller: self.controller.clone(),
            run_id,
            execution,
        })
    }

    pub async fn start_session_action_with_run_id(
        &self,
        run_id: RunId,
        title: impl Into<String>,
        action: AgentSessionActionInvocation,
    ) -> Result<AgentRunHandle, AgentSdkError> {
        let title = title.into();
        if title.trim().is_empty() {
            return Err(AgentSdkError::InvalidInput(
                "Agent session action title must not be empty".to_owned(),
            ));
        }
        let mut run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            self.session_id.clone(),
            run_id.clone(),
            vec![Content::text(title)],
        )?;
        run.spec.resources = self.resources.as_ref().clone();
        action
            .insert_into(&mut run.spec)
            .map_err(|error| AgentSdkError::InvalidInput(error.to_string()))?;
        let run = AgentRunEnvelope::seal(run.spec)?;
        let execution = self.controller.start(run).await?;
        Ok(AgentRunHandle {
            controller: self.controller.clone(),
            run_id,
            execution,
        })
    }

    /// Convenience path for non-interactive calls. It returns on a terminal
    /// state or when Host input/approval is required, never by bypassing the
    /// Agent request protocol.
    pub async fn run_text(&self, input: impl Into<String>) -> Result<AgentTurn, AgentSdkError> {
        self.start_text(input).await?.wait_until_blocked().await
    }
}

#[derive(Clone)]
pub struct AgentRunHandle {
    controller: Arc<AgentController>,
    run_id: RunId,
    execution: AgentExecutionRef,
}

impl AgentRunHandle {
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn execution(&self) -> &AgentExecutionRef {
        &self.execution
    }

    pub async fn inspect(&self) -> Result<AgentRunView, AgentSdkError> {
        Ok(self.controller.inspect(&self.run_id).await?)
    }

    pub async fn events(
        &self,
        after_run_seq: u64,
    ) -> Result<Vec<AgentJournalRecord>, AgentSdkError> {
        Ok(self.controller.events(&self.run_id, after_run_seq).await?)
    }

    pub async fn subscribe(&self) -> Result<broadcast::Receiver<AgentControlEvent>, AgentSdkError> {
        Ok(self.controller.subscribe(&self.run_id).await?)
    }

    pub async fn command(
        &self,
        command: AgentCommandEnvelope,
    ) -> Result<CommandAck, AgentSdkError> {
        if command.run_id != self.run_id {
            return Err(AgentSdkError::InvalidInput(
                "Agent command belongs to another Run".to_owned(),
            ));
        }
        Ok(self.controller.command(command).await?)
    }

    pub async fn cancel(&self, reason: impl Into<String>) -> Result<CommandAck, AgentSdkError> {
        Ok(self.controller.cancel(&self.run_id, reason).await?)
    }

    pub async fn steer_text(&self, input: impl Into<String>) -> Result<CommandAck, AgentSdkError> {
        let input = input.into();
        if input.trim().is_empty() {
            return Err(AgentSdkError::InvalidInput(
                "Steer input must not be empty".to_owned(),
            ));
        }
        let command = AgentCommandEnvelope::new(
            CommandId::new(format!("sdk-steer-{}", uuid::Uuid::new_v4())),
            self.run_id.clone(),
            None,
            AgentCommand::Steer {
                content: vec![Content::text(input)],
            },
        )?;
        self.command(command).await
    }

    pub async fn resolve_input_text(
        &self,
        request_id: RequestId,
        input: impl Into<String>,
    ) -> Result<CommandAck, AgentSdkError> {
        let input = input.into();
        if input.trim().is_empty() {
            return Err(AgentSdkError::InvalidInput(
                "Input resolution must not be empty".to_owned(),
            ));
        }
        let command = AgentCommandEnvelope::new(
            CommandId::new(format!("sdk-input-{}", uuid::Uuid::new_v4())),
            self.run_id.clone(),
            Some(request_id),
            AgentCommand::ResolveRequest {
                response: RequestResolution::Input {
                    content: vec![Content::text(input)],
                },
            },
        )?;
        self.command(command).await
    }

    pub async fn wait_until_blocked(&self) -> Result<AgentTurn, AgentSdkError> {
        let mut events = self.subscribe().await?;
        loop {
            let view = self.inspect().await?;
            if is_stable_turn_boundary(&view) {
                return Ok(AgentTurn {
                    run_id: self.run_id.clone(),
                    view,
                });
            }
            match events.recv().await {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    let view = self.inspect().await?;
                    if is_stable_turn_boundary(&view) {
                        return Ok(AgentTurn {
                            run_id: self.run_id.clone(),
                            view,
                        });
                    }
                    return Err(AgentSdkError::ControlStreamClosed(self.run_id.clone()));
                }
            }
        }
    }
}

fn is_stable_turn_boundary(view: &AgentRunView) -> bool {
    view.state.is_terminal()
        || view.state.status() == AgentRunStatus::Unknown
        || !view.pending_requests.is_empty()
}

#[derive(Debug, Clone)]
pub struct AgentTurn {
    pub run_id: RunId,
    pub view: AgentRunView,
}

impl AgentTurn {
    pub fn status(&self) -> AgentRunStatus {
        self.view.state.status()
    }

    pub fn is_waiting(&self) -> bool {
        !self.view.state.is_terminal() && !self.view.pending_requests.is_empty()
    }

    pub fn final_text(&self) -> Option<&str> {
        let delivery = self.view.delivery.as_ref()?;
        match &delivery.final_response.body {
            ContentBody::Inline(serde_json::Value::String(text)) => Some(text),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AgentSdkError {
    #[error("invalid Agent SDK input: {0}")]
    InvalidInput(String),
    #[error(transparent)]
    Protocol(#[from] orchestral_core::agent_protocol::wire::AgentProtocolError),
    #[error(transparent)]
    Control(#[from] AgentControlError),
    #[error("Agent control stream closed before Run {0} reached a stable boundary")]
    ControlStreamClosed(RunId),
}
