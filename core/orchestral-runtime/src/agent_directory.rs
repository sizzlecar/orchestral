//! Runtime registry for provider-neutral Agent session connectors.
//!
//! The directory owns discovery routing. Each registration also owns a normal
//! [`AgentController`](crate::AgentController), so active external turns use
//! the same durable Agent Protocol path as the built-in Generic Agent.

use std::collections::BTreeMap;
use std::sync::Arc;

use orchestral_core::agent_connector::{
    AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorHealth,
    AgentConnectorId, AgentSessionActionExecution, AgentSessionActionInvocation,
    AgentSessionActionOutcome, AgentSessionActionStatus, AgentSessionChange, AgentSessionDetail,
    AgentSessionListQuery, AgentSessionPage, AgentSessionReadQuery, AgentSessionSummary,
    CreateAgentSessionRequest, InvokeAgentSessionActionRequest,
};
use orchestral_core::agent_protocol::spi::{
    AgentJournalStore, AgentProvider, InMemoryAgentJournalStore,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, Extensions, RunId};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use crate::api::AgentApi;
use crate::{AgentController, AgentRunHandle, AgentSdkError};

struct AgentDirectoryEntry {
    descriptor: AgentConnectorDescriptor,
    connector: Arc<dyn AgentConnector>,
    api: AgentApi,
}

/// Process-level registry for installed Agent connectors.
///
/// Connector IDs namespace session IDs. Two different Agents may therefore
/// expose the same opaque session ID without colliding.
#[derive(Default)]
pub struct AgentDirectory {
    entries: RwLock<BTreeMap<AgentConnectorId, Arc<AgentDirectoryEntry>>>,
}

impl AgentDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(
        &self,
        connector: Arc<dyn AgentConnector>,
        provider: Arc<dyn AgentProvider>,
    ) -> Result<(), AgentDirectoryError> {
        self.register_with_journal(
            connector,
            provider,
            Arc::new(InMemoryAgentJournalStore::default()),
        )
        .await
    }

    pub async fn register_with_journal(
        &self,
        connector: Arc<dyn AgentConnector>,
        provider: Arc<dyn AgentProvider>,
        journal: Arc<dyn AgentJournalStore>,
    ) -> Result<(), AgentDirectoryError> {
        let descriptor = connector.describe();
        descriptor.validate()?;
        let connector_id = descriptor.connector_id.clone();
        let controller = Arc::new(AgentController::with_journal_store(
            provider,
            descriptor.provider_binding.clone(),
            journal,
        )?);
        let entry = Arc::new(AgentDirectoryEntry {
            descriptor,
            connector,
            api: AgentApi::new(controller),
        });

        let mut entries = self.entries.write().await;
        if entries.contains_key(&connector_id) {
            return Err(AgentDirectoryError::RegistrationConflict(connector_id));
        }
        entries.insert(connector_id, entry);
        Ok(())
    }

    pub async fn connectors(&self) -> Vec<AgentConnectorDescriptor> {
        self.entries
            .read()
            .await
            .values()
            .map(|entry| entry.descriptor.clone())
            .collect()
    }

    pub async fn health(
        &self,
        connector_id: &AgentConnectorId,
    ) -> Result<AgentConnectorHealth, AgentDirectoryError> {
        let entry = self.entry(connector_id).await?;
        self.verify_descriptor(&entry)?;
        Ok(entry.connector.health().await?)
    }

    pub async fn list_sessions(
        &self,
        connector_id: &AgentConnectorId,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentDirectoryError> {
        query.validate()?;
        let entry = self.entry(connector_id).await?;
        self.verify_descriptor(&entry)?;
        let requested_limit = query.limit;
        let page = entry.connector.list_sessions(query).await?;
        page.validate_for(connector_id, requested_limit)?;
        Ok(page)
    }

    pub async fn read_session(
        &self,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentDirectoryError> {
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty").into());
        }
        let entry = self.entry(connector_id).await?;
        self.verify_descriptor(&entry)?;
        let detail = entry.connector.read_session(session_id).await?;
        detail.validate_for(connector_id)?;
        if detail.summary.session_id != *session_id {
            return Err(AgentConnectorError::protocol(
                "connector returned a different session than requested",
            )
            .into());
        }
        Ok(detail)
    }

    pub async fn read_session_page(
        &self,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
        query: AgentSessionReadQuery,
    ) -> Result<AgentSessionDetail, AgentDirectoryError> {
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty").into());
        }
        query.validate()?;
        let entry = self.entry(connector_id).await?;
        self.verify_descriptor(&entry)?;
        let detail = entry.connector.read_session_page(session_id, query).await?;
        detail.validate_for(connector_id)?;
        if detail.summary.session_id != *session_id {
            return Err(AgentConnectorError::protocol(
                "connector returned a different session than requested",
            )
            .into());
        }
        Ok(detail)
    }

    pub async fn subscribe_session_changes(
        &self,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
    ) -> Result<broadcast::Receiver<AgentSessionChange>, AgentDirectoryError> {
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty").into());
        }
        let entry = self.entry(connector_id).await?;
        self.verify_descriptor(&entry)?;
        Ok(entry
            .connector
            .subscribe_session_changes(session_id)
            .await?)
    }

    pub async fn create_session(
        &self,
        connector_id: &AgentConnectorId,
        request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentDirectoryError> {
        let entry = self.entry(connector_id).await?;
        self.verify_descriptor(&entry)?;
        if !entry.descriptor.capabilities.create {
            return Err(AgentConnectorError::unsupported(
                "connector does not declare session creation",
            )
            .into());
        }
        let summary = entry.connector.create_session(request).await?;
        summary.validate_for(connector_id)?;
        entry
            .api
            .create_session(Some(summary.session_id.clone()))
            .await?;
        Ok(summary)
    }

    pub async fn invoke_action(
        &self,
        connector_id: &AgentConnectorId,
        request: InvokeAgentSessionActionRequest,
    ) -> Result<AgentSessionActionOutcome, AgentDirectoryError> {
        if request.session_id.is_empty() || request.action_id.is_empty() {
            return Err(AgentConnectorError::invalid(
                "session action requires session and action identities",
            )
            .into());
        }
        let entry = self.entry(connector_id).await?;
        self.verify_descriptor(&entry)?;
        let action = entry.descriptor.action(&request.action_id).ok_or_else(|| {
            AgentConnectorError::unsupported(format!(
                "connector does not declare action {}",
                request.action_id
            ))
        })?;
        if action.input_schema.is_none() && !request.arguments.is_null() {
            return Err(AgentConnectorError::invalid(format!(
                "action {} takes no arguments",
                request.action_id
            ))
            .into());
        }
        if action.execution == AgentSessionActionExecution::Run {
            // Resolve the session before starting so a forged connector/session
            // pair cannot allocate a Host-only Run.
            self.read_session_page(
                connector_id,
                &request.session_id,
                AgentSessionReadQuery {
                    cursor: None,
                    limit: 1,
                },
            )
            .await?;
            entry
                .api
                .create_session(Some(request.session_id.clone()))
                .await?;
            let run_id = request
                .run_id
                .unwrap_or_else(|| RunId::new(format!("session-action-{}", uuid::Uuid::new_v4())));
            entry
                .api
                .start_session_action(
                    &request.session_id,
                    run_id.clone(),
                    action.title.clone(),
                    AgentSessionActionInvocation {
                        action_id: request.action_id,
                        arguments: request.arguments,
                    },
                )
                .await?;
            return Ok(AgentSessionActionOutcome {
                status: AgentSessionActionStatus::Running { run_id },
                session: None,
                content: Vec::new(),
                details: serde_json::Value::Null,
            });
        }
        if request.run_id.is_some() {
            return Err(AgentConnectorError::invalid(
                "run_id is only valid for Run session actions",
            )
            .into());
        }
        let outcome = entry.connector.invoke_action(request).await?;
        if !matches!(outcome.status, AgentSessionActionStatus::Completed) {
            return Err(AgentConnectorError::protocol(
                "an immediate session action returned a running outcome",
            )
            .into());
        }
        if let Some(summary) = &outcome.session {
            summary.validate_for(connector_id)?;
        }
        for content in &outcome.content {
            content
                .validate_integrity()
                .map_err(|error| AgentConnectorError::protocol(error.to_string()))?;
        }
        Ok(outcome)
    }

    /// Start one Agent Protocol Run against a connector-owned session.
    pub async fn start_text(
        &self,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
        run_id: Option<RunId>,
        input: impl Into<String>,
    ) -> Result<AgentRunHandle, AgentDirectoryError> {
        // Read first so a stale or forged connector/session pair cannot create
        // a Host-only session that the external Provider cannot resolve.
        self.read_session_page(
            connector_id,
            session_id,
            AgentSessionReadQuery {
                cursor: None,
                limit: 1,
            },
        )
        .await?;
        let entry = self.entry(connector_id).await?;
        entry.api.create_session(Some(session_id.clone())).await?;
        Ok(entry.api.start_text(session_id, run_id, input).await?)
    }

    /// Start one Agent Protocol Run with provider-neutral Content blocks.
    pub async fn start_content(
        &self,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
        run_id: Option<RunId>,
        input: Vec<orchestral_core::agent_protocol::wire::Content>,
    ) -> Result<AgentRunHandle, AgentDirectoryError> {
        self.start_content_with_extensions(
            connector_id,
            session_id,
            run_id,
            input,
            Extensions::new(),
        )
        .await
    }

    /// Starts a Run with digest-bound, namespaced Host metadata. The directory
    /// keeps this provider-neutral and forwards the immutable extensions
    /// through the shared Agent Protocol controller.
    pub async fn start_content_with_extensions(
        &self,
        connector_id: &AgentConnectorId,
        session_id: &AgentSessionId,
        run_id: Option<RunId>,
        input: Vec<orchestral_core::agent_protocol::wire::Content>,
        extensions: Extensions,
    ) -> Result<AgentRunHandle, AgentDirectoryError> {
        self.read_session_page(
            connector_id,
            session_id,
            AgentSessionReadQuery {
                cursor: None,
                limit: 1,
            },
        )
        .await?;
        let entry = self.entry(connector_id).await?;
        entry.api.create_session(Some(session_id.clone())).await?;
        Ok(entry
            .api
            .start_content_with_extensions(session_id, run_id, input, extensions)
            .await?)
    }

    pub async fn agent_api(
        &self,
        connector_id: &AgentConnectorId,
    ) -> Result<AgentApi, AgentDirectoryError> {
        Ok(self.entry(connector_id).await?.api.clone())
    }

    async fn entry(
        &self,
        connector_id: &AgentConnectorId,
    ) -> Result<Arc<AgentDirectoryEntry>, AgentDirectoryError> {
        self.entries
            .read()
            .await
            .get(connector_id)
            .cloned()
            .ok_or_else(|| AgentDirectoryError::ConnectorNotFound(connector_id.clone()))
    }

    fn verify_descriptor(&self, entry: &AgentDirectoryEntry) -> Result<(), AgentDirectoryError> {
        let observed = entry.connector.describe();
        observed.validate()?;
        if observed != entry.descriptor {
            return Err(AgentDirectoryError::DescriptorChanged(
                entry.descriptor.connector_id.clone(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentDirectoryError {
    #[error("Agent connector is not registered: {0}")]
    ConnectorNotFound(AgentConnectorId),
    #[error("Agent connector is already registered: {0}")]
    RegistrationConflict(AgentConnectorId),
    #[error("Agent connector descriptor changed after registration: {0}")]
    DescriptorChanged(AgentConnectorId),
    #[error(transparent)]
    Connector(#[from] AgentConnectorError),
    #[error(transparent)]
    Protocol(#[from] orchestral_core::agent_protocol::wire::AgentProtocolError),
    #[error(transparent)]
    Agent(#[from] AgentSdkError),
}
