//! Codex integration through the supported `codex app-server` protocol.
//!
//! This crate intentionally depends only on Orchestral contracts. Codex wire
//! names and compatibility handling stay inside this concrete plugin.

mod normalize;
mod transport;

use std::sync::Arc;

use async_trait::async_trait;
use orchestral_core::agent_connector::{
    AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorErrorCode,
    AgentConnectorHealth, AgentConnectorId, AgentSessionCapabilities, AgentSessionDetail,
    AgentSessionListQuery, AgentSessionPage,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
use serde_json::{json, Value};
use tokio::sync::Mutex;

pub use transport::{CodexAppServerConfig, CodexTransportError};

use crate::normalize::NormalizationLimits;
use crate::transport::CodexRpcClient;

const CONNECTOR_ID: &str = "codex/local";
const PROVIDER_BINDING: &str = "codex/local";

struct ConnectedClient {
    rpc: Arc<CodexRpcClient>,
    user_agent: String,
}

/// Long-lived local Codex connector. The connector owns the app-server
/// process; UI subscribers never own Codex subscriptions directly.
pub struct CodexConnector {
    config: CodexAppServerConfig,
    client: Mutex<Option<Arc<ConnectedClient>>>,
    limits: NormalizationLimits,
}

impl CodexConnector {
    pub fn new(config: CodexAppServerConfig) -> Self {
        Self {
            config,
            client: Mutex::new(None),
            limits: NormalizationLimits::default(),
        }
    }

    async fn client(&self) -> Result<Arc<ConnectedClient>, AgentConnectorError> {
        let mut client = self.client.lock().await;
        if let Some(client) = client.as_ref() {
            return Ok(Arc::clone(client));
        }
        let (rpc, user_agent) = CodexRpcClient::spawn(&self.config)
            .await
            .map_err(connector_transport_error)?;
        let connected = Arc::new(ConnectedClient { rpc, user_agent });
        *client = Some(Arc::clone(&connected));
        Ok(connected)
    }

    #[cfg(test)]
    fn with_client(rpc: Arc<CodexRpcClient>, user_agent: impl Into<String>) -> Self {
        Self {
            config: CodexAppServerConfig::default(),
            client: Mutex::new(Some(Arc::new(ConnectedClient {
                rpc,
                user_agent: user_agent.into(),
            }))),
            limits: NormalizationLimits::default(),
        }
    }
}

impl Default for CodexConnector {
    fn default() -> Self {
        Self::new(CodexAppServerConfig::default())
    }
}

#[async_trait]
impl AgentConnector for CodexConnector {
    fn describe(&self) -> AgentConnectorDescriptor {
        AgentConnectorDescriptor {
            connector_id: AgentConnectorId::new(CONNECTOR_ID),
            provider_binding: ProviderBindingRef::new(PROVIDER_BINDING),
            agent_family: "coding-agent".to_owned(),
            display_name: "Codex".to_owned(),
            capabilities: AgentSessionCapabilities::discoverable(),
            actions: Vec::new(),
        }
    }

    async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
        let client = self.client().await?;
        Ok(AgentConnectorHealth::ready(Some(client.user_agent.clone())))
    }

    async fn list_sessions(
        &self,
        query: AgentSessionListQuery,
    ) -> Result<AgentSessionPage, AgentConnectorError> {
        query.validate()?;
        let client = self.client().await?;
        let mut params = json!({
            "limit": query.limit,
            "sortKey": "updated_at"
        });
        insert_optional(&mut params, "cursor", query.cursor.map(Value::String));
        insert_optional(&mut params, "cwd", query.cwd.map(Value::String));
        insert_optional(&mut params, "searchTerm", query.search.map(Value::String));
        let result = client
            .rpc
            .request("thread/list", params)
            .await
            .map_err(connector_transport_error)?;
        let page = normalize::session_page(&self.describe().connector_id, &result)?;
        page.validate_for(&self.describe().connector_id, query.limit)?;
        Ok(page)
    }

    async fn read_session(
        &self,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionDetail, AgentConnectorError> {
        if session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty"));
        }
        let client = self.client().await?;
        let result = client
            .rpc
            .request(
                "thread/read",
                json!({"threadId": session_id.as_str(), "includeTurns": true}),
            )
            .await
            .map_err(connector_transport_error)?;
        let detail =
            normalize::session_detail(&self.describe().connector_id, &result, &self.limits)?;
        detail.validate_for(&self.describe().connector_id)?;
        Ok(detail)
    }
}

fn insert_optional(object: &mut Value, key: &str, value: Option<Value>) {
    if let (Some(object), Some(value)) = (object.as_object_mut(), value) {
        object.insert(key.to_owned(), value);
    }
}

fn connector_transport_error(error: CodexTransportError) -> AgentConnectorError {
    let (code, retryable) = match error {
        CodexTransportError::Spawn(_) => (AgentConnectorErrorCode::Unavailable, false),
        CodexTransportError::Timeout | CodexTransportError::Closed | CodexTransportError::Io(_) => {
            (AgentConnectorErrorCode::Unavailable, true)
        }
        CodexTransportError::Rpc(_) => (AgentConnectorErrorCode::Protocol, false),
        CodexTransportError::InvalidJson(_)
        | CodexTransportError::FrameTooLarge { .. }
        | CodexTransportError::MissingResult => (AgentConnectorErrorCode::Protocol, false),
    };
    AgentConnectorError::new(code, error.to_string(), retryable)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use orchestral_core::agent_connector::AgentSessionState;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncWriteExt, BufReader};

    use super::*;

    #[tokio::test]
    async fn connector_lists_and_reads_sessions_over_jsonl() {
        let (client_io, server_io) = duplex(128 * 1024);
        let (client_read, client_write) = tokio::io::split(client_io);
        let (server_read, mut server_write) = tokio::io::split(server_io);
        let rpc = CodexRpcClient::from_io(
            client_read,
            client_write,
            Duration::from_secs(1),
            1024 * 1024,
        );
        let connector = CodexConnector::with_client(rpc, "codex/0.149.1");
        let server = tokio::spawn(async move {
            let mut requests = BufReader::new(server_read).lines();
            let list: Value =
                serde_json::from_str(&requests.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(list["method"], "thread/list");
            assert_eq!(list["params"]["searchTerm"], "compiler");
            server_write.write_all(format!("{}\n", json!({
                "id": list["id"],
                "result": {
                    "data": [{
                        "id": "thread-1", "preview": "compiler fix", "cwd": "/repo",
                        "createdAt": 10, "updatedAt": 20, "status": {"type": "idle"},
                        "modelProvider": "openai", "cliVersion": "0.149.1", "source": "cli"
                    }],
                    "nextCursor": "next-page",
                    "futureField": true
                }
            })).as_bytes()).await.unwrap();

            let read: Value =
                serde_json::from_str(&requests.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(read["method"], "thread/read");
            assert_eq!(read["params"]["includeTurns"], true);
            server_write.write_all(format!("{}\n", json!({
                "id": read["id"],
                "result": {"thread": {
                    "id": "thread-1", "preview": "compiler fix", "cwd": "/repo",
                    "createdAt": 10, "updatedAt": 20, "status": {"type": "idle"},
                    "turns": [{"id": "turn-1", "status": "completed", "items": [
                        {"type": "userMessage", "id": "u1", "content": [{"type": "text", "text": "fix"}]},
                        {"type": "agentMessage", "id": "a1", "text": "done"}
                    ]}]
                }}
            })).as_bytes()).await.unwrap();
        });

        assert_eq!(
            connector.health().await.unwrap().version.as_deref(),
            Some("codex/0.149.1")
        );
        let page = connector
            .list_sessions(AgentSessionListQuery {
                limit: 50,
                search: Some("compiler".to_owned()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.sessions.len(), 1);
        assert_eq!(page.sessions[0].state, AgentSessionState::Idle);
        assert_eq!(page.next_cursor.as_deref(), Some("next-page"));
        let detail = connector
            .read_session(&AgentSessionId::new("thread-1"))
            .await
            .unwrap();
        assert_eq!(detail.turns.len(), 1);
        assert_eq!(detail.turns[0].activities.len(), 2);
        server.await.unwrap();
    }
}
