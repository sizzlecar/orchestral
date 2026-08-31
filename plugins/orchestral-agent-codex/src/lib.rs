//! Codex integration through the supported `codex app-server` protocol.
//!
//! This crate intentionally depends only on Orchestral contracts. Codex wire
//! names and compatibility handling stay inside this concrete plugin.

mod normalize;
mod provider;
mod transport;

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use orchestral_core::agent_connector::{
    AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorErrorCode,
    AgentConnectorHealth, AgentConnectorId, AgentSessionActionDescriptor, AgentSessionActionId,
    AgentSessionActionOutcome, AgentSessionCapabilities, AgentSessionDetail, AgentSessionListQuery,
    AgentSessionPage, AgentSessionSummary, CreateAgentSessionRequest,
    InvokeAgentSessionActionRequest, SESSION_FORK_ACTION, SESSION_RENAME_ACTION,
};
use orchestral_core::agent_protocol::wire::{AgentSessionId, ProviderBindingRef};
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

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
    client: AsyncMutex<Option<Arc<ConnectedClient>>>,
    limits: NormalizationLimits,
    provider_state: StdMutex<provider::ProviderState>,
}

impl CodexConnector {
    pub fn new(config: CodexAppServerConfig) -> Self {
        Self {
            config,
            client: AsyncMutex::new(None),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
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

    async fn read_summary(
        &self,
        client: &ConnectedClient,
        session_id: &AgentSessionId,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        let result = client
            .rpc
            .request(
                "thread/read",
                json!({"threadId": session_id.as_str(), "includeTurns": false}),
            )
            .await
            .map_err(connector_transport_error)?;
        let thread = result
            .get("thread")
            .ok_or_else(|| AgentConnectorError::protocol("thread/read omitted thread"))?;
        normalize::session_summary(&self.describe().connector_id, thread)
    }

    #[cfg(test)]
    fn with_client(rpc: Arc<CodexRpcClient>, user_agent: impl Into<String>) -> Self {
        Self {
            config: CodexAppServerConfig::default(),
            client: AsyncMutex::new(Some(Arc::new(ConnectedClient {
                rpc,
                user_agent: user_agent.into(),
            }))),
            limits: NormalizationLimits::default(),
            provider_state: StdMutex::new(provider::ProviderState::default()),
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
            capabilities: AgentSessionCapabilities {
                create: true,
                ..AgentSessionCapabilities::discoverable()
            },
            actions: vec![
                AgentSessionActionDescriptor {
                    action_id: AgentSessionActionId::new(SESSION_FORK_ACTION),
                    title: "Fork session".to_owned(),
                    description: "Create a new session from this session's persisted history"
                        .to_owned(),
                    input_schema: None,
                },
                AgentSessionActionDescriptor {
                    action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                    title: "Rename session".to_owned(),
                    description: "Set the session's display name".to_owned(),
                    input_schema: Some(json!({
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["name"],
                        "properties": {"name": {"type": "string", "minLength": 1}}
                    })),
                },
            ],
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

    async fn create_session(
        &self,
        request: CreateAgentSessionRequest,
    ) -> Result<AgentSessionSummary, AgentConnectorError> {
        let cwd = non_empty_optional(request.cwd, "cwd")?;
        let title = non_empty_optional(request.title, "title")?;
        if !request.extensions.is_empty() {
            return Err(AgentConnectorError::invalid(
                "Codex session creation does not accept connector extensions",
            ));
        }
        let client = self.client().await?;
        let mut params = json!({
            "experimentalRawEvents": false,
            "persistExtendedHistory": true
        });
        insert_optional(&mut params, "cwd", cwd.map(Value::String));
        let result = client
            .rpc
            .request("thread/start", params)
            .await
            .map_err(connector_transport_error)?;
        let thread = result
            .get("thread")
            .ok_or_else(|| AgentConnectorError::protocol("thread/start omitted thread"))?;
        let mut summary = normalize::session_summary(&self.describe().connector_id, thread)?;
        if let Some(title) = title {
            client
                .rpc
                .request(
                    "thread/name/set",
                    json!({"threadId": summary.session_id.as_str(), "name": title}),
                )
                .await
                .map_err(|error| {
                    connector_transport_error(error).with_details(json!({
                        "createdSessionId": summary.session_id.as_str()
                    }))
                })?;
            summary.title = Some(title);
        }
        summary.validate_for(&self.describe().connector_id)?;
        Ok(summary)
    }

    async fn invoke_action(
        &self,
        request: InvokeAgentSessionActionRequest,
    ) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
        if request.session_id.is_empty() {
            return Err(AgentConnectorError::invalid("session id must not be empty"));
        }
        let session = match request.action_id.as_str() {
            SESSION_FORK_ACTION => {
                if !request.arguments.is_null() {
                    return Err(AgentConnectorError::invalid(
                        "session.fork takes no arguments",
                    ));
                }
                let client = self.client().await?;
                let result = client
                    .rpc
                    .request(
                        "thread/fork",
                        json!({
                            "threadId": request.session_id.as_str(),
                            "persistExtendedHistory": true
                        }),
                    )
                    .await
                    .map_err(connector_transport_error)?;
                let thread = result
                    .get("thread")
                    .ok_or_else(|| AgentConnectorError::protocol("thread/fork omitted thread"))?;
                Some(normalize::session_summary(
                    &self.describe().connector_id,
                    thread,
                )?)
            }
            SESSION_RENAME_ACTION => {
                let name = required_action_string(&request.arguments, "name")?;
                let client = self.client().await?;
                client
                    .rpc
                    .request(
                        "thread/name/set",
                        json!({"threadId": request.session_id.as_str(), "name": name}),
                    )
                    .await
                    .map_err(connector_transport_error)?;
                Some(self.read_summary(&client, &request.session_id).await?)
            }
            _ => {
                return Err(AgentConnectorError::unsupported(format!(
                    "Codex does not declare action {}",
                    request.action_id
                )))
            }
        };
        Ok(AgentSessionActionOutcome {
            session,
            content: Vec::new(),
            details: Value::Null,
        })
    }
}

fn non_empty_optional(
    value: Option<String>,
    field: &str,
) -> Result<Option<String>, AgentConnectorError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(AgentConnectorError::invalid(format!(
            "{field} must not be empty"
        ))),
        value => Ok(value),
    }
}

fn required_action_string(arguments: &Value, field: &str) -> Result<String, AgentConnectorError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| AgentConnectorError::invalid("action arguments must be an object"))?;
    if object.len() != 1 {
        return Err(AgentConnectorError::invalid(
            "rename action accepts only the name argument",
        ));
    }
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AgentConnectorError::invalid("rename action requires a non-empty name"))
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
    use std::collections::BTreeMap;
    use std::time::Duration;

    use orchestral_core::agent_connector::{
        AgentSessionState, CreateAgentSessionRequest, InvokeAgentSessionActionRequest,
    };
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

    #[tokio::test]
    async fn connector_creates_forks_and_renames_sessions_with_declared_rpc_methods() {
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

            let start = read_request(&mut requests).await;
            assert_eq!(start["method"], "thread/start");
            assert_eq!(start["params"]["cwd"], "/repo");
            assert_eq!(start["params"]["experimentalRawEvents"], false);
            assert_eq!(start["params"]["persistExtendedHistory"], true);
            write_result(
                &mut server_write,
                &start,
                json!({"thread": thread("thread-new", Value::Null)}),
            )
            .await;

            let initial_name = read_request(&mut requests).await;
            assert_eq!(initial_name["method"], "thread/name/set");
            assert_eq!(initial_name["params"]["threadId"], "thread-new");
            assert_eq!(initial_name["params"]["name"], "Compiler work");
            write_result(&mut server_write, &initial_name, json!({})).await;

            let fork = read_request(&mut requests).await;
            assert_eq!(fork["method"], "thread/fork");
            assert_eq!(fork["params"]["threadId"], "thread-new");
            assert_eq!(fork["params"]["persistExtendedHistory"], true);
            write_result(
                &mut server_write,
                &fork,
                json!({"thread": thread("thread-fork", "Compiler work fork")}),
            )
            .await;

            let rename = read_request(&mut requests).await;
            assert_eq!(rename["method"], "thread/name/set");
            assert_eq!(rename["params"]["threadId"], "thread-fork");
            assert_eq!(rename["params"]["name"], "Release review");
            write_result(&mut server_write, &rename, json!({})).await;

            let read = read_request(&mut requests).await;
            assert_eq!(read["method"], "thread/read");
            assert_eq!(read["params"]["includeTurns"], false);
            write_result(
                &mut server_write,
                &read,
                json!({"thread": thread("thread-fork", "Release review")}),
            )
            .await;
        });

        let descriptor = connector.describe();
        assert!(descriptor.capabilities.create);
        assert!(descriptor
            .action(&AgentSessionActionId::new(SESSION_FORK_ACTION))
            .is_some());
        assert!(descriptor
            .action(&AgentSessionActionId::new(SESSION_RENAME_ACTION))
            .is_some());

        let created = connector
            .create_session(CreateAgentSessionRequest {
                cwd: Some("/repo".to_owned()),
                title: Some("Compiler work".to_owned()),
                extensions: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(created.session_id.as_str(), "thread-new");
        assert_eq!(created.title.as_deref(), Some("Compiler work"));

        let forked = connector
            .invoke_action(InvokeAgentSessionActionRequest {
                session_id: created.session_id,
                action_id: AgentSessionActionId::new(SESSION_FORK_ACTION),
                arguments: Value::Null,
            })
            .await
            .unwrap()
            .session
            .unwrap();
        assert_eq!(forked.session_id.as_str(), "thread-fork");

        let renamed = connector
            .invoke_action(InvokeAgentSessionActionRequest {
                session_id: forked.session_id,
                action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                arguments: json!({"name": "Release review"}),
            })
            .await
            .unwrap()
            .session
            .unwrap();
        assert_eq!(renamed.title.as_deref(), Some("Release review"));
        server.await.unwrap();
    }

    #[test]
    fn connector_rejects_invalid_create_and_action_arguments_before_side_effects() {
        let connector = CodexConnector::default();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let create_error = runtime
            .block_on(connector.create_session(CreateAgentSessionRequest {
                cwd: Some("  ".to_owned()),
                title: None,
                extensions: BTreeMap::new(),
            }))
            .unwrap_err();
        assert_eq!(create_error.code, AgentConnectorErrorCode::InvalidRequest);

        let rename_error = runtime
            .block_on(connector.invoke_action(InvokeAgentSessionActionRequest {
                session_id: AgentSessionId::new("thread-1"),
                action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                arguments: json!({"name": "", "unexpected": true}),
            }))
            .unwrap_err();
        assert_eq!(rename_error.code, AgentConnectorErrorCode::InvalidRequest);
    }

    fn thread(id: &str, name: impl Into<Value>) -> Value {
        json!({
            "id": id,
            "name": name.into(),
            "preview": "session preview",
            "cwd": "/repo",
            "createdAt": 10,
            "updatedAt": 20,
            "status": {"type": "idle"}
        })
    }

    async fn read_request<R>(requests: &mut tokio::io::Lines<BufReader<R>>) -> Value
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        serde_json::from_str(&requests.next_line().await.unwrap().unwrap()).unwrap()
    }

    async fn write_result<W>(writer: &mut W, request: &Value, result: Value)
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        writer
            .write_all(format!("{}\n", json!({"id": request["id"], "result": result})).as_bytes())
            .await
            .unwrap();
    }
}
