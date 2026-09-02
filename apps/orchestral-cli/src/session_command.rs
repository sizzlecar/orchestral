use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context};
use clap::{Args, Subcommand};
use orchestral_core::agent_connector::{
    AgentConnectorId, AgentSessionActionId, AgentSessionActionStatus, AgentSessionListQuery,
    CreateAgentSessionRequest, InvokeAgentSessionActionRequest,
};
use orchestral_core::agent_protocol::wire::{
    AgentCommand, AgentCommandEnvelope, AgentSessionId, CommandId, Content, RunId,
};
use orchestral_runtime::AgentDirectory;
use serde::Serialize;
use serde_json::{json, Value};

use crate::agent_connectors::build_agent_directory;

#[derive(Debug, Args)]
pub(crate) struct SessionsCommand {
    #[command(subcommand)]
    command: SessionsSubcommand,
}

#[derive(Debug, Subcommand)]
enum SessionsSubcommand {
    /// List locally available Agent integrations.
    Agents(OutputArgs),
    /// List persisted sessions owned by an Agent.
    List(ListArgs),
    /// Read one persisted Agent session and its ordered activity history.
    Show(SessionArgs),
    /// Create a new native Agent session.
    Create(CreateArgs),
    /// Start a new turn in an existing native Agent session.
    Send(SendArgs),
    /// Guide a currently active turn without starting another turn.
    Steer(RunInputArgs),
    /// Stop a currently active turn.
    Cancel(CancelArgs),
    /// Reconcile a turn whose Provider connection was lost.
    Recover(RunArgs),
    /// Invoke a typed action declared by the selected Agent.
    Action(ActionArgs),
}

#[derive(Debug, Args)]
struct OutputArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ConnectorArgs {
    /// Connector ID. Optional when exactly one Agent integration is installed.
    #[arg(long)]
    connector: Option<String>,
}

#[derive(Debug, Args)]
struct ListArgs {
    #[command(flatten)]
    connector: ConnectorArgs,
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=200))]
    limit: u32,
    #[arg(long)]
    cursor: Option<String>,
    #[arg(long)]
    search: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct SessionArgs {
    session_id: String,
    #[command(flatten)]
    connector: ConnectorArgs,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct CreateArgs {
    #[command(flatten)]
    connector: ConnectorArgs,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct SendArgs {
    session_id: String,
    #[command(flatten)]
    connector: ConnectorArgs,
    /// Stable Run identity for idempotent automation.
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    json: bool,
    #[arg(value_name = "INPUT", required = true, num_args = 1..)]
    input: Vec<String>,
}

#[derive(Debug, Args)]
struct RunInputArgs {
    run_id: String,
    #[command(flatten)]
    connector: ConnectorArgs,
    #[arg(long)]
    json: bool,
    #[arg(value_name = "INPUT", required = true, num_args = 1..)]
    input: Vec<String>,
}

#[derive(Debug, Args)]
struct CancelArgs {
    run_id: String,
    #[command(flatten)]
    connector: ConnectorArgs,
    #[arg(long, default_value = "Cancelled from Orchestral CLI")]
    reason: String,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct RunArgs {
    run_id: String,
    #[command(flatten)]
    connector: ConnectorArgs,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ActionArgs {
    session_id: String,
    action_id: String,
    #[command(flatten)]
    connector: ConnectorArgs,
    /// JSON object matching the action's declared input schema.
    #[arg(long, default_value = "null")]
    arguments: String,
    /// Stable Run identity for idempotent Run actions.
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    json: bool,
}

impl SessionsCommand {
    pub(crate) async fn run(self, default_cwd: Option<PathBuf>) -> anyhow::Result<()> {
        let directory = build_agent_directory(None, None, None).await?;
        let stdout = std::io::stdout();
        self.run_with_directory(directory, default_cwd, &mut stdout.lock())
            .await
    }

    async fn run_with_directory(
        self,
        directory: Arc<AgentDirectory>,
        default_cwd: Option<PathBuf>,
        output: &mut dyn Write,
    ) -> anyhow::Result<()> {
        match self.command {
            SessionsSubcommand::Agents(args) => list_agents(&directory, output, args.json).await,
            SessionsSubcommand::List(args) => {
                let connector_id =
                    select_connector(&directory, args.connector.connector.as_deref()).await?;
                let page = directory
                    .list_sessions(
                        &connector_id,
                        AgentSessionListQuery {
                            cursor: args.cursor,
                            limit: args.limit,
                            cwd: default_cwd.map(path_text).transpose()?,
                            search: args.search,
                        },
                    )
                    .await?;
                if args.json {
                    write_json(output, &page)
                } else {
                    writeln!(output, "Agent\tSession\tState\tUpdated\tTitle\tWorkspace")?;
                    for session in page.sessions {
                        writeln!(
                            output,
                            "{}\t{}\t{}\t{}\t{}\t{}",
                            session.connector_id,
                            session.session_id,
                            json_name(&session.state)?,
                            session
                                .updated_at_unix_ms
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".to_owned()),
                            session.title.as_deref().unwrap_or("-"),
                            session.cwd.as_deref().unwrap_or("-"),
                        )?;
                    }
                    if let Some(cursor) = page.next_cursor {
                        writeln!(output, "Next cursor: {cursor}")?;
                    }
                    Ok(())
                }
            }
            SessionsSubcommand::Show(args) => {
                let connector_id =
                    select_connector(&directory, args.connector.connector.as_deref()).await?;
                let detail = directory
                    .read_session(&connector_id, &AgentSessionId::new(args.session_id))
                    .await?;
                if args.json {
                    write_json(output, &detail)
                } else {
                    writeln!(
                        output,
                        "{} · {} · {}",
                        detail.summary.connector_id,
                        detail.summary.session_id,
                        detail.summary.title.as_deref().unwrap_or("Untitled")
                    )?;
                    if let Some(cwd) = &detail.summary.cwd {
                        writeln!(output, "Workspace: {cwd}")?;
                    }
                    for turn in detail.turns {
                        writeln!(
                            output,
                            "\nTurn {} [{}]",
                            turn.turn_id,
                            json_name(&turn.status)?
                        )?;
                        for activity in turn.activities {
                            let label = activity
                                .title
                                .unwrap_or_else(|| json_name(&activity.kind).unwrap_or_default());
                            writeln!(output, "- {label} [{}]", json_name(&activity.status)?)?;
                            for content in activity.content {
                                if let Some(text) = inline_text(&content)? {
                                    writeln!(output, "  {text}")?;
                                }
                            }
                        }
                    }
                    if !detail.pending_requests.is_empty() {
                        writeln!(output, "\nPending requests:")?;
                        for request in detail.pending_requests {
                            writeln!(
                                output,
                                "- {} ({})",
                                request.request_id,
                                json_name(&request.kind())?
                            )?;
                        }
                    }
                    Ok(())
                }
            }
            SessionsSubcommand::Create(args) => {
                let connector_id =
                    select_connector(&directory, args.connector.connector.as_deref()).await?;
                let summary = directory
                    .create_session(
                        &connector_id,
                        CreateAgentSessionRequest {
                            cwd: default_cwd.map(path_text).transpose()?,
                            title: args.title,
                            options: serde_json::Value::Null,
                            extensions: Default::default(),
                        },
                    )
                    .await?;
                if args.json {
                    write_json(output, &summary)
                } else {
                    writeln!(
                        output,
                        "Created {}:{}",
                        summary.connector_id, summary.session_id
                    )?;
                    Ok(())
                }
            }
            SessionsSubcommand::Send(args) => {
                let connector_id =
                    select_connector(&directory, args.connector.connector.as_deref()).await?;
                let handle = directory
                    .start_text(
                        &connector_id,
                        &AgentSessionId::new(args.session_id),
                        args.run_id.map(RunId::new),
                        joined_input(args.input)?,
                    )
                    .await?;
                let turn = handle.wait_until_blocked().await?;
                if args.json {
                    write_json(output, &json!({"run_id": turn.run_id, "view": turn.view}))
                } else {
                    if let Some(text) = turn.final_text() {
                        writeln!(output, "{text}")?;
                    }
                    writeln!(
                        output,
                        "Run {}: {}",
                        turn.run_id,
                        json_name(&turn.status())?
                    )?;
                    if turn.is_waiting() {
                        writeln!(
                            output,
                            "Pending requests: {}",
                            turn.view.pending_requests.len()
                        )?;
                    }
                    Ok(())
                }
            }
            SessionsSubcommand::Steer(args) => {
                let (api, run_id) = run_target(&directory, args.connector, args.run_id).await?;
                let command = AgentCommandEnvelope::new(
                    command_id("steer"),
                    run_id,
                    None,
                    AgentCommand::Steer {
                        content: vec![Content::text(joined_input(args.input)?)],
                    },
                )?;
                let ack = api.command(command).await?;
                print_ack(output, &ack, args.json)
            }
            SessionsSubcommand::Cancel(args) => {
                let (api, run_id) = run_target(&directory, args.connector, args.run_id).await?;
                let ack = api.cancel(&run_id, args.reason).await?;
                print_ack(output, &ack, args.json)
            }
            SessionsSubcommand::Recover(args) => {
                let (api, run_id) = run_target(&directory, args.connector, args.run_id).await?;
                let view = api.recover(&run_id).await?;
                if args.json {
                    write_json(output, &view)
                } else {
                    writeln!(output, "Run {run_id}: {}", json_name(&view.state.status())?)?;
                    Ok(())
                }
            }
            SessionsSubcommand::Action(args) => {
                let connector_id =
                    select_connector(&directory, args.connector.connector.as_deref()).await?;
                let arguments = serde_json::from_str::<Value>(&args.arguments)
                    .context("--arguments must be valid JSON")?;
                let outcome = directory
                    .invoke_action(
                        &connector_id,
                        InvokeAgentSessionActionRequest {
                            session_id: AgentSessionId::new(args.session_id),
                            action_id: AgentSessionActionId::new(args.action_id),
                            arguments,
                            run_id: args.run_id.map(RunId::new),
                        },
                    )
                    .await?;
                if args.json {
                    write_json(output, &outcome)
                } else {
                    if let AgentSessionActionStatus::Running { run_id } = &outcome.status {
                        writeln!(output, "Run: {run_id}")?;
                    }
                    if let Some(session) = outcome.session {
                        writeln!(output, "Session: {}", session.session_id)?;
                    }
                    for content in outcome.content {
                        if let Some(text) = inline_text(&content)? {
                            writeln!(output, "{text}")?;
                        }
                    }
                    Ok(())
                }
            }
        }
    }
}

async fn list_agents(
    directory: &AgentDirectory,
    output: &mut dyn Write,
    json: bool,
) -> anyhow::Result<()> {
    let descriptors = directory.connectors().await;
    if json {
        return write_json(output, &descriptors);
    }
    writeln!(output, "ID\tAgent\tHealth\tVersion\tActions")?;
    for descriptor in descriptors {
        let health = directory.health(&descriptor.connector_id).await?;
        writeln!(
            output,
            "{}\t{}\t{}\t{}\t{}",
            descriptor.connector_id,
            descriptor.display_name,
            json_name(&health.status)?,
            health.version.as_deref().unwrap_or("-"),
            descriptor
                .actions
                .iter()
                .map(|action| action.action_id.as_str())
                .collect::<Vec<_>>()
                .join(","),
        )?;
    }
    Ok(())
}

async fn select_connector(
    directory: &AgentDirectory,
    requested: Option<&str>,
) -> anyhow::Result<AgentConnectorId> {
    if let Some(requested) = requested {
        if requested.trim().is_empty() {
            bail!("--connector must not be empty");
        }
        return Ok(AgentConnectorId::new(requested));
    }
    let descriptors = directory.connectors().await;
    match descriptors.as_slice() {
        [descriptor] => Ok(descriptor.connector_id.clone()),
        [] => bail!("No Agent integrations are installed"),
        _ => bail!(
            "Multiple Agent integrations are installed; select one with --connector ({})",
            descriptors
                .iter()
                .map(|descriptor| descriptor.connector_id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

async fn run_target(
    directory: &AgentDirectory,
    connector: ConnectorArgs,
    run_id: String,
) -> anyhow::Result<(orchestral_runtime::api::AgentApi, RunId)> {
    let connector_id = select_connector(directory, connector.connector.as_deref()).await?;
    Ok((
        directory.agent_api(&connector_id).await?,
        RunId::new(run_id),
    ))
}

fn command_id(kind: &str) -> CommandId {
    CommandId::new(format!("cli-{kind}-{}", uuid::Uuid::new_v4()))
}

fn joined_input(input: Vec<String>) -> anyhow::Result<String> {
    let input = input.join(" ");
    if input.trim().is_empty() {
        bail!("input must not be empty");
    }
    Ok(input)
}

fn path_text(path: PathBuf) -> anyhow::Result<String> {
    path.into_os_string()
        .into_string()
        .map_err(|_| anyhow::anyhow!("workspace path must be valid UTF-8"))
}

fn json_name(value: &impl Serialize) -> anyhow::Result<String> {
    Ok(serde_json::to_value(value)?
        .as_str()
        .unwrap_or("unknown")
        .to_owned())
}

fn inline_text(content: &Content) -> anyhow::Result<Option<String>> {
    let value = serde_json::to_value(content)?;
    Ok(value
        .get("body")
        .and_then(|body| body.get("value"))
        .and_then(Value::as_str)
        .map(str::to_owned))
}

fn write_json(output: &mut dyn Write, value: &impl Serialize) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(&mut *output, value)?;
    writeln!(output)?;
    Ok(())
}

fn print_ack(
    output: &mut dyn Write,
    ack: &orchestral_core::agent_protocol::wire::CommandAck,
    json: bool,
) -> anyhow::Result<()> {
    if json {
        write_json(output, ack)
    } else {
        writeln!(
            output,
            "Command {} for Run {}: {}",
            ack.command_id,
            ack.run_id,
            json_name(&ack.state)?
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use clap::Parser;
    use orchestral_agent_protocol_testkit::{
        ProviderFixtureFactory, ProviderScenario, SessionfulRecoverFactory, TestProbes,
    };
    use orchestral_core::agent_connector::{
        AgentConnector, AgentConnectorDescriptor, AgentConnectorError, AgentConnectorHealth,
        AgentSessionActionDescriptor, AgentSessionActionExecution, AgentSessionActionOutcome,
        AgentSessionActionStatus, AgentSessionCapabilities, AgentSessionDetail, AgentSessionPage,
        AgentSessionState, AgentSessionSummary, SESSION_RENAME_ACTION, SESSION_REVIEW_ACTION,
    };
    use orchestral_core::agent_protocol::wire::ProviderBindingRef;

    use super::*;

    struct FixtureConnector;

    #[async_trait]
    impl AgentConnector for FixtureConnector {
        fn describe(&self) -> AgentConnectorDescriptor {
            AgentConnectorDescriptor {
                connector_id: AgentConnectorId::new("fixture/local"),
                provider_binding: ProviderBindingRef::new("fixture/provider"),
                agent_family: "coding-agent".to_owned(),
                display_name: "Fixture Agent".to_owned(),
                capabilities: AgentSessionCapabilities {
                    create: true,
                    ..AgentSessionCapabilities::discoverable()
                },
                creation: None,
                actions: vec![
                    AgentSessionActionDescriptor {
                        action_id: AgentSessionActionId::new(SESSION_RENAME_ACTION),
                        title: "Rename".to_owned(),
                        description: "Rename session".to_owned(),
                        input_schema: Some(json!({"type": "object"})),
                        execution: AgentSessionActionExecution::Immediate,
                    },
                    AgentSessionActionDescriptor {
                        action_id: AgentSessionActionId::new(SESSION_REVIEW_ACTION),
                        title: "Review".to_owned(),
                        description: "Review changes".to_owned(),
                        input_schema: Some(json!({"type": "object"})),
                        execution: AgentSessionActionExecution::Run,
                    },
                ],
            }
        }

        async fn health(&self) -> Result<AgentConnectorHealth, AgentConnectorError> {
            Ok(AgentConnectorHealth::ready(Some("fixture-1".to_owned())))
        }

        async fn list_sessions(
            &self,
            _query: AgentSessionListQuery,
        ) -> Result<AgentSessionPage, AgentConnectorError> {
            Ok(AgentSessionPage {
                sessions: vec![summary("fixture-session")],
                next_cursor: None,
            })
        }

        async fn read_session(
            &self,
            session_id: &AgentSessionId,
        ) -> Result<AgentSessionDetail, AgentConnectorError> {
            Ok(AgentSessionDetail {
                summary: summary(session_id.as_str()),
                turns: Vec::new(),
                pending_requests: Vec::new(),
                next_cursor: None,
            })
        }

        async fn create_session(
            &self,
            _request: CreateAgentSessionRequest,
        ) -> Result<AgentSessionSummary, AgentConnectorError> {
            Ok(summary("created-session"))
        }

        async fn invoke_action(
            &self,
            request: InvokeAgentSessionActionRequest,
        ) -> Result<AgentSessionActionOutcome, AgentConnectorError> {
            Ok(AgentSessionActionOutcome {
                status: AgentSessionActionStatus::Completed,
                session: Some(summary(request.session_id.as_str())),
                content: vec![Content::text(request.action_id.as_str())],
                details: Value::Null,
            })
        }
    }

    fn summary(session_id: &str) -> AgentSessionSummary {
        AgentSessionSummary {
            connector_id: AgentConnectorId::new("fixture/local"),
            session_id: AgentSessionId::new(session_id),
            title: Some("Fixture session".to_owned()),
            preview: Some("hello".to_owned()),
            cwd: Some("/fixture".to_owned()),
            created_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(2),
            state: AgentSessionState::Idle,
            extensions: BTreeMap::new(),
        }
    }

    async fn directory() -> Arc<AgentDirectory> {
        let directory = Arc::new(AgentDirectory::new());
        let factory = SessionfulRecoverFactory::new().unwrap();
        let scenario = ProviderScenario::standard(&factory.descriptor()).unwrap();
        directory
            .register(
                Arc::new(FixtureConnector),
                factory.create(scenario, TestProbes::default()),
            )
            .await
            .unwrap();
        directory
    }

    #[test]
    fn sessions_is_a_management_entrypoint_without_replacing_root_agent() {
        crate::cli::Cli::try_parse_from([
            "orchestral",
            "sessions",
            "send",
            "thread-1",
            "continue the work",
        ])
        .expect("session turn command parses");
    }

    #[tokio::test]
    async fn provider_neutral_cli_lists_reads_runs_and_invokes_declared_actions() {
        let directory = directory().await;

        let mut output = Vec::new();
        SessionsCommand {
            command: SessionsSubcommand::List(ListArgs {
                connector: ConnectorArgs { connector: None },
                limit: 50,
                cursor: None,
                search: None,
                json: true,
            }),
        }
        .run_with_directory(directory.clone(), None, &mut output)
        .await
        .unwrap();
        let page: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(page["sessions"][0]["session_id"], "fixture-session");

        output.clear();
        SessionsCommand {
            command: SessionsSubcommand::Send(SendArgs {
                session_id: "fixture-session".to_owned(),
                connector: ConnectorArgs { connector: None },
                run_id: Some("cli-run".to_owned()),
                json: true,
                input: vec!["continue".to_owned()],
            }),
        }
        .run_with_directory(directory.clone(), None, &mut output)
        .await
        .unwrap();
        let turn: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(turn["run_id"], "cli-run");
        assert_eq!(turn["view"]["state"]["state"], "terminal");

        output.clear();
        SessionsCommand {
            command: SessionsSubcommand::Action(ActionArgs {
                session_id: "fixture-session".to_owned(),
                action_id: SESSION_RENAME_ACTION.to_owned(),
                connector: ConnectorArgs { connector: None },
                arguments: r#"{"name":"renamed"}"#.to_owned(),
                run_id: None,
                json: true,
            }),
        }
        .run_with_directory(directory.clone(), None, &mut output)
        .await
        .unwrap();
        let action: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(action["content"][0]["body"]["value"], SESSION_RENAME_ACTION);

        output.clear();
        SessionsCommand {
            command: SessionsSubcommand::Action(ActionArgs {
                session_id: "fixture-session".to_owned(),
                action_id: SESSION_REVIEW_ACTION.to_owned(),
                connector: ConnectorArgs { connector: None },
                arguments: r#"{"target":"uncommitted_changes"}"#.to_owned(),
                run_id: Some("cli-review-run".to_owned()),
                json: true,
            }),
        }
        .run_with_directory(directory, None, &mut output)
        .await
        .unwrap();
        let action: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(action["status"]["state"], "running");
        assert_eq!(action["status"]["run_id"], "cli-review-run");
    }
}
