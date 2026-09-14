use std::env;
use std::io::{self, IsTerminal};
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::envfile::load_env_file;
use crate::runtime::ModelOverrides;

#[derive(Debug, Parser)]
#[command(
    name = "orchestral",
    about = "A coding agent for your terminal and workspace",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,
    /// Read configuration from this YAML file.
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Load environment variables from a file before reading configuration.
    #[arg(long, global = true)]
    env_file: Option<PathBuf>,
    /// Provider credential document. For Google, this accepts a service-account JSON key.
    #[arg(long, value_name = "PATH", global = true)]
    credential_file: Option<PathBuf>,
    /// Select a model provider defined in your configuration.
    #[arg(long, global = true)]
    backend: Option<String>,
    /// Select a named model profile from your configuration.
    #[arg(long, global = true)]
    model_profile: Option<String>,
    /// Use this model ID (or choose from the server's model list).
    #[arg(long, global = true)]
    model: Option<String>,
    /// OpenAI-compatible URL. Also accepts OPENAI_BASE_URL; custom URLs default to no auth.
    #[arg(long, value_name = "URL", global = true)]
    base_url: Option<String>,
    /// Explicit credential environment variable for an OpenAI-compatible server.
    #[arg(long, value_name = "NAME", global = true, conflicts_with = "no_auth")]
    api_key_env: Option<String>,
    /// Omit authentication for the selected OpenAI-compatible server.
    #[arg(long, global = true)]
    no_auth: bool,
    /// Override the model's sampling temperature.
    #[arg(long, global = true)]
    temperature: Option<f32>,
    /// Continue the conversation with this session ID
    #[arg(long, global = true)]
    session_id: Option<String>,
    /// Add instructions to the agent for this session.
    #[arg(long, global = true)]
    system_prompt: Option<String>,
    /// Follow-up input for local runs. Serve uses its remote input channel.
    #[arg(long, value_enum, default_value = "auto", global = true)]
    input_mode: crate::agent::InputMode,
    /// Start without MCP servers.
    #[arg(long, global = true)]
    no_mcp: bool,
    /// Explicit local MCP manifest (`.mcp.json`). May be repeated.
    #[arg(long, value_name = "PATH", global = true)]
    mcp_config: Vec<PathBuf>,
    /// Start without loading skills.
    #[arg(long, global = true)]
    no_skills: bool,
    /// Show additional diagnostic logging on stderr.
    #[arg(long, global = true)]
    verbose: bool,
    /// Use DIR as the primary Agent workspace instead of the process directory.
    #[arg(short = 'C', long = "cwd", value_name = "DIR", global = true)]
    cwd: Option<PathBuf>,
    /// Add another read-write workspace root. May be repeated.
    #[arg(long = "add-dir", value_name = "DIR", global = true)]
    add_dirs: Vec<PathBuf>,
    /// A single turn. When omitted, starts an interactive conversation.
    #[arg(value_name = "INPUT")]
    input: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Check installation and model configuration without starting a task.
    Doctor(crate::doctor::DoctorCommand),
    /// Manage MCP servers registered for this user.
    Mcp(crate::mcp_command::McpCommand),
    /// List, enable, or disable Skills for the current workspace.
    Skills(crate::skill_command::SkillsCommand),
    /// Discover and control sessions owned by installed Agents.
    Sessions(crate::session_command::SessionsCommand),
    /// Reopen a built-in Agent conversation, optionally with a follow-up prompt.
    Resume(crate::local_sessions::ResumeCommand),
    /// Start the web interface for browser and phone access.
    Serve(crate::remote::ServeCommand),
}

impl Cli {
    fn model_overrides(&self) -> ModelOverrides {
        let base_url = self.base_url.clone().or_else(|| {
            (self.backend.is_none() && self.model_profile.is_none())
                .then(|| {
                    env::var("OPENAI_BASE_URL")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                })
                .flatten()
        });
        let model = self.model.clone().or_else(|| {
            base_url.as_ref().and_then(|_| {
                env::var("OPENAI_MODEL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })
        });
        ModelOverrides {
            backend: self.backend.clone(),
            model_profile: self.model_profile.clone(),
            model,
            temperature: self.temperature,
            base_url,
            api_key_env: self.api_key_env.clone(),
            no_auth: self.no_auth,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        if let Some(env_file) = &self.env_file {
            load_env_file(env_file)?;
        }
        ensure_log_filter(self.verbose);
        install_tracing_subscriber();
        let model_overrides = self.model_overrides();
        let options = crate::agent::AgentRunOptions {
            config: self.config,
            credential_file: self.credential_file,
            model_overrides,
            session_id: self.session_id,
            system_prompt: self.system_prompt,
            input: (!self.input.is_empty()).then(|| self.input.join(" ")),
            input_mode: self.input_mode,
            no_mcp: self.no_mcp,
            mcp_config: self.mcp_config,
            no_skills: self.no_skills,
            cwd: self.cwd,
            add_dirs: self.add_dirs,
        };
        match self.command {
            Some(CliCommand::Doctor(command)) => command.run(options).await,
            Some(CliCommand::Mcp(command)) => command.run().await,
            Some(CliCommand::Skills(command)) => command.run(options.config, options.cwd),
            Some(CliCommand::Sessions(command)) => command.run(options.config, options.cwd).await,
            Some(CliCommand::Resume(command)) => command.run(options).await,
            Some(CliCommand::Serve(command)) => crate::remote::serve(command, options).await,
            None => crate::agent::run(options).await,
        }
    }
}

fn ensure_log_filter(verbose: bool) {
    if !verbose && env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
}

fn install_tracing_subscriber() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|error| {
        eprintln!("Invalid RUST_LOG filter ({error}); falling back to info");
        EnvFilter::new("info")
    });
    // Embedders and tests may already own the global subscriber. In that case
    // retaining their subscriber is safer than failing the command at startup.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_ansi(io::stderr().is_terminal())
        .with_target(true)
        .compact()
        .try_init();
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::{CommandFactory, Parser};

    use super::Cli;

    #[test]
    fn input_mode_is_typed_and_defaults_to_auto() {
        assert_eq!(
            Cli::try_parse_from(["orchestral", "inspect"])
                .unwrap()
                .input_mode,
            crate::agent::InputMode::Auto
        );
        assert_eq!(
            Cli::try_parse_from(["orchestral", "--input-mode", "interactive", "inspect"])
                .unwrap()
                .input_mode,
            crate::agent::InputMode::Interactive
        );
        assert_eq!(
            Cli::try_parse_from(["orchestral", "--input-mode", "none", "inspect"])
                .unwrap()
                .input_mode,
            crate::agent::InputMode::None
        );
        assert!(Cli::try_parse_from(["orchestral", "--input-mode", "maybe"]).is_err());
    }

    #[test]
    fn root_command_is_the_agent_entrypoint() {
        let parsed = Cli::try_parse_from(["orchestral", "inspect this repository"])
            .expect("a positional prompt must start the Agent directly");
        assert_eq!(parsed.input, ["inspect this repository"]);
        assert!(parsed.command.is_none());
        assert_eq!(
            Cli::command()
                .get_subcommands()
                .map(clap::Command::get_name)
                .collect::<Vec<_>>(),
            ["doctor", "mcp", "skills", "sessions", "resume", "serve"]
        );
    }

    #[test]
    fn mcp_management_does_not_add_an_agent_subcommand() {
        let parsed = Cli::try_parse_from([
            "orchestral",
            "mcp",
            "add",
            "local",
            "--",
            "/bin/example-mcp",
        ])
        .expect("MCP registration must be a management subcommand");
        assert!(matches!(parsed.command, Some(super::CliCommand::Mcp(_))));
        assert!(parsed.input.is_empty());
    }

    #[test]
    fn skill_management_is_a_first_class_root_command() {
        let parsed = Cli::try_parse_from(["orchestral", "skills", "disable", "xlsx"])
            .expect("Skill management command must parse");
        assert!(matches!(parsed.command, Some(super::CliCommand::Skills(_))));
        assert!(parsed.input.is_empty());
    }

    #[test]
    fn local_mcp_manifests_are_explicit_repeatable_host_inputs() {
        let parsed = Cli::try_parse_from([
            "orchestral",
            "--mcp-config",
            "project.mcp.json",
            "--mcp-config",
            "/host/user.mcp.json",
            "inspect tools",
        ])
        .unwrap();
        assert_eq!(
            parsed.mcp_config,
            [
                PathBuf::from("project.mcp.json"),
                PathBuf::from("/host/user.mcp.json"),
            ]
        );
    }

    #[test]
    fn primary_and_additional_workspaces_are_explicit_repeatable_inputs() {
        let parsed = Cli::try_parse_from([
            "orchestral",
            "-C",
            "/work/primary",
            "--add-dir",
            "/work/shared",
            "--add-dir",
            "/work/other",
            "inspect both projects",
        ])
        .unwrap();
        assert_eq!(parsed.cwd, Some(PathBuf::from("/work/primary")));
        assert_eq!(
            parsed.add_dirs,
            [PathBuf::from("/work/shared"), PathBuf::from("/work/other")]
        );
    }

    #[test]
    fn serve_accepts_model_and_workspace_options_after_the_subcommand() {
        let parsed = Cli::try_parse_from([
            "orchestral",
            "serve",
            "--pair",
            "--model",
            "gemini-3.1-pro-preview",
            "-C",
            "/work/primary",
        ])
        .unwrap();
        assert_eq!(parsed.model.as_deref(), Some("gemini-3.1-pro-preview"));
        assert_eq!(parsed.cwd, Some(PathBuf::from("/work/primary")));
        assert!(matches!(parsed.command, Some(super::CliCommand::Serve(_))));
    }
}
