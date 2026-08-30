use std::env;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::envfile::load_env_file;
use crate::runtime::ModelOverrides;

#[derive(Debug, Parser)]
#[command(
    name = "orchestral",
    about = "Provider-neutral Orchestral Agent CLI",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<CliCommand>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    env_file: Option<PathBuf>,
    /// Provider credential document. For Google, this accepts a service-account JSON key.
    #[arg(long, value_name = "PATH")]
    credential_file: Option<PathBuf>,
    #[arg(long)]
    backend: Option<String>,
    #[arg(long)]
    model_profile: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long)]
    temperature: Option<f32>,
    /// Reuse this Agent Session identity for all turns in this process
    #[arg(long)]
    session_id: Option<String>,
    #[arg(long)]
    system_prompt: Option<String>,
    #[arg(long)]
    no_mcp: bool,
    /// Explicit local MCP manifest (`.mcp.json`). May be repeated.
    #[arg(long, value_name = "PATH")]
    mcp_config: Vec<PathBuf>,
    #[arg(long)]
    no_skills: bool,
    #[arg(long)]
    verbose: bool,
    /// Use DIR as the primary Agent workspace instead of the process directory.
    #[arg(short = 'C', long = "cwd", value_name = "DIR")]
    cwd: Option<PathBuf>,
    /// Add another read-write workspace root. May be repeated.
    #[arg(long = "add-dir", value_name = "DIR")]
    add_dirs: Vec<PathBuf>,
    /// A single turn. When omitted, starts an interactive conversation.
    #[arg(value_name = "INPUT")]
    input: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Manage MCP servers registered for this user.
    Mcp(crate::mcp_command::McpCommand),
}

impl Cli {
    fn model_overrides(&self) -> ModelOverrides {
        ModelOverrides {
            backend: self.backend.clone(),
            model_profile: self.model_profile.clone(),
            model: self.model.clone(),
            temperature: self.temperature,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        if let Some(env_file) = &self.env_file {
            load_env_file(env_file)?;
        }
        ensure_log_filter(self.verbose);
        if let Some(command) = self.command {
            return match command {
                CliCommand::Mcp(command) => command.run().await,
            };
        }
        let model_overrides = self.model_overrides();
        crate::agent::run(crate::agent::AgentRunOptions {
            config: self.config,
            credential_file: self.credential_file,
            model_overrides,
            session_id: self.session_id,
            system_prompt: self.system_prompt,
            input: (!self.input.is_empty()).then(|| self.input.join(" ")),
            no_mcp: self.no_mcp,
            mcp_config: self.mcp_config,
            no_skills: self.no_skills,
            cwd: self.cwd,
            add_dirs: self.add_dirs,
        })
        .await
    }
}

fn ensure_log_filter(verbose: bool) {
    if !verbose && env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::{CommandFactory, Parser};

    use super::Cli;

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
            ["mcp"]
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
}
