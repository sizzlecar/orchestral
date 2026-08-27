use std::env;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

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
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start an Agent conversation, optionally with one initial turn
    Agent(AgentArgs),
}

#[derive(Debug, Args, Clone, Default)]
struct AgentArgs {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    env_file: Option<PathBuf>,
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
    #[arg(long)]
    no_skills: bool,
    #[arg(long)]
    verbose: bool,
    /// A single turn. When omitted, starts an interactive conversation.
    #[arg(value_name = "INPUT")]
    input: Vec<String>,
}

impl AgentArgs {
    fn model_overrides(&self) -> ModelOverrides {
        ModelOverrides {
            backend: self.backend.clone(),
            model_profile: self.model_profile.clone(),
            model: self.model.clone(),
            temperature: self.temperature,
        }
    }
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        let args = match self.command {
            Some(Command::Agent(args)) => args,
            None => AgentArgs::default(),
        };
        if let Some(env_file) = &args.env_file {
            load_env_file(env_file)?;
        }
        ensure_log_filter(args.verbose);
        let model_overrides = args.model_overrides();
        crate::agent::run(crate::agent::AgentRunOptions {
            config: args.config,
            model_overrides,
            session_id: args.session_id,
            system_prompt: args.system_prompt,
            input: (!args.input.is_empty()).then(|| args.input.join(" ")),
            no_mcp: args.no_mcp,
            no_skills: args.no_skills,
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
    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn agent_is_the_only_explicit_conversation_entrypoint() {
        let parsed = Cli::try_parse_from(["orchestral", "agent", "inspect this repository"])
            .expect("the Agent entrypoint must parse");
        let Some(Command::Agent(args)) = parsed.command else {
            panic!("explicit Agent command must select the Agent surface");
        };
        assert_eq!(args.input, ["inspect this repository"]);

        assert!(Cli::try_parse_from(["orchestral", "run", "legacy input"]).is_err());
        assert!(Cli::try_parse_from(["orchestral", "scenario"]).is_err());
    }
}
