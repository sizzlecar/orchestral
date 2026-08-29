use std::env;
use std::path::PathBuf;

use clap::Parser;

use crate::envfile::load_env_file;
use crate::runtime::ModelOverrides;

#[derive(Debug, Parser)]
#[command(
    name = "orchestral",
    about = "Provider-neutral Orchestral Agent CLI",
    version
)]
pub struct Cli {
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
    #[arg(long)]
    no_skills: bool,
    #[arg(long)]
    verbose: bool,
    /// A single turn. When omitted, starts an interactive conversation.
    #[arg(value_name = "INPUT")]
    input: Vec<String>,
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
        let model_overrides = self.model_overrides();
        crate::agent::run(crate::agent::AgentRunOptions {
            config: self.config,
            credential_file: self.credential_file,
            model_overrides,
            session_id: self.session_id,
            system_prompt: self.system_prompt,
            input: (!self.input.is_empty()).then(|| self.input.join(" ")),
            no_mcp: self.no_mcp,
            no_skills: self.no_skills,
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
    use clap::{CommandFactory, Parser};

    use super::Cli;

    #[test]
    fn root_command_is_the_agent_entrypoint() {
        let parsed = Cli::try_parse_from(["orchestral", "inspect this repository"])
            .expect("a positional prompt must start the Agent directly");
        assert_eq!(parsed.input, ["inspect this repository"]);
        assert_eq!(Cli::command().get_subcommands().count(), 0);
    }
}
