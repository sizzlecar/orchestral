use std::env;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "orchestral", about = "Orchestral CLI")]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run with optional initial input; enters chat by default
    Run(RunArgs),
}

#[derive(Debug, Args, Clone)]
struct RunArgs {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    thread_id: Option<String>,
    /// Disable MCP action auto-discovery and registration
    #[arg(long)]
    no_mcp: bool,
    /// Disable Skill action auto-discovery and registration
    #[arg(long)]
    no_skills: bool,
    /// Read multi-turn inputs from file (one turn per line; '#' comments supported)
    #[arg(long)]
    script: Option<PathBuf>,
    #[arg(long)]
    verbose: bool,
    #[arg(long)]
    once: bool,
    #[arg(value_name = "INPUT")]
    input: Vec<String>,
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.command {
            Some(Command::Run(args)) => {
                ensure_log_filter(args.verbose);
                let initial_input = if args.input.is_empty() {
                    None
                } else {
                    Some(args.input.join(" "))
                };
                crate::tui::run_session(crate::tui::SessionRunOptions {
                    config: args.config,
                    thread_id: args.thread_id,
                    no_mcp: args.no_mcp,
                    no_skills: args.no_skills,
                    initial_input,
                    script_path: args.script,
                    once: args.once,
                    verbose: args.verbose,
                })
                .await
            }
            None => {
                ensure_log_filter(false);
                crate::tui::run_session(crate::tui::SessionRunOptions {
                    config: None,
                    thread_id: None,
                    no_mcp: false,
                    no_skills: false,
                    initial_input: None,
                    script_path: None,
                    once: false,
                    verbose: false,
                })
                .await
            }
        }
    }
}

fn ensure_log_filter(verbose: bool) {
    if verbose {
        return;
    }
    if env::var("RUST_LOG").is_ok() {
        return;
    }
    env::set_var("RUST_LOG", "info");
}
