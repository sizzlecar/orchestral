use std::env;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "orchestral", about = "Orchestral CLI/TUI")]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start interactive TUI chat
    Tui(TuiArgs),
    /// Run with optional initial input; enters chat by default
    Run(RunArgs),
}

#[derive(Debug, Args, Clone)]
struct TuiArgs {
    #[arg(long, default_value = "config/orchestral.cli.yaml")]
    config: PathBuf,
    #[arg(long)]
    thread_id: Option<String>,
    #[arg(long)]
    verbose: bool,
}

#[derive(Debug, Args, Clone)]
struct RunArgs {
    #[arg(long, default_value = "config/orchestral.cli.yaml")]
    config: PathBuf,
    #[arg(long)]
    thread_id: Option<String>,
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
            Some(Command::Tui(args)) => {
                ensure_log_filter(args.verbose);
                crate::tui::run_session(args.config, args.thread_id, None, false, args.verbose)
                    .await
            }
            Some(Command::Run(args)) => {
                ensure_log_filter(args.verbose);
                let initial_input = if args.input.is_empty() {
                    None
                } else {
                    Some(args.input.join(" "))
                };
                crate::tui::run_session(
                    args.config,
                    args.thread_id,
                    initial_input,
                    args.once,
                    args.verbose,
                )
                .await
            }
            None => {
                ensure_log_filter(false);
                crate::tui::run_session(
                    PathBuf::from("config/orchestral.cli.yaml"),
                    None,
                    None,
                    false,
                    false,
                )
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
