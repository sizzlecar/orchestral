use std::env;
use std::net::SocketAddr;
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
    /// Run HTTP/SSE server
    Server(ServerArgs),
}

#[derive(Debug, Args, Clone)]
struct RunArgs {
    #[arg(long, default_value = "configs/orchestral.cli.yaml")]
    config: PathBuf,
    #[arg(long)]
    thread_id: Option<String>,
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

#[derive(Debug, Args, Clone)]
struct ServerArgs {
    #[arg(long, default_value = "configs/orchestral.cli.yaml")]
    config: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
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
                crate::tui::run_session(
                    args.config,
                    args.thread_id,
                    initial_input,
                    args.script,
                    args.once,
                    args.verbose,
                )
                .await
            }
            Some(Command::Server(args)) => {
                ensure_log_filter(false);
                orchestral_server::run_server(args.config, args.listen).await
            }
            None => {
                ensure_log_filter(false);
                crate::tui::run_session(
                    PathBuf::from("configs/orchestral.cli.yaml"),
                    None,
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
