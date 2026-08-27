mod agent;
mod cli;
mod envfile;
mod google_auth;
mod runtime;
#[allow(dead_code)] // C3 pure UI core is connected to the terminal and AgentClient in C4.
mod tui;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::Cli::parse().run().await
}
