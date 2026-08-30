mod agent;
mod cli;
mod envfile;
mod google_auth;
mod mcp_config;
mod runtime;
mod tui;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::Cli::parse().run().await
}
