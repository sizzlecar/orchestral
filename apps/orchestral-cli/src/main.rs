mod channel;
mod cli;
mod envfile;
mod runtime;
mod scenario;
mod theme;
mod tui;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::Cli::parse().run().await
}
