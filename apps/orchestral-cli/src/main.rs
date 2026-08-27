mod agent;
mod cli;
mod envfile;
mod google_auth;
mod runtime;

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::Cli::parse().run().await
}
