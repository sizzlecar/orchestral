//! Orchestral application composition and transport surfaces.

pub mod agent;
mod cli;
mod envfile;
mod google_auth;
mod mcp_command;
mod mcp_config;
pub mod remote;
mod runtime;
mod tui;

use clap::Parser;

pub async fn run_cli() -> anyhow::Result<()> {
    cli::Cli::parse().run().await
}
