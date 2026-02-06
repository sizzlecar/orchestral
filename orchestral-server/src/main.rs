use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "orchestral-server")]
struct Args {
    #[arg(long, default_value = "config/orchestral.cli.yaml")]
    config: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    orchestral_server::run_server(args.config, args.listen).await
}
