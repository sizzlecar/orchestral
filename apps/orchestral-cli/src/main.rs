#[tokio::main]
async fn main() -> anyhow::Result<()> {
    orchestral_cli::run_cli().await
}
