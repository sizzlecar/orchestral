use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context};
use axum::Router;
use clap::Args;
use qrcode::render::unicode;
use qrcode::QrCode;

use crate::agent::{build_agent_host, AgentRunOptions};
use crate::mcp_config::user_config_root;

use super::{asset_router, router, PairingTicket, RemoteApiState, RemoteRegistry};

const DEFAULT_PAIRING_TTL_SECS: u64 = 5 * 60;

#[derive(Debug, Clone, Args)]
pub struct ServeCommand {
    /// Address for the local Host gateway.
    #[arg(long, default_value = "127.0.0.1:8765")]
    listen: SocketAddr,

    /// Browser-visible HTTPS URL when a relay or reverse proxy fronts this Host.
    #[arg(long, value_name = "URL")]
    public_url: Option<String>,

    /// Issue a new one-time mobile device pairing link.
    #[arg(long)]
    pair: bool,

    /// Pairing link lifetime.
    #[arg(long, default_value_t = DEFAULT_PAIRING_TTL_SECS)]
    pairing_ttl_secs: u64,

    /// Override the durable paired-device/session registry.
    #[arg(long, value_name = "PATH")]
    state_file: Option<PathBuf>,

    /// Permit a non-loopback browser URL over cleartext HTTP. Intended only
    /// for a trusted LAN; installable PWA features normally require HTTPS.
    #[arg(long)]
    allow_insecure_http: bool,
}

pub(crate) async fn serve(command: ServeCommand, options: AgentRunOptions) -> anyhow::Result<()> {
    validate_public_surface(&command)?;
    let pairing = command
        .pair
        .then(|| {
            let ttl_ms = i64::try_from(command.pairing_ttl_secs)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000);
            PairingTicket::issue(ttl_ms)
        })
        .transpose()?;
    let state_path = command
        .state_file
        .clone()
        .unwrap_or(user_config_root()?.join("remote-control.json"));
    let registry = RemoteRegistry::open(&state_path, pairing.clone())?;
    if registry.active_device_count().await == 0 && pairing.is_none() {
        bail!("no paired mobile device exists; start with `orchestral serve --pair` to pair one");
    }

    let host = build_agent_host(&options).await?;
    let app = Router::new()
        .nest(
            "/api/v1",
            router(RemoteApiState {
                agent: host.api.clone(),
                approvals: host.approvals.clone(),
                registry,
            }),
        )
        .merge(asset_router());
    let listener = tokio::net::TcpListener::bind(command.listen)
        .await
        .with_context(|| format!("bind Orchestral Host gateway at {}", command.listen))?;
    let local_address = listener.local_addr().context("read Host gateway address")?;
    let public_url = command
        .public_url
        .clone()
        .unwrap_or_else(|| format!("http://{local_address}"));

    eprintln!(
        "Orchestral Host: backend={} model={} listen={local_address}",
        host.backend_name, host.model
    );
    eprintln!("Device registry: {}", state_path.display());
    if let Some(ticket) = pairing {
        let pairing_url = format!(
            "{}/#pair={}",
            public_url.trim_end_matches('/'),
            ticket.secret()
        );
        print_pairing(&pairing_url, ticket.expires_at_unix_ms)?;
    } else {
        eprintln!("Open {public_url}");
    }

    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve Orchestral Host gateway");
    host.shutdown().await;
    result
}

fn validate_public_surface(command: &ServeCommand) -> anyhow::Result<()> {
    if command.pairing_ttl_secs == 0 {
        bail!("pairing TTL must be positive");
    }
    if let Some(public_url) = &command.public_url {
        let secure = public_url.starts_with("https://");
        let local = public_url.starts_with("http://127.0.0.1")
            || public_url.starts_with("http://localhost")
            || public_url.starts_with("http://[::1]");
        if !secure && !local && !command.allow_insecure_http {
            bail!(
                "non-loopback public URL must use HTTPS; pass --allow-insecure-http only for a trusted LAN"
            );
        }
        if !(public_url.starts_with("https://") || public_url.starts_with("http://")) {
            bail!("public URL must begin with https:// or http://");
        }
    } else if !command.listen.ip().is_loopback() && !command.allow_insecure_http {
        bail!(
            "non-loopback listen requires --public-url https://... or explicit --allow-insecure-http"
        );
    }
    Ok(())
}

fn print_pairing(url: &str, expires_at_unix_ms: i64) -> anyhow::Result<()> {
    let code = QrCode::new(url.as_bytes()).context("encode pairing QR")?;
    let image = code
        .render::<unicode::Dense1x2>()
        .dark_color(unicode::Dense1x2::Light)
        .light_color(unicode::Dense1x2::Dark)
        .quiet_zone(true)
        .build();
    eprintln!("\nScan to pair this device (expires at {expires_at_unix_ms}):");
    eprintln!("{image}");
    eprintln!("{url}\n");
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    // Let in-flight HTTP responses finish before the Agent/MCP host shuts down.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(listen: &str, public_url: Option<&str>) -> ServeCommand {
        ServeCommand {
            listen: listen.parse().unwrap(),
            public_url: public_url.map(str::to_owned),
            pair: true,
            pairing_ttl_secs: 300,
            state_file: None,
            allow_insecure_http: false,
        }
    }

    #[test]
    fn remote_cleartext_requires_an_explicit_lan_override() {
        let remote = command("0.0.0.0:8765", Some("http://192.168.1.4:8765"));
        assert!(validate_public_surface(&remote).is_err());
        let secure = command("0.0.0.0:8765", Some("https://agent.example.test"));
        assert!(validate_public_surface(&secure).is_ok());
    }

    #[test]
    fn loopback_development_surface_is_allowed() {
        assert!(validate_public_surface(&command("127.0.0.1:8765", None)).is_ok());
    }
}
