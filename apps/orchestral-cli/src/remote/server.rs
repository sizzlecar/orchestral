use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use axum::Router;
use clap::Args;
use orchestral_core::agent_protocol::reference::AgentRunStatus;
use orchestral_core::agent_protocol::wire::RunId;
use qrcode::render::unicode;
use qrcode::QrCode;

use crate::agent::{build_agent_host, AgentRunOptions};
use crate::mcp_config::user_config_root;

use super::api::spawn_remembered_approval_driver;
use super::{
    asset_router, router, GatewayAuthenticator, JwtGatewayAuthenticator, JwtGatewayConfig,
    PairingTicket, RemoteApiState, RemoteRegistry,
};

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

    /// Expected issuer for JWT assertions injected by an identity-aware reverse proxy.
    #[arg(long, value_name = "URL")]
    access_jwt_issuer: Option<String>,

    /// Expected audience for reverse-proxy JWT assertions.
    #[arg(long, value_name = "AUDIENCE")]
    access_jwt_audience: Option<String>,

    /// JWKS endpoint used to verify reverse-proxy JWT assertions.
    #[arg(long, value_name = "URL")]
    access_jwt_jwks_url: Option<String>,

    /// Request header carrying the reverse-proxy JWT assertion.
    #[arg(long, value_name = "HEADER")]
    access_jwt_header: Option<String>,

    /// Required JWT claim. Repeat for multiple NAME=VALUE checks; dotted names address nested claims.
    #[arg(long = "access-jwt-required-claim", value_name = "NAME=VALUE")]
    access_jwt_required_claims: Vec<String>,
}

pub(crate) async fn serve(command: ServeCommand, options: AgentRunOptions) -> anyhow::Result<()> {
    validate_public_surface(&command)?;
    let gateway_authenticator = build_gateway_authenticator(&command)?;
    if gateway_authenticator.is_some() && command.pair {
        bail!("--pair cannot be combined with gateway JWT authentication");
    }
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
    if registry.active_device_count().await == 0
        && pairing.is_none()
        && gateway_authenticator.is_none()
    {
        bail!("no paired mobile device exists; start with `orchestral serve --pair` to pair one");
    }

    let host = build_agent_host(&options).await?;
    let remote_state = RemoteApiState {
        agent: host.api.clone(),
        approvals: host.approvals.clone(),
        registry,
        gateway_authenticator: gateway_authenticator.clone(),
    };
    recover_registered_runs(&remote_state).await;
    let app = Router::new()
        .nest("/api/v1", router(remote_state))
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
    if gateway_authenticator.is_some() {
        eprintln!("Remote authentication: signed reverse-proxy JWT");
    }
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

/// Restores every durable, non-terminal Run owned by the remote registry
/// before the HTTP surface becomes reachable. Loading a Run into a fresh
/// controller first records continuity loss; Provider recovery then verifies
/// and replays its committed prefix, which also re-stages any pending approval
/// in the replacement Host broker.
async fn recover_registered_runs(state: &RemoteApiState) {
    let run_ids = state
        .registry
        .sessions()
        .await
        .into_iter()
        .flat_map(|session| session.run_ids)
        .collect::<std::collections::BTreeSet<_>>();

    for raw_run_id in run_ids {
        let run_id = RunId::new(raw_run_id);
        let view = match state.agent.inspect(&run_id).await {
            Ok(view) => view,
            Err(error) => {
                tracing::warn!(run_id = %run_id.as_str(), %error, "could not inspect registered remote Run during Host recovery");
                continue;
            }
        };
        if view.state.is_terminal() {
            continue;
        }

        if view.state.status() == AgentRunStatus::Unknown {
            if let Err(error) = state.agent.recover(&run_id).await {
                tracing::warn!(run_id = %run_id.as_str(), %error, "could not recover registered remote Run");
                continue;
            }
        }

        spawn_remembered_approval_driver(state.clone(), run_id);
    }
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

fn build_gateway_authenticator(
    command: &ServeCommand,
) -> anyhow::Result<Option<Arc<dyn GatewayAuthenticator>>> {
    let configured = command.access_jwt_issuer.is_some()
        || command.access_jwt_audience.is_some()
        || command.access_jwt_jwks_url.is_some()
        || command.access_jwt_header.is_some()
        || !command.access_jwt_required_claims.is_empty();
    if !configured {
        return Ok(None);
    }
    let issuer = command
        .access_jwt_issuer
        .clone()
        .context("--access-jwt-issuer is required when gateway JWT authentication is enabled")?;
    let audience = command
        .access_jwt_audience
        .clone()
        .context("--access-jwt-audience is required when gateway JWT authentication is enabled")?;
    let jwks_url = command
        .access_jwt_jwks_url
        .clone()
        .context("--access-jwt-jwks-url is required when gateway JWT authentication is enabled")?;
    let header = command
        .access_jwt_header
        .clone()
        .context("--access-jwt-header is required when gateway JWT authentication is enabled")?;
    let required_claims = parse_required_claims(&command.access_jwt_required_claims)?;
    let config = JwtGatewayConfig::new(issuer, audience, jwks_url, header, required_claims)?;
    Ok(Some(Arc::new(JwtGatewayAuthenticator::new(config)?)))
}

fn parse_required_claims(entries: &[String]) -> anyhow::Result<BTreeMap<String, String>> {
    entries
        .iter()
        .try_fold(BTreeMap::new(), |mut claims, entry| {
            let (name, value) = entry.split_once('=').with_context(|| {
                format!("invalid --access-jwt-required-claim '{entry}'; expected NAME=VALUE")
            })?;
            if claims.insert(name.to_owned(), value.to_owned()).is_some() {
                bail!("duplicate gateway JWT required claim '{name}'");
            }
            Ok(claims)
        })
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
            access_jwt_issuer: None,
            access_jwt_audience: None,
            access_jwt_jwks_url: None,
            access_jwt_header: None,
            access_jwt_required_claims: Vec::new(),
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

    #[test]
    fn gateway_jwt_configuration_is_all_or_nothing() {
        let mut partial = command("127.0.0.1:8765", None);
        partial.access_jwt_issuer = Some("https://access.example.com".to_owned());
        assert!(build_gateway_authenticator(&partial).is_err());

        partial.access_jwt_audience = Some("orchestral".to_owned());
        partial.access_jwt_jwks_url = Some("https://access.example.com/keys".to_owned());
        partial.access_jwt_header = Some("x-access-jwt".to_owned());
        partial.access_jwt_required_claims = vec!["email=person@example.com".to_owned()];
        assert!(build_gateway_authenticator(&partial).is_ok());
    }
}
