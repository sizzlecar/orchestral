mod api;
mod assets;
mod auth;
pub(crate) mod local_hosts;
pub(crate) mod model_controls;
mod server;
mod session_coordinator;
mod state;

pub use api::{router, RemoteApiState};
pub use assets::{router as asset_router, router_with_artifact_origin};
pub use auth::{GatewayAuthenticator, GatewayPrincipal, JwtGatewayAuthenticator, JwtGatewayConfig};
pub(crate) use server::serve;
pub use server::ServeCommand;
pub use state::{PairingTicket, RemoteRegistry};
