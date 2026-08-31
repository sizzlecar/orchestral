mod api;
mod assets;
mod auth;
mod server;
mod state;

pub use api::{router, RemoteApiState};
pub use assets::router as asset_router;
pub use auth::{GatewayAuthenticator, GatewayPrincipal, JwtGatewayAuthenticator, JwtGatewayConfig};
pub(crate) use server::serve;
pub use server::ServeCommand;
pub use state::{PairingTicket, RemoteRegistry};
