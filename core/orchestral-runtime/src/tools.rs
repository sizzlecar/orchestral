//! Host-guarded Tool implementations and MCP-to-Tool adapter.
//!
//! Applications explicitly register capabilities with `GuardedToolRuntime`;
//! there is no dynamic Action registry, factory, loader, or fallback path.

#[path = "action/builtin.rs"]
mod builtin;
#[path = "action/mcp.rs"]
mod mcp;
#[path = "action/shell_sandbox.rs"]
pub(crate) mod shell_sandbox;

pub use builtin::*;
pub use mcp::{
    GuardedMcpServerConfig, McpServerConnectionManager, McpServerHealth, McpToolsAdapterError,
    McpToolsAdapterRegistry,
};
