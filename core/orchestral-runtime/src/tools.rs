//! Host-guarded Tool implementations and MCP-to-Tool adapter.
//!
//! Applications explicitly register capabilities with `GuardedToolRuntime`.

mod builtin;
mod mcp;
pub(crate) mod shell_sandbox;
mod toolchain;
#[cfg(any(windows, test))]
mod windows_sandbox;

pub use builtin::*;
pub use mcp::{
    GuardedMcpServerConfig, McpServerConnectionManager, McpServerHealth, McpToolsAdapterError,
    McpToolsAdapterRegistry, StdioMcpSandboxPolicy, StdioMcpTransportFactory,
    MCP_STDIO_SANDBOX_PROFILE,
};
pub use toolchain::host_toolchain_readable_files;
