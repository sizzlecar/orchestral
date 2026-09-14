//! Native Windows has no implemented filesystem/network sandbox adapter yet.
//!
//! Host-approved execution is a separate capability with explicit approval.
//! Never label an unrestricted child as sandboxed by setting environment values.

use super::shell_sandbox::{
    SandboxCommandSpec, SandboxedCommand, ShellSandboxBackend, ShellSandboxPolicy,
};

pub(super) struct WindowsRestrictedBackend;

impl ShellSandboxBackend for WindowsRestrictedBackend {
    fn backend_name(&self) -> &'static str {
        "windows_unavailable"
    }

    fn transform(
        &self,
        _spec: SandboxCommandSpec,
        _policy: &ShellSandboxPolicy,
    ) -> Result<SandboxedCommand, String> {
        Err("Native Windows sandbox isolation is unavailable. Use WSL with bubblewrap for sandboxed commands, or explicitly request Host-approved execution when the Host permits it. No command was started.".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_never_reports_unrestricted_execution_as_sandboxed() {
        let spec = SandboxCommandSpec {
            program: "cmd.exe".to_owned(),
            args: vec!["/C".to_owned(), "echo test".to_owned()],
            cwd: std::path::PathBuf::from("C:\\workspace"),
            env: Default::default(),
        };
        assert!(WindowsRestrictedBackend
            .transform(spec, &ShellSandboxPolicy::default())
            .is_err());
    }
}
