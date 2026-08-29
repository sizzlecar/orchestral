//! Windows Shell Sandboxing implementation.
#![allow(dead_code)]
//!
//! Provides constrained execution environment on Windows through:
//! 1. Process and argument normalization with Windows-compliant argument quoting.
//! 2. Security token restriction modeling (Low Integrity, privilege stripping, restricted SIDs).
//! 3. Job Object policy definitions (kill-on-close, memory limits, process count limits).
//! 4. Optional Windows Sandbox (.wsb) configuration generator for hypervisor-isolated execution.
//! 5. `WindowsRestrictedBackend` adapter implementing `ShellSandboxBackend`.

use std::collections::BTreeSet;
use std::path::PathBuf;

#[allow(unused_imports)]
use super::shell_sandbox::{
    SandboxCommandSpec, SandboxNetworkAccess, SandboxedCommand, ShellSandboxBackend,
    ShellSandboxPolicy,
};

/// Windows process integrity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WindowsIntegrityLevel {
    Untrusted,
    Low,
    Medium,
    High,
}

impl WindowsIntegrityLevel {
    pub fn as_sid_suffix(&self) -> &'static str {
        match self {
            Self::Untrusted => "S-1-16-0",
            Self::Low => "S-1-16-4096",
            Self::Medium => "S-1-16-8192",
            Self::High => "S-1-16-12288",
        }
    }
}

/// Windows Job Object security limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsJobLimits {
    /// Automatically terminate all processes in the job when the last job handle is closed.
    pub kill_on_job_close: bool,
    /// Maximum number of simultaneously active processes in the job.
    pub max_active_processes: Option<u32>,
    /// Maximum committed memory per job in bytes.
    pub max_memory_bytes: Option<u64>,
    /// Prevent processes in the job from accessing clipboard, desktop, or displaying modal dialogs.
    pub restrict_ui_operations: bool,
}

impl Default for WindowsJobLimits {
    fn default() -> Self {
        Self {
            kill_on_job_close: true,
            max_active_processes: Some(32),
            max_memory_bytes: Some(2 * 1024 * 1024 * 1024), // 2 GiB
            restrict_ui_operations: true,
        }
    }
}

/// Windows Security Token privilege and restriction policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsTokenPolicy {
    /// Process integrity level. Default is Low for sandboxed execution.
    pub integrity_level: WindowsIntegrityLevel,
    /// Privileges explicitly stripped from the access token.
    pub stripped_privileges: BTreeSet<String>,
    /// Whether to disable all privileges in the restricted token.
    pub disable_all_privileges: bool,
}

impl Default for WindowsTokenPolicy {
    fn default() -> Self {
        let mut stripped = BTreeSet::new();
        // Strip dangerous administrative and system privileges
        for priv_name in [
            "SeDebugPrivilege",
            "SeTcbPrivilege",
            "SeTakeOwnershipPrivilege",
            "SeSecurityPrivilege",
            "SeBackupPrivilege",
            "SeRestorePrivilege",
            "SeShutdownPrivilege",
            "SeRemoteShutdownPrivilege",
            "SeSystemEnvironmentPrivilege",
            "SeLoadDriverPrivilege",
            "SeImpersonatePrivilege",
            "SeCreateTokenPrivilege",
            "SeAssignPrimaryTokenPrivilege",
        ] {
            stripped.insert(priv_name.to_owned());
        }

        Self {
            integrity_level: WindowsIntegrityLevel::Low,
            stripped_privileges: stripped,
            disable_all_privileges: true,
        }
    }
}

/// Windows Sandbox (.wsb) configuration builder for Hyper-V isolated execution.
#[derive(Debug, Clone, Default)]
pub struct WindowsSandboxWsbConfig {
    pub vgpu: bool,
    pub networking: bool,
    pub mapped_folders: Vec<WsbMappedFolder>,
    pub logon_command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsbMappedFolder {
    pub host_folder: PathBuf,
    pub sandbox_folder: Option<PathBuf>,
    pub read_only: bool,
}

impl WindowsSandboxWsbConfig {
    /// Render this configuration as XML compatible with Windows Sandbox (.wsb files).
    pub fn to_xml(&self) -> String {
        let mut xml = String::from("<Configuration>\n");
        xml.push_str(&format!(
            "  <VGpu>{}</VGpu>\n",
            if self.vgpu { "Enable" } else { "Disable" }
        ));
        xml.push_str(&format!(
            "  <Networking>{}</Networking>\n",
            if self.networking { "Enable" } else { "Disable" }
        ));

        if !self.mapped_folders.is_empty() {
            xml.push_str("  <MappedFolders>\n");
            for folder in &self.mapped_folders {
                xml.push_str("    <MappedFolder>\n");
                xml.push_str(&format!(
                    "      <HostFolder>{}</HostFolder>\n",
                    escape_xml_text(&folder.host_folder.to_string_lossy())
                ));
                if let Some(sandbox_folder) = &folder.sandbox_folder {
                    xml.push_str(&format!(
                        "      <SandboxFolder>{}</SandboxFolder>\n",
                        escape_xml_text(&sandbox_folder.to_string_lossy())
                    ));
                }
                xml.push_str(&format!(
                    "      <ReadOnly>{}</ReadOnly>\n",
                    if folder.read_only { "true" } else { "false" }
                ));
                xml.push_str("    </MappedFolder>\n");
            }
            xml.push_str("  </MappedFolders>\n");
        }

        if let Some(cmd) = &self.logon_command {
            xml.push_str("  <LogonCommand>\n");
            xml.push_str(&format!(
                "    <Command>{}</Command>\n",
                escape_xml_text(cmd)
            ));
            xml.push_str("  </LogonCommand>\n");
        }

        xml.push_str("</Configuration>");
        xml
    }
}

fn escape_xml_text(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Properly quote a command line argument according to Windows CommandLineToArgvW parsing rules.
pub fn quote_windows_arg(arg: &str) -> String {
    if arg.is_empty() {
        return "\"\"".to_owned();
    }

    let needs_quotes = arg
        .chars()
        .any(|c| c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '"');
    if !needs_quotes {
        return arg.to_owned();
    }

    let mut result = String::with_capacity(arg.len() + 2);
    result.push('"');

    let mut backslashes = 0;
    for c in arg.chars() {
        if c == '\\' {
            backslashes += 1;
        } else if c == '"' {
            // Escape all preceding backslashes plus the quote itself
            for _ in 0..(backslashes * 2 + 1) {
                result.push('\\');
            }
            result.push('"');
            backslashes = 0;
        } else {
            // Unescaped backslashes preceding regular characters
            for _ in 0..backslashes {
                result.push('\\');
            }
            backslashes = 0;
            result.push(c);
        }
    }

    // Trailing backslashes before the closing double quote must be doubled
    for _ in 0..(backslashes * 2) {
        result.push('\\');
    }
    result.push('"');

    result
}

/// Windows Restricted Execution Sandbox Backend.
#[derive(Debug, Clone, Default)]
pub struct WindowsRestrictedBackend {
    pub job_limits: WindowsJobLimits,
    pub token_policy: WindowsTokenPolicy,
}

impl ShellSandboxBackend for WindowsRestrictedBackend {
    fn backend_name(&self) -> &'static str {
        "windows_restricted"
    }

    fn transform(
        &self,
        spec: SandboxCommandSpec,
        policy: &ShellSandboxPolicy,
    ) -> Result<SandboxedCommand, String> {
        // Enforce network isolation
        let network_disabled = policy.network.is_disabled();
        if let SandboxNetworkAccess::ExactTargets(targets) = &policy.network {
            for target in targets {
                let (host, _) = target
                    .rsplit_once(':')
                    .ok_or_else(|| format!("Invalid network target format: {target}"))?;
                if host != "localhost" && host != "127.0.0.1" && host != "::1" {
                    return Err(format!(
                        "Exact remote network target '{target}' is restricted on Windows without a proxy"
                    ));
                }
            }
        }

        // Validate cwd is within readable roots
        let cwd_allowed = policy
            .readable_roots
            .iter()
            .any(|root| spec.cwd.starts_with(root));
        if !cwd_allowed {
            return Err(format!(
                "Working directory '{}' is outside sandbox readable roots",
                spec.cwd.display()
            ));
        }

        // Validate program is in launcher programs whitelist
        let program_path = PathBuf::from(&spec.program);
        let program_allowed = policy.launcher_programs.iter().any(|p| p == &program_path);
        if !program_allowed {
            return Err(format!(
                "Executable '{}' is not registered in launcher_programs whitelist",
                spec.program
            ));
        }

        let mut env = spec.env;
        env.insert(
            "ORCHESTRAL_SANDBOX_BACKEND".to_string(),
            self.backend_name().to_string(),
        );
        env.insert(
            "ORCHESTRAL_SANDBOX_NETWORK_DISABLED".to_string(),
            if network_disabled {
                "1".to_string()
            } else {
                "0".to_string()
            },
        );
        env.insert(
            "ORCHESTRAL_SANDBOX_INTEGRITY_LEVEL".to_string(),
            self.token_policy
                .integrity_level
                .as_sid_suffix()
                .to_string(),
        );

        Ok(SandboxedCommand {
            program: spec.program,
            args: spec.args,
            env,
            backend: self.backend_name(),
            backend_starts_new_session: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_quote_windows_arg_simple() {
        assert_eq!(quote_windows_arg("simple"), "simple");
        assert_eq!(quote_windows_arg(""), "\"\"");
        assert_eq!(quote_windows_arg("hello world"), "\"hello world\"");
    }

    #[test]
    fn test_quote_windows_arg_with_quotes_and_backslashes() {
        assert_eq!(
            quote_windows_arg(r#"C:\Program Files\"#),
            r#""C:\Program Files\\""#
        );
        assert_eq!(
            quote_windows_arg(r#"foo "bar" baz"#),
            r#""foo \"bar\" baz""#
        );
        assert_eq!(
            quote_windows_arg(r#"C:\test with space\a\"#),
            r#""C:\test with space\a\\""#
        );
        assert_eq!(quote_windows_arg(r#"C:\test\a\"#), r#"C:\test\a\"#);
        assert_eq!(quote_windows_arg(r#"\"#), r#"\"#);
        assert_eq!(quote_windows_arg(r#"\ "#), r#""\ ""#);
    }

    #[test]
    fn test_wsb_config_xml_generation() {
        let config = WindowsSandboxWsbConfig {
            vgpu: false,
            networking: false,
            mapped_folders: vec![
                WsbMappedFolder {
                    host_folder: PathBuf::from("C:\\workspace"),
                    sandbox_folder: Some(PathBuf::from(
                        "C:\\Users\\WDAGUtilityAccount\\Desktop\\workspace",
                    )),
                    read_only: false,
                },
                WsbMappedFolder {
                    host_folder: PathBuf::from("C:\\readonly_data"),
                    sandbox_folder: None,
                    read_only: true,
                },
            ],
            logon_command: Some("cmd.exe /c echo Ready".to_string()),
        };

        let xml = config.to_xml();
        assert!(xml.contains("<VGpu>Disable</VGpu>"));
        assert!(xml.contains("<Networking>Disable</Networking>"));
        assert!(xml.contains("<HostFolder>C:\\workspace</HostFolder>"));
        assert!(xml.contains("<ReadOnly>false</ReadOnly>"));
        assert!(xml.contains("<HostFolder>C:\\readonly_data</HostFolder>"));
        assert!(xml.contains("<ReadOnly>true</ReadOnly>"));
        assert!(xml.contains("<Command>cmd.exe /c echo Ready</Command>"));
    }

    #[test]
    fn test_windows_token_policy_defaults() {
        let policy = WindowsTokenPolicy::default();
        assert_eq!(policy.integrity_level, WindowsIntegrityLevel::Low);
        assert_eq!(policy.integrity_level.as_sid_suffix(), "S-1-16-4096");
        assert!(policy.stripped_privileges.contains("SeDebugPrivilege"));
        assert!(policy
            .stripped_privileges
            .contains("SeCreateTokenPrivilege"));
        assert!(policy.disable_all_privileges);
    }

    #[test]
    fn test_windows_job_limits_defaults() {
        let limits = WindowsJobLimits::default();
        assert!(limits.kill_on_job_close);
        assert_eq!(limits.max_active_processes, Some(32));
        assert!(limits.restrict_ui_operations);
    }

    #[test]
    fn test_windows_restricted_backend_transform_valid() {
        let backend = WindowsRestrictedBackend::default();
        let cwd = PathBuf::from("/workspace");
        let program = PathBuf::from("/workspace/bin/tool.exe");

        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: vec!["--flag".to_string(), "hello world".to_string()],
            cwd: cwd.clone(),
            env: HashMap::new(),
        };

        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            writable_roots: vec![cwd.clone()],
            launcher_programs: vec![program.clone()],
            ..ShellSandboxPolicy::default()
        };

        let result = backend.transform(spec, &policy);
        assert!(result.is_ok());
        let cmd = result.unwrap();
        assert_eq!(cmd.backend, "windows_restricted");
        assert!(cmd.backend_starts_new_session);
        assert_eq!(
            cmd.env
                .get("ORCHESTRAL_SANDBOX_BACKEND")
                .map(|s| s.as_str()),
            Some("windows_restricted")
        );
        assert_eq!(
            cmd.env
                .get("ORCHESTRAL_SANDBOX_NETWORK_DISABLED")
                .map(|s| s.as_str()),
            Some("1")
        );
        assert_eq!(
            cmd.env
                .get("ORCHESTRAL_SANDBOX_INTEGRITY_LEVEL")
                .map(|s| s.as_str()),
            Some("S-1-16-4096")
        );
    }

    #[test]
    fn test_windows_restricted_backend_rejects_unlisted_program() {
        let backend = WindowsRestrictedBackend::default();
        let cwd = PathBuf::from("/workspace");
        let program = PathBuf::from("/workspace/bin/tool.exe");
        let unlisted_program = PathBuf::from("/workspace/bin/evil.exe");

        let spec = SandboxCommandSpec {
            program: unlisted_program.to_string_lossy().into_owned(),
            args: vec![],
            cwd: cwd.clone(),
            env: HashMap::new(),
        };

        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            writable_roots: vec![cwd],
            launcher_programs: vec![program],
            ..ShellSandboxPolicy::default()
        };

        let result = backend.transform(spec, &policy);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("not registered in launcher_programs whitelist"));
    }

    #[test]
    fn test_windows_restricted_backend_rejects_unreadable_cwd() {
        let backend = WindowsRestrictedBackend::default();
        let cwd = PathBuf::from("/other/path");
        let program = PathBuf::from("/workspace/bin/tool.exe");

        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: vec![],
            cwd: cwd.clone(),
            env: HashMap::new(),
        };

        let policy = ShellSandboxPolicy {
            readable_roots: vec![PathBuf::from("/workspace")],
            writable_roots: vec![PathBuf::from("/workspace")],
            launcher_programs: vec![program],
            ..ShellSandboxPolicy::default()
        };

        let result = backend.transform(spec, &policy);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("outside sandbox readable roots"));
    }

    #[test]
    fn test_windows_restricted_backend_network_validation() {
        let backend = WindowsRestrictedBackend::default();
        let cwd = PathBuf::from("/workspace");
        let program = PathBuf::from("/workspace/bin/tool.exe");

        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: vec![],
            cwd: cwd.clone(),
            env: HashMap::new(),
        };

        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            writable_roots: vec![cwd.clone()],
            launcher_programs: vec![program.clone()],
            network: SandboxNetworkAccess::ExactTargets(BTreeSet::from([
                "localhost:8080".to_string()
            ])),
            ..ShellSandboxPolicy::default()
        };

        let result = backend.transform(spec.clone(), &policy);
        assert!(result.is_ok());
        assert_eq!(
            result
                .unwrap()
                .env
                .get("ORCHESTRAL_SANDBOX_NETWORK_DISABLED")
                .map(|s| s.as_str()),
            Some("0")
        );

        let mut remote_policy = policy.clone();
        remote_policy.network =
            SandboxNetworkAccess::ExactTargets(BTreeSet::from(["example.com:443".to_string()]));
        let result = backend.transform(spec.clone(), &remote_policy);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("restricted on Windows without a proxy"));

        let mut unrestricted_policy = policy;
        unrestricted_policy.network = SandboxNetworkAccess::Unrestricted;
        let result = backend.transform(spec, &unrestricted_policy).unwrap();
        assert_eq!(
            result
                .env
                .get("ORCHESTRAL_SANDBOX_NETWORK_DISABLED")
                .map(String::as_str),
            Some("0")
        );
    }
}
