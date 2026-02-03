use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShellSandboxMode {
    None,
    ReadOnly,
    WorkspaceWrite,
}

impl ShellSandboxMode {
    pub fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "none" | "off" => Some(Self::None),
            "read_only" | "readonly" | "ro" => Some(Self::ReadOnly),
            "workspace_write" | "workspace" | "ws" => Some(Self::WorkspaceWrite),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ReadOnly => "read_only",
            Self::WorkspaceWrite => "workspace_write",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShellSandboxBackendKind {
    Auto,
    MacosSeatbelt,
    LinuxSeccomp,
    WindowsRestricted,
}

impl ShellSandboxBackendKind {
    pub fn from_str(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "seatbelt" | "macos_seatbelt" | "macos" => Some(Self::MacosSeatbelt),
            "linux_seccomp" | "seccomp" | "linux" => Some(Self::LinuxSeccomp),
            "windows_restricted" | "windows" => Some(Self::WindowsRestricted),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShellSandboxPolicy {
    pub mode: ShellSandboxMode,
    pub backend: ShellSandboxBackendKind,
    pub allow_network: bool,
    pub writable_roots: Vec<PathBuf>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub linux_bwrap_path: Option<PathBuf>,
}

impl Default for ShellSandboxPolicy {
    fn default() -> Self {
        Self {
            mode: ShellSandboxMode::None,
            backend: ShellSandboxBackendKind::Auto,
            allow_network: false,
            writable_roots: Vec::new(),
            linux_bwrap_path: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SandboxedCommand {
    pub program: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub backend: &'static str,
}

#[derive(Debug, Clone)]
pub struct SandboxCommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
}

trait ShellSandboxBackend {
    fn backend_name(&self) -> &'static str;
    fn transform(
        &self,
        spec: SandboxCommandSpec,
        policy: &ShellSandboxPolicy,
    ) -> Result<SandboxedCommand, String>;
}

struct UnsupportedBackend {
    backend_name: &'static str,
    reason: &'static str,
}

impl ShellSandboxBackend for UnsupportedBackend {
    fn backend_name(&self) -> &'static str {
        self.backend_name
    }

    fn transform(
        &self,
        _spec: SandboxCommandSpec,
        _policy: &ShellSandboxPolicy,
    ) -> Result<SandboxedCommand, String> {
        Err(format!(
            "Sandbox backend '{}' is unavailable: {}",
            self.backend_name, self.reason
        ))
    }
}

#[cfg(target_os = "macos")]
struct MacosSeatbeltBackend;

#[cfg(target_os = "macos")]
impl ShellSandboxBackend for MacosSeatbeltBackend {
    fn backend_name(&self) -> &'static str {
        "macos_seatbelt"
    }

    fn transform(
        &self,
        spec: SandboxCommandSpec,
        policy: &ShellSandboxPolicy,
    ) -> Result<SandboxedCommand, String> {
        let seatbelt_path = Path::new("/usr/bin/sandbox-exec");
        if !seatbelt_path.exists() {
            return Err("sandbox-exec not found at /usr/bin/sandbox-exec".to_string());
        }

        let profile = build_macos_profile(&spec.cwd, policy);
        let mut sandboxed_args = vec!["-p".to_string(), profile, spec.program];
        sandboxed_args.extend(spec.args);
        let mut env = spec.env;
        env.insert(
            "ORCHESTRAL_SANDBOX_BACKEND".to_string(),
            self.backend_name().to_string(),
        );

        Ok(SandboxedCommand {
            program: seatbelt_path.to_string_lossy().to_string(),
            args: sandboxed_args,
            env,
            backend: self.backend_name(),
        })
    }
}

#[cfg(target_os = "linux")]
struct LinuxBwrapBackend;

#[cfg(target_os = "linux")]
impl ShellSandboxBackend for LinuxBwrapBackend {
    fn backend_name(&self) -> &'static str {
        "linux_bwrap"
    }

    fn transform(
        &self,
        spec: SandboxCommandSpec,
        policy: &ShellSandboxPolicy,
    ) -> Result<SandboxedCommand, String> {
        let bwrap = resolve_linux_bwrap_executable(policy)?;
        let mut args = build_linux_bwrap_args(&spec, policy);
        args.push("--".to_string());
        args.push(spec.program);
        args.extend(spec.args);

        let mut env = spec.env;
        env.insert(
            "ORCHESTRAL_SANDBOX_BACKEND".to_string(),
            self.backend_name().to_string(),
        );
        Ok(SandboxedCommand {
            program: bwrap.to_string_lossy().to_string(),
            args,
            env,
            backend: self.backend_name(),
        })
    }
}

fn resolve_backend(policy: &ShellSandboxPolicy) -> Box<dyn ShellSandboxBackend> {
    match policy.backend {
        ShellSandboxBackendKind::Auto => default_backend_for_platform(),
        ShellSandboxBackendKind::MacosSeatbelt => {
            #[cfg(target_os = "macos")]
            {
                Box::new(MacosSeatbeltBackend)
            }
            #[cfg(not(target_os = "macos"))]
            {
                Box::new(UnsupportedBackend {
                    backend_name: "macos_seatbelt",
                    reason: "only available on macOS",
                })
            }
        }
        ShellSandboxBackendKind::LinuxSeccomp => Box::new(UnsupportedBackend {
            backend_name: "linux_seccomp",
            reason: "backend adapter is not implemented yet",
        }),
        ShellSandboxBackendKind::WindowsRestricted => Box::new(UnsupportedBackend {
            backend_name: "windows_restricted",
            reason: "backend adapter is not implemented yet",
        }),
    }
}

fn default_backend_for_platform() -> Box<dyn ShellSandboxBackend> {
    #[cfg(target_os = "macos")]
    {
        return Box::new(MacosSeatbeltBackend);
    }

    #[cfg(target_os = "linux")]
    {
        return Box::new(LinuxBwrapBackend);
    }

    #[cfg(target_os = "windows")]
    {
        return Box::new(UnsupportedBackend {
            backend_name: "windows_restricted",
            reason: "backend adapter is not implemented yet",
        });
    }

    #[allow(unreachable_code)]
    Box::new(UnsupportedBackend {
        backend_name: "unsupported",
        reason: "platform has no sandbox backend adapter",
    })
}

pub fn sandbox_command(
    program: String,
    args: Vec<String>,
    cwd: &Path,
    policy: &ShellSandboxPolicy,
) -> Result<SandboxedCommand, String> {
    let mut env = HashMap::new();
    if !policy.allow_network {
        env.insert(
            "ORCHESTRAL_SANDBOX_NETWORK_DISABLED".to_string(),
            "1".to_string(),
        );
    }

    match policy.mode {
        ShellSandboxMode::None => Ok(SandboxedCommand {
            program,
            args,
            env,
            backend: "none",
        }),
        _ => {
            let backend = resolve_backend(policy);
            let spec = SandboxCommandSpec {
                program,
                args,
                cwd: cwd.to_path_buf(),
                env,
            };
            backend.transform(spec, policy).map_err(|e| {
                format!(
                    "{} (backend={}, mode={})",
                    e,
                    backend.backend_name(),
                    policy.mode.as_str()
                )
            })
        }
    }
}

pub fn resolve_root_path(cwd: &Path, root: &Path) -> PathBuf {
    let joined = if root.is_absolute() {
        root.to_path_buf()
    } else {
        cwd.join(root)
    };
    std::fs::canonicalize(&joined).unwrap_or(joined)
}

#[cfg(target_os = "macos")]
fn build_macos_profile(cwd: &Path, policy: &ShellSandboxPolicy) -> String {
    let mut profile = String::new();
    profile.push_str("(version 1)\n");
    profile.push_str("(deny default)\n");
    profile.push_str("(allow process*)\n");
    profile.push_str("(allow sysctl-read)\n");
    profile.push_str("(allow file-read*)\n");
    if policy.allow_network {
        profile.push_str("(allow network*)\n");
    }

    if policy.mode == ShellSandboxMode::WorkspaceWrite {
        let mut roots = policy.writable_roots.clone();
        if roots.is_empty() {
            roots.push(cwd.to_path_buf());
        }
        for root in roots {
            let resolved = resolve_root_path(cwd, &root);
            profile.push_str(&format!(
                "(allow file-write* (subpath \"{}\"))\n",
                escape_profile_string(&resolved.to_string_lossy())
            ));
        }
    }
    profile
}

#[cfg(target_os = "macos")]
fn escape_profile_string(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(target_os = "linux")]
fn resolve_linux_bwrap_executable(policy: &ShellSandboxPolicy) -> Result<PathBuf, String> {
    if let Some(path) = &policy.linux_bwrap_path {
        if path.exists() {
            return Ok(path.clone());
        }
        return Err(format!(
            "Configured sandbox_linux_bwrap_path does not exist: {}",
            path.to_string_lossy()
        ));
    }

    for candidate in ["bwrap", "bubblewrap"] {
        if let Some(path) = find_executable_in_path(candidate) {
            return Ok(path);
        }
    }
    Err(
        "bubblewrap executable not found (tried bwrap/bubblewrap in PATH); set config.sandbox_linux_bwrap_path"
            .to_string(),
    )
}

#[cfg(target_os = "linux")]
fn find_executable_in_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for base in std::env::split_paths(&path_var) {
        let candidate = base.join(name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn build_linux_bwrap_args(spec: &SandboxCommandSpec, policy: &ShellSandboxPolicy) -> Vec<String> {
    let mut args = vec![
        "--die-with-parent".to_string(),
        "--new-session".to_string(),
        "--proc".to_string(),
        "/proc".to_string(),
        "--dev".to_string(),
        "/dev".to_string(),
        "--ro-bind".to_string(),
        "/".to_string(),
        "/".to_string(),
        "--tmpfs".to_string(),
        "/tmp".to_string(),
        "--tmpfs".to_string(),
        "/var/tmp".to_string(),
    ];
    if !policy.allow_network {
        args.push("--unshare-net".to_string());
    }
    args.push("--chdir".to_string());
    args.push(spec.cwd.to_string_lossy().to_string());

    if policy.mode == ShellSandboxMode::WorkspaceWrite {
        let mut roots = policy.writable_roots.clone();
        if roots.is_empty() {
            roots.push(spec.cwd.clone());
        }
        for root in roots {
            let resolved = resolve_root_path(&spec.cwd, &root);
            let resolved_str = resolved.to_string_lossy().to_string();
            args.push("--bind".to_string());
            args.push(resolved_str.clone());
            args.push(resolved_str);
        }
    }

    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sandbox_mode() {
        assert_eq!(
            ShellSandboxMode::from_str("workspace_write"),
            Some(ShellSandboxMode::WorkspaceWrite)
        );
        assert_eq!(
            ShellSandboxMode::from_str("read_only"),
            Some(ShellSandboxMode::ReadOnly)
        );
        assert_eq!(
            ShellSandboxMode::from_str("none"),
            Some(ShellSandboxMode::None)
        );
        assert_eq!(ShellSandboxMode::from_str("invalid"), None);
    }

    #[test]
    fn test_parse_sandbox_backend() {
        assert_eq!(
            ShellSandboxBackendKind::from_str("auto"),
            Some(ShellSandboxBackendKind::Auto)
        );
        assert_eq!(
            ShellSandboxBackendKind::from_str("seatbelt"),
            Some(ShellSandboxBackendKind::MacosSeatbelt)
        );
        assert_eq!(
            ShellSandboxBackendKind::from_str("linux_seccomp"),
            Some(ShellSandboxBackendKind::LinuxSeccomp)
        );
        assert_eq!(
            ShellSandboxBackendKind::from_str("windows"),
            Some(ShellSandboxBackendKind::WindowsRestricted)
        );
        assert_eq!(ShellSandboxBackendKind::from_str("bad"), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_macos_profile_contains_write_root() {
        let cwd = PathBuf::from(".");
        let policy = ShellSandboxPolicy {
            mode: ShellSandboxMode::WorkspaceWrite,
            backend: ShellSandboxBackendKind::Auto,
            allow_network: false,
            writable_roots: vec![PathBuf::from(".")],
            linux_bwrap_path: None,
        };
        let profile = build_macos_profile(&cwd, &policy);
        assert!(profile.contains("file-write*"));
        assert!(profile.contains("(deny default)"));
    }

    #[test]
    fn test_none_mode_passthrough_command() {
        let policy = ShellSandboxPolicy::default();
        let output = sandbox_command(
            "echo".to_string(),
            vec!["hello".to_string()],
            Path::new("."),
            &policy,
        )
        .expect("sandbox command");
        assert_eq!(output.program, "echo");
        assert_eq!(output.args, vec!["hello".to_string()]);
        assert_eq!(output.backend, "none");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_bwrap_args_contain_expected_flags() {
        let spec = SandboxCommandSpec {
            program: "echo".to_string(),
            args: vec!["ok".to_string()],
            cwd: PathBuf::from("."),
            env: HashMap::new(),
        };
        let policy = ShellSandboxPolicy {
            mode: ShellSandboxMode::WorkspaceWrite,
            backend: ShellSandboxBackendKind::LinuxSeccomp,
            allow_network: false,
            writable_roots: vec![PathBuf::from(".")],
            linux_bwrap_path: None,
        };
        let args = build_linux_bwrap_args(&spec, &policy);
        assert!(args.iter().any(|v| v == "--unshare-net"));
        assert!(args.iter().any(|v| v == "--bind"));
        assert!(args.iter().any(|v| v == "--chdir"));
    }
}
