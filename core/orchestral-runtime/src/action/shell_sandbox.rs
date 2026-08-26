use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ShellSandboxPolicy {
    pub writable_roots: Vec<PathBuf>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub linux_bwrap_path: Option<PathBuf>,
}

impl Default for ShellSandboxPolicy {
    fn default() -> Self {
        Self {
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
    let env = HashMap::from([(
        "ORCHESTRAL_SANDBOX_NETWORK_DISABLED".to_owned(),
        "1".to_owned(),
    )]);
    let backend = default_backend_for_platform();
    let spec = SandboxCommandSpec {
        program,
        args,
        cwd: cwd.to_path_buf(),
        env,
    };
    backend.transform(spec, policy).map_err(|error| {
        format!(
            "{error} (backend={}, mode=workspace_write)",
            backend.backend_name()
        )
    })
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
    profile.push_str("(allow file-read* (literal \"/dev/null\"))\n");
    profile.push_str("(allow file-write* (literal \"/dev/null\"))\n");
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
    args.push("--unshare-net".to_string());
    args.push("--chdir".to_string());
    args.push(spec.cwd.to_string_lossy().to_string());

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

    args
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn test_macos_profile_contains_write_root() {
        let cwd = PathBuf::from(".");
        let policy = ShellSandboxPolicy {
            writable_roots: vec![PathBuf::from(".")],
            linux_bwrap_path: None,
        };
        let profile = build_macos_profile(&cwd, &policy);
        assert!(profile.contains("file-write*"));
        assert!(profile.contains("(deny default)"));
        assert!(profile.contains("(literal \"/dev/null\")"));
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
            writable_roots: vec![PathBuf::from(".")],
            linux_bwrap_path: None,
        };
        let args = build_linux_bwrap_args(&spec, &policy);
        assert!(args.iter().any(|v| v == "--unshare-net"));
        assert!(args.iter().any(|v| v == "--bind"));
        assert!(args.iter().any(|v| v == "--chdir"));
    }
}
