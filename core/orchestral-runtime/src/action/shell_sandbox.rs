use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct ShellSandboxPolicy {
    pub readable_roots: Vec<PathBuf>,
    pub writable_roots: Vec<PathBuf>,
    pub allowed_programs: Vec<PathBuf>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub linux_bwrap_path: Option<PathBuf>,
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

        let profile = build_macos_profile(&spec, policy);
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
    let backend = default_backend_for_platform();
    sandbox_command_with_backend(program, args, cwd, policy, backend.as_ref())
}

fn sandbox_command_with_backend(
    program: String,
    args: Vec<String>,
    cwd: &Path,
    policy: &ShellSandboxPolicy,
    backend: &dyn ShellSandboxBackend,
) -> Result<SandboxedCommand, String> {
    let (program, cwd, policy) = normalize_sandbox_inputs(&program, cwd, policy)?;
    let env = HashMap::from([(
        "ORCHESTRAL_SANDBOX_NETWORK_DISABLED".to_owned(),
        "1".to_owned(),
    )]);
    let spec = SandboxCommandSpec {
        program,
        args,
        cwd,
        env,
    };
    backend.transform(spec, &policy).map_err(|error| {
        format!(
            "{error} (backend={}, mode=workspace_write)",
            backend.backend_name()
        )
    })
}

fn normalize_sandbox_inputs(
    program: &str,
    cwd: &Path,
    policy: &ShellSandboxPolicy,
) -> Result<(String, PathBuf, ShellSandboxPolicy), String> {
    let program = canonical_file(Path::new(program), "sandbox executable")?;
    let cwd = canonical_directory(cwd, "sandbox cwd")?;
    let readable_roots = canonical_directories(&policy.readable_roots, "readable root")?;
    let writable_roots = canonical_directories(&policy.writable_roots, "writable root")?;
    let allowed_programs = policy
        .allowed_programs
        .iter()
        .map(|path| canonical_file(path, "allowed executable"))
        .collect::<Result<Vec<_>, _>>()?;

    if readable_roots.is_empty()
        || writable_roots.is_empty()
        || allowed_programs.is_empty()
        || !allowed_programs.contains(&program)
        || !writable_roots.iter().any(|root| cwd.starts_with(root))
    {
        return Err(
            "sandbox requires canonical read/write roots, an allowed executable, and a writable cwd"
                .to_owned(),
        );
    }

    Ok((
        program.to_string_lossy().into_owned(),
        cwd,
        ShellSandboxPolicy {
            readable_roots,
            writable_roots,
            allowed_programs,
            linux_bwrap_path: policy.linux_bwrap_path.clone(),
        },
    ))
}

fn canonical_directories(paths: &[PathBuf], label: &str) -> Result<Vec<PathBuf>, String> {
    paths
        .iter()
        .map(|path| canonical_directory(path, label))
        .collect::<Result<BTreeSet<_>, _>>()
        .map(BTreeSet::into_iter)
        .map(Iterator::collect)
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf, String> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|error| format!("canonicalize {label} '{}' failed: {error}", path.display()))?;
    if !canonical.is_dir() {
        return Err(format!("{label} '{}' is not a directory", path.display()));
    }
    Ok(canonical)
}

fn canonical_file(path: &Path, label: &str) -> Result<PathBuf, String> {
    let canonical = std::fs::canonicalize(path)
        .map_err(|error| format!("canonicalize {label} '{}' failed: {error}", path.display()))?;
    if !canonical.is_file() {
        return Err(format!("{label} '{}' is not a file", path.display()));
    }
    Ok(canonical)
}

#[cfg(target_os = "macos")]
fn build_macos_profile(spec: &SandboxCommandSpec, policy: &ShellSandboxPolicy) -> String {
    let mut profile = String::new();
    profile.push_str("(version 1)\n");
    profile.push_str("(deny default)\n");
    profile.push_str("(allow process-fork)\n");
    for program in &policy.allowed_programs {
        profile.push_str(&format!(
            "(allow process-exec (literal \"{}\"))\n",
            escape_profile_string(&program.to_string_lossy())
        ));
    }
    profile.push_str("(allow sysctl-read)\n");

    let mut literal_reads = BTreeSet::new();
    let mut subtree_reads = BTreeSet::new();
    for path in [
        Path::new("/usr/lib"),
        Path::new("/System/Library"),
        Path::new("/System/Volumes/Preboot/Cryptexes/OS/usr/lib"),
        Path::new("/System/Volumes/Preboot/Cryptexes/OS/System/Library"),
    ] {
        add_path_ancestors(path, &mut literal_reads);
        subtree_reads.insert(path.to_path_buf());
    }
    for path in [Path::new("/usr/bin/sandbox-exec"), Path::new("/dev/null")] {
        add_path_ancestors(path, &mut literal_reads);
        literal_reads.insert(path.to_path_buf());
    }
    for program in &policy.allowed_programs {
        add_path_ancestors(program, &mut literal_reads);
        literal_reads.insert(program.clone());
    }
    for root in &policy.readable_roots {
        add_path_ancestors(root, &mut literal_reads);
        literal_reads.insert(root.clone());
        subtree_reads.insert(root.clone());
    }
    add_path_ancestors(&spec.cwd, &mut literal_reads);
    literal_reads.insert(spec.cwd.clone());

    for path in literal_reads {
        profile.push_str(&format!(
            "(allow file-read* (literal \"{}\"))\n",
            escape_profile_string(&path.to_string_lossy())
        ));
    }
    for path in subtree_reads {
        profile.push_str(&format!(
            "(allow file-read* (subpath \"{}\"))\n",
            escape_profile_string(&path.to_string_lossy())
        ));
    }
    profile.push_str("(allow file-write* (literal \"/dev/null\"))\n");
    for root in &policy.writable_roots {
        profile.push_str(&format!(
            "(allow file-write* (subpath \"{}\"))\n",
            escape_profile_string(&root.to_string_lossy())
        ));
    }
    profile
}

#[cfg(target_os = "macos")]
fn add_path_ancestors(path: &Path, paths: &mut BTreeSet<PathBuf>) {
    for ancestor in path.ancestors().skip(1) {
        paths.insert(ancestor.to_path_buf());
    }
}

#[cfg(target_os = "macos")]
fn escape_profile_string(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(target_os = "linux")]
fn resolve_linux_bwrap_executable(policy: &ShellSandboxPolicy) -> Result<PathBuf, String> {
    if let Some(path) = &policy.linux_bwrap_path {
        return canonical_file(path, "configured bubblewrap executable");
    }

    for candidate in [
        "/usr/bin/bwrap",
        "/bin/bwrap",
        "/usr/bin/bubblewrap",
        "/bin/bubblewrap",
    ] {
        if let Ok(path) = canonical_file(Path::new(candidate), "system bubblewrap executable") {
            return Ok(path);
        }
    }
    Err(
        "trusted bubblewrap executable not found in system paths; set config.sandbox_linux_bwrap_path"
            .to_string(),
    )
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
        "--tmpfs".to_string(),
        "/tmp".to_string(),
        "--tmpfs".to_string(),
        "/var/tmp".to_string(),
    ];
    args.push("--unshare-net".to_string());
    args.push("--chdir".to_string());
    args.push(spec.cwd.to_string_lossy().to_string());

    for runtime_path in [
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/etc/ld.so.cache",
    ] {
        if Path::new(runtime_path).exists() {
            push_bwrap_bind(&mut args, "--ro-bind", Path::new(runtime_path));
        }
    }
    for program in &policy.allowed_programs {
        push_bwrap_bind(&mut args, "--ro-bind", program);
    }
    for root in &policy.readable_roots {
        push_bwrap_bind(&mut args, "--ro-bind", root);
    }
    for root in &policy.writable_roots {
        push_bwrap_bind(&mut args, "--bind", root);
    }

    args
}

#[cfg(target_os = "linux")]
fn push_bwrap_bind(args: &mut Vec<String>, operation: &str, path: &Path) {
    let path = path.to_string_lossy().into_owned();
    args.push(operation.to_owned());
    args.push(path.clone());
    args.push(path);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    fn run_sandboxed(command: SandboxedCommand, cwd: &Path) -> std::process::Output {
        let mut process = std::process::Command::new(command.program);
        process
            .args(command.args)
            .env_clear()
            .envs(command.env)
            .current_dir(cwd);
        process.output().unwrap()
    }

    #[cfg(target_os = "macos")]
    fn isolated_test_roots(label: &str) -> (PathBuf, PathBuf, PathBuf) {
        let parent = std::env::temp_dir().join(format!(
            "orchestral-sandbox-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        let workspace = parent.join("workspace");
        let outside = parent.join("outside");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let parent = std::fs::canonicalize(parent).unwrap();
        let workspace = std::fs::canonicalize(workspace).unwrap();
        let outside = std::fs::canonicalize(outside).unwrap();
        (parent, workspace, outside)
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_macos_profile_contains_write_root() {
        let cwd = std::fs::canonicalize(".").unwrap();
        let program = std::fs::canonicalize("/bin/echo").unwrap();
        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: vec!["ok".to_owned()],
            cwd: cwd.clone(),
            env: HashMap::new(),
        };
        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            writable_roots: vec![cwd.clone()],
            allowed_programs: vec![program.clone()],
            linux_bwrap_path: None,
        };
        let profile = build_macos_profile(&spec, &policy);
        assert!(profile.contains("file-write*"));
        assert!(profile.contains("(deny default)"));
        assert!(profile.contains("(literal \"/dev/null\")"));
        assert!(profile.contains(&format!(
            "(allow process-exec (literal \"{}\"))",
            program.to_string_lossy()
        )));
        assert!(!profile.contains("(allow process*)"));
        assert!(!profile.contains("(allow file-read*)\n"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn one_thousand_outside_secret_reads_and_symlink_escape_are_denied() {
        const ATTEMPTS: usize = 1_000;

        let (parent, workspace, outside) = isolated_test_roots("secret-read");
        let mut secret_paths = Vec::with_capacity(ATTEMPTS + 1);
        for index in 0..ATTEMPTS {
            let path = outside.join(format!("secret-{index}.txt"));
            std::fs::write(&path, format!("ORCHESTRAL_SENTINEL_SECRET_{index}")).unwrap();
            secret_paths.push(path.to_string_lossy().into_owned());
        }
        let symlink_target = outside.join("symlink-secret.txt");
        std::fs::write(&symlink_target, "ORCHESTRAL_SENTINEL_SYMLINK").unwrap();
        let symlink = workspace.join("escape-link.txt");
        std::os::unix::fs::symlink(&symlink_target, &symlink).unwrap();
        secret_paths.push(symlink.to_string_lossy().into_owned());

        let program = std::fs::canonicalize("/bin/cat").unwrap();
        let command = sandbox_command(
            program.to_string_lossy().into_owned(),
            secret_paths,
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![workspace.clone()],
                writable_roots: vec![workspace.clone()],
                allowed_programs: vec![program],
                linux_bwrap_path: None,
            },
        )
        .unwrap();
        let output = run_sandboxed(command, &workspace);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(!output.status.success());
        assert!(!stdout.contains("ORCHESTRAL_SENTINEL_SECRET_"));
        assert!(!stdout.contains("ORCHESTRAL_SENTINEL_SYMLINK"));

        std::fs::remove_dir_all(parent).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn allowed_parent_cannot_spawn_an_unlisted_program() {
        let (parent, workspace, _) = isolated_test_roots("alternate-spawn");
        let program = std::fs::canonicalize("/bin/bash").unwrap();
        let command = sandbox_command(
            program.to_string_lossy().into_owned(),
            vec![
                "--noprofile".to_owned(),
                "--norc".to_owned(),
                "-c".to_owned(),
                "/bin/echo ORCHESTRAL_SENTINEL_ALTERNATE_SPAWN".to_owned(),
            ],
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![workspace.clone()],
                writable_roots: vec![workspace.clone()],
                allowed_programs: vec![program],
                linux_bwrap_path: None,
            },
        )
        .unwrap();
        let output = run_sandboxed(command, &workspace);
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stdout)
            .contains("ORCHESTRAL_SENTINEL_ALTERNATE_SPAWN"));

        std::fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn one_thousand_unavailable_backend_attempts_fail_closed_without_a_bare_command() {
        let cwd = std::fs::canonicalize(".").unwrap();
        let program = if cfg!(windows) {
            std::env::current_exe().unwrap()
        } else {
            std::fs::canonicalize("/bin/echo").unwrap()
        };
        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            writable_roots: vec![cwd.clone()],
            allowed_programs: vec![program.clone()],
            linux_bwrap_path: None,
        };
        let unavailable = UnsupportedBackend {
            backend_name: "test_unavailable",
            reason: "injected backend outage",
        };
        for index in 0..1_000 {
            let error = sandbox_command_with_backend(
                program.to_string_lossy().into_owned(),
                vec![format!("must-not-run-{index}")],
                &cwd,
                &policy,
                &unavailable,
            )
            .expect_err("required sandbox outage must never return a bare command");
            assert!(error.contains("Sandbox backend 'test_unavailable' is unavailable"));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_bwrap_args_contain_expected_flags() {
        let cwd = std::fs::canonicalize(".").unwrap();
        let program = std::fs::canonicalize("/bin/echo").unwrap();
        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: vec!["ok".to_string()],
            cwd: cwd.clone(),
            env: HashMap::new(),
        };
        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            writable_roots: vec![cwd],
            allowed_programs: vec![program],
            linux_bwrap_path: None,
        };
        let args = build_linux_bwrap_args(&spec, &policy);
        assert!(args.iter().any(|v| v == "--unshare-net"));
        assert!(args.iter().any(|v| v == "--bind"));
        assert!(args.iter().any(|v| v == "--chdir"));
        assert!(!args
            .windows(3)
            .any(|window| { window[0] == "--ro-bind" && window[1] == "/" && window[2] == "/" }));
    }
}
