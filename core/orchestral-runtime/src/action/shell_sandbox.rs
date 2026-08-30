use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

/// Network boundary that the selected OS sandbox must materialize exactly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SandboxNetworkAccess {
    #[default]
    Disabled,
    ExactTargets(BTreeSet<String>),
    Unrestricted,
}

impl SandboxNetworkAccess {
    pub fn is_disabled(&self) -> bool {
        matches!(self, Self::Disabled)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ShellSandboxPolicy {
    pub readable_roots: Vec<PathBuf>,
    /// Host-owned files required by a toolchain at runtime. These are exposed
    /// as exact read-only paths, never as readable parent directories.
    pub readable_files: Vec<PathBuf>,
    pub writable_roots: Vec<PathBuf>,
    /// Allow the already-sandboxed launcher to execute child programs.
    /// Filesystem and network effects remain constrained by this profile.
    pub allow_child_processes: bool,
    /// Allow an explicitly trusted integration process to ask the Host OS to
    /// open a URL or application UI. Ordinary Agent shell commands leave this
    /// disabled.
    pub allow_host_ui: bool,
    pub launcher_programs: Vec<PathBuf>,
    pub network: SandboxNetworkAccess,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub linux_bwrap_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct SandboxedCommand {
    pub program: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub backend: &'static str,
    /// The sandbox launcher establishes a fresh session/process group itself.
    /// Callers must not make that launcher a process-group leader before it
    /// executes, because doing so makes a subsequent `setsid` fail.
    pub backend_starts_new_session: bool,
}

#[derive(Debug, Clone)]
pub struct SandboxCommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
}

pub(crate) trait ShellSandboxBackend {
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

        let profile = build_macos_profile(&spec, policy)?;
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
            backend_starts_new_session: false,
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
        if matches!(policy.network, SandboxNetworkAccess::ExactTargets(_)) {
            return Err(
                "target-restricted network is unavailable in the bubblewrap adapter; refusing to widen network access"
                    .to_owned(),
            );
        }
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
            backend_starts_new_session: true,
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
        return Box::new(crate::tools::windows_sandbox::WindowsRestrictedBackend::default());
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
        if policy.network.is_disabled() {
            "1".to_owned()
        } else {
            "0".to_owned()
        },
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
    let launch_program = PathBuf::from(program);
    if !launch_program.is_absolute() {
        return Err("sandbox executable must be a Host-resolved absolute path".to_owned());
    }
    let program_identity = canonical_file(&launch_program, "sandbox executable")?;
    let cwd = canonical_directory(cwd, "sandbox cwd")?;
    let readable_roots = canonical_directories(&policy.readable_roots, "readable root")?;
    let readable_files = canonical_files(&policy.readable_files, "readable file")?;
    let writable_roots = canonical_directories(&policy.writable_roots, "writable root")?;
    let launcher_programs = policy
        .launcher_programs
        .iter()
        .map(|path| canonical_file(path, "launcher executable"))
        .collect::<Result<Vec<_>, _>>()?;
    let network = normalize_network_access(&policy.network)?;

    if readable_roots.is_empty()
        || writable_roots.is_empty()
        || launcher_programs.is_empty()
        || !launcher_programs.contains(&program_identity)
        || !readable_roots.iter().any(|root| cwd.starts_with(root))
    {
        return Err(
            "sandbox requires canonical read/write roots, an allowed executable, and a readable cwd"
                .to_owned(),
        );
    }

    Ok((
        launch_program.to_string_lossy().into_owned(),
        cwd,
        ShellSandboxPolicy {
            readable_roots,
            readable_files,
            writable_roots,
            allow_child_processes: policy.allow_child_processes,
            allow_host_ui: policy.allow_host_ui,
            launcher_programs,
            network,
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

fn canonical_files(paths: &[PathBuf], label: &str) -> Result<Vec<PathBuf>, String> {
    paths
        .iter()
        .map(|path| canonical_file(path, label))
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

pub(crate) fn normalize_network_targets(
    targets: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    targets
        .iter()
        .map(|target| {
            let target = target.trim();
            let (host, port) = target
                .rsplit_once(':')
                .ok_or_else(|| format!("network target must use host:port syntax: {target}"))?;
            let host = host.trim_matches(['[', ']']);
            if host.is_empty()
                || !host.chars().all(|character| {
                    character.is_ascii_alphanumeric() || ".-_:".contains(character)
                })
            {
                return Err(format!("network target has an invalid host: {target}"));
            }
            if port.parse::<u16>().ok().filter(|port| *port > 0).is_none() {
                return Err(format!("network target has an invalid port: {target}"));
            }
            let host =
                if host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" || host == "::1" {
                    "localhost"
                } else {
                    host
                };
            Ok(format!("{host}:{port}"))
        })
        .collect()
}

fn normalize_network_access(access: &SandboxNetworkAccess) -> Result<SandboxNetworkAccess, String> {
    match access {
        SandboxNetworkAccess::Disabled => Ok(SandboxNetworkAccess::Disabled),
        SandboxNetworkAccess::ExactTargets(targets) => {
            normalize_network_targets(targets).map(SandboxNetworkAccess::ExactTargets)
        }
        SandboxNetworkAccess::Unrestricted => Ok(SandboxNetworkAccess::Unrestricted),
    }
}

#[cfg(target_os = "macos")]
fn build_macos_profile(
    spec: &SandboxCommandSpec,
    policy: &ShellSandboxPolicy,
) -> Result<String, String> {
    let mut profile = String::new();
    profile.push_str("(version 1)\n");
    profile.push_str("(deny default)\n");
    profile.push_str("(allow process-fork)\n");
    if policy.allow_child_processes {
        profile.push_str("(allow process-exec)\n");
    } else {
        for program in &policy.launcher_programs {
            profile.push_str(&format!(
                "(allow process-exec (literal \"{}\"))\n",
                escape_profile_string(&program.to_string_lossy())
            ));
        }
    }
    match &policy.network {
        SandboxNetworkAccess::Disabled => {}
        SandboxNetworkAccess::Unrestricted => {
            profile.push_str("(allow network-outbound)\n");
            // OAuth-style local MCP authentication needs an ephemeral loopback
            // callback listener. Keep inbound authority on localhost even when
            // outbound access is unrestricted.
            profile.push_str("(allow network-bind (local ip \"localhost:*\"))\n");
            profile.push_str("(allow network-inbound (local ip \"localhost:*\"))\n");
        }
        SandboxNetworkAccess::ExactTargets(targets) => {
            for target in targets {
                let (host, _) = target
                    .rsplit_once(':')
                    .expect("normalized network target has a host and port");
                if host != "localhost" {
                    return Err(format!(
                        "exact remote network target '{target}' requires a managed proxy on macOS; refusing broader network access"
                    ));
                }
                profile.push_str(&format!(
                    "(allow network-outbound (remote ip \"{}\"))\n",
                    escape_profile_string(target)
                ));
            }
        }
    }
    if !policy.network.is_disabled() {
        // Socket permission alone is insufficient on macOS. Native TLS and
        // DNS clients consult these Host services through Mach and AF_SYSTEM;
        // denying them surfaces only as a generic HTTP transport error.
        profile.push_str(
            r#"(allow system-socket
  (require-all
    (socket-domain AF_SYSTEM)
    (socket-protocol 2)))
(allow mach-lookup
  (global-name "com.apple.bsd.dirhelper")
  (global-name "com.apple.system.opendirectoryd.membership")
  (global-name "com.apple.SecurityServer")
  (global-name "com.apple.networkd")
  (global-name "com.apple.ocspd")
  (global-name "com.apple.trustd.agent")
  (global-name "com.apple.SystemConfiguration.DNSConfiguration")
  (global-name "com.apple.SystemConfiguration.configd"))
"#,
        );
    }
    if policy.allow_host_ui {
        // `/usr/bin/open` delegates URL/application activation to
        // LaunchServices. This grants that OS service boundary only; it does
        // not move OAuth state or protocol handling into the Agent Host.
        profile.push_str(
            r#"(with-filter (process-path "/usr/bin/open")
  (allow file-read*))
(with-filter (process-path "/usr/bin/open")
  (allow ipc-posix-shm-read* (ipc-posix-name-prefix "apple.cfprefs.")))
(with-filter (process-path "/usr/bin/open")
  (allow mach-lookup
    (global-name "com.apple.cfprefsd.daemon")
    (global-name "com.apple.cfprefsd.agent")
    (local-name "com.apple.cfprefsd.agent")
    (global-name "com.apple.coreservices.launchservicesd")
    (global-name "com.apple.coreservices.appleevents")
    (global-name "com.apple.coreservices.quarantine-resolver")
    (global-name "com.apple.lsd.mapdb")))
(with-filter (process-path "/usr/bin/open")
  (allow user-preference-read))
(with-filter (process-path "/usr/bin/open")
  (allow appleevent-send))
(with-filter (process-path "/usr/bin/open")
  (allow lsopen))
"#,
        );
    }
    profile.push_str("(allow sysctl-read)\n");

    let mut literal_reads = BTreeSet::new();
    let mut subtree_reads = BTreeSet::new();
    for path in [
        Path::new("/usr/lib"),
        Path::new("/usr/share"),
        Path::new("/System/Library"),
        Path::new("/System/Cryptexes"),
        Path::new("/System/Volumes/Preboot/Cryptexes/OS/usr/lib"),
        Path::new("/System/Volumes/Preboot/Cryptexes/OS/System/Library"),
        // Xcode command-line drivers load Apple-owned developer frameworks
        // from these system locations even when the selected SDK lives under
        // `/Applications/Xcode.app`.
        Path::new("/Library/Developer"),
        Path::new("/Library/Apple/System/Library"),
        Path::new("/etc"),
        Path::new("/private/etc"),
    ] {
        add_path_ancestors(path, &mut literal_reads);
        subtree_reads.insert(path.to_path_buf());
    }
    if policy.allow_host_ui {
        // LaunchServices resolves bundle registrations through the standard
        // application roots. These are system application bundles, not the
        // user's documents or home directory.
        for path in [
            Path::new("/Applications"),
            Path::new("/System/Applications"),
        ] {
            add_path_ancestors(path, &mut literal_reads);
            subtree_reads.insert(path.to_path_buf());
        }
    }
    for path in [
        Path::new("/usr/bin/sandbox-exec"),
        Path::new("/dev/null"),
        // Apple toolchains resolve the active developer directory through this
        // Host-owned selector before reading SDKs under the approved Xcode root.
        // Seatbelt observes the physical `/private/var` path while the tool
        // reports the public `/var` alias, so both exact identities are needed.
        Path::new("/var/select/developer_dir"),
        Path::new("/private/var/select/developer_dir"),
        // macOS resolves the system `sh` implementation through the same
        // selector mechanism when compiler drivers launch helper scripts.
        Path::new("/var/select/sh"),
        Path::new("/private/var/select/sh"),
        // xcrun/clang consult this non-secret Host preference to confirm the
        // installed Xcode SDK license before linking.
        Path::new("/Library/Preferences/com.apple.dt.Xcode.plist"),
        Path::new("/Library/Preferences/.GlobalPreferences.plist"),
    ] {
        add_path_ancestors(path, &mut literal_reads);
        literal_reads.insert(path.to_path_buf());
    }
    for program in &policy.launcher_programs {
        add_path_ancestors(program, &mut literal_reads);
        literal_reads.insert(program.clone());
    }
    if policy.allow_host_ui {
        let open = Path::new("/usr/bin/open");
        add_path_ancestors(open, &mut literal_reads);
        literal_reads.insert(open.to_path_buf());
    }
    let launch_program = Path::new(&spec.program);
    add_path_ancestors(launch_program, &mut literal_reads);
    literal_reads.insert(launch_program.to_path_buf());
    for root in &policy.readable_roots {
        add_path_ancestors(root, &mut literal_reads);
        literal_reads.insert(root.clone());
        subtree_reads.insert(root.clone());
    }
    for file in &policy.readable_files {
        add_path_ancestors(file, &mut literal_reads);
        literal_reads.insert(file.clone());
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
    Ok(profile)
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
    if policy.network.is_disabled() {
        args.push("--unshare-net".to_string());
    }
    args.push("--chdir".to_string());
    args.push(spec.cwd.to_string_lossy().to_string());

    for runtime_path in [
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        // GCC keeps compiler helpers (collect2, lto-wrapper, and on Debian/
        // Ubuntu the linker plugin it selects) under /usr/libexec. Exposing
        // only shared-library directories makes an otherwise available host
        // toolchain fail after it enters the sandbox.
        "/usr/libexec",
        "/etc/ld.so.cache",
    ] {
        if Path::new(runtime_path).exists() {
            push_bwrap_bind(&mut args, "--ro-bind", Path::new(runtime_path));
        }
    }
    for program in &policy.launcher_programs {
        push_bwrap_bind(&mut args, "--ro-bind", program);
    }
    let launch_program = Path::new(&spec.program);
    if !policy
        .launcher_programs
        .iter()
        .any(|program| program == launch_program)
    {
        push_bwrap_bind(&mut args, "--ro-bind", launch_program);
    }
    for root in &policy.readable_roots {
        push_bwrap_bind(&mut args, "--ro-bind", root);
    }
    for file in &policy.readable_files {
        push_bwrap_bind(&mut args, "--ro-bind", file);
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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn run_sandboxed(command: SandboxedCommand, cwd: &Path) -> std::process::Output {
        let mut process = std::process::Command::new(command.program);
        process
            .args(command.args)
            .env_clear()
            .envs(command.env)
            .current_dir(cwd);
        process.output().unwrap()
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
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

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn sandbox_cwd_may_be_read_only_when_writes_use_a_separate_runtime_root() {
        let (parent, workspace, runtime_root) = isolated_test_roots("read-only-cwd");
        let executable = std::fs::canonicalize("/bin/echo").unwrap();
        let normalized = normalize_sandbox_inputs(
            &executable.to_string_lossy(),
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![workspace.clone(), runtime_root.clone()],
                readable_files: Vec::new(),
                writable_roots: vec![runtime_root],
                allow_child_processes: false,
                allow_host_ui: false,
                launcher_programs: vec![executable.clone()],
                network: SandboxNetworkAccess::Disabled,
                linux_bwrap_path: None,
            },
        );
        assert!(normalized.is_ok(), "{normalized:?}");
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn sandbox_reads_an_exact_host_file_without_exposing_its_sibling() {
        let (parent, workspace, outside) = isolated_test_roots("exact-readable-file");
        let allowed = outside.join("allowed.txt");
        let denied = outside.join("denied.txt");
        std::fs::write(&allowed, "ORCHESTRAL_ALLOWED_FILE").unwrap();
        std::fs::write(&denied, "ORCHESTRAL_DENIED_SIBLING").unwrap();
        let program = std::fs::canonicalize("/bin/cat").unwrap();
        let command = sandbox_command(
            program.to_string_lossy().into_owned(),
            vec![
                allowed.to_string_lossy().into_owned(),
                denied.to_string_lossy().into_owned(),
            ],
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![workspace.clone()],
                readable_files: vec![allowed],
                writable_roots: vec![workspace.clone()],
                allow_child_processes: false,
                allow_host_ui: false,
                launcher_programs: vec![program],
                network: SandboxNetworkAccess::Disabled,
                linux_bwrap_path: None,
            },
        )
        .unwrap();

        let output = run_sandboxed(command, &workspace);
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("ORCHESTRAL_ALLOWED_FILE"), "{stdout}");
        assert!(!stdout.contains("ORCHESTRAL_DENIED_SIBLING"), "{stdout}");
        assert!(
            !output.status.success(),
            "the sibling read unexpectedly worked"
        );
        std::fs::remove_dir_all(parent).unwrap();
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
            readable_files: Vec::new(),
            writable_roots: vec![cwd.clone()],
            allow_child_processes: false,
            allow_host_ui: false,
            launcher_programs: vec![program.clone()],
            network: SandboxNetworkAccess::Disabled,
            linux_bwrap_path: None,
        };
        let profile = build_macos_profile(&spec, &policy).unwrap();
        assert!(profile.contains("file-write*"));
        assert!(profile.contains("(deny default)"));
        assert!(profile.contains("(literal \"/dev/null\")"));
        assert!(profile.contains("(literal \"/var/select/developer_dir\")"));
        assert!(profile.contains("(literal \"/private/var/select/developer_dir\")"));
        assert!(profile.contains("(literal \"/var/select/sh\")"));
        assert!(profile.contains("(literal \"/private/var/select/sh\")"));
        assert!(profile.contains("(literal \"/Library/Preferences/com.apple.dt.Xcode.plist\")"));
        assert!(profile.contains(&format!(
            "(allow process-exec (literal \"{}\"))",
            program.to_string_lossy()
        )));
        assert!(!profile.contains("(allow process*)"));
        assert!(!profile.contains("(allow file-read*)\n"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn host_resolved_executable_symlink_keeps_its_launch_identity() {
        let (parent, workspace, _) = isolated_test_roots("executable-symlink");
        let executable = std::fs::canonicalize("/bin/echo").unwrap();
        let launch_path = workspace.join("echo-alias");
        std::os::unix::fs::symlink(&executable, &launch_path).unwrap();
        let command = sandbox_command(
            launch_path.to_string_lossy().into_owned(),
            vec!["SYMLINK_LAUNCH_OK".to_owned()],
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![workspace.clone()],
                readable_files: Vec::new(),
                writable_roots: vec![workspace.clone()],
                allow_child_processes: false,
                allow_host_ui: false,
                launcher_programs: vec![executable],
                network: SandboxNetworkAccess::Disabled,
                linux_bwrap_path: None,
            },
        )
        .unwrap();

        let output = run_sandboxed(command, &workspace);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            "SYMLINK_LAUNCH_OK"
        );
        std::fs::remove_dir_all(parent).unwrap();
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
                readable_files: Vec::new(),
                writable_roots: vec![workspace.clone()],
                allow_child_processes: false,
                allow_host_ui: false,
                launcher_programs: vec![program],
                network: SandboxNetworkAccess::Disabled,
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
                readable_files: Vec::new(),
                writable_roots: vec![workspace.clone()],
                allow_child_processes: false,
                allow_host_ui: false,
                launcher_programs: vec![program],
                network: SandboxNetworkAccess::Disabled,
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

    #[cfg(target_os = "macos")]
    #[test]
    fn one_thousand_command_write_escapes_change_zero_outside_files() {
        const ATTEMPTS: usize = 1_000;

        let (parent, workspace, outside) = isolated_test_roots("write-escape");
        let child_inside = workspace.join("child-inside.txt");
        let child_outside = outside.join("child-outside.txt");
        let mut command_text = format!(
            "printf created > '{0}/inside.txt'; printf updated >> '{0}/inside.txt'; rm '{0}/inside.txt'\n/bin/sh -c \"printf child-ok > '{1}'\"\n/bin/sh -c \"printf child-escaped > '{2}'\" || true\n",
            workspace.display(),
            child_inside.display(),
            child_outside.display(),
        );
        let mut outside_files = Vec::with_capacity(ATTEMPTS);
        for index in 0..ATTEMPTS {
            let outside_file = outside.join(format!("outside-{index}.txt"));
            std::fs::write(&outside_file, format!("ORIGINAL-{index}")).unwrap();
            outside_files.push(outside_file.clone());
            let target = match index % 3 {
                0 => outside_file,
                1 => workspace
                    .join("..")
                    .join("outside")
                    .join(format!("outside-{index}.txt")),
                _ => {
                    let link = workspace.join(format!("escape-{index}.txt"));
                    std::os::unix::fs::symlink(&outside_file, &link).unwrap();
                    link
                }
            };
            command_text.push_str(&format!(
                "if printf MUTATED > '{}'; then printf ESCAPED; fi\n",
                target.display()
            ));
        }
        let program = std::fs::canonicalize("/bin/sh").unwrap();
        let command = sandbox_command(
            program.to_string_lossy().into_owned(),
            vec!["-c".to_owned(), command_text],
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![
                    workspace.clone(),
                    std::fs::canonicalize("/bin").unwrap(),
                    std::fs::canonicalize("/usr/bin").unwrap(),
                ],
                readable_files: Vec::new(),
                writable_roots: vec![workspace.clone()],
                allow_child_processes: true,
                allow_host_ui: false,
                launcher_programs: vec![program],
                network: SandboxNetworkAccess::Disabled,
                linux_bwrap_path: None,
            },
        )
        .unwrap();
        let output = run_sandboxed(command, &workspace);
        assert!(!String::from_utf8_lossy(&output.stdout).contains("ESCAPED"));
        assert!(!workspace.join("inside.txt").exists());
        assert_eq!(std::fs::read_to_string(child_inside).unwrap(), "child-ok");
        assert!(!child_outside.exists());
        for (index, path) in outside_files.iter().enumerate() {
            assert_eq!(
                std::fs::read_to_string(path).unwrap(),
                format!("ORIGINAL-{index}")
            );
        }
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn network_is_denied_by_default_and_exactly_one_host_target_can_be_opened() {
        let (parent, workspace, _) = isolated_test_roots("network-target");
        let allowed_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let denied_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let allowed_port = allowed_listener.local_addr().unwrap().port();
        let denied_port = denied_listener.local_addr().unwrap().port();
        let python = std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .map(|directory| directory.join("python3"))
            .find(|candidate| candidate.is_file())
            .map(|candidate| std::fs::canonicalize(candidate).unwrap())
            .expect("python3 is installed for the sandbox network test");
        let shell = std::fs::canonicalize("/bin/sh").unwrap();
        let mut readable_roots = vec![workspace.clone()];
        for candidate in [
            "/bin",
            "/usr",
            "/opt/homebrew",
            "/Library",
            "/System/Library",
        ] {
            if let Ok(path) = std::fs::canonicalize(candidate) {
                readable_roots.push(path);
            }
        }
        let run = |port: u16, targets: BTreeSet<String>| {
            let code = format!(
                "import socket; socket.create_connection(('127.0.0.1', {port}), .5); print('CONNECTED')"
            );
            let command = sandbox_command(
                shell.to_string_lossy().into_owned(),
                vec![
                    "-c".to_owned(),
                    format!(
                        "PYTHONDONTWRITEBYTECODE=1 '{}' -c \"{}\"",
                        python.display(),
                        code
                    ),
                ],
                &workspace,
                &ShellSandboxPolicy {
                    readable_roots: readable_roots.clone(),
                    readable_files: Vec::new(),
                    writable_roots: vec![workspace.clone()],
                    allow_child_processes: true,
                    allow_host_ui: false,
                    launcher_programs: vec![shell.clone()],
                    network: SandboxNetworkAccess::ExactTargets(targets),
                    linux_bwrap_path: None,
                },
            )
            .unwrap();
            run_sandboxed(command, &workspace)
        };

        let denied = run(allowed_port, BTreeSet::new());
        assert!(!denied.status.success());
        assert!(!String::from_utf8_lossy(&denied.stdout).contains("CONNECTED"));

        let target = format!("127.0.0.1:{allowed_port}");
        let allowed = run(allowed_port, BTreeSet::from([target.clone()]));
        assert!(
            allowed.status.success(),
            "{}",
            String::from_utf8_lossy(&allowed.stderr)
        );
        assert!(String::from_utf8_lossy(&allowed.stdout).contains("CONNECTED"));

        let wrong_target = run(denied_port, BTreeSet::from([target]));
        assert!(!wrong_target.status.success());
        assert!(!String::from_utf8_lossy(&wrong_target.stdout).contains("CONNECTED"));

        drop(allowed_listener);
        drop(denied_listener);
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn network_target_normalization_rejects_profile_injection_and_invalid_ports() {
        for target in [
            "",
            "example.com",
            "example.com:*",
            "example.com:0",
            "example.com:65536",
            "example.com:443\") (allow network-outbound)",
        ] {
            assert!(normalize_network_targets(&BTreeSet::from([target.to_owned()])).is_err());
        }
        assert_eq!(
            normalize_network_targets(&BTreeSet::from(["127.0.0.1:443".to_owned()])).unwrap(),
            BTreeSet::from(["localhost:443".to_owned()])
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn approved_unrestricted_network_is_explicit_in_the_macos_profile() {
        let cwd = std::fs::canonicalize(".").unwrap();
        let program = std::fs::canonicalize("/bin/sh").unwrap();
        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: Vec::new(),
            cwd: cwd.clone(),
            env: HashMap::new(),
        };
        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            readable_files: Vec::new(),
            writable_roots: vec![cwd],
            allow_child_processes: false,
            allow_host_ui: false,
            launcher_programs: vec![program],
            network: SandboxNetworkAccess::Unrestricted,
            linux_bwrap_path: None,
        };
        let profile = build_macos_profile(&spec, &policy).unwrap();
        assert!(profile.contains("(allow network-outbound)"));
        assert!(profile.contains("(allow network-bind (local ip \"localhost:*\"))"));
        assert!(profile.contains("(allow network-inbound (local ip \"localhost:*\"))"));
        assert!(profile.contains("com.apple.SystemConfiguration.DNSConfiguration"));
        assert!(profile.contains("com.apple.SecurityServer"));
        assert!(!profile.contains("com.apple.coreservices.launchservicesd"));
        assert!(!profile.contains("(allow appleevent-send)"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn registered_host_ui_is_explicit_in_the_macos_profile() {
        let cwd = std::fs::canonicalize(".").unwrap();
        let program = std::fs::canonicalize("/bin/sh").unwrap();
        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: Vec::new(),
            cwd: cwd.clone(),
            env: HashMap::new(),
        };
        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            readable_files: Vec::new(),
            writable_roots: vec![cwd],
            allow_child_processes: true,
            allow_host_ui: true,
            launcher_programs: vec![program],
            network: SandboxNetworkAccess::Unrestricted,
            linux_bwrap_path: None,
        };
        let profile = build_macos_profile(&spec, &policy).unwrap();
        assert!(profile.contains("com.apple.coreservices.launchservicesd"));
        assert!(profile.contains("com.apple.coreservices.appleevents"));
        assert!(profile.contains("com.apple.coreservices.quarantine-resolver"));
        assert!(profile.contains("com.apple.lsd.mapdb"));
        assert!(profile.contains("(allow appleevent-send)"));
        assert!(profile.contains("(allow lsopen)"));
        assert!(profile.contains("(allow file-read* (literal \"/usr/bin/open\"))"));
        assert!(profile.contains("(with-filter (process-path \"/usr/bin/open\")"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn registered_host_ui_can_resolve_an_application_through_launch_services() {
        let (parent, workspace, _) = isolated_test_roots("host-ui");
        let shell = std::fs::canonicalize("/bin/sh").unwrap();
        let command = sandbox_command(
            shell.to_string_lossy().into_owned(),
            vec!["-c".to_owned(), "/usr/bin/open -Ra Safari".to_owned()],
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![workspace.clone()],
                readable_files: Vec::new(),
                writable_roots: vec![workspace.clone()],
                allow_child_processes: true,
                allow_host_ui: true,
                launcher_programs: vec![shell],
                network: SandboxNetworkAccess::Disabled,
                linux_bwrap_path: None,
            },
        )
        .unwrap();

        let output = run_sandboxed(command, &workspace);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn approved_unrestricted_network_supports_a_loopback_oauth_callback() {
        let (parent, workspace, _) = isolated_test_roots("oauth-callback");
        let python = std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .map(|directory| directory.join("python3"))
            .find(|candidate| candidate.is_file())
            .map(|candidate| std::fs::canonicalize(candidate).unwrap())
            .expect("python3 is installed for the sandbox callback test");
        let code = concat!(
            "import socket; ",
            "listener=socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen(1); ",
            "client=socket.create_connection(listener.getsockname(), 1); ",
            "server,_=listener.accept(); client.sendall(b'X'); ",
            "assert server.recv(1)==b'X'; print('CALLBACK_OK')",
        );
        let mut readable_roots = vec![workspace.clone()];
        for candidate in ["/usr", "/opt/homebrew", "/Library", "/System/Library"] {
            if let Ok(path) = std::fs::canonicalize(candidate) {
                readable_roots.push(path);
            }
        }
        let command = sandbox_command(
            python.to_string_lossy().into_owned(),
            vec!["-c".to_owned(), code.to_owned()],
            &workspace,
            &ShellSandboxPolicy {
                readable_roots,
                readable_files: Vec::new(),
                writable_roots: vec![workspace.clone()],
                allow_child_processes: true,
                allow_host_ui: false,
                launcher_programs: vec![python],
                network: SandboxNetworkAccess::Unrestricted,
                linux_bwrap_path: None,
            },
        )
        .unwrap();

        let output = run_sandboxed(command, &workspace);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("CALLBACK_OK"));
        std::fs::remove_dir_all(parent).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn approved_unrestricted_network_keeps_the_host_network_namespace() {
        let cwd = std::fs::canonicalize(".").unwrap();
        let program = std::fs::canonicalize("/bin/sh").unwrap();
        let spec = SandboxCommandSpec {
            program: program.to_string_lossy().into_owned(),
            args: Vec::new(),
            cwd: cwd.clone(),
            env: HashMap::new(),
        };
        let policy = ShellSandboxPolicy {
            readable_roots: vec![cwd.clone()],
            readable_files: Vec::new(),
            writable_roots: vec![cwd],
            allow_child_processes: false,
            allow_host_ui: false,
            launcher_programs: vec![program],
            network: SandboxNetworkAccess::Unrestricted,
            linux_bwrap_path: None,
        };
        let args = build_linux_bwrap_args(&spec, &policy);
        assert!(!args.iter().any(|value| value == "--unshare-net"));
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
            readable_files: Vec::new(),
            writable_roots: vec![cwd.clone()],
            allow_child_processes: false,
            allow_host_ui: false,
            launcher_programs: vec![program.clone()],
            network: SandboxNetworkAccess::Disabled,
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
            readable_files: Vec::new(),
            writable_roots: vec![cwd],
            allow_child_processes: false,
            allow_host_ui: false,
            launcher_programs: vec![program],
            network: SandboxNetworkAccess::Disabled,
            linux_bwrap_path: None,
        };
        let args = build_linux_bwrap_args(&spec, &policy);
        assert!(args.iter().any(|v| v == "--unshare-net"));
        assert!(args.iter().any(|v| v == "--bind"));
        assert!(args.iter().any(|v| v == "--chdir"));
        if Path::new("/usr/libexec").is_dir() {
            assert!(args.windows(3).any(|window| {
                window[0] == "--ro-bind"
                    && window[1] == "/usr/libexec"
                    && window[2] == "/usr/libexec"
            }));
        }
        assert!(!args
            .windows(3)
            .any(|window| { window[0] == "--ro-bind" && window[1] == "/" && window[2] == "/" }));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_bwrap_executes_one_allowlisted_program() {
        let (parent, workspace, _) = isolated_test_roots("linux-exec");
        let output_path = workspace.join("sandbox-output.txt");
        let program = std::fs::canonicalize("/bin/bash").unwrap();
        let command = sandbox_command(
            program.to_string_lossy().into_owned(),
            vec![
                "--noprofile".to_owned(),
                "--norc".to_owned(),
                "-c".to_owned(),
                format!("printf sandbox-ok > '{}'", output_path.display()),
            ],
            &workspace,
            &ShellSandboxPolicy {
                readable_roots: vec![workspace.clone()],
                readable_files: Vec::new(),
                writable_roots: vec![workspace.clone()],
                allow_child_processes: false,
                allow_host_ui: false,
                launcher_programs: vec![program],
                network: SandboxNetworkAccess::Disabled,
                linux_bwrap_path: None,
            },
        )
        .unwrap();
        let output = run_sandboxed(command, &workspace);
        assert!(
            output.status.success(),
            "bubblewrap execution failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(std::fs::read_to_string(output_path).unwrap(), "sandbox-ok");
        std::fs::remove_dir_all(parent).unwrap();
    }
}
