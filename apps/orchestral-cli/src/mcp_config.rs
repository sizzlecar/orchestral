//! Application-owned MCP manifest loading.
//!
//! Runtime crates consume already-resolved transport factories. Compatibility
//! with project/user configuration formats belongs at this composition edge so
//! it cannot silently become ambient Agent authority.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use orchestral_core::config::{McpServerSpec, McpTransportSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JsonMcpManifest {
    #[serde(rename = "mcpServers")]
    pub(crate) servers: BTreeMap<String, JsonMcpServer>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JsonMcpServer {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) transport_type: Option<String>,
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    #[serde(default)]
    pub(crate) env: HashMap<String, String>,
    #[serde(
        default = "default_true",
        rename = "allowChildProcesses",
        alias = "allow_child_processes"
    )]
    pub(crate) allow_child_processes: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cwd: Option<String>,
    #[serde(default, rename = "readableRoots", alias = "readable_roots")]
    pub(crate) readable_roots: Vec<String>,
    #[serde(default, rename = "writableRoots", alias = "writable_roots")]
    pub(crate) writable_roots: Vec<String>,
    #[serde(default, rename = "networkTargets", alias = "network_targets")]
    pub(crate) network_targets: Vec<String>,
    #[serde(default)]
    pub(crate) disabled: bool,
    #[serde(default)]
    pub(crate) required: bool,
    #[serde(default, rename = "startupTimeoutMs", alias = "startup_timeout_ms")]
    pub(crate) startup_timeout_ms: Option<u64>,
    #[serde(default, rename = "toolTimeoutMs", alias = "tool_timeout_ms")]
    pub(crate) tool_timeout_ms: Option<u64>,
    #[serde(default, rename = "enabledTools", alias = "enabled_tools")]
    pub(crate) enabled_tools: Vec<String>,
    #[serde(default, rename = "disabledTools", alias = "disabled_tools")]
    pub(crate) disabled_tools: Vec<String>,
}

impl JsonMcpServer {
    pub(crate) fn display_target(&self) -> (&'static str, String) {
        let mut command = self.command.clone();
        for argument in &self.args {
            command.push(' ');
            command.push_str(argument);
        }
        ("stdio", command)
    }
}

/// User-managed MCP registrations are trusted Host configuration, not ambient
/// files discovered in a repository. `ORCHESTRAL_HOME` is the explicit escape
/// hatch; otherwise follow XDG and then the conventional user config path.
pub(crate) fn user_registry_path() -> anyhow::Result<PathBuf> {
    if let Some(root) = std::env::var_os("ORCHESTRAL_HOME") {
        return absolute_config_root(PathBuf::from(root)).map(|root| root.join("mcp.json"));
    }
    if let Some(root) = std::env::var_os("XDG_CONFIG_HOME") {
        return absolute_config_root(PathBuf::from(root))
            .map(|root| root.join("orchestral/mcp.json"));
    }
    if let Some(root) = std::env::var_os("HOME") {
        return absolute_config_root(PathBuf::from(root))
            .map(|root| root.join(".config/orchestral/mcp.json"));
    }
    if let Some(root) = std::env::var_os("APPDATA") {
        return absolute_config_root(PathBuf::from(root))
            .map(|root| root.join("orchestral/mcp.json"));
    }
    bail!("cannot resolve user config directory; set ORCHESTRAL_HOME")
}

fn absolute_config_root(root: PathBuf) -> anyhow::Result<PathBuf> {
    if !root.is_absolute() {
        bail!(
            "user config directory '{}' must be absolute",
            root.display()
        );
    }
    Ok(root)
}

pub(crate) fn load_registry(path: &Path) -> anyhow::Result<JsonMcpManifest> {
    if !path.exists() {
        return Ok(JsonMcpManifest::default());
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("read MCP registry '{}'", path.display()))?;
    serde_json::from_str(&content)
        .with_context(|| format!("parse MCP registry '{}'", path.display()))
}

pub(crate) fn save_registry(path: &Path, registry: &JsonMcpManifest) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("MCP registry path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create MCP config directory '{}'", parent.display()))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(".mcp.json.{}.{nonce}.tmp", std::process::id()));
    let bytes = serde_json::to_vec_pretty(registry).context("serialize MCP registry")?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let write_result = (|| -> anyhow::Result<()> {
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("create temporary MCP registry '{}'", temporary.display()))?;
        file.write_all(&bytes)
            .with_context(|| format!("write temporary MCP registry '{}'", temporary.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("finish temporary MCP registry '{}'", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("sync temporary MCP registry '{}'", temporary.display()))?;
        fs::rename(&temporary, path)
            .with_context(|| format!("replace MCP registry '{}'", path.display()))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

pub(crate) fn load_server_manifests(
    workspace: &Path,
    inline: &[McpServerSpec],
    configured_files: &[String],
    cli_files: &[PathBuf],
) -> anyhow::Result<Vec<McpServerSpec>> {
    let mut resolved = BTreeMap::<String, (String, McpServerSpec)>::new();
    for spec in inline {
        insert_unique(&mut resolved, "orchestral config", spec.clone())?;
    }

    let files = configured_files
        .iter()
        .map(PathBuf::from)
        .chain(cli_files.iter().cloned());
    for path in files {
        let path = if path.is_absolute() {
            path
        } else {
            workspace.join(path)
        };
        let path = std::fs::canonicalize(&path)
            .with_context(|| format!("canonicalize MCP manifest '{}'", path.display()))?;
        let source = path.display().to_string();
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("read MCP manifest '{source}'"))?;
        let manifest: JsonMcpManifest = serde_json::from_str(&content)
            .with_context(|| format!("parse MCP manifest '{source}'"))?;
        let base = path.parent().unwrap_or(workspace);
        for (name, server) in manifest.servers {
            let spec = server.into_spec(name, base, &source)?;
            insert_unique(&mut resolved, &source, spec)?;
        }
    }

    Ok(resolved.into_values().map(|(_, spec)| spec).collect())
}

fn insert_unique(
    resolved: &mut BTreeMap<String, (String, McpServerSpec)>,
    source: &str,
    spec: McpServerSpec,
) -> anyhow::Result<()> {
    let name = spec.name.trim().to_owned();
    if name.is_empty() {
        bail!("MCP server from '{source}' has an empty name");
    }
    if let Some((previous, _)) = resolved.get(&name) {
        bail!("MCP server '{name}' is declared by both '{previous}' and '{source}'");
    }
    resolved.insert(name, (source.to_owned(), spec));
    Ok(())
}

impl JsonMcpServer {
    fn into_spec(self, name: String, base: &Path, source: &str) -> anyhow::Result<McpServerSpec> {
        if self
            .transport_type
            .as_deref()
            .is_some_and(|kind| kind != "stdio")
        {
            bail!(
                "MCP server '{name}' in '{source}' is not a local stdio server; configure remote transports in Orchestral YAML"
            );
        }
        if self.command.trim().is_empty() {
            bail!("MCP server '{name}' in '{source}' has an empty command");
        }
        if self.startup_timeout_ms == Some(0) || self.tool_timeout_ms == Some(0) {
            bail!("MCP server '{name}' in '{source}' has a zero timeout");
        }
        let command = resolve_manifest_command(base, &self.command);
        let cwd = self
            .cwd
            .as_deref()
            .map(|path| resolve_manifest_path(base, path));
        let readable_roots = self
            .readable_roots
            .iter()
            .map(|path| resolve_manifest_path(base, path))
            .collect();
        let writable_roots = self
            .writable_roots
            .iter()
            .map(|path| resolve_manifest_path(base, path))
            .collect();
        Ok(McpServerSpec {
            name,
            enabled: !self.disabled,
            required: self.required,
            transport: McpTransportSpec::Stdio {
                command,
                args: self.args,
                env: self.env,
                allow_child_processes: self.allow_child_processes,
                cwd,
                readable_roots,
                writable_roots,
                network_targets: self.network_targets,
            },
            startup_timeout_ms: self.startup_timeout_ms,
            tool_timeout_ms: self.tool_timeout_ms,
            enabled_tools: self.enabled_tools,
            disabled_tools: self.disabled_tools,
        })
    }
}

fn resolve_manifest_command(base: &Path, command: &str) -> String {
    let path = Path::new(command);
    if path.is_absolute() || path.components().count() == 1 {
        command.to_owned()
    } else {
        base.join(path).to_string_lossy().into_owned()
    }
}

fn resolve_manifest_path(base: &Path, path: &str) -> String {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_string_lossy().into_owned()
    } else {
        base.join(path).to_string_lossy().into_owned()
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn test_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "orchestral-mcp-manifest-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn explicit_json_manifest_resolves_local_stdio_authority() {
        let root = test_root("local");
        std::fs::create_dir_all(root.join("bin")).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let path = root.join(".mcp.json");
        std::fs::write(
            &path,
            r#"{
  "mcpServers": {
    "local": {
      "type": "stdio",
      "command": "bin/server",
      "args": ["--stdio"],
      "cwd": ".",
      "readableRoots": ["bin"],
      "networkTargets": ["localhost:4317"]
    }
  }
}"#,
        )
        .unwrap();

        let specs = load_server_manifests(&root, &[], &[], &[path]).unwrap();
        assert_eq!(specs.len(), 1);
        let McpTransportSpec::Stdio {
            command,
            cwd,
            readable_roots,
            network_targets,
            ..
        } = &specs[0].transport
        else {
            panic!("expected stdio manifest")
        };
        assert_eq!(command, &root.join("bin/server").to_string_lossy());
        assert_eq!(
            cwd.as_deref(),
            Some(root.join(".").to_string_lossy().as_ref())
        );
        assert_eq!(
            readable_roots,
            &[root.join("bin").to_string_lossy().into_owned()]
        );
        assert_eq!(network_targets, &["localhost:4317"]);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn duplicate_server_names_never_silently_override_authority() {
        let inline = McpServerSpec {
            name: "same".to_owned(),
            enabled: true,
            required: false,
            transport: McpTransportSpec::Stdio {
                command: "/bin/echo".to_owned(),
                args: Vec::new(),
                env: HashMap::new(),
                allow_child_processes: true,
                cwd: None,
                readable_roots: Vec::new(),
                writable_roots: Vec::new(),
                network_targets: Vec::new(),
            },
            startup_timeout_ms: None,
            tool_timeout_ms: None,
            enabled_tools: Vec::new(),
            disabled_tools: Vec::new(),
        };
        let mut resolved = BTreeMap::new();
        insert_unique(&mut resolved, "one", inline.clone()).unwrap();
        let error = insert_unique(&mut resolved, "two", inline).unwrap_err();
        assert!(error.to_string().contains("both 'one' and 'two'"));
    }
}
