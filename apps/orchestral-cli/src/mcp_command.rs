use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::{Args, Subcommand};

use crate::mcp_config::{load_registry, save_registry, user_registry_path, JsonMcpServer};

#[derive(Debug, Args)]
pub(crate) struct McpCommand {
    #[command(subcommand)]
    command: McpSubcommand,
}

#[derive(Debug, Subcommand)]
enum McpSubcommand {
    /// List registered MCP servers.
    List(OutputArgs),
    /// Show one registered MCP server.
    Get(GetArgs),
    /// Register a local stdio MCP server.
    Add(AddArgs),
    /// Remove a registered MCP server.
    Remove(NameArgs),
}

#[derive(Debug, Args)]
struct OutputArgs {
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct GetArgs {
    name: String,
    /// Emit machine-readable JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct NameArgs {
    name: String,
}

#[derive(Debug, Args)]
#[command(override_usage = "orchestral mcp add [OPTIONS] <NAME> -- <COMMAND>...")]
struct AddArgs {
    /// Stable name used to expose this server's tools.
    name: String,

    /// Environment variable passed only to this MCP process (KEY=VALUE).
    #[arg(long, value_name = "KEY=VALUE", value_parser = parse_env_pair)]
    env: Vec<(String, String)>,

    /// MCP process working directory. Defaults to the Agent workspace at run time.
    #[arg(long, value_name = "PATH")]
    cwd: Option<PathBuf>,

    /// Additional readable directory. May be repeated.
    #[arg(long = "read", value_name = "PATH")]
    readable_roots: Vec<PathBuf>,

    /// Additional writable directory. May be repeated.
    #[arg(long = "write", value_name = "PATH")]
    writable_roots: Vec<PathBuf>,

    /// Exact network destination (HOST:PORT). May be repeated.
    #[arg(long = "network", value_name = "HOST:PORT")]
    network_targets: Vec<String>,

    /// Disable Host network access for this MCP process. User-registered
    /// local MCP servers inherit Host network access by default.
    #[arg(long, conflicts_with = "network_targets")]
    no_network: bool,

    /// Forbid the MCP process from creating children. By default the complete
    /// process tree is allowed but remains inside the same sandbox.
    #[arg(long)]
    no_child_processes: bool,

    /// Abort Agent startup when this MCP server cannot start.
    #[arg(long)]
    required: bool,

    /// Command and arguments used to start the MCP server.
    #[arg(last = true, required = true, num_args = 1..)]
    command: Vec<String>,
}

impl McpCommand {
    pub(crate) async fn run(self) -> anyhow::Result<()> {
        let path = user_registry_path()?;
        match self.command {
            McpSubcommand::List(args) => list(&path, args),
            McpSubcommand::Get(args) => get(&path, args),
            McpSubcommand::Add(args) => add(&path, args),
            McpSubcommand::Remove(args) => remove(&path, args),
        }
    }
}

fn list(path: &Path, args: OutputArgs) -> anyhow::Result<()> {
    let registry = load_registry(path)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&registry)?);
        return Ok(());
    }
    if registry.servers.is_empty() {
        println!("No MCP servers registered. Try `orchestral mcp add my-tool -- my-command`.");
        return Ok(());
    }
    println!("Name\tTransport\tCommand\tStatus");
    for (name, server) in registry.servers {
        let (transport, target) = server.display_target();
        let status = if server.disabled {
            "disabled"
        } else {
            "enabled"
        };
        println!("{name}\t{transport}\t{target}\t{status}");
    }
    Ok(())
}

fn get(path: &Path, args: GetArgs) -> anyhow::Result<()> {
    let registry = load_registry(path)?;
    let server = registry
        .servers
        .get(&args.name)
        .with_context(|| format!("MCP server '{}' is not registered", args.name))?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(server)?);
    } else {
        let (transport, target) = server.display_target();
        println!("Name: {}", args.name);
        println!("Transport: {transport}");
        println!("Command: {target}");
        println!("Enabled: {}", !server.disabled);
        println!("Required: {}", server.required);
        println!("Registry: {}", path.display());
    }
    Ok(())
}

fn add(path: &Path, args: AddArgs) -> anyhow::Result<()> {
    validate_name(&args.name)?;
    let mut command = args.command.into_iter();
    let program = command.next().context("MCP command is required")?;
    let program = normalize_program(&program)?;
    let cwd = args.cwd.map(canonical_directory).transpose()?;
    let readable_roots = canonical_directories(args.readable_roots)?;
    let writable_roots = canonical_directories(args.writable_roots)?;
    for target in &args.network_targets {
        validate_network_target(target)?;
    }
    let allow_unrestricted_network = !args.no_network && args.network_targets.is_empty();
    let server = JsonMcpServer {
        transport_type: Some("stdio".to_owned()),
        command: program,
        args: command.collect(),
        env: args.env.into_iter().collect::<HashMap<_, _>>(),
        allow_child_processes: !args.no_child_processes,
        cwd: cwd.map(|path| path.to_string_lossy().into_owned()),
        readable_roots: readable_roots
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        writable_roots: writable_roots
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        network_targets: args.network_targets,
        allow_unrestricted_network: Some(allow_unrestricted_network),
        disabled: false,
        required: args.required,
        startup_timeout_ms: None,
        tool_timeout_ms: None,
        enabled_tools: Vec::new(),
        disabled_tools: Vec::new(),
    };
    let mut registry = load_registry(path)?;
    let replaced = registry.servers.insert(args.name.clone(), server).is_some();
    save_registry(path, &registry)?;
    println!(
        "{} user MCP server '{}' in {}.",
        if replaced { "Updated" } else { "Added" },
        args.name,
        path.display()
    );
    Ok(())
}

fn remove(path: &Path, args: NameArgs) -> anyhow::Result<()> {
    validate_name(&args.name)?;
    let mut registry = load_registry(path)?;
    if registry.servers.remove(&args.name).is_none() {
        bail!("MCP server '{}' is not registered", args.name);
    }
    save_registry(path, &registry)?;
    println!("Removed user MCP server '{}'.", args.name);
    Ok(())
}

fn validate_name(name: &str) -> anyhow::Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        bail!("MCP server name must contain only letters, numbers, '-' or '_'");
    }
    Ok(())
}

fn parse_env_pair(value: &str) -> Result<(String, String), String> {
    let (key, value) = value
        .split_once('=')
        .ok_or_else(|| "environment values must use KEY=VALUE".to_owned())?;
    if key.is_empty()
        || !key.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        })
    {
        return Err("environment variable name is invalid".to_owned());
    }
    Ok((key.to_owned(), value.to_owned()))
}

fn normalize_program(program: &str) -> anyhow::Result<String> {
    if program.trim().is_empty() || program.chars().any(char::is_control) {
        bail!("MCP command must not be empty");
    }
    let path = Path::new(program);
    if path.is_absolute() || path.components().count() > 1 {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let canonical = std::fs::canonicalize(&path)
            .with_context(|| format!("resolve MCP executable '{}'", path.display()))?;
        if !canonical.is_file() {
            bail!("MCP executable '{}' is not a file", canonical.display());
        }
        return Ok(canonical.to_string_lossy().into_owned());
    }
    Ok(program.to_owned())
}

fn canonical_directory(path: PathBuf) -> anyhow::Result<PathBuf> {
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    };
    let canonical = std::fs::canonicalize(&path)
        .with_context(|| format!("resolve MCP directory '{}'", path.display()))?;
    if !canonical.is_dir() {
        bail!("MCP path '{}' is not a directory", canonical.display());
    }
    Ok(canonical)
}

fn canonical_directories(paths: Vec<PathBuf>) -> anyhow::Result<Vec<PathBuf>> {
    paths.into_iter().map(canonical_directory).collect()
}

fn validate_network_target(target: &str) -> anyhow::Result<()> {
    let Some((host, port)) = target.trim().rsplit_once(':') else {
        bail!("MCP network target '{target}' must use HOST:PORT");
    };
    if host.trim_matches(['[', ']']).is_empty() || !port.parse::<u16>().is_ok_and(|port| port > 0) {
        bail!("MCP network target '{target}' must use HOST:PORT");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_environment_without_losing_equals_in_value() {
        assert_eq!(
            parse_env_pair("TOKEN=a=b").unwrap(),
            ("TOKEN".to_owned(), "a=b".to_owned())
        );
        assert!(parse_env_pair("1TOKEN=value").is_err());
    }

    #[test]
    fn server_names_are_stable_config_keys() {
        assert!(validate_name("seekee-sidecar_1").is_ok());
        assert!(validate_name("seekee sidecar").is_err());
        assert!(validate_name("../seekee").is_err());
    }
}
