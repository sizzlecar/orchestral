//! Minimal command-line composition root for the provider-neutral Generic Agent.

use std::collections::BTreeSet;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_blob_fs::FileBlobStore;
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    spi::{AgentJournalStore, InMemoryAgentJournalStore},
    wire::{
        AgentCommand, AgentCommandEnvelope, AgentSessionId, AgentTelemetry, ApprovalDecision,
        BindingRequirement, CommandAckState, CommandId, Content, ContentBody, Digest,
        PendingRequest, PendingRequestPayload, ProviderBindingRef, RequestResolution,
        ResourceBinding, ResourceBindingId, ResourceBindingMode, ResourceId, ResourceKind,
        ResourceRef, ResourceRevision, RunId,
    },
};
use orchestral_core::agent_session::{AgentSessionJournalStore, InMemoryAgentSessionJournalStore};
use orchestral_core::config::{load_config, BackendSpec, ModelProfile, OrchestralConfig};
use orchestral_core::io::BlobStore;
use orchestral_core::mcp_protocol::McpServerId;
use orchestral_core::model_protocol::ModelBackend;
use orchestral_core::skill_protocol::{
    SkillSourceKind, SkillTrustLevel, SKILL_CATALOG_RESOURCE_KIND_V1,
};
use orchestral_core::tool_effect::{InMemoryToolEffectJournalStore, ToolEffectJournalStore};
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalVerifier,
    HostToolPolicy, InMemoryApprovalCapabilityStore, NetworkPolicy, ProcessPolicy, RunToolGrant,
    SandboxPolicy, ToolPolicyBounds, ToolRestriction,
};
use orchestral_model_gemini::{GeminiModelBackend, GeminiModelConfig};
use orchestral_model_openai::{OpenAiCompatibleBackend, OpenAiCompatibleConfig};
use orchestral_runtime::tools::{
    guarded_artifact_read_descriptor, guarded_file_read_descriptor, GuardedArtifactReadExecutor,
    GuardedFileReadExecutor,
};
use orchestral_runtime::{
    AgentClient, AgentControlEvent, AgentController, AgentToolRuntime, GenericAgentCheckpointStore,
    GenericAgentConfig, GuardedMcpServerConfig, GuardedToolRuntime, InMemoryBlobStore,
    InMemoryGenericAgentCheckpointStore, InMemoryHostApprovalBroker, InternalGenericAgentProvider,
    JsonSizeTokenMeter, McpToolsAdapterRegistry, SkillActivationPolicy, SkillHostProfile,
    SkillRoot, SkillRuntime, StdioMcpTransportFactory, ToolArtifactStore,
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;

use crate::runtime::client::prepare_runtime_config_path;
use crate::runtime::ModelOverrides;

pub struct AgentRunOptions {
    pub config: Option<PathBuf>,
    pub model_overrides: ModelOverrides,
    pub session_id: Option<String>,
    pub system_prompt: Option<String>,
    pub input: Option<String>,
    pub no_mcp: bool,
    pub no_skills: bool,
}

struct CliJournalStores {
    run: Arc<dyn AgentJournalStore>,
    session: Arc<dyn AgentSessionJournalStore>,
    effect: Arc<dyn ToolEffectJournalStore>,
    checkpoint: Arc<dyn GenericAgentCheckpointStore>,
}

pub async fn run(options: AgentRunOptions) -> anyhow::Result<()> {
    let config_path = prepare_runtime_config_path(options.config, &options.model_overrides)?;
    let config = load_config(&config_path)
        .with_context(|| format!("load Generic Agent config '{}'", config_path.display()))?;
    let (backend, profile, model, temperature) = resolve_model(&config)?;
    let max_output_tokens = profile
        .as_ref()
        .and_then(|profile| profile.max_tokens)
        .unwrap_or(8_192) as u64;
    let model_backend = build_model_backend(
        &backend,
        &model,
        temperature,
        max_output_tokens,
        config.agent.stream_buffer,
    )?;

    let mut agent_config = GenericAgentConfig::new("orchestral/internal", "generic-agent");
    agent_config.stream_buffer = config.agent.stream_buffer;
    agent_config.max_model_rounds = config.agent.max_model_rounds;
    agent_config.max_tool_calls = config.agent.max_tool_calls;
    agent_config.max_context_tokens = config.agent.max_context_tokens;
    agent_config.reserved_output_tokens = config.agent.reserved_output_tokens;
    if let Some(system_prompt) = options
        .system_prompt
        .or_else(|| config.agent.system_prompt.clone())
        .or_else(|| {
            profile
                .as_ref()
                .and_then(|profile| profile.system_prompt.clone())
        })
    {
        agent_config.system_prompt = system_prompt;
    }
    let CliJournalStores {
        run: run_journal,
        session: session_journal,
        effect: effect_journal,
        checkpoint: generic_checkpoint_journal,
    } = match config.journal.backend.as_str() {
        "memory" => CliJournalStores {
            run: Arc::new(InMemoryAgentJournalStore::default()),
            session: Arc::new(InMemoryAgentSessionJournalStore::default()),
            effect: Arc::new(InMemoryToolEffectJournalStore::default()),
            checkpoint: Arc::new(InMemoryGenericAgentCheckpointStore::default()),
        },
        "filesystem" | "fs" => {
            let root = config.journal.root_dir.as_str();
            let store = Arc::new(
                FileAgentJournalStore::open(root)
                    .with_context(|| format!("open Agent Journal at '{root}'"))?,
            );
            CliJournalStores {
                run: store.clone(),
                session: store.clone(),
                effect: store.clone(),
                checkpoint: store,
            }
        }
        backend => bail!("unsupported Agent Journal backend for CLI: {backend}"),
    };
    let mcp_configs = if options.no_mcp {
        Vec::new()
    } else {
        configured_mcp_servers(&config)?
    };
    let artifact_store = ToolArtifactStore::new(
        build_cli_blob_store(&config)?,
        config.artifacts.max_bytes,
        config.artifacts.summary_max_chars,
    )
    .context("configure Tool Artifact store")?;
    let (tool_runtime, run_grant, approval_broker) =
        build_cli_tool_runtime(&config, &mcp_configs, effect_journal, artifact_store)?;
    let mut mcp_restriction = run_grant.bounds.clone();
    mcp_restriction.approval = ApprovalPolicy::Required;
    let mcp_registry = McpToolsAdapterRegistry::register(
        tool_runtime.as_ref(),
        mcp_configs,
        ToolRestriction {
            bounds: mcp_restriction,
        },
        CancellationToken::new(),
    )
    .await
    .context("register guarded MCP stdio Tools")?;
    for (server, error) in mcp_registry.skipped_optional_servers() {
        tracing::warn!(server = %server, %error, "optional MCP server was unavailable");
    }
    let skills = if options.no_skills {
        None
    } else {
        build_cli_skill_runtime(&config, tool_runtime.as_ref(), mcp_registry.server_names())?
    };
    let provider = match skills.clone() {
        Some(skills) => {
            InternalGenericAgentProvider::new_with_tools_approval_skills_and_session_journal(
                model_backend,
                agent_config,
                tool_runtime,
                run_grant,
                approval_broker.clone(),
                skills,
                session_journal,
                Arc::new(JsonSizeTokenMeter::default()),
            )
        }
        None => InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            model_backend,
            agent_config,
            tool_runtime,
            run_grant,
            approval_broker.clone(),
            session_journal,
            Arc::new(JsonSizeTokenMeter::default()),
        ),
    }
    .context("create Generic Agent provider")?
    .with_checkpoint_store(generic_checkpoint_journal)
    .context("bind Generic Agent private checkpoint journal")?;
    let provider = Arc::new(provider);
    let controller = Arc::new(
        AgentController::with_journal_store(
            provider,
            ProviderBindingRef::new("cli/generic-agent"),
            run_journal,
        )
        .context("bind Generic Agent controller")?,
    );
    let session_id = AgentSessionId::new(
        options
            .session_id
            .unwrap_or_else(|| unique_id("cli-session", 0)),
    );
    let mut resources = Vec::new();
    if let Some(skills) = skills.as_deref() {
        resources.push(ResourceBinding {
            binding_id: ResourceBindingId::new("cli-skill-catalog"),
            resource: ResourceRef {
                kind: ResourceKind::new(SKILL_CATALOG_RESOURCE_KIND_V1),
                id: skills.catalog().resource_id.clone(),
                revision: ResourceRevision::new(skills.catalog().revision.as_str()),
            },
            requirement: BindingRequirement::Required,
            mode: ResourceBindingMode::Snapshot,
        });
    }
    let client = AgentClient::new(controller, session_id.clone()).with_resources(resources);

    eprintln!("Generic Agent: backend={} model={model}", backend.name);
    let result = async {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        if let Some(input) = options.input {
            run_turn(&client, &approval_broker, 1, input, &mut lines).await?;
            return Ok(());
        }

        eprintln!(
            "Interactive session {}. Type /exit to quit.",
            session_id.as_str()
        );
        let mut turn = 0_u64;
        loop {
            print!("> ");
            io::stdout().flush().context("flush prompt")?;
            let next = tokio::select! {
                line = lines.next_line() => line.context("read Agent input")?,
                signal = tokio::signal::ctrl_c() => {
                    signal.context("listen for Ctrl-C")?;
                    eprintln!();
                    break;
                }
            };
            let Some(input) = next else {
                break;
            };
            let input = input.trim();
            if input == "/exit" || input == "/quit" {
                break;
            }
            if input.is_empty() {
                continue;
            }
            turn += 1;
            run_turn(
                &client,
                &approval_broker,
                turn,
                input.to_owned(),
                &mut lines,
            )
            .await?;
        }
        Ok(())
    }
    .await;
    mcp_registry.shutdown().await;
    result
}

fn build_cli_tool_runtime(
    config: &OrchestralConfig,
    mcp_configs: &[GuardedMcpServerConfig],
    effect_journal: Arc<dyn ToolEffectJournalStore>,
    artifact_store: ToolArtifactStore,
) -> anyhow::Result<(
    Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>>,
    RunToolGrant,
    Arc<InMemoryHostApprovalBroker>,
)> {
    let workspace = std::fs::canonicalize(std::env::current_dir().context("resolve workspace")?)
        .context("canonicalize workspace")?;
    let workspace = workspace.to_string_lossy().to_string();
    let shell_programs = configured_shell_programs(config)?;
    let shell_enabled = !shell_programs.is_empty();
    let mcp_enabled = !mcp_configs.is_empty();
    let mut allowed_programs = shell_programs.clone();
    allowed_programs.extend(
        mcp_configs
            .iter()
            .flat_map(GuardedMcpServerConfig::allowed_programs),
    );
    let mut allowed_effects =
        BTreeSet::from([EffectScope::FilesystemRead, EffectScope::ArtifactRead]);
    if shell_enabled {
        allowed_effects.extend([
            EffectScope::Process,
            EffectScope::FilesystemWrite,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
        ]);
    }
    if mcp_enabled {
        allowed_effects.extend([
            EffectScope::Process,
            EffectScope::FilesystemWrite,
            EffectScope::ExternalSideEffect,
        ]);
    }
    if mcp_configs
        .iter()
        .any(|server| !server.environment_names().is_empty())
    {
        allowed_effects.insert(EffectScope::SecretRead);
    }
    let allowed_environment = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::environment_names)
        .collect();
    let writable_roots = if shell_enabled || mcp_enabled {
        BTreeSet::from([workspace.clone()])
    } else {
        BTreeSet::new()
    };
    let bounds = ToolPolicyBounds {
        allowed_effects,
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            required: true,
            allowed_profiles: BTreeSet::from([
                "workspace_read".to_owned(),
                orchestral_runtime::tools::GUARDED_SHELL_SANDBOX_PROFILE.to_owned(),
                orchestral_runtime::tools::GUARDED_PTY_SANDBOX_PROFILE.to_owned(),
            ]),
        },
        process: ProcessPolicy {
            allowed_programs,
            allow_shell_expression: false,
        },
        filesystem: FilesystemPolicy {
            readable_roots: BTreeSet::from([workspace.clone()]),
            writable_roots,
        },
        network: NetworkPolicy::default(),
        environment: EnvironmentPolicy {
            allowed_variables: allowed_environment,
            inherit_host_environment: false,
        },
        allowed_credentials: BTreeSet::new(),
        max_timeout_ms: Some(config.tools.max_timeout_ms),
        max_output_bytes: Some(config.tools.max_output_bytes),
    };
    let signing_material = Digest::sha256(unique_id("cli-approval-key", 0));
    let approval_broker = Arc::new(
        InMemoryHostApprovalBroker::new(signing_material.as_str().as_bytes())
            .context("create Host approval broker")?,
    );
    let verifier = HostApprovalVerifier::new(
        signing_material.as_str().as_bytes(),
        InMemoryApprovalCapabilityStore::default(),
    )
    .context("create Host approval verifier")?;
    let runtime = Arc::new(
        GuardedToolRuntime::new_with_effect_journal_and_artifacts(
            HostToolPolicy {
                bounds: bounds.clone(),
            },
            verifier,
            effect_journal,
            artifact_store.clone(),
        )
        .context("create guarded Tool Runtime")?,
    );
    runtime
        .register(
            guarded_artifact_read_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedArtifactReadExecutor::new(artifact_store)),
        )
        .context("register guarded artifact_read Tool")?;
    runtime
        .register(
            guarded_file_read_descriptor(ToolRestriction {
                bounds: bounds.clone(),
            }),
            Arc::new(GuardedFileReadExecutor),
        )
        .context("register guarded file_read Tool")?;
    if shell_enabled {
        runtime
            .register(
                orchestral_runtime::tools::guarded_shell_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(orchestral_runtime::tools::GuardedShellExecutor),
            )
            .context("register guarded shell Tool")?;
        let pty_manager = Arc::new(
            orchestral_runtime::PtyProcessManager::new(1024 * 1024, Duration::from_secs(10 * 60))
                .context("create run-scoped PTY process manager")?,
        );
        runtime
            .register(
                orchestral_runtime::tools::guarded_pty_create_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(orchestral_runtime::tools::GuardedPtyCreateExecutor::new(
                    pty_manager.clone(),
                )),
            )
            .context("register guarded pty_create Tool")?;
        runtime
            .register(
                orchestral_runtime::tools::guarded_pty_write_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(orchestral_runtime::tools::GuardedPtyWriteExecutor::new(
                    pty_manager.clone(),
                )),
            )
            .context("register guarded pty_write Tool")?;
        runtime
            .register(
                orchestral_runtime::tools::guarded_pty_read_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(orchestral_runtime::tools::GuardedPtyReadExecutor::new(
                    pty_manager.clone(),
                )),
            )
            .context("register guarded pty_read Tool")?;
        runtime
            .register(
                orchestral_runtime::tools::guarded_pty_close_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(orchestral_runtime::tools::GuardedPtyCloseExecutor::new(
                    pty_manager.clone(),
                )),
            )
            .context("register guarded pty_close Tool")?;
        runtime
            .register(
                orchestral_runtime::tools::guarded_pty_list_descriptor(ToolRestriction {
                    bounds: bounds.clone(),
                }),
                Arc::new(orchestral_runtime::tools::GuardedPtyListExecutor::new(
                    pty_manager,
                )),
            )
            .context("register guarded pty_list Tool")?;
    } else {
        tracing::warn!(
            "Generic Agent shell Tool is disabled because no explicit allowed_commands resolved"
        );
    }
    Ok((runtime, RunToolGrant { bounds }, approval_broker))
}

fn build_cli_blob_store(config: &OrchestralConfig) -> anyhow::Result<Arc<dyn BlobStore>> {
    match config.artifacts.backend.trim() {
        "memory" | "in_memory" => Ok(Arc::new(InMemoryBlobStore::default())),
        "local" | "filesystem" | "fs" => Ok(Arc::new(
            FileBlobStore::open(&config.artifacts.root_dir).with_context(|| {
                format!("open Artifact BlobStore at '{}'", config.artifacts.root_dir)
            })?,
        )),
        mode => bail!("unsupported BlobStore mode for Generic Agent Artifact results: {mode}"),
    }
}

fn build_cli_skill_runtime(
    config: &OrchestralConfig,
    tools: &dyn AgentToolRuntime,
    available_mcp_servers: BTreeSet<String>,
) -> anyhow::Result<Option<Arc<SkillRuntime>>> {
    if !config.skills.enabled {
        return Ok(None);
    }
    let workspace = std::fs::canonicalize(std::env::current_dir().context("resolve workspace")?)
        .context("canonicalize Skill workspace")?;
    let mut roots = Vec::new();
    for (index, configured) in config.skills.directories.iter().enumerate() {
        let path = PathBuf::from(configured);
        roots.push(SkillRoot {
            path: if path.is_absolute() {
                path
            } else {
                workspace.join(path)
            },
            source_kind: SkillSourceKind::UserConfigured,
            trust: SkillTrustLevel::UserTrusted,
            precedence: 10_000_u32.saturating_sub(index as u32),
            required: true,
        });
    }
    if config.skills.auto_discover {
        let trust = if config.skills.trust_workspace {
            SkillTrustLevel::WorkspaceTrusted
        } else {
            SkillTrustLevel::WorkspaceUntrusted
        };
        for (index, relative) in [".claude/skills", ".codex/skills", "skills"]
            .into_iter()
            .enumerate()
        {
            roots.push(SkillRoot {
                path: workspace.join(relative),
                source_kind: SkillSourceKind::Workspace,
                trust,
                precedence: 1_000_u32.saturating_sub(index as u32),
                required: false,
            });
        }
    }
    if roots.is_empty() {
        return Ok(None);
    }
    let mut host = SkillHostProfile::current();
    host.available_tools = tools
        .model_tool_schemas()
        .context("inspect Host Tool catalog for Skill dependencies")?
        .into_iter()
        .map(|schema| schema.name)
        .collect();
    host.available_mcp_servers = available_mcp_servers;
    for program in configured_shell_programs(config)? {
        host.available_programs.insert(program.clone());
        if let Some(name) = PathBuf::from(&program)
            .file_name()
            .and_then(|name| name.to_str())
        {
            host.available_programs.insert(name.to_owned());
        }
    }
    let runtime = SkillRuntime::discover(
        ResourceId::new("cli-skills"),
        &roots,
        host,
        SkillActivationPolicy {
            allow_untrusted_workspace: false,
            allow_incompatible: config.skills.allow_incompatible,
        },
    )
    .context("discover strict Skill catalog")?;
    if runtime.catalog().skills.is_empty() {
        return Ok(None);
    }
    for conflict in runtime.conflicts() {
        tracing::warn!(
            skill = conflict.name,
            selected = conflict.selected_source,
            shadowed = conflict.shadowed_source,
            "Skill name conflict resolved by deterministic precedence"
        );
    }
    Ok(Some(Arc::new(runtime)))
}

fn configured_shell_programs(config: &OrchestralConfig) -> anyhow::Result<BTreeSet<String>> {
    if !config.tools.shell.enabled {
        return Ok(BTreeSet::new());
    }
    let configured = config.tools.shell.allowed_programs.clone();
    let mut resolved = BTreeSet::new();
    for program in configured {
        match resolve_host_program(&program) {
            Ok(path) => {
                resolved.insert(path);
            }
            Err(error) => {
                tracing::warn!(program, %error, "configured shell program is unavailable")
            }
        }
    }
    Ok(resolved)
}

/// MCP and Skill remain distinct resources. This adapter accepts only explicit
/// stdio MCP servers and publishes their discovered methods as guarded Tools.
fn configured_mcp_servers(
    config: &OrchestralConfig,
) -> anyhow::Result<Vec<GuardedMcpServerConfig>> {
    if !config.mcp.enabled {
        return Ok(Vec::new());
    }
    let mut servers = Vec::new();
    for spec in config.mcp.servers.iter().filter(|server| server.enabled) {
        let program = match resolve_host_program(&spec.command) {
            Ok(program) => program,
            Err(error) if !spec.required => {
                tracing::warn!(server = spec.name, %error, "optional MCP command was unavailable");
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("resolve required MCP server '{}'", spec.name))
            }
        };
        let server = GuardedMcpServerConfig {
            server_id: McpServerId::new(spec.name.trim()),
            required: spec.required,
            transport: Arc::new(StdioMcpTransportFactory::new(
                PathBuf::from(program),
                spec.args.clone(),
                spec.env
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            )?),
            startup_timeout: Duration::from_millis(spec.startup_timeout_ms.unwrap_or(15_000)),
            tool_timeout: Duration::from_millis(spec.tool_timeout_ms.unwrap_or(20_000)),
            enabled_tools: spec.enabled_tools.iter().cloned().collect(),
            disabled_tools: spec.disabled_tools.iter().cloned().collect(),
        };
        if let Err(error) = server.validate() {
            if !spec.required {
                tracing::warn!(server = spec.name, %error, "optional MCP server config was invalid");
                continue;
            }
            return Err(error)
                .with_context(|| format!("validate required MCP server '{}'", spec.name));
        }
        servers.push(server);
    }
    Ok(servers)
}

fn resolve_host_program(program: &str) -> anyhow::Result<String> {
    let candidate = PathBuf::from(program);
    if candidate.is_absolute() {
        return std::fs::canonicalize(&candidate)
            .with_context(|| format!("canonicalize executable '{}'", candidate.display()))
            .map(|path| path.to_string_lossy().to_string());
    }
    if program.contains(std::path::MAIN_SEPARATOR) || program.trim().is_empty() {
        bail!("program must be an absolute path or a bare executable name")
    }
    let path = std::env::var_os("PATH").context("PATH is unavailable")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(program);
        if candidate.is_file() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if candidate
                    .metadata()
                    .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 == 0)
                {
                    continue;
                }
            }
            return std::fs::canonicalize(&candidate)
                .with_context(|| format!("canonicalize executable '{}'", candidate.display()))
                .map(|path| path.to_string_lossy().to_string());
        }
    }
    bail!("executable was not found on Host PATH")
}

fn resolve_model(
    config: &OrchestralConfig,
) -> anyhow::Result<(
    orchestral_core::config::BackendSpec,
    Option<ModelProfile>,
    String,
    f32,
)> {
    let profile = config
        .agent
        .model_profile
        .as_deref()
        .map(|name| {
            config
                .providers
                .get_model(name)
                .with_context(|| format!("model profile not found: {name}"))
        })
        .transpose()?;
    let backend_name = config
        .agent
        .backend
        .clone()
        .or_else(|| profile.as_ref().map(|profile| profile.backend.clone()))
        .or_else(|| config.providers.default_backend.clone());
    let backend = match backend_name {
        Some(name) => config
            .providers
            .get_backend(&name)
            .with_context(|| format!("model backend not found: {name}"))?,
        None => config
            .providers
            .get_default_backend()
            .context("no model backend configured")?,
    };
    let model = config
        .agent
        .model
        .clone()
        .or_else(|| profile.as_ref().map(|profile| profile.model.clone()))
        .context("no model configured for the selected backend")?;
    let candidate = config
        .agent
        .temperature
        .or_else(|| profile.as_ref().and_then(|profile| profile.temperature))
        .unwrap_or(0.2);
    let temperature = profile
        .as_ref()
        .map(|profile| profile.clamp_temperature(candidate))
        .unwrap_or(candidate);
    Ok((backend, profile, model, temperature))
}

fn build_model_backend(
    backend: &BackendSpec,
    model: &str,
    temperature: f32,
    max_output_tokens: u64,
    max_buffered_events: usize,
) -> anyhow::Result<Arc<dyn ModelBackend>> {
    let api_key = backend
        .resolve_api_key()
        .with_context(|| format!("resolve API key for backend '{}'", backend.name))?;
    let timeout = Duration::from_secs(backend.get_config("timeout_secs").unwrap_or(60));
    let max_context_tokens = backend.get_config("max_context_tokens");
    match backend.kind.trim().to_ascii_lowercase().as_str() {
        "google" | "gemini" => Ok(Arc::new(
            GeminiModelBackend::new(GeminiModelConfig {
                backend_id: format!("google-gemini/{}", backend.name),
                endpoint: backend.endpoint.clone().unwrap_or_else(|| {
                    "https://generativelanguage.googleapis.com/v1beta".to_owned()
                }),
                api_key,
                model: model.to_owned(),
                temperature,
                default_max_output_tokens: max_output_tokens,
                max_context_tokens,
                timeout,
                max_buffered_events,
            })
            .context("build Gemini ModelBackend")?,
        )),
        "openai" | "openrouter" | "deepseek" | "groq" | "xai" | "mistral" => {
            let endpoint = backend.endpoint.clone().or_else(|| match backend.kind.as_str() {
                "openai" => Some("https://api.openai.com/v1".to_owned()),
                "deepseek" => Some("https://api.deepseek.com".to_owned()),
                _ => None,
            }).with_context(|| {
                format!(
                    "OpenAI-compatible backend '{}' requires an endpoint",
                    backend.name
                )
            })?;
            Ok(Arc::new(
                OpenAiCompatibleBackend::new(OpenAiCompatibleConfig {
                    backend_id: format!("openai-compatible/{}", backend.name),
                    endpoint,
                    api_key,
                    model: model.to_owned(),
                    temperature,
                    default_max_output_tokens: max_output_tokens,
                    max_context_tokens,
                    timeout,
                    structured_output: backend
                        .get_config("structured_output")
                        .unwrap_or(true),
                    max_buffered_events,
                })
                .context("build OpenAI-compatible ModelBackend")?,
            ))
        }
        kind => bail!(
            "unsupported ModelBackend kind '{kind}'; supported protocol families are OpenAI-compatible and Gemini Native"
        ),
    }
}

async fn run_turn(
    client: &AgentClient,
    approval_broker: &Arc<InMemoryHostApprovalBroker>,
    turn: u64,
    input: String,
    lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> anyhow::Result<()> {
    let run_id = RunId::new(unique_id("cli-run", turn));
    let handle = client
        .start_with_run_id(run_id.clone(), vec![Content::text(input)])
        .await
        .context("start Agent Run")?;
    let mut events = handle.subscribe().await.context("subscribe to Agent Run")?;
    let mut handled_requests = BTreeSet::new();
    let mut streamed_output = false;
    let mut stdin_open = true;
    let view = loop {
        let view = handle.inspect().await.context("inspect Agent Run")?;
        if is_terminal(view.state.status()) {
            break view;
        }
        if let Some(request) = view
            .pending_requests
            .iter()
            .find(|request| !handled_requests.contains(&request.request_id))
            .cloned()
        {
            handled_requests.insert(request.request_id.clone());
            if !resolve_cli_request(
                client.controller(),
                approval_broker,
                &run_id,
                request,
                lines,
            )
            .await?
            {
                continue;
            }
            continue;
        }
        tokio::select! {
            event = events.recv() => match event {
                Ok(AgentControlEvent::Telemetry(telemetry)) => match telemetry.payload {
                    AgentTelemetry::OutputDelta { delta, .. } => {
                        if let ContentBody::Inline(serde_json::Value::String(text)) = delta.body {
                            print!("{text}");
                            io::stdout().flush().context("flush Agent output delta")?;
                            streamed_output = true;
                        }
                    }
                    AgentTelemetry::ProgressReported { message, fraction } => {
                        if let Some(fraction) = fraction {
                            eprintln!("\n[{:.0}%] {message}", fraction * 100.0);
                        } else {
                            eprintln!("\n{message}");
                        }
                    }
                    _ => {}
                },
                Ok(AgentControlEvent::Durable(_))
                | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    bail!("Agent event stream closed before terminal state")
                }
            },
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for Ctrl-C")?;
                eprintln!("\nCancelling current Agent Run...");
                if let Err(error) = handle.cancel("CLI interrupted by user").await {
                    tracing::debug!(%error, "Agent Run reached terminal while cancellation was sent");
                }
            }
            line = lines.next_line(), if stdin_open => {
                let line = line.context("read running Agent input")?;
                let Some(line) = line else {
                    stdin_open = false;
                    continue;
                };
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }
                if matches!(input, "/cancel" | "/stop") {
                    if let Err(error) = handle.cancel("CLI cancellation command").await {
                        tracing::debug!(%error, "Agent Run reached terminal while cancellation was sent");
                    }
                    continue;
                }
                let ack = handle
                    .steer_text(input.to_owned())
                    .await
                    .context("steer running Agent")?;
                match ack.state {
                    CommandAckState::Accepted { .. } | CommandAckState::Applied { .. } => {
                        eprintln!("\n[steer accepted]");
                    }
                    CommandAckState::Rejected { code, message, .. } => {
                        eprintln!("\n[steer rejected: {code:?}: {message}]");
                    }
                    CommandAckState::Unsupported { feature, .. } => {
                        eprintln!("\n[steer unsupported: {feature}]");
                    }
                    _ => eprintln!("\n[steer acknowledgement pending]"),
                }
            }
        }
    };

    match view.state.status() {
        AgentRunStatus::Delivered => {
            let delivery = view
                .delivery
                .context("Delivered Run omitted its Delivery")?;
            if streamed_output {
                println!();
            } else {
                print_content(&delivery.final_response.body)?;
            }
        }
        status => eprintln!("Agent Run ended with status {status:?}"),
    }
    Ok(())
}

async fn resolve_cli_request(
    controller: &Arc<AgentController>,
    approval_broker: &Arc<InMemoryHostApprovalBroker>,
    run_id: &RunId,
    request: PendingRequest,
    lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> anyhow::Result<bool> {
    let resolution = match &request.payload {
        PendingRequestPayload::Input { prompt, .. } => {
            eprintln!("\nInput required:");
            for item in prompt {
                eprintln!("{}", display_content(item));
            }
            print!("> ");
            io::stdout().flush().context("flush input prompt")?;
            let answer = tokio::select! {
                line = lines.next_line() => line.context("read requested Agent input")?,
                signal = tokio::signal::ctrl_c() => {
                    signal.context("listen for Ctrl-C")?;
                    eprintln!("\nCancelling current Agent Run...");
                    if let Err(error) = controller.cancel(run_id, "CLI interrupted during input request").await {
                        tracing::debug!(%error, "Agent Run reached terminal while cancellation was sent");
                    }
                    return Ok(false);
                }
            };
            let Some(answer) = answer else {
                controller
                    .cancel(run_id, "CLI input closed during input request")
                    .await
                    .context("cancel Agent after stdin closed")?;
                return Ok(false);
            };
            if answer.trim().is_empty() {
                bail!("input response must not be empty")
            }
            RequestResolution::Input {
                content: vec![Content::text(answer)],
            }
        }
        PendingRequestPayload::Approval {
            requested_scope,
            reason,
            ..
        } => {
            eprintln!("\nApproval required: {reason}");
            eprintln!("Effects: {}", requested_scope.join(", "));
            print!("Allow this exact operation? [y/N] ");
            io::stdout().flush().context("flush approval prompt")?;
            let answer = tokio::select! {
                line = lines.next_line() => line.context("read approval decision")?,
                signal = tokio::signal::ctrl_c() => {
                    signal.context("listen for Ctrl-C")?;
                    eprintln!("\nCancelling current Agent Run...");
                    if let Err(error) = controller.cancel(run_id, "CLI interrupted during approval").await {
                        tracing::debug!(%error, "Agent Run reached terminal while cancellation was sent");
                    }
                    return Ok(false);
                }
            };
            let allow = answer.as_deref().is_some_and(|answer| {
                matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
            });
            if allow {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let grant_ref = approval_broker
                    .approve(&request.request_id, now_ms.saturating_add(5 * 60 * 1_000))
                    .context("issue exact Host approval grant")?;
                RequestResolution::Approval {
                    decision: ApprovalDecision::Allow,
                    grant_ref: Some(grant_ref),
                }
            } else {
                RequestResolution::Approval {
                    decision: ApprovalDecision::Deny,
                    grant_ref: None,
                }
            }
        }
        PendingRequestPayload::ExternalAction { .. } => {
            bail!("CLI does not support external action requests")
        }
        _ => bail!("CLI does not support this pending request kind"),
    };
    let command = AgentCommandEnvelope::new(
        CommandId::new(unique_id("cli-request", 0)),
        run_id.clone(),
        Some(request.request_id),
        AgentCommand::ResolveRequest {
            response: resolution,
        },
    )
    .context("build request resolution command")?;
    let ack = controller
        .command(command)
        .await
        .context("resolve Agent request")?;
    match ack.state {
        CommandAckState::Accepted { .. } | CommandAckState::Applied { .. } => Ok(true),
        CommandAckState::Rejected { code, message, .. } => {
            bail!("request resolution was rejected ({code:?}): {message}")
        }
        CommandAckState::Unsupported { feature, .. } => {
            bail!("request resolution is unsupported: {feature}")
        }
        _ => bail!("request resolution returned an unknown acknowledgement state"),
    }
}

fn is_terminal(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Delivered
            | AgentRunStatus::Incomplete
            | AgentRunStatus::Cancelled
            | AgentRunStatus::Failed
    )
}

fn print_content(body: &ContentBody) -> anyhow::Result<()> {
    match body {
        ContentBody::Inline(serde_json::Value::String(text)) => println!("{text}"),
        ContentBody::Inline(value) => println!("{}", serde_json::to_string_pretty(value)?),
        ContentBody::Artifact(artifact) => {
            println!("{}", serde_json::to_string_pretty(artifact)?)
        }
        other => println!("{}", serde_json::to_string_pretty(other)?),
    }
    Ok(())
}

fn display_content(content: &Content) -> String {
    match &content.body {
        ContentBody::Inline(serde_json::Value::String(text)) => text.clone(),
        body => serde_json::to_string(body).unwrap_or_else(|_| "<unprintable content>".to_owned()),
    }
}

fn unique_id(prefix: &str, sequence: u64) -> String {
    let epoch_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{epoch_nanos}-{sequence}", std::process::id())
}
