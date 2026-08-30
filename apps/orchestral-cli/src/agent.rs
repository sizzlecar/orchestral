//! Minimal command-line composition root for the provider-neutral Generic Agent.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use orchestral_agent_journal_fs::FileAgentJournalStore;
use orchestral_blob_fs::FileBlobStore;
use orchestral_core::agent_protocol::{
    reference::AgentRunStatus,
    spi::{AgentJournalStore, InMemoryAgentJournalStore},
    wire::{
        AgentCommand, AgentCommandEnvelope, AgentRunState, AgentSessionId, AgentTelemetry,
        AgentTerminalState, ApprovalDecision, BindingRequirement, CommandAckState, CommandId,
        Content, ContentBody, Digest, PendingRequest, PendingRequestPayload, ProviderBindingRef,
        RequestResolution, ResourceBinding, ResourceBindingId, ResourceBindingMode, ResourceId,
        ResourceKind, ResourceRef, ResourceRevision, RunId,
    },
};
use orchestral_core::agent_session::{AgentSessionJournalStore, InMemoryAgentSessionJournalStore};
use orchestral_core::config::{
    load_config, BackendSpec, McpTransportSpec, ModelProfile, OrchestralConfig,
};
use orchestral_core::io::BlobStore;
use orchestral_core::mcp_protocol::{McpServerId, McpTransportFactory};
use orchestral_core::model_protocol::ModelBackend;
use orchestral_core::skill_protocol::{SkillSourceKind, SKILL_CATALOG_RESOURCE_KIND_V1};
use orchestral_core::tool_effect::{InMemoryToolEffectJournalStore, ToolEffectJournalStore};
use orchestral_core::tool_protocol::{
    ApprovalPolicy, EffectScope, EnvironmentPolicy, FilesystemPolicy, HostApprovalVerifier,
    HostToolPolicy, InMemoryApprovalCapabilityStore, InteractiveCommandPolicy, NetworkPolicy,
    ProcessPolicy, RunToolGrant, SandboxPolicy, ToolPolicyBounds, ToolRestriction,
    TransportLaunchPolicy,
};
use orchestral_mcp_streamable_http::{
    ResolvedCredentialHeader, StreamableHttpMcpTransportConfig, StreamableHttpMcpTransportFactory,
    DEFAULT_MAX_MCP_HTTP_FRAME_BYTES,
};
use orchestral_model_gemini::{
    GeminiAuthentication, GeminiModelBackend, GeminiModelConfig, GoogleCloudAccessTokenProvider,
};
use orchestral_model_openai::{OpenAiCompatibleBackend, OpenAiCompatibleConfig};
use orchestral_runtime::tools::{
    guarded_apply_patch_descriptor, guarded_artifact_read_descriptor, guarded_file_read_descriptor,
    guarded_file_search_descriptor, guarded_file_write_descriptor, guarded_text_search_descriptor,
    workspace_exec_command_descriptor, workspace_write_stdin_descriptor,
    CommandEnvironmentSnapshot, GuardedApplyPatchExecutor, GuardedArtifactReadExecutor,
    GuardedExecCommandExecutor, GuardedFileReadExecutor, GuardedFileSearchExecutor,
    GuardedFileWriteExecutor, GuardedTextSearchExecutor, GuardedWriteStdinExecutor,
};
use orchestral_runtime::{
    AgentClient, AgentControlEvent, AgentController, ContinuationPolicy,
    DeterministicExtractiveSessionSummarizer, GenericAgentCheckpointStore, GenericAgentConfig,
    GuardedMcpServerConfig, GuardedToolRuntime, InMemoryBlobStore,
    InMemoryGenericAgentCheckpointStore, InMemoryHostApprovalBroker, InternalGenericAgentProvider,
    McpToolsAdapterRegistry, ModelTokenMeter, ProcessSupervisor, SessionCompactionPolicy,
    SkillRoot, SkillRuntime, StdioMcpSandboxPolicy, StdioMcpTransportFactory, ToolArtifactStore,
    WorkspacePermissionPolicy,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio_util::sync::CancellationToken;

use crate::google_auth::{
    google_adc_is_explicitly_requested, resolve_google_vertex_auth, GoogleCredentialSource,
};
use crate::runtime::client::prepare_runtime_config_path;
use crate::runtime::ModelOverrides;

pub struct AgentRunOptions {
    pub config: Option<PathBuf>,
    pub credential_file: Option<PathBuf>,
    pub model_overrides: ModelOverrides,
    pub session_id: Option<String>,
    pub system_prompt: Option<String>,
    pub input: Option<String>,
    pub no_mcp: bool,
    pub mcp_config: Vec<PathBuf>,
    pub no_skills: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EntryMode {
    HeadlessPrompt(String),
    HeadlessPipe,
    Tui,
}

struct CliJournalStores {
    run: Arc<dyn AgentJournalStore>,
    session: Arc<dyn AgentSessionJournalStore>,
    effect: Arc<dyn ToolEffectJournalStore>,
    checkpoint: Arc<dyn GenericAgentCheckpointStore>,
}

struct CliExecHost {
    shell: PathBuf,
    runtime_readable_roots: Vec<PathBuf>,
    runtime_readable_files: Vec<PathBuf>,
    environment_names: BTreeSet<String>,
    environment: CommandEnvironmentSnapshot,
    network_targets: BTreeSet<String>,
}

struct CliToolComposition {
    runtime: Arc<GuardedToolRuntime<InMemoryApprovalCapabilityStore>>,
    run_grant: RunToolGrant,
    mcp_restriction: ToolRestriction,
    approval_broker: Arc<InMemoryHostApprovalBroker>,
    process_supervisor: Arc<ProcessSupervisor>,
}

pub async fn run(options: AgentRunOptions) -> anyhow::Result<()> {
    let config_path = prepare_runtime_config_path(
        options.config,
        &options.model_overrides,
        options.credential_file.as_deref(),
    )?;
    let config = load_config(&config_path)
        .with_context(|| format!("load Generic Agent config '{}'", config_path.display()))?;
    let (backend, profile, model, temperature) = resolve_model(&config)?;
    let max_output_tokens = profile
        .as_ref()
        .and_then(|profile| profile.max_tokens)
        .unwrap_or(8_192) as u64;
    let (model_backend, token_meter) = build_model_backend(
        &backend,
        &model,
        temperature,
        max_output_tokens,
        config.agent.stream_buffer,
        options.credential_file.as_deref(),
    )?;

    let mut agent_config = GenericAgentConfig::new("orchestral/internal", "generic-agent");
    agent_config.stream_buffer = config.agent.stream_buffer;
    agent_config.continuation = ContinuationPolicy {
        max_model_steps: config.agent.max_model_steps,
        max_tool_calls: config.agent.max_tool_calls,
    };
    agent_config.history_limit = config.agent.history_limit;
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
        agent_config.system_prompt.push_str(
            "\n\nAdditional Host-configured instructions (these refine, but do not replace, the Agent contract):\n",
        );
        agent_config.system_prompt.push_str(system_prompt.trim());
    }
    let workspace = std::fs::canonicalize(std::env::current_dir().context("resolve workspace")?)
        .context("canonicalize Agent workspace")?;
    agent_config.system_prompt.push_str(&format!(
        "\n\n<environment_context>\n  <cwd>{}</cwd>\n</environment_context>",
        workspace.display()
    ));
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
        configured_mcp_servers(&config, &options.mcp_config)?
    };
    let artifact_store = ToolArtifactStore::new(
        build_cli_blob_store(&config)?,
        config.artifacts.max_bytes,
        config.artifacts.summary_max_chars,
    )
    .context("configure Tool Artifact store")?;
    let CliToolComposition {
        runtime: tool_runtime,
        run_grant,
        mcp_restriction,
        approval_broker,
        process_supervisor,
    } = build_cli_tool_runtime(&config, &mcp_configs, effect_journal, artifact_store)?;
    let mcp_registry = McpToolsAdapterRegistry::register(
        tool_runtime.as_ref(),
        mcp_configs,
        mcp_restriction,
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
        build_cli_skill_runtime(&config)?
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
                token_meter,
            )
        }
        None => InternalGenericAgentProvider::new_with_tools_approval_and_session_journal(
            model_backend,
            agent_config,
            tool_runtime,
            run_grant,
            approval_broker.clone(),
            session_journal,
            token_meter,
        ),
    }
    .context("create Generic Agent provider")?;
    let provider = if config.agent.compaction.enabled {
        provider
            .with_session_compaction(
                Arc::new(
                    DeterministicExtractiveSessionSummarizer::new(
                        config.agent.compaction.summary_max_chars,
                    )
                    .context("configure deterministic Session summarizer")?,
                ),
                SessionCompactionPolicy {
                    minimum_source_records: config.agent.compaction.minimum_source_records,
                    keep_recent_records: config.agent.compaction.keep_recent_records,
                },
            )
            .context("bind Generic Agent Session compaction")?
    } else {
        provider
    };
    let provider = provider
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

    let entry_mode = select_entry_mode(
        options.input,
        io::stdin().is_terminal(),
        io::stdout().is_terminal(),
    )?;
    let result = async {
        match entry_mode {
            EntryMode::HeadlessPrompt(input) => {
                eprintln!("Generic Agent: backend={} model={model}", backend.name);
                let mut lines = BufReader::new(tokio::io::stdin()).lines();
                run_turn(&client, &approval_broker, 1, input, &mut lines, false).await
            }
            EntryMode::HeadlessPipe => {
                eprintln!("Generic Agent: backend={} model={model}", backend.name);
                let mut input = String::new();
                tokio::io::stdin()
                    .read_to_string(&mut input)
                    .await
                    .context("read piped Agent input")?;
                if input.trim().is_empty() {
                    bail!("stdin pipe did not contain an Agent prompt")
                }
                let mut lines = BufReader::new(tokio::io::stdin()).lines();
                run_turn(&client, &approval_broker, 1, input, &mut lines, false).await
            }
            EntryMode::Tui => {
                crate::tui::run_tui(client, approval_broker, process_supervisor, model).await
            }
        }
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
) -> anyhow::Result<CliToolComposition> {
    let workspace = std::fs::canonicalize(std::env::current_dir().context("resolve workspace")?)
        .context("canonicalize workspace")?;
    let workspace = workspace.to_string_lossy().to_string();
    let exec_host = configured_exec_host(config)?;
    let exec_programs = exec_host
        .as_ref()
        .map(|host| BTreeSet::from([host.shell.to_string_lossy().into_owned()]))
        .unwrap_or_default();
    let exec_enabled = exec_host.is_some();
    let mcp_effects = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::effect_scopes)
        .collect::<BTreeSet<_>>();
    let transport_programs = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::allowed_programs)
        .collect::<BTreeSet<_>>();
    let transport_allows_children = mcp_configs
        .iter()
        .any(GuardedMcpServerConfig::allows_child_processes);
    let mcp_readable_roots = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::filesystem_read_roots)
        .collect::<BTreeSet<_>>();
    let mcp_writable_roots = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::filesystem_write_roots)
        .collect::<BTreeSet<_>>();
    let mut allowed_effects = BTreeSet::from([
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::ArtifactRead,
    ]);
    if exec_enabled {
        allowed_effects.extend([
            EffectScope::Process,
            EffectScope::Network,
            EffectScope::EnvironmentRead,
            EffectScope::ExternalSideEffect,
        ]);
    }
    allowed_effects.extend(mcp_effects.iter().copied());
    let mcp_environment = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::environment_names)
        .collect::<BTreeSet<_>>();
    let mut allowed_environment = mcp_environment.clone();
    if let Some(host) = &exec_host {
        allowed_environment.extend(host.environment_names.iter().cloned());
    }
    let mcp_network_targets = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::allowed_network_targets)
        .collect::<BTreeSet<_>>();
    let mut allowed_network_targets = mcp_network_targets.clone();
    if let Some(host) = &exec_host {
        allowed_network_targets.extend(host.network_targets.iter().cloned());
    }
    let allowed_credentials = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::credential_references)
        .collect::<BTreeSet<_>>();
    let mcp_sandbox_profiles = mcp_configs
        .iter()
        .flat_map(GuardedMcpServerConfig::sandbox_profiles)
        .collect::<BTreeSet<_>>();
    let mut readable_roots = BTreeSet::from([workspace.clone()]);
    readable_roots.extend(mcp_readable_roots.iter().cloned());
    let mut writable_roots = BTreeSet::from([workspace.clone()]);
    writable_roots.extend(mcp_writable_roots.iter().cloned());
    let mcp_restriction = ToolRestriction {
        bounds: ToolPolicyBounds {
            allowed_effects: mcp_effects.clone(),
            approval: ApprovalPolicy::Required,
            sandbox: SandboxPolicy {
                required: !mcp_sandbox_profiles.is_empty(),
                allowed_profiles: mcp_sandbox_profiles.clone(),
            },
            process: ProcessPolicy {
                interactive: InteractiveCommandPolicy::default(),
                transport: TransportLaunchPolicy {
                    allowed_programs: transport_programs.clone(),
                    allow_child_processes: transport_allows_children,
                },
            },
            filesystem: FilesystemPolicy {
                readable_roots: mcp_readable_roots,
                writable_roots: mcp_writable_roots,
            },
            network: NetworkPolicy {
                allowed_targets: mcp_network_targets,
                allow_unrestricted: false,
            },
            environment: EnvironmentPolicy {
                allowed_variables: mcp_environment,
                inherit_host_environment: false,
            },
            allowed_credentials: allowed_credentials.clone(),
            max_timeout_ms: Some(config.tools.max_timeout_ms),
            max_output_bytes: Some(config.tools.max_output_bytes),
        },
    };
    let bounds = ToolPolicyBounds {
        allowed_effects,
        approval: ApprovalPolicy::NotRequired,
        sandbox: SandboxPolicy {
            // The Host ceiling admits both sandboxed local capabilities and
            // non-process transports. Each tool lane decides whether its own
            // sandbox is mandatory.
            required: false,
            allowed_profiles: BTreeSet::from([
                "workspace_read".to_owned(),
                orchestral_runtime::tools::GUARDED_EXEC_SANDBOX_PROFILE.to_owned(),
            ])
            .union(&mcp_sandbox_profiles)
            .cloned()
            .collect(),
        },
        process: ProcessPolicy {
            interactive: InteractiveCommandPolicy {
                enabled: exec_enabled,
                command_shells: exec_programs.clone(),
                allow_child_processes: exec_enabled,
            },
            transport: TransportLaunchPolicy {
                allowed_programs: transport_programs,
                allow_child_processes: transport_allows_children,
            },
        },
        filesystem: FilesystemPolicy {
            readable_roots,
            writable_roots,
        },
        network: NetworkPolicy {
            allowed_targets: allowed_network_targets,
            allow_unrestricted: exec_enabled,
        },
        environment: EnvironmentPolicy {
            allowed_variables: allowed_environment,
            inherit_host_environment: false,
        },
        allowed_credentials,
        max_timeout_ms: Some(config.tools.max_timeout_ms),
        max_output_bytes: Some(config.tools.max_output_bytes),
    };
    let mut workspace_bounds = bounds.clone();
    workspace_bounds.allowed_effects = BTreeSet::from([
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::ArtifactRead,
    ]);
    workspace_bounds.sandbox.required = true;
    workspace_bounds.sandbox.allowed_profiles = BTreeSet::from(["workspace_read".to_owned()]);
    workspace_bounds.process = ProcessPolicy::default();
    workspace_bounds.filesystem = FilesystemPolicy {
        readable_roots: BTreeSet::from([workspace.clone()]),
        writable_roots: BTreeSet::from([workspace.clone()]),
    };
    workspace_bounds.network = NetworkPolicy::default();
    workspace_bounds.environment = EnvironmentPolicy::default();
    workspace_bounds.allowed_credentials.clear();
    // Tool restrictions are capability-local. MCP transport programs,
    // credentials, environment names, and network targets never become
    // ambient authority for generic command execution.
    let mut exec_bounds = bounds.clone();
    exec_bounds.allowed_effects = BTreeSet::from([
        EffectScope::Process,
        EffectScope::Network,
        EffectScope::FilesystemRead,
        EffectScope::FilesystemWrite,
        EffectScope::EnvironmentRead,
        EffectScope::ExternalSideEffect,
    ]);
    exec_bounds.sandbox.allowed_profiles =
        BTreeSet::from([orchestral_runtime::tools::GUARDED_EXEC_SANDBOX_PROFILE.to_owned()]);
    exec_bounds.sandbox.required = true;
    exec_bounds.process.interactive = InteractiveCommandPolicy {
        enabled: true,
        command_shells: exec_programs,
        allow_child_processes: true,
    };
    exec_bounds.process.transport = TransportLaunchPolicy::default();
    exec_bounds.filesystem = workspace_bounds.filesystem.clone();
    exec_bounds.network = NetworkPolicy {
        allowed_targets: exec_host
            .as_ref()
            .map(|host| host.network_targets.clone())
            .unwrap_or_default(),
        allow_unrestricted: true,
    };
    exec_bounds.environment = EnvironmentPolicy {
        allowed_variables: exec_host
            .as_ref()
            .map(|host| host.environment_names.clone())
            .unwrap_or_default(),
        inherit_host_environment: false,
    };
    exec_bounds.allowed_credentials.clear();
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
        .context("create guarded Tool Runtime")?
        .with_permission_policy(Arc::new(WorkspacePermissionPolicy)),
    );
    runtime
        .register(
            guarded_artifact_read_descriptor(ToolRestriction {
                bounds: workspace_bounds.clone(),
            }),
            Arc::new(GuardedArtifactReadExecutor::new(artifact_store)),
        )
        .context("register guarded artifact_read Tool")?;
    runtime
        .register(
            guarded_file_read_descriptor(ToolRestriction {
                bounds: workspace_bounds.clone(),
            }),
            Arc::new(
                GuardedFileReadExecutor::new(&workspace)
                    .context("open file_read workspace capability")?,
            ),
        )
        .context("register guarded file_read Tool")?;
    runtime
        .register(
            guarded_file_search_descriptor(ToolRestriction {
                bounds: workspace_bounds.clone(),
            }),
            Arc::new(
                GuardedFileSearchExecutor::new(&workspace)
                    .context("open file_search workspace capability")?,
            ),
        )
        .context("register guarded file_search Tool")?;
    runtime
        .register(
            guarded_text_search_descriptor(ToolRestriction {
                bounds: workspace_bounds.clone(),
            }),
            Arc::new(
                GuardedTextSearchExecutor::new(&workspace)
                    .context("open text_search workspace capability")?,
            ),
        )
        .context("register guarded text_search Tool")?;
    runtime
        .register(
            guarded_file_write_descriptor(ToolRestriction {
                bounds: workspace_bounds.clone(),
            }),
            Arc::new(
                GuardedFileWriteExecutor::new(&workspace)
                    .context("open file_write workspace capability")?,
            ),
        )
        .context("register guarded file_write Tool")?;
    runtime
        .register(
            guarded_apply_patch_descriptor(ToolRestriction {
                bounds: workspace_bounds,
            }),
            Arc::new(
                GuardedApplyPatchExecutor::new(&workspace)
                    .context("open apply_patch workspace capability")?,
            ),
        )
        .context("register guarded apply_patch Tool")?;
    let process_supervisor = Arc::new(
        ProcessSupervisor::new(
            usize::try_from(config.tools.max_output_bytes).unwrap_or(usize::MAX),
        )
        .context("create run-scoped process supervisor")?,
    );
    if let Some(exec_host) = exec_host {
        runtime
            .register(
                workspace_exec_command_descriptor(ToolRestriction {
                    bounds: exec_bounds.clone(),
                }),
                Arc::new(
                    GuardedExecCommandExecutor::new(
                        process_supervisor.clone(),
                        exec_host.shell,
                        exec_host.runtime_readable_roots,
                        exec_host.runtime_readable_files,
                        exec_host.environment,
                    )
                    .map_err(anyhow::Error::msg)
                    .context("configure guarded exec_command Tool")?,
                ),
            )
            .context("register guarded exec_command Tool")?;
        runtime
            .register(
                workspace_write_stdin_descriptor(ToolRestriction {
                    bounds: exec_bounds,
                }),
                Arc::new(GuardedWriteStdinExecutor::new(process_supervisor.clone())),
            )
            .context("register guarded write_stdin Tool")?;
    } else {
        tracing::warn!("Generic Agent command execution is disabled by Host config");
    }
    Ok(CliToolComposition {
        runtime,
        run_grant: RunToolGrant { bounds },
        mcp_restriction,
        approval_broker,
        process_supervisor,
    })
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

fn build_cli_skill_runtime(config: &OrchestralConfig) -> anyhow::Result<Option<Arc<SkillRuntime>>> {
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
            precedence: 10_000_u32.saturating_sub(index as u32),
            required: true,
        });
    }
    if config.skills.auto_discover {
        for (index, relative) in [".claude/skills", ".codex/skills", "skills"]
            .into_iter()
            .enumerate()
        {
            roots.push(SkillRoot {
                path: workspace.join(relative),
                source_kind: SkillSourceKind::Workspace,
                precedence: 1_000_u32.saturating_sub(index as u32),
                required: false,
            });
        }
    }
    if roots.is_empty() {
        return Ok(None);
    }
    let runtime = SkillRuntime::discover(ResourceId::new("cli-skills"), &roots)
        .context("discover Skill catalog")?;
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

fn configured_exec_host(config: &OrchestralConfig) -> anyhow::Result<Option<CliExecHost>> {
    if !config.tools.exec.enabled {
        return Ok(None);
    }
    let configured = config
        .tools
        .exec
        .shell
        .as_deref()
        .filter(|shell| !shell.trim().is_empty());
    let shell = if let Some(shell) = configured {
        PathBuf::from(resolve_host_program(shell)?)
    } else if let Some(shell) = std::env::var_os("SHELL").filter(|shell| !shell.is_empty()) {
        PathBuf::from(resolve_host_program(&shell.to_string_lossy())?)
    } else {
        ["/bin/zsh", "/bin/bash", "/bin/sh"]
            .into_iter()
            .find_map(|candidate| resolve_host_program(candidate).ok())
            .map(PathBuf::from)
            .context("no command shell is available; set tools.exec.shell")?
    };
    let environment_names = [
        "PATH",
        "HOME",
        "USER",
        "LANG",
        "LC_ALL",
        "TERM",
        "COLORTERM",
        "NO_COLOR",
        "CARGO_HOME",
        "RUSTUP_HOME",
        "XDG_CONFIG_HOME",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_NOSYSTEM",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    Ok(Some(CliExecHost {
        runtime_readable_roots: exec_runtime_readable_roots(&shell),
        runtime_readable_files: exec_runtime_readable_files(),
        shell,
        environment: CommandEnvironmentSnapshot::capture(environment_names.iter().cloned()),
        environment_names,
        network_targets: config.tools.exec.network_targets.iter().cloned().collect(),
    }))
}

fn exec_runtime_readable_files() -> Vec<PathBuf> {
    let mut candidates = BTreeSet::new();
    let home = std::env::var_os("HOME").map(PathBuf::from);
    if let Some(home) = &home {
        candidates.insert(home.join(".gitconfig"));
        let xdg_home = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .unwrap_or_else(|| home.join(".config"));
        candidates.insert(xdg_home.join("git/config"));
    }
    for name in ["GIT_CONFIG_GLOBAL", "GIT_CONFIG_SYSTEM"] {
        if let Some(path) = std::env::var_os(name).map(PathBuf::from) {
            if path.is_absolute() {
                candidates.insert(path);
            }
        }
    }
    candidates
        .into_iter()
        .filter_map(|path| std::fs::canonicalize(path).ok())
        .filter(|path| path.is_file())
        .collect()
}

fn exec_runtime_readable_roots(shell: &Path) -> Vec<PathBuf> {
    let mut candidates = BTreeSet::new();
    if let Some(parent) = shell.parent() {
        candidates.insert(parent.to_path_buf());
    }
    if let Some(path) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&path).filter(|path| path.is_absolute()) {
            candidates.insert(directory.clone());
            let text = directory.to_string_lossy();
            // Version-managed runtimes may load adjacent libraries, but never
            // widen a PATH entry to the manager's entire user directory (for
            // example ~/.cargo, which can contain credentials).
            if ["/.nvm/versions/", "/.pyenv/versions/"]
                .iter()
                .any(|marker| text.contains(marker))
                && directory.file_name().is_some_and(|name| name == "bin")
            {
                if let Some(installation) = directory.parent() {
                    candidates.insert(installation.to_path_buf());
                }
            }
            if text.starts_with("/opt/homebrew/") {
                candidates.insert(PathBuf::from("/opt/homebrew"));
            }
            if text.starts_with("/nix/store/") {
                candidates.insert(PathBuf::from("/nix/store"));
            }
        }
    }
    for candidate in [
        "/bin",
        "/usr/bin",
        "/usr/sbin",
        "/usr/local",
        "/opt/homebrew",
        "/nix/store",
        "/etc",
        "/Library/Frameworks",
        "/Library/Developer/CommandLineTools",
        "/Applications/Xcode.app",
    ] {
        candidates.insert(PathBuf::from(candidate));
    }
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        candidates.insert(home.join(".cargo/bin"));
        candidates.insert(home.join(".cargo/registry"));
        candidates.insert(home.join(".cargo/git"));
        candidates.insert(home.join(".rustup"));
    }
    candidates
        .into_iter()
        .filter_map(|path| std::fs::canonicalize(path).ok())
        .filter(|path| path.is_dir())
        .collect()
}

/// MCP and Skill remain distinct resources. Only explicit Host MCP transports
/// are composed here; discovered methods publish as guarded Tools.
fn configured_mcp_servers(
    config: &OrchestralConfig,
    cli_manifest_paths: &[PathBuf],
) -> anyhow::Result<Vec<GuardedMcpServerConfig>> {
    if !config.mcp.enabled {
        return Ok(Vec::new());
    }
    let workspace = std::fs::canonicalize(std::env::current_dir().context("resolve workspace")?)
        .context("canonicalize workspace")?;
    let mut manifest_paths = Vec::new();
    match crate::mcp_config::user_registry_path() {
        Ok(path) if path.is_file() => manifest_paths.push(path),
        Ok(_) => {}
        Err(error) => tracing::debug!(%error, "user MCP registry path is unavailable"),
    }
    manifest_paths.extend(cli_manifest_paths.iter().cloned());
    let specs = crate::mcp_config::load_server_manifests(
        &workspace,
        &config.mcp.servers,
        &config.mcp.import_files,
        &manifest_paths,
    )?;
    let mut servers = Vec::new();
    for spec in specs.iter().filter(|server| server.enabled) {
        let transport = (|| -> anyhow::Result<Arc<dyn McpTransportFactory>> {
            match &spec.transport {
                McpTransportSpec::Stdio {
                    command,
                    args,
                    env,
                    allow_child_processes,
                    cwd,
                    readable_roots,
                    writable_roots,
                    network_targets,
                } => {
                    let program = resolve_host_program(command)?;
                    let cwd =
                        resolve_mcp_directory(&workspace, cwd.as_deref().unwrap_or("."), "cwd")?;
                    let mut reads =
                        resolve_mcp_directories(&workspace, readable_roots, "readable root")?;
                    reads.insert(cwd.clone());
                    let mut writes =
                        resolve_mcp_directories(&workspace, writable_roots, "writable root")?;
                    let runtime_root = prepare_mcp_runtime_root(&workspace, &spec.name)?;
                    writes.insert(runtime_root.clone());
                    Ok(Arc::new(StdioMcpTransportFactory::new(
                        PathBuf::from(program),
                        args.clone(),
                        env.iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                        StdioMcpSandboxPolicy::scoped(
                            cwd,
                            reads,
                            writes,
                            network_targets.iter().cloned().collect(),
                        )
                        .with_child_processes(*allow_child_processes)
                        .with_private_runtime_home(runtime_root),
                    )?))
                }
                McpTransportSpec::StreamableHttp {
                    endpoint,
                    credential_headers,
                    max_frame_bytes,
                } => {
                    let mut resolved = BTreeMap::new();
                    for (header, credential) in credential_headers {
                        let env_name = credential.env.trim();
                        let value = std::env::var(env_name).with_context(|| {
                            format!(
                                "resolve credential environment variable '{env_name}' for header '{header}'"
                            )
                        })?;
                        resolved.insert(
                            header.clone(),
                            ResolvedCredentialHeader {
                                reference: format!("env:{env_name}"),
                                value,
                            },
                        );
                    }
                    Ok(Arc::new(StreamableHttpMcpTransportFactory::new(
                        StreamableHttpMcpTransportConfig {
                            endpoint: endpoint.clone(),
                            credential_headers: resolved,
                            max_frame_bytes: max_frame_bytes
                                .unwrap_or(DEFAULT_MAX_MCP_HTTP_FRAME_BYTES),
                        },
                    )?))
                }
            }
        })();
        let transport = match transport {
            Ok(transport) => transport,
            Err(error) if !spec.required => {
                tracing::warn!(server = spec.name, %error, "optional MCP transport was unavailable");
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("compose required MCP server '{}'", spec.name))
            }
        };
        let server = GuardedMcpServerConfig {
            server_id: McpServerId::new(spec.name.trim()),
            required: spec.required,
            transport,
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

fn resolve_mcp_directories(
    workspace: &Path,
    configured: &[String],
    label: &str,
) -> anyhow::Result<BTreeSet<PathBuf>> {
    configured
        .iter()
        .map(|path| resolve_mcp_directory(workspace, path, label))
        .collect()
}

fn resolve_mcp_directory(
    workspace: &Path,
    configured: &str,
    label: &str,
) -> anyhow::Result<PathBuf> {
    let path = PathBuf::from(configured);
    let path = if path.is_absolute() {
        path
    } else {
        workspace.join(path)
    };
    let canonical = std::fs::canonicalize(&path)
        .with_context(|| format!("canonicalize MCP stdio {label} '{}'", path.display()))?;
    if !canonical.is_dir() {
        bail!("MCP stdio {label} '{}' is not a directory", path.display());
    }
    Ok(canonical)
}

fn prepare_mcp_runtime_root(workspace: &Path, server_name: &str) -> anyhow::Result<PathBuf> {
    let identity = Digest::sha256(server_name.as_bytes());
    let root = workspace
        .join(".orchestral/mcp")
        .join(&identity.as_str()[..16]);
    std::fs::create_dir_all(&root)
        .with_context(|| format!("create MCP runtime directory '{}'", root.display()))?;
    std::fs::canonicalize(&root)
        .with_context(|| format!("canonicalize MCP runtime directory '{}'", root.display()))
}

fn resolve_host_program(program: &str) -> anyhow::Result<String> {
    let candidate = locate_host_program(program)?;
    std::fs::canonicalize(&candidate)
        .with_context(|| format!("canonicalize executable '{}'", candidate.display()))
        .map(|path| path.to_string_lossy().to_string())
}

fn locate_host_program(program: &str) -> anyhow::Result<PathBuf> {
    let candidate = PathBuf::from(program);
    if candidate.is_absolute() {
        if host_executable_file(&candidate) {
            return Ok(candidate);
        }
        bail!("executable is unavailable: {}", candidate.display())
    }
    if program.contains(std::path::MAIN_SEPARATOR) || program.trim().is_empty() {
        bail!("program must be an absolute path or a bare executable name")
    }
    let path = std::env::var_os("PATH").context("PATH is unavailable")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(program);
        if host_executable_file(&candidate) {
            return if candidate.is_absolute() {
                Ok(candidate)
            } else {
                Ok(std::env::current_dir()
                    .context("resolve current directory for relative PATH entry")?
                    .join(candidate))
            };
        }
    }
    bail!("executable was not found on Host PATH")
}

fn host_executable_file(candidate: &Path) -> bool {
    let Ok(metadata) = candidate.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
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
    credential_file: Option<&std::path::Path>,
) -> anyhow::Result<(Arc<dyn ModelBackend>, Arc<dyn ModelTokenMeter>)> {
    let timeout = Duration::from_secs(backend.get_config("timeout_secs").unwrap_or(60));
    let max_context_tokens = backend.get_config("max_context_tokens");
    match backend.kind.trim().to_ascii_lowercase().as_str() {
        "google" | "gemini" => {
            let auth_mode = backend
                .get_config::<String>("auth")
                .unwrap_or_else(|| "auto".to_owned())
                .trim()
                .to_ascii_lowercase();
            if !matches!(auth_mode.as_str(), "auto" | "api_key" | "adc") {
                bail!(
                    "unsupported Google auth mode '{auth_mode}'; expected auto, api_key, or adc"
                );
            }
            let explicit_adc = auth_mode == "adc"
                || google_adc_is_explicitly_requested(credential_file, backend);
            let api_key = (auth_mode != "adc")
                .then(|| backend.resolve_api_key().ok())
                .flatten();
            let vertex_plan = if explicit_adc || api_key.is_none() {
                resolve_google_vertex_auth(credential_file, backend)?
            } else {
                None
            };
            let (authentication, endpoint) = if let Some(plan) = vertex_plan {
                let provider = match &plan.source {
                    GoogleCredentialSource::ServiceAccountFile(path) => {
                        GoogleCloudAccessTokenProvider::from_service_account_file(path)
                    }
                    GoogleCredentialSource::ApplicationDefault => {
                        GoogleCloudAccessTokenProvider::application_default()
                    }
                }
                .context("initialize Google Cloud authentication")?;
                (
                    GeminiAuthentication::AccessTokenProvider(Arc::new(provider)),
                    backend.endpoint.clone().unwrap_or_else(|| plan.endpoint()),
                )
            } else if let Some(api_key) = api_key {
                (
                    GeminiAuthentication::ApiKey(api_key),
                    backend.endpoint.clone().unwrap_or_else(|| {
                        "https://generativelanguage.googleapis.com/v1beta".to_owned()
                    }),
                )
            } else {
                bail!(
                    "no Google credentials found for backend '{}'; use --credential-file, \
                     GOOGLE_APPLICATION_CREDENTIALS, `gcloud auth application-default login`, \
                     or GOOGLE_API_KEY",
                    backend.name
                );
            };
            let backend = Arc::new(
                GeminiModelBackend::new(GeminiModelConfig {
                    backend_id: format!("google-gemini/{}", backend.name),
                    endpoint,
                    authentication,
                    model: model.to_owned(),
                    temperature,
                    thinking_level: None,
                    default_max_output_tokens: max_output_tokens,
                    max_context_tokens,
                    timeout,
                    max_buffered_events,
                })
                .context("build Gemini ModelBackend")?,
            );
            Ok((backend.clone(), backend))
        }
        "openai" | "openrouter" | "deepseek" | "groq" | "xai" | "mistral" => {
            let api_key = backend
                .resolve_api_key()
                .with_context(|| format!("resolve API key for backend '{}'", backend.name))?;
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
            let backend = Arc::new(
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
            );
            Ok((backend.clone(), backend))
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
    accept_unsolicited_stdin: bool,
) -> anyhow::Result<()> {
    let run_id = RunId::new(unique_id("cli-run", turn));
    let handle = client
        .start_with_run_id(run_id.clone(), vec![Content::text(input)])
        .await
        .context("start Agent Run")?;
    let mut events = handle.subscribe().await.context("subscribe to Agent Run")?;
    let mut handled_requests = BTreeSet::new();
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
                    AgentTelemetry::OutputDelta { .. } => {}
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
            line = lines.next_line(), if stdin_open && accept_unsolicited_stdin => {
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

    match &view.state {
        AgentRunState::Terminal {
            terminal: AgentTerminalState::Delivered { .. },
        } => {
            let delivery = view
                .delivery
                .context("Delivered Run omitted its Delivery")?;
            print_content(&delivery.final_response.body)?;
        }
        AgentRunState::Terminal {
            terminal: AgentTerminalState::Failed { failure },
        } => {
            bail!(
                "Agent Run failed [{}]{}: {}",
                failure.code,
                if failure.retryable {
                    " (retryable)"
                } else {
                    ""
                },
                failure.message
            )
        }
        AgentRunState::Terminal {
            terminal: AgentTerminalState::Incomplete { reason },
        } => bail!("Agent Run incomplete: {reason:?}"),
        AgentRunState::Terminal {
            terminal: AgentTerminalState::Cancelled { reason },
        } => bail!("Agent Run cancelled: {reason}"),
        state => bail!("Agent Run stopped in unexpected state: {state:?}"),
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
            eprint!("> ");
            io::stderr().flush().context("flush input prompt")?;
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
            eprint!("Allow this exact operation? [y/N] ");
            io::stderr().flush().context("flush approval prompt")?;
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

fn select_entry_mode(
    input: Option<String>,
    stdin_is_terminal: bool,
    stdout_is_terminal: bool,
) -> anyhow::Result<EntryMode> {
    if let Some(input) = input {
        return Ok(EntryMode::HeadlessPrompt(input));
    }
    if !stdin_is_terminal {
        return Ok(EntryMode::HeadlessPipe);
    }
    if stdout_is_terminal {
        return Ok(EntryMode::Tui);
    }
    bail!(
        "interactive mode requires a TTY on stdout; pass a prompt or pipe stdin for Headless mode"
    )
}

#[cfg(test)]
mod entry_mode_tests {
    use super::{select_entry_mode, EntryMode};

    #[test]
    fn explicit_prompt_is_always_headless() {
        assert_eq!(
            select_entry_mode(Some("fix it".to_owned()), true, true).unwrap(),
            EntryMode::HeadlessPrompt("fix it".to_owned())
        );
        assert_eq!(
            select_entry_mode(Some("fix it".to_owned()), false, false).unwrap(),
            EntryMode::HeadlessPrompt("fix it".to_owned())
        );
    }

    #[test]
    fn pipe_is_headless_and_interactive_ttys_use_tui() {
        assert_eq!(
            select_entry_mode(None, false, true).unwrap(),
            EntryMode::HeadlessPipe
        );
        assert_eq!(select_entry_mode(None, true, true).unwrap(), EntryMode::Tui);
    }

    #[test]
    fn interactive_stdin_without_terminal_output_is_rejected() {
        let error = select_entry_mode(None, true, false).unwrap_err();
        assert!(error.to_string().contains("requires a TTY on stdout"));
    }
}
