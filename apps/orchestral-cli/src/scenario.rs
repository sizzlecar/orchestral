use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::envfile::load_env_file;
use crate::runtime::{PlannerOverrides, RuntimeClient, RuntimeMsg};

#[derive(Debug, Clone)]
pub struct ScenarioRunOptions {
    pub spec: Option<PathBuf>,
    pub env_file: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub planner_overrides: PlannerOverrides,
    pub report: Option<PathBuf>,
    pub thread_id: Option<String>,
    pub no_mcp: bool,
    pub no_skills: bool,
    pub mcp_paths: Vec<PathBuf>,
    pub skill_dirs: Vec<PathBuf>,
    pub timeout_secs: u64,
    pub verbose: bool,
    pub input: Option<String>,
    pub persist_contains: Vec<String>,
    pub persist_not_contains: Vec<String>,
    pub transient_contains: Vec<String>,
    pub transient_not_contains: Vec<String>,
    pub max_approvals: Option<usize>,
    pub max_errors: Option<usize>,
    pub allow_missing_execution_end: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioSpec {
    #[serde(default = "default_spec_version")]
    version: u32,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    env_file: Option<PathBuf>,
    #[serde(default)]
    config: Option<PathBuf>,
    #[serde(default)]
    planner_backend: Option<String>,
    #[serde(default)]
    planner_model_profile: Option<String>,
    #[serde(default)]
    planner_model: Option<String>,
    #[serde(default)]
    planner_temperature: Option<f32>,
    #[serde(default)]
    report: Option<PathBuf>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    fresh_thread_per_turn: bool,
    #[serde(default)]
    no_mcp: bool,
    #[serde(default)]
    no_skills: bool,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    cleanup: ScenarioCleanupSpec,
    #[serde(default)]
    workspace: ScenarioWorkspaceSpec,
    #[serde(default)]
    turns: Vec<ScenarioTurnSpec>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ScenarioCleanupSpec {
    #[serde(default)]
    restore: Vec<ScenarioCleanupTarget>,
    #[serde(default)]
    delete: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ScenarioWorkspaceSpec {
    #[serde(default)]
    copies: Vec<ScenarioWorkspaceCopySpec>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioWorkspaceCopySpec {
    from: PathBuf,
    #[serde(default)]
    to: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioCleanupTarget {
    path: PathBuf,
    #[serde(default)]
    extensions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioTurnSpec {
    #[serde(default)]
    name: Option<String>,
    input: String,
    #[serde(default)]
    expect: ScenarioExpect,
    #[serde(default)]
    verify: Vec<ScenarioVerifySpec>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ScenarioExpect {
    #[serde(default = "default_require_execution_end")]
    require_execution_end: bool,
    #[serde(default)]
    execution_status: Option<String>,
    #[serde(default = "default_zero_usize")]
    max_approvals: Option<usize>,
    #[serde(default = "default_zero_usize")]
    max_errors: Option<usize>,
    #[serde(default)]
    persist_contains: Vec<String>,
    #[serde(default)]
    persist_not_contains: Vec<String>,
    #[serde(default)]
    transient_contains: Vec<String>,
    #[serde(default)]
    transient_not_contains: Vec<String>,
    #[serde(default)]
    activity_contains: Vec<String>,
    #[serde(default)]
    activity_not_contains: Vec<String>,
    #[serde(default)]
    assistant_contains: Vec<String>,
    #[serde(default)]
    assistant_not_contains: Vec<String>,
    #[serde(default)]
    execution_mode_contains: Vec<String>,
    #[serde(default)]
    execution_mode_not_contains: Vec<String>,
    #[serde(default)]
    artifact_contains: Vec<ScenarioArtifactTextExpect>,
    #[serde(default)]
    artifact_not_contains: Vec<ScenarioArtifactTextExpect>,
    #[serde(default)]
    activity_contains_once: Vec<String>,
    #[serde(default)]
    activity_not_repeated: Vec<String>,
    #[serde(default)]
    capture_artifacts: Vec<PathBuf>,
    #[serde(default)]
    artifact_matches_turn: Vec<ScenarioArtifactTurnExpect>,
    #[serde(default)]
    assistant_matches_turn: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioArtifactTextExpect {
    path: PathBuf,
    text: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioArtifactTurnExpect {
    path: PathBuf,
    turn: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ScenarioVerifySpec {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    phase: ScenarioVerifyPhase,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
    #[serde(default)]
    stdout_contains: Vec<String>,
    #[serde(default)]
    stdout_not_contains: Vec<String>,
    #[serde(default)]
    stderr_contains: Vec<String>,
    #[serde(default)]
    stderr_not_contains: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum ScenarioVerifyPhase {
    #[default]
    AfterExecution,
    PostTurn,
}

#[derive(Debug, Clone)]
struct PreparedScenario {
    name: String,
    env_file: Option<PathBuf>,
    config: Option<PathBuf>,
    planner_overrides: PlannerOverrides,
    report: Option<PathBuf>,
    thread_id: Option<String>,
    fresh_thread_per_turn: bool,
    no_mcp: bool,
    no_skills: bool,
    mcp_paths: Vec<PathBuf>,
    skill_dirs: Vec<PathBuf>,
    timeout_secs: u64,
    verbose: bool,
    cleanup: ScenarioCleanupSpec,
    workspace_copies: Vec<PreparedWorkspaceCopy>,
    turns: Vec<ScenarioTurnSpec>,
}

#[derive(Debug, Clone)]
struct PreparedWorkspaceCopy {
    from: PathBuf,
    to: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct ScenarioReport {
    version: u32,
    name: String,
    passed: bool,
    thread_id: String,
    config: Option<String>,
    planner_backend: Option<String>,
    planner_model_profile: Option<String>,
    planner_model: Option<String>,
    planner_temperature: Option<f32>,
    env_file: Option<String>,
    report_path: String,
    timeout_secs: u64,
    started_at_ms: u128,
    finished_at_ms: u128,
    runtime_log_path: Option<String>,
    turns: Vec<TurnReport>,
    failures: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TurnReport {
    name: Option<String>,
    thread_id: String,
    input: String,
    duration_ms: u128,
    planning_started: bool,
    planning_finished: bool,
    execution_started: bool,
    execution_finished: bool,
    execution_status: Option<String>,
    timed_out: bool,
    approvals: Vec<ApprovalReport>,
    errors: Vec<String>,
    persist_lines: Vec<String>,
    transient_lines: Vec<String>,
    activity_lines: Vec<String>,
    assistant_stream: String,
    execution_modes: Vec<String>,
    verifications: Vec<VerificationReport>,
    failures: Vec<String>,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct ApprovalReport {
    reason: String,
    command: Option<String>,
}

#[derive(Debug, Serialize)]
struct VerificationReport {
    name: Option<String>,
    command: String,
    cwd: Option<String>,
    status: Option<i32>,
    stdout: String,
    stderr: String,
    passed: bool,
    failures: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct ScenarioSnapshots {
    assistants: HashMap<String, String>,
    artifacts: HashMap<String, HashMap<PathBuf, String>>,
}

#[derive(Debug, Clone)]
struct ScenarioRunLayout {
    run_id: String,
    run_dir: PathBuf,
    workspace_dir: Option<PathBuf>,
    runtime_log_path: PathBuf,
    runtime_log_archive_path: PathBuf,
    report_path: PathBuf,
}

impl ScenarioRunLayout {
    fn create(
        name: &str,
        explicit_report: Option<&Path>,
        use_workspace: bool,
    ) -> anyhow::Result<Self> {
        let base_dir = absolutize_path(PathBuf::from(".orchestral/scenario-runs"));
        fs::create_dir_all(&base_dir)
            .with_context(|| format!("create scenario run dir '{}' failed", base_dir.display()))?;

        let run_id = format!("{}-{}", sanitize_name(name), now_ms());
        let run_dir = base_dir.join(format!("{}.run", run_id));
        fs::create_dir_all(&run_dir)
            .with_context(|| format!("create scenario temp dir '{}' failed", run_dir.display()))?;

        let report_path = resolve_report_path(name, explicit_report, Some(&run_id))?;
        let runtime_log_archive_path = report_path.with_extension("runtime.log");
        let runtime_log_path = run_dir.join("logs/orchestral-runtime.log");
        let workspace_dir = if use_workspace {
            let dir = run_dir.join("workspace");
            fs::create_dir_all(&dir)
                .with_context(|| format!("create scenario workspace '{}' failed", dir.display()))?;
            Some(dir)
        } else {
            None
        };

        Ok(Self {
            run_id,
            run_dir,
            workspace_dir,
            runtime_log_path,
            runtime_log_archive_path,
            report_path,
        })
    }

    fn cleanup_run_dir(&self) -> anyhow::Result<()> {
        remove_path_if_exists(&self.run_dir)
    }
}

#[derive(Debug)]
struct CurrentDirGuard {
    original: PathBuf,
}

impl CurrentDirGuard {
    fn enter(path: &Path) -> anyhow::Result<Self> {
        let original = std::env::current_dir().context("resolve current dir failed")?;
        std::env::set_current_dir(path)
            .with_context(|| format!("set current dir to '{}' failed", path.display()))?;
        Ok(Self { original })
    }
}

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.original);
    }
}

#[derive(Debug)]
struct EnvVarGuard {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(previous) = &self.previous {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

#[derive(Debug)]
struct ScenarioEnvGuard {
    _guards: Vec<EnvVarGuard>,
}

impl ScenarioEnvGuard {
    fn apply(
        no_mcp: bool,
        no_skills: bool,
        mcp_paths: &[PathBuf],
        skill_dirs: &[PathBuf],
        layout: &ScenarioRunLayout,
    ) -> anyhow::Result<Self> {
        if let Some(parent) = layout.runtime_log_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "create scenario runtime log dir '{}' failed",
                    parent.display()
                )
            })?;
        }

        let mut guards = vec![
            EnvVarGuard::set("ORCHESTRAL_LOG_FILE", &layout.runtime_log_path),
            EnvVarGuard::set("ORCHESTRAL_SCENARIO_RUN_ID", &layout.run_id),
            EnvVarGuard::set("ORCHESTRAL_SCENARIO_RUN_DIR", &layout.run_dir),
        ];

        if let Some(workspace_dir) = &layout.workspace_dir {
            guards.push(EnvVarGuard::set(
                "ORCHESTRAL_SCENARIO_WORKSPACE_DIR",
                workspace_dir,
            ));
        }
        if no_mcp {
            guards.push(EnvVarGuard::set("ORCHESTRAL_DISABLE_MCP", "1"));
        }
        if no_skills {
            guards.push(EnvVarGuard::set("ORCHESTRAL_DISABLE_SKILLS", "1"));
        }
        if !mcp_paths.is_empty() {
            let joined = mcp_paths
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(":");
            guards.push(EnvVarGuard::set("ORCHESTRAL_MCP_EXTRA_PATHS", joined));
        }
        if !skill_dirs.is_empty() {
            let joined = skill_dirs
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(":");
            guards.push(EnvVarGuard::set("ORCHESTRAL_SKILL_EXTRA_DIRS", joined));
        }

        Ok(Self { _guards: guards })
    }
}

pub async fn run(options: ScenarioRunOptions) -> anyhow::Result<()> {
    let prepared = prepare_scenario(options)?;

    if let Some(env_file) = &prepared.env_file {
        let loaded = load_env_file(env_file)?;
        println!("Loaded {} env var(s) from {}", loaded, env_file.display());
    }

    ensure_log_filter();
    let cleanup_session = ScenarioCleanupSession::capture(&prepared.cleanup)
        .context("capture scenario cleanup snapshot")?;
    let layout = ScenarioRunLayout::create(
        &prepared.name,
        prepared.report.as_deref(),
        !prepared.workspace_copies.is_empty(),
    )?;
    materialize_workspace(&prepared.workspace_copies, layout.workspace_dir.as_deref())?;

    let env_guard = ScenarioEnvGuard::apply(
        prepared.no_mcp,
        prepared.no_skills,
        &prepared.mcp_paths,
        &prepared.skill_dirs,
        &layout,
    )?;
    let _cwd_guard = layout
        .workspace_dir
        .as_deref()
        .map(CurrentDirGuard::enter)
        .transpose()?;
    let executed = execute_prepared_scenario(&prepared, layout.report_path.clone()).await;
    drop(_cwd_guard);
    drop(env_guard);

    let archived_runtime_log =
        archive_runtime_log(&layout.runtime_log_path, &layout.runtime_log_archive_path)?;
    let cleanup_result = cleanup_session.restore();
    let run_dir_cleanup = layout.cleanup_run_dir();

    let (mut report, report_path) = match executed {
        Ok(value) => value,
        Err(err) => {
            if let Err(cleanup_err) = run_dir_cleanup {
                return Err(err.context(format!(
                    "scenario temp cleanup also failed: {}",
                    cleanup_err
                )));
            }
            if let Err(cleanup_err) = cleanup_result {
                return Err(err.context(format!("scenario cleanup also failed: {}", cleanup_err)));
            }
            return Err(err);
        }
    };
    report.runtime_log_path = archived_runtime_log
        .as_ref()
        .map(|path| path.display().to_string());
    if let Err(cleanup_err) = cleanup_result {
        report
            .failures
            .push(format!("cleanup failed: {}", cleanup_err));
        report.passed = false;
    }
    if let Err(cleanup_err) = run_dir_cleanup {
        report
            .failures
            .push(format!("scenario temp cleanup failed: {}", cleanup_err));
        report.passed = false;
    }

    write_report(&report_path, &report)?;

    if report.passed {
        println!(
            "SCENARIO PASS name={} thread_id={} report={}",
            report.name, report.thread_id, report.report_path
        );
        return Ok(());
    }

    for failure in &report.failures {
        eprintln!("FAIL: {}", failure);
    }
    bail!(
        "scenario '{}' failed; report written to {}",
        report.name,
        report.report_path
    )
}

async fn execute_prepared_scenario(
    prepared: &PreparedScenario,
    report_path: PathBuf,
) -> anyhow::Result<(ScenarioReport, PathBuf)> {
    let started_at_ms = now_ms();
    let shared_runtime_client = if prepared.fresh_thread_per_turn {
        None
    } else {
        Some(
            build_runtime_client(prepared, prepared.thread_id.clone())
                .await
                .context("initialize runtime client for scenario")?,
        )
    };
    let thread_id = shared_runtime_client
        .as_ref()
        .map(|client| client.thread_id().to_string())
        .unwrap_or_else(|| "fresh-per-turn".to_string());

    if prepared.verbose {
        if prepared.fresh_thread_per_turn {
            println!(
                "Scenario name={} thread_mode=fresh-per-turn timeout={}s report={}",
                prepared.name,
                prepared.timeout_secs,
                report_path.display()
            );
        } else {
            println!(
                "Scenario name={} thread_id={} timeout={}s report={}",
                prepared.name,
                thread_id,
                prepared.timeout_secs,
                report_path.display()
            );
        }
    }

    let mut reports = Vec::with_capacity(prepared.turns.len());
    let mut failures = Vec::new();
    let workspace_dir = std::env::var_os("ORCHESTRAL_SCENARIO_WORKSPACE_DIR").map(PathBuf::from);
    let mut snapshots = ScenarioSnapshots::default();
    for (index, turn) in prepared.turns.iter().enumerate() {
        let turn_label = turn
            .name
            .clone()
            .unwrap_or_else(|| format!("turn-{}", index + 1));
        let runtime_client = match &shared_runtime_client {
            Some(client) => client.clone(),
            None => build_runtime_client(prepared, None)
                .await
                .with_context(|| format!("initialize runtime client for turn '{}'", turn_label))?,
        };
        let mut turn_report = execute_turn(
            runtime_client,
            turn.clone(),
            prepared.timeout_secs,
            workspace_dir.as_deref(),
            &snapshots,
            &turn_label,
        )
        .await?;
        snapshots
            .assistants
            .insert(turn_label.clone(), turn_report.assistant_stream.clone());
        let captured_artifacts =
            capture_turn_artifacts(&turn.expect.capture_artifacts, workspace_dir.as_deref())?;
        if !captured_artifacts.is_empty() {
            snapshots
                .artifacts
                .insert(turn_label.clone(), captured_artifacts);
        }
        run_post_turn_verifications(&mut turn_report, &turn.verify).await;
        turn_report.passed = turn_report.failures.is_empty();
        if !turn_report.passed {
            failures.push(format!("{} failed", turn_label));
        }
        reports.push(turn_report);
    }

    let finished_at_ms = now_ms();
    let passed = failures.is_empty();
    let report = ScenarioReport {
        version: 1,
        name: prepared.name.clone(),
        passed,
        thread_id,
        config: prepared
            .config
            .as_ref()
            .map(|path| path.display().to_string()),
        planner_backend: prepared.planner_overrides.backend.clone(),
        planner_model_profile: prepared.planner_overrides.model_profile.clone(),
        planner_model: prepared.planner_overrides.model.clone(),
        planner_temperature: prepared.planner_overrides.temperature,
        env_file: prepared
            .env_file
            .as_ref()
            .map(|path| path.display().to_string()),
        report_path: report_path.display().to_string(),
        timeout_secs: prepared.timeout_secs,
        started_at_ms,
        finished_at_ms,
        runtime_log_path: None,
        turns: reports,
        failures,
    };
    Ok((report, report_path))
}

fn archive_runtime_log(source: &Path, destination: &Path) -> anyhow::Result<Option<PathBuf>> {
    if !source.exists() {
        return Ok(None);
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create runtime log archive dir '{}' failed",
                parent.display()
            )
        })?;
    }
    fs::copy(source, destination).with_context(|| {
        format!(
            "archive runtime log '{}' -> '{}' failed",
            source.display(),
            destination.display()
        )
    })?;
    Ok(Some(destination.to_path_buf()))
}

fn prepare_scenario(options: ScenarioRunOptions) -> anyhow::Result<PreparedScenario> {
    if let Some(spec_path) = &options.spec {
        if options.input.is_some()
            || !options.persist_contains.is_empty()
            || !options.persist_not_contains.is_empty()
            || !options.transient_contains.is_empty()
            || !options.transient_not_contains.is_empty()
            || options.max_approvals.is_some()
            || options.max_errors.is_some()
            || options.allow_missing_execution_end
        {
            bail!("cannot combine --spec with ad-hoc scenario assertions or input");
        }
        let raw = fs::read_to_string(spec_path)
            .with_context(|| format!("read scenario spec '{}' failed", spec_path.display()))?;
        let mut spec: ScenarioSpec = serde_yaml::from_str(&raw)
            .with_context(|| format!("parse scenario spec '{}' failed", spec_path.display()))?;
        if spec.version != 1 {
            bail!("unsupported scenario spec version {}", spec.version);
        }
        if spec.turns.is_empty() {
            bail!(
                "scenario spec '{}' must define at least one turn",
                spec_path.display()
            );
        }
        if spec.fresh_thread_per_turn && (options.thread_id.is_some() || spec.thread_id.is_some()) {
            bail!(
                "scenario spec '{}' cannot combine thread_id with fresh_thread_per_turn",
                spec_path.display()
            );
        }
        let base_dir = spec_path.parent().unwrap_or_else(|| Path::new("."));
        spec.env_file = options
            .env_file
            .or_else(|| spec.env_file.take().map(|p| resolve_relative(base_dir, p)));
        spec.config = options
            .config
            .or_else(|| spec.config.take().map(|p| resolve_relative(base_dir, p)));
        spec.report = options
            .report
            .or_else(|| spec.report.take().map(|p| resolve_relative(base_dir, p)));
        for target in &mut spec.cleanup.restore {
            target.path = resolve_relative(base_dir, target.path.clone());
        }
        spec.cleanup.delete = spec
            .cleanup
            .delete
            .into_iter()
            .map(|path| resolve_relative(base_dir, path))
            .collect();
        for turn in &mut spec.turns {
            for verify in &mut turn.verify {
                if let Some(cwd) = verify.cwd.take() {
                    verify.cwd = Some(resolve_relative(base_dir, cwd));
                }
                if command_looks_like_path(&verify.command) {
                    verify.command = resolve_relative(base_dir, PathBuf::from(&verify.command))
                        .display()
                        .to_string();
                }
                if command_looks_like_python(&verify.command) {
                    if let Some(script_arg) = verify.args.first_mut() {
                        if command_looks_like_path(script_arg) {
                            *script_arg =
                                resolve_relative(base_dir, PathBuf::from(script_arg.as_str()))
                                    .display()
                                    .to_string();
                        }
                    }
                }
            }
        }
        let workspace_copies = spec
            .workspace
            .copies
            .into_iter()
            .map(|copy| prepare_workspace_copy(base_dir, copy))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let planner_overrides = merge_planner_overrides(
            PlannerOverrides {
                backend: spec.planner_backend,
                model_profile: spec.planner_model_profile,
                model: spec.planner_model,
                temperature: spec.planner_temperature,
            },
            options.planner_overrides,
        );
        return Ok(PreparedScenario {
            name: spec
                .name
                .unwrap_or_else(|| scenario_name_from_path(spec_path.as_path())),
            env_file: spec.env_file,
            config: spec.config,
            planner_overrides,
            report: spec.report,
            thread_id: options.thread_id.or(spec.thread_id),
            fresh_thread_per_turn: spec.fresh_thread_per_turn,
            no_mcp: options.no_mcp || spec.no_mcp,
            no_skills: options.no_skills || spec.no_skills,
            mcp_paths: options.mcp_paths,
            skill_dirs: options.skill_dirs,
            timeout_secs: spec.timeout_secs.unwrap_or(options.timeout_secs),
            verbose: options.verbose,
            cleanup: spec.cleanup,
            workspace_copies,
            turns: spec.turns,
        });
    }

    let input = options
        .input
        .ok_or_else(|| anyhow::anyhow!("scenario requires either --spec or positional INPUT"))?;
    Ok(PreparedScenario {
        name: "adhoc-scenario".to_string(),
        env_file: options.env_file,
        config: options.config,
        planner_overrides: options.planner_overrides,
        report: options.report,
        thread_id: options.thread_id,
        fresh_thread_per_turn: false,
        no_mcp: options.no_mcp,
        no_skills: options.no_skills,
        mcp_paths: options.mcp_paths,
        skill_dirs: options.skill_dirs,
        timeout_secs: options.timeout_secs,
        verbose: options.verbose,
        cleanup: ScenarioCleanupSpec::default(),
        workspace_copies: Vec::new(),
        turns: vec![ScenarioTurnSpec {
            name: Some("turn-1".to_string()),
            input,
            expect: ScenarioExpect {
                require_execution_end: !options.allow_missing_execution_end,
                execution_status: None,
                max_approvals: options.max_approvals.or(Some(0)),
                max_errors: options.max_errors.or(Some(0)),
                persist_contains: options.persist_contains,
                persist_not_contains: options.persist_not_contains,
                transient_contains: options.transient_contains,
                transient_not_contains: options.transient_not_contains,
                activity_contains: Vec::new(),
                activity_not_contains: Vec::new(),
                assistant_contains: Vec::new(),
                assistant_not_contains: Vec::new(),
                execution_mode_contains: Vec::new(),
                execution_mode_not_contains: Vec::new(),
                artifact_contains: Vec::new(),
                artifact_not_contains: Vec::new(),
                activity_contains_once: Vec::new(),
                activity_not_repeated: Vec::new(),
                capture_artifacts: Vec::new(),
                artifact_matches_turn: Vec::new(),
                assistant_matches_turn: Vec::new(),
            },
            verify: Vec::new(),
        }],
    })
}

fn merge_planner_overrides(base: PlannerOverrides, cli: PlannerOverrides) -> PlannerOverrides {
    PlannerOverrides {
        backend: cli.backend.or(base.backend),
        model_profile: cli.model_profile.or(base.model_profile),
        model: cli.model.or(base.model),
        temperature: cli.temperature.or(base.temperature),
    }
}

fn prepare_workspace_copy(
    base_dir: &Path,
    copy: ScenarioWorkspaceCopySpec,
) -> anyhow::Result<PreparedWorkspaceCopy> {
    if copy.to.as_ref().is_some_and(|path| path.is_absolute()) {
        bail!("scenario workspace copy target must be relative to the workspace root");
    }
    Ok(PreparedWorkspaceCopy {
        from: resolve_relative(base_dir, copy.from),
        to: copy.to,
    })
}

async fn execute_turn(
    runtime_client: RuntimeClient,
    turn: ScenarioTurnSpec,
    timeout_secs: u64,
    workspace_dir: Option<&Path>,
    snapshots: &ScenarioSnapshots,
    turn_label: &str,
) -> anyhow::Result<TurnReport> {
    let (tx, mut rx) = mpsc::channel::<RuntimeMsg>(256);
    let input = turn.input.clone();
    let submit_client = runtime_client.clone();
    let submit = tokio::spawn(async move { submit_client.submit_input(input, tx).await });

    let started_at = Instant::now();
    let mut report = TurnReport {
        name: turn.name.clone(),
        thread_id: runtime_client.thread_id().to_string(),
        input: turn.input.clone(),
        duration_ms: 0,
        planning_started: false,
        planning_finished: false,
        execution_started: false,
        execution_finished: false,
        execution_status: None,
        timed_out: false,
        approvals: Vec::new(),
        errors: Vec::new(),
        persist_lines: Vec::new(),
        transient_lines: Vec::new(),
        activity_lines: Vec::new(),
        assistant_stream: String::new(),
        execution_modes: Vec::new(),
        verifications: Vec::new(),
        failures: Vec::new(),
        passed: false,
    };

    let deadline = tokio::time::sleep(Duration::from_secs(timeout_secs));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            maybe_msg = rx.recv() => {
                let Some(msg) = maybe_msg else {
                    break;
                };
                handle_runtime_msg(&mut report, msg);
            }
            _ = &mut deadline => {
                report.timed_out = true;
                report.failures.push(format!("turn timed out after {} seconds", timeout_secs));
                submit.abort();
                break;
            }
        }
    }

    if !report.timed_out {
        match submit.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => report.errors.push(err.to_string()),
            Err(err) if err.is_cancelled() => {}
            Err(err) => report
                .errors
                .push(format!("scenario submit join failed: {}", err)),
        }
    }

    report.duration_ms = started_at.elapsed().as_millis();
    let after_execution_verifications = turn
        .verify
        .iter()
        .filter(|verify| verify.phase == ScenarioVerifyPhase::AfterExecution)
        .cloned()
        .collect::<Vec<_>>();
    if report.execution_finished && !after_execution_verifications.is_empty() {
        report.verifications = run_verifications(&after_execution_verifications).await;
        for verification in &report.verifications {
            if !verification.passed {
                let label = verification
                    .name
                    .clone()
                    .unwrap_or_else(|| verification.command.clone());
                report
                    .failures
                    .push(format!("verification '{}' failed", label));
            }
        }
    }
    evaluate_expectations(
        &mut report,
        &turn.expect,
        workspace_dir,
        snapshots,
        turn_label,
    );
    report.passed = report.failures.is_empty();
    Ok(report)
}

async fn run_post_turn_verifications(report: &mut TurnReport, verify: &[ScenarioVerifySpec]) {
    let post_turn = verify
        .iter()
        .filter(|item| item.phase == ScenarioVerifyPhase::PostTurn)
        .cloned()
        .collect::<Vec<_>>();
    if post_turn.is_empty() {
        return;
    }
    let results = run_verifications(&post_turn).await;
    for verification in &results {
        if !verification.passed {
            let label = verification
                .name
                .clone()
                .unwrap_or_else(|| verification.command.clone());
            report
                .failures
                .push(format!("verification '{}' failed", label));
        }
    }
    report.verifications.extend(results);
}

async fn build_runtime_client(
    prepared: &PreparedScenario,
    thread_id_override: Option<String>,
) -> anyhow::Result<RuntimeClient> {
    RuntimeClient::from_config(
        prepared.config.clone(),
        thread_id_override,
        prepared.planner_overrides.clone(),
    )
    .await
}

fn handle_runtime_msg(report: &mut TurnReport, msg: RuntimeMsg) {
    match msg {
        RuntimeMsg::PlanningStart => report.planning_started = true,
        RuntimeMsg::PlanningEnd => report.planning_finished = true,
        RuntimeMsg::ExecutionStart { execution_mode, .. } => {
            report.execution_started = true;
            if let Some(mode) = execution_mode {
                report.execution_modes.push(mode);
            }
        }
        RuntimeMsg::ExecutionProgress { step } => {
            report
                .transient_lines
                .push(format!("[progress] step={}", step));
        }
        RuntimeMsg::ExecutionEnd => report.execution_finished = true,
        RuntimeMsg::ActivityStart {
            kind,
            step_id,
            action,
            input_summary,
        } => {
            let mut line = format!("[start {:?}] {} {}", kind, step_id, action);
            if let Some(summary) = input_summary {
                line.push_str(&format!(" | {}", summary));
            }
            report.activity_lines.push(line);
        }
        RuntimeMsg::ActivityItem {
            step_id,
            action,
            line,
        } => {
            report
                .activity_lines
                .push(format!("[item] {} {} | {}", step_id, action, line));
        }
        RuntimeMsg::ActivityEnd {
            step_id,
            action,
            failed,
        } => {
            report
                .activity_lines
                .push(format!("[end] {} {} failed={}", step_id, action, failed));
        }
        RuntimeMsg::OutputPersist(line) => {
            if let Some(status) = parse_status_line(&line) {
                report.execution_status = Some(status);
            }
            report.persist_lines.push(line)
        }
        RuntimeMsg::AssistantOutput(message) => {
            report.assistant_stream = message;
        }
        RuntimeMsg::AssistantDelta { chunk, done } => {
            report.assistant_stream.push_str(&chunk);
            if done {
                report.assistant_stream.push('\n');
            }
        }
        RuntimeMsg::OutputTransient { slot, text } => {
            report
                .transient_lines
                .push(format!("[{:?}] {}", slot, text));
        }
        RuntimeMsg::ApprovalRequested { reason, command } => {
            report.approvals.push(ApprovalReport { reason, command })
        }
        RuntimeMsg::Error(err) => report.errors.push(err),
    }
}

fn evaluate_expectations(
    report: &mut TurnReport,
    expect: &ScenarioExpect,
    workspace_dir: Option<&Path>,
    snapshots: &ScenarioSnapshots,
    turn_label: &str,
) {
    if expect.require_execution_end && !report.execution_finished {
        report
            .failures
            .push("expected execution to finish, but ExecutionEnd was not observed".to_string());
    }
    if let Some(expected_status) = expect.execution_status.as_deref() {
        if report.execution_status.as_deref() != Some(expected_status) {
            report.failures.push(format!(
                "expected execution status '{}', got '{}'",
                expected_status,
                report.execution_status.as_deref().unwrap_or("<none>")
            ));
        }
    } else if matches!(report.execution_status.as_deref(), Some("failed")) {
        report
            .failures
            .push("expected successful execution, but runtime reported Status: failed".to_string());
    }
    if let Some(max_approvals) = expect.max_approvals {
        if report.approvals.len() > max_approvals {
            report.failures.push(format!(
                "expected at most {} approvals, got {}",
                max_approvals,
                report.approvals.len()
            ));
        }
    }
    if let Some(max_errors) = expect.max_errors {
        if report.errors.len() > max_errors {
            report.failures.push(format!(
                "expected at most {} errors, got {}",
                max_errors,
                report.errors.len()
            ));
        }
    }

    assert_contains(
        "persist",
        &report.persist_lines,
        &expect.persist_contains,
        &mut report.failures,
    );
    assert_not_contains(
        "persist",
        &report.persist_lines,
        &expect.persist_not_contains,
        &mut report.failures,
    );
    assert_contains(
        "transient",
        &report.transient_lines,
        &expect.transient_contains,
        &mut report.failures,
    );
    assert_not_contains(
        "transient",
        &report.transient_lines,
        &expect.transient_not_contains,
        &mut report.failures,
    );
    assert_contains(
        "activity",
        &report.activity_lines,
        &expect.activity_contains,
        &mut report.failures,
    );
    assert_not_contains(
        "activity",
        &report.activity_lines,
        &expect.activity_not_contains,
        &mut report.failures,
    );
    assert_text_contains(
        "assistant",
        &report.assistant_stream,
        &expect.assistant_contains,
        &mut report.failures,
    );
    assert_text_not_contains(
        "assistant",
        &report.assistant_stream,
        &expect.assistant_not_contains,
        &mut report.failures,
    );
    assert_contains(
        "execution_mode",
        &report.execution_modes,
        &expect.execution_mode_contains,
        &mut report.failures,
    );
    assert_not_contains(
        "execution_mode",
        &report.execution_modes,
        &expect.execution_mode_not_contains,
        &mut report.failures,
    );
    assert_artifact_contains(
        "artifact",
        &expect.artifact_contains,
        workspace_dir,
        &mut report.failures,
    );
    assert_artifact_not_contains(
        "artifact",
        &expect.artifact_not_contains,
        workspace_dir,
        &mut report.failures,
    );
    assert_line_occurrence(
        "activity",
        &report.activity_lines,
        &expect.activity_contains_once,
        1,
        1,
        &mut report.failures,
    );
    assert_line_occurrence(
        "activity",
        &report.activity_lines,
        &expect.activity_not_repeated,
        0,
        1,
        &mut report.failures,
    );
    assert_artifact_matches_turn(
        &expect.artifact_matches_turn,
        workspace_dir,
        snapshots,
        &mut report.failures,
    );
    assert_assistant_matches_turn(
        &report.assistant_stream,
        &expect.assistant_matches_turn,
        snapshots,
        turn_label,
        &mut report.failures,
    );
}

fn materialize_workspace(
    copies: &[PreparedWorkspaceCopy],
    workspace_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let Some(workspace_dir) = workspace_dir else {
        return Ok(());
    };

    for copy in copies {
        let destination = workspace_copy_destination(workspace_dir, copy)?;
        if copy.from.is_dir() {
            copy_directory_recursive(&copy.from, &destination)?;
        } else {
            copy_file_to_destination(&copy.from, &destination)?;
        }
    }
    Ok(())
}

fn workspace_copy_destination(
    workspace_dir: &Path,
    copy: &PreparedWorkspaceCopy,
) -> anyhow::Result<PathBuf> {
    let relative = match &copy.to {
        Some(path) => path.clone(),
        None => copy
            .from
            .file_name()
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("workspace copy source has no file name"))?,
    };
    Ok(workspace_dir.join(relative))
}

fn copy_file_to_destination(source: &Path, destination: &Path) -> anyhow::Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create workspace dir '{}' failed", parent.display()))?;
    }
    fs::copy(source, destination).with_context(|| {
        format!(
            "copy workspace file '{}' -> '{}' failed",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn copy_directory_recursive(source: &Path, destination: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(destination)
        .with_context(|| format!("create workspace dir '{}' failed", destination.display()))?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("read source dir '{}' failed", source.display()))?
    {
        let entry =
            entry.with_context(|| format!("read dir entry in '{}' failed", source.display()))?;
        let path = entry.path();
        let target = destination.join(entry.file_name());
        if path.is_dir() {
            copy_directory_recursive(&path, &target)?;
        } else if path.is_file() {
            copy_file_to_destination(&path, &target)?;
        }
    }
    Ok(())
}

async fn run_verifications(specs: &[ScenarioVerifySpec]) -> Vec<VerificationReport> {
    let mut reports = Vec::with_capacity(specs.len());
    for spec in specs {
        reports.push(run_verification(spec).await);
    }
    reports
}

async fn run_verification(spec: &ScenarioVerifySpec) -> VerificationReport {
    let mut command = Command::new(&spec.command);
    command.args(&spec.args);
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    for (key, value) in &spec.env {
        command.env(key, value);
    }

    let output = if let Some(timeout_secs) = spec.timeout_secs {
        match tokio::time::timeout(Duration::from_secs(timeout_secs), command.output()).await {
            Ok(result) => result,
            Err(_) => {
                return VerificationReport {
                    name: spec.name.clone(),
                    command: render_verify_command(spec),
                    cwd: spec.cwd.as_ref().map(|path| path.display().to_string()),
                    status: None,
                    stdout: String::new(),
                    stderr: String::new(),
                    passed: false,
                    failures: vec![format!("timed out after {} second(s)", timeout_secs)],
                };
            }
        }
    } else {
        command.output().await
    };

    let mut report = match output {
        Ok(output) => VerificationReport {
            name: spec.name.clone(),
            command: render_verify_command(spec),
            cwd: spec.cwd.as_ref().map(|path| path.display().to_string()),
            status: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            passed: output.status.success(),
            failures: Vec::new(),
        },
        Err(err) => VerificationReport {
            name: spec.name.clone(),
            command: render_verify_command(spec),
            cwd: spec.cwd.as_ref().map(|path| path.display().to_string()),
            status: None,
            stdout: String::new(),
            stderr: String::new(),
            passed: false,
            failures: vec![format!("spawn failed: {}", err)],
        },
    };

    if report.passed {
        collect_verify_assertions(
            "stdout",
            &report.stdout,
            &spec.stdout_contains,
            true,
            &mut report.failures,
        );
        collect_verify_assertions(
            "stdout",
            &report.stdout,
            &spec.stdout_not_contains,
            false,
            &mut report.failures,
        );
        collect_verify_assertions(
            "stderr",
            &report.stderr,
            &spec.stderr_contains,
            true,
            &mut report.failures,
        );
        collect_verify_assertions(
            "stderr",
            &report.stderr,
            &spec.stderr_not_contains,
            false,
            &mut report.failures,
        );
        if !report.failures.is_empty() {
            report.passed = false;
        }
    }

    if !report.passed && report.failures.is_empty() {
        report
            .failures
            .push(format!("exit status was {:?}", report.status.unwrap_or(-1)));
    }

    report
}

fn render_verify_command(spec: &ScenarioVerifySpec) -> String {
    if spec.args.is_empty() {
        return spec.command.clone();
    }
    format!("{} {}", spec.command, spec.args.join(" "))
}

fn collect_verify_assertions(
    label: &str,
    haystack: &str,
    needles: &[String],
    should_contain: bool,
    failures: &mut Vec<String>,
) {
    for needle in needles {
        let contains = haystack.contains(needle);
        if should_contain && !contains {
            failures.push(format!("expected {} to contain '{}'", label, needle));
        } else if !should_contain && contains {
            failures.push(format!("expected {} not to contain '{}'", label, needle));
        }
    }
}

fn assert_contains(
    label: &str,
    haystack: &[String],
    needles: &[String],
    failures: &mut Vec<String>,
) {
    let joined = haystack.join("\n");
    for needle in needles {
        if !joined.contains(needle) {
            failures.push(format!("expected {} output to contain '{}'", label, needle));
        }
    }
}

fn assert_not_contains(
    label: &str,
    haystack: &[String],
    needles: &[String],
    failures: &mut Vec<String>,
) {
    let joined = haystack.join("\n");
    for needle in needles {
        if joined.contains(needle) {
            failures.push(format!(
                "expected {} output not to contain '{}'",
                label, needle
            ));
        }
    }
}

fn assert_text_contains(
    label: &str,
    haystack: &str,
    needles: &[String],
    failures: &mut Vec<String>,
) {
    for needle in needles {
        if !haystack.contains(needle) {
            failures.push(format!("expected {} output to contain '{}'", label, needle));
        }
    }
}

fn assert_text_not_contains(
    label: &str,
    haystack: &str,
    needles: &[String],
    failures: &mut Vec<String>,
) {
    for needle in needles {
        if haystack.contains(needle) {
            failures.push(format!(
                "expected {} output not to contain '{}'",
                label, needle
            ));
        }
    }
}

fn assert_artifact_contains(
    label: &str,
    expects: &[ScenarioArtifactTextExpect],
    workspace_dir: Option<&Path>,
    failures: &mut Vec<String>,
) {
    for expect in expects {
        match read_artifact_text(&expect.path, workspace_dir) {
            Ok(content) => {
                if !content.contains(&expect.text) {
                    failures.push(format!(
                        "expected {} '{}' to contain '{}'",
                        label,
                        expect.path.display(),
                        expect.text
                    ));
                }
            }
            Err(err) => failures.push(format!(
                "failed reading {} '{}': {}",
                label,
                expect.path.display(),
                err
            )),
        }
    }
}

fn assert_artifact_not_contains(
    label: &str,
    expects: &[ScenarioArtifactTextExpect],
    workspace_dir: Option<&Path>,
    failures: &mut Vec<String>,
) {
    for expect in expects {
        match read_artifact_text(&expect.path, workspace_dir) {
            Ok(content) => {
                if content.contains(&expect.text) {
                    failures.push(format!(
                        "expected {} '{}' to not contain '{}'",
                        label,
                        expect.path.display(),
                        expect.text
                    ));
                }
            }
            Err(err) => failures.push(format!(
                "failed reading {} '{}': {}",
                label,
                expect.path.display(),
                err
            )),
        }
    }
}

fn read_artifact_text(path: &Path, workspace_dir: Option<&Path>) -> anyhow::Result<String> {
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(workspace_dir) = workspace_dir {
        workspace_dir.join(path)
    } else {
        path.to_path_buf()
    };
    fs::read_to_string(&resolved)
        .with_context(|| format!("read artifact '{}' failed", resolved.display()))
}

fn capture_turn_artifacts(
    paths: &[PathBuf],
    workspace_dir: Option<&Path>,
) -> anyhow::Result<HashMap<PathBuf, String>> {
    let mut captured = HashMap::new();
    for path in paths {
        let content = read_artifact_text(path, workspace_dir)?;
        captured.insert(path.clone(), content);
    }
    Ok(captured)
}

fn assert_artifact_matches_turn(
    expects: &[ScenarioArtifactTurnExpect],
    workspace_dir: Option<&Path>,
    snapshots: &ScenarioSnapshots,
    failures: &mut Vec<String>,
) {
    for expect in expects {
        let Some(turn_artifacts) = snapshots.artifacts.get(&expect.turn) else {
            failures.push(format!(
                "expected artifact snapshot for turn '{}' but none was recorded",
                expect.turn
            ));
            continue;
        };
        let Some(expected_content) = turn_artifacts.get(&expect.path) else {
            failures.push(format!(
                "expected artifact '{}' to be captured for turn '{}'",
                expect.path.display(),
                expect.turn
            ));
            continue;
        };
        match read_artifact_text(&expect.path, workspace_dir) {
            Ok(actual_content) => {
                if &actual_content != expected_content {
                    failures.push(format!(
                        "expected artifact '{}' to match turn '{}'",
                        expect.path.display(),
                        expect.turn
                    ));
                }
            }
            Err(err) => failures.push(format!(
                "failed reading artifact '{}' for turn comparison: {}",
                expect.path.display(),
                err
            )),
        }
    }
}

fn assert_assistant_matches_turn(
    assistant_stream: &str,
    turn_names: &[String],
    snapshots: &ScenarioSnapshots,
    turn_label: &str,
    failures: &mut Vec<String>,
) {
    for turn_name in turn_names {
        let Some(expected) = snapshots.assistants.get(turn_name) else {
            failures.push(format!(
                "expected assistant snapshot for turn '{}' but none was recorded",
                turn_name
            ));
            continue;
        };
        if assistant_stream.trim() != expected.trim() {
            failures.push(format!(
                "expected assistant output for '{}' to match turn '{}'",
                turn_label, turn_name
            ));
        }
    }
}

fn assert_line_occurrence(
    label: &str,
    haystack: &[String],
    needles: &[String],
    min_count: usize,
    max_count: usize,
    failures: &mut Vec<String>,
) {
    for needle in needles {
        let count = haystack.iter().filter(|line| line.contains(needle)).count();
        if count < min_count || count > max_count {
            failures.push(format!(
                "expected {} '{}' occurrence count in [{}..={}] but got {}",
                label, needle, min_count, max_count, count
            ));
        }
    }
}

fn ensure_log_filter() {
    if std::env::var("RUST_LOG").is_ok() {
        return;
    }
    std::env::set_var("RUST_LOG", "info");
}

#[derive(Debug)]
struct ScenarioCleanupSession {
    restore: Vec<CapturedCleanupTarget>,
    delete: Vec<PathBuf>,
}

#[derive(Debug)]
struct CapturedCleanupTarget {
    path: PathBuf,
    extensions: HashSet<String>,
    is_dir: bool,
    files: HashMap<PathBuf, Vec<u8>>,
}

impl ScenarioCleanupSession {
    fn capture(spec: &ScenarioCleanupSpec) -> anyhow::Result<Self> {
        let mut restore = Vec::with_capacity(spec.restore.len());
        for target in &spec.restore {
            restore.push(capture_cleanup_target(target)?);
        }
        Ok(Self {
            restore,
            delete: spec.delete.clone(),
        })
    }

    fn restore(self) -> anyhow::Result<()> {
        let mut failures = Vec::new();
        for target in &self.restore {
            if let Err(err) = restore_cleanup_target(target) {
                failures.push(format!(
                    "restore '{}' failed: {}",
                    target.path.display(),
                    err
                ));
            }
        }
        for path in &self.delete {
            if let Err(err) = remove_path_if_exists(path) {
                failures.push(format!("delete '{}' failed: {}", path.display(), err));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            bail!(failures.join("; "));
        }
    }
}

fn capture_cleanup_target(target: &ScenarioCleanupTarget) -> anyhow::Result<CapturedCleanupTarget> {
    let extensions = normalize_extensions(&target.extensions);
    let is_dir = target.path.is_dir();
    let files = collect_matching_files(&target.path, &extensions)?
        .into_iter()
        .map(|path| {
            let payload = fs::read(&path)
                .with_context(|| format!("read cleanup snapshot '{}' failed", path.display()))?;
            Ok((path, payload))
        })
        .collect::<anyhow::Result<HashMap<_, _>>>()?;
    Ok(CapturedCleanupTarget {
        path: target.path.clone(),
        extensions,
        is_dir,
        files,
    })
}

fn restore_cleanup_target(target: &CapturedCleanupTarget) -> anyhow::Result<()> {
    if target.is_dir {
        for path in collect_matching_files(&target.path, &target.extensions)? {
            if !target.files.contains_key(&path) {
                remove_path_if_exists(&path)?;
            }
        }
        remove_empty_dirs(&target.path)?;
    } else if !target.files.contains_key(&target.path) {
        remove_path_if_exists(&target.path)?;
    }

    for (path, payload) in &target.files {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create restore dir '{}' failed", parent.display()))?;
        }
        fs::write(path, payload)
            .with_context(|| format!("restore file '{}' failed", path.display()))?;
    }
    Ok(())
}

fn collect_matching_files(
    root: &Path,
    extensions: &HashSet<String>,
) -> anyhow::Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    if root.is_file() {
        return Ok(if matches_extension(root, extensions) {
            vec![root.to_path_buf()]
        } else {
            Vec::new()
        });
    }
    let mut out = Vec::new();
    collect_matching_files_recursive(root, extensions, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect_matching_files_recursive(
    dir: &Path,
    extensions: &HashSet<String>,
    out: &mut Vec<PathBuf>,
) -> anyhow::Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("read cleanup dir '{}' failed", dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("read dir entry in '{}' failed", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_matching_files_recursive(&path, extensions, out)?;
        } else if path.is_file() && matches_extension(&path, extensions) {
            out.push(path);
        }
    }
    Ok(())
}

fn normalize_extensions(raw: &[String]) -> HashSet<String> {
    raw.iter()
        .map(|item| item.trim().trim_start_matches('.').to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect()
}

fn matches_extension(path: &Path, extensions: &HashSet<String>) -> bool {
    if extensions.is_empty() {
        return true;
    }
    path.extension()
        .and_then(|value| value.to_str())
        .map(|value| extensions.contains(&value.to_ascii_lowercase()))
        .unwrap_or(false)
}

fn remove_path_if_exists(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path)
            .with_context(|| format!("remove dir '{}' failed", path.display()))?;
    } else {
        fs::remove_file(path)
            .with_context(|| format!("remove file '{}' failed", path.display()))?;
    }
    Ok(())
}

fn remove_empty_dirs(root: &Path) -> anyhow::Result<bool> {
    if !root.is_dir() {
        return Ok(false);
    }
    let mut empty = true;
    for entry in
        fs::read_dir(root).with_context(|| format!("read dir '{}' failed", root.display()))?
    {
        let entry =
            entry.with_context(|| format!("read dir entry in '{}' failed", root.display()))?;
        let path = entry.path();
        if path.is_dir() {
            if !remove_empty_dirs(&path)? {
                empty = false;
            }
        } else {
            empty = false;
        }
    }
    if empty {
        fs::remove_dir(root)
            .with_context(|| format!("remove empty dir '{}' failed", root.display()))?;
    }
    Ok(empty)
}

fn parse_status_line(line: &str) -> Option<String> {
    if let Some(status) = line
        .strip_prefix("Status: ")
        .map(|status| status.trim().to_ascii_lowercase())
        .filter(|status| !status.is_empty())
    {
        return Some(status);
    }
    if line.starts_with("Need input") || line.starts_with("Waiting: ") {
        return Some("waiting_user".to_string());
    }
    None
}

fn resolve_report_path(
    name: &str,
    explicit: Option<&Path>,
    run_id: Option<&str>,
) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        let path = absolutize_path(path.to_path_buf());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create report dir '{}' failed", parent.display()))?;
        }
        return Ok(path);
    }

    let dir = absolutize_path(PathBuf::from(".orchestral/scenario-runs"));
    fs::create_dir_all(&dir)
        .with_context(|| format!("create scenario run dir '{}' failed", dir.display()))?;
    let file_name = run_id
        .map(|value| format!("{}.json", value))
        .unwrap_or_else(|| format!("{}-{}.json", sanitize_name(name), now_ms()));
    Ok(dir.join(file_name))
}

fn write_report(path: &Path, report: &ScenarioReport) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create report dir '{}' failed", parent.display()))?;
    }
    let payload = serde_json::to_string_pretty(report).context("serialize scenario report")?;
    fs::write(path, payload).with_context(|| format!("write report '{}' failed", path.display()))
}

fn sanitize_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_matches('-').to_string()
}

fn scenario_name_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("scenario")
        .to_string()
}

fn resolve_relative(base_dir: &Path, path: PathBuf) -> PathBuf {
    let joined = if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    };
    absolutize_path(joined)
}

fn command_looks_like_path(command: &str) -> bool {
    command.contains('/') || command.contains('\\') || command.starts_with('.')
}

fn command_looks_like_python(command: &str) -> bool {
    let lowered = command.to_ascii_lowercase();
    lowered.ends_with("python")
        || lowered.ends_with("python3")
        || lowered.ends_with("python.exe")
        || lowered.ends_with("python3.exe")
}

fn absolutize_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(path)
    } else {
        path
    }
}

fn default_spec_version() -> u32 {
    1
}

fn default_require_execution_end() -> bool {
    true
}

fn default_zero_usize() -> Option<usize> {
    Some(0)
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::{
        archive_runtime_log, assert_contains, command_looks_like_path, evaluate_expectations,
        materialize_workspace, parse_status_line, prepare_scenario, resolve_relative,
        run_verification, sanitize_name, PreparedWorkspaceCopy, ScenarioArtifactTextExpect,
        ScenarioArtifactTurnExpect, ScenarioCleanupSession, ScenarioCleanupSpec,
        ScenarioCleanupTarget, ScenarioExpect, ScenarioRunOptions, ScenarioSnapshots,
        ScenarioVerifyPhase, ScenarioVerifySpec, TurnReport,
    };
    use crate::runtime::PlannerOverrides;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_resolve_relative_keeps_absolute_paths() {
        let base = Path::new("/tmp/specs");
        let abs = PathBuf::from("/var/report.json");
        assert_eq!(resolve_relative(base, abs.clone()), abs);
    }

    #[test]
    fn test_resolve_relative_joins_spec_dir() {
        let base = Path::new("/tmp/specs");
        assert_eq!(
            resolve_relative(base, PathBuf::from("report.json")),
            PathBuf::from("/tmp/specs/report.json")
        );
    }

    #[test]
    fn test_sanitize_name_normalizes_symbols() {
        assert_eq!(sanitize_name("Excel Fill Smoke"), "excel-fill-smoke");
        assert_eq!(sanitize_name("a/b/c"), "a-b-c");
    }

    #[test]
    fn test_prepare_scenario_rejects_thread_id_with_fresh_thread_per_turn() {
        let spec_path = std::env::temp_dir().join(format!(
            "orchestral-scenario-conflict-{}.yaml",
            super::now_ms()
        ));
        fs::write(
            &spec_path,
            r#"
version: 1
name: conflict
thread_id: fixed-thread
fresh_thread_per_turn: true
turns:
  - input: hello
"#,
        )
        .expect("write spec");

        let error = prepare_scenario(ScenarioRunOptions {
            spec: Some(spec_path.clone()),
            env_file: None,
            config: None,
            planner_overrides: PlannerOverrides::default(),
            report: None,
            thread_id: None,
            no_mcp: false,
            no_skills: false,
            mcp_paths: Vec::new(),
            skill_dirs: Vec::new(),
            timeout_secs: 30,
            verbose: false,
            input: None,
            persist_contains: Vec::new(),
            persist_not_contains: Vec::new(),
            transient_contains: Vec::new(),
            transient_not_contains: Vec::new(),
            max_approvals: None,
            max_errors: None,
            allow_missing_execution_end: false,
        })
        .expect_err("conflicting thread modes should fail");

        assert!(error
            .to_string()
            .contains("cannot combine thread_id with fresh_thread_per_turn"));
        let _ = fs::remove_file(spec_path);
    }

    #[test]
    fn test_evaluate_expectations_collects_failures() {
        let mut report = TurnReport {
            name: None,
            thread_id: "thread-1".to_string(),
            input: "hello".to_string(),
            duration_ms: 0,
            planning_started: true,
            planning_finished: true,
            execution_started: true,
            execution_finished: false,
            execution_status: None,
            timed_out: false,
            approvals: Vec::new(),
            errors: vec!["boom".to_string()],
            persist_lines: vec!["plan ready".to_string()],
            transient_lines: vec!["executing".to_string()],
            activity_lines: vec!["inspect".to_string()],
            assistant_stream: String::new(),
            execution_modes: Vec::new(),
            verifications: Vec::new(),
            failures: Vec::new(),
            passed: false,
        };
        let expect = ScenarioExpect {
            require_execution_end: true,
            execution_status: None,
            max_approvals: Some(0),
            max_errors: Some(0),
            persist_contains: vec!["plan".to_string()],
            persist_not_contains: vec!["error".to_string()],
            transient_contains: vec!["executing".to_string()],
            transient_not_contains: vec!["approval".to_string()],
            activity_contains: vec!["inspect".to_string()],
            activity_not_contains: vec!["shell".to_string()],
            assistant_contains: Vec::new(),
            assistant_not_contains: Vec::new(),
            execution_mode_contains: Vec::new(),
            execution_mode_not_contains: Vec::new(),
            artifact_contains: Vec::new(),
            artifact_not_contains: Vec::new(),
            activity_contains_once: Vec::new(),
            activity_not_repeated: Vec::new(),
            capture_artifacts: Vec::new(),
            artifact_matches_turn: Vec::new(),
            assistant_matches_turn: Vec::new(),
        };

        evaluate_expectations(
            &mut report,
            &expect,
            None,
            &ScenarioSnapshots::default(),
            "turn-1",
        );
        assert!(report
            .failures
            .iter()
            .any(|failure| failure.contains("ExecutionEnd")));
        assert!(report
            .failures
            .iter()
            .any(|failure| failure.contains("at most 0 errors")));
    }

    #[test]
    fn test_evaluate_expectations_fails_on_failed_status() {
        let mut report = TurnReport {
            name: None,
            thread_id: "thread-1".to_string(),
            input: "hello".to_string(),
            duration_ms: 0,
            planning_started: true,
            planning_finished: true,
            execution_started: true,
            execution_finished: true,
            execution_status: Some("failed".to_string()),
            timed_out: false,
            approvals: Vec::new(),
            errors: Vec::new(),
            persist_lines: vec!["Status: failed".to_string()],
            transient_lines: Vec::new(),
            activity_lines: Vec::new(),
            assistant_stream: String::new(),
            execution_modes: Vec::new(),
            verifications: Vec::new(),
            failures: Vec::new(),
            passed: false,
        };
        evaluate_expectations(
            &mut report,
            &ScenarioExpect::default(),
            None,
            &ScenarioSnapshots::default(),
            "turn-1",
        );
        assert!(report
            .failures
            .iter()
            .any(|failure| failure.contains("Status: failed")));
    }

    #[test]
    fn test_assert_contains_passes_when_joined_output_matches() {
        let mut failures = Vec::new();
        assert_contains(
            "persist",
            &["line-1".to_string(), "line-2".to_string()],
            &["line-1\nline-2".to_string()],
            &mut failures,
        );
        assert!(failures.is_empty());
    }

    #[test]
    fn test_evaluate_expectations_checks_assistant_and_execution_mode() {
        let mut report = TurnReport {
            name: None,
            thread_id: "thread-1".to_string(),
            input: "hello".to_string(),
            duration_ms: 0,
            planning_started: true,
            planning_finished: true,
            execution_started: true,
            execution_finished: true,
            execution_status: Some("completed".to_string()),
            timed_out: false,
            approvals: Vec::new(),
            errors: Vec::new(),
            persist_lines: Vec::new(),
            transient_lines: Vec::new(),
            activity_lines: Vec::new(),
            assistant_stream: "updated app.toml successfully".to_string(),
            execution_modes: vec!["single_action".to_string()],
            verifications: Vec::new(),
            failures: Vec::new(),
            passed: false,
        };
        let expect = ScenarioExpect {
            require_execution_end: true,
            execution_status: Some("completed".to_string()),
            max_approvals: Some(0),
            max_errors: Some(0),
            persist_contains: Vec::new(),
            persist_not_contains: Vec::new(),
            transient_contains: Vec::new(),
            transient_not_contains: Vec::new(),
            activity_contains: Vec::new(),
            activity_not_contains: Vec::new(),
            assistant_contains: vec!["app.toml".to_string()],
            assistant_not_contains: vec!["ls -la".to_string()],
            execution_mode_contains: vec!["single_action".to_string()],
            execution_mode_not_contains: vec!["done".to_string()],
            artifact_contains: Vec::new(),
            artifact_not_contains: Vec::new(),
            activity_contains_once: Vec::new(),
            activity_not_repeated: Vec::new(),
            capture_artifacts: Vec::new(),
            artifact_matches_turn: Vec::new(),
            assistant_matches_turn: Vec::new(),
        };

        evaluate_expectations(
            &mut report,
            &expect,
            None,
            &ScenarioSnapshots::default(),
            "turn-1",
        );
        assert!(report.failures.is_empty());
    }

    #[test]
    fn test_evaluate_expectations_allows_expected_failed_status() {
        let mut report = TurnReport {
            name: None,
            thread_id: "thread-1".to_string(),
            input: "hello".to_string(),
            duration_ms: 0,
            planning_started: true,
            planning_finished: true,
            execution_started: true,
            execution_finished: true,
            execution_status: Some("failed".to_string()),
            timed_out: false,
            approvals: Vec::new(),
            errors: Vec::new(),
            persist_lines: vec!["Status: failed".to_string()],
            transient_lines: Vec::new(),
            activity_lines: Vec::new(),
            assistant_stream: String::new(),
            execution_modes: vec!["reactor".to_string()],
            verifications: Vec::new(),
            failures: Vec::new(),
            passed: false,
        };
        let expect = ScenarioExpect {
            require_execution_end: true,
            execution_status: Some("failed".to_string()),
            max_approvals: Some(0),
            max_errors: Some(0),
            persist_contains: Vec::new(),
            persist_not_contains: Vec::new(),
            transient_contains: Vec::new(),
            transient_not_contains: Vec::new(),
            activity_contains: Vec::new(),
            activity_not_contains: Vec::new(),
            assistant_contains: Vec::new(),
            assistant_not_contains: Vec::new(),
            execution_mode_contains: Vec::new(),
            execution_mode_not_contains: Vec::new(),
            artifact_contains: Vec::new(),
            artifact_not_contains: Vec::new(),
            activity_contains_once: Vec::new(),
            activity_not_repeated: Vec::new(),
            capture_artifacts: Vec::new(),
            artifact_matches_turn: Vec::new(),
            assistant_matches_turn: Vec::new(),
        };

        evaluate_expectations(
            &mut report,
            &expect,
            None,
            &ScenarioSnapshots::default(),
            "turn-1",
        );
        assert!(report.failures.is_empty());
    }

    #[test]
    fn test_evaluate_expectations_checks_artifact_content() {
        let root =
            std::env::temp_dir().join(format!("orchestral-scenario-artifact-{}", super::now_ms()));
        fs::create_dir_all(root.join("fixtures/config")).expect("create artifact dir");
        fs::write(
            root.join("fixtures/config/settings.yaml"),
            "level: info\npretty: false\n",
        )
        .expect("write artifact");

        let mut report = TurnReport {
            name: None,
            thread_id: "thread-1".to_string(),
            input: "hello".to_string(),
            duration_ms: 0,
            planning_started: true,
            planning_finished: true,
            execution_started: true,
            execution_finished: true,
            execution_status: Some("completed".to_string()),
            timed_out: false,
            approvals: Vec::new(),
            errors: Vec::new(),
            persist_lines: Vec::new(),
            transient_lines: Vec::new(),
            activity_lines: Vec::new(),
            assistant_stream: String::new(),
            execution_modes: Vec::new(),
            verifications: Vec::new(),
            failures: Vec::new(),
            passed: false,
        };
        let expect = ScenarioExpect {
            require_execution_end: true,
            execution_status: Some("completed".to_string()),
            max_approvals: Some(0),
            max_errors: Some(0),
            persist_contains: Vec::new(),
            persist_not_contains: Vec::new(),
            transient_contains: Vec::new(),
            transient_not_contains: Vec::new(),
            activity_contains: Vec::new(),
            activity_not_contains: Vec::new(),
            assistant_contains: Vec::new(),
            assistant_not_contains: Vec::new(),
            execution_mode_contains: Vec::new(),
            execution_mode_not_contains: Vec::new(),
            artifact_contains: vec![ScenarioArtifactTextExpect {
                path: PathBuf::from("fixtures/config/settings.yaml"),
                text: "level: info".to_string(),
            }],
            artifact_not_contains: vec![ScenarioArtifactTextExpect {
                path: PathBuf::from("fixtures/config/settings.yaml"),
                text: "debug".to_string(),
            }],
            activity_contains_once: Vec::new(),
            activity_not_repeated: Vec::new(),
            capture_artifacts: Vec::new(),
            artifact_matches_turn: Vec::new(),
            assistant_matches_turn: Vec::new(),
        };

        evaluate_expectations(
            &mut report,
            &expect,
            Some(root.as_path()),
            &ScenarioSnapshots::default(),
            "turn-1",
        );
        assert!(report.failures.is_empty());
    }

    #[test]
    fn test_evaluate_expectations_checks_activity_occurrence_bounds() {
        let mut report = TurnReport {
            name: None,
            thread_id: "thread-1".to_string(),
            input: "hello".to_string(),
            duration_ms: 0,
            planning_started: true,
            planning_finished: true,
            execution_started: true,
            execution_finished: true,
            execution_status: Some("completed".to_string()),
            timed_out: false,
            approvals: Vec::new(),
            errors: Vec::new(),
            persist_lines: Vec::new(),
            transient_lines: Vec::new(),
            activity_lines: vec![
                "[start Ran] apply_patch spreadsheet_apply_patch".to_string(),
                "[item] apply_patch spreadsheet_apply_patch | completed".to_string(),
                "[end] apply_patch spreadsheet_apply_patch failed=false".to_string(),
            ],
            assistant_stream: String::new(),
            execution_modes: Vec::new(),
            verifications: Vec::new(),
            failures: Vec::new(),
            passed: false,
        };
        let expect = ScenarioExpect {
            require_execution_end: true,
            execution_status: Some("completed".to_string()),
            max_approvals: Some(0),
            max_errors: Some(0),
            persist_contains: Vec::new(),
            persist_not_contains: Vec::new(),
            transient_contains: Vec::new(),
            transient_not_contains: Vec::new(),
            activity_contains: Vec::new(),
            activity_not_contains: Vec::new(),
            assistant_contains: Vec::new(),
            assistant_not_contains: Vec::new(),
            execution_mode_contains: Vec::new(),
            execution_mode_not_contains: Vec::new(),
            artifact_contains: Vec::new(),
            artifact_not_contains: Vec::new(),
            activity_contains_once: vec![
                r#"[start Ran] apply_patch spreadsheet_apply_patch"#.to_string()
            ],
            activity_not_repeated: vec![
                r#"[start Ran] apply_patch spreadsheet_apply_patch"#.to_string()
            ],
            capture_artifacts: Vec::new(),
            artifact_matches_turn: Vec::new(),
            assistant_matches_turn: Vec::new(),
        };

        evaluate_expectations(
            &mut report,
            &expect,
            None,
            &ScenarioSnapshots::default(),
            "turn-1",
        );
        assert!(report.failures.is_empty());
    }

    #[test]
    fn test_evaluate_expectations_checks_turn_snapshots() {
        let root =
            std::env::temp_dir().join(format!("orchestral-scenario-snapshot-{}", super::now_ms()));
        fs::create_dir_all(root.join("fixtures/docs")).expect("create docs dir");
        fs::write(root.join("fixtures/docs/summary.md"), "stable summary\n")
            .expect("write artifact");

        let mut snapshots = ScenarioSnapshots::default();
        snapshots
            .assistants
            .insert("run-1".to_string(), "stable reply".to_string());
        snapshots.artifacts.insert(
            "run-1".to_string(),
            HashMap::from([(
                PathBuf::from("fixtures/docs/summary.md"),
                "stable summary\n".to_string(),
            )]),
        );

        let mut report = TurnReport {
            name: Some("run-2".to_string()),
            thread_id: "thread-2".to_string(),
            input: "hello".to_string(),
            duration_ms: 0,
            planning_started: true,
            planning_finished: true,
            execution_started: true,
            execution_finished: true,
            execution_status: Some("completed".to_string()),
            timed_out: false,
            approvals: Vec::new(),
            errors: Vec::new(),
            persist_lines: Vec::new(),
            transient_lines: Vec::new(),
            activity_lines: Vec::new(),
            assistant_stream: "stable reply".to_string(),
            execution_modes: Vec::new(),
            verifications: Vec::new(),
            failures: Vec::new(),
            passed: false,
        };
        let expect = ScenarioExpect {
            require_execution_end: true,
            execution_status: Some("completed".to_string()),
            max_approvals: Some(0),
            max_errors: Some(0),
            persist_contains: Vec::new(),
            persist_not_contains: Vec::new(),
            transient_contains: Vec::new(),
            transient_not_contains: Vec::new(),
            activity_contains: Vec::new(),
            activity_not_contains: Vec::new(),
            assistant_contains: Vec::new(),
            assistant_not_contains: Vec::new(),
            execution_mode_contains: Vec::new(),
            execution_mode_not_contains: Vec::new(),
            artifact_contains: Vec::new(),
            artifact_not_contains: Vec::new(),
            activity_contains_once: Vec::new(),
            activity_not_repeated: Vec::new(),
            capture_artifacts: Vec::new(),
            artifact_matches_turn: vec![ScenarioArtifactTurnExpect {
                path: PathBuf::from("fixtures/docs/summary.md"),
                turn: "run-1".to_string(),
            }],
            assistant_matches_turn: vec!["run-1".to_string()],
        };

        evaluate_expectations(
            &mut report,
            &expect,
            Some(root.as_path()),
            &snapshots,
            "run-2",
        );
        assert!(report.failures.is_empty());
    }

    #[test]
    fn test_parse_status_line_extracts_status() {
        assert_eq!(
            parse_status_line("Status: failed"),
            Some("failed".to_string())
        );
        assert_eq!(parse_status_line("Done"), None);
    }

    #[test]
    fn test_command_looks_like_path_detects_relative_paths() {
        assert!(command_looks_like_path("./scripts/check.py"));
        assert!(command_looks_like_path("configs/scenarios/check.py"));
        assert!(!command_looks_like_path("python3"));
    }

    #[tokio::test]
    async fn test_run_verification_collects_stdout_assertions() {
        let report = run_verification(&ScenarioVerifySpec {
            name: Some("echo".to_string()),
            phase: ScenarioVerifyPhase::AfterExecution,
            command: "/bin/echo".to_string(),
            args: vec!["ok".to_string()],
            cwd: None,
            env: HashMap::new(),
            timeout_secs: Some(5),
            stdout_contains: vec!["ok".to_string()],
            stdout_not_contains: vec!["boom".to_string()],
            stderr_contains: Vec::new(),
            stderr_not_contains: Vec::new(),
        })
        .await;

        assert!(report.passed);
        assert!(report.failures.is_empty());
        assert!(report.stdout.contains("ok"));
    }

    #[test]
    fn test_archive_runtime_log_copies_runtime_log() {
        let root =
            std::env::temp_dir().join(format!("orchestral-scenario-archive-{}", super::now_ms()));
        let source = root.join("run/logs/orchestral-runtime.log");
        let destination = root.join(".orchestral/scenario-runs/demo.runtime.log");
        fs::create_dir_all(source.parent().expect("source parent")).expect("create logs dir");
        fs::create_dir_all(destination.parent().expect("destination parent"))
            .expect("create reports dir");
        fs::write(&source, b"runtime-debug").expect("write runtime log");

        let archived = archive_runtime_log(&source, &destination).expect("archive log");

        let archived = archived.expect("archived log path");
        let payload = fs::read_to_string(&archived).expect("read archived log");
        assert_eq!(payload, "runtime-debug");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_materialize_workspace_copies_file_to_relative_target() {
        let root =
            std::env::temp_dir().join(format!("orchestral-scenario-workspace-{}", super::now_ms()));
        let source = root.join("fixtures/source.txt");
        let workspace = root.join("run/workspace");
        fs::create_dir_all(source.parent().expect("source parent")).expect("create fixture dir");
        fs::create_dir_all(&workspace).expect("create workspace dir");
        fs::write(&source, b"hello").expect("write source");

        materialize_workspace(
            &[PreparedWorkspaceCopy {
                from: source.clone(),
                to: Some(PathBuf::from("docs/copied.txt")),
            }],
            Some(&workspace),
        )
        .expect("materialize workspace");

        let copied = workspace.join("docs/copied.txt");
        assert_eq!(
            fs::read_to_string(copied).expect("read copied file"),
            "hello"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn test_cleanup_session_restores_snapshots_and_removes_new_matching_files() {
        let root =
            std::env::temp_dir().join(format!("orchestral-scenario-cleanup-{}", super::now_ms()));
        let docs = root.join("docs");
        let logs = root.join("logs");
        fs::create_dir_all(&docs).expect("create docs dir");
        fs::create_dir_all(&logs).expect("create logs dir");
        let workbook = docs.join("sample.xlsx");
        let log = logs.join("orchestral-runtime.log");
        fs::write(&workbook, b"before").expect("write workbook");
        fs::write(&log, b"log-before").expect("write log");

        let cleanup = ScenarioCleanupSession::capture(&ScenarioCleanupSpec {
            restore: vec![
                ScenarioCleanupTarget {
                    path: docs.clone(),
                    extensions: vec!["xlsx".to_string()],
                },
                ScenarioCleanupTarget {
                    path: log.clone(),
                    extensions: Vec::new(),
                },
            ],
            delete: vec![root.join(".orchestral/tmp")],
        })
        .expect("capture");

        fs::write(&workbook, b"after").expect("mutate workbook");
        fs::write(docs.join("new.xlsx"), b"new").expect("write new workbook");
        fs::write(&log, b"log-after").expect("mutate log");
        let tmp_dir = root.join(".orchestral/tmp");
        fs::create_dir_all(&tmp_dir).expect("create tmp dir");
        fs::write(tmp_dir.join("generated.py"), b"print('hi')").expect("write temp script");

        cleanup.restore().expect("restore");

        assert_eq!(fs::read(&workbook).expect("read workbook"), b"before");
        assert!(!docs.join("new.xlsx").exists());
        assert_eq!(fs::read(&log).expect("read log"), b"log-before");
        assert!(!tmp_dir.exists());

        let _ = fs::remove_dir_all(root);
    }
}
