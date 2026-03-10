//! Orchestrator - minimal intent → plan → normalize → execute pipeline
//!
//! This bridges the ThreadRuntime (events + concurrency) with core planning/execution.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::context::{ContextBuilder, ContextError, ContextRequest, ContextWindow, TokenBudget};
use crate::skill::SkillCatalog;
use orchestral_core::action::{extract_meta, ActionMeta};
use orchestral_core::executor::{
    ExecutionProgressEvent, ExecutionProgressReporter, ExecutionResult, Executor, ExecutorContext,
};
use orchestral_core::interpreter::InterpretRequest;
use orchestral_core::normalizer::{NormalizeError, PlanNormalizer, ValidationError};
use orchestral_core::planner::{
    HistoryItem, PlanError, Planner, PlannerContext, PlannerOutput, PlannerRuntimeInfo,
    SkillInstruction,
};
use orchestral_core::spi::{HookRegistry, RuntimeHookContext, RuntimeHookEventEnvelope, SpiMeta};
use orchestral_core::store::{
    Event, EventBus, EventStore, InteractionId, ReferenceStore, StoreError, TaskStore, ThreadId,
    WorkingSet,
};
use orchestral_core::types::{
    ArtifactFamily, ContinuationState, ContinuationStatus, DerivationPolicy, Intent, IntentContext,
    ReactorTaskState, SkeletonChoice, StageChoice, StageKind, Step, StepId, StepIoBinding,
    StepKind, Task, TaskId, TaskState, VerifyDecision, VerifyStatus, WaitUserReason,
};

use crate::{HandleEventResult, InteractionState, RuntimeError, ThreadRuntime};

const MAX_LOG_CHARS: usize = 8_000;
const REACTOR_SPREADSHEET_LOCATE_ACTION: &str = "reactor_spreadsheet_locate";
const REACTOR_SPREADSHEET_INSPECT_ACTION: &str = "reactor_spreadsheet_inspect";
const REACTOR_SPREADSHEET_ASSESS_ACTION: &str = "reactor_spreadsheet_assess_readiness";
const REACTOR_SPREADSHEET_APPLY_ACTION: &str = "reactor_spreadsheet_apply_patch";
const REACTOR_SPREADSHEET_VERIFY_ACTION: &str = "reactor_spreadsheet_verify_patch";
const REACTOR_DOCUMENT_LOCATE_ACTION: &str = "reactor_document_locate";
const REACTOR_DOCUMENT_INSPECT_ACTION: &str = "reactor_document_inspect";
const REACTOR_DOCUMENT_ASSESS_ACTION: &str = "reactor_document_assess_readiness";
const REACTOR_DOCUMENT_APPLY_ACTION: &str = "reactor_document_apply_patch";
const REACTOR_DOCUMENT_VERIFY_ACTION: &str = "reactor_document_verify_patch";

fn truncate_for_log(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    let mut preview: String = input.chars().take(max_chars).collect();
    preview.push_str(&format!("... [truncated, total_chars={}]", char_count));
    preview
}

fn truncate_debug_for_log(value: &impl std::fmt::Debug, max_chars: usize) -> String {
    truncate_for_log(&format!("{:?}", value), max_chars)
}

fn parse_working_set_value<T: DeserializeOwned>(
    snapshot: &HashMap<String, Value>,
    key: &str,
) -> Result<T, OrchestratorError> {
    let value = snapshot.get(key).ok_or_else(|| {
        OrchestratorError::Planner(PlanError::Generation(format!(
            "reactor expected working_set key '{}'",
            key
        )))
    })?;
    serde_json::from_value::<T>(value.clone()).map_err(|err| {
        OrchestratorError::Planner(PlanError::Generation(format!(
            "reactor failed to parse working_set key '{}': {}",
            key, err
        )))
    })
}

fn require_working_set_string(
    snapshot: &HashMap<String, Value>,
    key: &str,
) -> Result<String, OrchestratorError> {
    snapshot
        .get(key)
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(format!(
                "reactor expected string working_set key '{}'",
                key
            )))
        })
}

fn require_working_set_string_array(
    snapshot: &HashMap<String, Value>,
    key: &str,
) -> Result<Vec<String>, OrchestratorError> {
    snapshot
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(format!(
                "reactor expected string array working_set key '{}'",
                key
            )))
        })
}

fn reactor_stage_goal(
    artifact_family: orchestral_core::types::ArtifactFamily,
    stage: StageKind,
) -> String {
    match (artifact_family, stage) {
        (_, StageKind::Locate) => "locate the target artifact".to_string(),
        (orchestral_core::types::ArtifactFamily::Spreadsheet, StageKind::Probe) => {
            "inspect workbook structure and assess readiness".to_string()
        }
        (_, StageKind::Derive) => "derive a structured patch spec".to_string(),
        (_, StageKind::Assess) => "assess whether the task is ready to commit".to_string(),
        (orchestral_core::types::ArtifactFamily::Spreadsheet, StageKind::Commit) => {
            "derive a typed spreadsheet patch and apply it".to_string()
        }
        (orchestral_core::types::ArtifactFamily::Spreadsheet, StageKind::Verify) => {
            "verify the applied spreadsheet patch".to_string()
        }
        (orchestral_core::types::ArtifactFamily::Document, StageKind::Probe) => {
            "inspect document structure and assess readiness".to_string()
        }
        (orchestral_core::types::ArtifactFamily::Document, StageKind::Commit) => {
            "derive a typed document patch and apply it".to_string()
        }
        (orchestral_core::types::ArtifactFamily::Document, StageKind::Verify) => {
            "verify the applied document patch".to_string()
        }
        (_, StageKind::Export) => "export the verified result".to_string(),
        (_, StageKind::Prepare) => "prepare inputs for controlled execution".to_string(),
        (_, StageKind::Run) => "run the controlled program".to_string(),
        (_, StageKind::Collect) => "collect outputs from the controlled run".to_string(),
        (_, StageKind::Compare) => "compare structures and derive differences".to_string(),
        (_, StageKind::WaitUser) => "wait for user input".to_string(),
        (_, StageKind::Done) => "finish the reactor task".to_string(),
        (_, StageKind::Failed) => "mark the reactor task as failed".to_string(),
        (_, StageKind::Probe) => "inspect the target artifact and assess readiness".to_string(),
        (_, StageKind::Commit) => "derive a typed patch and apply it".to_string(),
        (_, StageKind::Verify) => "verify the applied patch".to_string(),
    }
}

fn resolve_stage_choice_from_skeleton_choice(
    choice: SkeletonChoice,
    fallback_artifact_family: Option<ArtifactFamily>,
    derivation_policy: DerivationPolicy,
) -> Result<StageChoice, OrchestratorError> {
    let artifact_family = choice
        .artifact_family
        .or(fallback_artifact_family)
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "skeleton_choice requires explicit artifact_family hint".to_string(),
            ))
        })?;
    Ok(StageChoice {
        skeleton: choice.skeleton,
        artifact_family,
        current_stage: choice.initial_stage,
        stage_goal: reactor_stage_goal(artifact_family, choice.initial_stage),
        derivation_policy,
        next_stage_hint: None,
        reason: choice.reason,
    })
}

fn build_leaf_output_rules(keys: &[&str], result_slot: &str) -> Value {
    let mut map = serde_json::Map::new();
    for key in keys {
        map.insert(
            (*key).to_string(),
            serde_json::json!({
                "candidates": [
                    { "slot": result_slot, "path": key }
                ]
            }),
        );
    }
    Value::Object(map)
}

fn build_spreadsheet_probe_plan(task: &Task, choice: &StageChoice) -> orchestral_core::types::Plan {
    let mut plan = orchestral_core::types::Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::action("reactor_probe_locate", REACTOR_SPREADSHEET_LOCATE_ACTION)
                .with_exports(vec![
                    "source_path".to_string(),
                    "artifact_candidates".to_string(),
                    "artifact_count".to_string(),
                ])
                .with_params(serde_json::json!({
                    "source_root": ".",
                    "user_request": task.intent.content,
                })),
            Step::action("reactor_probe_inspect", REACTOR_SPREADSHEET_INSPECT_ACTION)
                .with_depends_on(vec![StepId::from("reactor_probe_locate")])
                .with_exports(vec!["inspection".to_string()])
                .with_io_bindings(vec![StepIoBinding::required("reactor_probe_locate.source_path", "path")]),
            Step::leaf_agent("reactor_probe_derive")
                .with_depends_on(vec![
                    StepId::from("reactor_probe_locate"),
                    StepId::from("reactor_probe_inspect"),
                ])
                .with_exports(vec!["patch_candidates".to_string(), "summary".to_string()])
                .with_io_bindings(vec![
                    StepIoBinding::required("reactor_probe_locate.source_path", "source_path"),
                    StepIoBinding::required("reactor_probe_inspect.inspection", "inspection"),
                ])
                .with_params(serde_json::json!({
                    "mode": "leaf",
                    "goal": "Probe a spreadsheet patch task. Using user_request, source_path, inspection, and derivation_policy, return JSON with keys patch_candidates and summary. patch_candidates must describe candidate fills for patchable cells without modifying the file. permissive policy may propose generic but coherent spreadsheet fill content when structure is clear. strict policy should surface unresolved unknowns through patch_candidates. Do not decide continuation here.",
                    "allowed_actions": ["json_stdout"],
                    "max_iterations": 1,
                    "result_slot": "probe_result",
                    "output_keys": ["patch_candidates", "summary"],
                    "output_rules": build_leaf_output_rules(&["patch_candidates", "summary"], "probe_result"),
                    "user_request": task.intent.content,
                    "stage_goal": choice.stage_goal,
                    "derivation_policy": choice.derivation_policy,
                })),
            Step::action("reactor_probe_assess", REACTOR_SPREADSHEET_ASSESS_ACTION)
                .with_depends_on(vec![
                    StepId::from("reactor_probe_inspect"),
                    StepId::from("reactor_probe_derive"),
                ])
                .with_exports(vec!["continuation".to_string(), "summary".to_string()])
                .with_io_bindings(vec![
                    StepIoBinding::required("reactor_probe_inspect.inspection", "inspection"),
                    StepIoBinding::required("reactor_probe_derive.patch_candidates", "patch_candidates"),
                ])
                .with_params(serde_json::json!({
                    "derivation_policy": choice.derivation_policy,
                })),
        ],
    );
    plan.on_failure = Some("Spreadsheet probe failed: {{error}}".to_string());
    plan
}

fn build_spreadsheet_commit_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<orchestral_core::types::Plan, OrchestratorError> {
    let source_path = require_working_set_string(&task.working_set_snapshot, "source_path")?;
    let mut plan = orchestral_core::types::Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::leaf_agent("reactor_commit_derive")
                .with_exports(vec!["patch_spec".to_string(), "summary".to_string()])
                .with_params(serde_json::json!({
                    "mode": "leaf",
                    "goal": "Commit stage for spreadsheet patching. Using user_request, source_path, inspection, patch_candidates, and derivation_policy, return JSON with keys patch_spec and summary. patch_spec must be an object with path and fills. patch_spec.fills must be an array of {cell, value}. Cover every patchable cell listed in inspection.selected_region.patchable_cells unless the cell is already handled by a formula or existing non-empty value. If derivation_policy is permissive, fill any remaining uncovered patchable cells with coherent generic content inferred from row labels and column headers instead of leaving gaps. Do not include unchanged cells. Preserve formulas and workbook structure. When a proposed fill is numeric, emit a numeric JSON value instead of prose.",
                    "allowed_actions": ["json_stdout"],
                    "max_iterations": 1,
                    "result_slot": "commit_result",
                    "output_keys": ["patch_spec", "summary"],
                    "output_rules": build_leaf_output_rules(&["patch_spec", "summary"], "commit_result"),
                    "user_request": task.intent.content,
                    "source_path": source_path,
                    "inspection": task.working_set_snapshot.get("inspection").cloned().unwrap_or(Value::Null),
                    "patch_candidates": task.working_set_snapshot.get("patch_candidates").cloned().unwrap_or(Value::Null),
                    "derivation_policy": choice.derivation_policy,
                })),
            Step::action("reactor_commit_apply", REACTOR_SPREADSHEET_APPLY_ACTION)
                .with_depends_on(vec![StepId::from("reactor_commit_derive")])
                .with_exports(vec![
                    "updated_file_path".to_string(),
                    "patch_count".to_string(),
                    "summary".to_string(),
                ])
                .with_io_bindings(vec![StepIoBinding::required("reactor_commit_derive.patch_spec", "patch_spec")])
                .with_params(serde_json::json!({
                    "path": source_path,
                })),
        ],
    );
    plan.on_failure = Some("Spreadsheet commit failed: {{error}}".to_string());
    Ok(plan)
}

fn build_spreadsheet_verify_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<orchestral_core::types::Plan, OrchestratorError> {
    let path = require_working_set_string(&task.working_set_snapshot, "updated_file_path")
        .or_else(|_| require_working_set_string(&task.working_set_snapshot, "source_path"))?;
    let mut plan = orchestral_core::types::Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::action("reactor_verify", REACTOR_SPREADSHEET_VERIFY_ACTION)
                .with_exports(vec!["verify_decision".to_string(), "summary".to_string()])
                .with_params(serde_json::json!({
                    "path": path,
                })),
        ],
    );
    plan.on_complete = Some("Workbook updated and verified.".to_string());
    plan.on_failure = Some("Spreadsheet verify failed: {{error}}".to_string());
    Ok(plan)
}

fn build_document_probe_plan(task: &Task, choice: &StageChoice) -> orchestral_core::types::Plan {
    let mut plan = orchestral_core::types::Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::action("reactor_probe_locate", REACTOR_DOCUMENT_LOCATE_ACTION)
                .with_exports(vec![
                    "source_paths".to_string(),
                    "artifact_candidates".to_string(),
                    "artifact_count".to_string(),
                ])
                .with_params(serde_json::json!({
                    "source_root": ".",
                    "user_request": task.intent.content,
                })),
            Step::action("reactor_probe_inspect", REACTOR_DOCUMENT_INSPECT_ACTION)
                .with_depends_on(vec![StepId::from("reactor_probe_locate")])
                .with_exports(vec!["inspection".to_string()])
                .with_io_bindings(vec![StepIoBinding::required(
                    "reactor_probe_locate.source_paths",
                    "source_paths",
                )]),
            Step::leaf_agent("reactor_probe_derive")
                .with_depends_on(vec![
                    StepId::from("reactor_probe_locate"),
                    StepId::from("reactor_probe_inspect"),
                ])
                .with_exports(vec!["patch_candidates".to_string(), "summary".to_string()])
                .with_io_bindings(vec![
                    StepIoBinding::required("reactor_probe_locate.source_paths", "source_paths"),
                    StepIoBinding::required("reactor_probe_inspect.inspection", "inspection"),
                ])
                .with_params(serde_json::json!({
                    "mode": "leaf",
                    "goal": "Probe a document patch task. Using user_request, source_paths, inspection, and derivation_policy, return JSON with keys patch_candidates and summary. patch_candidates must be an object with a files array. Each file entry should contain path, planned_changes, needs_user_input, and unknowns. summary should be a concise change plan. Do not modify files. Do not decide continuation here.",
                    "allowed_actions": ["json_stdout"],
                    "max_iterations": 1,
                    "result_slot": "probe_result",
                    "output_keys": ["patch_candidates", "summary"],
                    "output_rules": build_leaf_output_rules(&["patch_candidates", "summary"], "probe_result"),
                    "user_request": task.intent.content,
                    "stage_goal": choice.stage_goal,
                    "derivation_policy": choice.derivation_policy,
                })),
            Step::action("reactor_probe_assess", REACTOR_DOCUMENT_ASSESS_ACTION)
                .with_depends_on(vec![
                    StepId::from("reactor_probe_inspect"),
                    StepId::from("reactor_probe_derive"),
                ])
                .with_exports(vec!["continuation".to_string(), "summary".to_string()])
                .with_io_bindings(vec![
                    StepIoBinding::required("reactor_probe_inspect.inspection", "inspection"),
                    StepIoBinding::required("reactor_probe_derive.patch_candidates", "patch_candidates"),
                ])
                .with_params(serde_json::json!({
                    "derivation_policy": choice.derivation_policy,
                    "user_request": task.intent.content,
                })),
        ],
    );
    plan.on_failure = Some("Document probe failed: {{error}}".to_string());
    plan
}

fn build_document_commit_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<orchestral_core::types::Plan, OrchestratorError> {
    let source_paths =
        require_working_set_string_array(&task.working_set_snapshot, "source_paths")?;
    let inspection = task
        .working_set_snapshot
        .get("inspection")
        .cloned()
        .unwrap_or(Value::Null);
    let patch_candidates = task
        .working_set_snapshot
        .get("patch_candidates")
        .cloned()
        .unwrap_or(Value::Null);
    let resume_user_input = task
        .working_set_snapshot
        .get("resume_user_input")
        .cloned()
        .unwrap_or(Value::Null);
    let mut plan = orchestral_core::types::Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::leaf_agent("reactor_commit_derive")
                .with_exports(vec!["patch_spec".to_string(), "summary".to_string()])
                .with_params(serde_json::json!({
                    "mode": "leaf",
                    "goal": "Commit stage for document patching. Using user_request, resume_user_input, source_paths, inspection, patch_candidates, and derivation_policy, return JSON with keys patch_spec and summary. patch_spec must be an object with updates. updates must be an array of {path, content}. Generate complete final markdown/text content for each file that needs changes. Replace TODO placeholders with concrete coherent content, add a top-level title when missing, preserve unaffected content, and if resume_user_input requests a summary/report markdown path include an update for that file too. Do not emit unchanged files.",
                    "allowed_actions": ["json_stdout"],
                    "max_iterations": 1,
                    "result_slot": "commit_result",
                    "output_keys": ["patch_spec", "summary"],
                    "output_rules": build_leaf_output_rules(&["patch_spec", "summary"], "commit_result"),
                    "user_request": task.intent.content,
                    "resume_user_input": resume_user_input,
                    "source_paths": source_paths,
                    "inspection": inspection,
                    "patch_candidates": patch_candidates,
                    "derivation_policy": choice.derivation_policy,
                })),
            Step::action("reactor_commit_apply", REACTOR_DOCUMENT_APPLY_ACTION)
                .with_depends_on(vec![StepId::from("reactor_commit_derive")])
                .with_exports(vec![
                    "updated_paths".to_string(),
                    "patch_count".to_string(),
                    "summary".to_string(),
                ])
                .with_io_bindings(vec![StepIoBinding::required(
                    "reactor_commit_derive.patch_spec",
                    "patch_spec",
                )]),
        ],
    );
    plan.on_failure = Some("Document commit failed: {{error}}".to_string());
    Ok(plan)
}

fn build_document_verify_plan(
    task: &Task,
    choice: &StageChoice,
) -> Result<orchestral_core::types::Plan, OrchestratorError> {
    let patch_spec = task
        .working_set_snapshot
        .get("patch_spec")
        .cloned()
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "reactor expected working_set key 'patch_spec'".to_string(),
            ))
        })?;
    let inspection = task
        .working_set_snapshot
        .get("inspection")
        .cloned()
        .ok_or_else(|| {
            OrchestratorError::Planner(PlanError::Generation(
                "reactor expected working_set key 'inspection'".to_string(),
            ))
        })?;
    let resume_user_input = task
        .working_set_snapshot
        .get("resume_user_input")
        .cloned()
        .unwrap_or(Value::Null);
    let mut plan = orchestral_core::types::Plan::new(
        choice.stage_goal.clone(),
        vec![
            Step::action("reactor_verify", REACTOR_DOCUMENT_VERIFY_ACTION)
                .with_exports(vec!["verify_decision".to_string(), "summary".to_string()])
                .with_params(serde_json::json!({
                    "patch_spec": patch_spec,
                    "inspection": inspection,
                    "user_request": task.intent.content,
                    "resume_user_input": resume_user_input,
                })),
        ],
    );
    plan.on_complete = Some("Documents updated and verified.".to_string());
    plan.on_failure = Some("Document verify failed: {{error}}".to_string());
    Ok(plan)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReactorResumeDecision {
    Approve,
    Deny,
    Unknown,
}

fn parse_reactor_resume_decision(message: &str) -> ReactorResumeDecision {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return ReactorResumeDecision::Unknown;
    }

    let deny_markers = ["拒绝", "不同意", "取消", "不要执行", "deny", "cancel"];
    if deny_markers
        .iter()
        .any(|marker| normalized.contains(&marker.to_ascii_lowercase()))
    {
        return ReactorResumeDecision::Deny;
    }

    let approve_markers = [
        "确认",
        "同意",
        "批准",
        "继续",
        "按计划",
        "写回",
        "执行",
        "approve",
        "approved",
        "proceed",
    ];
    if approve_markers
        .iter()
        .any(|marker| normalized.contains(&marker.to_ascii_lowercase()))
    {
        return ReactorResumeDecision::Approve;
    }

    ReactorResumeDecision::Unknown
}

fn maybe_shape_probe_first_plan(
    intent: &Intent,
    skills: &[SkillInstruction],
    available_actions: &[ActionMeta],
    mut plan: orchestral_core::types::Plan,
) -> orchestral_core::types::Plan {
    if plan.steps.is_empty()
        || plan.steps.iter().any(|step| {
            matches!(
                step.kind,
                StepKind::Recipe | StepKind::WaitUser | StepKind::WaitEvent | StepKind::Replan
            )
        })
    {
        return plan;
    }

    let split_index = probe_split_index(&plan, available_actions);
    let probe_scope_len = split_index.unwrap_or(plan.steps.len());
    if !looks_like_local_exploratory_task(intent, skills, &plan, probe_scope_len, available_actions)
    {
        return plan;
    }

    if split_index.is_none()
        && plan
            .steps
            .iter()
            .all(|step| is_probe_action(step, available_actions))
        && plan_needs_follow_up(intent, &plan)
    {
        let prompt = probe_replan_prompt(intent, &plan.on_complete, None);
        append_probe_replan_step(&mut plan, prompt);
        tracing::info!(
            shaped_step_count = plan.steps.len(),
            "orchestrator appended continuation replan to probe-only workflow"
        );
        return plan;
    }

    let Some(split_index) = split_index else {
        return plan;
    };

    let probe_steps = plan.steps[..split_index].to_vec();
    let tail_ids = terminal_step_ids(&probe_steps);
    if tail_ids.is_empty() {
        return plan;
    }

    let deferred_summary = summarize_deferred_steps(&plan.steps[split_index..]);
    let prompt = probe_replan_prompt(intent, &plan.on_complete, Some(&deferred_summary));
    let original_step_count = plan.steps.len();
    plan.steps = probe_steps;
    append_probe_replan_step_with_deps(&mut plan, tail_ids, prompt);
    tracing::info!(
        original_step_count,
        shaped_step_count = plan.steps.len(),
        split_index,
        "orchestrator shaped initial workflow into probe-first plan"
    );
    plan
}

fn probe_split_index(
    plan: &orchestral_core::types::Plan,
    available_actions: &[ActionMeta],
) -> Option<usize> {
    for (index, step) in plan.steps.iter().enumerate() {
        if index == 0 {
            continue;
        }
        if step.kind == StepKind::Agent {
            return Some(index);
        }
        if is_explicit_mutation_step(step, available_actions) {
            return Some(index);
        }
    }
    None
}

fn is_explicit_mutation_step(step: &Step, available_actions: &[ActionMeta]) -> bool {
    if step.kind != StepKind::Action {
        return false;
    }
    if step.action == "file_write" {
        return true;
    }
    let Some(meta) = available_actions
        .iter()
        .find(|meta| meta.name == step.action)
    else {
        return false;
    };
    meta.has_role("apply")
        && !meta.has_role("inspect")
        && !meta.has_role("collect")
        && !meta.has_role("verify")
}

fn looks_like_local_exploratory_task(
    intent: &Intent,
    skills: &[SkillInstruction],
    plan: &orchestral_core::types::Plan,
    split_index: usize,
    available_actions: &[ActionMeta],
) -> bool {
    let probe_steps = &plan.steps[..split_index];
    let has_probe_action = probe_steps
        .iter()
        .any(|step| is_probe_action(step, available_actions));
    if !has_probe_action {
        return false;
    }

    let has_network = probe_steps.iter().any(|step| {
        available_actions
            .iter()
            .find(|meta| meta.name == step.action)
            .is_some_and(|meta| meta.has_capability("network_io"))
    });
    if has_network {
        return false;
    }

    let lowered_intent = intent.content.to_ascii_lowercase();
    let local_keywords = [
        "docs",
        "directory",
        "folder",
        "file",
        "local",
        "excel",
        "xlsx",
        "csv",
        "json",
        "yaml",
        "markdown",
        "md",
        "workbook",
        "spreadsheet",
        "目录",
        "文件",
        "文档",
        "表格",
    ];
    if local_keywords
        .iter()
        .any(|keyword| lowered_intent.contains(keyword))
    {
        return true;
    }

    skills_match_keywords(
        skills,
        &[
            "excel",
            "xlsx",
            "spreadsheet",
            "workbook",
            "markdown",
            "document",
            "file",
        ],
    )
}

fn is_probe_action(step: &Step, available_actions: &[ActionMeta]) -> bool {
    if step.kind != StepKind::Action {
        return false;
    }
    if matches!(step.action.as_str(), "shell" | "file_read") {
        return true;
    }
    available_actions
        .iter()
        .find(|meta| meta.name == step.action)
        .is_some_and(|meta| meta.has_role("inspect") || meta.has_role("collect"))
}

fn terminal_step_ids(steps: &[Step]) -> Vec<String> {
    let mut has_downstream: HashMap<&str, bool> =
        steps.iter().map(|step| (step.id.as_str(), false)).collect();
    for step in steps {
        for dep in &step.depends_on {
            if let Some(value) = has_downstream.get_mut(dep.as_str()) {
                *value = true;
            }
        }
    }
    steps
        .iter()
        .filter(|step| {
            !has_downstream
                .get(step.id.as_str())
                .copied()
                .unwrap_or(false)
        })
        .map(|step| step.id.to_string())
        .collect()
}

fn summarize_deferred_steps(steps: &[Step]) -> String {
    steps
        .iter()
        .map(|step| format!("{}:{}:{:?}", step.id, step.action, step.kind))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn append_probe_replan_step(plan: &mut orchestral_core::types::Plan, prompt: String) {
    let tail_ids = terminal_step_ids(&plan.steps);
    append_probe_replan_step_with_deps(plan, tail_ids, prompt);
}

fn append_probe_replan_step_with_deps(
    plan: &mut orchestral_core::types::Plan,
    tail_ids: Vec<String>,
    prompt: String,
) {
    if tail_ids.is_empty() {
        return;
    }
    let replan_step_id = unique_step_id("probe_commit", plan);
    let replan_step = Step::replan(replan_step_id)
        .with_depends_on(tail_ids.into_iter().map(StepId::from).collect())
        .with_params(serde_json::json!({ "prompt": prompt }));
    plan.steps.push(replan_step);
}

fn unique_step_id(prefix: &str, plan: &orchestral_core::types::Plan) -> String {
    if plan.get_step(prefix).is_none() {
        return prefix.to_string();
    }
    let mut suffix = 1usize;
    loop {
        let candidate = format!("{}_{}", prefix, suffix);
        if plan.get_step(&candidate).is_none() {
            return candidate;
        }
        suffix += 1;
    }
}

fn probe_replan_prompt(
    intent: &Intent,
    on_complete: &Option<String>,
    deferred_summary: Option<&str>,
) -> String {
    let mut prompt = format!(
        "Probe phase is complete for intent: {}.\n\
        Continue from the current working set only. Do not repeat discovery or inspection when \
        the target path, content preview, or environment facts are already available. \
        If commit parameters are still ambiguous, ask the user or wait instead of guessing.",
        intent.content
    );
    if let Some(summary) = deferred_summary.filter(|value| !value.trim().is_empty()) {
        prompt.push_str(&format!(
            "\nDeferred plan summary before shaping: {}",
            summary
        ));
    }
    if let Some(message) = on_complete
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        prompt.push_str(&format!("\nOriginal completion hint: {}", message));
    }
    prompt
}

fn plan_needs_follow_up(intent: &Intent, plan: &orchestral_core::types::Plan) -> bool {
    let mut haystack = intent.content.to_ascii_lowercase();
    haystack.push(' ');
    haystack.push_str(&plan.goal.to_ascii_lowercase());
    if let Some(on_complete) = &plan.on_complete {
        haystack.push(' ');
        haystack.push_str(&on_complete.to_ascii_lowercase());
    }
    let follow_up_keywords = [
        "fill", "write", "update", "modify", "edit", "patch", "apply", "verify", "save", "emit",
        "replan", "continue", "生成", "填写", "补", "改", "写", "更新", "保存", "输出", "继续",
    ];
    follow_up_keywords
        .iter()
        .any(|keyword| haystack.contains(keyword))
}

fn skills_match_keywords(skills: &[SkillInstruction], keywords: &[&str]) -> bool {
    skills.iter().any(|skill| {
        let haystack = format!(
            "{} {}",
            skill.skill_name.to_ascii_lowercase(),
            skill.instructions.to_ascii_lowercase()
        );
        keywords.iter().any(|keyword| haystack.contains(keyword))
    })
}

fn map_progress_phase_to_event_type(phase: &str) -> &'static str {
    match phase {
        "step_started" => "step.started",
        "step_completed" => "step.completed",
        "step_failed" => "step.failed",
        _ => "execution.progress",
    }
}

/// Orchestrator result for a handled event
#[derive(Debug)]
pub enum OrchestratorResult {
    /// A new interaction was started and executed
    Started {
        interaction_id: InteractionId,
        task_id: TaskId,
        result: ExecutionResult,
    },
    /// The event was merged into an existing interaction and executed
    Merged {
        interaction_id: InteractionId,
        task_id: TaskId,
        result: ExecutionResult,
    },
    /// The event was rejected
    Rejected { reason: String },
    /// The event was queued
    Queued,
}

/// Orchestrator errors
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("runtime error: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("planner error: {0}")]
    Planner(#[from] PlanError),
    #[error("normalize error: {0}")]
    Normalize(#[from] NormalizeError),
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("context error: {0}")]
    Context(#[from] ContextError),
    #[error("task not found: {0}")]
    TaskNotFound(String),
    #[error("task has no plan: {0}")]
    MissingPlan(String),
    #[error("resume error: {0}")]
    ResumeError(String),
    #[error("unsupported event: {0}")]
    UnsupportedEvent(String),
}

/// Orchestrator - wires runtime + planner + executor for a minimal pipeline
pub struct Orchestrator {
    pub thread_runtime: ThreadRuntime,
    pub planner: Arc<dyn Planner>,
    pub normalizer: PlanNormalizer,
    pub executor: Executor,
    pub task_store: Arc<dyn TaskStore>,
    pub reference_store: Arc<dyn ReferenceStore>,
    pub context_builder: Option<Arc<dyn ContextBuilder>>,
    pub config: OrchestratorConfig,
    pub hook_registry: Arc<HookRegistry>,
    pub skill_catalog: Arc<SkillCatalog>,
}

/// Orchestrator configuration
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Max events to include in planner history (0 = all)
    pub history_limit: usize,
    /// Token budget for context assembly
    pub context_budget: TokenBudget,
    /// Whether to include history when building context
    pub include_history: bool,
    /// Whether to include references when building context
    pub include_references: bool,
    /// Retry once by replanning only the failed subgraph.
    pub auto_replan_once: bool,
    /// Retry once by asking the planner to repair a plan rejected by normalization.
    pub auto_repair_plan_once: bool,
    /// Enable stage-based reactor flow for supported artifact families.
    pub reactor_enabled: bool,
    /// Max stages runtime may execute in one turn before failing closed.
    pub reactor_stage_loop_limit: usize,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            history_limit: 50,
            context_budget: TokenBudget::default(),
            include_history: true,
            include_references: true,
            auto_replan_once: true,
            auto_repair_plan_once: true,
            reactor_enabled: false,
            reactor_stage_loop_limit: 4,
        }
    }
}

#[derive(Debug, Clone)]
enum NormalizeRepairMode {
    FullPlan,
    SubgraphPatch { cut_step_id: String },
}

#[derive(Debug, Clone)]
struct NormalizeRepairTarget {
    mode: NormalizeRepairMode,
    invalid_step_id: Option<String>,
    affected_steps: Vec<String>,
}

impl NormalizeRepairTarget {
    fn mode_label(&self) -> &'static str {
        match self.mode {
            NormalizeRepairMode::FullPlan => "full_plan",
            NormalizeRepairMode::SubgraphPatch { .. } => "subgraph_patch",
        }
    }

    fn cut_step_id(&self) -> Option<&str> {
        match &self.mode {
            NormalizeRepairMode::FullPlan => None,
            NormalizeRepairMode::SubgraphPatch { cut_step_id } => Some(cut_step_id.as_str()),
        }
    }
}

impl Orchestrator {
    /// Create a new orchestrator
    pub fn new(
        thread_runtime: ThreadRuntime,
        planner: Arc<dyn Planner>,
        normalizer: PlanNormalizer,
        executor: Executor,
        task_store: Arc<dyn TaskStore>,
        reference_store: Arc<dyn ReferenceStore>,
    ) -> Self {
        Self::with_config(
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            reference_store,
            OrchestratorConfig::default(),
        )
    }

    /// Create a new orchestrator with config
    pub fn with_config(
        thread_runtime: ThreadRuntime,
        planner: Arc<dyn Planner>,
        normalizer: PlanNormalizer,
        executor: Executor,
        task_store: Arc<dyn TaskStore>,
        reference_store: Arc<dyn ReferenceStore>,
        config: OrchestratorConfig,
    ) -> Self {
        Self {
            thread_runtime,
            planner,
            normalizer,
            executor,
            task_store,
            reference_store,
            context_builder: None,
            config,
            hook_registry: Arc::new(HookRegistry::new()),
            skill_catalog: Arc::new(SkillCatalog::new(Vec::new(), 0)),
        }
    }

    /// Attach a context builder (optional)
    pub fn with_context_builder(mut self, builder: Arc<dyn ContextBuilder>) -> Self {
        self.context_builder = Some(builder);
        self
    }

    /// Attach runtime hook registry.
    pub fn with_hook_registry(mut self, hook_registry: Arc<HookRegistry>) -> Self {
        self.hook_registry = hook_registry;
        self
    }

    /// Attach skill catalog for planner-time instruction injection.
    pub fn with_skill_catalog(mut self, skill_catalog: Arc<SkillCatalog>) -> Self {
        self.skill_catalog = skill_catalog;
        self
    }

    /// Handle an event end-to-end (intent → plan → normalize → execute)
    pub async fn handle_event(
        &self,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        if let Some(interaction_id) = self.thread_runtime.find_resume_interaction(&event).await {
            return self.resume_interaction(interaction_id, event).await;
        }

        self.start_new_interaction(event).await
    }

    async fn start_new_interaction(
        &self,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let event_clone = event.clone();
        tracing::info!(
            thread_id = %event_clone.thread_id(),
            event_type = %event_type_label(&event_clone),
            "orchestrator received event"
        );
        let decision = self.thread_runtime.handle_event(event).await?;

        let (interaction_id, started_kind) = match &decision {
            HandleEventResult::Started { interaction_id } => (interaction_id.clone(), "started"),
            HandleEventResult::Merged { interaction_id } => (interaction_id.clone(), "merged"),
            HandleEventResult::Rejected { reason } => {
                self.emit_lifecycle_event(
                    "turn_rejected",
                    None,
                    None,
                    Some(reason),
                    serde_json::json!({ "reason": reason }),
                )
                .await;
                return Ok(OrchestratorResult::Rejected {
                    reason: reason.clone(),
                });
            }
            HandleEventResult::Queued => {
                self.emit_lifecycle_event(
                    "turn_queued",
                    None,
                    None,
                    Some("event queued"),
                    Value::Null,
                )
                .await;
                return Ok(OrchestratorResult::Queued);
            }
        };

        // Build intent from event
        let intent = intent_from_event(&event_clone, Some(interaction_id.to_string()))?;

        // Create and persist task
        let task = Task::new(intent);
        self.task_store.save(&task).await?;
        self.thread_runtime
            .add_task_to_interaction(interaction_id.as_str(), task.id.clone())
            .await?;
        self.emit_lifecycle_event(
            "turn_started",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("turn started"),
            serde_json::json!({
                "started_kind": started_kind,
                "event_type": event_type_label(&event_clone),
            }),
        )
        .await;

        self.run_planning_pipeline(interaction_id, started_kind, task)
            .await
    }

    async fn resume_interaction(
        &self,
        interaction_id: InteractionId,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        self.thread_runtime
            .append_event_to_interaction(interaction_id.as_str(), event.clone())
            .await?;
        self.thread_runtime
            .resume_interaction(interaction_id.as_str())
            .await?;

        let interaction = self
            .thread_runtime
            .get_interaction(interaction_id.as_str())
            .await
            .ok_or_else(|| OrchestratorError::ResumeError("interaction missing".to_string()))?;
        let task_id =
            interaction.task_ids.last().cloned().ok_or_else(|| {
                OrchestratorError::ResumeError("interaction has no task".to_string())
            })?;

        let mut task = self
            .task_store
            .load(task_id.as_str())
            .await?
            .ok_or_else(|| OrchestratorError::TaskNotFound(task_id.to_string()))?;
        self.emit_lifecycle_event(
            "turn_resumed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("resuming waiting turn"),
            serde_json::json!({
                "event_type": event_type_label(&event),
            }),
        )
        .await;
        if task.plan.is_none() {
            let intent = intent_from_event(&event, Some(interaction_id.to_string()))?;
            let new_task = Task::new(intent);
            self.task_store.save(&new_task).await?;
            self.thread_runtime
                .add_task_to_interaction(interaction_id.as_str(), new_task.id.clone())
                .await?;
            self.emit_lifecycle_event(
                "turn_started",
                Some(interaction_id.as_str()),
                Some(new_task.id.as_str()),
                Some("turn started from clarification follow-up"),
                serde_json::json!({
                    "started_kind": "merged",
                    "event_type": event_type_label(&event),
                    "resumed_from_task_id": task.id.clone(),
                }),
            )
            .await;
            return self
                .run_planning_pipeline(interaction_id, "merged", new_task)
                .await;
        }
        if task.reactor.is_some() {
            return self
                .resume_reactor_waiting_interaction(interaction_id, task, event)
                .await;
        }
        task.start_executing();
        self.task_store.save(&task).await?;

        self.emit_lifecycle_event(
            "execution_started",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("execution resumed"),
            serde_json::json!({ "resume": true }),
        )
        .await;
        let result = self
            .execute_existing_task(&mut task, interaction_id.as_str(), Some(&event))
            .await?;
        self.emit_lifecycle_event(
            "execution_completed",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("execution completed"),
            execution_result_metadata(&result),
        )
        .await;
        self.emit_interpreted_output(interaction_id.as_str(), &task, &result)
            .await;

        Ok(OrchestratorResult::Merged {
            interaction_id,
            task_id: task.id.clone(),
            result,
        })
    }

    async fn run_planning_pipeline(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let turn_started_at = Instant::now();
        let actions = self.available_actions().await;
        let mut history = self
            .history_for_planner(interaction_id.as_str(), task.id.as_str())
            .await?;
        drop_current_turn_user_input(&mut history, &task.intent.content);
        self.emit_lifecycle_event(
            "planning_started",
            Some(interaction_id.as_str()),
            Some(task.id.as_str()),
            Some("planning started"),
            serde_json::json!({
                "available_actions": actions.len(),
                "history_items": history.len(),
            }),
        )
        .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            available_actions = actions.len(),
            history_items = history.len(),
            "orchestrator planning started"
        );
        let skill_instructions = self.skill_catalog.build_instructions(&task.intent.content);
        let runtime_info = PlannerRuntimeInfo::detect();
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions.clone());
        let planner_started_at = Instant::now();
        let planner_output = self.planner.plan(&task.intent, &context).await?;
        let planner_elapsed_ms = planner_started_at.elapsed().as_millis() as u64;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            planner_elapsed_ms = planner_elapsed_ms,
            "orchestrator planner latency"
        );
        match planner_output {
            PlannerOutput::SkeletonChoice(choice) => {
                if !self.config.reactor_enabled {
                    return Err(OrchestratorError::Planner(PlanError::Generation(
                        "planner returned skeleton_choice while runtime.reactor.enabled=false"
                            .to_string(),
                    )));
                }
                let stage_choice = resolve_stage_choice_from_skeleton_choice(
                    choice,
                    None,
                    DerivationPolicy::Strict,
                )?;
                self.run_reactor_pipeline(interaction_id, started_kind, task, stage_choice)
                    .await
            }
            PlannerOutput::Workflow(plan) => {
                let plan = maybe_shape_probe_first_plan(
                    &task.intent,
                    &skill_instructions,
                    &context.available_actions,
                    plan,
                );
                self.emit_lifecycle_event(
                    "planning_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "workflow",
                        "goal": plan.goal.clone(),
                        "step_count": plan.steps.len(),
                        "steps": summarize_plan_steps(&plan),
                    }),
                )
                .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    goal = %plan.goal,
                    step_count = plan.steps.len(),
                    "orchestrator planning completed"
                );
                if tracing::enabled!(tracing::Level::DEBUG) {
                    tracing::debug!(
                        interaction_id = %interaction_id,
                        task_id = %task.id,
                        plan = %truncate_debug_for_log(&plan.steps, MAX_LOG_CHARS),
                        "orchestrator plan detail"
                    );
                }
                task.set_plan(plan);
                task.start_executing();
                self.task_store.save(&task).await?;

                let planned_step_count = task
                    .plan
                    .as_ref()
                    .map(|p| p.steps.len())
                    .unwrap_or_default();
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution started"),
                    serde_json::json!({ "step_count": planned_step_count }),
                )
                .await;
                let result = self
                    .execute_existing_task(&mut task, interaction_id.as_str(), None)
                    .await?;
                let execute_elapsed_ms = turn_started_at.elapsed().as_millis() as u64;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution completed"),
                    execution_result_metadata(&result),
                )
                .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    execute_elapsed_ms = execute_elapsed_ms,
                    "orchestrator execution pipeline latency"
                );
                let interpret_started_at = Instant::now();
                self.emit_interpreted_output(interaction_id.as_str(), &task, &result)
                    .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    interpret_elapsed_ms = interpret_started_at.elapsed().as_millis() as u64,
                    total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
                    "orchestrator interpretation latency"
                );

                let response = match started_kind {
                    "started" => OrchestratorResult::Started {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                    _ => OrchestratorResult::Merged {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                };
                Ok(response)
            }
            PlannerOutput::StageChoice(choice) => {
                if !self.config.reactor_enabled {
                    return Err(OrchestratorError::Planner(PlanError::Generation(
                        "planner returned stage_choice while runtime.reactor.enabled=false"
                            .to_string(),
                    )));
                }
                self.run_reactor_pipeline(interaction_id, started_kind, task, choice)
                    .await
            }
            PlannerOutput::DirectResponse(message) => {
                self.emit_lifecycle_event(
                    "planning_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "direct_response",
                        "step_count": 0,
                    }),
                )
                .await;
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution skipped"),
                    serde_json::json!({
                        "step_count": 0,
                        "execution_mode": "direct_response",
                    }),
                )
                .await;

                let result = ExecutionResult::Completed;
                task.set_state(TaskState::Done);
                self.task_store.save(&task).await?;
                self.thread_runtime
                    .update_interaction_state(interaction_id.as_str(), InteractionState::Completed)
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution completed"),
                    serde_json::json!({
                        "status": "completed",
                        "execution_mode": "direct_response",
                    }),
                )
                .await;
                self.emit_assistant_output_message(
                    interaction_id.as_str(),
                    task.id.as_str(),
                    message,
                    Value::Null,
                    Some(serde_json::json!({
                        "status": "completed",
                        "execution_mode": "direct_response",
                    })),
                )
                .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
                    "orchestrator direct_response latency"
                );

                let response = match started_kind {
                    "started" => OrchestratorResult::Started {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                    _ => OrchestratorResult::Merged {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                };
                Ok(response)
            }
            PlannerOutput::Clarification(question) => {
                self.emit_lifecycle_event(
                    "planning_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("planning completed"),
                    serde_json::json!({
                        "output_type": "clarification",
                        "step_count": 0,
                    }),
                )
                .await;
                self.emit_lifecycle_event(
                    "execution_started",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution skipped"),
                    serde_json::json!({
                        "step_count": 0,
                        "execution_mode": "clarification",
                    }),
                )
                .await;

                let result = ExecutionResult::WaitingUser {
                    step_id: "planner".into(),
                    prompt: question.clone(),
                    approval: None,
                };
                task.wait_for_user(question.clone());
                self.task_store.save(&task).await?;
                self.thread_runtime
                    .update_interaction_state(
                        interaction_id.as_str(),
                        InteractionState::WaitingUser,
                    )
                    .await?;
                self.emit_lifecycle_event(
                    "execution_completed",
                    Some(interaction_id.as_str()),
                    Some(task.id.as_str()),
                    Some("execution completed"),
                    serde_json::json!({
                        "status": "clarification",
                        "prompt": truncate_for_log(&question, 400),
                        "waiting_kind": "input",
                        "execution_mode": "clarification",
                    }),
                )
                .await;
                self.emit_assistant_output_message(
                    interaction_id.as_str(),
                    task.id.as_str(),
                    question,
                    Value::Null,
                    Some(serde_json::json!({
                        "status": "clarification",
                        "waiting_kind": "input",
                        "execution_mode": "clarification",
                    })),
                )
                .await;
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
                    "orchestrator clarification latency"
                );

                let response = match started_kind {
                    "started" => OrchestratorResult::Started {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                    _ => OrchestratorResult::Merged {
                        interaction_id,
                        task_id: task.id,
                        result,
                    },
                };
                Ok(response)
            }
        }
    }

    async fn run_reactor_pipeline(
        &self,
        interaction_id: InteractionId,
        started_kind: &str,
        mut task: Task,
        initial_choice: StageChoice,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        let turn_started_at = Instant::now();
        let mut choice = initial_choice;
        let mut remaining_stages = self.config.reactor_stage_loop_limit;

        let final_result = loop {
            if remaining_stages == 0 {
                break ExecutionResult::Failed {
                    step_id: StepId::from("reactor"),
                    error: "reactor stage loop limit reached".to_string(),
                };
            }
            remaining_stages -= 1;

            let mut reactor_state = task.reactor.clone().unwrap_or(ReactorTaskState {
                skeleton: choice.skeleton,
                artifact_family: choice.artifact_family,
                current_stage: choice.current_stage,
                derivation_policy: choice.derivation_policy,
                last_continuation: None,
                last_verify: None,
                verify_required: true,
            });
            reactor_state.skeleton = choice.skeleton;
            reactor_state.artifact_family = choice.artifact_family;
            reactor_state.current_stage = choice.current_stage;
            reactor_state.derivation_policy = choice.derivation_policy;
            task.set_reactor_state(reactor_state.clone());
            self.task_store.save(&task).await?;

            let stage_plan = match choice.current_stage {
                StageKind::Probe | StageKind::Commit | StageKind::Verify => {
                    Some(self.lower_reactor_stage_plan(&task, &choice)?)
                }
                StageKind::WaitUser | StageKind::Done | StageKind::Failed => None,
                unsupported_stage => {
                    return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                        "reactor stage {:?} not implemented yet",
                        unsupported_stage
                    ))));
                }
            };

            self.emit_lifecycle_event(
                "planning_completed",
                Some(interaction_id.as_str()),
                Some(task.id.as_str()),
                Some("reactor stage selected"),
                serde_json::json!({
                    "output_type": "stage_choice",
                    "skeleton": choice.skeleton,
                    "artifact_family": choice.artifact_family,
                    "current_stage": choice.current_stage,
                    "stage_goal": choice.stage_goal,
                    "derivation_policy": choice.derivation_policy,
                    "next_stage_hint": choice.next_stage_hint,
                    "reason": choice.reason,
                    "step_count": stage_plan.as_ref().map(|plan| plan.steps.len()).unwrap_or(0),
                    "steps": stage_plan
                        .as_ref()
                        .map(summarize_plan_steps)
                        .unwrap_or_else(|| serde_json::json!({ "items": [], "total_steps": 0 })),
                }),
            )
            .await;

            match choice.current_stage {
                StageKind::WaitUser => {
                    let prompt = choice
                        .reason
                        .clone()
                        .unwrap_or_else(|| "Need additional input".to_string());
                    break ExecutionResult::WaitingUser {
                        step_id: StepId::from("reactor"),
                        prompt,
                        approval: None,
                    };
                }
                StageKind::Done => break ExecutionResult::Completed,
                StageKind::Failed => {
                    break ExecutionResult::Failed {
                        step_id: StepId::from("reactor"),
                        error: choice
                            .reason
                            .clone()
                            .unwrap_or_else(|| "reactor stage failed".to_string()),
                    }
                }
                StageKind::Probe | StageKind::Commit | StageKind::Verify => {}
                unsupported_stage => {
                    return Err(OrchestratorError::Planner(PlanError::Generation(format!(
                        "reactor stage {:?} cannot execute without lowering support",
                        unsupported_stage
                    ))));
                }
            }

            let plan = stage_plan.expect("stage plan must exist for executable reactor stages");
            task.set_plan(plan.clone());
            task.start_executing();
            self.task_store.save(&task).await?;
            self.emit_lifecycle_event(
                "execution_started",
                Some(interaction_id.as_str()),
                Some(task.id.as_str()),
                Some("reactor stage execution started"),
                serde_json::json!({
                    "execution_mode": "reactor",
                    "current_stage": choice.current_stage,
                    "step_count": plan.steps.len(),
                }),
            )
            .await;

            let snapshot = self
                .execute_plan_once(&task, interaction_id.as_str(), &plan, None)
                .await?;
            task.set_checkpoint(
                snapshot.completed_step_ids.clone(),
                snapshot.working_set_snapshot.clone(),
            );
            self.task_store.save(&task).await?;
            self.emit_lifecycle_event(
                "execution_completed",
                Some(interaction_id.as_str()),
                Some(task.id.as_str()),
                Some("reactor stage execution completed"),
                serde_json::json!({
                    "execution_mode": "reactor",
                    "current_stage": choice.current_stage,
                    "result": execution_result_metadata(&snapshot.result),
                }),
            )
            .await;

            match snapshot.result.clone() {
                ExecutionResult::Completed => match choice.current_stage {
                    StageKind::Probe => {
                        let continuation: ContinuationState = parse_working_set_value(
                            &snapshot.working_set_snapshot,
                            "continuation",
                        )?;
                        reactor_state.last_continuation = Some(continuation.clone());
                        task.set_reactor_state(reactor_state);
                        self.task_store.save(&task).await?;
                        match continuation.status {
                            ContinuationStatus::CommitReady => {
                                choice = StageChoice {
                                    skeleton: choice.skeleton,
                                    artifact_family: choice.artifact_family,
                                    current_stage: StageKind::Commit,
                                    stage_goal: reactor_stage_goal(
                                        choice.artifact_family,
                                        StageKind::Commit,
                                    ),
                                    derivation_policy: choice.derivation_policy,
                                    next_stage_hint: continuation.next_stage_hint,
                                    reason: Some(continuation.reason),
                                };
                                continue;
                            }
                            ContinuationStatus::WaitUser => {
                                let prompt = continuation.user_message.unwrap_or_else(|| {
                                    format!("Need more input: {}", continuation.reason)
                                });
                                break ExecutionResult::WaitingUser {
                                    step_id: StepId::from("reactor_probe"),
                                    prompt,
                                    approval: None,
                                };
                            }
                            ContinuationStatus::NeedReplan => {
                                choice = self
                                    .build_reactor_replan_choice(
                                        &task,
                                        &choice,
                                        &continuation.reason,
                                        interaction_id.as_str(),
                                    )
                                    .await?;
                                continue;
                            }
                            ContinuationStatus::Done => break ExecutionResult::Completed,
                            ContinuationStatus::Failed => {
                                break ExecutionResult::Failed {
                                    step_id: StepId::from("reactor_probe"),
                                    error: continuation.reason,
                                };
                            }
                        }
                    }
                    StageKind::Commit => {
                        choice = StageChoice {
                            skeleton: choice.skeleton,
                            artifact_family: choice.artifact_family,
                            current_stage: StageKind::Verify,
                            stage_goal: reactor_stage_goal(
                                choice.artifact_family,
                                StageKind::Verify,
                            ),
                            derivation_policy: choice.derivation_policy,
                            next_stage_hint: Some(StageKind::Verify),
                            reason: choice.reason.clone(),
                        };
                        continue;
                    }
                    StageKind::Verify => {
                        let verify_decision: VerifyDecision = parse_working_set_value(
                            &snapshot.working_set_snapshot,
                            "verify_decision",
                        )?;
                        let mut updated_reactor_state =
                            task.reactor.clone().unwrap_or(ReactorTaskState {
                                skeleton: choice.skeleton,
                                artifact_family: choice.artifact_family,
                                current_stage: choice.current_stage,
                                derivation_policy: choice.derivation_policy,
                                last_continuation: None,
                                last_verify: None,
                                verify_required: true,
                            });
                        updated_reactor_state.last_verify = Some(verify_decision.clone());
                        task.set_reactor_state(updated_reactor_state);
                        self.task_store.save(&task).await?;
                        match verify_decision.status {
                            VerifyStatus::Passed => break ExecutionResult::Completed,
                            VerifyStatus::Failed => {
                                break ExecutionResult::Failed {
                                    step_id: StepId::from("reactor_verify"),
                                    error: verify_decision.reason,
                                }
                            }
                        }
                    }
                    StageKind::WaitUser | StageKind::Done | StageKind::Failed => {
                        break ExecutionResult::Completed;
                    }
                    unsupported_stage => {
                        break ExecutionResult::Failed {
                            step_id: StepId::from("reactor"),
                            error: format!(
                                "reactor stage {:?} completed without runtime completion handler",
                                unsupported_stage
                            ),
                        };
                    }
                },
                ExecutionResult::NeedReplan { prompt, .. } => {
                    choice = self
                        .build_reactor_replan_choice(
                            &task,
                            &choice,
                            &prompt,
                            interaction_id.as_str(),
                        )
                        .await?;
                    continue;
                }
                other => break other,
            }
        };

        let new_state = task_state_from_execution(&final_result);
        task.set_state(new_state.clone());
        self.task_store.save(&task).await?;
        self.thread_runtime
            .update_interaction_state(
                interaction_id.as_str(),
                interaction_state_from_task(&new_state),
            )
            .await?;
        self.emit_interpreted_output(interaction_id.as_str(), &task, &final_result)
            .await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            total_turn_elapsed_ms = turn_started_at.elapsed().as_millis() as u64,
            "orchestrator reactor pipeline finished"
        );

        let response = match started_kind {
            "started" => OrchestratorResult::Started {
                interaction_id,
                task_id: task.id,
                result: final_result,
            },
            _ => OrchestratorResult::Merged {
                interaction_id,
                task_id: task.id,
                result: final_result,
            },
        };
        Ok(response)
    }

    async fn resume_reactor_waiting_interaction(
        &self,
        interaction_id: InteractionId,
        mut task: Task,
        event: Event,
    ) -> Result<OrchestratorResult, OrchestratorError> {
        match &event {
            Event::UserInput { payload, .. } => {
                task.working_set_snapshot
                    .insert("resume_user_input".to_string(), payload.clone());
            }
            Event::ExternalEvent { payload, .. } => {
                task.working_set_snapshot
                    .insert("resume_external_event".to_string(), payload.clone());
            }
            _ => {}
        }
        self.task_store.save(&task).await?;

        let choice = self
            .build_reactor_resume_choice(&task, &event, interaction_id.as_str())
            .await?;
        self.run_reactor_pipeline(interaction_id, "merged", task, choice)
            .await
    }

    async fn build_reactor_resume_choice(
        &self,
        task: &Task,
        event: &Event,
        interaction_id: &str,
    ) -> Result<StageChoice, OrchestratorError> {
        let reactor_state = task.reactor.clone().ok_or_else(|| {
            OrchestratorError::ResumeError("reactor task state missing during resume".to_string())
        })?;
        let current_choice = StageChoice {
            skeleton: reactor_state.skeleton,
            artifact_family: reactor_state.artifact_family,
            current_stage: reactor_state.current_stage,
            stage_goal: reactor_stage_goal(
                reactor_state.artifact_family,
                reactor_state.current_stage,
            ),
            derivation_policy: reactor_state.derivation_policy,
            next_stage_hint: reactor_state
                .last_continuation
                .as_ref()
                .and_then(|continuation| continuation.next_stage_hint),
            reason: reactor_state
                .last_continuation
                .as_ref()
                .map(|continuation| continuation.reason.clone()),
        };
        let resume_text = match event {
            Event::UserInput { payload, .. } | Event::ExternalEvent { payload, .. } => {
                payload_to_string(payload)
            }
            _ => String::new(),
        };

        match parse_reactor_resume_decision(&resume_text) {
            ReactorResumeDecision::Approve => {
                if let Some(next_stage) = reactor_state
                    .last_continuation
                    .as_ref()
                    .and_then(|continuation| continuation.next_stage_hint)
                {
                    return Ok(StageChoice {
                        skeleton: reactor_state.skeleton,
                        artifact_family: reactor_state.artifact_family,
                        current_stage: next_stage,
                        stage_goal: reactor_stage_goal(reactor_state.artifact_family, next_stage),
                        derivation_policy: reactor_state.derivation_policy,
                        next_stage_hint: Some(next_stage),
                        reason: Some(format!(
                            "reactor resume approved by user: {}",
                            truncate_for_log(&resume_text, 300)
                        )),
                    });
                }
            }
            ReactorResumeDecision::Deny => {
                return Ok(StageChoice {
                    skeleton: reactor_state.skeleton,
                    artifact_family: reactor_state.artifact_family,
                    current_stage: StageKind::WaitUser,
                    stage_goal: reactor_stage_goal(
                        reactor_state.artifact_family,
                        StageKind::WaitUser,
                    ),
                    derivation_policy: reactor_state.derivation_policy,
                    next_stage_hint: Some(StageKind::WaitUser),
                    reason: Some(
                        "User declined the pending reactor change. Awaiting updated instructions."
                            .to_string(),
                    ),
                });
            }
            ReactorResumeDecision::Unknown => {}
        }

        let prompt = format!(
            "User resumed the waiting reactor task with: {}",
            truncate_for_log(&resume_text, 600)
        );
        self.build_reactor_replan_choice(task, &current_choice, &prompt, interaction_id)
            .await
    }

    fn lower_reactor_stage_plan(
        &self,
        task: &Task,
        choice: &StageChoice,
    ) -> Result<orchestral_core::types::Plan, OrchestratorError> {
        match (choice.artifact_family, choice.current_stage) {
            (orchestral_core::types::ArtifactFamily::Spreadsheet, StageKind::Probe) => {
                Ok(build_spreadsheet_probe_plan(task, choice))
            }
            (orchestral_core::types::ArtifactFamily::Spreadsheet, StageKind::Commit) => {
                build_spreadsheet_commit_plan(task, choice)
            }
            (orchestral_core::types::ArtifactFamily::Spreadsheet, StageKind::Verify) => {
                build_spreadsheet_verify_plan(task, choice)
            }
            (orchestral_core::types::ArtifactFamily::Document, StageKind::Probe) => {
                Ok(build_document_probe_plan(task, choice))
            }
            (orchestral_core::types::ArtifactFamily::Document, StageKind::Commit) => {
                build_document_commit_plan(task, choice)
            }
            (orchestral_core::types::ArtifactFamily::Document, StageKind::Verify) => {
                build_document_verify_plan(task, choice)
            }
            _ => Err(OrchestratorError::Planner(PlanError::Generation(format!(
                "reactor lowering not implemented for artifact_family={:?} current_stage={:?}",
                choice.artifact_family, choice.current_stage
            )))),
        }
    }

    async fn build_reactor_replan_choice(
        &self,
        task: &Task,
        current_choice: &StageChoice,
        replan_prompt: &str,
        interaction_id: &str,
    ) -> Result<StageChoice, OrchestratorError> {
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            current_stage = ?current_choice.current_stage,
            skeleton = ?current_choice.skeleton,
            artifact_family = ?current_choice.artifact_family,
            replan_prompt = %truncate_for_log(replan_prompt, 400),
            "build_reactor_replan_choice started"
        );

        let actions = self.available_actions().await;
        let history = self
            .history_for_planner(interaction_id, task.id.as_str())
            .await?;
        let ws_summary = summarize_working_set(&task.working_set_snapshot);
        let content = format!(
            "Reactor stage replanning request.\n\
            Original user intent: {}\n\
            Current skeleton: {:?}\n\
            Current artifact family: {:?}\n\
            Current stage: {:?}\n\
            Current working set summary:\n{}\n\
            Replan reason: {}\n\
            Return the next STAGE_CHOICE for the current reactor task. \
            Prefer staying within the current skeleton/artifact family unless current evidence requires otherwise. \
            Do not return a full workflow DAG.",
            task.intent.content,
            current_choice.skeleton,
            current_choice.artifact_family,
            current_choice.current_stage,
            ws_summary,
            replan_prompt,
        );
        let intent = Intent::new(content);

        let runtime_info = PlannerRuntimeInfo::detect();
        let skill_instructions = self.skill_catalog.build_instructions(&task.intent.content);
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions);

        let output = self.planner.plan(&intent, &context).await?;
        match output {
            PlannerOutput::SkeletonChoice(choice) => resolve_stage_choice_from_skeleton_choice(
                choice,
                Some(current_choice.artifact_family),
                current_choice.derivation_policy,
            ),
            PlannerOutput::StageChoice(choice) => Ok(choice),
            PlannerOutput::Clarification(question) => Ok(StageChoice {
                skeleton: current_choice.skeleton,
                artifact_family: current_choice.artifact_family,
                current_stage: StageKind::WaitUser,
                stage_goal: "wait for additional user input".to_string(),
                derivation_policy: current_choice.derivation_policy,
                next_stage_hint: Some(StageKind::WaitUser),
                reason: Some(question),
            }),
            PlannerOutput::Workflow(_) => Err(OrchestratorError::Planner(PlanError::Generation(
                "reactor replan returned workflow; stage_choice required".to_string(),
            ))),
            PlannerOutput::DirectResponse(message) => {
                Err(OrchestratorError::Planner(PlanError::Generation(format!(
                    "reactor replan returned direct_response unexpectedly: {}",
                    truncate_for_log(&message, 200)
                ))))
            }
        }
    }

    async fn execute_existing_task(
        &self,
        task: &mut Task,
        interaction_id: &str,
        resume_event: Option<&Event>,
    ) -> Result<ExecutionResult, OrchestratorError> {
        let initial_plan = task
            .plan
            .clone()
            .ok_or_else(|| OrchestratorError::MissingPlan(task.id.to_string()))?;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            step_count = initial_plan.steps.len(),
            "orchestrator normalize/execute started"
        );
        let mut current_plan = initial_plan.clone();
        let mut remaining_replans = if self.config.auto_replan_once { 1 } else { 0 };
        let mut remaining_plan_repairs = if self.config.auto_repair_plan_once {
            1
        } else {
            0
        };
        let mut remaining_iterative_replans: u32 = 2;
        let mut resume = resume_event;
        let execute_started_at = Instant::now();
        let final_result = loop {
            let one_run_started_at = Instant::now();
            let run = match self
                .execute_plan_once(task, interaction_id, &current_plan, resume)
                .await
            {
                Ok(run) => run,
                Err(OrchestratorError::Normalize(normalize_error))
                    if remaining_plan_repairs > 0 =>
                {
                    remaining_plan_repairs -= 1;
                    self.emit_lifecycle_event(
                        "plan_repair_started",
                        Some(interaction_id),
                        Some(task.id.as_str()),
                        Some("plan normalization failed, attempting constrained repair replan"),
                        serde_json::json!({
                            "error": truncate_for_log(&normalize_error.to_string(), 400),
                        }),
                    )
                    .await;
                    match self
                        .build_normalization_repair_plan(
                            task,
                            &current_plan,
                            &normalize_error,
                            interaction_id,
                        )
                        .await
                    {
                        Ok(Some(repaired_plan)) => {
                            task.set_plan(repaired_plan.clone());
                            task.start_executing();
                            self.task_store.save(task).await?;
                            current_plan = repaired_plan;
                            resume = None;
                            self.emit_lifecycle_event(
                                "plan_repair_completed",
                                Some(interaction_id),
                                Some(task.id.as_str()),
                                Some(
                                    "planner returned a repaired plan after normalization failure",
                                ),
                                serde_json::json!({
                                    "step_count": current_plan.steps.len(),
                                }),
                            )
                            .await;
                            tracing::info!(
                                interaction_id = %interaction_id,
                                task_id = %task.id,
                                plan_repair_elapsed_ms = one_run_started_at.elapsed().as_millis() as u64,
                                "orchestrator plan repair latency"
                            );
                            continue;
                        }
                        Ok(None) => return Err(OrchestratorError::Normalize(normalize_error)),
                        Err(err) => {
                            self.emit_lifecycle_event(
                                "plan_repair_failed",
                                Some(interaction_id),
                                Some(task.id.as_str()),
                                Some("plan repair failed after normalization error"),
                                serde_json::json!({
                                    "error": truncate_for_log(&err.to_string(), 400),
                                }),
                            )
                            .await;
                            return Err(err);
                        }
                    }
                }
                Err(err) => return Err(err),
            };
            tracing::info!(
                interaction_id = %interaction_id,
                task_id = %task.id,
                run_elapsed_ms = one_run_started_at.elapsed().as_millis() as u64,
                result = %truncate_debug_for_log(&run.result, MAX_LOG_CHARS),
                "orchestrator execute_plan_once latency"
            );
            tracing::info!(
                interaction_id = %interaction_id,
                task_id = %task.id,
                completed_steps = ?run.completed_step_ids,
                ws_keys = ?run.working_set_snapshot.keys().collect::<Vec<_>>(),
                "orchestrator checkpoint snapshot"
            );
            task.set_checkpoint(run.completed_step_ids, run.working_set_snapshot);
            self.task_store.save(task).await?;

            if let ExecutionResult::Failed { step_id, error } = &run.result {
                if remaining_replans > 0 {
                    remaining_replans -= 1;
                    self.emit_lifecycle_event(
                        "replanning_started",
                        Some(interaction_id),
                        Some(task.id.as_str()),
                        Some("execution failed, attempting one-shot recovery replan"),
                        serde_json::json!({
                            "failed_step_id": step_id,
                            "error": truncate_for_log(error, 400),
                        }),
                    )
                    .await;
                    match self
                        .build_recovery_plan(
                            task,
                            &current_plan,
                            step_id.as_str(),
                            error,
                            interaction_id,
                        )
                        .await
                    {
                        Ok(Some(patched_plan)) => {
                            task.set_plan(patched_plan.clone());
                            task.start_executing();
                            self.task_store.save(task).await?;
                            current_plan = patched_plan;
                            resume = None;
                            self.emit_lifecycle_event(
                                "replanning_completed",
                                Some(interaction_id),
                                Some(task.id.as_str()),
                                Some("recovery plan patched"),
                                serde_json::json!({
                                    "step_count": current_plan.steps.len(),
                                }),
                            )
                            .await;
                            tracing::info!(
                                interaction_id = %interaction_id,
                                task_id = %task.id,
                                recovery_replan_elapsed_ms = one_run_started_at.elapsed().as_millis() as u64,
                                "orchestrator recovery replan latency"
                            );
                            continue;
                        }
                        Ok(None) => {}
                        Err(err) => {
                            self.emit_lifecycle_event(
                                "replanning_failed",
                                Some(interaction_id),
                                Some(task.id.as_str()),
                                Some("replanning failed, fallback to failure result"),
                                serde_json::json!({
                                    "error": truncate_for_log(&err.to_string(), 400),
                                }),
                            )
                            .await;
                        }
                    }
                }
            }

            if let ExecutionResult::NeedReplan { step_id, prompt } = &run.result {
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    replan_step_id = %step_id,
                    prompt = %truncate_for_log(prompt, 200),
                    completed_step_ids = ?task.completed_step_ids,
                    remaining_iterative_replans = remaining_iterative_replans,
                    "orchestrator NeedReplan received"
                );
                if remaining_iterative_replans == 0 {
                    tracing::warn!(
                        interaction_id = %interaction_id,
                        task_id = %task.id,
                        "iterative replan depth limit reached, breaking"
                    );
                    break ExecutionResult::Failed {
                        step_id: step_id.clone(),
                        error: "Iterative replan depth limit reached".to_string(),
                    };
                }
                remaining_iterative_replans -= 1;
                self.emit_lifecycle_event(
                    "iterative_replan_started",
                    Some(interaction_id),
                    Some(task.id.as_str()),
                    Some("replan step reached, invoking planner for continuation"),
                    serde_json::json!({
                        "replan_step_id": step_id,
                        "prompt": truncate_for_log(prompt, 400),
                    }),
                )
                .await;

                match self
                    .build_continuation_plan(task, &current_plan, step_id, prompt, interaction_id)
                    .await
                {
                    Ok(extended_plan) => {
                        if !task.completed_step_ids.contains(step_id) {
                            task.completed_step_ids.push(step_id.clone());
                        }
                        task.set_plan(extended_plan.clone());
                        task.start_executing();
                        self.task_store.save(task).await?;
                        current_plan = extended_plan;
                        resume = None;
                        self.emit_lifecycle_event(
                            "iterative_replan_completed",
                            Some(interaction_id),
                            Some(task.id.as_str()),
                            Some("continuation steps appended"),
                            serde_json::json!({
                                "step_count": current_plan.steps.len(),
                            }),
                        )
                        .await;
                        continue;
                    }
                    Err(err) => {
                        self.emit_lifecycle_event(
                            "iterative_replan_failed",
                            Some(interaction_id),
                            Some(task.id.as_str()),
                            Some("iterative replan failed"),
                            serde_json::json!({
                                "error": truncate_for_log(&err.to_string(), 400),
                            }),
                        )
                        .await;
                    }
                }
            }

            break run.result;
        };

        let new_state = task_state_from_execution(&final_result);
        task.set_state(new_state.clone());
        self.task_store.save(task).await?;
        self.thread_runtime
            .update_interaction_state(interaction_id, interaction_state_from_task(&new_state))
            .await?;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            execute_existing_task_elapsed_ms = execute_started_at.elapsed().as_millis() as u64,
            "orchestrator execute_existing_task finished"
        );

        Ok(final_result)
    }

    async fn execute_plan_once(
        &self,
        task: &Task,
        interaction_id: &str,
        plan: &orchestral_core::types::Plan,
        resume_event: Option<&Event>,
    ) -> Result<PlanExecutionSnapshot, OrchestratorError> {
        let normalized = self.normalizer.normalize(plan.clone())?;
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                interaction_id = %interaction_id,
                task_id = %task.id,
                normalized_steps = %truncate_debug_for_log(&normalized.plan.steps, MAX_LOG_CHARS),
                "orchestrator normalized plan detail"
            );
        }
        let mut dag = normalized.dag;
        restore_checkpoint(&mut dag, task);
        let dag_completed_after_restore: Vec<&str> = dag.completed_nodes();
        let dag_ready_after_restore: Vec<String> = dag.ready_nodes.clone();
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            task_completed_step_ids = ?task.completed_step_ids,
            dag_completed = ?dag_completed_after_restore,
            dag_ready = ?dag_ready_after_restore,
            plan_step_count = plan.steps.len(),
            "execute_plan_once DAG state after restore_checkpoint"
        );
        if let Some(event) = resume_event {
            complete_wait_step_for_resume(&mut dag, plan, &task.completed_step_ids, event);
        }

        let mut ws = WorkingSet::new();
        ws.import_task_data(task.working_set_snapshot.clone());
        if let Some(event) = resume_event {
            apply_resume_event_to_working_set(&mut ws, event);
        }
        let working_set = Arc::new(RwLock::new(ws));
        let progress_reporter = Arc::new(RuntimeProgressReporter::new(
            self.thread_runtime.thread_id().await,
            InteractionId::from(interaction_id),
            self.thread_runtime.event_store.clone(),
            self.thread_runtime.event_bus.clone(),
            self.hook_registry.clone(),
        ));
        let runtime_info = PlannerRuntimeInfo::detect();
        let skill_instructions = self.skill_catalog.build_instructions(&task.intent.content);
        let exec_ctx = ExecutorContext::new(
            task.id.clone(),
            working_set.clone(),
            self.reference_store.clone(),
        )
        .with_progress_reporter(progress_reporter)
        .with_runtime_info(runtime_info)
        .with_skill_instructions(skill_instructions);
        let result = self.executor.execute(&mut dag, &exec_ctx).await;
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            result = %truncate_debug_for_log(&result, MAX_LOG_CHARS),
            "orchestrator execution completed"
        );
        let working_set_snapshot = {
            let ws_guard = working_set.read().await;
            ws_guard.export_task_data()
        };
        let completed_step_ids = dag
            .completed_nodes()
            .into_iter()
            .map(StepId::from)
            .collect();
        Ok(PlanExecutionSnapshot {
            result,
            completed_step_ids,
            working_set_snapshot,
        })
    }

    async fn build_recovery_plan(
        &self,
        task: &Task,
        plan: &orchestral_core::types::Plan,
        failed_step_id: &str,
        error: &str,
        interaction_id: &str,
    ) -> Result<Option<orchestral_core::types::Plan>, OrchestratorError> {
        let cut = locate_recovery_cut_step(
            plan,
            failed_step_id,
            error,
            &task.completed_step_ids,
            &task.working_set_snapshot,
        )?;
        let affected = affected_subgraph_steps(plan, &cut.cut_step_id);
        if affected.is_empty() {
            return Ok(None);
        }
        self.emit_lifecycle_event(
            "replanning_cut_selected",
            Some(interaction_id),
            Some(task.id.as_str()),
            Some("selected recovery cut step"),
            serde_json::json!({
                "failed_step_id": failed_step_id,
                "cut_step_id": cut.cut_step_id,
                "reason": cut.reason,
                "confidence": cut.confidence,
                "affected_steps": affected,
            }),
        )
        .await;
        let actions = self.available_actions().await;
        let history = self
            .history_for_planner(interaction_id, task.id.as_str())
            .await?;
        let recovery_intent = build_recovery_intent(
            &task.intent,
            plan,
            failed_step_id,
            &cut.cut_step_id,
            error,
            &affected,
            &task.completed_step_ids,
        );
        let runtime_info = PlannerRuntimeInfo::detect();
        let skill_instructions = self
            .skill_catalog
            .build_instructions(&recovery_intent.content);
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions);
        let recovery_output = self.planner.plan(&recovery_intent, &context).await?;
        let recovery_plan = match recovery_output {
            PlannerOutput::Workflow(plan) => plan,
            PlannerOutput::SkeletonChoice(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "recovery planner returned skeleton_choice; workflow required".to_string(),
                )));
            }
            PlannerOutput::StageChoice(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "recovery planner returned stage_choice; workflow required".to_string(),
                )));
            }
            PlannerOutput::DirectResponse(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "recovery planner returned direct_response; workflow required".to_string(),
                )));
            }
            PlannerOutput::Clarification(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "recovery planner returned clarification; workflow required".to_string(),
                )));
            }
        };
        let patched =
            patch_plan_with_recovery(plan, &cut.cut_step_id, &affected, &recovery_plan.steps)?;
        Ok(Some(patched))
    }

    async fn build_normalization_repair_plan(
        &self,
        task: &Task,
        plan: &orchestral_core::types::Plan,
        normalize_error: &NormalizeError,
        interaction_id: &str,
    ) -> Result<Option<orchestral_core::types::Plan>, OrchestratorError> {
        let Some(target) =
            locate_normalization_repair_target(plan, normalize_error, &task.completed_step_ids)
        else {
            return Ok(None);
        };
        self.emit_lifecycle_event(
            "plan_repair_target_selected",
            Some(interaction_id),
            Some(task.id.as_str()),
            Some("selected constrained repair target for normalization failure"),
            serde_json::json!({
                "mode": target.mode_label(),
                "invalid_step_id": target.invalid_step_id.clone(),
                "cut_step_id": target.cut_step_id(),
                "affected_steps": target.affected_steps,
                "error": truncate_for_log(&normalize_error.to_string(), 400),
            }),
        )
        .await;

        let actions = self.available_actions().await;
        let history = self
            .history_for_planner(interaction_id, task.id.as_str())
            .await?;
        let repair_intent = build_normalization_repair_intent(
            &task.intent,
            plan,
            normalize_error,
            &target,
            &task.completed_step_ids,
        );
        let runtime_info = PlannerRuntimeInfo::detect();
        let skill_instructions = self
            .skill_catalog
            .build_instructions(&repair_intent.content);
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions);
        let repair_output = self.planner.plan(&repair_intent, &context).await?;
        let repair_plan = match repair_output {
            PlannerOutput::Workflow(plan) => plan,
            PlannerOutput::SkeletonChoice(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "normalization repair planner returned skeleton_choice; workflow required"
                        .to_string(),
                )));
            }
            PlannerOutput::StageChoice(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "normalization repair planner returned stage_choice; workflow required"
                        .to_string(),
                )));
            }
            PlannerOutput::DirectResponse(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "normalization repair planner returned direct_response; workflow required"
                        .to_string(),
                )));
            }
            PlannerOutput::Clarification(_) => {
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "normalization repair planner returned clarification; workflow required"
                        .to_string(),
                )));
            }
        };

        match &target.mode {
            NormalizeRepairMode::FullPlan => Ok(Some(repair_plan)),
            NormalizeRepairMode::SubgraphPatch { cut_step_id } => {
                let patched = patch_plan_with_recovery(
                    plan,
                    cut_step_id,
                    &target.affected_steps,
                    &repair_plan.steps,
                )?;
                Ok(Some(patched))
            }
        }
    }

    async fn build_continuation_plan(
        &self,
        task: &Task,
        current_plan: &orchestral_core::types::Plan,
        replan_step_id: &StepId,
        replan_prompt: &str,
        interaction_id: &str,
    ) -> Result<orchestral_core::types::Plan, OrchestratorError> {
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            replan_step_id = %replan_step_id,
            completed_step_ids = ?task.completed_step_ids,
            ws_key_count = task.working_set_snapshot.len(),
            current_plan_steps = current_plan.steps.len(),
            "build_continuation_plan started"
        );
        let actions = self.available_actions().await;
        let history = self
            .history_for_planner(interaction_id, task.id.as_str())
            .await?;

        let ws_summary = summarize_working_set(&task.working_set_snapshot);
        let content = format!(
            "Continuation planning request.\n\
            Original goal: {}\n\
            Original intent: {}\n\
            Completed steps: {}\n\
            Current working set summary:\n{}\n\
            Replan prompt: {}\n\
            Rules: generate ONLY the continuation steps. \
            Do not repeat completed steps. \
            If the working set already contains target paths, previews, or environment facts, do not rediscover or reinspect them. \
            Prefer derive/apply/verify or wait_user from the current evidence. \
            New step IDs must not collide with existing ones.",
            current_plan.goal,
            task.intent.content,
            serde_json::to_string(&task.completed_step_ids).unwrap_or_else(|_| "[]".to_string()),
            ws_summary,
            replan_prompt,
        );
        let continuation_intent = Intent::new(content);

        let runtime_info = PlannerRuntimeInfo::detect();
        let skill_instructions = self.skill_catalog.build_instructions(&task.intent.content);
        let context = PlannerContext::with_history(actions, history, self.reference_store.clone())
            .with_runtime_info(runtime_info)
            .with_skill_instructions(skill_instructions);

        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            "build_continuation_plan calling planner"
        );
        let output = self.planner.plan(&continuation_intent, &context).await?;
        let continuation_plan = match output {
            PlannerOutput::Workflow(plan) => {
                tracing::info!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    continuation_step_count = plan.steps.len(),
                    continuation_step_ids = ?plan.steps.iter().map(|s| s.id.to_string()).collect::<Vec<_>>(),
                    "build_continuation_plan planner returned workflow"
                );
                plan
            }
            PlannerOutput::SkeletonChoice(_) => {
                tracing::warn!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    "build_continuation_plan planner returned skeleton_choice (expected workflow)"
                );
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "continuation planner returned skeleton_choice; workflow required".to_string(),
                )));
            }
            PlannerOutput::StageChoice(_) => {
                tracing::warn!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    "build_continuation_plan planner returned stage_choice (expected workflow)"
                );
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "continuation planner returned stage_choice; workflow required".to_string(),
                )));
            }
            PlannerOutput::DirectResponse(msg) => {
                tracing::warn!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    message = %truncate_for_log(&msg, 200),
                    "build_continuation_plan planner returned direct_response (expected workflow)"
                );
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "continuation planner returned direct_response; workflow required".to_string(),
                )));
            }
            PlannerOutput::Clarification(q) => {
                tracing::warn!(
                    interaction_id = %interaction_id,
                    task_id = %task.id,
                    question = %truncate_for_log(&q, 200),
                    "build_continuation_plan planner returned clarification (expected workflow)"
                );
                return Err(OrchestratorError::Planner(PlanError::Generation(
                    "continuation planner returned clarification; workflow required".to_string(),
                )));
            }
        };

        let mut merged_steps = current_plan.steps.clone();
        let existing_ids: std::collections::HashSet<String> =
            merged_steps.iter().map(|s| s.id.to_string()).collect();

        let all_continuation_steps = continuation_plan.steps;

        let replan_ids: std::collections::HashSet<String> = all_continuation_steps
            .iter()
            .filter(|s| s.kind == StepKind::Replan)
            .map(|s| s.id.to_string())
            .collect();

        let mut replan_redirect: HashMap<String, String> = HashMap::new();
        for replan_step in all_continuation_steps
            .iter()
            .filter(|s| s.kind == StepKind::Replan)
        {
            let redirect_to = replan_step
                .depends_on
                .last()
                .map(|d| d.to_string())
                .unwrap_or_else(|| replan_step_id.to_string());
            replan_redirect.insert(replan_step.id.to_string(), redirect_to);
        }

        let continuation_steps: Vec<Step> = all_continuation_steps
            .into_iter()
            .filter(|s| s.kind != StepKind::Replan)
            .collect();

        let mut rename_map: HashMap<String, String> = HashMap::new();
        for step in &continuation_steps {
            if existing_ids.contains(step.id.as_str()) {
                let new_id = format!("c_{}", step.id);
                rename_map.insert(step.id.to_string(), new_id);
            }
        }

        for mut step in continuation_steps {
            if let Some(new_id) = rename_map.get(step.id.as_str()) {
                step.id = new_id.clone().into();
            }
            step.depends_on = step
                .depends_on
                .into_iter()
                .filter_map(|dep| {
                    if replan_ids.contains(dep.as_str()) {
                        replan_redirect.get(dep.as_str()).map(|target| {
                            rename_map
                                .get(target)
                                .map(|r| StepId::from(r.as_str()))
                                .unwrap_or_else(|| StepId::from(target.as_str()))
                        })
                    } else if let Some(renamed) = rename_map.get(dep.as_str()) {
                        Some(StepId::from(renamed.as_str()))
                    } else {
                        Some(dep)
                    }
                })
                .collect();
            if step.depends_on.is_empty() {
                step.depends_on = vec![replan_step_id.clone()];
            }
            merged_steps.push(step);
        }

        let merged_ids: Vec<String> = merged_steps.iter().map(|s| s.id.to_string()).collect();
        tracing::info!(
            interaction_id = %interaction_id,
            task_id = %task.id,
            merged_step_count = merged_steps.len(),
            merged_step_ids = ?merged_ids,
            "build_continuation_plan merged plan ready"
        );

        Ok(orchestral_core::types::Plan {
            goal: current_plan.goal.clone(),
            steps: merged_steps,
            confidence: current_plan.confidence,
            on_complete: continuation_plan
                .on_complete
                .or_else(|| current_plan.on_complete.clone()),
            on_failure: continuation_plan
                .on_failure
                .or_else(|| current_plan.on_failure.clone()),
        })
    }

    async fn available_actions(&self) -> Vec<ActionMeta> {
        let mut actions = Vec::new();
        let registry = self.executor.action_registry.read().await;
        for name in registry.names() {
            if let Some(action) = registry.get(&name) {
                actions.push(extract_meta(action.as_ref()));
            }
        }
        actions
    }

    async fn history_for_planner(
        &self,
        interaction_id: &str,
        task_id: &str,
    ) -> Result<Vec<HistoryItem>, OrchestratorError> {
        if let Some(builder) = &self.context_builder {
            let request = ContextRequest {
                thread_id: self.thread_runtime.thread_id().await.to_string(),
                task_id: Some(task_id.to_string()),
                interaction_id: Some(interaction_id.to_string()),
                query: None,
                budget: self.config.context_budget.clone(),
                include_history: self.config.include_history,
                include_references: self.config.include_references,
                ref_type_filter: None,
                tags: Vec::new(),
            };
            let window = builder.build(&request).await?;
            return Ok(context_window_to_history(&window));
        }

        let mut events = self
            .thread_runtime
            .query_history(self.config.history_limit)
            .await?;
        events.sort_by_key(|a| a.timestamp());
        Ok(events.iter().filter_map(event_to_history_item).collect())
    }

    async fn emit_lifecycle_event(
        &self,
        event_type: &str,
        interaction_id: Option<&str>,
        task_id: Option<&str>,
        message: Option<&str>,
        metadata: Value,
    ) {
        let thread_id = self.thread_runtime.thread_id().await;
        let mut payload = serde_json::Map::new();
        payload.insert(
            "category".to_string(),
            Value::String("runtime_lifecycle".to_string()),
        );
        payload.insert(
            "event_type".to_string(),
            Value::String(event_type.to_string()),
        );
        if let Some(interaction_id) = interaction_id {
            payload.insert(
                "interaction_id".to_string(),
                Value::String(interaction_id.to_string()),
            );
        }
        if let Some(task_id) = task_id {
            payload.insert("task_id".to_string(), Value::String(task_id.to_string()));
        }
        if let Some(message) = message {
            payload.insert("message".to_string(), Value::String(message.to_string()));
        }
        if !metadata.is_null() {
            payload.insert("metadata".to_string(), metadata);
        }

        let trace = Event::trace(thread_id, "info", Value::Object(payload));
        if let Err(err) = self.thread_runtime.event_store.append(trace.clone()).await {
            tracing::warn!(
                event_type = %event_type,
                error = %err,
                "failed to persist lifecycle event"
            );
            return;
        }

        if let Err(err) = self.thread_runtime.event_bus.publish(trace).await {
            tracing::warn!(
                event_type = %event_type,
                error = %err,
                "failed to publish lifecycle event"
            );
        }
    }

    async fn emit_interpreted_output(
        &self,
        interaction_id: &str,
        task: &Task,
        result: &ExecutionResult,
    ) {
        let started_at = Instant::now();
        let Some(plan) = task.plan.clone() else {
            return;
        };

        let message = resolve_plan_response_template(&plan, result, &task.working_set_snapshot)
            .unwrap_or_else(|| {
                let request = InterpretRequest {
                    intent: task.intent.content.clone(),
                    plan: plan.clone(),
                    execution_result: result.clone(),
                    completed_step_ids: task.completed_step_ids.clone(),
                    working_set_snapshot: task.working_set_snapshot.clone(),
                };
                noop_interpret_sync(&request)
            });

        self.emit_assistant_output_message(
            interaction_id,
            task.id.as_str(),
            message,
            Value::Null,
            Some(execution_result_metadata(result)),
        )
        .await;
        tracing::info!(
            task_id = %task.id,
            interaction_id = %interaction_id,
            emit_interpreted_output_elapsed_ms = started_at.elapsed().as_millis() as u64,
            "orchestrator emit_interpreted_output finished"
        );
    }

    async fn emit_assistant_output_message(
        &self,
        interaction_id: &str,
        task_id: &str,
        message: String,
        metadata: Value,
        result: Option<Value>,
    ) {
        let mut payload = serde_json::Map::new();
        payload.insert("task_id".to_string(), Value::String(task_id.to_string()));
        payload.insert("message".to_string(), Value::String(message.clone()));
        if !metadata.is_null() {
            payload.insert("metadata".to_string(), metadata);
        }
        if let Some(result) = result {
            if !result.is_null() {
                payload.insert("result".to_string(), result);
            }
        }
        let assistant_event = Event::assistant_output(
            self.thread_runtime.thread_id().await,
            interaction_id,
            Value::Object(payload),
        );
        if let Err(err) = self
            .thread_runtime
            .event_store
            .append(assistant_event.clone())
            .await
        {
            tracing::warn!(
                task_id = %task_id,
                interaction_id = %interaction_id,
                error = %err,
                "failed to persist assistant output"
            );
            return;
        }
        if let Err(err) = self.thread_runtime.event_bus.publish(assistant_event).await {
            tracing::warn!(
                task_id = %task_id,
                interaction_id = %interaction_id,
                error = %err,
                "failed to publish assistant output"
            );
            return;
        }
        tracing::info!(
            task_id = %task_id,
            interaction_id = %interaction_id,
            message = %truncate_for_log(&message, 400),
            "assistant output emitted"
        );
    }
}

struct RuntimeProgressReporter {
    thread_id: ThreadId,
    interaction_id: InteractionId,
    event_store: Arc<dyn EventStore>,
    event_bus: Arc<dyn EventBus>,
    hook_registry: Arc<HookRegistry>,
}

impl RuntimeProgressReporter {
    fn new(
        thread_id: ThreadId,
        interaction_id: InteractionId,
        event_store: Arc<dyn EventStore>,
        event_bus: Arc<dyn EventBus>,
        hook_registry: Arc<HookRegistry>,
    ) -> Self {
        Self {
            thread_id,
            interaction_id,
            event_store,
            event_bus,
            hook_registry,
        }
    }
}

#[async_trait]
impl ExecutionProgressReporter for RuntimeProgressReporter {
    async fn report(&self, event: ExecutionProgressEvent) -> Result<(), String> {
        let phase = event.phase.clone();
        let step_id = event.step_id.clone();
        let action = event.action.clone();
        let message = event.message.clone();
        let metadata = event.metadata.clone();
        let hook_ctx = RuntimeHookContext {
            thread_id: self.thread_id.clone(),
            interaction_id: self.interaction_id.clone(),
            task_id: Some(event.task_id.clone()),
            step_id: step_id.clone(),
            action: action.clone(),
            message: message.clone(),
            metadata: metadata.clone(),
            extensions: serde_json::Map::new(),
        };

        let hook_event = RuntimeHookEventEnvelope {
            meta: SpiMeta::runtime_defaults(env!("CARGO_PKG_VERSION")),
            event_type: map_progress_phase_to_event_type(&phase).to_string(),
            event_version: "1.0.0".to_string(),
            occurred_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            payload: serde_json::json!({
                "thread_id": self.thread_id.to_string(),
                "interaction_id": self.interaction_id.to_string(),
                "task_id": event.task_id.to_string(),
                "step_id": step_id.as_ref().map(ToString::to_string),
                "action": action,
                "phase": phase,
                "message": message,
                "metadata": metadata,
            }),
            extensions: serde_json::Map::new(),
        };
        self.hook_registry.dispatch(&hook_event, &hook_ctx).await;

        let mut payload = serde_json::Map::new();
        payload.insert(
            "category".to_string(),
            Value::String("execution_progress".to_string()),
        );
        payload.insert(
            "interaction_id".to_string(),
            Value::String(self.interaction_id.to_string()),
        );
        payload.insert(
            "task_id".to_string(),
            Value::String(event.task_id.to_string()),
        );
        payload.insert("phase".to_string(), Value::String(event.phase));
        if let Some(step_id) = event.step_id {
            payload.insert("step_id".to_string(), Value::String(step_id.to_string()));
        }
        if let Some(action) = event.action {
            payload.insert("action".to_string(), Value::String(action));
        }
        if let Some(message) = event.message {
            payload.insert("message".to_string(), Value::String(message));
        }
        if !event.metadata.is_null() {
            payload.insert("metadata".to_string(), event.metadata);
        }

        let trace = Event::trace(
            self.thread_id.clone(),
            "info",
            Value::Object(payload.clone()),
        );
        self.event_store
            .append(trace.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.event_bus
            .publish(trace)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}

struct PlanExecutionSnapshot {
    result: ExecutionResult,
    completed_step_ids: Vec<StepId>,
    working_set_snapshot: HashMap<String, Value>,
}

struct RecoveryCutStep {
    cut_step_id: String,
    reason: String,
    confidence: f32,
}

fn affected_subgraph_steps(plan: &orchestral_core::types::Plan, cut_step_id: &str) -> Vec<String> {
    if plan.get_step(cut_step_id).is_none() {
        return Vec::new();
    }
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for step in &plan.steps {
        for dep in &step.depends_on {
            dependents
                .entry(dep.as_str())
                .or_default()
                .push(step.id.as_str());
        }
    }
    let mut stack = vec![cut_step_id.to_string()];
    let mut visited = std::collections::HashSet::new();
    while let Some(step_id) = stack.pop() {
        if !visited.insert(step_id.clone()) {
            continue;
        }
        if let Some(next) = dependents.get(step_id.as_str()) {
            for child in next {
                stack.push((*child).to_string());
            }
        }
    }
    visited.into_iter().collect()
}

fn locate_recovery_cut_step(
    plan: &orchestral_core::types::Plan,
    failed_step_id: &str,
    error: &str,
    completed_step_ids: &[StepId],
    _working_set_snapshot: &HashMap<String, Value>,
) -> Result<RecoveryCutStep, OrchestratorError> {
    let failed_step = plan.get_step(failed_step_id).ok_or_else(|| {
        OrchestratorError::ResumeError(format!("failed step '{}' missing", failed_step_id))
    })?;
    let error_lower = error.to_ascii_lowercase();
    let data_contract_error = is_data_contract_error(&error_lower);

    if !data_contract_error {
        return Ok(RecoveryCutStep {
            cut_step_id: failed_step_id.to_string(),
            reason: "step-local/runtime error; keep cut at failed step".to_string(),
            confidence: 0.9,
        });
    }

    if let Some(dep_id) = find_error_referenced_dependency(failed_step, &error_lower) {
        if completed_step_ids.iter().any(|id| id.as_str() == dep_id) {
            return Ok(RecoveryCutStep {
                cut_step_id: dep_id.to_string(),
                reason: "data-contract error references upstream dependency".to_string(),
                confidence: 0.82,
            });
        }
    }

    if let Some(dep_id) = failed_step
        .io_bindings
        .iter()
        .filter_map(|b| step_id_from_key(&b.from))
        .find(|dep| dep.as_str() != failed_step.id.as_str())
    {
        if completed_step_ids
            .iter()
            .any(|id| id.as_str() == dep_id.as_str())
        {
            return Ok(RecoveryCutStep {
                cut_step_id: dep_id,
                reason: "data-contract error with upstream io_binding source".to_string(),
                confidence: 0.76,
            });
        }
    }

    if let Some(dep_id) = failed_step.depends_on.last() {
        if completed_step_ids
            .iter()
            .any(|id| id.as_str() == dep_id.as_str())
        {
            return Ok(RecoveryCutStep {
                cut_step_id: dep_id.to_string(),
                reason: "data-contract error fallback to nearest upstream dependency".to_string(),
                confidence: 0.68,
            });
        }
    }

    Ok(RecoveryCutStep {
        cut_step_id: failed_step_id.to_string(),
        reason: "data-contract error but no reliable upstream producer, keep failed step"
            .to_string(),
        confidence: 0.55,
    })
}

fn is_data_contract_error(error_lower: &str) -> bool {
    const TOKENS: [&str; 11] = [
        "missing declared import",
        "missing required io binding",
        "schema validation failed",
        "invalid io binding",
        "invalid input",
        "invalid output",
        "parse error",
        "failed to parse",
        "json parse",
        "deserializ",
        "invalid format",
    ];
    TOKENS.iter().any(|token| error_lower.contains(token))
}

fn find_error_referenced_dependency<'a>(
    failed_step: &'a Step,
    error_lower: &str,
) -> Option<&'a str> {
    for dep in &failed_step.depends_on {
        if error_lower.contains(&dep.as_str().to_ascii_lowercase()) {
            return Some(dep.as_str());
        }
    }
    None
}

fn step_id_from_key(key: &str) -> Option<String> {
    let (step_id, _) = key.split_once('.')?;
    if step_id.is_empty() {
        None
    } else {
        Some(step_id.to_string())
    }
}

fn build_recovery_intent(
    original: &Intent,
    current_plan: &orchestral_core::types::Plan,
    failed_step_id: &str,
    cut_step_id: &str,
    error: &str,
    affected_steps: &[String],
    completed_step_ids: &[StepId],
) -> Intent {
    let mut context = original.context.clone().unwrap_or_default();
    context.metadata.insert(
        "recovery_mode".to_string(),
        Value::String("subgraph_patch".to_string()),
    );
    let content = format!(
        "Recovery planning request.\n\
Original goal: {}\n\
Original intent: {}\n\
Failed step: {}\n\
Recovery cut step: {}\n\
Error: {}\n\
Affected steps to replace: {}\n\
Completed immutable steps: {}\n\
Rules: return ONLY replacement steps for the affected subgraph; do not redesign completed immutable steps.",
        current_plan.goal,
        original.content,
        failed_step_id,
        cut_step_id,
        truncate_for_log(error, 600),
        serde_json::to_string(affected_steps).unwrap_or_else(|_| "[]".to_string()),
        serde_json::to_string(completed_step_ids).unwrap_or_else(|_| "[]".to_string()),
    );
    Intent::with_context(content, context)
}

fn build_normalization_repair_intent(
    original: &Intent,
    current_plan: &orchestral_core::types::Plan,
    normalize_error: &NormalizeError,
    target: &NormalizeRepairTarget,
    completed_step_ids: &[StepId],
) -> Intent {
    let mut context = original.context.clone().unwrap_or_default();
    context.metadata.insert(
        "recovery_mode".to_string(),
        Value::String(format!("normalize_{}", target.mode_label())),
    );
    if let Some(step_id) = &target.invalid_step_id {
        context.metadata.insert(
            "normalize_invalid_step_id".to_string(),
            Value::String(step_id.clone()),
        );
    }

    let affected_summary = summarize_selected_plan_steps(current_plan, &target.affected_steps);
    let affected_summary = serde_json::to_string_pretty(&affected_summary)
        .unwrap_or_else(|_| "<unable to serialize affected steps>".to_string());
    let mode_rule = match &target.mode {
        NormalizeRepairMode::FullPlan => {
            "Return a FULL replacement WORKFLOW for the original goal. Reuse safe discovery steps if helpful, but do not preserve the invalid topology."
        }
        NormalizeRepairMode::SubgraphPatch { .. } => {
            "Return ONLY replacement steps for the affected subgraph. Do not redesign completed immutable steps."
        }
    };
    let content = format!(
        "Normalization repair request.\n\
Original goal: {}\n\
Original intent: {}\n\
Current plan was rejected by the runtime normalizer before execution.\n\
Normalization error: {}\n\
Repair mode: {}\n\
Invalid step: {}\n\
Affected steps to replace: {}\n\
Completed immutable steps: {}\n\
Affected step summary:\n{}\n\
Rules:\n\
- {}\n\
- Materialize side effects in explicit action or recipe stages.\n\
- Never return an explore agent that owns outputs with output_rules candidates.requires.action.\n\
- If local reasoning is needed, use kind=agent with params.mode=\"leaf\" and let runtime materialize outputs from json_stdout evidence.\n\
- Prefer kind=recipe for multi-stage repairs. A good generic shape is inspect/collect -> derive leaf -> apply/emit -> verify.\n\
- If shell is the only applicable mutable tool, use shell in explicit action stages and verify the outcome explicitly.\n",
        current_plan.goal,
        original.content,
        truncate_for_log(&normalize_error.to_string(), 600),
        target.mode_label(),
        target.invalid_step_id.as_deref().unwrap_or("<unknown>"),
        serde_json::to_string(&target.affected_steps).unwrap_or_else(|_| "[]".to_string()),
        serde_json::to_string(completed_step_ids).unwrap_or_else(|_| "[]".to_string()),
        truncate_for_log(&affected_summary, 3_000),
        mode_rule,
    );
    Intent::with_context(content, context)
}

fn locate_normalization_repair_target(
    current_plan: &orchestral_core::types::Plan,
    normalize_error: &NormalizeError,
    completed_step_ids: &[StepId],
) -> Option<NormalizeRepairTarget> {
    let invalid_step_id = normalize_error_step_id(normalize_error)
        .filter(|step_id| current_plan.get_step(step_id).is_some())
        .map(str::to_string);
    let affected_steps = invalid_step_id
        .as_deref()
        .map(|step_id| affected_subgraph_steps(current_plan, step_id))
        .filter(|steps| !steps.is_empty())
        .unwrap_or_else(|| {
            current_plan
                .steps
                .iter()
                .map(|step| step.id.to_string())
                .collect()
        });

    if completed_step_ids.is_empty() {
        return Some(NormalizeRepairTarget {
            mode: NormalizeRepairMode::FullPlan,
            invalid_step_id,
            affected_steps,
        });
    }

    let cut_step_id = invalid_step_id?;
    Some(NormalizeRepairTarget {
        mode: NormalizeRepairMode::SubgraphPatch {
            cut_step_id: cut_step_id.clone(),
        },
        invalid_step_id: Some(cut_step_id),
        affected_steps,
    })
}

fn normalize_error_step_id(normalize_error: &NormalizeError) -> Option<&str> {
    match normalize_error {
        NormalizeError::Validation(ValidationError::MissingDependency(step_id, _))
        | NormalizeError::Validation(ValidationError::InvalidIoBinding(step_id, _))
        | NormalizeError::Validation(ValidationError::IoBindingMissingDependency(step_id, _))
        | NormalizeError::Validation(ValidationError::InvalidAgentParams(step_id, _))
        | NormalizeError::Validation(ValidationError::CycleDetected(step_id))
        | NormalizeError::Validation(ValidationError::DuplicateStepId(step_id)) => {
            Some(step_id.as_str())
        }
        _ => None,
    }
}

fn patch_plan_with_recovery(
    current_plan: &orchestral_core::types::Plan,
    cut_step_id: &str,
    affected_steps: &[String],
    replacement_steps: &[Step],
) -> Result<orchestral_core::types::Plan, OrchestratorError> {
    if replacement_steps.is_empty() {
        return Err(OrchestratorError::ResumeError(
            "recovery planner returned empty replacement steps".to_string(),
        ));
    }
    let affected: std::collections::HashSet<&str> =
        affected_steps.iter().map(String::as_str).collect();
    let mut base_steps = current_plan
        .steps
        .iter()
        .filter(|s| !affected.contains(s.id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut used_ids: std::collections::HashSet<String> =
        base_steps.iter().map(|s| s.id.to_string()).collect();
    let mut rename_map: HashMap<String, String> = HashMap::new();

    for step in replacement_steps {
        let mut unique = step.id.to_string();
        if used_ids.contains(&unique) {
            let mut idx = 1usize;
            while used_ids.contains(&format!("r{}_{}", idx, step.id)) {
                idx = idx.saturating_add(1);
            }
            unique = format!("r{}_{}", idx, step.id);
        }
        used_ids.insert(unique.clone());
        rename_map.insert(step.id.to_string(), unique);
    }

    let replacement_roots = current_plan
        .get_step(cut_step_id)
        .map(|s| s.depends_on.clone())
        .unwrap_or_default();

    let mut patched_replacements = Vec::with_capacity(replacement_steps.len());
    for step in replacement_steps {
        let mut patched = step.clone();
        if let Some(new_id) = rename_map.get(step.id.as_str()) {
            patched.id = new_id.clone().into();
        }
        patched.depends_on = step
            .depends_on
            .iter()
            .filter_map(|dep| {
                if affected.contains(dep.as_str()) {
                    rename_map.get(dep.as_str()).cloned().map(Into::into)
                } else {
                    Some(dep.clone())
                }
            })
            .collect();
        if patched.depends_on.is_empty() && !replacement_roots.is_empty() {
            patched.depends_on = replacement_roots.clone();
        }
        patched_replacements.push(patched);
    }

    base_steps.extend(patched_replacements);
    Ok(orchestral_core::types::Plan {
        goal: current_plan.goal.clone(),
        steps: base_steps,
        confidence: current_plan.confidence,
        on_complete: current_plan.on_complete.clone(),
        on_failure: current_plan.on_failure.clone(),
    })
}

fn intent_from_event(
    event: &Event,
    interaction_id: Option<String>,
) -> Result<Intent, OrchestratorError> {
    let mut metadata = HashMap::new();
    metadata.insert(
        "event_type".to_string(),
        Value::String(event_type_label(event).to_string()),
    );
    if let Some(id) = interaction_id {
        metadata.insert("interaction_id".to_string(), Value::String(id));
    }

    let context = IntentContext {
        thread_id: Some(event.thread_id().to_string()),
        previous_task_id: None,
        metadata,
    };

    match event {
        Event::UserInput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::ExternalEvent { kind, payload, .. } => {
            let content = format!("external:{} {}", kind, payload_to_string(payload));
            Ok(Intent::with_context(content, context))
        }
        Event::AssistantOutput { payload, .. } => {
            Ok(Intent::with_context(payload_to_string(payload), context))
        }
        Event::SystemTrace { level, payload, .. } => Ok(Intent::with_context(
            format!("trace:{} {}", level, payload_to_string(payload)),
            context,
        )),
        Event::Artifact { reference_id, .. } => Ok(Intent::with_context(
            format!("artifact:{}", reference_id),
            context,
        )),
    }
}

fn summarize_selected_plan_steps(
    plan: &orchestral_core::types::Plan,
    selected_step_ids: &[String],
) -> Value {
    let selected: std::collections::HashSet<&str> =
        selected_step_ids.iter().map(String::as_str).collect();
    let steps = plan
        .steps
        .iter()
        .filter(|step| selected.contains(step.id.as_str()))
        .map(|step| {
            serde_json::json!({
                "id": step.id.clone(),
                "kind": step.kind.clone(),
                "action": step.action.clone(),
                "depends_on": step.depends_on.clone(),
                "exports": step.exports.clone(),
                "io_bindings": step.io_bindings.clone(),
                "params": step.params.clone(),
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "items": steps,
        "total_steps": steps.len(),
    })
}

fn payload_to_string(payload: &Value) -> String {
    if let Some(s) = payload.as_str() {
        return s.to_string();
    }
    for key in ["content", "message", "text"] {
        if let Some(s) = payload.get(key).and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    payload.to_string()
}

fn event_to_history_item(event: &Event) -> Option<HistoryItem> {
    let timestamp = event.timestamp();
    match event {
        Event::UserInput { payload, .. } => Some(HistoryItem {
            role: "user".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        Event::AssistantOutput { payload, .. } => Some(HistoryItem {
            role: "assistant".to_string(),
            content: payload_to_string(payload),
            timestamp,
        }),
        // Planner dialog history should stay focused on user/assistant exchanges.
        // Lifecycle traces/artifacts create high-volume noise and can evict useful turns.
        Event::ExternalEvent { .. } | Event::SystemTrace { .. } | Event::Artifact { .. } => None,
    }
}

fn context_window_to_history(window: &ContextWindow) -> Vec<HistoryItem> {
    let mut items = Vec::new();
    for slice in window.core.iter().chain(window.optional.iter()) {
        items.push(HistoryItem {
            role: slice.role.clone(),
            content: slice.content.clone(),
            timestamp: slice.timestamp.unwrap_or_else(chrono::Utc::now),
        });
    }
    items
}

fn drop_current_turn_user_input(history: &mut Vec<HistoryItem>, current_intent: &str) {
    let Some(last) = history.last() else {
        return;
    };
    if last.role == "user" && last.content.trim() == current_intent.trim() {
        history.pop();
    }
}

fn event_type_label(event: &Event) -> &'static str {
    match event {
        Event::UserInput { .. } => "user_input",
        Event::AssistantOutput { .. } => "assistant_output",
        Event::Artifact { .. } => "artifact",
        Event::ExternalEvent { .. } => "external_event",
        Event::SystemTrace { .. } => "system_trace",
    }
}

fn summarize_plan_steps(plan: &orchestral_core::types::Plan) -> Value {
    const STEP_PREVIEW_LIMIT: usize = 32;
    let steps: Vec<Value> = plan
        .steps
        .iter()
        .take(STEP_PREVIEW_LIMIT)
        .map(|step| {
            serde_json::json!({
                "id": step.id.clone(),
                "action": step.action.clone(),
                "kind": format!("{:?}", step.kind),
                "depends_on": step.depends_on.clone(),
                "io_bindings": step.io_bindings.clone(),
            })
        })
        .collect();

    if plan.steps.len() > STEP_PREVIEW_LIMIT {
        serde_json::json!({
            "items": steps,
            "truncated": true,
            "total_steps": plan.steps.len(),
        })
    } else {
        serde_json::json!({
            "items": steps,
            "truncated": false,
            "total_steps": plan.steps.len(),
        })
    }
}

fn execution_result_metadata(result: &ExecutionResult) -> Value {
    match result {
        ExecutionResult::Completed => serde_json::json!({
            "status": "completed"
        }),
        ExecutionResult::Failed { step_id, error } => serde_json::json!({
            "status": "failed",
            "step_id": step_id,
            "error": truncate_for_log(error, 400),
        }),
        ExecutionResult::WaitingUser {
            step_id,
            prompt,
            approval,
        } => {
            let mut meta = serde_json::json!({
                "status": "waiting_user",
                "step_id": step_id,
                "prompt": truncate_for_log(prompt, 400),
                "waiting_kind": "input",
            });
            if let Some(req) = approval {
                meta["waiting_kind"] = Value::String("approval".to_string());
                meta["approval_reason"] = Value::String(truncate_for_log(&req.reason, 400));
                if let Some(command) = &req.command {
                    meta["approval_command"] = Value::String(truncate_for_log(command, 400));
                }
            }
            meta
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => serde_json::json!({
            "status": "waiting_event",
            "step_id": step_id,
            "event_type": event_type,
        }),
        ExecutionResult::NeedReplan { step_id, prompt } => serde_json::json!({
            "status": "need_replan",
            "step_id": step_id,
            "prompt": truncate_for_log(prompt, 400),
        }),
    }
}

fn task_state_from_execution(result: &ExecutionResult) -> TaskState {
    match result {
        ExecutionResult::Completed => TaskState::Done,
        ExecutionResult::Failed { error, .. } => TaskState::Failed {
            reason: error.clone(),
            recoverable: false,
        },
        ExecutionResult::WaitingUser {
            prompt, approval, ..
        } => TaskState::WaitingUser {
            prompt: prompt.clone(),
            reason: approval
                .as_ref()
                .map(|a| WaitUserReason::Approval {
                    reason: a.reason.clone(),
                    command: a.command.clone(),
                })
                .unwrap_or(WaitUserReason::Input),
        },
        ExecutionResult::WaitingEvent { event_type, .. } => TaskState::WaitingEvent {
            event_type: event_type.clone(),
        },
        ExecutionResult::NeedReplan { .. } => TaskState::Executing,
    }
}

fn restore_checkpoint(dag: &mut orchestral_core::executor::ExecutionDag, task: &Task) {
    for step_id in &task.completed_step_ids {
        dag.mark_completed(step_id.as_str());
    }
}

fn complete_wait_step_for_resume(
    dag: &mut orchestral_core::executor::ExecutionDag,
    plan: &orchestral_core::types::Plan,
    completed_steps: &[StepId],
    event: &Event,
) {
    let target_kind = match event {
        Event::UserInput { .. } => StepKind::WaitUser,
        Event::ExternalEvent { .. } => StepKind::WaitEvent,
        _ => return,
    };

    let completed: std::collections::HashSet<&str> =
        completed_steps.iter().map(|id| id.as_str()).collect();
    let candidates = plan
        .steps
        .iter()
        .filter(|s| {
            s.kind == target_kind
                && !completed.contains(s.id.as_str())
                && wait_step_matches_resume_event(s, event)
                && s.depends_on
                    .iter()
                    .all(|dep| completed.contains(dep.as_str()))
        })
        .collect::<Vec<_>>();
    if candidates.len() == 1 {
        dag.mark_completed(candidates[0].id.as_str());
    }
}

fn wait_step_matches_resume_event(step: &orchestral_core::types::Step, event: &Event) -> bool {
    match event {
        Event::ExternalEvent { kind, .. } if step.kind == StepKind::WaitEvent => step
            .params
            .get("event_type")
            .and_then(|v| v.as_str())
            .map(|expected| expected == "*" || expected.eq_ignore_ascii_case(kind))
            .unwrap_or(true),
        Event::UserInput { .. } if step.kind == StepKind::WaitUser => true,
        _ => false,
    }
}

fn apply_resume_event_to_working_set(ws: &mut WorkingSet, event: &Event) {
    match event {
        Event::UserInput { payload, .. } => {
            ws.set_task("resume_user_input", payload.clone());
            ws.set_task("last_event_payload", payload.clone());
        }
        Event::ExternalEvent { payload, kind, .. } => {
            ws.set_task("resume_external_event", payload.clone());
            ws.set_task("last_event_payload", payload.clone());
            ws.set_task("last_event_kind", Value::String(kind.clone()));
        }
        _ => {}
    }
}

fn interaction_state_from_task(state: &TaskState) -> InteractionState {
    match state {
        TaskState::Done => InteractionState::Completed,
        TaskState::Failed { .. } => InteractionState::Failed,
        TaskState::WaitingUser { .. } => InteractionState::WaitingUser,
        TaskState::WaitingEvent { .. } => InteractionState::WaitingEvent,
        TaskState::Paused => InteractionState::Paused,
        TaskState::Planning | TaskState::Executing => InteractionState::Active,
    }
}

fn resolve_plan_response_template(
    plan: &orchestral_core::types::Plan,
    result: &ExecutionResult,
    working_set: &HashMap<String, Value>,
) -> Option<String> {
    let template = match result {
        ExecutionResult::Completed => plan.on_complete.as_deref(),
        ExecutionResult::Failed { .. } => plan.on_failure.as_deref(),
        _ => None,
    }?;

    let is_completed = matches!(result, ExecutionResult::Completed);
    let mut resolved = template.to_string();
    for (key, value) in working_set {
        let placeholder = format!("{{{{{}}}}}", key);
        let replacement = value_to_template_replacement(value);
        resolved = resolved.replace(&placeholder, &replacement);
    }
    if let ExecutionResult::Failed { error, .. } = result {
        resolved = resolved.replace("{{error}}", error);
    }
    if is_completed && !template_contains_summary_placeholder(template) {
        if let Some(summary) = best_summary_from_working_set(working_set) {
            if !summary.trim().is_empty() && !resolved.contains(&summary) {
                if !resolved.trim().is_empty() {
                    resolved.push_str("\n\n");
                }
                resolved.push_str(&summary);
            }
        }
    }
    Some(resolved)
}

fn value_to_template_replacement(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn template_contains_summary_placeholder(template: &str) -> bool {
    template.contains("{{summary}}") || template.contains(".summary}}")
}

fn best_summary_from_working_set(working_set: &HashMap<String, Value>) -> Option<String> {
    if let Some(value) = working_set.get("summary") {
        let summary = value_to_template_replacement(value);
        if !summary.trim().is_empty() {
            return Some(summary);
        }
    }

    let mut scoped_keys = working_set
        .keys()
        .filter(|key| key.ends_with(".summary"))
        .cloned()
        .collect::<Vec<_>>();
    scoped_keys.sort();

    for key in scoped_keys {
        if let Some(value) = working_set.get(&key) {
            let summary = value_to_template_replacement(value);
            if !summary.trim().is_empty() {
                return Some(summary);
            }
        }
    }
    None
}

fn noop_interpret_sync(request: &InterpretRequest) -> String {
    let completed = request.completed_step_ids.len();
    let total = request.plan.steps.len();
    match &request.execution_result {
        ExecutionResult::Completed => {
            format!("Done ({}/{} steps).", completed, total)
        }
        ExecutionResult::Failed { step_id, error } => {
            format!(
                "Failed at step '{}' ({}/{} steps): {}",
                step_id,
                completed,
                total,
                truncate_for_log(error, 400)
            )
        }
        ExecutionResult::WaitingUser { prompt, .. } => {
            format!(
                "Need input ({}/{} steps done): {}",
                completed, total, prompt
            )
        }
        ExecutionResult::WaitingEvent {
            step_id,
            event_type,
        } => {
            format!(
                "Waiting for '{}' at step '{}' ({}/{} steps done).",
                event_type, step_id, completed, total
            )
        }
        ExecutionResult::NeedReplan { step_id, prompt } => {
            format!(
                "Replanning at step '{}' ({}/{} steps done): {}",
                step_id, completed, total, prompt
            )
        }
    }
}

fn summarize_working_set(snapshot: &HashMap<String, Value>) -> String {
    const MAX_WS_SUMMARY_CHARS: usize = 16_000;
    const LIMIT_STDOUT: usize = 4_000;
    const LIMIT_STDERR: usize = 1_000;
    const LIMIT_CONTENT: usize = 4_000;
    const LIMIT_DEFAULT: usize = 200;

    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
    enum KeyPriority {
        High = 0,
        Medium = 1,
        Low = 2,
    }

    fn classify_key(key: &str) -> (KeyPriority, usize) {
        if key == "stdout" || key.ends_with(".stdout") {
            (KeyPriority::High, LIMIT_STDOUT)
        } else if key == "content" || key.ends_with(".content") {
            (KeyPriority::High, LIMIT_CONTENT)
        } else if key == "stderr" || key.ends_with(".stderr") {
            (KeyPriority::Medium, LIMIT_STDERR)
        } else {
            (KeyPriority::Low, LIMIT_DEFAULT)
        }
    }

    fn truncate_plain(input: &str, max_chars: usize) -> String {
        let char_count = input.chars().count();
        if char_count <= max_chars {
            return input.to_string();
        }
        if max_chars <= 3 {
            return ".".repeat(max_chars);
        }
        let mut out: String = input.chars().take(max_chars - 3).collect();
        out.push_str("...");
        out
    }

    fn preview_value(value: &Value, max_chars: usize) -> String {
        match value {
            Value::String(s) => format!("\"{}\"", truncate_plain(s.trim(), max_chars)),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
            other => truncate_plain(&other.to_string(), max_chars),
        }
    }

    let mut entries = snapshot.iter().collect::<Vec<_>>();
    entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut buckets: [Vec<(&String, &Value)>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for (key, value) in entries {
        let (priority, _) = classify_key(key);
        buckets[priority as usize].push((key, value));
    }

    let mut output = String::new();
    let mut remaining = MAX_WS_SUMMARY_CHARS;
    let mut omitted_count = 0usize;

    for bucket in &buckets {
        for (key, value) in bucket {
            if remaining == 0 {
                omitted_count += 1;
                continue;
            }
            let (_, per_key_limit) = classify_key(key.as_str());
            let preview = preview_value(value, per_key_limit);
            let line = format!("  {}: {}", key, preview);
            let line_len = line.chars().count();
            let sep_len = if output.is_empty() { 0 } else { 1 };

            if remaining <= sep_len {
                omitted_count += 1;
                continue;
            }
            let line_budget = remaining - sep_len;
            let line_to_append = if line_len > line_budget {
                truncate_plain(&line, line_budget)
            } else {
                line
            };
            if !output.is_empty() {
                output.push('\n');
                remaining -= 1;
            }
            remaining = remaining.saturating_sub(line_to_append.chars().count());
            output.push_str(&line_to_append);

            if line_len > line_budget {
                omitted_count += 1;
            }
        }
    }

    if output.is_empty() {
        return "(empty)".to_string();
    }

    if omitted_count > 0 && remaining > 0 {
        let summary_line = format!(
            "  ... ({} keys omitted due to summary limit)",
            omitted_count
        );
        let truncated = truncate_plain(&summary_line, remaining);
        if !truncated.is_empty() {
            output.push('\n');
            output.push_str(&truncated);
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestral_core::executor::{ExecutionDag, ExecutionProgressEvent};
    use orchestral_core::store::{BroadcastEventBus, EventBus, EventStore, InMemoryEventStore};
    use orchestral_core::types::{Plan, Step, StepId, StepIoBinding, StepKind};
    use serde_json::json;

    #[test]
    fn test_complete_wait_step_for_resume_marks_wait_user() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::wait_user("wait"),
                Step::action("next", "noop").with_depends_on(vec![StepId::from("wait")]),
            ],
        );
        let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
        let event = Event::user_input("thread-1", "int-1", json!({"text":"ok"}));

        complete_wait_step_for_resume(&mut dag, &plan, &[], &event);

        assert!(dag.completed_nodes().contains(&"wait"));
    }

    #[test]
    fn test_complete_wait_step_for_resume_matches_wait_event_kind() {
        let mut wait_timer = Step::action("wait_timer", "wait_event");
        wait_timer.kind = StepKind::WaitEvent;
        wait_timer.params = json!({"event_type":"timer"});

        let mut wait_webhook = Step::action("wait_webhook", "wait_event");
        wait_webhook.kind = StepKind::WaitEvent;
        wait_webhook.params = json!({"event_type":"webhook"});

        let plan = Plan::new(
            "goal",
            vec![
                wait_timer,
                Step::action("prep", "noop"),
                wait_webhook.with_depends_on(vec![StepId::from("prep")]),
            ],
        );
        let mut dag = ExecutionDag::from_plan(&plan).expect("dag");
        dag.mark_completed("prep");
        let event = Event::external("thread-1", "webhook", json!({"ok":true}));

        complete_wait_step_for_resume(&mut dag, &plan, &[StepId::from("prep")], &event);

        assert!(dag.completed_nodes().contains(&"wait_webhook"));
        assert!(!dag.completed_nodes().contains(&"wait_timer"));
    }

    #[test]
    fn test_apply_resume_event_to_working_set_for_external_event() {
        let mut ws = WorkingSet::new();
        let event = Event::external("thread-1", "timer", json!({"due":true}));

        apply_resume_event_to_working_set(&mut ws, &event);

        assert_eq!(
            ws.get_task("resume_external_event"),
            Some(&json!({"due":true}))
        );
        assert_eq!(
            ws.get_task("last_event_payload"),
            Some(&json!({"due":true}))
        );
        assert_eq!(ws.get_task("last_event_kind"), Some(&json!("timer")));
    }

    #[test]
    fn test_runtime_progress_reporter_persists_and_publishes() {
        tokio_test::block_on(async {
            let event_store: Arc<dyn EventStore> = Arc::new(InMemoryEventStore::new());
            let event_bus = Arc::new(BroadcastEventBus::new(16));
            let mut sub = event_bus.subscribe();
            let reporter = RuntimeProgressReporter::new(
                "thread-1".into(),
                "int-1".into(),
                event_store.clone(),
                event_bus.clone(),
                Arc::new(HookRegistry::new()),
            );

            reporter
                .report(
                    ExecutionProgressEvent::new(
                        "task-1",
                        Some(StepId::from("s1")),
                        Some("echo".to_string()),
                        "step_started",
                    )
                    .with_message("running"),
                )
                .await
                .expect("report");

            let events = event_store
                .query_by_thread("thread-1")
                .await
                .expect("query");
            assert_eq!(events.len(), 1);
            match &events[0] {
                Event::SystemTrace { payload, .. } => {
                    assert_eq!(payload["category"], "execution_progress");
                    assert_eq!(payload["interaction_id"], "int-1");
                    assert_eq!(payload["task_id"], "task-1");
                    assert_eq!(payload["phase"], "step_started");
                }
                _ => panic!("expected system trace"),
            }

            let bus_event = sub.recv().await.expect("bus event");
            assert!(matches!(bus_event, Event::SystemTrace { .. }));
        });
    }

    #[test]
    fn test_locate_recovery_cut_step_backtracks_to_upstream_on_data_contract_error() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A", "file_read"),
                Step::action("B", "doc_parse")
                    .with_depends_on(vec![StepId::from("A")])
                    .with_io_bindings(vec![StepIoBinding::required("A.content", "content")]),
                Step::action("C", "summarize").with_depends_on(vec![StepId::from("B")]),
            ],
        );
        let completed = vec![StepId::from("A")];
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "input schema validation failed at $.content",
            &completed,
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "A");
        let affected = affected_subgraph_steps(&plan, &cut.cut_step_id);
        let affected_set: std::collections::HashSet<_> = affected.into_iter().collect();
        assert!(affected_set.contains("A"));
        assert!(affected_set.contains("B"));
        assert!(affected_set.contains("C"));
    }

    #[test]
    fn test_locate_recovery_cut_step_keeps_failed_step_on_runtime_error() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A", "file_read"),
                Step::action("B", "http").with_depends_on(vec![StepId::from("A")]),
            ],
        );
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "request timeout after 10s",
            &[StepId::from("A")],
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "B");
    }

    #[test]
    fn test_locate_recovery_cut_step_does_not_backtrack_to_unfinished_dependency() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A", "file_read"),
                Step::action("B", "doc_parse")
                    .with_depends_on(vec![StepId::from("A")])
                    .with_io_bindings(vec![StepIoBinding::required("A.content", "content")]),
            ],
        );
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "missing required io binding 'content'",
            &[],
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "B");
    }

    #[test]
    fn test_locate_recovery_cut_step_prefers_error_referenced_dependency() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("A1", "fetch_profile"),
                Step::action("A2", "fetch_policy"),
                Step::action("B", "merge")
                    .with_depends_on(vec![StepId::from("A1"), StepId::from("A2")])
                    .with_io_bindings(vec![
                        StepIoBinding::required("A1.result", "profile"),
                        StepIoBinding::required("A2.result", "policy"),
                    ]),
            ],
        );
        let cut = locate_recovery_cut_step(
            &plan,
            "B",
            "schema validation failed: dependency A2 returned invalid format",
            &[StepId::from("A1"), StepId::from("A2")],
            &HashMap::new(),
        )
        .expect("cut");
        assert_eq!(cut.cut_step_id, "A2");
    }

    #[test]
    fn test_locate_normalization_repair_target_prefers_full_rewrite_without_checkpoint() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("s1", "shell"),
                Step::agent("s2")
                    .with_depends_on(vec![StepId::from("s1")])
                    .with_params(json!({
                        "goal": "inspect and update",
                        "allowed_actions": ["shell", "file_write"],
                        "max_iterations": 5,
                        "output_keys": ["updated_file_path"],
                        "output_rules": {
                            "updated_file_path": {
                                "candidates": [
                                    {
                                        "slot": "fill_result",
                                        "path": "updated_file_path",
                                        "requires": { "action": "file_write" }
                                    }
                                ]
                            }
                        }
                    })),
            ],
        );
        let error = NormalizeError::Validation(ValidationError::InvalidAgentParams(
            "s2".to_string(),
            "explore agent may not own side-effect-sensitive outputs".to_string(),
        ));

        let target = locate_normalization_repair_target(&plan, &error, &[]).expect("target");

        assert_eq!(target.mode_label(), "full_plan");
        assert_eq!(target.invalid_step_id.as_deref(), Some("s2"));
        assert_eq!(target.affected_steps, vec!["s2".to_string()]);
    }

    #[test]
    fn test_locate_normalization_repair_target_patches_only_unfinished_subgraph() {
        let plan = Plan::new(
            "goal",
            vec![
                Step::action("s1", "shell"),
                Step::agent("s2")
                    .with_depends_on(vec![StepId::from("s1")])
                    .with_params(json!({
                        "goal": "inspect and update",
                        "allowed_actions": ["shell", "file_write"],
                        "max_iterations": 5,
                        "output_keys": ["updated_file_path"],
                        "output_rules": {
                            "updated_file_path": {
                                "candidates": [
                                    {
                                        "slot": "fill_result",
                                        "path": "updated_file_path",
                                        "requires": { "action": "file_write" }
                                    }
                                ]
                            }
                        }
                    })),
                Step::action("s3", "notify").with_depends_on(vec![StepId::from("s2")]),
            ],
        );
        let error = NormalizeError::Validation(ValidationError::InvalidAgentParams(
            "s2".to_string(),
            "explore agent may not own side-effect-sensitive outputs".to_string(),
        ));

        let target = locate_normalization_repair_target(&plan, &error, &[StepId::from("s1")])
            .expect("target");

        assert_eq!(target.mode_label(), "subgraph_patch");
        assert_eq!(target.cut_step_id(), Some("s2"));
        assert_eq!(target.invalid_step_id.as_deref(), Some("s2"));
        let affected: std::collections::HashSet<_> = target.affected_steps.into_iter().collect();
        assert_eq!(
            affected,
            std::collections::HashSet::from(["s2".to_string(), "s3".to_string(),])
        );
    }

    #[test]
    fn test_build_normalization_repair_intent_adds_hard_rules() {
        let plan = Plan::new(
            "goal",
            vec![Step::agent("s2").with_params(json!({
                "goal": "inspect and update",
                "allowed_actions": ["shell", "file_write"],
                "max_iterations": 5,
                "output_keys": ["updated_file_path"]
            }))],
        );
        let target = NormalizeRepairTarget {
            mode: NormalizeRepairMode::FullPlan,
            invalid_step_id: Some("s2".to_string()),
            affected_steps: vec!["s2".to_string()],
        };
        let error = NormalizeError::Validation(ValidationError::InvalidAgentParams(
            "s2".to_string(),
            "bad agent".to_string(),
        ));
        let intent = build_normalization_repair_intent(
            &Intent::new("fill the workbook"),
            &plan,
            &error,
            &target,
            &[],
        );

        assert!(intent.content.contains("Normalization repair request."));
        assert!(intent.content.contains("Repair mode: full_plan"));
        assert!(intent
            .content
            .contains("Never return an explore agent that owns outputs"));
        assert!(intent
            .content
            .contains("inspect/collect -> derive leaf -> apply/emit -> verify"));
    }

    #[test]
    fn test_summarize_working_set_applies_semantic_limits() {
        let mut snapshot = HashMap::new();
        snapshot.insert("stdout".to_string(), json!("s".repeat(5_000)));
        snapshot.insert("stderr".to_string(), json!("e".repeat(1_500)));
        snapshot.insert("path".to_string(), json!("p".repeat(500)));
        snapshot.insert("reader.content".to_string(), json!("c".repeat(5_000)));

        let summary = summarize_working_set(&snapshot);

        let stdout_line = summary
            .lines()
            .find(|line| line.starts_with("  stdout: \""))
            .expect("stdout line");
        let stdout_value = stdout_line
            .strip_prefix("  stdout: \"")
            .and_then(|line| line.strip_suffix('"'))
            .expect("stdout quoted value");
        assert_eq!(stdout_value.chars().count(), 4_000);

        let stderr_line = summary
            .lines()
            .find(|line| line.starts_with("  stderr: \""))
            .expect("stderr line");
        let stderr_value = stderr_line
            .strip_prefix("  stderr: \"")
            .and_then(|line| line.strip_suffix('"'))
            .expect("stderr quoted value");
        assert_eq!(stderr_value.chars().count(), 1_000);

        let path_line = summary
            .lines()
            .find(|line| line.starts_with("  path: \""))
            .expect("path line");
        let path_value = path_line
            .strip_prefix("  path: \"")
            .and_then(|line| line.strip_suffix('"'))
            .expect("path quoted value");
        assert_eq!(path_value.chars().count(), 200);

        let content_line = summary
            .lines()
            .find(|line| line.starts_with("  reader.content: \""))
            .expect("content line");
        let content_value = content_line
            .strip_prefix("  reader.content: \"")
            .and_then(|line| line.strip_suffix('"'))
            .expect("content quoted value");
        assert_eq!(content_value.chars().count(), 4_000);
    }

    #[test]
    fn test_summarize_working_set_is_utf8_safe() {
        let mut snapshot = HashMap::new();
        snapshot.insert("note".to_string(), json!("你好".repeat(260)));

        let summary = summarize_working_set(&snapshot);
        let note_line = summary
            .lines()
            .find(|line| line.starts_with("  note: \""))
            .expect("note line");
        let note_value = note_line
            .strip_prefix("  note: \"")
            .and_then(|line| line.strip_suffix('"'))
            .expect("note quoted value");

        assert_eq!(note_value.chars().count(), 200);
        assert!(note_value.ends_with("..."));
    }

    #[test]
    fn test_summarize_working_set_prioritizes_stdout_content_and_stderr() {
        let mut snapshot = HashMap::new();
        snapshot.insert("stdout".to_string(), json!("s".repeat(10_000)));
        snapshot.insert("stderr".to_string(), json!("e".repeat(3_000)));
        snapshot.insert("reader.content".to_string(), json!("c".repeat(10_000)));
        for idx in 0..120 {
            snapshot.insert(format!("low_{idx:03}"), json!("l".repeat(400)));
        }

        let summary = summarize_working_set(&snapshot);

        assert!(summary.contains("  stdout: \""));
        assert!(summary.contains("  reader.content: \""));
        assert!(summary.contains("  stderr: \""));
        assert!(!summary.contains("  low_119: \""));
    }

    #[test]
    fn test_resolve_plan_response_template_appends_summary_when_on_complete_is_static() {
        let mut plan = Plan::new("goal", vec![]);
        plan.on_complete = Some("Successfully found and summarized the Excel file.".to_string());
        let mut ws = HashMap::new();
        ws.insert("summary".to_string(), json!("Sheet 2025-Q2: 28 rows"));

        let resolved =
            resolve_plan_response_template(&plan, &ExecutionResult::Completed, &ws).expect("msg");

        assert_eq!(
            resolved,
            "Successfully found and summarized the Excel file.\n\nSheet 2025-Q2: 28 rows"
        );
    }

    #[test]
    fn test_resolve_plan_response_template_does_not_duplicate_summary_when_placeholder_exists() {
        let mut plan = Plan::new("goal", vec![]);
        plan.on_complete = Some("Done:\n{{summary}}".to_string());
        let mut ws = HashMap::new();
        ws.insert("summary".to_string(), json!("Sheet 2025-Q2: 28 rows"));

        let resolved =
            resolve_plan_response_template(&plan, &ExecutionResult::Completed, &ws).expect("msg");

        assert_eq!(resolved, "Done:\nSheet 2025-Q2: 28 rows");
    }

    #[test]
    fn test_resolve_plan_response_template_uses_scoped_summary_fallback() {
        let mut plan = Plan::new("goal", vec![]);
        plan.on_complete = Some("Done".to_string());
        let mut ws = HashMap::new();
        ws.insert(
            "summarize_excel_agent.summary".to_string(),
            json!("Sheet 2025-Q2: 28 rows"),
        );

        let resolved =
            resolve_plan_response_template(&plan, &ExecutionResult::Completed, &ws).expect("msg");

        assert_eq!(resolved, "Done\n\nSheet 2025-Q2: 28 rows");
    }
}
