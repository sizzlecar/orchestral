#[cfg(test)]
use super::*;
#[cfg(test)]
use orchestral_core::types::Step;

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) enum NormalizeRepairMode {
    FullPlan,
    SubgraphPatch { cut_step_id: String },
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct NormalizeRepairTarget {
    pub(super) mode: NormalizeRepairMode,
    pub(super) invalid_step_id: Option<String>,
    pub(super) affected_steps: Vec<String>,
}

#[cfg(test)]
impl NormalizeRepairTarget {
    pub(super) fn mode_label(&self) -> &'static str {
        match self.mode {
            NormalizeRepairMode::FullPlan => "full_plan",
            NormalizeRepairMode::SubgraphPatch { .. } => "subgraph_patch",
        }
    }

    pub(super) fn cut_step_id(&self) -> Option<&str> {
        match &self.mode {
            NormalizeRepairMode::FullPlan => None,
            NormalizeRepairMode::SubgraphPatch { cut_step_id } => Some(cut_step_id.as_str()),
        }
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) struct RecoveryCutStep {
    pub(super) cut_step_id: String,
    pub(super) reason: String,
    pub(super) confidence: f32,
}

#[cfg(test)]
pub(super) fn affected_subgraph_steps(
    plan: &orchestral_core::types::Plan,
    cut_step_id: &str,
) -> Vec<String> {
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

#[cfg(test)]
pub(super) fn locate_recovery_cut_step(
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn step_id_from_key(key: &str) -> Option<String> {
    let (step_id, _) = key.split_once('.')?;
    if step_id.is_empty() {
        None
    } else {
        Some(step_id.to_string())
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) fn build_recovery_intent(
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

#[cfg(test)]
pub(super) fn build_normalization_repair_intent(
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

#[cfg(test)]
pub(super) fn locate_normalization_repair_target(
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

#[cfg(test)]
fn normalize_error_step_id(normalize_error: &NormalizeError) -> Option<&str> {
    match normalize_error {
        NormalizeError::Validation(
            orchestral_core::normalizer::ValidationError::MissingDependency(step_id, _),
        )
        | NormalizeError::Validation(
            orchestral_core::normalizer::ValidationError::InvalidIoBinding(step_id, _),
        )
        | NormalizeError::Validation(
            orchestral_core::normalizer::ValidationError::IoBindingMissingDependency(step_id, _),
        )
        | NormalizeError::Validation(
            orchestral_core::normalizer::ValidationError::InvalidAgentParams(step_id, _),
        )
        | NormalizeError::Validation(
            orchestral_core::normalizer::ValidationError::CycleDetected(step_id),
        )
        | NormalizeError::Validation(
            orchestral_core::normalizer::ValidationError::DuplicateStepId(step_id),
        ) => Some(step_id.as_str()),
        _ => None,
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(super) fn patch_plan_with_recovery(
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

#[cfg(test)]
pub(super) fn summarize_selected_plan_steps(
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
