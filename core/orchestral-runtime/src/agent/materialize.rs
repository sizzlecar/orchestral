use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::types::{
    AgentCaptureMode, AgentEvidenceRecord, AgentEvidenceStore, AgentMode, AgentOutputCandidate,
    AgentOutputRule, AgentPathSegment, AgentStepParams, MaterializeError,
};

pub(super) fn derive_evidence_value(
    exports: &HashMap<String, Value>,
    capture: Option<&AgentCaptureMode>,
) -> Result<Value, String> {
    match capture {
        Some(AgentCaptureMode::JsonStdout) => {
            let stdout = exports
                .get("stdout")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "capture json_stdout requires string stdout".to_string())?;
            serde_json::from_str::<Value>(stdout)
                .map_err(|e| format!("capture json_stdout invalid JSON: {}", e))
        }
        None => Ok(exports_to_value(exports)),
    }
}

pub(super) fn validate_agent_action_success(
    params: &AgentStepParams,
    evidence: &AgentEvidenceStore,
    action_name: &str,
    exports: &HashMap<String, Value>,
    captured_value: &Value,
    save_as: Option<&str>,
) -> Result<(), MaterializeError> {
    if params.mode != AgentMode::Leaf || action_name != "json_stdout" {
        return Ok(());
    }
    let mut probe = evidence.clone();
    probe.record_success(
        action_name,
        exports.clone(),
        captured_value.clone(),
        save_as,
    );
    materialize_final_exports(params, &probe).map(|_| ())
}

pub(super) fn exports_to_value(exports: &HashMap<String, Value>) -> Value {
    Value::Object(
        exports
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<serde_json::Map<_, _>>(),
    )
}

pub(super) fn parse_json_export_field(
    exports: &HashMap<String, Value>,
    field: &str,
) -> Option<Value> {
    let raw = exports.get(field)?.as_str()?;
    serde_json::from_str(raw).ok()
}

pub(super) fn materialize_final_exports(
    params: &AgentStepParams,
    evidence: &AgentEvidenceStore,
) -> Result<HashMap<String, Value>, MaterializeError> {
    let mut out = HashMap::with_capacity(params.output_keys.len());
    for key in &params.output_keys {
        let value = materialize_output_key(key, params, evidence, &out)?;
        out.insert(key.clone(), value);
    }
    Ok(out)
}

fn materialize_output_key(
    key: &str,
    params: &AgentStepParams,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Result<Value, MaterializeError> {
    if let Some(rule) = params.output_rules.get(key) {
        if let Some(value) =
            resolve_output_from_rule(key, rule, params, evidence, resolved_outputs)?
        {
            return Ok(value);
        }
        let required_action = rule_required_action(rule);
        if let Some(value) =
            resolve_slot_with_requirement(key, key, evidence, required_action.as_deref())?
        {
            return Ok(value);
        }
        if !rule.fallback_aliases.is_empty() {
            let aliases = output_rule_fallback_aliases(key, rule);
            if let Some(value) =
                find_candidate_in_evidence(evidence, &aliases, required_action.as_deref())
            {
                return Ok(value);
            }
            let code = if required_action.is_some() {
                "missing_requires"
            } else {
                "rule_unresolved"
            };
            return Err(MaterializeError::new(
                key,
                code,
                format!(
                    "fallback aliases {:?} produced no value{}",
                    aliases,
                    required_action_suffix(required_action.as_deref())
                ),
            ));
        }
        let code = if required_action.is_some() {
            "missing_requires"
        } else {
            "rule_unresolved"
        };
        return Err(MaterializeError::new(
            key,
            code,
            format!(
                "output_rules could not materialize value{}",
                required_action_suffix(required_action.as_deref())
            ),
        ));
    }

    if let Some(value) = resolve_slot_with_requirement(key, key, evidence, None)? {
        return Ok(value);
    }

    let aliases = output_key_aliases(key, None);
    if let Some(value) = find_candidate_in_evidence(evidence, &aliases, None) {
        return Ok(value);
    }
    Err(MaterializeError::new(
        key,
        "missing_evidence",
        format!("no evidence found for aliases {:?}", aliases),
    ))
}

fn resolve_slot_with_requirement(
    output_key: &str,
    slot: &str,
    evidence: &AgentEvidenceStore,
    required_action: Option<&str>,
) -> Result<Option<Value>, MaterializeError> {
    let Some(value) = evidence.slots.get(slot) else {
        return Ok(None);
    };
    if let Some(required_action) = required_action {
        let actual = evidence.slot_actions.get(slot).map(String::as_str);
        if actual != Some(required_action) {
            return Err(MaterializeError::new(
                output_key,
                "slot_provenance_mismatch",
                format!(
                    "slot '{}' was produced by {:?}, expected '{}'{}",
                    slot,
                    actual,
                    required_action,
                    required_action_suffix(Some(required_action))
                ),
            ));
        }
    }
    Ok(Some(value.clone()))
}

fn required_action_suffix(required_action: Option<&str>) -> String {
    match required_action {
        Some(action) => format!(" (requires.action='{}')", action),
        None => String::new(),
    }
}

fn find_candidate_in_record(
    record: &AgentEvidenceRecord,
    aliases: &[String],
    required_action: Option<&str>,
) -> Option<Value> {
    if let Some(required_action) = required_action {
        if record.action_name != required_action {
            return None;
        }
    }
    for field in ["stdout", "body", "result"] {
        if let Some(parsed) = parse_json_field_from_value(&record.value, field) {
            if let Some(value) = find_candidate_in_value_or_wrapped(&parsed, aliases) {
                return Some(value);
            }
        }
    }
    if let Some(value) = find_candidate_in_value_or_wrapped(&record.value, aliases) {
        return Some(value);
    }
    for alias in aliases {
        if let Some(value) = record.raw_exports.get(alias.as_str()) {
            return Some(value.clone());
        }
    }
    for field in ["stdout", "body", "result"] {
        if let Some(parsed) = parse_json_export_field(&record.raw_exports, field) {
            if let Some(value) = find_candidate_in_value_or_wrapped(&parsed, aliases) {
                return Some(value);
            }
        }
    }
    None
}

fn find_candidate_in_evidence(
    evidence: &AgentEvidenceStore,
    aliases: &[String],
    required_action: Option<&str>,
) -> Option<Value> {
    for record in evidence.records.iter().rev() {
        if let Some(value) = find_candidate_in_record(record, aliases, required_action) {
            return Some(value);
        }
    }

    if let Some(last_raw) = evidence.slot_value_with_requirement("last_raw", required_action) {
        if let Some(value) = find_candidate_in_value_or_wrapped(&last_raw, aliases) {
            return Some(value);
        }
    }
    if let Some(last_stdout_json) =
        evidence.slot_value_with_requirement("last_stdout_json", required_action)
    {
        if let Some(value) = find_candidate_in_value_or_wrapped(&last_stdout_json, aliases) {
            return Some(value);
        }
    }
    if let Some(last_body_json) =
        evidence.slot_value_with_requirement("last_body_json", required_action)
    {
        if let Some(value) = find_candidate_in_value_or_wrapped(&last_body_json, aliases) {
            return Some(value);
        }
    }

    None
}

fn resolve_output_from_rule(
    key: &str,
    rule: &AgentOutputRule,
    params: &AgentStepParams,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Result<Option<Value>, MaterializeError> {
    let mut first_error: Option<MaterializeError> = None;
    for candidate in &rule.candidates {
        let required_action = candidate_required_action(rule, candidate, params);
        match resolve_output_candidate(key, candidate, evidence, resolved_outputs, required_action)
        {
            Ok(Some(value)) => return Ok(Some(value)),
            Ok(None) => {}
            Err(err) => {
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }
    }
    if let Some(template) = &rule.template {
        if let Some(rendered) = render_output_template(template, evidence, resolved_outputs) {
            return Ok(Some(Value::String(rendered)));
        }
    }
    if let Some(err) = first_error {
        return Err(err);
    }
    Ok(None)
}

fn resolve_output_candidate(
    output_key: &str,
    candidate: &AgentOutputCandidate,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
    required_action: Option<&str>,
) -> Result<Option<Value>, MaterializeError> {
    let root = if candidate.slot == "outputs" {
        if required_action.is_some() {
            return Err(MaterializeError::new(
                output_key,
                "missing_requires",
                format!(
                    "slot 'outputs' cannot satisfy{}",
                    required_action_suffix(required_action)
                ),
            ));
        }
        Some(Value::Object(
            resolved_outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ))
    } else {
        resolve_slot_with_requirement(output_key, &candidate.slot, evidence, required_action)?
    };
    let Some(root) = root else {
        return Ok(None);
    };
    if candidate.path.is_empty() {
        Ok(Some(root))
    } else {
        match resolve_path_value(&root, &candidate.path) {
            Some(value) => Ok(Some(value)),
            None => Err(MaterializeError::new(
                output_key,
                "rule_unresolved",
                format!(
                    "slot '{}' does not contain path '{}'{}",
                    candidate.slot,
                    render_path_segments(&candidate.path),
                    required_action_suffix(required_action)
                ),
            )),
        }
    }
}

fn rule_required_action(rule: &AgentOutputRule) -> Option<String> {
    rule.required_action
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn candidate_required_action<'a>(
    rule: &'a AgentOutputRule,
    candidate: &'a AgentOutputCandidate,
    params: &'a AgentStepParams,
) -> Option<&'a str> {
    candidate
        .required_action
        .as_deref()
        .or(rule.required_action.as_deref())
        .or_else(|| {
            if candidate.slot != "outputs"
                && params.allowed_actions.contains(candidate.slot.as_str())
            {
                Some(candidate.slot.as_str())
            } else {
                None
            }
        })
}

fn resolve_path_value(value: &Value, path: &[AgentPathSegment]) -> Option<Value> {
    if path.is_empty() {
        return Some(value.clone());
    }
    let mut current = value;
    for segment in path {
        current = match segment {
            AgentPathSegment::Key(key) => current.get(key)?,
            AgentPathSegment::Index(idx) => current.as_array()?.get(*idx)?,
        };
    }
    Some(current.clone())
}

fn find_candidate_in_value(value: &Value, aliases: &[String]) -> Option<Value> {
    let obj = value.as_object()?;
    for alias in aliases {
        if let Some(found) = obj.get(alias.as_str()) {
            return Some(found.clone());
        }
    }
    None
}

fn find_candidate_in_value_or_wrapped(value: &Value, aliases: &[String]) -> Option<Value> {
    if let Some(value) = find_candidate_in_value(value, aliases) {
        return Some(value);
    }
    let obj = value.as_object()?;
    for wrapper in ["data", "result", "payload", "output", "response"] {
        if let Some(nested) = obj.get(wrapper) {
            if let Some(value) = find_candidate_in_value(nested, aliases) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_json_field_from_value(value: &Value, field: &str) -> Option<Value> {
    let obj = value.as_object()?;
    let raw = obj.get(field)?.as_str()?;
    serde_json::from_str(raw).ok()
}

fn output_key_aliases(key: &str, rule: Option<&AgentOutputRule>) -> Vec<String> {
    let mut aliases = vec![key.to_string()];
    if key.ends_with("_path") {
        aliases.extend(["path", "output_path", "file_path"].map(str::to_string));
    }
    if key == "summary" {
        aliases.extend(["result", "message", "stdout"].map(str::to_string));
    }
    if key == "content" {
        aliases.extend(["body", "stdout", "result"].map(str::to_string));
    }
    if let Some(rule) = rule {
        aliases.extend(rule.fallback_aliases.iter().map(|alias| alias.to_string()));
    }
    dedupe_aliases(aliases)
}

fn output_rule_fallback_aliases(key: &str, rule: &AgentOutputRule) -> Vec<String> {
    let mut aliases = vec![key.to_string()];
    aliases.extend(rule.fallback_aliases.iter().map(|alias| alias.to_string()));
    dedupe_aliases(aliases)
}

fn dedupe_aliases(aliases: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::with_capacity(aliases.len());
    let mut seen = HashSet::with_capacity(aliases.len());
    for alias in aliases {
        if seen.insert(alias.clone()) {
            deduped.push(alias);
        }
    }
    deduped
}

pub(super) fn parse_path_segments(raw: &str) -> Option<Vec<AgentPathSegment>> {
    raw.split('.')
        .map(str::trim)
        .map(|token| {
            if token.is_empty() {
                return None;
            }
            if let Ok(index) = token.parse::<usize>() {
                Some(AgentPathSegment::Index(index))
            } else {
                Some(AgentPathSegment::Key(token.to_string()))
            }
        })
        .collect::<Option<Vec<_>>>()
}

pub(super) fn render_path_segments(path: &[AgentPathSegment]) -> String {
    path.iter()
        .map(|segment| match segment {
            AgentPathSegment::Key(key) => key.clone(),
            AgentPathSegment::Index(index) => index.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn render_output_template(
    template: &str,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Option<String> {
    let mut out = String::new();
    let mut cursor = 0usize;
    let mut unresolved = false;

    while let Some(start_rel) = template[cursor..].find("{{") {
        let start = cursor + start_rel;
        out.push_str(&template[cursor..start]);
        let open_end = start + 2;
        let Some(end_rel) = template[open_end..].find("}}") else {
            unresolved = true;
            break;
        };
        let end = open_end + end_rel;
        let token = template[open_end..end].trim();
        if let Some(value) = resolve_template_token(token, evidence, resolved_outputs) {
            out.push_str(&template_value_to_text(&value));
        } else {
            unresolved = true;
        }
        cursor = end + 2;
    }
    out.push_str(&template[cursor..]);

    if unresolved {
        None
    } else {
        Some(out)
    }
}

fn resolve_template_token(
    token: &str,
    evidence: &AgentEvidenceStore,
    resolved_outputs: &HashMap<String, Value>,
) -> Option<Value> {
    if let Some(raw) = token.strip_prefix("slot:") {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        let mut parts = raw.splitn(2, '.');
        let slot = parts.next()?.trim();
        if slot.is_empty() {
            return None;
        }
        let root = evidence.slots.get(slot)?;
        if let Some(path_text) = parts.next() {
            let path = parse_path_segments(path_text.trim())?;
            return resolve_path_value(root, &path);
        }
        return Some(root.clone());
    }

    if let Some(value) = resolved_outputs.get(token) {
        return Some(value.clone());
    }
    evidence.slots.get(token).cloned()
}

fn template_value_to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}
