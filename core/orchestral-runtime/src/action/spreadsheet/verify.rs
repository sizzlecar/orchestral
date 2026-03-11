use std::path::Path;
use std::process::Command;

use serde_json::{json, Value};

use super::inspect::{cell_display_text, inspect_workbook};
use super::model::load_xlsx_model;
use super::support::{
    normalize_path, parse_cell_ref, resolve_project_path, resolve_validator_python,
};

pub(super) fn verify_patch(
    path: &Path,
    patch_spec: Option<&Value>,
    validator_path: Option<&str>,
) -> Result<Value, String> {
    let workbook_path = normalize_path(path)?;
    let spec_evidence = match patch_spec {
        Some(spec) => Some(verify_patch_against_spec(&workbook_path, spec)?),
        None => None,
    };
    if let Some(evidence) = &spec_evidence {
        let mismatch_count = evidence
            .get("mismatch_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if mismatch_count > 0 {
            return Ok(json!({
                "status": "failed",
                "reason": format!("{} spreadsheet fills do not match patch_spec", mismatch_count),
                "evidence": {
                    "spec_verification": evidence,
                    "path": workbook_path,
                },
            }));
        }
    }

    let inspection = inspect_workbook(&workbook_path)?;
    let patchable = inspection
        .get("selected_region")
        .and_then(|v| v.get("patchable_cells"))
        .and_then(Value::as_array)
        .map(|v| v.len())
        .unwrap_or(0);
    if let Some(validator_path) = validator_path.filter(|value| !value.trim().is_empty()) {
        let validator_decision = run_external_validator(&workbook_path, validator_path)?;
        return Ok(attach_verify_evidence(
            validator_decision,
            spec_evidence,
            Some(inspection),
        ));
    }
    if patchable == 0 {
        Ok(attach_verify_evidence(
            json!({
                "status": "passed",
                "reason": "selected spreadsheet region no longer has patchable cells",
                "evidence": inspection,
            }),
            spec_evidence,
            None,
        ))
    } else {
        Ok(attach_verify_evidence(
            json!({
                "status": "failed",
                "reason": format!("selected spreadsheet region still has {} patchable cells", patchable),
                "evidence": inspection,
            }),
            spec_evidence,
            None,
        ))
    }
}

fn run_external_validator(workbook_path: &Path, validator_path: &str) -> Result<Value, String> {
    let validator_script = resolve_project_path(Path::new(validator_path))?;
    if !validator_script.exists() {
        return Err(format!(
            "validator script not found: {}",
            validator_script.display()
        ));
    }

    let python = resolve_validator_python()?;
    let output = Command::new(&python)
        .arg(&validator_script)
        .arg(workbook_path)
        .output()
        .map_err(|err| format!("run validator failed: {}", err))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stdout.is_empty() {
        return Err(format!(
            "validator produced empty stdout (status={} stderr={})",
            output
                .status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            stderr
        ));
    }

    let parsed: Value = serde_json::from_str(&stdout)
        .map_err(|err| format!("validator returned invalid json: {}", err))?;
    let ok = parsed
        .get("ok")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| output.status.success());
    let failures = parsed
        .get("failures")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let reason = if ok {
        "external spreadsheet validator passed".to_string()
    } else if !failures.is_empty() {
        failures.join("; ")
    } else if !stderr.is_empty() {
        stderr.clone()
    } else {
        format!(
            "validator exited with status {}",
            output
                .status
                .code()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "signal".to_string())
        )
    };

    Ok(json!({
        "status": if ok { "passed" } else { "failed" },
        "reason": reason,
        "evidence": {
            "validator_result": parsed,
            "validator_stderr": stderr,
            "validator_status": output.status.code(),
            "validator_script": validator_script,
            "python": python,
            "path": workbook_path,
        }
    }))
}

fn verify_patch_against_spec(path: &Path, patch_spec: &Value) -> Result<Value, String> {
    let workbook = load_xlsx_model(path)?;
    let fills = patch_spec
        .get("fills")
        .and_then(Value::as_array)
        .ok_or_else(|| "patch_spec.fills must be an array".to_string())?;
    let mut mismatches = Vec::new();

    for fill in fills {
        let Some(cell_ref) = fill.get("cell").and_then(Value::as_str) else {
            continue;
        };
        let expected = fill
            .get("value")
            .ok_or_else(|| format!("fill {} missing value", cell_ref))?;
        let (row, col) = parse_cell_ref(cell_ref)?;
        let actual_text = workbook
            .rows
            .get(&row)
            .and_then(|row_data| row_data.cells.get(&col))
            .map(cell_display_text)
            .unwrap_or_default();
        if !cell_value_matches_expected(expected, &actual_text) {
            mismatches.push(json!({
                "cell": cell_ref,
                "expected": expected,
                "actual": actual_text,
            }));
        }
    }

    Ok(json!({
        "checked_fill_count": fills.len(),
        "mismatch_count": mismatches.len(),
        "mismatches": mismatches,
    }))
}

pub(super) fn cell_value_matches_expected(expected: &Value, actual_text: &str) -> bool {
    let actual = actual_text.trim();
    match expected {
        Value::Null => actual.is_empty(),
        Value::Bool(boolean) => {
            let normalized = actual.to_ascii_lowercase();
            if *boolean {
                normalized == "1" || normalized == "true"
            } else {
                normalized == "0" || normalized == "false"
            }
        }
        Value::Number(number) => number
            .as_f64()
            .and_then(|expected_number| {
                actual
                    .parse::<f64>()
                    .ok()
                    .map(|actual_number| (actual_number - expected_number).abs() < 1e-9)
            })
            .unwrap_or(false),
        Value::String(text) => actual == text.trim(),
        other => actual == other.to_string().trim_matches('"'),
    }
}

fn attach_verify_evidence(
    mut decision: Value,
    spec_evidence: Option<Value>,
    inspection: Option<Value>,
) -> Value {
    let Some(decision_object) = decision.as_object_mut() else {
        return decision;
    };
    let evidence = decision_object
        .entry("evidence".to_string())
        .or_insert_with(|| json!({}));
    if !evidence.is_object() {
        *evidence = json!({});
    }
    let evidence_object = evidence
        .as_object_mut()
        .expect("evidence should be an object after initialization");
    if let Some(spec_evidence) = spec_evidence {
        evidence_object.insert("spec_verification".to_string(), spec_evidence);
    }
    if let Some(inspection) = inspection {
        evidence_object.insert("inspection".to_string(), inspection);
    }
    decision
}
