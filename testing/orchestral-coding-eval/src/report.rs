use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::process::ProcessResult;
use crate::verify::Verification;

#[derive(Debug, Serialize)]
pub struct Attempt {
    pub task: String,
    pub category: String,
    pub repetition: u32,
    pub status: String,
    pub turns: Vec<ProcessResult>,
    pub violations: Vec<String>,
    pub verification: Option<Verification>,
    pub error: Option<String>,
    pub model_usage: Option<Value>,
    pub cost: Option<f64>,
    pub human_corrections: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub mode: String,
    pub base_commit: String,
    pub suite_digest: String,
    pub agent: Option<Value>,
    pub rustc: String,
    pub execution: Value,
    pub planned_attempts: usize,
    pub attempts: Vec<Attempt>,
    pub validations: Vec<Value>,
}

impl Report {
    pub fn save(&self, directory: &Path) -> Result<()> {
        write_json(&directory.join("report.json"), self)?;
        let passed = self
            .attempts
            .iter()
            .filter(|attempt| attempt.status == "passed")
            .count();
        let counts =
            self.attempts
                .iter()
                .fold(std::collections::BTreeMap::new(), |mut counts, attempt| {
                    *counts.entry(attempt.status.as_str()).or_insert(0) += 1;
                    counts
                });
        let mut elapsed = self
            .attempts
            .iter()
            .filter(|attempt| !attempt.turns.is_empty())
            .map(|attempt| {
                attempt
                    .turns
                    .iter()
                    .map(|turn| turn.elapsed_ms)
                    .sum::<u128>()
            })
            .collect::<Vec<_>>();
        elapsed.sort_unstable();
        let p95 = elapsed
            .get((elapsed.len() * 95).div_ceil(100).saturating_sub(1))
            .copied();
        let mut per_task = std::collections::BTreeMap::new();
        for attempt in &self.attempts {
            let entry = per_task.entry(&attempt.task).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += usize::from(attempt.status == "passed");
        }
        write_json(
            &directory.join("summary.json"),
            &serde_json::json!({
                "mode": self.mode,
                "planned_attempts": self.planned_attempts,
                "completed_attempts": self.attempts.len(),
                "not_run": self.planned_attempts.saturating_sub(self.attempts.len()),
                "status_counts": counts,
                "repairs_verified": self.attempts.iter().filter(|attempt| attempt.verification.as_ref().is_some_and(|v| v.passed)).count(),
                "agent_wall_time_p95_ms": p95,
                "per_task": per_task.into_iter().map(|(task, (attempts, passed))| serde_json::json!({"task": task, "attempts": attempts, "passed": passed})).collect::<Vec<_>>(),
                "pass_rate": if self.attempts.is_empty() { None } else { Some(passed as f64 / self.attempts.len() as f64) },
                "validated_tasks": self.validations.iter().filter(|v| v["valid"] == true).count(),
                "note": "Controlled seeded regressions in one repository; validation runs are not agent task successes. Cost and human corrections are unknown unless measured."
            }),
        )
    }
}

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

/// Count reported usage once per committed model request. Multiple tools from
/// one request may repeat usage; Run delivery totals must not be added again.
pub fn usage(directory: &Path) -> Option<Value> {
    let mut input = 0_u64;
    let mut output = 0_u64;
    let mut observations = 0;
    let mut unknown_input = false;
    let mut unknown_output = false;
    let mut seen = std::collections::BTreeSet::new();
    for entry in fs::read_dir(directory).ok()?.flatten() {
        if !entry.file_name().to_string_lossy().starts_with("session-") {
            continue;
        }
        let document: Value = serde_json::from_slice(&fs::read(entry.path()).ok()?).ok()?;
        for record in document.as_array()? {
            let payload = record.get("payload")?;
            if !matches!(
                payload["type"].as_str(),
                Some("tool_exchange_committed" | "run_output_committed")
            ) {
                continue;
            }
            let key = (
                record["session_id"].to_string(),
                record["run_id"].to_string(),
                payload["request_id"].to_string(),
            );
            if !seen.insert(key) {
                continue;
            }
            observations += 1;
            match payload
                .pointer("/usage/input_tokens")
                .and_then(Value::as_u64)
            {
                Some(tokens) => input += tokens,
                None => unknown_input = true,
            }
            match payload
                .pointer("/usage/output_tokens")
                .and_then(Value::as_u64)
            {
                Some(tokens) => output += tokens,
                None => unknown_output = true,
            }
        }
    }
    (observations > 0).then(|| {
        serde_json::json!({
            "observations": observations,
            "input_tokens": (!unknown_input).then_some(input),
            "output_tokens": (!unknown_output).then_some(output),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn model_usage_deduplicates_tool_batches_and_preserves_unknown_usage() {
        let root = std::env::temp_dir().join(format!("coding-eval-usage-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let record = |request: &str, usage: Value| json!({"session_id":"s", "run_id":"r", "payload":{"type":"tool_exchange_committed", "request_id":request, "usage":usage}});
        let reported = json!({"input_tokens": 10, "output_tokens": 3});
        let path = root.join("session-example.json");
        fs::write(
            &path,
            serde_json::to_vec(&vec![
                record("a", reported.clone()),
                record("a", Value::Null),
                record("b", reported),
            ])
            .unwrap(),
        )
        .unwrap();
        let result = usage(&root).unwrap();
        assert_eq!(result["input_tokens"], 20);
        assert_eq!(result["output_tokens"], 6);
        fs::write(
            &path,
            serde_json::to_vec(&vec![record("a", Value::Null)]).unwrap(),
        )
        .unwrap();
        assert!(usage(&root).unwrap()["input_tokens"].is_null());
        fs::remove_dir_all(root).unwrap();
    }
}
