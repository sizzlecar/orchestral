use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use orchestral_core::types::{VerifyDecision, VerifyStatus};
use serde_json::json;

fn once_failures() -> &'static Mutex<HashSet<String>> {
    static ONCE_FAILURES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    ONCE_FAILURES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn matches_csv_env(key: &str, action_name: &str) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .any(|item| item == action_name)
        })
        .unwrap_or(false)
}

pub(crate) fn forced_action_failure_reason(action_name: &str) -> Option<String> {
    if matches_csv_env("ORCHESTRAL_TEST_FAIL_ACTION_ALWAYS", action_name) {
        return Some(format!(
            "forced action failure for testing: {}",
            action_name
        ));
    }

    if !matches_csv_env("ORCHESTRAL_TEST_FAIL_ACTION_ONCE", action_name) {
        return None;
    }

    let mut fired = once_failures().lock().ok()?;
    if fired.insert(format!("action:{action_name}")) {
        Some(format!(
            "forced action failure-once for testing: {}",
            action_name
        ))
    } else {
        None
    }
}

pub(crate) fn forced_verify_failure(action_name: &str) -> Option<VerifyDecision> {
    if matches_csv_env("ORCHESTRAL_TEST_FORCE_VERIFY_FAIL_ALWAYS", action_name) {
        return Some(build_forced_verify_failure(action_name, false));
    }

    if !matches_csv_env("ORCHESTRAL_TEST_FORCE_VERIFY_FAIL_ONCE", action_name) {
        return None;
    }

    let mut fired = once_failures().lock().ok()?;
    if fired.insert(format!("verify:{action_name}")) {
        Some(build_forced_verify_failure(action_name, true))
    } else {
        None
    }
}

fn build_forced_verify_failure(action_name: &str, once: bool) -> VerifyDecision {
    VerifyDecision {
        status: VerifyStatus::Failed,
        reason: if once {
            format!("forced verify failure-once for testing: {}", action_name)
        } else {
            format!("forced verify failure for testing: {}", action_name)
        },
        evidence: json!({
            "action": action_name,
            "hook": if once {
                "ORCHESTRAL_TEST_FORCE_VERIFY_FAIL_ONCE"
            } else {
                "ORCHESTRAL_TEST_FORCE_VERIFY_FAIL_ALWAYS"
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{forced_action_failure_reason, forced_verify_failure};

    #[test]
    fn test_forced_action_failure_once_consumes_single_hit() {
        std::env::set_var("ORCHESTRAL_TEST_FAIL_ACTION_ONCE", "file_read");
        let first = forced_action_failure_reason("file_read");
        let second = forced_action_failure_reason("file_read");
        std::env::remove_var("ORCHESTRAL_TEST_FAIL_ACTION_ONCE");

        assert!(first.is_some());
        assert!(second.is_none());
    }

    #[test]
    fn test_forced_verify_failure_always_matches_action() {
        std::env::set_var("ORCHESTRAL_TEST_FORCE_VERIFY_FAIL_ALWAYS", "reactor_document_verify_patch");
        let decision = forced_verify_failure("reactor_document_verify_patch");
        std::env::remove_var("ORCHESTRAL_TEST_FORCE_VERIFY_FAIL_ALWAYS");

        assert!(decision.is_some());
        let decision = decision.expect("decision");
        assert_eq!(decision.reason, "forced verify failure for testing: reactor_document_verify_patch");
    }
}
