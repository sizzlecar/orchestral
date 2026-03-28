use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

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

#[cfg(test)]
mod tests {
    use super::forced_action_failure_reason;

    #[test]
    fn test_forced_action_failure_once_consumes_single_hit() {
        std::env::set_var("ORCHESTRAL_TEST_FAIL_ACTION_ONCE", "file_read");
        let first = forced_action_failure_reason("file_read");
        let second = forced_action_failure_reason("file_read");
        std::env::remove_var("ORCHESTRAL_TEST_FAIL_ACTION_ONCE");

        assert!(first.is_some());
        assert!(second.is_none());
    }
}
