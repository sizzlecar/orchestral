//! Browser-independent presentation rules for live Agent activity.

pub fn operation_is_failure(state: &str) -> bool {
    matches!(state, "failed" | "error" | "rejected")
}

pub fn operation_is_running(state: &str) -> bool {
    matches!(state, "running" | "pending" | "received")
}

/// Running work is useful to expose live. Failures retain their visible
/// summary but keep verbose output collapsed until the user asks for it.
pub fn operation_details_expanded_by_default(state: &str) -> bool {
    operation_is_running(state)
}

/// Returns a forced group state when live status changes. Completed groups
/// keep the user's current choice; running groups open and failed groups close.
pub fn activity_group_expansion_for_status(failures: usize, running: usize) -> Option<bool> {
    if running > 0 {
        Some(true)
    } else if failures > 0 {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_operations_and_groups_are_collapsed_by_default() {
        assert!(!operation_details_expanded_by_default("failed"));
        assert!(!operation_details_expanded_by_default("error"));
        assert!(!operation_details_expanded_by_default("rejected"));
        assert_eq!(activity_group_expansion_for_status(1, 0), Some(false));
        assert_eq!(activity_group_expansion_for_status(0, 1), Some(true));
        assert_eq!(activity_group_expansion_for_status(0, 0), None);
    }
}
