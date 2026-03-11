mod actions;
mod apply;
mod assess;
mod inspect;
mod locate;
mod model;
mod support;
mod verify;

pub use self::actions::build_spreadsheet_action;

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::assess::assess_readiness;
    use super::verify::cell_value_matches_expected;

    #[test]
    fn test_assess_readiness_accepts_enveloped_candidate_cells() {
        let inspection = json!({
            "selected_region": {
                "patchable_cells": [
                    { "cell": "E5" },
                    { "cell": "F5" }
                ]
            }
        });
        let patch_candidates = json!({
            "candidates": {
                "cells": [
                    {
                        "cell": "E5",
                        "proposed_action": "fill",
                        "proposed_value": "按计划完成核心需求开发"
                    },
                    {
                        "cell": "F5",
                        "proposed_action": "fill",
                        "proposed_value": "95"
                    }
                ]
            },
            "unknowns": [],
            "assumptions": ["generic spreadsheet wording is acceptable"]
        });

        let (continuation, summary) =
            assess_readiness(&inspection, &patch_candidates, "strict").expect("assess");
        assert_eq!(
            continuation["status"],
            Value::String("commit_ready".to_string())
        );
        assert!(summary.contains("ready for strict commit"));
    }

    #[test]
    fn test_cell_value_matches_expected_handles_numeric_and_string_values() {
        assert!(cell_value_matches_expected(&json!(95), "95"));
        assert!(cell_value_matches_expected(&json!("完成"), "完成"));
        assert!(cell_value_matches_expected(&Value::Null, ""));
        assert!(!cell_value_matches_expected(&json!(96), "95"));
        assert!(!cell_value_matches_expected(&json!("完成"), "未完成"));
    }
}
