mod actions;
mod apply;
mod assess;
mod derive;
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
    use super::derive::derive_spreadsheet_patch_candidates;
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
        assert_eq!(
            continuation["fills"].as_array().map(|items| items.len()),
            Some(2)
        );
        assert_eq!(
            continuation["patch_spec"]["fills"]
                .as_array()
                .map(|items| items.len()),
            Some(2)
        );
        assert!(summary.contains("ready for strict commit"));
    }

    #[test]
    fn test_assess_readiness_generates_default_fills_for_spreadsheet_forms() {
        let inspection = json!({
            "selected_region": {
                "patchable_cells": [
                    { "cell": "F5" },
                    { "cell": "G5" },
                    { "cell": "H5" },
                    { "cell": "I5" },
                    { "cell": "K5" }
                ],
                "rows": [
                    {
                        "label": "关键任务完成情况",
                        "missing_cells": [
                            { "cell": "F5", "header": "预期目标\n考核期初设定的绩效目标" },
                            { "cell": "G5", "header": "完成情况自我评估\n(列举实际完成数据)" },
                            { "cell": "H5", "header": "员工自评\n(100分制)" },
                            { "cell": "I5", "header": "上级评分\n(100分制)" },
                            { "cell": "K5", "header": "评分备注" }
                        ]
                    }
                ]
            }
        });

        let (continuation, summary) =
            assess_readiness(&inspection, &json!([]), "permissive").expect("assess");
        assert_eq!(
            continuation["status"],
            Value::String("commit_ready".to_string())
        );
        let fills = continuation["fills"].as_array().expect("fills array");
        assert_eq!(fills.len(), 5);
        assert_eq!(fills[2]["cell"], json!("H5"));
        assert_eq!(fills[2]["value"], json!(90));
        assert_eq!(fills[3]["cell"], json!("I5"));
        assert_eq!(fills[3]["value"], json!(88));
        assert!(summary.contains("permissive commit"));
    }

    #[test]
    fn test_derive_spreadsheet_patch_candidates_generates_fill_candidates() {
        let inspection = json!({
            "selected_region": {
                "rows": [
                    {
                        "label": "关键任务完成情况",
                        "missing_cells": [
                            { "cell": "F5", "header": "预期目标\n考核期初设定的绩效目标" },
                            { "cell": "H5", "header": "员工自评\n(100分制)" }
                        ]
                    }
                ]
            }
        });

        let (patch_candidates, summary) =
            derive_spreadsheet_patch_candidates("把需要填的都填了", &inspection, "permissive")
                .expect("derive");
        let cells = patch_candidates["candidates"]["cells"]
            .as_array()
            .expect("cells");
        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0]["cell"], json!("F5"));
        assert_eq!(cells[1]["cell"], json!("H5"));
        assert_eq!(cells[1]["proposed_value"], json!(90));
        assert!(summary.contains("Derived 2 spreadsheet fill candidates"));
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
