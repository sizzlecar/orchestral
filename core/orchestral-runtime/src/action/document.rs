mod actions;
mod apply;
mod assess;
mod inspect;
mod locate;
mod model;
mod support;
mod verify;

pub use self::actions::build_document_action;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::{json, Value};

    use super::apply::apply_document_patch;
    use super::assess::{assess_document_readiness, collect_candidate_unknowns};
    use super::support::request_requires_confirmation;

    #[test]
    fn test_request_requires_confirmation_for_plan_first_prompt() {
        assert!(request_requires_confirmation(
            "先不要写回，先给出修改计划并等待我确认"
        ));
        assert!(!request_requires_confirmation("直接写回所有文档"));
    }

    #[test]
    fn test_assess_document_readiness_waits_for_confirmation() {
        let inspection = json!({
            "files": [
                {
                    "path": "docs/a.md",
                    "missing_title": true,
                    "todo_count": 2
                }
            ]
        });
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "docs/a.md",
                        "planned_changes": ["补全标题", "替换 TODO"],
                        "needs_user_input": false,
                        "unknowns": []
                    }
                ]
            },
            "unknowns": [],
            "assumptions": []
        });

        let (continuation, summary) = assess_document_readiness(
            "扫描 markdown，先给出修改计划并等待我确认",
            &inspection,
            &patch_candidates,
            "permissive",
        )
        .expect("assess");
        assert!(summary.contains("计划修改以下文档"));
        assert_eq!(
            continuation["status"],
            Value::String("wait_user".to_string())
        );
        assert_eq!(
            continuation["next_stage_hint"],
            Value::String("commit".to_string())
        );
    }

    #[test]
    fn test_collect_candidate_unknowns_supports_enveloped_files() {
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "docs/a.md",
                        "needs_user_input": true,
                        "unknowns": ["missing owner"]
                    }
                ]
            },
            "unknowns": ["missing due date"]
        });

        let unknowns = collect_candidate_unknowns(&patch_candidates);
        assert!(unknowns.contains(&"docs/a.md requires additional user input".to_string()));
        assert!(unknowns.contains(&"missing owner".to_string()));
        assert!(unknowns.contains(&"missing due date".to_string()));
    }

    #[test]
    fn test_apply_document_patch_generates_requested_report_when_missing_from_patch_spec() {
        let root = std::env::temp_dir().join(format!(
            "orchestral-document-apply-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("unix time")
                .as_millis()
        ));
        let docs_dir = root.join("docs");
        let reports_dir = root.join("reports");
        fs::create_dir_all(&docs_dir).expect("create docs dir");
        fs::create_dir_all(&reports_dir).expect("create reports dir");

        let doc_path = docs_dir.join("guide.md");
        let report_path = reports_dir.join("summary.md");
        let patch_spec = json!({
            "summary": "Updated missing title.",
            "updates": [
                {
                    "path": doc_path.to_string_lossy(),
                    "content": "# Guide\n\n## Scope\n\nExample.\n"
                }
            ]
        });

        let exports = apply_document_patch(&patch_spec, Some(report_path.to_string_lossy().as_ref()))
            .expect("apply document patch");
        let report = fs::read_to_string(&report_path).expect("read report");

        assert!(report.contains("# Patch Summary"));
        assert!(report.contains("Updated missing title."));
        assert!(report.contains("guide.md"));
        assert!(exports
            .get("updated_paths")
            .and_then(Value::as_array)
            .is_some_and(|paths| paths.iter().any(|value| value.as_str() == Some(report_path.to_string_lossy().as_ref()))));

        let _ = fs::remove_dir_all(root);
    }
}
