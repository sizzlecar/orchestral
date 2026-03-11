mod actions;
mod apply;
mod assess;
mod inspect;
mod locate;
mod model;
mod support;
mod verify;

pub use self::actions::build_structured_action;

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::assess::assess_structured_readiness;
    use super::model::{json_to_toml_value, remove_json_pointer_value, set_json_pointer_value};

    #[test]
    fn test_assess_structured_readiness_accepts_enveloped_candidates() {
        let inspection = json!({
            "files": [
                { "path": "configs/app.json", "format": "json" }
            ]
        });
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "configs/app.json",
                        "operations": [
                            { "op": "set", "path": "/service/enabled", "value": true }
                        ],
                        "needs_user_input": false,
                        "unknowns": []
                    }
                ]
            },
            "unknowns": [],
            "assumptions": ["existing config structure is authoritative"]
        });

        let (continuation, summary) =
            assess_structured_readiness(&inspection, &patch_candidates, "strict").expect("assess");
        assert_eq!(
            continuation["status"],
            Value::String("commit_ready".to_string())
        );
        assert!(summary.contains("ready for commit"));
    }

    #[test]
    fn test_assess_structured_readiness_permissive_commits_with_concrete_ops() {
        let inspection = json!({
            "files": [
                { "path": "configs/app.json", "format": "json" }
            ]
        });
        let patch_candidates = json!({
            "candidates": {
                "files": [
                    {
                        "path": "configs/app.json",
                        "operations": [
                            { "op": "set", "path": "/service/enabled", "value": true },
                            { "op": "remove", "path": "/obsolete" }
                        ],
                        "needs_user_input": true,
                        "unknowns": ["model claimed more user input was needed"]
                    }
                ]
            },
            "unknowns": ["narrative uncertainty from derive"],
            "assumptions": []
        });

        let (continuation, summary) =
            assess_structured_readiness(&inspection, &patch_candidates, "permissive")
                .expect("assess");
        assert_eq!(
            continuation["status"],
            Value::String("commit_ready".to_string())
        );
        assert!(summary.contains("ready for commit"));
    }

    #[test]
    fn test_set_and_remove_json_pointer_value() {
        let mut value = json!({
            "service": {
                "enabled": false,
                "port": 8080
            }
        });

        set_json_pointer_value(&mut value, "/service/enabled", json!(true)).expect("set");
        set_json_pointer_value(&mut value, "/owner", json!("platform-team")).expect("set owner");
        remove_json_pointer_value(&mut value, "/service/port").expect("remove");

        assert_eq!(value.pointer("/service/enabled"), Some(&json!(true)));
        assert_eq!(value.pointer("/owner"), Some(&json!("platform-team")));
        assert!(value.pointer("/service/port").is_none());
    }

    #[test]
    fn test_json_to_toml_value_supports_nested_objects() {
        let value = json!({
            "service": {
                "enabled": true,
                "port": 9090
            },
            "owner": "platform-team"
        });

        let toml_value = json_to_toml_value(&value).expect("convert");
        let rendered = toml::to_string_pretty(&toml_value).expect("render");
        assert!(rendered.contains("owner = \"platform-team\""));
        assert!(rendered.contains("[service]"));
        assert!(rendered.contains("enabled = true"));
    }
}
