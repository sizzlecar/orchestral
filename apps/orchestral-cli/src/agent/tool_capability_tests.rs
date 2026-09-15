use super::*;
use orchestral_core::tool_protocol::{ToolCallId, ToolInvocation, ToolOutcome, ToolOutput};
use orchestral_runtime::GuardedToolResult;
use serde_json::json;

#[test]
fn file_schema_projection_keeps_sdk_contracts_and_binds_descriptor_identity() {
    let single = CliWorkspaceSet {
        primary: PathBuf::from("primary"),
        additional: Vec::new(),
    };
    let multiple = CliWorkspaceSet {
        additional: vec![PathBuf::from("shared")],
        ..single.clone()
    };
    let restriction = ToolRestriction {
        bounds: ToolPolicyBounds {
            allowed_effects: BTreeSet::from([
                EffectScope::FilesystemRead,
                EffectScope::FilesystemWrite,
            ]),
            ..Default::default()
        },
    };
    for (original, arguments) in [
        (
            guarded_file_read_descriptor(restriction.clone()),
            json!({"path":"source.rs"}),
        ),
        (
            guarded_file_search_descriptor(restriction.clone()),
            json!({"pattern":"*.rs"}),
        ),
        (
            guarded_text_search_descriptor(restriction.clone()),
            json!({"pattern":"needle"}),
        ),
        (
            guarded_file_write_descriptor(restriction.clone()),
            json!({"path":"new.rs","content":"text","mode":"create"}),
        ),
        (
            guarded_file_edit_descriptor(restriction.clone()),
            json!({"path":"source.rs","old_text":"before","new_text":"after"}),
        ),
        (
            guarded_apply_patch_descriptor(restriction),
            json!({"patch":"*** Begin Patch\n*** Add File: new.rs\n+text\n*** End Patch"}),
        ),
    ] {
        let projected = single.file_tool_descriptor(original.clone());
        projected
            .model_schema
            .validate_arguments(&arguments)
            .unwrap();
        let mut selected = arguments;
        selected["workspace"] = json!("shared");
        original.model_schema.validate_arguments(&selected).unwrap();
        assert!(projected
            .model_schema
            .validate_arguments(&selected)
            .is_err());
        assert_eq!(multiple.file_tool_descriptor(original.clone()), original);
        assert_ne!(projected.digest().unwrap(), original.digest().unwrap());
        // All other schema, effect, approval, and output contract fields remain equal.
        let mut restored = projected;
        restored.model_schema.input_schema["properties"]["workspace"] =
            original.model_schema.input_schema["properties"]["workspace"].clone();
        assert_eq!(restored, original);
    }
}

#[tokio::test]
async fn cli_file_schema_projection_preserves_primary_and_additional_root_operations() {
    let root = std::env::temp_dir().join(unique_id("tool-capability-projection", 0));
    let primary = root.join("primary");
    let shared = root.join("shared");
    std::fs::create_dir_all(&primary).unwrap();
    std::fs::create_dir_all(&shared).unwrap();
    for additional in [Vec::new(), vec![shared.clone()]] {
        std::fs::write(primary.join("same.txt"), "primary\n").unwrap();
        std::fs::write(shared.join("same.txt"), "shared\n").unwrap();
        let workspaces = CliWorkspaceSet::resolve(Some(&primary), &additional).unwrap();
        let effects = Arc::new(InMemoryToolEffectJournalStore::default());
        let artifacts =
            ToolArtifactStore::new(Arc::new(InMemoryBlobStore::default()), 8192, 128).unwrap();
        let reader = GuardedArtifactReadExecutor::new_session_scoped(
            artifacts.clone(),
            Arc::new(InMemoryAgentJournalStore::default()),
            Arc::new(InMemoryAgentSessionJournalStore::default()),
            effects.clone(),
        );
        let composition = build_cli_tool_runtime(
            &OrchestralConfig::default(),
            &[],
            effects,
            artifacts,
            &workspaces,
            reader,
        )
        .unwrap();
        let schemas = composition.runtime.model_tool_schemas().unwrap();
        for name in [
            "file_read",
            "file_search",
            "text_search",
            "file_write",
            "file_edit",
            "apply_patch",
        ] {
            let schema = schemas.iter().find(|schema| schema.name == name).unwrap();
            assert_eq!(
                schema.input_schema["properties"].get("workspace").is_some(),
                !additional.is_empty()
            );
        }
        let invoke = |call: &str, tool: &str, arguments| {
            composition.runtime.invoke(
                ToolInvocation {
                    run_id: RunId::new("projected-workspace"),
                    call_id: ToolCallId::new(call),
                    tool_id: composition.runtime.resolve_tool_id(tool).unwrap().unwrap(),
                    arguments,
                },
                composition.run_grant.clone(),
                None,
                CancellationToken::new(),
            )
        };
        let result = invoke("primary", "file_read", json!({"path":"same.txt"})).await;
        let GuardedToolResult::Outcome {
            outcome:
                ToolOutcome::Completed {
                    output: ToolOutput::Inline(output),
                },
            ..
        } = result
        else {
            panic!("primary read failed: {result:?}");
        };
        assert_eq!(output["content"], "primary\n");
        assert_eq!(
            output["workspace"],
            workspaces.primary.to_string_lossy().as_ref()
        );
        let result = invoke(
            "shared",
            "file_read",
            json!({"path":"same.txt", "workspace":std::fs::canonicalize(&shared).unwrap()}),
        )
        .await;
        if additional.is_empty() {
            assert!(
                matches!(
                    result,
                    GuardedToolResult::Outcome {
                        outcome: ToolOutcome::Rejected { .. },
                        ..
                    }
                ),
                "{result:?}"
            );
        } else {
            let GuardedToolResult::Outcome {
                outcome:
                    ToolOutcome::Completed {
                        output: ToolOutput::Inline(output),
                    },
                ..
            } = result
            else {
                panic!("additional root read failed: {result:?}");
            };
            assert_eq!(output["content"], "shared\n");
        }
        let result = invoke("escape", "file_read", json!({"path":"../shared/same.txt"})).await;
        assert!(
            matches!(
                result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Rejected { .. },
                    ..
                }
            ),
            "{result:?}"
        );
        let (target, untouched, old_text) = if additional.is_empty() {
            (&primary, &shared, "primary")
        } else {
            (&shared, &primary, "shared")
        };
        let untouched_before = std::fs::read(untouched.join("same.txt")).unwrap();
        let mut arguments = json!({"path":"same.txt", "old_text":old_text, "new_text":"edited"});
        if !additional.is_empty() {
            arguments["workspace"] = json!(std::fs::canonicalize(target).unwrap());
        }
        let result = invoke("edit", "file_edit", arguments).await;
        assert!(
            matches!(
                result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Completed { .. },
                    ..
                }
            ),
            "{result:?}"
        );
        assert_eq!(
            std::fs::read_to_string(target.join("same.txt")).unwrap(),
            "edited\n"
        );
        let mut arguments = json!({"path":"same.txt", "edits":[
            {"old_text":"edi", "new_text":"up"},
            {"old_text":"ted", "new_text":"dated"}
        ]});
        if !additional.is_empty() {
            arguments["workspace"] = json!(std::fs::canonicalize(target).unwrap());
        }
        let result = invoke("batch-edit", "file_edit", arguments).await;
        assert!(
            matches!(
                result,
                GuardedToolResult::Outcome {
                    outcome: ToolOutcome::Completed { .. },
                    ..
                }
            ),
            "{result:?}"
        );
        assert_eq!(
            std::fs::read_to_string(target.join("same.txt")).unwrap(),
            "updated\n"
        );
        assert_eq!(
            std::fs::read(untouched.join("same.txt")).unwrap(),
            untouched_before
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
