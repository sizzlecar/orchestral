use serde_json::json;

use super::*;
use crate::types::{Step, StepIoBinding};

#[test]
fn test_recipe_compiler_lowers_local_stages_and_rewrites_exports() {
    let mut plan = Plan::new(
        "compile recipe",
        vec![
            Step::recipe("r1")
                .with_exports(vec!["change_spec".to_string(), "updated_path".to_string()])
                .with_params(json!({
                    "stages": [
                        {
                            "id": "inspect",
                            "kind": "action",
                            "action": "inspect_doc",
                            "exports": ["view"]
                        },
                        {
                            "id": "derive",
                            "kind": "agent",
                            "params": {
                                "mode": "leaf",
                                "goal": "derive patch",
                                "output_keys": ["change_spec"]
                            }
                        },
                        {
                            "id": "apply",
                            "kind": "action",
                            "action": "apply_patch",
                            "io_bindings": [
                                {"from": "derive.change_spec", "to": "patch", "required": true}
                            ],
                            "exports": ["updated_path"]
                        }
                    ],
                    "export_from": {
                        "change_spec": "derive.change_spec",
                        "updated_path": "apply.updated_path"
                    }
                })),
            Step::action("s2", "finalize").with_io_bindings(vec![
                StepIoBinding::required("r1.change_spec", "patch"),
                StepIoBinding::required("r1.updated_path", "path"),
            ]),
        ],
    );

    let compiled = RecipeCompiler::new().compile(&mut plan).expect("compile");
    assert!(compiled);
    assert_eq!(plan.steps.len(), 4);
    assert_eq!(plan.steps[0].id.as_str(), "r1__inspect");
    assert_eq!(plan.steps[1].id.as_str(), "r1__derive");
    assert_eq!(plan.steps[2].id.as_str(), "r1__apply");
    let final_step = plan.get_step("s2").expect("s2");
    assert_eq!(final_step.io_bindings[0].from, "r1__derive.change_spec");
    assert_eq!(final_step.io_bindings[1].from, "r1__apply.updated_path");
    assert_eq!(plan.steps[2].depends_on, vec![StepId::from("r1__derive")]);
}

#[test]
fn test_recipe_compiler_attaches_recipe_inputs_to_first_stage() {
    let mut plan = Plan::new(
        "attach inputs",
        vec![Step::recipe("r1")
            .with_depends_on(vec![StepId::from("s0")])
            .with_io_bindings(vec![StepIoBinding::required("s0.path", "path")])
            .with_params(json!({
                "stages": [
                    {
                        "id": "inspect",
                        "kind": "action",
                        "action": "file_read"
                    }
                ]
            }))],
    );

    RecipeCompiler::new().compile(&mut plan).expect("compile");
    let first = plan.get_step("r1__inspect").expect("first stage");
    assert_eq!(first.depends_on, vec![StepId::from("s0")]);
    assert_eq!(first.io_bindings.len(), 1);
    assert_eq!(first.io_bindings[0].from, "s0.path");
}

#[test]
fn test_recipe_compiler_rewrites_recipe_input_placeholders_in_stage_params() {
    let mut plan = Plan::new(
        "rewrite recipe inputs",
        vec![Step::recipe("r1")
            .with_depends_on(vec![StepId::from("s0")])
            .with_io_bindings(vec![StepIoBinding::required("s0.stdout", "excel_path")])
            .with_params(json!({
                "stages": [
                    {
                        "id": "unpack",
                        "kind": "action",
                        "action": "shell",
                        "params": {
                            "command": "python",
                            "args": ["scripts/unpack.py", "{{excel_path}}"]
                        }
                    }
                ]
            }))],
    );

    RecipeCompiler::new().compile(&mut plan).expect("compile");
    let unpack = plan.get_step("r1__unpack").expect("unpack stage");
    assert_eq!(unpack.depends_on, vec![StepId::from("s0")]);
    assert_eq!(
        unpack.params.pointer("/args/1"),
        Some(&json!("{{s0.stdout}}"))
    );
}

#[test]
fn test_recipe_compiler_selects_actions_by_role() {
    let mut compiler = RecipeCompiler::new();
    compiler.register_action_meta(
        &ActionMeta::new("inspect_doc", "inspect")
            .with_capabilities(["filesystem_read"])
            .with_roles(["inspect"]),
    );
    compiler.register_action_meta(
        &ActionMeta::new("persist_doc", "persist")
            .with_capabilities(["filesystem_write"])
            .with_roles(["apply"]),
    );
    compiler.register_action_meta(
        &ActionMeta::new("verify_doc", "verify")
            .with_capabilities(["verification"])
            .with_roles(["verify"]),
    );

    let mut plan = Plan::new(
        "template recipe",
        vec![Step::recipe("r1").with_params(json!({
            "template": "inspect_derive_apply_verify",
            "stage_overrides": {
                "inspect": {
                    "exports": ["content"]
                },
                "derive": {
                    "params": {
                        "goal": "derive change spec",
                        "output_keys": ["change_spec"]
                    },
                    "io_bindings": [
                        {"from": "inspect.content", "to": "source_content", "required": true}
                    ]
                },
                "apply": {
                    "exports": ["path"],
                    "io_bindings": [
                        {"from": "derive.change_spec", "to": "content", "required": true}
                    ]
                },
                "verify": {
                    "exports": ["verified"],
                    "io_bindings": [
                        {"from": "apply.path", "to": "path", "required": true}
                    ]
                }
            },
            "export_from": {
                "updated_path": "apply.path"
            }
        }))],
    );

    compiler.compile(&mut plan).expect("compile");
    assert_eq!(plan.steps.len(), 4);
    assert_eq!(plan.steps[0].action, "inspect_doc");
    assert_eq!(plan.steps[1].kind, StepKind::Agent);
    assert_eq!(plan.steps[2].action, "persist_doc");
    assert_eq!(plan.steps[3].action, "verify_doc");
}

#[test]
fn test_recipe_compiler_prefers_non_fallback_action_for_builtin_template() {
    let mut compiler = RecipeCompiler::new();
    compiler.register_action_meta(
        &ActionMeta::new("file_read", "read file")
            .with_roles(["inspect"])
            .with_capabilities(["filesystem_read"]),
    );
    compiler.register_action_meta(
        &ActionMeta::new("file_write", "write file")
            .with_roles(["apply"])
            .with_capabilities(["filesystem_write"]),
    );
    compiler.register_action_meta(
        &ActionMeta::new("file_verify", "verify file")
            .with_roles(["verify"])
            .with_capabilities(["verification"]),
    );
    compiler.register_action_meta(
        &ActionMeta::new("shell", "fallback shell")
            .with_roles(["inspect", "apply", "verify"])
            .with_capabilities(["shell", "fallback", "filesystem_write", "verification"]),
    );

    let mut plan = Plan::new(
        "prefer specific action",
        vec![Step::recipe("r1").with_params(json!({
            "template": "inspect_derive_apply_verify"
        }))],
    );

    compiler.compile(&mut plan).expect("compile");
    assert_eq!(
        plan.get_step("r1__inspect")
            .map(|step| step.action.as_str()),
        Some("file_read")
    );
    assert_eq!(
        plan.get_step("r1__apply").map(|step| step.action.as_str()),
        Some("file_write")
    );
    assert_eq!(
        plan.get_step("r1__verify").map(|step| step.action.as_str()),
        Some("file_verify")
    );
}

#[test]
fn test_recipe_compiler_rejects_unknown_stage_override() {
    let mut plan = Plan::new(
        "bad override",
        vec![Step::recipe("r1").with_params(json!({
            "template": "inspect_derive_apply_verify",
            "stage_overrides": {
                "unknown": {
                    "action": "echo"
                }
            }
        }))],
    );

    let err = RecipeCompiler::new()
        .compile(&mut plan)
        .expect_err("expected compile error");
    match err {
        RecipeCompileError::InvalidRecipe { reason, .. } => {
            assert!(reason.contains("unknown stage"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_recipe_compiler_expands_verify_with_contract() {
    let mut compiler = RecipeCompiler::new();
    compiler.register_action_meta(
        &ActionMeta::new("write_doc", "write").with_capabilities(["filesystem_write"]),
    );
    compiler.register_action_meta(
        &ActionMeta::new("verify_doc", "verify").with_capabilities(["verification"]),
    );

    let mut plan = Plan::new(
        "verify contract",
        vec![Step::recipe("r1").with_params(json!({
            "stages": [
                {
                    "id": "apply",
                    "kind": "action",
                    "selector": { "all_of": ["filesystem_write"] },
                    "exports": ["path"],
                    "verify_with": {
                        "selector": { "any_of": ["verification"] },
                        "exports": ["verified"]
                    }
                }
            ],
            "export_from": {
                "verified": "apply_verify.verified"
            }
        }))],
    );

    compiler.compile(&mut plan).expect("compile");
    assert_eq!(plan.steps.len(), 2);
    assert_eq!(plan.steps[0].id.as_str(), "r1__apply");
    assert_eq!(plan.steps[0].action, "write_doc");
    assert_eq!(plan.steps[1].id.as_str(), "r1__apply_verify");
    assert_eq!(plan.steps[1].action, "verify_doc");
    assert_eq!(plan.steps[1].depends_on, vec![StepId::from("r1__apply")]);
    assert_eq!(plan.steps[1].io_bindings[0].from, "r1__apply.path");
}
