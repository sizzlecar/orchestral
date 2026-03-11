use serde_json::Value;

use crate::types::{StepId, StepIoBinding, StepKind};

use super::{ActionSelector, RecipeStageTemplate, RecipeTemplate, RecipeVerificationTemplate};

pub(super) fn builtin_recipe_templates() -> Vec<RecipeTemplate> {
    vec![
        builtin_inspect_derive_apply_verify_template(),
        builtin_collect_derive_emit_template(),
        builtin_resolve_execute_verify_template(),
    ]
}

fn builtin_inspect_derive_apply_verify_template() -> RecipeTemplate {
    RecipeTemplate::new(
        "inspect_derive_apply_verify",
        vec![
            RecipeStageTemplate {
                id: StepId::from("inspect"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["inspect"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: Vec::new(),
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("derive"),
                kind: StepKind::Agent,
                action: None,
                selector: None,
                depends_on: vec![StepId::from("inspect")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: serde_json::json!({
                    "mode": "leaf"
                }),
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("apply"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["apply"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: vec![StepId::from("derive")],
                exports: vec!["path".to_string()],
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: Some(RecipeVerificationTemplate {
                    id: Some(StepId::from("verify")),
                    action: None,
                    selector: Some(
                        ActionSelector::default()
                            .with_roles_any_of(["verify"])
                            .with_none_of(["fallback"]),
                    ),
                    exports: Vec::new(),
                    io_bindings: vec![StepIoBinding::required("apply.path", "path")],
                    params: Value::Null,
                }),
            },
        ],
    )
    .with_description("Generic mutable workflow: inspect current state, derive a structured delta, apply it, then verify.")
}

fn builtin_collect_derive_emit_template() -> RecipeTemplate {
    RecipeTemplate::new(
        "collect_derive_emit",
        vec![
            RecipeStageTemplate {
                id: StepId::from("collect"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["collect"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: Vec::new(),
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("derive"),
                kind: StepKind::Agent,
                action: None,
                selector: None,
                depends_on: vec![StepId::from("collect")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: serde_json::json!({
                    "mode": "leaf"
                }),
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("emit"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["emit"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: vec![StepId::from("derive")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
        ],
    )
    .with_description("Generic read-only workflow: collect upstream facts, derive a structured result, then emit an artifact or response.")
}

fn builtin_resolve_execute_verify_template() -> RecipeTemplate {
    RecipeTemplate::new(
        "resolve_execute_verify",
        vec![
            RecipeStageTemplate {
                id: StepId::from("resolve"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["resolve"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: Vec::new(),
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: None,
            },
            RecipeStageTemplate {
                id: StepId::from("execute"),
                kind: StepKind::Action,
                action: None,
                selector: Some(
                    ActionSelector::default()
                        .with_roles_all_of(["execute"])
                        .with_none_of(["fallback"]),
                ),
                depends_on: vec![StepId::from("resolve")],
                exports: Vec::new(),
                io_bindings: Vec::new(),
                params: Value::Null,
                verify_with: Some(RecipeVerificationTemplate {
                    id: Some(StepId::from("verify")),
                    action: None,
                    selector: Some(
                        ActionSelector::default()
                            .with_roles_any_of(["verify"])
                            .with_none_of(["fallback"]),
                    ),
                    exports: Vec::new(),
                    io_bindings: Vec::new(),
                    params: Value::Null,
                }),
            },
        ],
    )
    .with_description("Generic side-effect workflow: resolve a target, execute the effect deterministically, then verify.")
}
