use std::collections::BTreeMap;
use std::fmt::Write;

use orchestral_core::action::ActionMeta;

pub(super) fn build_capability_catalog(actions: &[ActionMeta]) -> String {
    let mut out = String::new();

    let category_lines = build_category_lines(actions);
    let action_lines = build_action_lines(actions);
    let mcp_lines = build_mcp_lines(actions);

    if category_lines.is_empty() && action_lines.is_empty() && mcp_lines.is_empty() {
        return out;
    }

    out.push_str("Capability Catalog:\n");

    if !category_lines.is_empty() {
        out.push_str("Action Categories:\n");
        for line in category_lines {
            let _ = writeln!(out, "- {}", line);
        }
    }

    if !action_lines.is_empty() {
        out.push_str("Actions:\n");
        for line in action_lines {
            let _ = writeln!(out, "- {}", line);
        }
    }

    if !mcp_lines.is_empty() {
        out.push_str("MCP Tools:\n");
        for line in mcp_lines {
            let _ = writeln!(out, "- {}", line);
        }
    }

    out
}

fn build_category_lines(actions: &[ActionMeta]) -> Vec<String> {
    let mut categories = BTreeMap::<String, Vec<&ActionMeta>>::new();
    for action in actions {
        let Some(category) = action.category.as_deref() else {
            continue;
        };
        if category == "mcp" {
            continue;
        }
        categories
            .entry(category.to_string())
            .or_default()
            .push(action);
    }

    categories
        .into_iter()
        .map(|(category, mut actions)| {
            actions.sort_by(|a, b| a.name.cmp(&b.name));
            let names = actions
                .iter()
                .map(|action| action.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}: {}", category, names)
        })
        .collect()
}

fn build_action_lines(actions: &[ActionMeta]) -> Vec<String> {
    let mut lines = actions
        .iter()
        .filter(|action| !is_mcp_action(action))
        .map(|action| {
            let prefix = action
                .category
                .as_deref()
                .map(|category| format!("{} [{}]", action.name, category))
                .unwrap_or_else(|| action.name.clone());
            let mut parts = vec![format!("{}: {}", prefix, action.description)];
            let input_fields = summarize_schema_fields(&action.input_schema);
            if !input_fields.is_empty() {
                parts.push(format!("input_fields: {}", input_fields.join(", ")));
            }
            let output_fields = summarize_schema_fields(&action.output_schema);
            if !output_fields.is_empty() {
                parts.push(format!("output_fields: {}", output_fields.join(", ")));
            }
            parts.join(" | ")
        })
        .collect::<Vec<_>>();
    lines.sort();
    lines
}

fn build_mcp_lines(actions: &[ActionMeta]) -> Vec<String> {
    let mut lines = actions
        .iter()
        .filter(|action| is_mcp_action(action))
        .map(|action| format!("{}: {}", action.name, action.description))
        .collect::<Vec<_>>();
    lines.sort();
    lines
}

fn is_mcp_action(action: &ActionMeta) -> bool {
    action.has_capability("mcp") || action.name.starts_with("mcp__")
}

fn summarize_schema_fields(schema: &serde_json::Value) -> Vec<String> {
    let properties = match schema.get("properties").and_then(|value| value.as_object()) {
        Some(properties) => properties,
        None => return Vec::new(),
    };
    let required = schema_required_fields(schema);
    let mut names = properties.keys().cloned().collect::<Vec<_>>();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            if required.contains(name.as_str()) {
                format!("{} (required)", name)
            } else {
                name
            }
        })
        .collect()
}

fn schema_required_fields(schema: &serde_json::Value) -> std::collections::BTreeSet<&str> {
    schema
        .get("required")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_capability_catalog_groups_by_category() {
        let catalog = build_capability_catalog(&[
            ActionMeta::new("file_read", "Read a file")
                .with_category("direct")
                .with_input_schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" }
                    },
                    "required": ["path"]
                }))
                .with_output_schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "content": { "type": "string" }
                    },
                    "required": ["content"]
                })),
            ActionMeta::new("document_apply_patch", "Apply document patch")
                .with_category("document")
                .with_input_schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "patch_spec": { "type": "object" }
                    },
                    "required": ["patch_spec"]
                }))
                .with_output_schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "updated_paths": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    },
                    "required": ["updated_paths"]
                })),
            ActionMeta::new("mcp__demo", "Call MCP").with_capability("mcp"),
        ]);

        assert!(catalog.contains("Action Categories:"));
        assert!(catalog.contains("direct: file_read"));
        assert!(catalog.contains("document: document_apply_patch"));
        assert!(catalog.contains("Actions:"));
        assert!(catalog.contains("file_read [direct]: Read a file"));
        assert!(catalog.contains("document_apply_patch [document]: Apply document patch"));
        assert!(catalog.contains("input_fields: path (required)"));
        assert!(catalog.contains("output_fields: content (required)"));
        assert!(catalog.contains("input_fields: patch_spec (required)"));
        assert!(catalog.contains("output_fields: updated_paths (required)"));
        assert!(catalog.contains("MCP Tools:"));
    }
}
