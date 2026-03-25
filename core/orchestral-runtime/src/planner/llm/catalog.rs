use std::fmt::Write;

use orchestral_core::action::ActionMeta;

pub(super) fn build_capability_catalog(actions: &[ActionMeta]) -> String {
    let mut out = String::new();

    let action_lines = build_action_lines(actions);
    let mcp_lines = build_mcp_lines(actions);

    if action_lines.is_empty() && mcp_lines.is_empty() {
        return out;
    }

    out.push_str("Capability Catalog:\n");

    if !action_lines.is_empty() {
        out.push_str("Actions:\n");
        for line in action_lines {
            out.push_str(&line);
        }
    }

    if !mcp_lines.is_empty() {
        out.push_str("MCP Tools (use tool_lookup to get full input schema before calling):\n");
        for line in mcp_lines {
            let _ = writeln!(out, "- {}", line);
        }
    }

    out
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
            let mut card = format!("- {}: {}", prefix, action.description);
            if !action.input_kinds.is_empty() {
                let _ = write!(card, "\n  consumes: {}", action.input_kinds.join(", "));
            }
            if !action.output_kinds.is_empty() {
                let _ = write!(card, "\n  produces: {}", action.output_kinds.join(", "));
            }
            let input_fields = summarize_schema_fields(&action.input_schema);
            if !input_fields.is_empty() {
                let _ = write!(card, "\n  input_fields: {}", input_fields.join(", "));
            }
            let output_fields = summarize_schema_fields(&action.output_schema);
            if !output_fields.is_empty() {
                let _ = write!(card, "\n  output_fields: {}", output_fields.join(", "));
            }
            card.push('\n');
            card
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
            summarize_schema_field(&name, &properties[&name], required.contains(name.as_str()))
        })
        .collect()
}

fn summarize_schema_field(name: &str, schema: &serde_json::Value, required: bool) -> String {
    let mut label = if required {
        format!("{} (required)", name)
    } else {
        name.to_string()
    };
    let mut hints = Vec::new();

    if let Some(values) = schema.get("enum").and_then(|value| value.as_array()) {
        let items = values
            .iter()
            .filter_map(|value| value.as_str())
            .collect::<Vec<_>>();
        if !items.is_empty() {
            hints.push(format!("enum: {}", items.join(" | ")));
        }
    }

    if let Some(shape) = summarize_object_shape(schema) {
        hints.push(format!("shape: {}", shape));
    }

    if let Some(description) = schema.get("description").and_then(|value| value.as_str()) {
        if description.to_ascii_lowercase().contains("utf-8") {
            hints.push("UTF-8 text".to_string());
        }
    }

    if !hints.is_empty() {
        label.push_str(&format!(" [{}]", hints.join("; ")));
    }
    label
}

fn summarize_object_shape(schema: &serde_json::Value) -> Option<String> {
    let properties = schema
        .get("properties")
        .and_then(|value| value.as_object())?;
    if properties.is_empty() {
        return None;
    }
    let required = schema_required_fields(schema);
    let mut names = properties.keys().cloned().collect::<Vec<_>>();
    names.sort();
    if names.len() > 4 {
        names.truncate(4);
        names.push("...".to_string());
    }
    let summary = names
        .into_iter()
        .map(|name| {
            if required.contains(name.as_str()) {
                format!("{}*", name)
            } else {
                name
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("{{{}}}", summary))
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
                .with_input_kinds(["workspace.path"])
                .with_output_kinds(["workspace.text_file"])
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
                .with_input_kinds(["document.patch_spec"])
                .with_output_kinds(["document.apply_result"])
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

        assert!(catalog.contains("Actions:"));
        assert!(catalog.contains("file_read [direct]: Read a file"));
        assert!(catalog.contains("document_apply_patch [document]: Apply document patch"));
        assert!(catalog.contains("consumes: workspace.path"));
        assert!(catalog.contains("produces: workspace.text_file"));
        assert!(catalog.contains("consumes: document.patch_spec"));
        assert!(catalog.contains("produces: document.apply_result"));
        assert!(catalog.contains("input_fields: path (required)"));
        assert!(catalog.contains("output_fields: content (required)"));
        assert!(catalog.contains("input_fields: patch_spec (required)"));
        assert!(catalog.contains("output_fields: updated_paths (required)"));
        assert!(catalog
            .contains("MCP Tools (use tool_lookup to get full input schema before calling):"));
    }
}
