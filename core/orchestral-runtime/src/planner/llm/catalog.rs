use std::collections::BTreeSet;
use std::fmt::Write;

use orchestral_core::action::ActionMeta;

const DIRECT_ACTIONS: &[&str] = &["shell", "file_read", "http"];

pub(super) fn build_capability_catalog(actions: &[ActionMeta]) -> String {
    let mut out = String::new();

    let reactor_lines = build_reactor_lines(actions);
    let direct_lines = build_direct_action_lines(actions);
    let mcp_lines = build_mcp_lines(actions);

    if reactor_lines.is_empty() && direct_lines.is_empty() && mcp_lines.is_empty() {
        return out;
    }

    out.push_str("Capability Catalog:\n");

    if !reactor_lines.is_empty() {
        out.push_str("Reactor Pipelines:\n");
        for line in reactor_lines {
            let _ = writeln!(out, "- {}", line);
        }
    }

    if !direct_lines.is_empty() {
        out.push_str("Direct Actions:\n");
        for line in direct_lines {
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

fn build_reactor_lines(actions: &[ActionMeta]) -> Vec<String> {
    let names = actions
        .iter()
        .map(|action| action.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut lines = Vec::new();

    push_reactor_family(
        &mut lines,
        "spreadsheet",
        &names,
        &[
            "reactor_spreadsheet_locate",
            "reactor_spreadsheet_inspect",
            "reactor_spreadsheet_assess_readiness",
            "reactor_spreadsheet_apply_patch",
            "reactor_spreadsheet_verify_patch",
        ],
    );
    push_reactor_family(
        &mut lines,
        "document",
        &names,
        &[
            "reactor_document_locate",
            "reactor_document_inspect",
            "reactor_document_assess_readiness",
            "reactor_document_apply_patch",
            "reactor_document_verify_patch",
        ],
    );
    push_reactor_family(
        &mut lines,
        "structured",
        &names,
        &[
            "reactor_structured_locate",
            "reactor_structured_inspect",
            "reactor_structured_assess_readiness",
            "reactor_structured_apply_patch",
            "reactor_structured_verify_patch",
        ],
    );

    lines
}

fn push_reactor_family(
    lines: &mut Vec<String>,
    family: &str,
    names: &BTreeSet<&str>,
    required: &[&str],
) {
    if required.iter().all(|name| names.contains(name)) {
        lines.push(format!(
            "{}: locate_and_patch + inspect_and_extract coverage",
            family
        ));
    }
}

fn build_direct_action_lines(actions: &[ActionMeta]) -> Vec<String> {
    actions
        .iter()
        .filter(|action| DIRECT_ACTIONS.contains(&action.name.as_str()))
        .map(|action| format!("{}: {}", action.name, action.description))
        .collect()
}

fn build_mcp_lines(actions: &[ActionMeta]) -> Vec<String> {
    actions
        .iter()
        .filter(|action| action.name.starts_with("mcp__"))
        .map(|action| format!("{}: {}", action.name, action.description))
        .collect()
}

