//! Model views of result structures owned by these builtins. Canonical outputs
//! remain unchanged for journaling, validation, and complete-read evidence.

use orchestral_core::agent_protocol::wire::Digest;
use orchestral_core::tool_protocol::ToolInvocation;
use serde_json::{json, Map, Value};

type Field = (&'static str, fn(&Value) -> bool);

pub(super) fn contract(kind: &str) -> Value {
    json!({ "contract": format!("orchestral.builtin-{kind}-model-output/v1") })
}

// A changed producer schema must opt in explicitly. Unknown fields and malformed
// results pass through, rather than losing a new warning or outcome detail.
fn shape(value: &Value, required: &[Field], optional: &[Field]) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    required
        .iter()
        .all(|(name, valid)| object.get(*name).is_some_and(valid))
        && object.iter().all(|(name, value)| {
            required
                .iter()
                .chain(optional)
                .find(|(key, _)| name.as_str() == *key)
                .is_some_and(|(_, valid)| valid(value))
        })
}

fn unsigned(value: &Value) -> bool {
    value.as_u64().is_some()
}
fn integer(value: &Value) -> bool {
    value.as_i64().is_some()
}
fn strings(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|items| items.iter().all(Value::is_string))
}
fn unsigneds(value: &Value) -> bool {
    value
        .as_array()
        .is_some_and(|items| items.iter().all(unsigned))
}
fn select(output: &Value, names: &[&str]) -> Map<String, Value> {
    names
        .iter()
        .filter_map(|name| {
            output
                .get(*name)
                .map(|value| ((*name).to_owned(), value.clone()))
        })
        .collect()
}
fn workspace(invocation: &ToolInvocation, output: &Value, view: &mut Map<String, Value>) {
    if invocation.arguments.get("workspace").is_some() {
        view.insert("workspace".to_owned(), output["workspace"].clone());
    }
}

pub(super) fn file_read(invocation: &ToolInvocation, output: &Value) -> Value {
    if !shape(
        output,
        &[
            ("workspace", Value::is_string),
            ("path", Value::is_string),
            ("revision", Value::is_string),
            ("content", Value::is_string),
            ("content_digest", Value::is_string),
            ("start_line", unsigned),
            ("end_line", unsigned),
            ("next_offset", unsigned),
            ("eof", Value::is_boolean),
            ("truncated", Value::is_boolean),
            ("truncation_reasons", strings),
            ("truncated_line_numbers", unsigneds),
            ("file_size_bytes", unsigned),
            ("scanned_bytes", unsigned),
        ],
        &[],
    ) {
        return output.clone();
    }
    let content = output["content"].as_str().unwrap();
    let truncated = output["truncated"].as_bool().unwrap();
    if output["start_line"] == 0
        || output["content_digest"].as_str() != Some(Digest::sha256(content.as_bytes()).as_str())
        || truncated == output["truncation_reasons"].as_array().unwrap().is_empty()
        || (!truncated
            && !output["truncated_line_numbers"]
                .as_array()
                .unwrap()
                .is_empty())
    {
        return output.clone();
    }
    let complete = output["start_line"] == 1 && output["eof"] == true && !truncated;
    if complete && output["file_size_bytes"].as_u64() != Some(content.len() as u64) {
        return output.clone();
    }
    let mut view = select(output, &["path", "content"]);
    workspace(invocation, output, &mut view);
    if !complete {
        view.extend(select(
            output,
            &["start_line", "end_line", "next_offset", "eof", "truncated"],
        ));
        for name in ["truncation_reasons", "truncated_line_numbers"] {
            if !output[name].as_array().unwrap().is_empty() {
                view.insert(name.to_owned(), output[name].clone());
            }
        }
    }
    Value::Object(view)
}

pub(super) fn exec(output: &Value) -> Value {
    if !shape(
        output,
        &[
            ("stdout", Value::is_string),
            ("stderr", Value::is_string),
            ("alive", Value::is_boolean),
            ("wall_time_seconds", Value::is_number),
            ("truncated", Value::is_boolean),
            ("dropped_bytes", unsigned),
            ("sandbox_backend", Value::is_string),
        ],
        &[("session_id", unsigned), ("exit_code", integer)],
    ) {
        return output.clone();
    }
    let alive = output["alive"] == true;
    if output.get("exit_code").is_some_and(|code| code != 0)
        || alive != output.get("session_id").is_some()
        || (alive && output.get("exit_code").is_some())
        || output["truncated"].as_bool() != Some(output["dropped_bytes"].as_u64().unwrap() > 0)
    {
        return output.clone();
    }
    let mut view = select(output, &["stdout", "stderr", "exit_code"]);
    if alive || output.get("exit_code").is_none() {
        view.extend(select(output, &["alive", "session_id"]));
    }
    if output["truncated"] == true {
        view.extend(select(output, &["truncated", "dropped_bytes"]));
    }
    if !matches!(
        output["sandbox_backend"].as_str(),
        Some("macos_seatbelt" | "linux_bwrap" | "existing_session")
    ) {
        view.insert(
            "sandbox_backend".to_owned(),
            output["sandbox_backend"].clone(),
        );
    }
    Value::Object(view)
}

pub(super) fn mutation(invocation: &ToolInvocation, output: &Value) -> Value {
    if !shape(
        output,
        &[
            ("workspace", Value::is_string),
            ("changed_files", unsigned),
            ("changes", Value::is_array),
        ],
        &[],
    ) {
        return output.clone();
    }
    let changes = output["changes"].as_array().unwrap();
    if output["changed_files"].as_u64() != Some(changes.len() as u64) {
        return output.clone();
    }
    let mut projected = Vec::with_capacity(changes.len());
    for change in changes {
        if !shape(
            change,
            &[
                ("operation", Value::is_string),
                ("path", Value::is_string),
                ("bytes", unsigned),
            ],
            &[
                ("before_digest", Value::is_string),
                ("after_digest", Value::is_string),
            ],
        ) {
            return output.clone();
        }
        let expected = match change["operation"].as_str() {
            Some("add") => (false, true),
            Some("update") => (true, true),
            Some("delete") => (true, false),
            _ => return output.clone(),
        };
        if (
            change.get("before_digest").is_some(),
            change.get("after_digest").is_some(),
        ) != expected
        {
            return output.clone();
        }
        projected.push(Value::Object(select(
            change,
            &["operation", "path", "bytes"],
        )));
    }
    let mut view = select(output, &["changed_files"]);
    view.insert("changes".to_owned(), Value::Array(projected));
    workspace(invocation, output, &mut view);
    Value::Object(view)
}

pub(super) fn search(invocation: &ToolInvocation, output: &Value, text_matches: bool) -> Value {
    if !shape(
        output,
        &[
            ("workspace", Value::is_string),
            ("root", Value::is_string),
            ("matches", Value::is_array),
            ("count", unsigned),
            ("completeness", Value::is_string),
            ("partial_reasons", strings),
            ("refinement", Value::is_string),
            ("warnings", strings),
            ("stats", Value::is_object),
        ],
        &[],
    ) || !shape(
        &output["stats"],
        &[
            ("scanned_entries", unsigned),
            ("considered_files", unsigned),
            ("searched_files", unsigned),
            ("scanned_bytes", unsigned),
            ("skipped_binary_files", unsigned),
            ("skipped_unreadable_files", unsigned),
        ],
        &[],
    ) {
        return output.clone();
    }
    let complete = match output["completeness"].as_str() {
        Some("complete") => true,
        Some("partial") => false,
        _ => return output.clone(),
    };
    if output["count"].as_u64() != Some(output["matches"].as_array().unwrap().len() as u64)
        || complete != output["partial_reasons"].as_array().unwrap().is_empty()
    {
        return output.clone();
    }
    if !output["matches"].as_array().unwrap().iter().all(|item| {
        if !text_matches {
            return item.is_string();
        }
        shape(
            item,
            &[
                ("path", Value::is_string),
                ("line_number", unsigned),
                ("column", unsigned),
                ("match_start_byte", unsigned),
                ("match_end_byte", unsigned),
                ("preview", Value::is_string),
                ("preview_truncated", Value::is_boolean),
                ("context_before", strings),
                ("context_after", strings),
            ],
            &[],
        )
    }) {
        return output.clone();
    }
    // Match objects (including previews, byte/line positions and context) are
    // kept intact. Only this producer's outer administrative fields are reduced.
    let mut view = select(output, &["root", "matches", "completeness"]);
    workspace(invocation, output, &mut view);
    for name in ["partial_reasons", "warnings"] {
        if !output[name].as_array().unwrap().is_empty() {
            view.insert(name.to_owned(), output[name].clone());
        }
    }
    if output["refinement"] != "" {
        view.insert("refinement".to_owned(), output["refinement"].clone());
    }
    let mut skipped = Map::new();
    for name in ["skipped_binary_files", "skipped_unreadable_files"] {
        if output["stats"][name] != 0 {
            skipped.insert(name.to_owned(), output["stats"][name].clone());
        }
    }
    if !skipped.is_empty() {
        view.insert("stats".to_owned(), Value::Object(skipped));
    }
    Value::Object(view)
}

#[cfg(test)]
mod tests;
