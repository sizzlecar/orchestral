use super::*;
use orchestral_core::agent_protocol::wire::RunId;
use orchestral_core::tool_protocol::{ToolCallId, ToolId};

fn invocation(tool: &str) -> ToolInvocation {
    ToolInvocation {
        run_id: RunId::new("model-view-run"),
        call_id: ToolCallId::new("model-view-call"),
        tool_id: ToolId::new(tool),
        arguments: json!({"path": "source.rs"}),
    }
}

fn read_output() -> Value {
    let content = "\tlet text = \"读取\\n\";\r\n\nlast line  ";
    json!({
        "workspace": "/workspace", "path": "source.rs",
        "revision": Digest::sha256(b"revision"),
        "content": content, "content_digest": Digest::sha256(content.as_bytes()),
        "start_line": 1, "end_line": 3, "next_offset": 4, "eof": true,
        "truncated": false, "truncation_reasons": [], "truncated_line_numbers": [],
        "file_size_bytes": content.len(), "scanned_bytes": content.len(),
    })
}

#[test]
fn builtin_model_read_preserves_content_and_canonical_evidence() {
    use crate::tool_runtime::GuardedToolExecutor;
    use crate::tools::builtin::GuardedFileReadExecutor;
    let executor = GuardedFileReadExecutor::new(std::env::temp_dir()).unwrap();
    let invocation = invocation("orchestral/file_read/v1");
    let output = read_output();
    let before = output.clone();
    let view = executor.project_model_output(&invocation, &output);
    assert_eq!(
        view,
        json!({"path": output["path"], "content": output["content"]})
    );
    assert_eq!(
        view["content"].as_str().unwrap().as_bytes(),
        output["content"].as_str().unwrap().as_bytes()
    );
    assert_eq!(output, before);
    assert!(executor.complete_file_read(&invocation, &output).is_some());
    assert!(executor.complete_file_read(&invocation, &view).is_none());

    let mut malformed = output.clone();
    malformed["content_digest"] = json!("wrong");
    assert_eq!(
        executor.project_model_output(&invocation, &malformed),
        malformed
    );
}

#[test]
fn builtin_model_read_keeps_partial_ranges_truncation_and_workspace() {
    let mut invocation = invocation("orchestral/file_read/v1");
    invocation.arguments["workspace"] = json!("/workspace");
    for (start, eof, truncated) in [(2, true, false), (1, false, true), (1, true, true)] {
        let mut output = read_output();
        output["start_line"] = json!(start);
        output["eof"] = json!(eof);
        output["truncated"] = json!(truncated);
        if truncated {
            output["truncation_reasons"] = json!(["line_too_long"]);
            output["truncated_line_numbers"] = json!([2]);
        }
        let view = file_read(&invocation, &output);
        for field in [
            "workspace",
            "path",
            "content",
            "start_line",
            "end_line",
            "next_offset",
            "eof",
            "truncated",
        ] {
            assert_eq!(view[field], output[field], "{field}");
        }
        if truncated {
            assert_eq!(view["truncation_reasons"], output["truncation_reasons"]);
            assert_eq!(
                view["truncated_line_numbers"],
                output["truncated_line_numbers"]
            );
        }
    }
}

#[test]
fn builtin_model_exec_keeps_streams_exit_and_polling_facts() {
    let output = json!({
        "stdout": "\ttext  \r\n读取", "stderr": "warning\n", "exit_code": 0,
        "alive": false, "wall_time_seconds": 0.25, "truncated": false,
        "dropped_bytes": 0, "sandbox_backend": "macos_seatbelt",
    });
    assert_eq!(
        exec(&output),
        json!({"stdout": output["stdout"], "stderr": output["stderr"], "exit_code": 0})
    );
    let mut polled = output.clone();
    polled["sandbox_backend"] = json!("existing_session");
    assert_eq!(exec(&polled), exec(&output));
    let mut running = output.clone();
    running.as_object_mut().unwrap().remove("exit_code");
    running["alive"] = json!(true);
    running["session_id"] = json!(17);
    running["truncated"] = json!(true);
    running["dropped_bytes"] = json!(1024);
    running["sandbox_backend"] = json!("host-approved");
    let view = exec(&running);
    for field in [
        "stdout",
        "stderr",
        "alive",
        "session_id",
        "truncated",
        "dropped_bytes",
        "sandbox_backend",
    ] {
        assert_eq!(view[field], running[field], "{field}");
    }
    assert!(view.get("exit_code").is_none());
    let mut failure = output.clone();
    failure["exit_code"] = json!(1);
    assert_eq!(exec(&failure), failure);
    let mut inconsistent = running.clone();
    inconsistent["exit_code"] = json!(0);
    assert_eq!(exec(&inconsistent), inconsistent);
}

#[test]
fn builtin_model_mutation_keeps_changed_paths_and_no_change_confirmation() {
    let invocation = invocation("orchestral/apply_patch/v1");
    let output = json!({"workspace": "/workspace", "changed_files": 3, "changes": [
        {"operation": "add", "path": "a.rs", "bytes": 3, "after_digest": "a"},
        {"operation": "update", "path": "b.rs", "bytes": 4, "before_digest": "b", "after_digest": "c"},
        {"operation": "delete", "path": "c.rs", "bytes": 5, "before_digest": "d"},
    ]});
    let view = mutation(&invocation, &output);
    assert_eq!(view["changed_files"], 3);
    for (original, projected) in output["changes"]
        .as_array()
        .unwrap()
        .iter()
        .zip(view["changes"].as_array().unwrap())
    {
        for field in ["operation", "path", "bytes"] {
            assert_eq!(projected[field], original[field]);
        }
        assert!(projected.get("before_digest").is_none());
        assert!(projected.get("after_digest").is_none());
    }
    let unchanged = json!({"workspace": "/workspace", "changed_files": 0, "changes": []});
    assert_eq!(
        mutation(&invocation, &unchanged),
        json!({"changed_files": 0, "changes": []})
    );
    let mut inconsistent = output.clone();
    inconsistent["changed_files"] = json!(4);
    assert_eq!(mutation(&invocation, &inconsistent), inconsistent);
}

#[test]
fn builtin_model_search_keeps_matches_partial_reasons_and_warnings() {
    let invocation = invocation("orchestral/text_search/v1");
    let output = json!({
        "workspace": "/workspace", "root": ".", "matches": [{
            "path": "source.rs", "line_number": 12, "column": 2,
            "match_start_byte": 1, "match_end_byte": 4, "preview": "\t读取  \r\n",
            "preview_truncated": true, "context_before": ["before  "], "context_after": ["after\t"],
        }], "count": 1, "completeness": "partial", "partial_reasons": ["result_limit"],
        "refinement": "Narrow the search.", "warnings": ["Cannot read another file."],
        "stats": {"scanned_entries": 30, "considered_files": 20, "searched_files": 19,
            "scanned_bytes": 1000, "skipped_binary_files": 2, "skipped_unreadable_files": 1},
    });
    let view = search(&invocation, &output, true);
    for field in [
        "root",
        "matches",
        "completeness",
        "partial_reasons",
        "refinement",
        "warnings",
    ] {
        assert_eq!(view[field], output[field], "{field}");
    }
    assert_eq!(
        view["stats"],
        json!({"skipped_binary_files": 2, "skipped_unreadable_files": 1})
    );
    let mut paths = output.clone();
    paths["matches"] = json!(["source.rs"]);
    assert_eq!(
        search(&invocation, &paths, false)["matches"],
        paths["matches"]
    );
    let mut changed_match = output.clone();
    changed_match["matches"][0]["warning"] = json!("new producer fact");
    assert_eq!(search(&invocation, &changed_match, true), changed_match);
}

#[test]
fn builtin_model_unknown_and_error_shapes_are_identity() {
    let invocation = invocation("orchestral/file_read/v1");
    let mut future_read = read_output();
    future_read["warning"] = json!("new producer fact");
    let mut incomplete_read = read_output();
    incomplete_read.as_object_mut().unwrap().remove("eof");
    for output in [
        future_read,
        incomplete_read,
        json!({"error": "denied", "content": "partial"}),
        json!(["unknown"]),
    ] {
        assert_eq!(file_read(&invocation, &output), output);
        assert_eq!(exec(&output), output);
        assert_eq!(mutation(&invocation, &output), output);
        assert_eq!(search(&invocation, &output, true), output);
    }
}
