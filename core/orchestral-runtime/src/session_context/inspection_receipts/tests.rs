use super::*;
use orchestral_core::agent_session::SessionSourceRange;
use orchestral_core::model_protocol::{ModelMessage, ModelRole, ModelToolCallId};
use serde_json::json;

fn call(id: &str, path: &str) -> ModelContent {
    ModelContent::ToolCall {
        call_id: ModelToolCallId::new(id),
        name: "file_read".to_owned(),
        arguments: json!({"path":path,"offset":1}),
        extensions: Default::default(),
    }
}

fn result(id: &str, path: &str, revision: &str, start: u64, end: u64, eof: bool) -> ModelContent {
    ModelContent::ToolResult {
        call_id: ModelToolCallId::new(id),
        result: json!({
            "workspace":"/workspace", "path":path,"revision":revision,
            "start_line":start,"end_line":end,"next_offset":end+1,
            "content":if end == 0 {String::new()} else {"code\n".repeat((end-start+1) as usize)},
            "eof":eof,"truncated":!eof,
            "truncation_reasons":if eof {vec![]} else {vec!["line_limit"]},
            "truncated_line_numbers":[],
        }),
        is_error: false,
    }
}

fn group(seq: u64, calls: Vec<ModelContent>, results: Vec<ModelContent>) -> SessionCompactionGroup {
    SessionCompactionGroup {
        source: SessionSourceRange {
            first_session_seq: seq,
            last_session_seq: seq,
        },
        messages: vec![
            ModelMessage {
                role: ModelRole::Assistant,
                content: calls,
            },
            ModelMessage {
                role: ModelRole::Tool,
                content: results,
            },
        ],
    }
}

fn page(
    seq: u64,
    path: &str,
    revision: &str,
    start: u64,
    end: u64,
    eof: bool,
) -> SessionCompactionGroup {
    group(
        seq,
        vec![call("read", path)],
        vec![result("read", path, revision, start, end, eof)],
    )
}

fn block(groups: &[SessionCompactionGroup], budget: usize) -> Value {
    serde_json::from_str(&inspection_receipts(groups, budget).unwrap().unwrap()).unwrap()
}

#[test]
fn artifact_receipts_keep_exact_references_and_valid_page_cursors() {
    let digest = orchestral_core::agent_protocol::wire::Digest::sha256("artifact bytes");
    let mut produced = page(1, "large.rs", "old", 1, 3, true);
    if let ModelContent::ToolResult { result, .. } = &mut produced.messages[1].content[0] {
        *result = json!({"kind":"artifact","artifact":{
            "artifact_ref":"opaque-artifact-reference", "digest":digest,
        }});
    }
    let mut first_page = group(
        2,
        vec![ModelContent::ToolCall {
            call_id: ModelToolCallId::new("page"),
            name: "artifact_read".to_owned(),
            arguments: json!({"artifact_ref":"opaque-artifact-reference"}),
            extensions: Default::default(),
        }],
        vec![ModelContent::ToolResult {
            call_id: ModelToolCallId::new("page"),
            result: json!({"artifact_ref":"opaque-artifact-reference","digest":digest,
                "offset":0,"next_offset":6,"bytes_read":6,"total_bytes":12,
                "complete":false,"content":"你好"}),
            is_error: false,
        }],
    );
    let mut second_page = first_page.clone();
    second_page.source = super::super::single_range(3);
    if let ModelContent::ToolResult { result, .. } = &mut second_page.messages[1].content[0] {
        result["offset"] = json!(6);
        result["next_offset"] = json!(12);
        result["complete"] = json!(true);
    }
    let receipts = block(&[produced, first_page.clone(), second_page], 4096);
    let artifacts = receipts["artifact_observations_v1"].as_array().unwrap();
    assert_eq!(artifacts.len(), 2);
    let retained = artifacts
        .iter()
        .find(|item| item["status"] == "retained")
        .unwrap();
    assert_eq!(retained["artifact_ref"], "opaque-artifact-reference");
    assert_eq!(retained["digest"], json!(digest));
    let page = artifacts
        .iter()
        .find(|item| item["status"] == "page")
        .unwrap();
    assert_eq!(page["next_offset"], 12);
    assert_eq!(page["complete"], true);
    assert_eq!(page["sources"], json!([[2, 3]]));

    if let ModelContent::ToolResult { result, .. } = &mut first_page.messages[1].content[0] {
        result["bytes_read"] = json!(2); // Characters are not UTF-8 byte counts.
    }
    let invalid = block(&[first_page], 4096);
    assert!(invalid.get("artifact_observations_v1").is_none());
    assert_eq!(invalid["omitted_sources"], json!([[2, 2]]));
}

#[test]
fn receipt_merges_pages_only_within_the_same_workspace_path_and_revision() {
    let mut other_workspace = page(5, "a.rs", "new", 1, 3, true);
    if let ModelContent::ToolResult { result, .. } = &mut other_workspace.messages[1].content[0] {
        result["workspace"] = json!("/other");
    }
    let groups = vec![
        page(1, "a.rs", "old", 1, 4, false),
        page(2, "a.rs", "old", 5, 8, true),
        page(3, "a.rs", "new", 1, 2, false),
        page(4, "a.rs", "new", 3, 3, true),
        other_workspace,
    ];
    let value = block(&groups, 4096);
    let receipts = value["file_inspections_v1"].as_array().unwrap();
    assert_eq!(receipts.len(), 3);
    let old = receipts.iter().find(|r| r["revision"] == "old").unwrap();
    assert_eq!(old["ranges"], json!([[1, 8]]));
    assert_eq!(old["eof_line"], 8);
    assert_eq!(old["next_offset"], 9);
    assert_eq!(old["sources"], json!([[1, 2]]));
    let new = receipts
        .iter()
        .find(|r| r["revision"] == "new" && r["workspace"] == "/workspace")
        .unwrap();
    assert_eq!(new["ranges"], json!([[1, 3]]));
    assert_eq!(new["eof_line"], 3);
}

#[test]
fn receipt_pairs_parallel_calls_by_id_and_does_not_turn_an_error_into_a_read() {
    let groups = vec![group(
        10,
        vec![call("good", "exists.rs"), call("bad", "absent.rs")],
        vec![
            ModelContent::ToolResult {
                call_id: ModelToolCallId::new("bad"),
                result: json!({"code":"workspace_path_unavailable","status":"rejected"}),
                is_error: true,
            },
            result("good", "exists.rs", "rev", 1, 3, true),
        ],
    )];
    let value = block(&groups, 4096);
    let receipts = value["file_inspections_v1"].as_array().unwrap();
    assert_eq!(receipts.len(), 2);
    let error = receipts.iter().find(|r| r["path"] == "absent.rs").unwrap();
    assert_eq!(error["status"], "failed");
    assert_eq!(error["code"], "workspace_path_unavailable");
    assert!(error.get("ranges").is_none());
    assert!(error.get("revision").is_none());
    assert_eq!(
        receipts.iter().find(|r| r["path"] == "exists.rs").unwrap()["eof_line"],
        3
    );
}

#[test]
fn missing_legacy_metadata_and_unmatched_results_are_explicitly_recoverable() {
    let legacy = group(
        3,
        vec![call("legacy", "a.rs")],
        vec![ModelContent::ToolResult {
            call_id: ModelToolCallId::new("legacy"),
            result: json!({"path":"a.rs","content":"text"}),
            is_error: false,
        }],
    );
    let unmatched = group(
        4,
        vec![call("requested", "b.rs")],
        vec![result("other", "b.rs", "rev", 1, 4, true)],
    );
    let value = block(&[legacy, unmatched], 256);
    assert_eq!(value["file_inspections_v1"], json!([]));
    assert_eq!(value["omitted_sources"], json!([[3, 4]]));
    assert_eq!(value["omitted_receipts"], 2);
}

#[test]
fn empty_files_and_clipped_lines_do_not_invent_read_coverage() {
    let mut clipped = page(2, "long.rs", "rev", 1, 3, true);
    if let ModelContent::ToolResult { result, .. } = &mut clipped.messages[1].content[0] {
        result["truncated"] = json!(true);
        result["truncation_reasons"] = json!(["line_too_long"]);
        result["truncated_line_numbers"] = json!([2]);
    }
    let mut byte_clipped = page(3, "bytes.rs", "rev", 1, 1, true);
    if let ModelContent::ToolResult { result, .. } = &mut byte_clipped.messages[1].content[0] {
        result["truncated"] = json!(true);
        result["truncation_reasons"] = json!(["byte_limit"]);
    }
    let value = block(
        &[
            page(1, "empty.rs", "rev", 1, 0, true),
            clipped,
            byte_clipped,
        ],
        4096,
    );
    let rows = value["file_inspections_v1"].as_array().unwrap();
    let empty = rows.iter().find(|r| r["path"] == "empty.rs").unwrap();
    assert_eq!(empty["ranges"], json!([]));
    assert_eq!(empty["eof_line"], 0);
    assert_eq!(empty["next_offset"], 1);
    assert_eq!(
        rows.iter().find(|r| r["path"] == "long.rs").unwrap()["ranges"],
        json!([[1, 1], [3, 3]])
    );
    assert_eq!(
        rows.iter().find(|r| r["path"] == "bytes.rs").unwrap()["ranges"],
        json!([])
    );
}

#[test]
fn bounded_receipts_are_valid_json_with_explicit_sources_for_every_omission() {
    let groups = (1..=20)
        .map(|seq| {
            page(
                seq * 2,
                &format!("nested/{seq}/source.rs"),
                "rev",
                1,
                20,
                true,
            )
        })
        .collect::<Vec<_>>();
    let text = inspection_receipts(&groups, 450).unwrap().unwrap();
    assert!(text.chars().count() <= 450);
    let value: Value = serde_json::from_str(&text).unwrap();
    assert!(value["omitted_receipts"].as_u64().unwrap() > 0);
    let receipts = value["file_inspections_v1"].as_array().unwrap();
    let recover = value["omitted_sources"].as_array().unwrap();
    for seq in (2..=40).step_by(2) {
        let present = receipts
            .iter()
            .flat_map(|r| r["sources"].as_array().unwrap())
            .chain(recover)
            .any(|r| r[0].as_u64().unwrap() <= seq && r[1].as_u64().unwrap() >= seq);
        assert!(present, "source {seq} silently lost");
    }
    assert!(inspection_receipts(&groups, 1).is_err());
}

#[test]
fn rebuilding_from_original_groups_is_stable_and_does_not_parse_past_prose() {
    let groups = vec![
        page(2, "source.rs", "rev", 1, 4, false),
        page(3, "source.rs", "rev", 5, 8, true),
    ];
    let first = inspection_receipts(&groups, 4096).unwrap();
    let mut reordered = groups.clone();
    reordered.reverse();
    assert_eq!(inspection_receipts(&reordered, 4096).unwrap(), first);
    let prose = SessionCompactionGroup {
        source: SessionSourceRange {
            first_session_seq: 2,
            last_session_seq: 3,
        },
        messages: vec![ModelMessage::text(ModelRole::Assistant, first.unwrap())],
    };
    assert!(inspection_receipts(&[prose], 4096).unwrap().is_none());
}
