use super::*;

fn batch(edits: &[(&str, &str)]) -> Value {
    json!({
        "path": "source.rs",
        "edits": edits.iter().map(|(old, new)| json!({
            "old_text": old, "new_text": new
        })).collect::<Vec<_>>()
    })
}

#[tokio::test]
async fn batch_edits_match_original_bytes_and_commit_one_file_in_any_order() {
    let before = "α=left;\r\nβ=right;\r\nremove;\r\n";
    let after = "α=right!;\r\nβ=left;\r\n";
    let edits = [("left", "right!"), ("right", "left"), ("remove;\r\n", "")];
    for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
        let workspace = Workspace::new();
        let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
        let runtime = runtime(&workspace, &[], &policy);
        std::fs::write(workspace.file(), before).unwrap();
        let result = invoke(&runtime, &policy, "batch", batch(&order.map(|i| edits[i]))).await;
        let output = completed(&result);
        assert_eq!(std::fs::read(workspace.file()).unwrap(), after.as_bytes());
        assert_eq!(output["changed_files"], 1);
        assert_eq!(output["changes"].as_array().unwrap().len(), 1);
        assert_eq!(
            output["changes"][0]["before_digest"],
            json!(Digest::sha256(before.as_bytes()))
        );
        assert_eq!(
            output["changes"][0]["after_digest"],
            json!(Digest::sha256(after.as_bytes()))
        );
        assert_eq!(std::fs::read_dir(&workspace.0).unwrap().count(), 1);
    }
}

#[tokio::test]
async fn a_later_invalid_match_never_commits_an_earlier_edit() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    for (index, (source, edits, expected_code)) in [
        (
            "first second",
            [("first", "changed"), ("missing", "new")],
            "file_edit_no_match",
        ),
        (
            "first same same",
            [("first", "changed"), ("same", "new")],
            "file_edit_ambiguous",
        ),
        (
            "first second",
            [("first", "created"), ("created", "new")],
            "file_edit_no_match",
        ),
        (
            "first second",
            [("first second", "changed"), ("second", "new")],
            "file_edit_overlap",
        ),
        (
            "first second",
            [("first", "changed"), ("first", "new")],
            "file_edit_overlap",
        ),
        (
            "αβγ",
            [("αβ", "changed"), ("βγ", "new")],
            "file_edit_overlap",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        std::fs::write(workspace.file(), source).unwrap();
        let result = invoke(
            &runtime,
            &policy,
            &format!("invalid-match-{index}"),
            batch(&edits),
        )
        .await;
        assert!(
            matches!(&result, GuardedToolResult::Outcome {
            outcome: ToolOutcome::Rejected {code, ..}, ..
        } if code == expected_code),
            "{result:?}"
        );
        assert_eq!(std::fs::read(workspace.file()).unwrap(), source.as_bytes());
        assert_eq!(std::fs::read_dir(&workspace.0).unwrap().count(), 1);
    }
}

#[tokio::test]
async fn batch_edits_allow_adjacent_deletions_and_combined_noops() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    std::fs::write(workspace.file(), "αβtail").unwrap();
    let noop = invoke(&runtime, &policy, "noop", batch(&[("α", "α"), ("β", "β")])).await;
    assert_eq!(completed(&noop)["changed_files"], 0);
    assert_eq!(completed(&noop)["changes"], json!([]));
    let deleted = invoke(&runtime, &policy, "delete", batch(&[("β", ""), ("α", "")])).await;
    assert_eq!(completed(&deleted)["changed_files"], 1);
    assert_eq!(std::fs::read_to_string(workspace.file()).unwrap(), "tail");
}

#[tokio::test]
async fn batch_edits_reject_mixed_forms_and_invalid_entries() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    std::fs::write(workspace.file(), "original").unwrap();
    let mut invalid = vec![
        batch(&[]),
        json!({"path":"source.rs","edits":null}),
        json!({"path":"source.rs","edits":[null]}),
        json!({"path":"source.rs","edits":[{"old_text":"original"}]}),
        json!({"path":"source.rs","edits":[{"old_text":"original","new_text":"new","path":"other"}]}),
        batch(&[("", "new")]),
        batch(&[("original", "bad\0text")]),
        batch(&[("bad\0text", "new")]),
    ];
    for fields in [
        vec!["old_text"],
        vec!["new_text"],
        vec!["old_text", "new_text"],
    ] {
        let mut value = batch(&[("original", "changed")]);
        for field in fields {
            value[field] = json!("original");
        }
        invalid.push(value);
    }
    for (index, arguments) in invalid.into_iter().enumerate() {
        let result = invoke(&runtime, &policy, &format!("bad-batch-{index}"), arguments).await;
        assert_rejected(&result);
        assert_eq!(
            std::fs::read_to_string(workspace.file()).unwrap(),
            "original"
        );
    }
}

#[tokio::test]
async fn batch_capacity_uses_the_combined_result_without_intermediate_writes() {
    const FILE_LIMIT: usize = 8 * 1024 * 1024;
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&workspace, &[], &policy);
    let mut before = vec![b'a'; FILE_LIMIT];
    before[..2].copy_from_slice(b"xy");
    std::fs::write(workspace.file(), &before).unwrap();
    // Growing the first span alone exceeds capacity, but deleting the adjacent
    // span in the same transaction keeps the actual final file at the limit.
    completed(
        &invoke(
            &runtime,
            &policy,
            "balanced",
            batch(&[("x", "xx"), ("y", "")]),
        )
        .await,
    );
    let after = std::fs::read(workspace.file()).unwrap();
    assert_eq!(after.len(), FILE_LIMIT);
    assert_eq!(&after[..2], b"xx");
    assert_eq!(&after[2..], &before[2..]);

    std::fs::write(workspace.file(), &before).unwrap();
    let oversized = invoke(
        &runtime,
        &policy,
        "oversized",
        batch(&[("x", "xx"), ("y", "yy")]),
    )
    .await;
    assert_rejected(&oversized);
    assert_eq!(std::fs::read(workspace.file()).unwrap(), before);
    let replacement = "z".repeat(FILE_LIMIT / 2 + 1);
    let oversized = invoke(
        &runtime,
        &policy,
        "oversized-text",
        batch(&[("x", &replacement), ("y", &replacement)]),
    )
    .await;
    assert_rejected(&oversized);
    assert_eq!(std::fs::read(workspace.file()).unwrap(), before);
}

#[tokio::test]
async fn batch_replay_recovers_one_effect_and_binds_every_replacement() {
    let workspace = Workspace::new();
    let policy = bounds(&[&workspace.0], ApprovalPolicy::NotRequired);
    let journal = Arc::new(InMemoryToolEffectJournalStore::default());
    let first_runtime = runtime_with_journal(&workspace, &[], &policy, journal.clone());
    std::fs::write(workspace.file(), "left right").unwrap();
    let arguments = batch(&[("left", "left!"), ("right", "right!")]);
    let first = invoke(&first_runtime, &policy, "same-batch", arguments.clone()).await;
    completed(&first);
    drop(first_runtime);
    let resumed = runtime_with_journal(&workspace, &[], &policy, journal);
    let replay = invoke(&resumed, &policy, "same-batch", arguments.clone()).await;
    assert_eq!(completed(&first), completed(&replay));
    assert!(matches!(
        replay,
        GuardedToolResult::Outcome { cached: true, .. }
    ));
    let mut different = arguments;
    different["edits"][1]["new_text"] = json!("right?");
    let conflict = invoke(&resumed, &policy, "same-batch", different).await;
    assert!(matches!(conflict, GuardedToolResult::Outcome {
        outcome: ToolOutcome::Rejected {ref code, ..}, ..
    } if code == "call_identity_conflict"));
    assert_eq!(
        std::fs::read_to_string(workspace.file()).unwrap(),
        "left! right!"
    );
}

#[tokio::test]
async fn batch_edits_remain_bound_to_host_selected_workspaces() {
    let primary = Workspace::new();
    let additional = Workspace::new();
    for workspace in [&primary, &additional] {
        std::fs::write(workspace.file(), "left right").unwrap();
    }
    let policy = bounds(&[&primary.0, &additional.0], ApprovalPolicy::NotRequired);
    let runtime = runtime(&primary, &[&additional.0], &policy);
    let mut arguments = batch(&[("left", "first"), ("right", "second")]);
    arguments["workspace"] = json!(additional.0.to_string_lossy());
    let mut denied = policy.clone();
    denied.allowed_effects.remove(&EffectScope::FilesystemWrite);
    assert_rejected(&invoke(&runtime, &denied, "denied-batch", arguments.clone()).await);
    assert_eq!(
        std::fs::read_to_string(additional.file()).unwrap(),
        "left right"
    );
    completed(&invoke(&runtime, &policy, "selected-batch", arguments).await);
    assert_eq!(
        std::fs::read_to_string(additional.file()).unwrap(),
        "first second"
    );
    assert_eq!(
        std::fs::read_to_string(primary.file()).unwrap(),
        "left right"
    );
}
