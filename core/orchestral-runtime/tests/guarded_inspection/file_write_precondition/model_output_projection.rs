use super::*;

#[tokio::test]
async fn projected_complete_read_preserves_canonical_evidence_and_requires_exact_visible_content() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let runtime = fixture.runtime();
    let source = "\tlet λ = \"\\n\";\r\n```\r\n// no final newline";
    fs::write(fixture.file(), source).unwrap();
    let invocation = call(
        "run",
        "read",
        "orchestral/file_read/v3",
        json!({"path":"source.rs"}),
    );
    let canonical = fixture
        .read(&runtime, "run", "read", invocation.arguments.clone())
        .await;
    let visible = runtime
        .project_model_output(&invocation, &canonical)
        .unwrap();
    assert_ne!(visible, canonical);
    assert_eq!(
        visible["content"].as_str().unwrap().as_bytes(),
        source.as_bytes()
    );
    assert!(visible.get("revision").is_none());
    assert!(visible.get("content_digest").is_none());
    let key = ToolEffectKey::new(invocation.run_id.clone(), invocation.call_id.clone());
    let before = fixture.journal.load_effect(&key).await.unwrap();
    assert_eq!(
        runtime
            .recover_outcome(invocation, fixture.grant())
            .await
            .unwrap(),
        Some(ToolOutcome::Completed {
            output: ToolOutput::Inline(canonical.clone())
        })
    );

    let mut changed = visible.clone();
    changed["content"] = json!(source.replace("λ", "x"));
    let mut missing = visible.clone();
    missing.as_object_mut().unwrap().remove("content");
    for (index, (run, call_id, view, pending)) in [
        ("run", "read", changed, vec![]),
        ("run", "read", missing, vec![]),
        ("other-run", "read", visible.clone(), vec![]),
        ("run", "missing-read", visible.clone(), vec![]),
        ("run", "read", visible.clone(), vec!["read"]),
    ]
    .into_iter()
    .enumerate()
    {
        let observations = freeze(&runtime, run, &[tool_message(call_id, view)], &pending).await;
        rejected_with(
            fixture
                .write(
                    &runtime,
                    replace(run, &format!("invalid-{index}"), "wrong"),
                    &observations,
                )
                .await,
            "file_write_precondition_missing",
        );
    }
    assert_eq!(fs::read(fixture.file()).unwrap(), source.as_bytes());
    let observations = freeze(&runtime, "run", &[tool_message("read", visible)], &[]).await;
    completed(
        fixture
            .write(
                &runtime,
                replace("run", "write", "replacement"),
                &observations,
            )
            .await,
    );
    assert_eq!(fs::read(fixture.file()).unwrap(), b"replacement");
    assert_eq!(fixture.journal.load_effect(&key).await.unwrap(), before);
}

struct VersionedProjection(&'static str);

#[async_trait]
impl GuardedToolExecutor for VersionedProjection {
    fn project_model_output(&self, _: &ToolInvocation, output: &Value) -> Value {
        json!({"content": output["content"]})
    }

    fn model_output_contract(&self) -> Value {
        json!({"contract": self.0})
    }

    async fn execute(&self, _: GuardedToolExecution) -> ToolOutcome {
        unreachable!("contract inspection must not execute a tool")
    }
}

#[test]
fn model_output_contract_is_bound_without_changing_tool_schemas_or_executing_tools() {
    let fixture = Fixture::new(ApprovalPolicy::NotRequired, 64 * 1024);
    let build = |version| {
        let runtime = fixture.runtime();
        let mut descriptor = guarded_file_read_descriptor(ToolRestriction {
            bounds: fixture.policy.clone(),
        });
        descriptor.tool_id = ToolId::new("test/projected-output");
        descriptor.model_schema.name = "projected_output".to_owned();
        runtime
            .register(descriptor, Arc::new(VersionedProjection(version)))
            .unwrap();
        runtime
    };
    let first = build("test/model-output/v1");
    let second = build("test/model-output/v2");
    assert_eq!(
        first.model_tool_schemas().unwrap(),
        second.model_tool_schemas().unwrap()
    );
    assert_ne!(
        first.execution_contract_digest().unwrap(),
        second.execution_contract_digest().unwrap()
    );
    let invocation = call("run", "output", "test/projected-output", json!({}));
    let canonical = json!({"content":"model data", "audit":"retained"});
    assert_eq!(
        first.project_model_output(&invocation, &canonical).unwrap(),
        json!({"content":"model data"})
    );
    assert_eq!(canonical["audit"], "retained");
}
