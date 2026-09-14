use super::*;
use orchestral_core::agent_session::InMemoryAgentSessionJournalStore;

struct Meter(bool);

impl ModelTokenMeter for Meter {
    fn supports_observed_prefix_estimation(&self) -> bool {
        self.0
    }
    fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
        ModelTokenMeterDescriptor {
            strategy: "fixture/full-overhead".to_owned(),
            version: "1".to_owned(),
            accounting: ModelTokenAccounting::ConservativeUpperBound,
            config_digest: Digest::sha256([u8::from(self.0)]),
        }
    }
    fn count_request_input(
        &self,
        _: &[ModelMessage],
        _: &[ModelToolDefinition],
    ) -> Result<u64, ModelError> {
        Ok(20_000)
    }
    fn estimate_context_input(
        &self,
        messages: &[ModelMessage],
        tools: &[ModelToolDefinition],
    ) -> Result<ModelContextEstimate, ModelError> {
        Ok(ModelContextEstimate {
            tokens: 1_000 + messages.len() as u64 * 100 + tools.len() as u64 * 50,
            accounting: ModelTokenAccounting::Estimated,
        })
    }
}

fn new_engine(supported: bool) -> AgentSessionContextEngine {
    AgentSessionContextEngine::new(
        Arc::new(InMemoryAgentSessionJournalStore::default()),
        Arc::new(Meter(supported)),
    )
}

fn tools() -> Vec<ModelToolDefinition> {
    vec![ModelToolDefinition {
        name: "inspect".to_owned(),
        description: "Inspect a record".to_owned(),
        input_schema: serde_json::json!({"type":"object"}),
    }]
}

fn anchor(
    engine: &AgentSessionContextEngine,
    messages: &[ModelMessage],
    tools: &[ModelToolDefinition],
    input_tokens: u64,
) -> ObservedPrefixAnchor {
    ObservedPrefixAnchor {
        run_id: RunId::new("run"),
        config_digest: Digest::sha256("config"),
        source_request_id: ModelRequestId::new("completed-request"),
        input: engine
            .planning_trace(messages, tools, None)
            .unwrap()
            .unwrap()
            .input,
        observed_input_tokens: input_tokens,
    }
}

fn estimate(
    engine: &AgentSessionContextEngine,
    anchor: &ObservedPrefixAnchor,
    messages: &[ModelMessage],
    tools: &[ModelToolDefinition],
) -> u64 {
    engine
        .with_observed_prefix(&RunId::new("run"), &Digest::sha256("config"), Some(anchor))
        .context_input_tokens(messages, tools, ContextTokenPolicy::Planning)
        .unwrap()
}

fn append_exchange(messages: &mut Vec<ModelMessage>) {
    messages.push(ModelMessage::text(ModelRole::Assistant, "inspect"));
    messages.push(ModelMessage {
        role: ModelRole::Tool,
        content: vec![ModelContent::ToolResult {
            call_id: orchestral_core::model_protocol::ModelToolCallId::new("call"),
            result: serde_json::json!({"text":"record"}),
            is_error: false,
        }],
    });
}

#[test]
fn three_rounds_use_raw_prior_estimate_without_repeated_calibration() {
    let engine = new_engine(true);
    let tools = tools();
    let mut messages = vec![ModelMessage::text(ModelRole::User, "review")];
    let first = anchor(&engine, &messages, &tools, 200);
    append_exchange(&mut messages);
    assert_eq!(estimate(&engine, &first, &messages, &tools), 400);
    let trace = engine
        .planning_trace(&messages, &tools, Some(&first))
        .unwrap()
        .unwrap();
    assert_eq!(trace.input.raw_estimate_tokens, 1_350);
    let second = ObservedPrefixAnchor {
        input: trace.input,
        observed_input_tokens: 350,
        ..first.clone()
    };
    append_exchange(&mut messages);
    assert_eq!(estimate(&engine, &second, &messages, &tools), 550);
    assert_eq!(
        engine
            .with_observed_prefix(&RunId::new("run"), &Digest::sha256("config"), Some(&second))
            .context_input_tokens(&messages, &tools, ContextTokenPolicy::UpperBound)
            .unwrap(),
        20_000
    );
    assert_eq!(
        engine
            .token_meter
            .estimate_context_input(&messages, &tools)
            .unwrap()
            .tokens,
        1_550
    );
}

#[test]
fn changed_scope_prefix_tools_or_new_user_falls_back_to_full_estimate() {
    let engine = new_engine(true);
    let tools = tools();
    let original = vec![ModelMessage::text(ModelRole::User, "review")];
    let first = anchor(&engine, &original, &tools, 200);
    for mutation in 0..6 {
        let mut messages = original.clone();
        append_exchange(&mut messages);
        let mut supplied = first.clone();
        let mut changed_tools = tools.clone();
        match mutation {
            0 => supplied.run_id = RunId::new("other-run"),
            1 => supplied.config_digest = Digest::sha256("other-config"),
            2 => messages[0] = ModelMessage::text(ModelRole::User, "different request"),
            3 => changed_tools[0].description.push_str(" differently"),
            4 => messages.push(ModelMessage::text(ModelRole::User, "steer")),
            _ => messages.push(ModelMessage::text(ModelRole::System, "loaded instruction")),
        }
        let raw = engine
            .token_meter
            .estimate_context_input(&messages, &changed_tools)
            .unwrap()
            .tokens;
        assert_eq!(estimate(&engine, &supplied, &messages, &changed_tools), raw);
    }
    assert_eq!(estimate(&engine, &first, &[], &tools), 1_050);
    let unsupported = new_engine(false);
    assert_eq!(estimate(&unsupported, &first, &original, &tools), 1_150);
}

#[test]
fn non_monotonic_or_overflowing_estimate_cannot_replace_the_hard_bound() {
    let engine = new_engine(true);
    let messages = vec![ModelMessage::text(ModelRole::User, "review")];
    let tools = tools();
    let mut first = anchor(&engine, &messages, &tools, 200);
    first.input.raw_estimate_tokens = 2_000;
    assert_eq!(estimate(&engine, &first, &messages, &tools), 1_150);
    first.input.raw_estimate_tokens = 0;
    first.observed_input_tokens = u64::MAX;
    assert_eq!(estimate(&engine, &first, &messages, &tools), 1_150);
    first.observed_input_tokens = 20_000;
    assert_eq!(estimate(&engine, &first, &messages, &tools), 1_150);
}

#[test]
fn completed_request_after_new_user_can_establish_a_fresh_anchor() {
    let engine = new_engine(true);
    let tools = tools();
    let mut messages = vec![ModelMessage::text(ModelRole::User, "review")];
    let old = anchor(&engine, &messages, &tools, 200);
    messages.push(ModelMessage::text(ModelRole::User, "changed request"));
    assert_eq!(estimate(&engine, &old, &messages, &tools), 1_250);
    let fresh = anchor(&engine, &messages, &tools, 420);
    append_exchange(&mut messages);
    assert_eq!(estimate(&engine, &fresh, &messages, &tools), 620);
}
