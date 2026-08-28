mod run_limit_tests {
    use super::super::*;
    use orchestral_core::agent_protocol::wire::{
        AgentRunEnvelope, AgentSessionId, ProviderBindingRef, RunLimits,
    };

    fn request() -> AgentStartRequest {
        let descriptor = AgentDescriptorEnvelope::seal(AgentDescriptor {
            provider_id: AgentProviderId::new("test/provider"),
            agent_id: AgentId::new("test/agent"),
            supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
            accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
            capabilities: AgentCapabilities {
                session_reuse: true,
                structured_output: false,
                controls: ControlCapabilities {
                    steer: true,
                    cancel: CancelSupport::Confirmed,
                    recover: true,
                },
                pending_request_kinds: BTreeSet::new(),
                supported_limits: BTreeSet::from([
                    RunLimitKind::Deadline,
                    RunLimitKind::ModelSteps,
                    RunLimitKind::ToolCalls,
                    RunLimitKind::InputTokens,
                    RunLimitKind::OutputTokens,
                    RunLimitKind::Cost,
                ]),
                resources: Vec::new(),
                effect_mediation: EffectMediation::None,
            },
            extensions: Default::default(),
        })
        .expect("test descriptor is valid");
        let run = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("limit-session"),
            RunId::new("limit-run"),
            vec![Content::text("bounded request")],
        )
        .expect("test Run is valid");
        AgentStartRequest::new(run, ProviderBindingRef::new("limit-binding"), &descriptor)
            .expect("test start is valid")
    }

    #[test]
    fn one_thousand_boundaries_per_run_limit_never_reserve_past_the_ceiling() {
        let mut request = request();
        let mut config = GenericAgentConfig::new("test/provider", "test/agent");
        config.reserved_output_tokens = 10_000;
        config.model_cost_policy = Some(
            ModelCostPolicy::new("USD", 1_000_000, 1_000_000)
                .expect("linear test pricing is valid"),
        );

        for boundary in 1_u64..=1_000 {
            request.run.spec.limits = RunLimits {
                max_model_steps: Some(boundary),
                ..RunLimits::default()
            };
            assert_eq!(
                continuation_limit(
                    &config,
                    &request,
                    &ModelUsage::default(),
                    boundary.saturating_sub(1),
                    Some(boundary),
                ),
                None
            );
            assert_eq!(
                continuation_limit(
                    &config,
                    &request,
                    &ModelUsage::default(),
                    boundary,
                    Some(boundary),
                ),
                Some(RunLimitKind::ModelSteps)
            );

            assert_eq!(
                reserve_tool_call(boundary - 1, Some(boundary)),
                Ok(boundary)
            );
            assert_eq!(
                reserve_tool_call(boundary, Some(boundary)),
                Err(RunLimitKind::ToolCalls)
            );

            let now = 1_000_000_i64;
            assert_eq!(
                deadline_delay_ms(now + boundary as i64, now),
                Some(boundary)
            );
            assert_eq!(deadline_delay_ms(now, now), None);

            let token_limit = boundary.saturating_mul(2);
            request.run.spec.limits = RunLimits {
                max_input_tokens: Some(token_limit),
                ..RunLimits::default()
            };
            let previous = ModelUsage {
                input_tokens: Some(boundary),
                output_tokens: None,
            };
            assert_eq!(
                remaining_input_tokens(&request, &previous),
                Ok(Some(boundary))
            );
            assert!(model_dispatch_budget(&config, &request, &previous, boundary, 1).is_ok());
            assert_eq!(
                model_dispatch_budget(&config, &request, &previous, boundary.saturating_add(1), 1,),
                Err(RunLimitKind::InputTokens)
            );

            request.run.spec.limits = RunLimits {
                max_output_tokens: Some(token_limit),
                ..RunLimits::default()
            };
            let previous = ModelUsage {
                input_tokens: None,
                output_tokens: Some(boundary),
            };
            assert_eq!(
                output_reserve_tokens(&config, &request, &previous),
                Ok(boundary)
            );
            let dispatch = model_dispatch_budget(&config, &request, &previous, 1, boundary)
                .expect("remaining output budget is reservable");
            assert_eq!(dispatch.max_output_tokens, Some(boundary));
            let exhausted = ModelUsage {
                input_tokens: None,
                output_tokens: Some(token_limit),
            };
            assert_eq!(
                output_reserve_tokens(&config, &request, &exhausted),
                Err(RunLimitKind::OutputTokens)
            );

            request.run.spec.limits = RunLimits {
                max_cost: Some(MoneyAmount {
                    currency: "USD".to_owned(),
                    microunits: boundary.saturating_mul(2).saturating_add(4),
                }),
                ..RunLimits::default()
            };
            let previous = ModelUsage {
                input_tokens: Some(boundary),
                output_tokens: Some(boundary),
            };
            let dispatch = model_dispatch_budget(&config, &request, &previous, 1, 16)
                .expect("cost ceiling admits the exact reservation");
            assert_eq!(dispatch.max_output_tokens, Some(3));
            assert!(validate_observed_usage(
                &config,
                &request,
                &previous,
                Some(&ModelUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(3),
                }),
                dispatch,
            )
            .is_ok());
            request.run.spec.limits.max_cost = Some(MoneyAmount {
                currency: "USD".to_owned(),
                microunits: boundary.saturating_mul(2),
            });
            assert_eq!(
                model_dispatch_budget(&config, &request, &previous, 1, 16),
                Err(RunLimitKind::Cost)
            );
        }
    }

    #[test]
    fn absent_continuation_ceilings_do_not_create_hidden_step_or_tool_limits() {
        let request = request();
        let config = GenericAgentConfig::new("test/provider", "test/agent");

        assert_eq!(config.continuation, ContinuationPolicy::default());
        assert_eq!(
            continuation_limit(&config, &request, &ModelUsage::default(), 10_000, None,),
            None
        );
        assert_eq!(reserve_tool_call(10_000, None), Ok(10_001));
    }

    #[test]
    fn host_and_run_continuation_limits_intersect_without_implicit_defaults() {
        let policy = ContinuationPolicy {
            max_model_steps: Some(40),
            max_tool_calls: Some(80),
        };

        assert_eq!(policy.effective_model_steps(None), Some(40));
        assert_eq!(policy.effective_model_steps(Some(60)), Some(40));
        assert_eq!(policy.effective_model_steps(Some(20)), Some(20));
        assert_eq!(policy.effective_tool_calls(None), Some(80));
        assert_eq!(policy.effective_tool_calls(Some(100)), Some(80));
        assert_eq!(policy.effective_tool_calls(Some(30)), Some(30));
    }
}
