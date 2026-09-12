use super::GenericAgentConfig;

mod context_planning_tests {
    use super::super::*;
    use orchestral_core::agent_protocol::wire::{
        AgentRunEnvelope, AgentSessionId, ProviderBindingRef, RunLimits,
    };
    use orchestral_core::model_protocol::{
        ModelCapabilities, ModelContextEstimate, ModelDescriptor, ModelStream, ModelTokenAccounting,
    };

    struct PlanningModel {
        starts: AtomicU64,
    }

    #[async_trait]
    impl ModelBackend for PlanningModel {
        fn descriptor(&self) -> ModelDescriptor {
            ModelDescriptor {
                backend_id: "planning-test".to_owned(),
                capabilities: ModelCapabilities {
                    streaming: true,
                    max_context_tokens: Some(1_024),
                    ..ModelCapabilities::default()
                },
                extensions: Default::default(),
            }
        }

        async fn start(
            &self,
            _request: ModelRequest,
            _cancellation: CancellationToken,
        ) -> Result<ModelStream, ModelError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Err(ModelError::protocol(
                "context preflight must not start a model",
            ))
        }
    }

    struct PlanningMeter;

    impl ModelTokenMeter for PlanningMeter {
        fn meter_descriptor(&self) -> ModelTokenMeterDescriptor {
            ModelTokenMeterDescriptor {
                strategy: "planning-boundary-test".to_owned(),
                version: "1".to_owned(),
                accounting: ModelTokenAccounting::ConservativeUpperBound,
                config_digest: Digest::sha256("planning-boundary-test/v1;bound=900;estimate=150"),
            }
        }

        fn count_request_input(
            &self,
            _messages: &[ModelMessage],
            _tools: &[ModelToolDefinition],
        ) -> Result<u64, ModelError> {
            Ok(900)
        }

        fn estimate_context_input(
            &self,
            _messages: &[ModelMessage],
            _tools: &[ModelToolDefinition],
        ) -> Result<ModelContextEstimate, ModelError> {
            Ok(ModelContextEstimate {
                tokens: 150,
                accounting: ModelTokenAccounting::Estimated,
            })
        }
    }

    fn fixture() -> (
        InternalGenericAgentProvider,
        Arc<InMemoryAgentSessionJournalStore>,
        Arc<PlanningModel>,
    ) {
        let journal = Arc::new(InMemoryAgentSessionJournalStore::default());
        let backend = Arc::new(PlanningModel {
            starts: AtomicU64::new(0),
        });
        let mut config = GenericAgentConfig::new("test/provider", "test/agent");
        config.max_context_tokens = 1_024;
        config.reserved_output_tokens = 524;
        config.model_cost_policy = Some(ModelCostPolicy::new("USD", 1_000_000, 1_000_000).unwrap());
        let provider = InternalGenericAgentProvider::new_with_session_journal(
            backend.clone(),
            config,
            journal.clone(),
            Arc::new(PlanningMeter),
        )
        .unwrap();
        (provider, journal, backend)
    }

    fn request(provider: &InternalGenericAgentProvider, limits: RunLimits) -> AgentStartRequest {
        let mut spec = AgentRunEnvelope::new(
            AGENT_PROTOCOL_V1,
            AgentSessionId::new("planning-session"),
            RunId::new("planning-run"),
            vec![Content::text("review the change")],
        )
        .unwrap()
        .spec;
        spec.limits = limits;
        AgentStartRequest::new(
            AgentRunEnvelope::seal(spec).unwrap(),
            ProviderBindingRef::new("planning-binding"),
            &provider.describe(),
        )
        .unwrap()
    }

    async fn initial_projection(
        provider: &InternalGenericAgentProvider,
        request: &AgentStartRequest,
    ) -> Result<SessionContextProjection, SessionContextError> {
        project_model_context(
            &provider.inner,
            request,
            &[],
            None,
            Some(ModelMessage::text(ModelRole::User, "review the change")),
            None,
            ModelContextBudget::default(),
        )
        .await
    }

    #[tokio::test]
    async fn context_planning_keeps_hard_dispatch_and_observed_usage_bound() {
        let (provider, _, backend) = fixture();
        let request = request(&provider, RunLimits::default());
        let projection = initial_projection(&provider, &request).await.unwrap();
        assert_eq!(projection.input_budget_tokens, 500);
        assert_eq!(projection.used_input_tokens, 900);
        assert_eq!(
            projection.context_estimate,
            Some(ModelContextEstimate {
                tokens: 150,
                accounting: ModelTokenAccounting::Estimated,
            })
        );
        let trace = model_context_trace(&projection, provider.inner.config.history_limit);
        assert_eq!(trace.context_estimate, projection.context_estimate);
        assert_eq!(trace.used_input_tokens, 900);

        let dispatch = model_dispatch_budget(
            &provider.inner.config,
            &request,
            &ModelUsage::default(),
            projection.used_input_tokens,
            provider.inner.config.reserved_output_tokens,
        )
        .unwrap();
        assert_eq!(dispatch.projected_input_tokens, 900);
        let observed = |input_tokens| ModelUsage {
            input_tokens: Some(input_tokens),
            output_tokens: Some(1),
        };
        assert!(validate_observed_usage(
            &provider.inner.config,
            &request,
            &ModelUsage::default(),
            Some(&observed(700)),
            dispatch,
        )
        .is_ok());
        let error = validate_observed_usage(
            &provider.inner.config,
            &request,
            &ModelUsage::default(),
            Some(&observed(901)),
            dispatch,
        )
        .unwrap_err();
        assert_eq!(error.code, "model_input_usage_exceeded_reservation");
        assert_eq!(backend.starts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn context_planning_cannot_relax_input_or_cost_limited_projection() {
        for limits in [
            RunLimits {
                max_input_tokens: Some(500),
                ..RunLimits::default()
            },
            RunLimits {
                max_cost: Some(MoneyAmount {
                    currency: "USD".to_owned(),
                    microunits: 10_000,
                }),
                ..RunLimits::default()
            },
        ] {
            let (provider, _, backend) = fixture();
            let request = request(&provider, limits);
            assert!(matches!(
                initial_projection(&provider, &request).await,
                Err(SessionContextError::ContextOverflow {
                    used: 900,
                    budget: 500,
                })
            ));
            assert_eq!(backend.starts.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn context_planning_cursor_replay_preserves_request_without_new_compaction() {
        let (provider, journal, _) = fixture();
        let policy = SessionCompactionPolicy {
            minimum_source_records: 1,
            keep_recent_records: 1,
        };
        let provider = provider
            .with_session_compaction(
                Arc::new(crate::DeterministicExtractiveSessionSummarizer::new(512).unwrap()),
                policy.clone(),
            )
            .unwrap();
        let request = request(&provider, RunLimits::default());
        let projection = initial_projection(&provider, &request).await.unwrap();
        let original = model_request_for_round(&request, 1, &projection.messages, &[], None);

        // Later durable history would permit compaction at the current head.
        // Recovery must instead reproduce the original cursor without writes.
        for (run, message) in [
            ("later-one", "first follow-up"),
            ("later-two", "second follow-up"),
        ] {
            journal
                .append(AgentSessionEventDraft {
                    event_id: AgentSessionEventId::new(format!("{run}-input")),
                    session_id: request.run.spec.session_id.clone(),
                    run_id: RunId::new(run),
                    payload: AgentSessionEvent::RunInputCommitted {
                        message: ModelMessage::text(ModelRole::User, message),
                    },
                })
                .await
                .unwrap();
        }
        let before = journal
            .load_session(&request.run.spec.session_id)
            .await
            .unwrap();
        assert!(crate::session_context::select_compaction_source(
            &before,
            &request.run.spec.run_id,
            &policy,
        )
        .is_some());
        let replay = project_model_context(
            &provider.inner,
            &request,
            &[],
            None,
            None,
            Some(projection.through_session_seq),
            ModelContextBudget::default(),
        )
        .await
        .unwrap();
        let replay_request = model_request_for_round(&request, 1, &replay.messages, &[], None);
        assert_eq!(
            model_request_digest(&original).unwrap(),
            model_request_digest(&replay_request).unwrap()
        );
        assert_eq!(
            model_context_trace(&projection, provider.inner.config.history_limit),
            model_context_trace(&replay, provider.inner.config.history_limit),
        );
        assert_eq!(
            before,
            journal
                .load_session(&request.run.spec.session_id)
                .await
                .unwrap()
        );
    }
}

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

#[test]
fn default_agent_contract_preserves_requested_end_states_and_scope() {
    let config = GenericAgentConfig::new("test/provider", "test/agent");

    assert!(config.system_prompt.contains("requested final states"));
    assert!(config.system_prompt.contains("verify them before delivery"));
    assert!(config
        .system_prompt
        .contains("unrequested integration, publication, cleanup, or reversal"));
    assert!(config
        .system_prompt
        .contains("sandbox_permissions='require_escalated'"));
    assert!(config
        .system_prompt
        .contains("do not offload the command to the user"));
}
