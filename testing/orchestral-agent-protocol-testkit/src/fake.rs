use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::stream;
use futures_util::StreamExt;
use orchestral_core::agent_protocol::{
    spi::{AgentProvider, AgentProviderStream, AgentStart, AgentStartError},
    wire::{
        AgentAdmission, AgentCapabilities, AgentCommandEnvelope, AgentDescriptor,
        AgentDescriptorEnvelope, AgentEventDraft, AgentExecutionRef, AgentId, AgentProtocolError,
        AgentProtocolErrorCode, AgentProviderId, AgentProviderStreamItem, AgentRejection,
        AgentRejectionCode, AgentSessionId, AgentStartRequest, ProviderCommandDisposition,
        ProviderCommandOutcome, RunId,
    },
    AGENT_PROTOCOL_V1,
};

use crate::{OutcomeUnknownFixtureFactory, ProviderFixtureFactory, ProviderScenario, TestProbes};

/// Native-start behavior used to prove that the suite detects duplicate work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptedStartMode {
    Conformant,
    DuplicateWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamStyle {
    Immediate,
    OpaqueAsync,
}

#[derive(Debug, Clone, Copy)]
struct FixtureBehavior {
    start_mode: ScriptedStartMode,
    stream_style: StreamStyle,
    lose_first_start_response: bool,
}

/// Deterministic, stateless-turn fixture. It declares both session reuse and
/// Provider-native recovery unsupported.
#[derive(Debug, Clone)]
pub struct ScriptedStatelessFactory {
    descriptor: AgentDescriptorEnvelope,
    start_mode: ScriptedStartMode,
}

impl ScriptedStatelessFactory {
    pub fn conformant() -> Result<Self, AgentProtocolError> {
        Self::new(ScriptedStartMode::Conformant)
    }

    pub fn duplicate_work() -> Result<Self, AgentProtocolError> {
        Self::new(ScriptedStartMode::DuplicateWork)
    }

    pub fn new(start_mode: ScriptedStartMode) -> Result<Self, AgentProtocolError> {
        Ok(Self {
            descriptor: fixture_descriptor(
                "testkit.scripted-stateless",
                "immediate-completion-v1",
                false,
                false,
            )?,
            start_mode,
        })
    }
}

impl ProviderFixtureFactory for ScriptedStatelessFactory {
    fn descriptor(&self) -> AgentDescriptorEnvelope {
        self.descriptor.clone()
    }

    fn create(&self, scenario: ProviderScenario, probes: TestProbes) -> Arc<dyn AgentProvider> {
        Arc::new(DeterministicProvider::new(
            self.descriptor.clone(),
            scenario,
            probes,
            FixtureBehavior {
                start_mode: self.start_mode,
                stream_style: StreamStyle::Immediate,
                lose_first_start_response: false,
            },
        ))
    }
}

/// Stateful fixture that accepts many Runs in one public Agent session and
/// can replay its stable Provider Draft stream.
#[derive(Debug, Clone)]
pub struct SessionfulRecoverFactory {
    descriptor: AgentDescriptorEnvelope,
}

impl SessionfulRecoverFactory {
    pub fn new() -> Result<Self, AgentProtocolError> {
        Ok(Self {
            descriptor: fixture_descriptor(
                "testkit.sessionful-recover",
                "sessionful-recover-v1",
                true,
                true,
            )?,
        })
    }
}

impl ProviderFixtureFactory for SessionfulRecoverFactory {
    fn descriptor(&self) -> AgentDescriptorEnvelope {
        self.descriptor.clone()
    }

    fn create(&self, scenario: ProviderScenario, probes: TestProbes) -> Arc<dyn AgentProvider> {
        Arc::new(DeterministicProvider::new(
            self.descriptor.clone(),
            scenario,
            probes,
            FixtureBehavior {
                start_mode: ScriptedStartMode::Conformant,
                stream_style: StreamStyle::Immediate,
                lose_first_start_response: false,
            },
        ))
    }
}

/// Opaque-adapter fixture whose observations arrive through an asynchronous
/// stream but which has no Provider-native recovery contract.
#[derive(Debug, Clone)]
pub struct OpaqueAsyncNoRecoverFactory {
    descriptor: AgentDescriptorEnvelope,
}

impl OpaqueAsyncNoRecoverFactory {
    pub fn new() -> Result<Self, AgentProtocolError> {
        Ok(Self {
            descriptor: fixture_descriptor(
                "testkit.opaque-async",
                "opaque-no-recover-v1",
                false,
                false,
            )?,
        })
    }
}

impl ProviderFixtureFactory for OpaqueAsyncNoRecoverFactory {
    fn descriptor(&self) -> AgentDescriptorEnvelope {
        self.descriptor.clone()
    }

    fn create(&self, scenario: ProviderScenario, probes: TestProbes) -> Arc<dyn AgentProvider> {
        Arc::new(DeterministicProvider::new(
            self.descriptor.clone(),
            scenario,
            probes,
            FixtureBehavior {
                start_mode: ScriptedStartMode::Conformant,
                stream_style: StreamStyle::OpaqueAsync,
                lose_first_start_response: false,
            },
        ))
    }
}

/// Fault fixture that begins exactly one native execution, loses the first
/// response, then reconciles only an identical immutable start request.
#[derive(Debug, Clone)]
pub struct OutcomeUnknownFactory {
    descriptor: AgentDescriptorEnvelope,
}

impl OutcomeUnknownFactory {
    pub fn new() -> Result<Self, AgentProtocolError> {
        Ok(Self {
            descriptor: fixture_descriptor(
                "testkit.outcome-unknown",
                "lost-first-response-v1",
                true,
                false,
            )?,
        })
    }
}

impl ProviderFixtureFactory for OutcomeUnknownFactory {
    fn descriptor(&self) -> AgentDescriptorEnvelope {
        self.descriptor.clone()
    }

    fn create(&self, scenario: ProviderScenario, probes: TestProbes) -> Arc<dyn AgentProvider> {
        Arc::new(DeterministicProvider::new(
            self.descriptor.clone(),
            scenario,
            probes,
            FixtureBehavior {
                start_mode: ScriptedStartMode::Conformant,
                stream_style: StreamStyle::OpaqueAsync,
                lose_first_start_response: true,
            },
        ))
    }
}

impl OutcomeUnknownFixtureFactory for OutcomeUnknownFactory {}

fn fixture_descriptor(
    provider_id: &str,
    agent_id: &str,
    session_reuse: bool,
    recover: bool,
) -> Result<AgentDescriptorEnvelope, AgentProtocolError> {
    let mut capabilities = AgentCapabilities {
        session_reuse,
        ..AgentCapabilities::default()
    };
    capabilities.controls.recover = recover;
    AgentDescriptorEnvelope::seal(AgentDescriptor {
        provider_id: AgentProviderId::new(provider_id),
        agent_id: AgentId::new(agent_id),
        supported_protocol_versions: vec![AGENT_PROTOCOL_V1],
        accepted_content_types: BTreeSet::from(["text/plain".to_owned()]),
        capabilities,
        extensions: Default::default(),
    })
}

#[derive(Clone)]
struct StoredRun {
    execution: AgentExecutionRef,
    admission: AgentAdmission,
    events: Vec<AgentEventDraft>,
}

#[derive(Default)]
struct ProviderState {
    runs: BTreeMap<RunId, StoredRun>,
    sessions: BTreeMap<AgentSessionId, RunId>,
}

struct DeterministicProvider {
    descriptor: AgentDescriptorEnvelope,
    template_request: AgentStartRequest,
    template_events: Vec<AgentEventDraft>,
    probes: TestProbes,
    behavior: FixtureBehavior,
    state: Mutex<ProviderState>,
}

impl DeterministicProvider {
    fn new(
        descriptor: AgentDescriptorEnvelope,
        scenario: ProviderScenario,
        probes: TestProbes,
        behavior: FixtureBehavior,
    ) -> Self {
        Self {
            descriptor,
            template_request: scenario.start_request,
            template_events: scenario.immediate_events,
            probes,
            behavior,
            state: Mutex::new(ProviderState::default()),
        }
    }

    fn stream_for(&self, events: Vec<AgentEventDraft>) -> AgentProviderStream {
        match self.behavior.stream_style {
            StreamStyle::Immediate => stream::iter(
                events
                    .into_iter()
                    .map(|draft| Ok(AgentProviderStreamItem::Event(Box::new(draft)))),
            )
            .boxed(),
            StreamStyle::OpaqueAsync => {
                stream::unfold(events.into_iter(), |mut events| async move {
                    events
                        .next()
                        .map(|draft| (Ok(AgentProviderStreamItem::Event(Box::new(draft))), events))
                })
                .boxed()
            }
        }
    }

    fn events_for(&self, request: &AgentStartRequest) -> Vec<AgentEventDraft> {
        if *request == self.template_request {
            self.template_events.clone()
        } else {
            ProviderScenario::completion_events_for(&self.descriptor, request)
        }
    }

    fn start_from(&self, stored: &StoredRun) -> AgentStart {
        AgentStart {
            execution: stored.execution.clone(),
            admission: stored.admission.clone(),
            stream: self.stream_for(stored.events.clone()),
        }
    }

    fn rejection(code: AgentRejectionCode, message: impl Into<String>) -> AgentStartError {
        AgentStartError::Rejected(AgentRejection::new(code, message))
    }
}

#[async_trait]
impl AgentProvider for DeterministicProvider {
    fn describe(&self) -> AgentDescriptorEnvelope {
        self.descriptor.clone()
    }

    async fn start(&self, request: AgentStartRequest) -> Result<AgentStart, AgentStartError> {
        request
            .validate_for_descriptor(&self.descriptor)
            .map_err(|error| {
                Self::rejection(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;
        let compatibility = self
            .descriptor
            .descriptor
            .check_run_compatibility(&request.run)
            .map_err(AgentStartError::Rejected)?;
        let admission = AgentAdmission {
            skipped_optional_bindings: compatibility.skipped_optional_bindings.clone(),
        };
        admission
            .validate_against(&request.run, &compatibility)
            .map_err(|error| Self::rejection(AgentRejectionCode::InvalidSpec, error.to_string()))?;

        let mut state = self.state.lock().map_err(|_| {
            AgentStartError::OutcomeUnknown(AgentProtocolError::new(
                AgentProtocolErrorCode::Internal,
                "deterministic fixture state lock is poisoned",
            ))
        })?;
        if let Some(existing) = state.runs.get(&request.run.spec.run_id) {
            let expected =
                AgentExecutionRef::for_start(&request, &self.descriptor).map_err(|error| {
                    Self::rejection(AgentRejectionCode::RunIdConflict, error.to_string())
                })?;
            if existing.execution != expected {
                return Err(Self::rejection(
                    AgentRejectionCode::RunIdConflict,
                    "run_id already belongs to a different immutable start",
                ));
            }
            if self.behavior.start_mode == ScriptedStartMode::DuplicateWork {
                self.probes.record_native_start_for(&request);
            }
            return Ok(self.start_from(existing));
        }

        if !self.descriptor.descriptor.capabilities.session_reuse
            && state
                .sessions
                .get(&request.run.spec.session_id)
                .is_some_and(|run_id| *run_id != request.run.spec.run_id)
        {
            return Err(Self::rejection(
                AgentRejectionCode::SessionConflict,
                "provider declares one Run per Agent session",
            ));
        }

        let execution =
            AgentExecutionRef::for_start(&request, &self.descriptor).map_err(|error| {
                Self::rejection(AgentRejectionCode::RunIdConflict, error.to_string())
            })?;
        let stored = StoredRun {
            execution,
            admission,
            events: self.events_for(&request),
        };
        state.sessions.insert(
            request.run.spec.session_id.clone(),
            request.run.spec.run_id.clone(),
        );
        state
            .runs
            .insert(request.run.spec.run_id.clone(), stored.clone());
        self.probes.record_native_start_for(&request);

        if self.behavior.lose_first_start_response {
            return Err(AgentStartError::OutcomeUnknown(
                AgentProtocolError::new(
                    AgentProtocolErrorCode::ProviderUnavailable,
                    "native start succeeded but the first response was lost",
                )
                .with_retryable(true),
            ));
        }
        Ok(self.start_from(&stored))
    }

    async fn command(
        &self,
        execution: &AgentExecutionRef,
        command: AgentCommandEnvelope,
    ) -> Result<ProviderCommandDisposition, AgentProtocolError> {
        if execution.run_id != command.run_id {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunNotFound,
                "command does not target this execution",
            ));
        }
        Ok(ProviderCommandDisposition {
            command_id: command.command_id,
            run_id: command.run_id,
            outcome: ProviderCommandOutcome::Unsupported {
                feature: "commands".to_owned(),
            },
            duplicate: false,
        })
    }

    fn recover(
        &self,
        execution: &AgentExecutionRef,
    ) -> Result<AgentProviderStream, AgentProtocolError> {
        if !self.descriptor.descriptor.capabilities.controls.recover {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::Unsupported,
                "provider-native recovery is not declared",
            ));
        }
        let state = self.state.lock().map_err(|_| {
            AgentProtocolError::new(
                AgentProtocolErrorCode::Internal,
                "deterministic fixture state lock is poisoned",
            )
        })?;
        let stored = state.runs.get(&execution.run_id).ok_or_else(|| {
            AgentProtocolError::new(AgentProtocolErrorCode::RunNotFound, "run does not exist")
        })?;
        if stored.execution != *execution {
            return Err(AgentProtocolError::new(
                AgentProtocolErrorCode::RunIdConflict,
                "execution reference does not match the stored immutable Run",
            ));
        }
        Ok(self.stream_for(stored.events.clone()))
    }
}
