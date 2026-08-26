//! External, provider-neutral Agent Protocol conformance fixtures.
//!
//! This crate intentionally depends only on the public
//! `orchestral_core::agent_protocol` API. It is the first M0 slice, not the
//! complete Agent Protocol v1 conformance program.

mod fake;
mod report;
pub mod schema_snapshot;
mod suite;

pub use fake::{
    OpaqueAsyncNoRecoverFactory, OutcomeUnknownFactory, ScriptedStartMode,
    ScriptedStatelessFactory, SessionfulRecoverFactory,
};
pub use report::{case_ids, CaseId, CaseResult, CaseVerdict, ConformanceReport};
pub use suite::{
    ConformanceSuite, OutcomeUnknownConformanceSuite, OutcomeUnknownFixtureFactory,
    ProviderFixtureFactory, ProviderScenario, TestProbes,
};
