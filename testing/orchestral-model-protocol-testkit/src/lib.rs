//! External, provider-neutral conformance fixtures for `ModelBackend`.
//!
//! Concrete adapters supply only wire fixtures and a backend composition
//! function. The same suite owns canonical request, stream, terminal, and
//! cancellation assertions for every model family.

mod fake;
mod suite;

pub use fake::ScriptedModelFixture;
pub use suite::{
    ModelConformanceCase, ModelConformanceReport, ModelConformanceResult, ModelConformanceSuite,
    ModelFixtureFactory, ModelFixtureResponse, ModelFixtureScenario,
};
