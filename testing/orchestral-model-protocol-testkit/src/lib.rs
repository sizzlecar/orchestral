//! External, provider-neutral conformance fixtures for `ModelBackend`.
//!
//! Concrete adapters supply only wire fixtures and a backend composition
//! function. The same suite owns canonical request, stream, terminal, and
//! cancellation assertions for every model family.

mod fake;
mod live;
mod stress;
mod suite;

pub use fake::ScriptedModelFixture;
pub use live::{run_live_text_smoke, LiveModelSmokeReport};
pub use stress::{
    ModelStreamStressCase, ModelStreamStressFault, ModelStreamStressReport, ModelStreamStressSuite,
    DEFAULT_MODEL_STREAM_STRESS_CASES,
};
pub use suite::{
    ModelConformanceCase, ModelConformanceReport, ModelConformanceResult, ModelConformanceSuite,
    ModelFixtureFactory, ModelFixtureResponse, ModelFixtureScenario,
};
