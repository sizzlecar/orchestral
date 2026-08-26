//! Skill Context Plane for the Generic Agent.

mod runtime;

pub use runtime::{
    ActivatedSkillSet, SkillActivationOutcome, SkillActivationPolicy, SkillActivationRequest,
    SkillConflict, SkillHostProfile, SkillRoot, SkillRuntime, SkillRuntimeError,
};
