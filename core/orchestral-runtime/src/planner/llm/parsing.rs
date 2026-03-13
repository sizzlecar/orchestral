use serde::Deserialize;

use orchestral_core::planner::{ActionCall, PlanError, PlannerOutput};
use orchestral_core::types::{
    ArtifactFamily, DerivationPolicy, SkeletonChoice, SkeletonKind, StageChoice, StageKind,
};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
enum PlannerJsonOutput {
    SkeletonChoice {
        skeleton: SkeletonKind,
        #[serde(default)]
        artifact_family: Option<ArtifactFamily>,
        initial_stage: StageKind,
        #[serde(default = "default_confidence")]
        confidence: f32,
        #[serde(default)]
        reason: Option<String>,
    },
    StageChoice {
        skeleton: SkeletonKind,
        artifact_family: ArtifactFamily,
        current_stage: StageKind,
        stage_goal: String,
        derivation_policy: DerivationPolicy,
        #[serde(default)]
        next_stage_hint: Option<StageKind>,
        #[serde(default)]
        reason: Option<String>,
    },
    ActionCall {
        action: String,
        #[serde(default)]
        params: serde_json::Value,
        #[serde(default)]
        reason: Option<String>,
    },
    DirectResponse {
        message: String,
    },
    Clarification {
        question: String,
    },
}

pub(super) fn parse_planner_output(json: &str) -> Result<PlannerOutput, PlanError> {
    let parsed = serde_json::from_str::<PlannerJsonOutput>(json)
        .map_err(|e| PlanError::Generation(format!("Invalid planner output JSON: {}", e)))?;

    match parsed {
        PlannerJsonOutput::SkeletonChoice {
            skeleton,
            artifact_family,
            initial_stage,
            confidence,
            reason,
        } => Ok(PlannerOutput::SkeletonChoice(SkeletonChoice {
            skeleton,
            artifact_family,
            initial_stage,
            confidence: confidence.clamp(0.0, 1.0),
            reason,
        })),
        PlannerJsonOutput::StageChoice {
            skeleton,
            artifact_family,
            current_stage,
            stage_goal,
            derivation_policy,
            next_stage_hint,
            reason,
        } => Ok(PlannerOutput::StageChoice(StageChoice {
            skeleton,
            artifact_family,
            current_stage,
            stage_goal,
            derivation_policy,
            next_stage_hint,
            reason,
        })),
        PlannerJsonOutput::ActionCall {
            action,
            params,
            reason,
        } => Ok(PlannerOutput::ActionCall(ActionCall {
            action,
            params,
            reason,
        })),
        PlannerJsonOutput::DirectResponse { message } => Ok(PlannerOutput::DirectResponse(message)),
        PlannerJsonOutput::Clarification { question } => Ok(PlannerOutput::Clarification(question)),
    }
}

pub(super) fn extract_json(text: &str) -> Option<String> {
    for (start, ch) in text.char_indices() {
        if ch != '{' {
            continue;
        }
        if let Some(end) = find_json_object_end(text, start) {
            let candidate = &text[start..=end];
            if serde_json::from_str::<serde_json::Value>(candidate)
                .map(|v| v.is_object())
                .unwrap_or(false)
            {
                return Some(candidate.to_string());
            }
        }
    }
    None
}

fn default_confidence() -> f32 {
    1.0
}

fn find_json_object_end(text: &str, start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (idx, ch) in text[start..].char_indices() {
        let abs = start + idx;
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    return Some(abs);
                }
            }
            _ => {}
        }
    }
    None
}
