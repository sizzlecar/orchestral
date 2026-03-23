use serde::Deserialize;

use orchestral_core::planner::{PlanError, PlannerOutput, SingleAction};
use orchestral_core::types::Plan;

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub(super) struct ActionSelection {
    #[serde(default, alias = "actions")]
    pub selected_actions: Vec<String>,
    #[serde(default)]
    pub blocked_actions: Vec<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum PlannerJsonOutput {
    #[serde(rename = "SINGLE_ACTION", alias = "ACTION_CALL")]
    SingleAction {
        action: String,
        #[serde(default)]
        params: serde_json::Value,
        #[serde(default)]
        reason: Option<String>,
    },
    #[serde(rename = "MINI_PLAN")]
    MiniPlan {
        goal: String,
        steps: Vec<orchestral_core::types::Step>,
        #[serde(default)]
        confidence: Option<f32>,
        #[serde(default)]
        on_complete: Option<String>,
        #[serde(default)]
        on_failure: Option<String>,
    },
    #[serde(rename = "DONE", alias = "DIRECT_RESPONSE")]
    Done { message: String },
    #[serde(rename = "NEED_INPUT", alias = "CLARIFICATION")]
    NeedInput { question: String },
}

pub(super) fn parse_planner_output(json: &str) -> Result<PlannerOutput, PlanError> {
    let parsed = serde_json::from_str::<PlannerJsonOutput>(json)
        .map_err(|e| PlanError::Generation(format!("Invalid planner output JSON: {}", e)))?;

    match parsed {
        PlannerJsonOutput::SingleAction {
            action,
            params,
            reason,
        } => Ok(PlannerOutput::SingleAction(SingleAction {
            action,
            params,
            reason,
        })),
        PlannerJsonOutput::MiniPlan {
            goal,
            steps,
            confidence,
            on_complete,
            on_failure,
        } => Ok(PlannerOutput::MiniPlan(Plan {
            goal,
            steps,
            confidence,
            on_complete,
            on_failure,
        })),
        PlannerJsonOutput::Done { message } => Ok(PlannerOutput::Done(message)),
        PlannerJsonOutput::NeedInput { question } => Ok(PlannerOutput::NeedInput(question)),
    }
}

pub(super) fn parse_action_selection(json: &str) -> Result<ActionSelection, PlanError> {
    serde_json::from_str::<ActionSelection>(json)
        .map_err(|e| PlanError::Generation(format!("Invalid action selector output JSON: {}", e)))
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
