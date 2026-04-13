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
    #[serde(rename = "SINGLE_ACTION")]
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
    #[serde(rename = "DONE")]
    Done { message: String },
    #[serde(rename = "NEED_INPUT")]
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
    // First try: extract verbatim JSON object.
    if let Some(json) = extract_json_verbatim(text) {
        return Some(json);
    }

    // Second try: strip markdown code fences and retry.
    let stripped = strip_markdown_code_fences(text);
    if stripped != text {
        if let Some(json) = extract_json_verbatim(&stripped) {
            return Some(json);
        }
    }

    // Third try: apply common LLM JSON repairs and retry.
    let repaired = repair_json_text(&stripped);
    if repaired != stripped {
        if let Some(json) = extract_json_verbatim(&repaired) {
            return Some(json);
        }
    }

    None
}

fn extract_json_verbatim(text: &str) -> Option<String> {
    let mut fallback: Option<String> = None;
    for (start, ch) in text.char_indices() {
        if ch != '{' {
            continue;
        }
        if let Some(end) = find_json_object_end(text, start) {
            let candidate = &text[start..=end];
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(candidate) {
                if v.is_object() {
                    // Prefer objects with a "type" field (planner output envelope)
                    if v.get("type").is_some() {
                        return Some(candidate.to_string());
                    }
                    if fallback.is_none() {
                        fallback = Some(candidate.to_string());
                    }
                }
            }
        }
    }
    fallback
}

/// Strip markdown code fences (```json ... ``` or ``` ... ```).
fn strip_markdown_code_fences(text: &str) -> String {
    let mut result = text.to_string();
    // Remove opening fence: ```json or ```
    if let Some(start) = result.find("```") {
        let fence_end = result[start + 3..]
            .find('\n')
            .map(|p| start + 3 + p + 1)
            .unwrap_or(start + 3);
        result = format!("{}{}", &result[..start], &result[fence_end..]);
    }
    // Remove closing fence
    if let Some(end) = result.rfind("```") {
        result = format!("{}{}", &result[..end], &result[end + 3..]);
    }
    result
}

/// Fix common LLM JSON output errors:
/// - Trailing commas before } or ]
/// - Single unclosed } or ] at the end
fn repair_json_text(text: &str) -> String {
    let mut result = text.to_string();

    // Fix trailing commas: ,} ,] and , <whitespace> } / ]
    loop {
        let before = result.clone();
        result = remove_trailing_commas(&result);
        if result == before {
            break;
        }
    }

    // Fix unclosed braces/brackets: count opens vs closes
    let open_braces = result.chars().filter(|&c| c == '{').count();
    let close_braces = result.chars().filter(|&c| c == '}').count();
    for _ in 0..open_braces.saturating_sub(close_braces) {
        result.push('}');
    }
    let open_brackets = result.chars().filter(|&c| c == '[').count();
    let close_brackets = result.chars().filter(|&c| c == ']').count();
    for _ in 0..open_brackets.saturating_sub(close_brackets) {
        result.push(']');
    }

    result
}

/// Remove trailing commas before closing braces/brackets, including across whitespace.
fn remove_trailing_commas(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == ',' {
            // Look ahead past whitespace for } or ]
            let mut j = i + 1;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && (chars[j] == '}' || chars[j] == ']') {
                // Skip the comma, keep the whitespace and closing bracket
                i += 1;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
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
