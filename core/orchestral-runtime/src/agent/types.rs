use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::materialize::{exports_to_value, parse_json_export_field};

#[derive(Debug)]
pub(super) struct AgentStepParams {
    pub(super) mode: AgentMode,
    pub(super) goal: String,
    pub(super) allowed_actions: HashSet<String>,
    pub(super) max_iterations: u64,
    pub(super) output_keys: Vec<String>,
    pub(super) output_rules: HashMap<String, AgentOutputRule>,
    pub(super) result_slot: Option<String>,
    pub(super) bound_inputs: HashMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AgentMode {
    Explore,
    Leaf,
}

impl AgentMode {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Explore => "explore",
            Self::Leaf => "leaf",
        }
    }
}

#[derive(Debug)]
pub(super) enum AgentDecision {
    Action {
        name: String,
        params: Value,
        save_as: Option<String>,
        capture: Option<AgentCaptureMode>,
    },
    Final {
        exports: HashMap<String, Value>,
    },
    Finish,
}

#[derive(Debug, Clone, Default)]
pub(super) struct AgentOutputRule {
    pub(super) candidates: Vec<AgentOutputCandidate>,
    pub(super) template: Option<String>,
    pub(super) fallback_aliases: Vec<String>,
    pub(super) required_action: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct AgentOutputCandidate {
    pub(super) slot: String,
    pub(super) path: Vec<AgentPathSegment>,
    pub(super) required_action: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) enum AgentPathSegment {
    Key(String),
    Index(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AgentCaptureMode {
    JsonStdout,
}

#[derive(Debug, Clone)]
pub(super) struct AgentEvidenceRecord {
    pub(super) action_name: String,
    pub(super) raw_exports: HashMap<String, Value>,
    pub(super) value: Value,
}

#[derive(Debug, Default, Clone)]
pub(super) struct AgentEvidenceStore {
    pub(super) slots: HashMap<String, Value>,
    pub(super) slot_actions: HashMap<String, String>,
    pub(super) records: Vec<AgentEvidenceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MaterializeError {
    pub(super) key: String,
    pub(super) reason_code: &'static str,
    pub(super) detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AgentDebugTextPayload {
    pub(super) path: String,
    pub(super) text: String,
}

impl MaterializeError {
    pub(super) fn new(key: &str, reason_code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            key: key.to_string(),
            reason_code,
            detail: detail.into(),
        }
    }

    pub(super) fn summary(&self) -> String {
        format!("{} [{}]: {}", self.key, self.reason_code, self.detail)
    }
}

impl AgentEvidenceStore {
    fn set_slot(&mut self, key: String, value: Value, action_name: Option<&str>) {
        self.slots.insert(key.clone(), value);
        if let Some(action_name) = action_name {
            self.slot_actions.insert(key, action_name.to_string());
        } else {
            self.slot_actions.remove(&key);
        }
    }

    pub(super) fn slot_value_with_requirement(
        &self,
        key: &str,
        required_action: Option<&str>,
    ) -> Option<Value> {
        let value = self.slots.get(key)?;
        if let Some(required_action) = required_action {
            let actual = self.slot_actions.get(key).map(String::as_str)?;
            if actual != required_action {
                return None;
            }
        }
        Some(value.clone())
    }

    pub(super) fn record_success(
        &mut self,
        action_name: &str,
        raw_exports: HashMap<String, Value>,
        value: Value,
        save_as: Option<&str>,
    ) {
        let raw_object = exports_to_value(&raw_exports);
        self.set_slot("last".to_string(), value.clone(), Some(action_name));
        self.set_slot(
            "last_raw".to_string(),
            raw_object.clone(),
            Some(action_name),
        );
        self.set_slot(
            format!("action:{}", action_name),
            value.clone(),
            Some(action_name),
        );
        self.set_slot(
            format!("action:{}:raw", action_name),
            raw_object,
            Some(action_name),
        );
        self.set_slot(
            "last_action".to_string(),
            Value::String(action_name.to_string()),
            Some(action_name),
        );

        if let Some(parsed) = parse_json_export_field(&raw_exports, "stdout") {
            self.set_slot(
                "last_stdout_json".to_string(),
                parsed.clone(),
                Some(action_name),
            );
            self.set_slot(
                format!("action:{}:stdout_json", action_name),
                parsed,
                Some(action_name),
            );
        }
        if let Some(parsed) = parse_json_export_field(&raw_exports, "body") {
            self.set_slot(
                "last_body_json".to_string(),
                parsed.clone(),
                Some(action_name),
            );
            self.set_slot(
                format!("action:{}:body_json", action_name),
                parsed,
                Some(action_name),
            );
        }

        if let Some(slot) = save_as {
            self.set_slot(slot.to_string(), value.clone(), Some(action_name));
        }
        self.records.push(AgentEvidenceRecord {
            action_name: action_name.to_string(),
            raw_exports,
            value,
        });
    }
}
