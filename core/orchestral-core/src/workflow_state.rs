//! Bounded, serializable state shared by steps in one workflow execution.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_WORKING_SET_MAX_ENTRIES: usize = 10_000;

fn default_max_entries() -> usize {
    DEFAULT_WORKING_SET_MAX_ENTRIES
}

/// Scope of a working-set value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Scope {
    /// Private state for one step.
    Step { step_id: String },
    /// State shared by all steps in one workflow execution.
    Workflow,
    /// Host-provided state visible across workflow executions.
    Global,
}

impl Scope {
    pub fn step(step_id: impl Into<String>) -> Self {
        Self::Step {
            step_id: step_id.into(),
        }
    }

    pub fn workflow() -> Self {
        Self::Workflow
    }

    pub fn global() -> Self {
        Self::Global
    }
}

/// Weakly typed, bounded data plane for DAG step bindings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingSet {
    data: HashMap<(Scope, String), Value>,
    order: VecDeque<(Scope, String)>,
    #[serde(default = "default_max_entries")]
    max_entries: usize,
}

impl Default for WorkingSet {
    fn default() -> Self {
        Self {
            data: HashMap::new(),
            order: VecDeque::new(),
            max_entries: default_max_entries(),
        }
    }
}

impl WorkingSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            ..Self::default()
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn get(&self, scope: &Scope, key: &str) -> Option<&Value> {
        self.data.get(&(scope.clone(), key.to_owned()))
    }

    pub fn set(&mut self, scope: Scope, key: impl Into<String>, value: Value) {
        let entry_key = (scope, key.into());
        self.data.insert(entry_key.clone(), value);
        self.touch_entry(entry_key);
        self.evict_if_needed();
    }

    pub fn remove(&mut self, scope: &Scope, key: &str) -> Option<Value> {
        let entry_key = (scope.clone(), key.to_owned());
        let removed = self.data.remove(&entry_key);
        if removed.is_some() {
            self.remove_from_order(&entry_key);
        }
        removed
    }

    pub fn contains(&self, scope: &Scope, key: &str) -> bool {
        self.data.contains_key(&(scope.clone(), key.to_owned()))
    }

    pub fn clear_scope(&mut self, scope: &Scope) {
        self.data.retain(|(candidate, _), _| candidate != scope);
        self.order.retain(|(candidate, _)| candidate != scope);
    }

    pub fn clear_all_step_scopes(&mut self) {
        self.data
            .retain(|(scope, _), _| !matches!(scope, Scope::Step { .. }));
        self.order
            .retain(|(scope, _)| !matches!(scope, Scope::Step { .. }));
    }

    pub fn get_workflow(&self, key: &str) -> Option<&Value> {
        self.get(&Scope::Workflow, key)
    }

    pub fn set_workflow(&mut self, key: impl Into<String>, value: Value) {
        self.set(Scope::Workflow, key, value);
    }

    pub fn remove_workflow(&mut self, key: &str) -> Option<Value> {
        self.remove(&Scope::Workflow, key)
    }

    pub fn get_global(&self, key: &str) -> Option<&Value> {
        self.get(&Scope::Global, key)
    }

    pub fn set_global(&mut self, key: impl Into<String>, value: Value) {
        self.set(Scope::Global, key, value);
    }

    pub fn remove_global(&mut self, key: &str) -> Option<Value> {
        self.remove(&Scope::Global, key)
    }

    pub fn get_step(&self, step_id: &str, key: &str) -> Option<&Value> {
        self.get(&Scope::step(step_id), key)
    }

    pub fn set_step(&mut self, step_id: impl Into<String>, key: impl Into<String>, value: Value) {
        self.set(Scope::step(step_id), key, value);
    }

    pub fn clear_step(&mut self, step_id: &str) {
        self.clear_scope(&Scope::step(step_id));
    }

    pub fn keys_in_scope(&self, scope: &Scope) -> Vec<&str> {
        self.data
            .keys()
            .filter(|(candidate, _)| candidate == scope)
            .map(|(_, key)| key.as_str())
            .collect()
    }

    pub fn export_workflow_data(&self) -> HashMap<String, Value> {
        self.data
            .iter()
            .filter(|((scope, _), _)| *scope == Scope::Workflow)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect()
    }

    pub fn import_workflow_data(&mut self, data: HashMap<String, Value>) {
        for (key, value) in data {
            self.set_workflow(key, value);
        }
    }

    fn touch_entry(&mut self, key: (Scope, String)) {
        self.remove_from_order(&key);
        self.order.push_back(key);
    }

    fn remove_from_order(&mut self, key: &(Scope, String)) {
        self.order.retain(|item| item != key);
    }

    fn evict_if_needed(&mut self) {
        while self.data.len() > self.max_entries {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.data.remove(&oldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn evicts_the_oldest_entry_at_the_host_limit() {
        let mut working_set = WorkingSet::with_max_entries(2);
        working_set.set_workflow("a", json!(1));
        working_set.set_workflow("b", json!(2));
        working_set.set_workflow("c", json!(3));

        assert_eq!(working_set.len(), 2);
        assert_eq!(working_set.get_workflow("a"), None);
        assert_eq!(working_set.get_workflow("b"), Some(&json!(2)));
        assert_eq!(working_set.get_workflow("c"), Some(&json!(3)));
    }

    #[test]
    fn updating_an_entry_refreshes_its_eviction_order() {
        let mut working_set = WorkingSet::with_max_entries(2);
        working_set.set_workflow("a", json!(1));
        working_set.set_workflow("b", json!(2));
        working_set.set_workflow("a", json!(11));
        working_set.set_workflow("c", json!(3));

        assert_eq!(working_set.get_workflow("a"), Some(&json!(11)));
        assert_eq!(working_set.get_workflow("b"), None);
        assert_eq!(working_set.get_workflow("c"), Some(&json!(3)));
    }
}
