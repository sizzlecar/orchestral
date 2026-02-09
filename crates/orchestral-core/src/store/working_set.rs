//! WorkingSet - Scoped KV data container

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};

const DEFAULT_WORKING_SET_MAX_ENTRIES: usize = 10_000;

fn default_max_entries() -> usize {
    DEFAULT_WORKING_SET_MAX_ENTRIES
}

/// Data scope for WorkingSet
/// Avoids naming conflicts and data pollution
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Scope {
    /// Step-level scope, cleared when step completes
    Step { step_id: String },
    /// Task-level scope, cleared when task completes
    Task,
    /// Global scope, persists across tasks
    Global,
}

impl Scope {
    /// Create a step scope
    pub fn step(step_id: impl Into<String>) -> Self {
        Self::Step {
            step_id: step_id.into(),
        }
    }

    /// Create a task scope
    pub fn task() -> Self {
        Self::Task
    }

    /// Create a global scope
    pub fn global() -> Self {
        Self::Global
    }
}

/// Working set - weakly-typed KV data container for inter-step communication
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
    /// Create a new empty working set
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a working set with a hard entry limit.
    pub fn with_max_entries(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            ..Self::default()
        }
    }

    /// Number of key-value entries currently stored.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true when there are no entries in the working set.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a value by scope and key
    pub fn get(&self, scope: &Scope, key: &str) -> Option<&Value> {
        self.data.get(&(scope.clone(), key.to_string()))
    }

    /// Set a value by scope and key
    pub fn set(&mut self, scope: Scope, key: impl Into<String>, value: Value) {
        let entry_key = (scope, key.into());
        self.data.insert(entry_key.clone(), value);
        self.touch_entry(entry_key);
        self.evict_if_needed();
    }

    /// Remove a value by scope and key
    pub fn remove(&mut self, scope: &Scope, key: &str) -> Option<Value> {
        let entry_key = (scope.clone(), key.to_string());
        let removed = self.data.remove(&entry_key);
        if removed.is_some() {
            self.remove_from_order(&entry_key);
        }
        removed
    }

    /// Check if a key exists in scope
    pub fn contains(&self, scope: &Scope, key: &str) -> bool {
        self.data.contains_key(&(scope.clone(), key.to_string()))
    }

    /// Clear all data in a specific scope
    pub fn clear_scope(&mut self, scope: &Scope) {
        self.data.retain(|(s, _), _| s != scope);
        self.order.retain(|(s, _)| s != scope);
    }

    /// Clear all step scopes
    pub fn clear_all_step_scopes(&mut self) {
        self.data
            .retain(|(s, _), _| !matches!(s, Scope::Step { .. }));
        self.order.retain(|(s, _)| !matches!(s, Scope::Step { .. }));
    }

    // ============ Convenience methods for Task scope ============

    /// Get a value from task scope
    pub fn get_task(&self, key: &str) -> Option<&Value> {
        self.get(&Scope::Task, key)
    }

    /// Set a value in task scope
    pub fn set_task(&mut self, key: impl Into<String>, value: Value) {
        self.set(Scope::Task, key, value);
    }

    /// Remove a value from task scope
    pub fn remove_task(&mut self, key: &str) -> Option<Value> {
        self.remove(&Scope::Task, key)
    }

    // ============ Convenience methods for Global scope ============

    /// Get a value from global scope
    pub fn get_global(&self, key: &str) -> Option<&Value> {
        self.get(&Scope::Global, key)
    }

    /// Set a value in global scope
    pub fn set_global(&mut self, key: impl Into<String>, value: Value) {
        self.set(Scope::Global, key, value);
    }

    /// Remove a value from global scope
    pub fn remove_global(&mut self, key: &str) -> Option<Value> {
        self.remove(&Scope::Global, key)
    }

    // ============ Convenience methods for Step scope ============

    /// Get a value from a specific step scope
    pub fn get_step(&self, step_id: &str, key: &str) -> Option<&Value> {
        self.get(&Scope::step(step_id), key)
    }

    /// Set a value in a specific step scope
    pub fn set_step(&mut self, step_id: impl Into<String>, key: impl Into<String>, value: Value) {
        self.set(Scope::step(step_id), key, value);
    }

    /// Clear a specific step scope
    pub fn clear_step(&mut self, step_id: &str) {
        self.clear_scope(&Scope::step(step_id));
    }

    // ============ Bulk operations ============

    /// Get all keys in a scope
    pub fn keys_in_scope(&self, scope: &Scope) -> Vec<&str> {
        self.data
            .keys()
            .filter(|(s, _)| s == scope)
            .map(|(_, k)| k.as_str())
            .collect()
    }

    /// Export all task-scope data as a HashMap
    pub fn export_task_data(&self) -> HashMap<String, Value> {
        self.data
            .iter()
            .filter(|((s, _), _)| *s == Scope::Task)
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Import data into task scope
    pub fn import_task_data(&mut self, data: HashMap<String, Value>) {
        for (k, v) in data {
            self.set_task(k, v);
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
    fn test_working_set_evicts_oldest_entry_when_limit_exceeded() {
        let mut ws = WorkingSet::with_max_entries(2);
        ws.set_task("a", json!(1));
        ws.set_task("b", json!(2));
        ws.set_task("c", json!(3));

        assert_eq!(ws.len(), 2);
        assert_eq!(ws.get_task("a"), None);
        assert_eq!(ws.get_task("b"), Some(&json!(2)));
        assert_eq!(ws.get_task("c"), Some(&json!(3)));
    }

    #[test]
    fn test_working_set_update_marks_entry_as_recent() {
        let mut ws = WorkingSet::with_max_entries(2);
        ws.set_task("a", json!(1));
        ws.set_task("b", json!(2));
        ws.set_task("a", json!(11));
        ws.set_task("c", json!(3));

        assert_eq!(ws.len(), 2);
        assert_eq!(ws.get_task("a"), Some(&json!(11)));
        assert_eq!(ws.get_task("b"), None);
        assert_eq!(ws.get_task("c"), Some(&json!(3)));
    }
}
