//! WorkingSet - Scoped KV data container

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkingSet {
    data: HashMap<(Scope, String), Value>,
}

impl WorkingSet {
    /// Create a new empty working set
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a value by scope and key
    pub fn get(&self, scope: &Scope, key: &str) -> Option<&Value> {
        self.data.get(&(scope.clone(), key.to_string()))
    }

    /// Set a value by scope and key
    pub fn set(&mut self, scope: Scope, key: impl Into<String>, value: Value) {
        self.data.insert((scope, key.into()), value);
    }

    /// Remove a value by scope and key
    pub fn remove(&mut self, scope: &Scope, key: &str) -> Option<Value> {
        self.data.remove(&(scope.clone(), key.to_string()))
    }

    /// Check if a key exists in scope
    pub fn contains(&self, scope: &Scope, key: &str) -> bool {
        self.data.contains_key(&(scope.clone(), key.to_string()))
    }

    /// Clear all data in a specific scope
    pub fn clear_scope(&mut self, scope: &Scope) {
        self.data.retain(|(s, _), _| s != scope);
    }

    /// Clear all step scopes
    pub fn clear_all_step_scopes(&mut self) {
        self.data.retain(|(s, _), _| !matches!(s, Scope::Step { .. }));
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
}
