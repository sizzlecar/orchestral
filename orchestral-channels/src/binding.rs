use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::RwLock;

#[async_trait]
pub trait ChannelBindingStore: Send + Sync {
    async fn get_thread_id(&self, binding_key: &str) -> Option<String>;
    async fn set_thread_id(&self, binding_key: &str, thread_id: String);
}

#[derive(Default)]
pub struct InMemoryChannelBindingStore {
    bindings: RwLock<HashMap<String, String>>,
}

impl InMemoryChannelBindingStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ChannelBindingStore for InMemoryChannelBindingStore {
    async fn get_thread_id(&self, binding_key: &str) -> Option<String> {
        let bindings = self.bindings.read().await;
        bindings.get(binding_key).cloned()
    }

    async fn set_thread_id(&self, binding_key: &str, thread_id: String) {
        let mut bindings = self.bindings.write().await;
        bindings.insert(binding_key.to_string(), thread_id);
    }
}
