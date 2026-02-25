use std::collections::HashMap;
use std::sync::Arc;

use orchestral_core::store::Event;
use orchestral_runtime::api::{
    ApiError, ApiService, HistoryEventView, InteractionSubmitRequest, InteractionSubmitResponse,
    RuntimeApi,
};
use thiserror::Error;
use tokio::sync::{broadcast, RwLock};
use tracing::debug;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("api error: {0}")]
    Api(#[from] ApiError),
}

struct InMemoryChannelBindingStore {
    bindings: RwLock<HashMap<String, String>>,
}

impl InMemoryChannelBindingStore {
    fn new() -> Self {
        Self {
            bindings: RwLock::new(HashMap::new()),
        }
    }

    async fn get_thread_id(&self, binding_key: &str) -> Option<String> {
        self.bindings.read().await.get(binding_key).cloned()
    }

    async fn set_thread_id(&self, binding_key: &str, thread_id: String) {
        self.bindings
            .write()
            .await
            .insert(binding_key.to_string(), thread_id);
    }
}

struct CliChannel {
    api: Arc<RuntimeApi>,
    binding_store: Arc<InMemoryChannelBindingStore>,
}

impl CliChannel {
    fn new(api: Arc<RuntimeApi>, binding_store: Arc<InMemoryChannelBindingStore>) -> Self {
        Self { api, binding_store }
    }

    async fn submit_input(
        &self,
        session_key: &str,
        request_id: Option<String>,
        input: String,
    ) -> Result<InteractionSubmitResponse, ChannelError> {
        let thread_id = self.bind_session(session_key).await?;
        debug!(
            session_key = %session_key,
            thread_id = %thread_id,
            request_id = ?request_id,
            input_len = input.len(),
            input_preview = %log_preview(&input, 80),
            "submit_chain: cli_channel submit_input"
        );
        let request = InteractionSubmitRequest { request_id, input };
        self.api
            .submit_interaction(&thread_id, request)
            .await
            .map_err(Into::into)
    }

    #[allow(dead_code)]
    async fn query_history(
        &self,
        session_key: &str,
        limit: usize,
    ) -> Result<Vec<HistoryEventView>, ChannelError> {
        let thread_id = self.bind_session(session_key).await?;
        self.api
            .query_history(&thread_id, limit)
            .await
            .map_err(Into::into)
    }

    async fn bind_session(&self, session_key: &str) -> Result<String, ChannelError> {
        if let Some(thread_id) = self.binding_store.get_thread_id(session_key).await {
            debug!(
                session_key = %session_key,
                thread_id = %thread_id,
                "submit_chain: cli_channel reuse thread binding"
            );
            return Ok(thread_id);
        }
        let thread = self.api.create_thread(None).await?;
        debug!(
            session_key = %session_key,
            thread_id = %thread.id,
            "submit_chain: cli_channel created thread binding"
        );
        self.binding_store
            .set_thread_id(session_key, thread.id.clone())
            .await;
        Ok(thread.id)
    }
}

#[derive(Clone)]
pub struct CliRuntime {
    api: Arc<RuntimeApi>,
    channel: Arc<CliChannel>,
    thread_id: String,
    session_key: String,
}

impl CliRuntime {
    pub async fn from_api(
        api: Arc<RuntimeApi>,
        thread_id_override: Option<String>,
    ) -> Result<Self, ChannelError> {
        let binding_store = Arc::new(InMemoryChannelBindingStore::new());
        let thread_id_override_for_log = thread_id_override.clone();
        let thread = match thread_id_override {
            Some(thread_id) => api.create_thread(Some(thread_id)).await?,
            None => api.create_thread(None).await?,
        };
        debug!(
            thread_id = %thread.id,
            thread_id_override = ?thread_id_override_for_log,
            "submit_chain: cli_runtime initialized thread"
        );
        let session_key = format!("cli:{}", thread.id);
        binding_store
            .set_thread_id(&session_key, thread.id.clone())
            .await;
        let channel = Arc::new(CliChannel::new(api.clone(), binding_store));

        Ok(Self {
            api,
            channel,
            thread_id: thread.id,
            session_key,
        })
    }

    pub fn thread_id(&self) -> &str {
        &self.thread_id
    }

    pub async fn subscribe_events(&self) -> Result<broadcast::Receiver<Event>, ChannelError> {
        Ok(self.api.subscribe_events(&self.thread_id).await?)
    }

    pub async fn submit_input(
        &self,
        input: String,
    ) -> Result<InteractionSubmitResponse, ChannelError> {
        debug!(
            thread_id = %self.thread_id,
            session_key = %self.session_key,
            input_len = input.len(),
            input_preview = %log_preview(&input, 80),
            "submit_chain: cli_runtime submit_input"
        );
        self.channel
            .submit_input(&self.session_key, Some("tui".to_string()), input)
            .await
    }

    pub async fn interrupt(&self) -> Result<(), ChannelError> {
        let app = self.api.runtime_app_for_thread(&self.thread_id).await?;
        app.orchestrator.thread_runtime.cancel_all_active().await;
        Ok(())
    }
}

fn log_preview(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}
