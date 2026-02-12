use std::path::PathBuf;
use std::sync::Arc;

use orchestral_api::{ApiService, InteractionSubmitResponse, RuntimeApi};
use orchestral_stores::Event;
use tokio::sync::broadcast;

use crate::{ChannelBindingStore, ChannelError, CliChannel, InMemoryChannelBindingStore};

#[derive(Clone)]
pub struct CliRuntime {
    api: Arc<RuntimeApi>,
    channel: Arc<CliChannel<RuntimeApi, InMemoryChannelBindingStore>>,
    thread_id: String,
    session_key: String,
}

impl CliRuntime {
    pub async fn from_config(
        config: PathBuf,
        thread_id_override: Option<String>,
    ) -> Result<Self, ChannelError> {
        let api = Arc::new(RuntimeApi::from_config_path(config).await?);
        Self::from_api(api, thread_id_override).await
    }

    pub async fn from_api(
        api: Arc<RuntimeApi>,
        thread_id_override: Option<String>,
    ) -> Result<Self, ChannelError> {
        let binding_store = Arc::new(InMemoryChannelBindingStore::new());
        let thread = match thread_id_override {
            Some(thread_id) => api.create_thread(Some(thread_id)).await?,
            None => api.create_thread(None).await?,
        };
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
