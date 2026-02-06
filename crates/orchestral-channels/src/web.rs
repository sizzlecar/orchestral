use std::sync::Arc;

use orchestral_api::{ApiError, ApiService, InteractionSubmitRequest, InteractionSubmitResponse};

use crate::binding::ChannelBindingStore;

pub struct WebChannel<A: ApiService, B: ChannelBindingStore> {
    api: Arc<A>,
    binding_store: Arc<B>,
}

impl<A: ApiService, B: ChannelBindingStore> WebChannel<A, B> {
    pub fn new(api: Arc<A>, binding_store: Arc<B>) -> Self {
        Self { api, binding_store }
    }

    pub async fn submit_message(
        &self,
        session_key: &str,
        request_id: Option<String>,
        input: String,
    ) -> Result<InteractionSubmitResponse, ApiError> {
        let thread_id = self.bind_session(session_key).await?;
        self.api
            .submit_interaction(&thread_id, InteractionSubmitRequest { request_id, input })
            .await
    }

    pub async fn resolve_thread_id(&self, session_key: &str) -> Result<String, ApiError> {
        self.bind_session(session_key).await
    }

    async fn bind_session(&self, session_key: &str) -> Result<String, ApiError> {
        if let Some(thread_id) = self.binding_store.get_thread_id(session_key).await {
            return Ok(thread_id);
        }

        let thread = self.api.create_thread(None).await?;
        self.binding_store
            .set_thread_id(session_key, thread.id.clone())
            .await;
        Ok(thread.id)
    }
}
