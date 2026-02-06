use std::sync::Arc;

use thiserror::Error;

use orchestral_api::{
    ApiError, ApiService, HistoryEventView, InteractionSubmitRequest, InteractionSubmitResponse,
};

use crate::binding::ChannelBindingStore;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("api error: {0}")]
    Api(#[from] ApiError),
}

pub struct CliChannel<A: ApiService, B: ChannelBindingStore> {
    api: Arc<A>,
    binding_store: Arc<B>,
}

impl<A: ApiService, B: ChannelBindingStore> CliChannel<A, B> {
    pub fn new(api: Arc<A>, binding_store: Arc<B>) -> Self {
        Self { api, binding_store }
    }

    pub async fn submit_input(
        &self,
        session_key: &str,
        request_id: Option<String>,
        input: String,
    ) -> Result<InteractionSubmitResponse, ChannelError> {
        self.bind_session(session_key).await?;
        let request = InteractionSubmitRequest { request_id, input };
        self.api
            .submit_interaction(request)
            .await
            .map_err(Into::into)
    }

    pub async fn query_history(
        &self,
        session_key: &str,
        limit: usize,
    ) -> Result<Vec<HistoryEventView>, ChannelError> {
        self.bind_session(session_key).await?;
        self.api.query_history(limit).await.map_err(Into::into)
    }

    async fn bind_session(&self, session_key: &str) -> Result<(), ChannelError> {
        if let Some(thread_id) = self.binding_store.get_thread_id(session_key).await {
            self.api.set_thread_id(thread_id).await?;
            return Ok(());
        }

        let thread = self.api.thread().await?;
        self.binding_store
            .set_thread_id(session_key, thread.id.clone())
            .await;
        self.api.set_thread_id(thread.id).await?;
        Ok(())
    }
}
