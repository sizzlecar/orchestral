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
        let thread_id = self.bind_session(session_key).await?;
        let request = InteractionSubmitRequest { request_id, input };
        self.api
            .submit_interaction(&thread_id, request)
            .await
            .map_err(Into::into)
    }

    pub async fn query_history(
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

    pub async fn resolve_thread_id(&self, session_key: &str) -> Result<String, ChannelError> {
        self.bind_session(session_key).await
    }

    async fn bind_session(&self, session_key: &str) -> Result<String, ChannelError> {
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
