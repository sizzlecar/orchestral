use async_trait::async_trait;
use tokio::sync::broadcast;

use orchestral_stores::Event;

use crate::{
    ApiError, HistoryEventView, InteractionSubmitRequest, InteractionSubmitResponse, ThreadView,
};

#[async_trait]
pub trait ApiService: Send + Sync {
    async fn create_thread(&self, preferred_id: Option<String>) -> Result<ThreadView, ApiError>;
    async fn get_thread(&self, thread_id: &str) -> Result<ThreadView, ApiError>;
    async fn submit_interaction(
        &self,
        thread_id: &str,
        request: InteractionSubmitRequest,
    ) -> Result<InteractionSubmitResponse, ApiError>;
    async fn query_history(
        &self,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<HistoryEventView>, ApiError>;
    async fn subscribe_events(
        &self,
        thread_id: &str,
    ) -> Result<broadcast::Receiver<Event>, ApiError>;
}
