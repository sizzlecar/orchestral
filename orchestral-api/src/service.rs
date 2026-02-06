use async_trait::async_trait;
use tokio::sync::broadcast;

use orchestral_stores::Event;

use crate::{
    ApiError, HistoryEventView, InteractionSubmitRequest, InteractionSubmitResponse, ThreadView,
};

#[async_trait]
pub trait ApiService: Send + Sync {
    async fn set_thread_id(&self, thread_id: String) -> Result<(), ApiError>;
    async fn thread(&self) -> Result<ThreadView, ApiError>;
    async fn submit_interaction(
        &self,
        request: InteractionSubmitRequest,
    ) -> Result<InteractionSubmitResponse, ApiError>;
    async fn query_history(&self, limit: usize) -> Result<Vec<HistoryEventView>, ApiError>;
    fn subscribe_events(&self) -> broadcast::Receiver<Event>;
}
