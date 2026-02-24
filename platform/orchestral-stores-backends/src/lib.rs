//! Store backend implementations for Orchestral.
//!
//! Enable backends via feature flags:
//! - `sqlite` (default) - SQLite via sqlx
//! - `external` - Redis + PostgreSQL backends
//! - `all` - All backends

#[cfg(feature = "external")]
pub mod event_store;
#[cfg(feature = "external")]
pub mod reference_store;
#[cfg(feature = "sqlite")]
pub mod sqlite_store;
#[cfg(feature = "external")]
pub mod task_store;

#[cfg(feature = "external")]
pub use event_store::{PostgresEventStore, RedisEventStore};
#[cfg(feature = "external")]
pub use reference_store::{PostgresReferenceStore, RedisReferenceStore};
#[cfg(feature = "sqlite")]
pub use sqlite_store::{SqliteEventStore, SqliteReferenceStore, SqliteTaskStore};
#[cfg(feature = "external")]
pub use task_store::{PostgresTaskStore, RedisTaskStore};

pub use orchestral_core::store::{
    EmbeddingStatus, Event, EventStore, Reference, ReferenceMatch, ReferenceStore, ReferenceType,
    StoreError, TaskStore,
};
