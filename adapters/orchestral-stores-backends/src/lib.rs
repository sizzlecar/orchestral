//! Redis/Postgres/SQLite store backend implementations.

pub mod event_store;
pub mod reference_store;
pub mod sqlite_store;
pub mod task_store;

pub use event_store::{PostgresEventStore, RedisEventStore};
pub use reference_store::{PostgresReferenceStore, RedisReferenceStore};
pub use sqlite_store::{SqliteEventStore, SqliteReferenceStore, SqliteTaskStore};
pub use task_store::{PostgresTaskStore, RedisTaskStore};

pub use orchestral_core::store::{
    EmbeddingStatus, Event, EventStore, Reference, ReferenceMatch, ReferenceStore, ReferenceType,
    StoreError, TaskStore,
};
