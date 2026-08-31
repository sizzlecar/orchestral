//! Browser-only adapters live behind the `web` feature so the state reducer
//! remains testable on the native host.

pub mod api;
pub mod controller;
pub mod platform;
pub mod storage;
