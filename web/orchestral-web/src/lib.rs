//! Typed Dioxus client for Orchestral's remote Agent API.
//!
//! State reduction and SSE framing stay platform-neutral so they can be
//! covered by ordinary Rust tests. Browser effects and components are only
//! compiled for the `web` feature.

pub mod model;
pub mod sse;
pub mod state;

#[cfg(feature = "web")]
pub mod app;
#[cfg(feature = "web")]
pub mod browser;
#[cfg(feature = "web")]
pub mod components;
