//! Convoy: async task orchestration for Rust.
//!
//! This crate aims to feel approachable to humans first. The public
//! API is intentionally small: you define jobs, enqueue them with a
//! `Client`, and execute them with a `Worker` on top of a pluggable
//! backend.

pub mod backend;
pub mod config;
pub mod error;
pub mod handler;
pub mod job;
pub mod metrics;
pub mod client;
pub mod worker;
pub mod admin;

pub use crate::backend::{Backend, QueueStats};
pub use crate::config::{BackoffStrategy, BackendConfig, ClientConfig, EnqueueOptions, QueueConfig, RetryPolicy, WorkerConfig};
pub use crate::error::{ConvoyError, Result};
pub use crate::handler::{Context, JobHandler};
pub use crate::job::{Job, JobState};
pub use crate::metrics::Metrics;
pub use crate::client::Client;
pub use crate::worker::Worker;
pub use crate::admin::Admin;
