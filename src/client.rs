use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::backend::{backend_from_config, DynBackend};
use crate::config::{ClientConfig, EnqueueOptions};
use crate::error::Result;
use crate::handler::JobHandler;
use crate::job::Job;
use crate::metrics::{Metrics, NoopMetrics};

/// High-level entry point for enqueueing jobs into Convoy.
///
/// In a typical application you construct a single `Client` during
/// startup and clone it wherever you need to schedule background work
/// (HTTP handlers, message consumers, CLI commands, ...).
pub struct Client {
    backend: DynBackend,
    metrics: Arc<dyn Metrics>,
    _config: ClientConfig,
}

impl Client {
    /// Construct a new client from high-level configuration.
    ///
    /// For now this always uses the in-memory backend, but the
    /// `BackendConfig` is kept so the API remains stable as more
    /// backends are implemented.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let backend = backend_from_config(&config.backend).await?;
        Ok(Self {
            backend,
            metrics: Arc::new(NoopMetrics::default()),
            _config: config,
        })
    }

    /// Lower-level constructor that lets callers plug in a custom
    /// backend and metrics implementation. This is handy for tests
    /// or when you want to share infrastructure across crates.
    pub fn from_parts(backend: DynBackend, metrics: Arc<dyn Metrics>, config: ClientConfig) -> Self {
        Self {
            backend,
            metrics,
            _config: config,
        }
    }

    /// Enqueue a job to run as soon as capacity is available using
    /// the queue and retry behaviour defined on the job type.
    pub async fn enqueue<H>(&self, args: H::Args) -> Result<()>
    where
        H: JobHandler,
    {
        self.enqueue_with::<H>(args, EnqueueOptions::default()).await
    }

    /// Enqueue a job with additional options, such as overriding the
    /// target queue or adjusting priority.
    pub async fn enqueue_with<H>(&self, args: H::Args, opts: EnqueueOptions) -> Result<()>
    where
        H: JobHandler,
    {
        let payload = serde_json::to_value(args)?;
        let queue = opts.queue.unwrap_or_else(|| H::queue().to_string());
        let priority = opts.priority.unwrap_or(0);
        let job = Job::new(
            queue.clone(),
            H::job_type().to_string(),
            payload,
            H::max_retries(),
            priority,
            None,
        );

        self.metrics
            .job_enqueued(&job.queue, &job.job_type);

        self.backend.push(&job).await
    }

    /// Schedule a job for execution at (or after) the given time.
    /// The job will be picked up by any worker polling the matching
    /// queue once `at` is in the past.
    pub async fn schedule<H>(&self, args: H::Args, at: DateTime<Utc>) -> Result<()>
    where
        H: JobHandler,
    {
        let payload = serde_json::to_value(args)?;
        let queue = H::queue().to_string();
        let job = Job::new(
            queue.clone(),
            H::job_type().to_string(),
            payload,
            H::max_retries(),
            0,
            Some(at),
        );

        self.metrics
            .job_enqueued(&job.queue, &job.job_type);

        self.backend.push(&job).await
    }
}
