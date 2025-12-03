use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::error::Result;
use crate::job::Job;

/// Lightweight handle passed to job handlers so they can learn
/// about the current execution (for now just the job id).
#[derive(Clone)]
pub struct Context {
    job_id: Uuid,
}

impl Context {
    pub fn job_id(&self) -> Uuid {
        self.job_id
    }
}

#[async_trait]
/// Trait implemented by user code to define a background job.
///
/// The design mirrors frameworks like Sidekiq: each job type defines
/// its argument payload, queue, retry policy and the async `perform`
/// function that does the actual work.
pub trait JobHandler: Send + Sync + 'static {
    type Args: Serialize + DeserializeOwned + Send + Sync + 'static;

    fn job_type() -> &'static str;

    fn queue() -> &'static str {
        "default"
    }

    fn max_retries() -> u32 {
        3
    }

    fn timeout() -> Duration {
        Duration::from_secs(300)
    }

    async fn perform(&self, ctx: Context, args: Self::Args) -> Result<()>;
}

#[async_trait]
/// Type-erased representation of a job handler.
///
/// Library code uses this so that different job types can be stored
/// together in a single map keyed by `job_type` strings.
pub trait ErasedHandler: Send + Sync {
    async fn handle(&self, job: Job) -> Result<()>;

    fn job_type(&self) -> &'static str;
}

/// Small adapter that turns a strongly-typed `JobHandler` into an
/// `ErasedHandler` by handling JSON (de)serialization for you.
pub struct TypedHandler<H: JobHandler> {
    inner: H,
}

impl<H: JobHandler> TypedHandler<H> {
    pub fn new(inner: H) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<H> ErasedHandler for TypedHandler<H>
where
    H: JobHandler,
{
    async fn handle(&self, job: Job) -> Result<()> {
        let args: H::Args = serde_json::from_value(job.payload)?;
        let ctx = Context { job_id: job.id };
        self.inner.perform(ctx, args).await
    }

    fn job_type(&self) -> &'static str {
        H::job_type()
    }
}

pub type BoxedHandler = Arc<dyn ErasedHandler>;
