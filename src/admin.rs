use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::backend::{DynBackend, QueueStats};
use crate::error::Result;
use crate::job::Job;

/// Convenience helper for inspecting queues and dead-letter jobs.
///
/// This sits on top of the lower-level `Backend` trait and exposes a
/// smaller, monitoring-oriented surface that is easy to plug into a
/// dashboard, CLI tool, or HTTP API.
#[derive(Clone)]
pub struct Admin {
    backend: DynBackend,
}

impl Admin {
    /// Create a new `Admin` wrapper around an existing backend.
    pub fn new(backend: DynBackend) -> Self {
        Self { backend }
    }

    /// Obtain stats for a single queue.
    pub async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        self.backend.queue_stats(queue).await
    }

    /// Obtain stats for many queues at once.
    pub async fn multi_queue_stats(&self, queues: &[String]) -> Result<HashMap<String, QueueStats>> {
        let mut out = HashMap::new();
        for q in queues {
            let stats = self.backend.queue_stats(q).await?;
            out.insert(q.clone(), stats);
        }
        Ok(out)
    }

    /// Fetch a single job by id, if it exists.
    pub async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        self.backend.get_job(job_id).await
    }

    /// List jobs that have ended up in the dead-letter bucket.
    pub async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        self.backend.list_dead(limit, offset).await
    }

    /// Retry a job from the dead-letter bucket.
    pub async fn retry_dead(&self, job_id: Uuid) -> Result<()> {
        self.backend.retry_dead(job_id).await
    }

    /// Permanently remove a job from the dead-letter bucket.
    pub async fn delete_dead(&self, job_id: Uuid) -> Result<()> {
        self.backend.delete_dead(job_id).await
    }
}
