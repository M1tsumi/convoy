use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// High-level configuration types used by `Client` and `Worker`.
///
/// These are designed to be serializable so you can load them from
/// files or environment-specific config management if you like.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub backend: BackendConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub backend: BackendConfig,
    pub queues: Vec<QueueConfig>,
    pub shutdown_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub scheduled_poll_interval: Duration,
    pub orphan_recovery_interval: Duration,
    pub orphan_threshold: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackendConfig {
    Redis { url: String, pool_size: Option<usize> },
    Postgres { url: String, pool_size: Option<usize> },
    Sqlite { path: PathBuf },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub name: String,
    pub concurrency: usize,
    pub retry_policy: RetryPolicy,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    Fixed(Duration),
    Exponential { base: Duration, max: Duration },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnqueueOptions {
    pub queue: Option<String>,
    pub priority: Option<i32>,
    pub unique_for: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategy::Exponential {
                base: Duration::from_secs(15),
                max: Duration::from_secs(3600),
            },
        }
    }
}

impl RetryPolicy {
    /// Compute the delay before the next attempt for a given
    /// zero-based retry counter.
    ///
    /// `attempt` here is the number of times the job has already run.
    /// When this is greater than or equal to `max_attempts`, `None`
    /// is returned to signal that the job should be sent to the dead
    /// letter queue.
    pub fn next_delay(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_attempts {
            return None;
        }

        match &self.backoff {
            BackoffStrategy::Fixed(d) => Some(*d),
            BackoffStrategy::Exponential { base, max } => {
                let exp = 2u32.saturating_pow(attempt.min(16));
                let millis = base.as_millis().saturating_mul(exp as u128);
                let max_millis = max.as_millis();
                let clamped = std::cmp::min(millis, max_millis);
                Some(Duration::from_millis(clamped as u64))
            }
        }
    }
}
