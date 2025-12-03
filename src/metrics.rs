use std::time::Duration;

/// Hook points for integrating Convoy with your observability stack.
///
/// You can implement this trait for Prometheus, StatsD, OpenTelemetry,
/// or whatever you use to ship metrics in production.
pub trait Metrics: Send + Sync {
    fn job_enqueued(&self, queue: &str, job_type: &str);
    fn job_started(&self, queue: &str, job_type: &str);
    fn job_completed(&self, queue: &str, job_type: &str, duration: Duration);
    fn job_failed(&self, queue: &str, job_type: &str, will_retry: bool);
    fn queue_depth(&self, queue: &str, depth: u64);
}

#[derive(Clone, Default)]
/// Default no-op metrics implementation.
///
/// This keeps the core library free from mandatory dependencies
/// while still making it easy to plug in real metrics later.
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn job_enqueued(&self, _queue: &str, _job_type: &str) {}

    fn job_started(&self, _queue: &str, _job_type: &str) {}

    fn job_completed(&self, _queue: &str, _job_type: &str, _duration: Duration) {}

    fn job_failed(&self, _queue: &str, _job_type: &str, _will_retry: bool) {}

    fn queue_depth(&self, _queue: &str, _depth: u64) {}
}
