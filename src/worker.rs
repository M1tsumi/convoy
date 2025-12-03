use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use tokio::time::sleep;

use crate::backend::{backend_from_config, DynBackend};
use crate::config::WorkerConfig;
use crate::error::Result;
use crate::handler::{BoxedHandler, JobHandler, TypedHandler};
use crate::metrics::{Metrics, NoopMetrics};

/// A long-running process that pulls jobs from queues and executes
/// registered handlers.
///
/// In production you typically run one or more workers in separate
/// processes or containers alongside your main application.
pub struct Worker {
    backend: DynBackend,
    metrics: Arc<dyn Metrics>,
    config: WorkerConfig,
    handlers: HashMap<String, BoxedHandler>,
    shutdown: Arc<AtomicBool>,
}

impl Worker {
    /// Construct a worker from high-level configuration.
    pub async fn new(config: WorkerConfig) -> Result<Self> {
        let backend = backend_from_config(&config.backend).await?;
        Ok(Self {
            backend,
            metrics: Arc::new(NoopMetrics::default()),
            config,
            handlers: HashMap::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Lower-level constructor that lets you provide a concrete
    /// backend and metrics implementation. Useful for tests or when
    /// wiring Convoy into an existing infrastructure stack.
    pub fn from_parts(
        backend: DynBackend,
        metrics: Arc<dyn Metrics>,
        config: WorkerConfig,
    ) -> Self {
        Self {
            backend,
            metrics,
            config,
            handlers: HashMap::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register a `JobHandler` implementation so this worker knows
    /// how to execute jobs of a given `job_type`.
    pub fn register<H>(&mut self, handler: H)
    where
        H: JobHandler,
    {
        let typed = TypedHandler::new(handler);
        let job_type = H::job_type().to_string();
        self.handlers.insert(job_type, Arc::new(typed));
    }

    /// Request that the worker stops processing new jobs.
    ///
    /// This method is cooperative: the main loop checks the flag
    /// between polling cycles and will return from `run` shortly
    /// after shutdown is requested.
    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Main worker loop.
    ///
    /// This method only returns if the underlying task finishes or
    /// panics. In a real deployment you would typically run it in a
    /// dedicated Tokio runtime and control shutdown via cancellation.
    pub async fn run(&self) -> Result<()> {
        let queues: Vec<String> = self
            .config
            .queues
            .iter()
            .map(|q| q.name.clone())
            .collect();
        let backend = self.backend.clone();
        let handlers = self.handlers.clone();
        let metrics = self.metrics.clone();
        let heartbeat_interval = self.config.heartbeat_interval;
        let scheduled_poll_interval = self.config.scheduled_poll_interval;
        let queue_configs = self.config.queues.clone();
        let shutdown = self.shutdown.clone();
        let orphan_recovery_interval = self.config.orphan_recovery_interval;
        let orphan_threshold = self.config.orphan_threshold;

        let worker_task = tokio::spawn(async move {
            let mut last_orphan_recovery = Utc::now();
            loop {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                let now = Utc::now();
                let _ = backend.enqueue_scheduled(now).await;

                if let Ok(Some(job)) = backend.pop(&queues, Duration::from_secs(1)).await {
                    if let Some(handler) = handlers.get(&job.job_type) {
                        metrics.job_started(&job.queue, &job.job_type);
                        let start = std::time::Instant::now();
                        let job_id = job.id;

                        let hb_backend = backend.clone();
                        let hb_interval = heartbeat_interval;
                        let hb_stop = Arc::new(AtomicBool::new(false));
                        let hb_stop_flag = hb_stop.clone();

                        let heartbeat_handle = tokio::spawn(async move {
                            loop {
                                if hb_stop_flag.load(Ordering::SeqCst) {
                                    break;
                                }
                                tokio::time::sleep(hb_interval).await;
                                let _ = hb_backend.heartbeat(job_id).await;
                            }
                        });

                        let res = handler.handle(job.clone()).await;
                        hb_stop.store(true, Ordering::SeqCst);
                        let _ = heartbeat_handle.await;
                        let duration = start.elapsed();

                        match res {
                            Ok(()) => {
                                let _ = backend.ack(job.id).await;
                                metrics.job_completed(&job.queue, &job.job_type, duration);
                            }
                            Err(err) => {
                                // Determine queue configuration
                                let queue_cfg = queue_configs
                                    .iter()
                                    .find(|q| q.name == job.queue)
                                    .or_else(|| queue_configs.first());

                                let (will_retry, run_at) = if let Some(cfg) = queue_cfg {
                                    let attempt = job.retry_count + 1;
                                    if let Some(delay) = cfg.retry_policy.next_delay(attempt) {
                                        let chrono_delay = ChronoDuration::from_std(delay)
                                            .unwrap_or_else(|_| ChronoDuration::seconds(0));
                                        (true, Utc::now() + chrono_delay)
                                    } else {
                                        (false, Utc::now())
                                    }
                                } else {
                                    (false, Utc::now())
                                };

                                metrics.job_failed(&job.queue, &job.job_type, will_retry);
                                if will_retry {
                                    let _ = backend
                                        .nack(job.id, &err.to_string(), run_at)
                                        .await;
                                } else {
                                    let _ = backend.fail(job.id, &err.to_string()).await;
                                }
                            }
                        }
                    } else {
                        let _ = backend
                            .fail(job.id, "handler not found")
                            .await;
                    }
                } else {
                    sleep(Duration::from_millis(100)).await;
                }
                let now = Utc::now();
                if (now - last_orphan_recovery)
                    >= ChronoDuration::from_std(orphan_recovery_interval)
                        .unwrap_or_else(|_| ChronoDuration::seconds(0))
                {
                    let _ = backend.recover_orphans(orphan_threshold).await;
                    last_orphan_recovery = now;
                }
                sleep(scheduled_poll_interval).await;
            }
        });

        let _ = worker_task.await;
        Ok(())
    }
}
