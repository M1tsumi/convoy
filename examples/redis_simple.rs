//! Example showing how to run Convoy against a real Redis instance.
//!
//! This assumes you have Redis running locally and reachable via the
//! connection string below. You can override the URL with the
//! `REDIS_URL` environment variable.
//!
//! ```bash
//! # start redis in the background (example)
//! redis-server &
//!
//! # run the example
//! REDIS_URL=redis://127.0.0.1/ cargo run --example redis_simple
//! ```

use std::env;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use convoy::{
    BackendConfig, Client, ClientConfig, Context, JobHandler, QueueConfig, RetryPolicy, Worker,
    WorkerConfig,
};

#[derive(Debug, Serialize, Deserialize)]
struct LogArgs {
    message: String,
}

struct LogJob;

#[async_trait]
impl JobHandler for LogJob {
    type Args = LogArgs;

    fn job_type() -> &'static str {
        "log_message"
    }

    fn queue() -> &'static str {
        "logs"
    }

    async fn perform(&self, _ctx: Context, args: Self::Args) -> convoy::Result<()> {
        println!("[{}] redis job: {}", Utc::now(), args.message);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

    let backend = BackendConfig::Redis {
        url,
        pool_size: None,
    };

    let client_cfg = ClientConfig {
        backend: backend.clone(),
    };

    let worker_cfg = WorkerConfig {
        backend,
        queues: vec![QueueConfig {
            name: "logs".to_string(),
            concurrency: 2,
            retry_policy: RetryPolicy::default(),
            timeout: Duration::from_secs(30),
        }],
        shutdown_timeout: Duration::from_secs(30),
        heartbeat_interval: Duration::from_secs(5),
        scheduled_poll_interval: Duration::from_secs(5),
        orphan_recovery_interval: Duration::from_secs(60),
        orphan_threshold: Duration::from_secs(300),
    };

    let client = Client::new(client_cfg).await?;

    let worker = Worker::new(worker_cfg).await?;

    // Run the worker in the background
    let worker_task = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("worker error (redis example): {e}");
        }
    });

    // Enqueue a couple of jobs
    client
        .enqueue::<LogJob>(LogArgs {
            message: "hello from redis convoy".into(),
        })
        .await?;

    client
        .enqueue::<LogJob>(LogArgs {
            message: "another job via redis".into(),
        })
        .await?;

    // Give the worker a moment to process then abort the task.
    tokio::time::sleep(Duration::from_secs(3)).await;
    worker_task.abort();

    Ok(())
}
