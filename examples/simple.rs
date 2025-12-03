//! Small end-to-end example that wires a `Client` and `Worker`
//! together using the in-memory backend. Run with:
//!
//! ```bash
//! cargo run --example simple
//! ```
//!
//! You should see a single line printed from the background job.

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use convoy::{
    BackendConfig, Client, ClientConfig, Context, JobHandler, QueueConfig, RetryPolicy, Worker,
    WorkerConfig,
};

#[derive(Debug, Serialize, Deserialize)]
struct PrintArgs {
    message: String,
}

struct PrintJob;

#[async_trait]
impl JobHandler for PrintJob {
    type Args = PrintArgs;

    fn job_type() -> &'static str {
        "print_message"
    }

    fn queue() -> &'static str {
        "default"
    }

    async fn perform(&self, _ctx: Context, args: Self::Args) -> convoy::Result<()> {
        println!("[{}] job says: {}", Utc::now(), args.message);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a non-Redis backend config here so the example remains fully
    // self-contained. Other backends currently map to the in-memory
    // implementation.
    let backend = BackendConfig::Sqlite {
        path: ":memory:".into(),
    };

    let client_cfg = ClientConfig { backend: backend.clone() };

    let worker_cfg = WorkerConfig {
        backend,
        queues: vec![QueueConfig {
            name: "default".to_string(),
            concurrency: 1,
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

    let mut worker = Worker::new(worker_cfg).await?;
    worker.register(PrintJob);

    // Run the worker in the background
    let worker_task = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("worker error: {e}");
        }
    });

    // Enqueue a job
    client
        .enqueue::<PrintJob>(PrintArgs {
            message: "Hello from convoy in-memory backend".into(),
        })
        .await?;

    // Give the worker some time to process then exit
    tokio::time::sleep(Duration::from_secs(2)).await;

    // For this simple example we just exit; in real applications you would
    // implement graceful shutdown signalling.
    worker_task.abort();

    Ok(())
}
