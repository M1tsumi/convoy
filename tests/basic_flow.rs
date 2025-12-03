use std::sync::{atomic::{AtomicU32, Ordering}, Arc};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use convoy::{
    BackendConfig, Client, ClientConfig, Context, JobHandler, QueueConfig, RetryPolicy, Worker,
    WorkerConfig,
};

#[derive(Debug, Serialize, Deserialize)]
struct CounterArgs {
    amount: u32,
}

struct CounterJob {
    counter: Arc<AtomicU32>,
}

#[async_trait]
impl JobHandler for CounterJob {
    type Args = CounterArgs;

    fn job_type() -> &'static str {
        "counter"
    }

    fn queue() -> &'static str {
        "default"
    }

    async fn perform(&self, _ctx: Context, args: Self::Args) -> convoy::Result<()> {
        self.counter.fetch_add(args.amount, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn processes_job_with_in_memory_backend() {
    let counter = Arc::new(AtomicU32::new(0));

    // Use a non-Redis variant so tests do not require Redis to be
    // running. Other backends currently map to the in-memory
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
        scheduled_poll_interval: Duration::from_secs(1),
        orphan_recovery_interval: Duration::from_secs(60),
        orphan_threshold: Duration::from_secs(300),
    };

    let client = Client::new(client_cfg).await.expect("client");

    let mut worker = Worker::new(worker_cfg).await.expect("worker");
    worker.register(CounterJob {
        counter: counter.clone(),
    });

    let worker_handle = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("worker error in test: {e}");
        }
    });

    client
        .enqueue::<CounterJob>(CounterArgs { amount: 1 })
        .await
        .expect("enqueue");

    // Wait until the worker has had a chance to process the job.
    let wait_result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if counter.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    worker_handle.abort();

    assert!(wait_result.is_ok(), "job did not complete in time");
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}
