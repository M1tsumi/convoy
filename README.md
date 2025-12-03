# ConvoyX


Async task orchestration for Rust with pluggable backends (Redis, PostgreSQL, SQLite).

Convoy is inspired by systems like Sidekiq: you define jobs in normal Rust code, enqueue them from your application, and run one or more worker processes to execute those jobs in the background.

---

## Features

- **Simple API** – define a `JobHandler`, enqueue it with a `Client`, run it with a `Worker`.
- **Multiple backends**:
  - Redis – high-throughput, low-latency queues.
  - PostgreSQL – transactional, RDBMS-backed queues.
  - SQLite – embedded storage, great for local/dev or small deployments.
- **Retries with backoff** – configurable retry policy per queue.
- **Scheduling** – enqueue jobs to run in the future.
- **Dead-letter queue** – failed jobs are retained for inspection and manual retry.
- **Heartbeats & orphan recovery** – for robust crash handling (Redis/Postgres).
- **Admin helper** – light-weight API for queue stats and dead-letter management.

---

## Getting started

Add this crate to your `Cargo.toml`:

```toml
[dependencies]
convoyx = "0.1"
```

### Define a job

```rust
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use convoyx::{Context, JobHandler, Result};

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

    async fn perform(&self, _ctx: Context, args: Self::Args) -> Result<()> {
        println!("[{}] job says: {}", Utc::now(), args.message);
        Ok(())
    }
}
```

### Run a worker and enqueue jobs (SQLite in-memory)

This is essentially what `examples/simple.rs` does:

```rust,no_run
use std::time::Duration;

use convoyx::{
    BackendConfig, Client, ClientConfig, QueueConfig, RetryPolicy, Worker, WorkerConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // In-memory SQLite backend (no external services required).
    let backend = BackendConfig::Sqlite { path: ":memory:".into() };

    let client_cfg = ClientConfig { backend: backend.clone() };
    let worker_cfg = WorkerConfig {
        backend,
        queues: vec![QueueConfig {
            name: "default".to_string(),
            concurrency: 1,
            retry_policy: RetryPolicy::default(),
            timeout: Duration::from_secs(30),
        }],
        ..Default::default()
    };

    let client = Client::new(client_cfg).await?;

    let mut worker = Worker::new(worker_cfg).await?;
    worker.register(PrintJob);

    // Run the worker in the background.
    let worker_task = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("worker error: {e}");
        }
    });

    // Enqueue a job.
    client
        .enqueue::<PrintJob>(PrintArgs {
            message: "Hello from convoy".into(),
        })
        .await?;

    tokio::time::sleep(Duration::from_secs(2)).await;
    worker_task.abort();

    Ok(())
}
```

You can run this example with:

```bash
cargo run --example simple
```

---

## Backends

Select a backend via `BackendConfig` in your `ClientConfig` / `WorkerConfig`.

### SQLite

Embedded database, no external service required.

```rust
use convoyx::BackendConfig;

let backend = BackendConfig::Sqlite {
    // File-backed database
    path: "convoy.db".into(),
};

let in_memory = BackendConfig::Sqlite {
    // Special case: uses an in-memory SQLite database
    path: ":memory:".into(),
};
```

### Redis

```rust
use convoy::BackendConfig;

let backend = BackendConfig::Redis {
    url: "redis://127.0.0.1/".into(),
    pool_size: None,
};
```

See `examples/redis_simple.rs`:

```bash
REDIS_URL=redis://127.0.0.1/ cargo run --example redis_simple
```

### PostgreSQL

```rust
use convoy::BackendConfig;

let backend = BackendConfig::Postgres {
    url: "postgres://user:pass@localhost/convoy".into(),
    pool_size: None,
};
```

You’ll need a `convoy_jobs` table similar to:

```sql
CREATE TABLE convoy_jobs (
    id UUID PRIMARY KEY,
    queue VARCHAR(255) NOT NULL,
    job_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    state VARCHAR(32) NOT NULL,
    priority INT NOT NULL,
    retry_count INT NOT NULL,
    max_retries INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    scheduled_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT
);
```

---

## Admin / monitoring API

The `Admin` helper exposes a small, monitoring-oriented surface on top of a backend:

```rust,no_run
use convoyx::{Admin, BackendConfig, ClientConfig, Client};

#[tokio::main]
async fn main() -> convoyx::Result<()> {
    let backend = BackendConfig::Sqlite { path: "convoy.db".into() };

    let client = Client::new(ClientConfig { backend }).await?;
    let admin = Admin::new(client.backend());

    let stats = admin.queue_stats("default").await?;
    println!("pending={}, scheduled={}, dead={}", stats.pending, stats.scheduled, stats.dead);

    Ok(())
}
```

You can use this to back an HTTP API, CLI, or dashboard.

---

## Running tests and examples

- Run unit tests:

  ```bash
  cargo test
  ```

- Run the self-contained example (SQLite in-memory):

  ```bash
  cargo run --example simple
  ```

- Run the Redis example (requires a Redis server):

  ```bash
  REDIS_URL=redis://127.0.0.1/ cargo run --example redis_simple
  ```

---

## Documentation

Once published on crates.io, API documentation will be available on [docs.rs](https://docs.rs/convoyx).

You can also build docs locally with:

```bash
cargo doc --no-deps --open
```

