use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_postgres::{Client as PgClient, NoTls, Row as PgRow};
use sqlx::{sqlite::{SqlitePoolOptions, SqliteRow}, SqlitePool, Row};
use uuid::Uuid;

use crate::config::BackendConfig;
use crate::error::{BackendError, ConvoyError, Result};
use crate::job::{Job, JobState};

/// High-level snapshot of a queue's health.
///
/// The exact semantics of what counts as "pending" or "running"
/// depend on the backend, but the shape is stable across them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub pending: u64,
    pub scheduled: u64,
    pub running: u64,
    pub dead: u64,
}

#[async_trait]
/// Abstraction over the storage engine used to persist and deliver
/// jobs.
///
/// Backends are responsible for durability, visibility timeouts,
/// scheduling and crash recovery. The rest of the crate only talks to
/// this trait, which keeps higher-level code easy to reason about.
pub trait Backend: Send + Sync + 'static {
    async fn push(&self, job: &Job) -> Result<()>;

    async fn pop(&self, queues: &[String], timeout: Duration) -> Result<Option<Job>>;

    async fn ack(&self, job_id: Uuid) -> Result<()>;

    async fn nack(&self, job_id: Uuid, error: &str, run_at: DateTime<Utc>) -> Result<()>;

    async fn fail(&self, job_id: Uuid, error: &str) -> Result<()>;

    async fn enqueue_scheduled(&self, now: DateTime<Utc>) -> Result<u64>;

    async fn heartbeat(&self, job_id: Uuid) -> Result<()>;

    async fn recover_orphans(&self, older_than: Duration) -> Result<u64>;

    async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>>;

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats>;

    async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<Job>>;

    async fn retry_dead(&self, job_id: Uuid) -> Result<()>;

    async fn delete_dead(&self, job_id: Uuid) -> Result<()>;
}

fn state_to_str(state: JobState) -> &'static str {
    match state {
        JobState::Pending => "pending",
        JobState::Scheduled => "scheduled",
        JobState::Running => "running",
        JobState::Completed => "completed",
        JobState::Failed => "failed",
        JobState::Dead => "dead",
    }
}

fn state_from_str(s: &str) -> Result<JobState> {
    let state = match s {
        "pending" => JobState::Pending,
        "scheduled" => JobState::Scheduled,
        "running" => JobState::Running,
        "completed" => JobState::Completed,
        "failed" => JobState::Failed,
        "dead" => JobState::Dead,
        other => {
            return Err(ConvoyError::Other(format!(
                "unknown job state in database: {other}"
            )));
        }
    };
    Ok(state)
}

fn row_to_job(row: &PgRow) -> Result<Job> {
    Ok(Job {
        id: row.get("id"),
        queue: row.get("queue"),
        job_type: row.get("job_type"),
        payload: row.get("payload"),
        state: state_from_str(row.get::<_, &str>("state"))?,
        priority: row.get("priority"),
        retry_count: row.get::<_, i32>("retry_count") as u32,
        max_retries: row.get::<_, i32>("max_retries") as u32,
        created_at: row.get("created_at"),
        scheduled_at: row.get("scheduled_at"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        error: row.get("error"),
    })
}

fn sqlite_row_to_job(row: &SqliteRow) -> Result<Job> {
    fn parse_dt(s: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(s)
            .map_err(|e| ConvoyError::Other(e.to_string()))?
            .with_timezone(&Utc))
    }

    fn parse_opt_dt(opt: Option<String>) -> Result<Option<DateTime<Utc>>> {
        if let Some(s) = opt {
            Ok(Some(parse_dt(&s)?))
        } else {
            Ok(None)
        }
    }

    let id_str: String = row.get::<String, _>("id");
    let id = Uuid::parse_str(&id_str).map_err(|e| ConvoyError::Other(e.to_string()))?;

    let queue: String = row.get::<String, _>("queue");
    let job_type: String = row.get::<String, _>("job_type");

    let payload_str: String = row.get::<String, _>("payload");
    let payload: serde_json::Value = serde_json::from_str(&payload_str)
        .map_err(|e| ConvoyError::Other(e.to_string()))?;

    let state_str: String = row.get::<String, _>("state");
    let state = state_from_str(&state_str)?;

    let priority: i32 = row.get::<i32, _>("priority");
    let retry_count: i32 = row.get::<i32, _>("retry_count");
    let max_retries: i32 = row.get::<i32, _>("max_retries");

    let created_at_str: String = row.get::<String, _>("created_at");
    let created_at = parse_dt(&created_at_str)?;

    let scheduled_at = parse_opt_dt(row.get::<Option<String>, _>("scheduled_at"))?;
    let started_at = parse_opt_dt(row.get::<Option<String>, _>("started_at"))?;
    let completed_at = parse_opt_dt(row.get::<Option<String>, _>("completed_at"))?;

    let error: Option<String> = row.get::<Option<String>, _>("error");

    Ok(Job {
        id,
        queue,
        job_type,
        payload,
        state,
        priority,
        retry_count: retry_count as u32,
        max_retries: max_retries as u32,
        created_at,
        scheduled_at,
        started_at,
        completed_at,
        error,
    })
}

#[derive(Default)]
/// Minimal in-memory backend implementation.
///
/// This is primarily intended for tests, development and examples.
/// It keeps all state in a `Vec<Job>` guarded by a mutex.
struct InMemoryState {
    jobs: Vec<Job>,
}

#[derive(Clone, Default)]
/// A clonable handle to the in-memory backend.
pub struct InMemoryBackend {
    state: Arc<Mutex<InMemoryState>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Backend for InMemoryBackend {
    async fn push(&self, job: &Job) -> Result<()> {
        let mut state = self.state.lock().await;
        state.jobs.push(job.clone());
        Ok(())
    }

    async fn pop(&self, queues: &[String], _timeout: Duration) -> Result<Option<Job>> {
        let mut state = self.state.lock().await;
        if queues.is_empty() {
            return Ok(None);
        }

        if let Some(pos) = state
            .jobs
            .iter()
            .position(|j| j.state == JobState::Pending && queues.contains(&j.queue))
        {
            let mut job = state.jobs.remove(pos);
            job.state = JobState::Running;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    async fn ack(&self, job_id: Uuid) -> Result<()> {
        let mut state = self.state.lock().await;
        state.jobs.retain(|j| j.id != job_id);
        Ok(())
    }

    async fn nack(&self, job_id: Uuid, error: &str, run_at: DateTime<Utc>) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(job) = state.jobs.iter_mut().find(|j| j.id == job_id) {
            job.state = JobState::Scheduled;
            job.retry_count += 1;
            job.scheduled_at = Some(run_at);
            job.error = Some(error.to_string());
        }
        Ok(())
    }

    async fn fail(&self, job_id: Uuid, error: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(job) = state.jobs.iter_mut().find(|j| j.id == job_id) {
            job.state = JobState::Dead;
            job.error = Some(error.to_string());
        }
        Ok(())
    }

    async fn enqueue_scheduled(&self, now: DateTime<Utc>) -> Result<u64> {
        let mut state = self.state.lock().await;
        let mut count = 0;
        for job in state.jobs.iter_mut() {
            if job.state == JobState::Scheduled {
                if let Some(when) = job.scheduled_at {
                    if when <= now {
                        job.state = JobState::Pending;
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }

    async fn heartbeat(&self, _job_id: Uuid) -> Result<()> {
        Ok(())
    }

    async fn recover_orphans(&self, _older_than: Duration) -> Result<u64> {
        Ok(0)
    }

    async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        let state = self.state.lock().await;
        Ok(state.jobs.iter().find(|j| j.id == job_id).cloned())
    }

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        let state = self.state.lock().await;
        let mut stats = QueueStats {
            pending: 0,
            scheduled: 0,
            running: 0,
            dead: 0,
        };

        for job in state.jobs.iter().filter(|j| j.queue == queue) {
            match job.state {
                JobState::Pending => stats.pending += 1,
                JobState::Scheduled => stats.scheduled += 1,
                JobState::Running => stats.running += 1,
                JobState::Dead => stats.dead += 1,
                _ => {}
            }
        }

        Ok(stats)
    }

    async fn list_dead(&self, _limit: usize, _offset: usize) -> Result<Vec<Job>> {
        let state = self.state.lock().await;
        Ok(state
            .jobs
            .iter()
            .filter(|j| j.state == JobState::Dead)
            .cloned()
            .collect())
    }

    async fn retry_dead(&self, job_id: Uuid) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(job) = state.jobs.iter_mut().find(|j| j.id == job_id) {
            job.state = JobState::Pending;
            job.error = None;
        }
        Ok(())
    }

    async fn delete_dead(&self, job_id: Uuid) -> Result<()> {
        let mut state = self.state.lock().await;
        state.jobs.retain(|j| j.id != job_id);
        Ok(())
    }
}

struct PostgresBackend {
    client: PgClient,
}

impl PostgresBackend {
    fn new(client: PgClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    async fn push(&self, job: &Job) -> Result<()> {
        let state = state_to_str(job.state);
        self.client
            .execute(
                "INSERT INTO convoy_jobs (
                    id, queue, job_type, payload, state, priority,
                    retry_count, max_retries, created_at, scheduled_at,
                    started_at, completed_at, error
                 ) VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10,
                    $11, $12, $13
                 )
                 ON CONFLICT (id) DO UPDATE SET
                    queue = EXCLUDED.queue,
                    job_type = EXCLUDED.job_type,
                    payload = EXCLUDED.payload,
                    state = EXCLUDED.state,
                    priority = EXCLUDED.priority,
                    retry_count = EXCLUDED.retry_count,
                    max_retries = EXCLUDED.max_retries,
                    created_at = EXCLUDED.created_at,
                    scheduled_at = EXCLUDED.scheduled_at,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    error = EXCLUDED.error",
                &[&job.id,
                  &job.queue,
                  &job.job_type,
                  &job.payload,
                  &state,
                  &job.priority,
                  &(job.retry_count as i32),
                  &(job.max_retries as i32),
                  &job.created_at,
                  &job.scheduled_at,
                  &job.started_at,
                  &job.completed_at,
                  &job.error],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn pop(&self, queues: &[String], _timeout: Duration) -> Result<Option<Job>> {
        if queues.is_empty() {
            return Ok(None);
        }

        let queue_slices: Vec<&str> = queues.iter().map(|q| q.as_str()).collect();

        let rows = self
            .client
            .query(
                "UPDATE convoy_jobs SET
                    state = 'running',
                    started_at = NOW()
                 WHERE id = (
                    SELECT id FROM convoy_jobs
                    WHERE queue = ANY($1) AND state = 'pending'
                    ORDER BY priority ASC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                 )
                 RETURNING id, queue, job_type, payload, state, priority,
                           retry_count, max_retries, created_at, scheduled_at,
                           started_at, completed_at, error",
                &[&queue_slices],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        if let Some(row) = rows.into_iter().next() {
            Ok(Some(row_to_job(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn ack(&self, job_id: Uuid) -> Result<()> {
        self.client
            .execute("DELETE FROM convoy_jobs WHERE id = $1", &[&job_id])
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, job_id: Uuid, error: &str, run_at: DateTime<Utc>) -> Result<()> {
        self.client
            .execute(
                "UPDATE convoy_jobs
                 SET state = 'scheduled',
                     retry_count = retry_count + 1,
                     scheduled_at = $2,
                     error = $3
                 WHERE id = $1",
                &[&job_id, &run_at, &error],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn fail(&self, job_id: Uuid, error: &str) -> Result<()> {
        self.client
            .execute(
                "UPDATE convoy_jobs
                 SET state = 'dead',
                     error = $2
                 WHERE id = $1",
                &[&job_id, &error],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn enqueue_scheduled(&self, now: DateTime<Utc>) -> Result<u64> {
        let updated = self
            .client
            .execute(
                "UPDATE convoy_jobs
                 SET state = 'pending'
                 WHERE state = 'scheduled' AND scheduled_at <= $1",
                &[&now],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(updated)
    }

    async fn heartbeat(&self, job_id: Uuid) -> Result<()> {
        self.client
            .execute(
                "UPDATE convoy_jobs
                 SET started_at = NOW()
                 WHERE id = $1",
                &[&job_id],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn recover_orphans(&self, older_than: Duration) -> Result<u64> {
        let seconds = older_than.as_secs() as i64;
        let updated = self
            .client
            .execute(
                "UPDATE convoy_jobs
                 SET state = 'pending', started_at = NULL
                 WHERE state = 'running'
                   AND started_at < NOW() - ($1::bigint * INTERVAL '1 second')",
                &[&seconds],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(updated)
    }

    async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        let rows = self
            .client
            .query(
                "SELECT id, queue, job_type, payload, state, priority,
                        retry_count, max_retries, created_at, scheduled_at,
                        started_at, completed_at, error
                 FROM convoy_jobs
                 WHERE id = $1",
                &[&job_id],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        if let Some(row) = rows.into_iter().next() {
            Ok(Some(row_to_job(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        let rows = self
            .client
            .query(
                "SELECT
                    COUNT(*) FILTER (WHERE state = 'pending')   AS pending,
                    COUNT(*) FILTER (WHERE state = 'scheduled') AS scheduled,
                    COUNT(*) FILTER (WHERE state = 'running')   AS running,
                    COUNT(*) FILTER (WHERE state = 'dead')      AS dead
                 FROM convoy_jobs
                 WHERE queue = $1",
                &[&queue],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| ConvoyError::Other("missing stats row".to_string()))?;

        Ok(QueueStats {
            pending: row.get::<_, i64>("pending") as u64,
            scheduled: row.get::<_, i64>("scheduled") as u64,
            running: row.get::<_, i64>("running") as u64,
            dead: row.get::<_, i64>("dead") as u64,
        })
    }

    async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        let rows = self
            .client
            .query(
                "SELECT id, queue, job_type, payload, state, priority,
                        retry_count, max_retries, created_at, scheduled_at,
                        started_at, completed_at, error
                 FROM convoy_jobs
                 WHERE state = 'dead'
                 ORDER BY created_at DESC
                 LIMIT $1 OFFSET $2",
                &[&(limit as i64), &(offset as i64)],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            jobs.push(row_to_job(row)?);
        }
        Ok(jobs)
    }

    async fn retry_dead(&self, job_id: Uuid) -> Result<()> {
        self.client
            .execute(
                "UPDATE convoy_jobs
                 SET state = 'pending', error = NULL
                 WHERE id = $1 AND state = 'dead'",
                &[&job_id],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn delete_dead(&self, job_id: Uuid) -> Result<()> {
        self.client
            .execute(
                "DELETE FROM convoy_jobs
                 WHERE id = $1 AND state = 'dead'",
                &[&job_id],
            )
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }
}

struct SqliteBackend {
    pool: SqlitePool,
}

impl SqliteBackend {
    async fn from_path(path: &std::path::Path) -> Result<Self> {
        let conn_str = if path.to_string_lossy() == ":memory:" {
            "sqlite::memory:".to_string()
        } else {
            format!("sqlite://{}", path.to_string_lossy())
        };

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&conn_str)
            .await
            .map_err(|e| BackendError::Connection(e.to_string()))?;

        // Basic schema bootstrap; safe to run repeatedly.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS convoy_jobs (
                id TEXT PRIMARY KEY,
                queue TEXT NOT NULL,
                job_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                state TEXT NOT NULL,
                priority INTEGER NOT NULL,
                retry_count INTEGER NOT NULL,
                max_retries INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                scheduled_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                error TEXT
             );",
        )
        .execute(&pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl Backend for SqliteBackend {
    async fn push(&self, job: &Job) -> Result<()> {
        let state = state_to_str(job.state);
        let payload = serde_json::to_string(&job.payload)
            .map_err(|e| ConvoyError::Other(e.to_string()))?;

        sqlx::query(
            "INSERT INTO convoy_jobs (
                id, queue, job_type, payload, state, priority,
                retry_count, max_retries, created_at, scheduled_at,
                started_at, completed_at, error
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,
                ?7, ?8, ?9, ?10,
                ?11, ?12, ?13
             )
             ON CONFLICT(id) DO UPDATE SET
                queue = excluded.queue,
                job_type = excluded.job_type,
                payload = excluded.payload,
                state = excluded.state,
                priority = excluded.priority,
                retry_count = excluded.retry_count,
                max_retries = excluded.max_retries,
                created_at = excluded.created_at,
                scheduled_at = excluded.scheduled_at,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                error = excluded.error",
        )
        .bind(job.id.to_string())
        .bind(&job.queue)
        .bind(&job.job_type)
        .bind(&payload)
        .bind(&state)
        .bind(job.priority)
        .bind(job.retry_count as i32)
        .bind(job.max_retries as i32)
        .bind(job.created_at.to_rfc3339())
        .bind(job.scheduled_at.map(|d| d.to_rfc3339()))
        .bind(job.started_at.map(|d| d.to_rfc3339()))
        .bind(job.completed_at.map(|d| d.to_rfc3339()))
        .bind(&job.error)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;

        Ok(())
    }

    async fn pop(&self, queues: &[String], _timeout: Duration) -> Result<Option<Job>> {
        if queues.is_empty() {
            return Ok(None);
        }

        // Build a dynamic IN clause for the queue list.
        let mut sql = String::from(
            "SELECT id, queue, job_type, payload, state, priority,
                    retry_count, max_retries, created_at, scheduled_at,
                    started_at, completed_at, error
             FROM convoy_jobs
             WHERE state = 'pending' AND queue IN (",
        );
        for (i, _) in queues.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push('?');
        }
        sql.push_str(") ORDER BY priority ASC, created_at ASC LIMIT 1");

        let mut query = sqlx::query(&sql);
        for q in queues {
            query = query.bind(q);
        }

        if let Some(row) = query
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?
        {
            let id_str: String = row.get("id");
            sqlx::query(
                "UPDATE convoy_jobs
                 SET state = 'running', started_at = ?2
                 WHERE id = ?1",
            )
            .bind(&id_str)
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

            Ok(Some(sqlite_row_to_job(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn ack(&self, job_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM convoy_jobs WHERE id = ?1")
            .bind(job_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, job_id: Uuid, error: &str, run_at: DateTime<Utc>) -> Result<()> {
        sqlx::query(
            "UPDATE convoy_jobs
             SET state = 'scheduled',
                 retry_count = retry_count + 1,
                 scheduled_at = ?2,
                 error = ?3
             WHERE id = ?1",
        )
        .bind(job_id.to_string())
        .bind(run_at.to_rfc3339())
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn fail(&self, job_id: Uuid, error: &str) -> Result<()> {
        sqlx::query(
            "UPDATE convoy_jobs
             SET state = 'dead',
                 error = ?2
             WHERE id = ?1",
        )
        .bind(job_id.to_string())
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn enqueue_scheduled(&self, now: DateTime<Utc>) -> Result<u64> {
        let updated = sqlx::query(
            "UPDATE convoy_jobs
             SET state = 'pending'
             WHERE state = 'scheduled' AND scheduled_at <= ?1",
        )
        .bind(now.to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(updated.rows_affected())
    }

    async fn heartbeat(&self, _job_id: Uuid) -> Result<()> {
        // For now we keep SQLite's heartbeat as a no-op; most SQLite
        // deployments are single-process.
        Ok(())
    }

    async fn recover_orphans(&self, _older_than: Duration) -> Result<u64> {
        // No-op for SQLite; there is no separate heartbeat column.
        Ok(0)
    }

    async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        let row = sqlx::query(
            "SELECT id, queue, job_type, payload, state, priority,
                    retry_count, max_retries, created_at, scheduled_at,
                    started_at, completed_at, error
             FROM convoy_jobs
             WHERE id = ?1",
        )
        .bind(job_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;

        if let Some(row) = row {
            Ok(Some(sqlite_row_to_job(&row)?))
        } else {
            Ok(None)
        }
    }

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        let row = sqlx::query(
            "SELECT
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END)   AS pending,
                SUM(CASE WHEN state = 'scheduled' THEN 1 ELSE 0 END) AS scheduled,
                SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END)   AS running,
                SUM(CASE WHEN state = 'dead' THEN 1 ELSE 0 END)      AS dead
             FROM convoy_jobs
             WHERE queue = ?1",
        )
        .bind(queue)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;

        Ok(QueueStats {
            pending: row.get::<i64, _>("pending") as u64,
            scheduled: row.get::<i64, _>("scheduled") as u64,
            running: row.get::<i64, _>("running") as u64,
            dead: row.get::<i64, _>("dead") as u64,
        })
    }

    async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        let rows = sqlx::query(
            "SELECT id, queue, job_type, payload, state, priority,
                    retry_count, max_retries, created_at, scheduled_at,
                    started_at, completed_at, error
             FROM convoy_jobs
             WHERE state = 'dead'
             ORDER BY created_at DESC
             LIMIT ?1 OFFSET ?2",
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            jobs.push(sqlite_row_to_job(row)?);
        }
        Ok(jobs)
    }

    async fn retry_dead(&self, job_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE convoy_jobs
             SET state = 'pending', error = NULL
             WHERE id = ?1 AND state = 'dead'",
        )
        .bind(job_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn delete_dead(&self, job_id: Uuid) -> Result<()> {
        sqlx::query(
            "DELETE FROM convoy_jobs
             WHERE id = ?1 AND state = 'dead'",
        )
        .bind(job_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }
}

#[derive(Clone)]
struct RedisBackend {
    client: redis::Client,
    namespace: String,
}

impl RedisBackend {
    fn new(client: redis::Client) -> Self {
        Self {
            client,
            namespace: "convoy".to_string(),
        }
    }

    async fn conn(&self) -> Result<MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| BackendError::Connection(e.to_string()).into())
    }

    fn key_job(&self, id: Uuid) -> String {
        format!("{}:job:{}", self.namespace, id)
    }

    fn key_queue(&self, queue: &str) -> String {
        format!("{}:queue:{}", self.namespace, queue)
    }

    fn key_scheduled(&self) -> String {
        format!("{}:scheduled", self.namespace)
    }

    fn key_dead(&self) -> String {
        format!("{}:dead", self.namespace)
    }

    fn key_running(&self) -> String {
        format!("{}:running", self.namespace)
    }
}

#[async_trait]
impl Backend for RedisBackend {
    async fn push(&self, job: &Job) -> Result<()> {
        let mut conn = self.conn().await?;
        let key_job = self.key_job(job.id);
        let data = serde_json::to_string(job)
            .map_err(|e| ConvoyError::Other(e.to_string()))?;

        conn.set(key_job, data)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        match job.state {
            JobState::Scheduled => {
                if let Some(at) = job.scheduled_at {
                    let score = at.timestamp_millis() as f64;
                    let scheduled = self.key_scheduled();
                    conn.zadd(scheduled, job.id.to_string(), score)
                        .await
                        .map_err(|e| BackendError::Operation(e.to_string()))?;
                }
            }
            _ => {
                let score = job.priority as f64;
                let queue_key = self.key_queue(&job.queue);
                conn.zadd(queue_key, job.id.to_string(), score)
                    .await
                    .map_err(|e| BackendError::Operation(e.to_string()))?;
            }
        }

        Ok(())
    }

    async fn pop(&self, queues: &[String], _timeout: Duration) -> Result<Option<Job>> {
        let mut conn = self.conn().await?;

        for queue in queues {
            let queue_key = self.key_queue(queue);
            let res: Vec<(String, f64)> = redis::cmd("ZPOPMIN")
                .arg(&queue_key)
                .arg(1)
                .query_async(&mut conn)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;

            if let Some((job_id, _score)) = res.into_iter().next() {
                let id = Uuid::parse_str(&job_id)
                    .map_err(|e| ConvoyError::Other(e.to_string()))?;
                let key_job = self.key_job(id);
                let raw: Option<String> = conn
                    .get(&key_job)
                    .await
                    .map_err(|e| BackendError::Operation(e.to_string()))?;
                if let Some(raw) = raw {
                    let mut job: Job = serde_json::from_str(&raw)
                        .map_err(|e| ConvoyError::Other(e.to_string()))?;
                    job.state = JobState::Running;
                    let data = serde_json::to_string(&job)
                        .map_err(|e| ConvoyError::Other(e.to_string()))?;
                    conn.set(&key_job, data)
                        .await
                        .map_err(|e| BackendError::Operation(e.to_string()))?;
                    return Ok(Some(job));
                }
            }
        }

        Ok(None)
    }

    async fn ack(&self, job_id: Uuid) -> Result<()> {
        let mut conn = self.conn().await?;
        let key_job = self.key_job(job_id);
        let _: () = redis::cmd("DEL")
            .arg(&key_job)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        Ok(())
    }

    async fn nack(&self, job_id: Uuid, error: &str, run_at: DateTime<Utc>) -> Result<()> {
        let mut conn = self.conn().await?;
        let key_job = self.key_job(job_id);
        let raw: Option<String> = conn
            .get(&key_job)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        if let Some(raw) = raw {
            let mut job: Job = serde_json::from_str(&raw)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            job.state = JobState::Scheduled;
            job.retry_count += 1;
            job.scheduled_at = Some(run_at);
            job.error = Some(error.to_string());
            let data = serde_json::to_string(&job)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            conn.set(&key_job, data)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;

            let score = run_at.timestamp_millis() as f64;
            let scheduled = self.key_scheduled();
            conn.zadd(scheduled, job.id.to_string(), score)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;
        }
        Ok(())
    }

    async fn fail(&self, job_id: Uuid, error: &str) -> Result<()> {
        let mut conn = self.conn().await?;
        let key_job = self.key_job(job_id);
        let raw: Option<String> = conn
            .get(&key_job)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        if let Some(raw) = raw {
            let mut job: Job = serde_json::from_str(&raw)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            job.state = JobState::Dead;
            job.error = Some(error.to_string());
            let data = serde_json::to_string(&job)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            conn.set(&key_job, data)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;

            let dead = self.key_dead();
            let _: () = redis::cmd("LPUSH")
                .arg(&dead)
                .arg(job.id.to_string())
                .query_async(&mut conn)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;
        }
        Ok(())
    }

    async fn enqueue_scheduled(&self, now: DateTime<Utc>) -> Result<u64> {
        let mut conn = self.conn().await?;
        let scheduled = self.key_scheduled();
        let max_score = now.timestamp_millis() as f64;
        let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&scheduled)
            .arg("-inf")
            .arg(max_score)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        if ids.is_empty() {
            return Ok(0);
        }

        let _: () = redis::cmd("ZREMRANGEBYSCORE")
            .arg(&scheduled)
            .arg("-inf")
            .arg(max_score)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        let mut moved = 0u64;
        for id_str in ids {
            let job_id = Uuid::parse_str(&id_str)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            let key_job = self.key_job(job_id);
            let raw: Option<String> = conn
                .get(&key_job)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;
            if let Some(raw) = raw {
                let mut job: Job = serde_json::from_str(&raw)
                    .map_err(|e| ConvoyError::Other(e.to_string()))?;
                job.state = JobState::Pending;
                let data = serde_json::to_string(&job)
                    .map_err(|e| ConvoyError::Other(e.to_string()))?;
                conn.set(&key_job, data)
                    .await
                    .map_err(|e| BackendError::Operation(e.to_string()))?;

                let queue_key = self.key_queue(&job.queue);
                let score = job.priority as f64;
                conn.zadd(queue_key, job.id.to_string(), score)
                    .await
                    .map_err(|e| BackendError::Operation(e.to_string()))?;
                moved += 1;
            }
        }

        Ok(moved)
    }

    async fn heartbeat(&self, _job_id: Uuid) -> Result<()> {
        Ok(())
    }

    async fn recover_orphans(&self, _older_than: Duration) -> Result<u64> {
        Ok(0)
    }

    async fn get_job(&self, job_id: Uuid) -> Result<Option<Job>> {
        let mut conn = self.conn().await?;
        let key_job = self.key_job(job_id);
        let raw: Option<String> = conn
            .get(&key_job)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        if let Some(raw) = raw {
            let job: Job = serde_json::from_str(&raw)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            Ok(Some(job))
        } else {
            Ok(None)
        }
    }

    async fn queue_stats(&self, queue: &str) -> Result<QueueStats> {
        let mut conn = self.conn().await?;
        let queue_key = self.key_queue(queue);
        let pending: i64 = redis::cmd("ZCARD")
            .arg(&queue_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        let scheduled_key = self.key_scheduled();
        let scheduled: i64 = redis::cmd("ZCARD")
            .arg(&scheduled_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        let dead_key = self.key_dead();
        let dead: i64 = redis::cmd("LLEN")
            .arg(&dead_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        Ok(QueueStats {
            pending: pending as u64,
            scheduled: scheduled as u64,
            running: 0,
            dead: dead as u64,
        })
    }

    async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<Job>> {
        let mut conn = self.conn().await?;
        let dead = self.key_dead();
        let start = offset as isize;
        let end = (offset + limit) as isize - 1;
        let ids: Vec<String> = redis::cmd("LRANGE")
            .arg(&dead)
            .arg(start)
            .arg(end)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        let mut jobs = Vec::new();
        for id_str in ids {
            let job_id = Uuid::parse_str(&id_str)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            if let Some(job) = self.get_job(job_id).await? {
                jobs.push(job);
            }
        }

        Ok(jobs)
    }

    async fn retry_dead(&self, job_id: Uuid) -> Result<()> {
        let mut conn = self.conn().await?;
        let key_job = self.key_job(job_id);
        let raw: Option<String> = conn
            .get(&key_job)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;
        if let Some(raw) = raw {
            let mut job: Job = serde_json::from_str(&raw)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            job.state = JobState::Pending;
            job.error = None;
            let data = serde_json::to_string(&job)
                .map_err(|e| ConvoyError::Other(e.to_string()))?;
            conn.set(&key_job, data)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;

            let dead = self.key_dead();
            let _: () = redis::cmd("LREM")
                .arg(&dead)
                .arg(0)
                .arg(job.id.to_string())
                .query_async(&mut conn)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;

            let queue_key = self.key_queue(&job.queue);
            let score = job.priority as f64;
            conn.zadd(queue_key, job.id.to_string(), score)
                .await
                .map_err(|e| BackendError::Operation(e.to_string()))?;
        }
        Ok(())
    }

    async fn delete_dead(&self, job_id: Uuid) -> Result<()> {
        let mut conn = self.conn().await?;
        let dead = self.key_dead();
        let _: () = redis::cmd("LREM")
            .arg(&dead)
            .arg(0)
            .arg(job_id.to_string())
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        let key_job = self.key_job(job_id);
        let _: () = redis::cmd("DEL")
            .arg(&key_job)
            .query_async(&mut conn)
            .await
            .map_err(|e| BackendError::Operation(e.to_string()))?;

        Ok(())
    }
}

pub type DynBackend = Arc<dyn Backend>;

/// Construct a backend from configuration.
///
/// Uses a real Redis or Postgres backend when selected; SQLite
/// currently falls back to the in-memory backend.
pub async fn backend_from_config(config: &BackendConfig) -> Result<DynBackend> {
    match config {
        BackendConfig::Redis { url, .. } => {
            let client = redis::Client::open(url.as_str())
                .map_err(|e| BackendError::Connection(e.to_string()))?;
            Ok(Arc::new(RedisBackend::new(client)) as DynBackend)
        }
        BackendConfig::Postgres { url, .. } => {
            let (client, connection) = tokio_postgres::connect(url, NoTls)
                .await
                .map_err(|e| BackendError::Connection(e.to_string()))?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!("postgres connection error: {e}");
                }
            });

            Ok(Arc::new(PostgresBackend::new(client)) as DynBackend)
        }
        BackendConfig::Sqlite { path } => {
            let backend = SqliteBackend::from_path(path).await?;
            Ok(Arc::new(backend) as DynBackend)
        }
    }
}

