use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Internal representation of a single job in the system.
///
/// Most application code never constructs this directly; instead
/// you define a `JobHandler` and let Convoy handle serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub queue: String,
    pub job_type: String,
    pub payload: Value,
    pub state: JobState,
    pub priority: i32,
    pub retry_count: u32,
    pub max_retries: u32,
    pub created_at: DateTime<Utc>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

/// High-level lifecycle states for a job as it moves through the
/// system.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Scheduled,
    Running,
    Completed,
    Failed,
    Dead,
}

impl Job {
    pub fn new(
        queue: String,
        job_type: String,
        payload: Value,
        max_retries: u32,
        priority: i32,
        scheduled_at: Option<DateTime<Utc>>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            queue,
            job_type,
            payload,
            state: if scheduled_at.is_some() { JobState::Scheduled } else { JobState::Pending },
            priority,
            retry_count: 0,
            max_retries,
            created_at: now,
            scheduled_at,
            started_at: None,
            completed_at: None,
            error: None,
        }
    }
}
