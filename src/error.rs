use thiserror::Error;

/// Convenient result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, ConvoyError>;

/// Errors originating from the underlying backend implementation.
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("backend connection error: {0}")]
    Connection(String),

    #[error("backend operation failed: {0}")]
    Operation(String),
}

/// Top-level error type returned by most Convoy APIs.
#[derive(Debug, Error)]
pub enum ConvoyError {
    #[error("backend error: {0}")]
    Backend(#[from] BackendError),

    #[error("job timed out")]
    Timeout,

    #[error("failed to deserialize job payload: {0}")]
    Deserialization(#[from] serde_json::Error),

    #[error("handler not found for job type `{0}`")]
    HandlerNotFound(String),

    #[error("retryable error: {0}")]
    Retryable(String),

    #[error("fatal error: {0}")]
    Fatal(String),

    #[error("other error: {0}")]
    Other(String),
}
