//! Error types for SmithyQ operations.

use thiserror::Error;

/// Result type used throughout SmithyQ.
pub type SmithyResult<T> = Result<T, SmithyError>;

/// Main error type for SmithyQ operations.
#[derive(Error, Debug)]
pub enum SmithyError {
    /// Task execution failed
    #[error("Task execution failed: {message}")]
    TaskExecutionFailed {
        /// Error message
        message: String,
        /// Optional underlying error
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Task not found in registry
    #[error("Task type '{task_type}' not found in registry")]
    TaskNotFound {
        /// The task type that wasn't found
        task_type: String,
    },

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// Queue backend error
    #[error("Queue error: {message}")]
    QueueError {
        /// Error message
        message: String,
        /// Optional underlying error
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Worker timeout
    #[error("Worker operation timed out after {timeout_secs} seconds")]
    Timeout {
        /// Timeout duration in seconds
        timeout_secs: u64,
    },

    /// Configuration error
    #[error("Configuration error: {message}")]
    ConfigError {
        /// Error message
        message: String,
    },

    /// Smithy is already running
    #[error("Smithy is already running")]
    AlreadyRunning,

    /// Smithy is not running
    #[error("Smithy is not running")]
    NotRunning,

    /// Redis connection error
    #[cfg(feature = "redis-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// Database error  
    #[cfg(feature = "postgres-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl SmithyError {
    /// Create a new task execution error
    pub fn task_execution<E>(message: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::TaskExecutionFailed {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create a new queue error
    pub fn queue<E>(message: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::QueueError {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create a configuration error
    pub fn config(message: impl Into<String>) -> Self {
        Self::ConfigError {
            message: message.into(),
        }
    }
}
