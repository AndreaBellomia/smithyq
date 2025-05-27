//! Task definition and execution traits.

use crate::error::SmithyResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Unique identifier for a task
pub type TaskId = String;

/// Status of a task in the queue
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting to be processed
    Pending,
    /// Task is currently being processed
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed and will be retried
    Failed,
    /// Task failed permanently (max retries exceeded)
    Dead,
}

/// Internal representation of a task in the queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedTask {
    /// Unique task identifier
    pub id: TaskId,
    /// Type identifier for the task
    pub task_type: String,
    /// Serialized task payload
    pub payload: serde_json::Value,
    /// Current status
    pub status: TaskStatus,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Maximum number of retries allowed
    pub max_retries: u32,
    /// When the task was created
    pub created_at: SystemTime,
    /// When the task was last updated
    pub updated_at: SystemTime,
    /// When the task should be executed (for delayed tasks)
    pub execute_at: Option<SystemTime>,
}

/// Trait that all tasks must implement to be executable by SmithyQ
#[async_trait]
pub trait SmithyTask: Send + Sync + Serialize + for<'de> Deserialize<'de> {
    /// The output type returned by this task
    type Output: Serialize + Send + Sync;

    /// Execute the task and return the result
    ///
    /// This is where your task logic goes. The method is called `forge`
    /// to align with the SmithyQ metaphor of "forging" tasks.
    async fn forge(self) -> SmithyResult<Self::Output>;

    /// Get the task type identifier
    ///
    /// By default, this returns the type name, but can be overridden
    /// for custom task type identifiers.
    fn task_type(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Get the maximum number of retries for this task
    ///
    /// Default is 3 retries. Override this method to customize
    /// retry behavior for specific task types.
    fn max_retries(&self) -> u32 {
        3
    }

    /// Get the delay before executing this task
    ///
    /// Default is no delay (execute immediately). Override this
    /// to implement delayed task execution.
    fn delay(&self) -> Option<std::time::Duration> {
        None
    }
}
