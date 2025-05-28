//! Queue backends for SmithyQ task processing.
//!
//! SmithyQ supports multiple queue backends for different use cases:
//! - **In-Memory**: Fast, lightweight, perfect for development and single-process applications
//! - **Redis**: Distributed, persistent, ideal for multi-process/multi-server setups
//! - **PostgreSQL**: ACID compliant, perfect for applications already using PostgreSQL
//!
//! # Examples
//!
//! ```rust,no_run
//! use smithyq::prelude::*;
//!
//! // In-memory queue (default)
//! let queue = InMemoryQueue::new();
//!
//! // Redis queue (requires redis-queue feature)
//! # #[cfg(feature = "redis-queue")]
//! let queue = RedisQueue::new("redis://localhost:6379").await?;
//!
//! // PostgreSQL queue (requires postgres-queue feature)
//! # #[cfg(feature = "postgres-queue")]
//! let queue = PostgresQueue::new("postgresql://user:pass@localhost/db").await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use crate::config::QueueConfig;
use crate::error::SmithyResult;
use crate::task::{QueuedTask, TaskId, TaskStatus};
use async_trait::async_trait;

pub mod memory;
pub use memory::InMemoryQueue;

#[cfg(feature = "redis-queue")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
pub mod redis;

#[cfg(feature = "redis-queue")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
pub use redis::RedisQueue;

#[cfg(feature = "postgres-queue")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
pub mod postgres;

#[cfg(feature = "postgres-queue")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
pub use postgres::PostgresQueue;

/// Statistics about the queue state
#[derive(Debug, Clone)]
pub struct QueueStats {
    /// Number of pending tasks
    pub pending: u64,
    /// Number of running tasks
    pub running: u64,
    /// Number of completed tasks
    pub completed: u64,
    /// Number of failed tasks
    pub failed: u64,
    /// Number of dead tasks
    pub dead: u64,
    /// Total number of tasks ever processed
    pub total_processed: u64,
}

/// Trait that all queue backends must implement
#[async_trait]
pub trait QueueBackend: Send + Sync {
    /// Enqueue a new task
    async fn enqueue(&self, task: QueuedTask) -> SmithyResult<TaskId>;

    /// Dequeue the next available task
    ///
    /// Returns `None` if no tasks are available. The task is marked as running
    /// and will be invisible for the visibility timeout period.
    async fn dequeue(&self) -> SmithyResult<Option<QueuedTask>>;

    /// Dequeue multiple tasks in a batch
    async fn dequeue_batch(&self, count: usize) -> SmithyResult<Vec<QueuedTask>>;

    /// Mark a task as completed successfully
    async fn complete_task(&self, task_id: &TaskId) -> SmithyResult<()>;

    /// Mark a task as failed and optionally retry it
    async fn fail_task(&self, task_id: &TaskId, error: &str, retry: bool) -> SmithyResult<()>;

    /// Requeue a task (for retries)
    async fn requeue_task(
        &self,
        task_id: &TaskId,
        delay: Option<std::time::Duration>,
    ) -> SmithyResult<()>;

    /// Get a task by ID
    async fn get_task(&self, task_id: &TaskId) -> SmithyResult<Option<QueuedTask>>;

    /// Update task status
    async fn update_task_status(&self, task_id: &TaskId, status: TaskStatus) -> SmithyResult<()>;

    /// Get queue statistics
    async fn stats(&self) -> SmithyResult<QueueStats>;

    /// Clean up expired or old tasks
    async fn cleanup(&self) -> SmithyResult<u64>;

    /// Get tasks by status with optional limit
    async fn get_tasks_by_status(
        &self,
        status: TaskStatus,
        limit: Option<usize>,
    ) -> SmithyResult<Vec<QueuedTask>>;

    /// Purge all tasks (dangerous operation)
    async fn purge(&self) -> SmithyResult<u64>;

    /// Health check for the queue backend
    async fn health_check(&self) -> SmithyResult<()>;
}

/// Convenient type alias for boxed queue backend
pub type TaskQueue = Box<dyn QueueBackend>;

/// Factory methods for creating queue backends
pub struct QueueFactory;

impl QueueFactory {
    /// Create an in-memory queue
    pub fn in_memory(config: QueueConfig) -> TaskQueue {
        Box::new(InMemoryQueue::with_config(config))
    }

    /// Create a Redis queue
    #[cfg(feature = "redis-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
    pub async fn redis(connection_string: &str, config: QueueConfig) -> SmithyResult<TaskQueue> {
        let queue = RedisQueue::new(connection_string, config).await?;
        Ok(Box::new(queue))
    }

    /// Create a PostgreSQL queue
    #[cfg(feature = "postgres-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
    pub async fn postgres(connection_string: &str, config: QueueConfig) -> SmithyResult<TaskQueue> {
        let queue = PostgresQueue::new(connection_string, config).await?;
        Ok(Box::new(queue))
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::task::QueuedTask;
    use serde_json::json;

    fn create_test_task(id: &str) -> QueuedTask {
        QueuedTask {
            id: id.to_string(),
            task_type: "test_task".to_string(),
            payload: json!({"test": "data"}),
            status: TaskStatus::Pending,
            retry_count: 0,
            max_retries: 3,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            execute_at: None,
        }
    }

    #[tokio::test]
    async fn test_in_memory_queue_basic_operations() {
        let queue = InMemoryQueue::new();
        let task = create_test_task("test-1");

        // Test enqueue
        let task_id = queue.enqueue(task.clone()).await.unwrap();
        assert_eq!(task_id, "test-1");

        // Test dequeue
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_some());
        let dequeued_task = dequeued.unwrap();
        assert_eq!(dequeued_task.id, "test-1");
        assert_eq!(dequeued_task.status, TaskStatus::Running);

        // Test complete
        queue.complete_task(&task_id).await.unwrap();

        // Test stats
        let stats = queue.stats().await.unwrap();
        assert_eq!(stats.completed, 1);
    }

    #[tokio::test]
    async fn test_queue_factory() {
        let queue = QueueFactory::in_memory(QueueConfig::default());
        assert!(queue.health_check().await.is_ok());
    }
}
