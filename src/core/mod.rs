//! The main Smithy worker manager and engine.
//!
//! The Smithy is the heart of SmithyQ - it manages workers, dispatches tasks,
//! and ensures reliable task processing with fault tolerance and monitoring.

use crate::config::SmithyConfig;
use crate::error::{SmithyError, SmithyResult};
use crate::queue::{QueueBackend, QueueFactory};
use crate::task::{QueuedTask, SmithyTask, TaskId};
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod engine;
pub mod registry;
pub mod worker;

pub use engine::SmithyEngine;
pub use registry::{TaskCaller, TaskExecutor, TaskRegistry, get_registry};
pub use worker::{Worker, WorkerManager, WorkerRequest, WorkerStats};

/// The main Smithy worker manager.
///
/// This is the primary interface for SmithyQ. It manages the worker pool,
/// handles task distribution, and provides a clean API for task enqueueing.
///
/// # Examples
///
/// ```rust
/// use smithyq::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> SmithyResult<()> {
///     let smithy = Smithy::new(SmithyConfig::default()).await?;
///     smithy.start_forging().await?;
///     Ok(())
/// }
/// ```
pub struct Smithy {
    engine: Arc<RwLock<Option<SmithyEngine>>>,
    queue: Arc<dyn QueueBackend>,
    config: SmithyConfig,
    is_running: Arc<RwLock<bool>>,
}

impl Smithy {
    /// Create a new Smithy with the given configuration.
    pub async fn new(config: SmithyConfig) -> SmithyResult<Self> {
        let queue = QueueFactory::in_memory(config.queue.clone());

        println!("config: {:?}", config);
        Ok(Self {
            engine: Arc::new(RwLock::new(None)),
            queue: Arc::from(queue),
            config,
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// Create a new Smithy with a custom queue backend.
    pub async fn with_queue<Q: QueueBackend + 'static>(
        config: SmithyConfig,
        queue: Q,
    ) -> SmithyResult<Self> {
        Ok(Self {
            engine: Arc::new(RwLock::new(None)),
            queue: Arc::new(queue),
            config,
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// Create a Smithy with Redis queue backend.
    #[cfg(feature = "redis-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
    pub async fn with_redis(connection_string: &str) -> SmithyResult<Self> {
        let config = SmithyConfig::default();
        let queue = QueueFactory::redis(connection_string, config.queue.clone()).await?;

        Ok(Self {
            engine: Arc::new(RwLock::new(None)),
            queue: Arc::from(queue),
            config,
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// Create a Smithy with PostgreSQL queue backend.
    #[cfg(feature = "postgres-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
    pub async fn with_postgres(connection_string: &str) -> SmithyResult<Self> {
        let config = SmithyConfig::default();
        let queue = QueueFactory::postgres(connection_string, config.queue.clone()).await?;

        Ok(Self {
            engine: Arc::new(RwLock::new(None)),
            queue: Arc::from(queue),
            config,
            is_running: Arc::new(RwLock::new(false)),
        })
    }

    /// Start the smithy forging process (begin processing tasks).
    ///
    /// This spawns the worker pool and begins processing tasks from the queue.
    /// The method returns immediately - use `wait_for_shutdown()` to block.
    pub async fn start_forging(&self) -> SmithyResult<()> {
        let mut is_running = self.is_running.write().await;
        if *is_running {
            return Err(SmithyError::AlreadyRunning);
        }

        let mut engine_guard = self.engine.write().await;
        let mut engine = SmithyEngine::new(self.config.clone(), Arc::clone(&self.queue));

        engine.start().await?;
        *engine_guard = Some(engine);
        *is_running = true;

        tracing::info!(
            "🔨 Smithy started forging with {} workers",
            self.config.workers.num_workers
        );
        Ok(())
    }

    /// Stop the smithy and wait for all workers to finish.
    pub async fn stop_forging(&self) -> SmithyResult<()> {
        let mut is_running = self.is_running.write().await;
        if !*is_running {
            return Err(SmithyError::NotRunning);
        }

        let mut engine_guard = self.engine.write().await;
        if let Some(engine) = engine_guard.take() {
            engine.shutdown().await?;
        }

        *is_running = false;
        tracing::info!("🔨 Smithy stopped forging");
        Ok(())
    }

    /// Enqueue a task for processing.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use smithyq::prelude::*;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # struct EmailTask { to: String }
    /// # #[async_trait::async_trait]
    /// # impl SmithyTask for EmailTask {
    /// #     type Output = String;
    /// #     async fn forge(self) -> SmithyResult<Self::Output> { Ok("sent".to_string()) }
    /// # }
    /// # async fn example() -> SmithyResult<()> {
    /// let smithy = Smithy::new(SmithyConfig::default()).await?;
    ///
    /// let task = EmailTask { to: "user@example.com".to_string() };
    /// let task_id = smithy.enqueue(task).await?;
    /// println!("Task forged: {}", task_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue<T: SmithyTask>(&self, task: T) -> SmithyResult<TaskId> {
        let payload = serde_json::to_value(&task)?;
        let execute_at = task
            .delay()
            .map(|delay| std::time::SystemTime::now() + delay);

        let queued_task = QueuedTask {
            id: uuid::Uuid::new_v4().to_string(),
            task_type: task.task_type().to_string(),
            payload,
            status: crate::task::TaskStatus::Pending,
            retry_count: 0,
            max_retries: task.max_retries(),
            created_at: std::time::SystemTime::now(),
            updated_at: std::time::SystemTime::now(),
            execute_at,
        };

        let task_id = queued_task.id.clone();
        self.queue.enqueue(queued_task).await?;

        tracing::debug!(
            "🔨 Task forged and enqueued: {} (type: {})",
            task_id,
            task.task_type()
        );
        Ok(task_id)
    }

    /// Get task status by ID.
    pub async fn get_task_status(&self, task_id: &TaskId) -> SmithyResult<Option<QueuedTask>> {
        self.queue.get_task(task_id).await
    }

    /// Get queue statistics.
    pub async fn stats(&self) -> SmithyResult<crate::queue::QueueStats> {
        self.queue.stats().await
    }

    /// Get worker statistics (if running).
    pub async fn worker_stats(&self) -> SmithyResult<Option<WorkerStats>> {
        let engine_guard = self.engine.read().await;
        if let Some(engine) = engine_guard.as_ref() {
            Ok(Some(engine.worker_stats().await))
        } else {
            Ok(None)
        }
    }

    /// Check if the smithy is currently running.
    pub async fn is_forging(&self) -> bool {
        *self.is_running.read().await
    }

    /// Wait for the smithy to shutdown (blocks until shutdown).
    pub async fn wait_for_shutdown(&self) -> SmithyResult<()> {
        let engine_guard = self.engine.read().await;
        if let Some(engine) = engine_guard.as_ref() {
            engine.wait_for_shutdown().await?;
        }
        Ok(())
    }

    /// Perform a health check on the smithy and queue.
    pub async fn health_check(&self) -> SmithyResult<()> {
        // Check queue health
        self.queue.health_check().await?;

        // Check worker health if running
        let engine_guard = self.engine.read().await;
        if let Some(engine) = engine_guard.as_ref() {
            engine.health_check().await?;
        }

        Ok(())
    }

    /// Get the configuration used by this smithy.
    pub fn config(&self) -> &SmithyConfig {
        &self.config
    }
}

impl Drop for Smithy {
    fn drop(&mut self) {
        // Note: We can't call async methods in Drop, so we just log a warning
        // if the smithy is still running. Users should call stop_forging() explicitly.
        tracing::warn!(
            "🔨 Smithy dropped while potentially still running. Call stop_forging() explicitly for graceful shutdown."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::SmithyTask;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct TestTask {
        data: String,
    }

    #[async_trait::async_trait]
    impl SmithyTask for TestTask {
        type Output = String;

        async fn forge(self) -> SmithyResult<Self::Output> {
            Ok(format!("Processed: {}", self.data))
        }
    }

    #[tokio::test]
    async fn test_smithy_creation() {
        let smithy = Smithy::new(SmithyConfig::default()).await.unwrap();
        assert!(!smithy.is_forging().await);
    }

    #[tokio::test]
    async fn test_task_enqueue() {
        let smithy = Smithy::new(SmithyConfig::default()).await.unwrap();

        let task = TestTask {
            data: "test".to_string(),
        };

        let task_id = smithy.enqueue(task).await.unwrap();
        assert!(!task_id.is_empty());

        let task_status = smithy.get_task_status(&task_id).await.unwrap();
        assert!(task_status.is_some());
    }

    #[tokio::test]
    async fn test_smithy_lifecycle() {
        let mut config = SmithyConfig::default();
        config.workers.num_workers = 1; // Single worker for test

        let smithy = Smithy::new(config).await.unwrap();

        // Should not be running initially
        assert!(!smithy.is_forging().await);

        // Start forging
        smithy.start_forging().await.unwrap();
        assert!(smithy.is_forging().await);

        // Should fail to start again
        assert!(smithy.start_forging().await.is_err());

        // Stop forging
        smithy.stop_forging().await.unwrap();
        assert!(!smithy.is_forging().await);
    }
}
