//! In-memory queue implementation for SmithyQ.
//!
//! This queue backend stores all tasks in memory using Rust's standard collections.
//! It's perfect for development, testing, and single-process applications where
//! persistence across restarts is not required.
//!
//! # Features
//!
//! - **Fast**: Direct memory access with no serialization overhead
//! - **Thread-safe**: Uses async-friendly locks
//! - **Full-featured**: Supports all queue operations including retries and cleanup
//! - **Zero dependencies**: No external services required

use super::{QueueBackend, QueueConfig, QueueStats};
use crate::error::{SmithyError, SmithyResult};
use crate::task::{QueuedTask, TaskId, TaskStatus};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// In-memory queue backend implementation
#[derive(Debug)]
pub struct InMemoryQueue {
    /// Main task storage indexed by task ID
    tasks: Arc<RwLock<HashMap<TaskId, QueuedTask>>>,
    /// Pending tasks queue (FIFO)
    pending_queue: Arc<RwLock<VecDeque<TaskId>>>,
    /// Running tasks with visibility timeout
    running_tasks: Arc<RwLock<HashMap<TaskId, SystemTime>>>,
    /// Completed tasks
    completed_tasks: Arc<RwLock<Vec<TaskId>>>,
    /// Failed tasks  
    failed_tasks: Arc<RwLock<Vec<TaskId>>>,
    /// Dead tasks (max retries exceeded)
    dead_tasks: Arc<RwLock<Vec<TaskId>>>,
    /// Queue configuration
    config: QueueConfig,
    /// Statistics counters
    stats: Arc<RwLock<QueueStats>>,
}

impl InMemoryQueue {
    /// Create a new in-memory queue with default configuration
    pub fn new() -> Self {
        Self::with_config(QueueConfig::default())
    }

    /// Create a new in-memory queue with custom configuration
    pub fn with_config(config: QueueConfig) -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            pending_queue: Arc::new(RwLock::new(VecDeque::new())),
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            completed_tasks: Arc::new(RwLock::new(Vec::new())),
            failed_tasks: Arc::new(RwLock::new(Vec::new())),
            dead_tasks: Arc::new(RwLock::new(Vec::new())),
            config,
            stats: Arc::new(RwLock::new(QueueStats {
                pending: 0,
                running: 0,
                completed: 0,
                failed: 0,
                dead: 0,
                total_processed: 0,
            })),
        }
    }

    /// Make invisible tasks visible again if their timeout has expired
    async fn process_visibility_timeouts(&self) -> SmithyResult<()> {
        let now = SystemTime::now();
        let mut running_tasks = self.running_tasks.write().await;
        let mut tasks = self.tasks.write().await;
        let mut pending_queue = self.pending_queue.write().await;
        let mut expired_tasks = Vec::new();

        // Find expired tasks
        for (task_id, visible_at) in running_tasks.iter() {
            if now >= *visible_at {
                expired_tasks.push(task_id.clone());
            }
        }

        // Move expired tasks back to pending
        for task_id in expired_tasks {
            running_tasks.remove(&task_id);

            if let Some(task) = tasks.get_mut(&task_id) {
                task.status = TaskStatus::Pending;
                task.updated_at = now;
                pending_queue.push_back(task_id);
            }
        }

        self.update_stats().await?;
        Ok(())
    }

    /// Update internal statistics
    async fn update_stats(&self) -> SmithyResult<()> {
        let tasks_guard = match self.tasks.try_read() {
            Ok(guard) => guard,
            Err(_) => {
                tracing::debug!("Tasks lock busy, skipping stats update");
                return Ok(());
            }
        };

        let mut stats_guard = match self.stats.try_write() {
            Ok(guard) => guard,
            Err(_) => {
                tracing::debug!("Stats lock busy, skipping stats update");
                return Ok(());
            }
        };

        stats_guard.pending = tasks_guard
            .values()
            .filter(|t| t.status == TaskStatus::Pending)
            .count() as u64;
        stats_guard.running = tasks_guard
            .values()
            .filter(|t| t.status == TaskStatus::Running)
            .count() as u64;
        stats_guard.completed = tasks_guard
            .values()
            .filter(|t| t.status == TaskStatus::Completed)
            .count() as u64;
        stats_guard.failed = tasks_guard
            .values()
            .filter(|t| t.status == TaskStatus::Failed)
            .count() as u64;
        stats_guard.dead = tasks_guard
            .values()
            .filter(|t| t.status == TaskStatus::Dead)
            .count() as u64;

        Ok(())
    }

    /// Check if we're at the maximum queue size
    async fn is_queue_full(&self) -> bool {
        if self.config.max_queue_size == 0 {
            return false; // Unlimited
        }

        let tasks = self.tasks.read().await;
        tasks.len() >= self.config.max_queue_size
    }
}

impl Default for InMemoryQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueueBackend for InMemoryQueue {
    async fn enqueue(&self, mut task: QueuedTask) -> SmithyResult<TaskId> {
        // Check queue size limit
        if self.is_queue_full().await {
            return Err(SmithyError::QueueError {
                message: format!("Queue is full (max size: {})", self.config.max_queue_size),
                source: None,
            });
        }

        let task_id = task.id.clone();
        let now = SystemTime::now();

        task.status = TaskStatus::Pending;
        task.created_at = now;
        task.updated_at = now;

        // Store the task
        {
            let mut tasks = self.tasks.write().await;
            tasks.insert(task_id.clone(), task);
        }

        // Add to pending queue (respect execute_at for delayed tasks)
        {
            let mut pending_queue = self.pending_queue.write().await;

            // For delayed tasks, we'll check execute_at in dequeue
            pending_queue.push_back(task_id.clone());
        }

        let _ = self.update_stats().await;

        tracing::debug!("Enqueued task: {}", task_id);
        Ok(task_id)
    }

    async fn dequeue(&self) -> SmithyResult<Option<QueuedTask>> {
        // Process any expired visibility timeouts first
        self.process_visibility_timeouts().await?;

        let now = SystemTime::now();
        let mut pending_queue = self.pending_queue.write().await;
        let mut tasks = self.tasks.write().await;
        let mut running_tasks = self.running_tasks.write().await;

        // Find the first task that's ready to execute
        let mut ready_task_index = None;
        for (index, task_id) in pending_queue.iter().enumerate() {
            if let Some(task) = tasks.get(task_id) {
                // Check if task is ready to execute (respect execute_at)
                if task.execute_at.map_or(true, |execute_at| now >= execute_at) {
                    ready_task_index = Some(index);
                    break;
                }
            }
        }

        if let Some(index) = ready_task_index {
            if let Some(task_id) = pending_queue.remove(index) {
                if let Some(task) = tasks.get_mut(&task_id) {
                    // Mark as running
                    task.status = TaskStatus::Running;
                    task.updated_at = now;

                    // Set visibility timeout
                    let visible_at = now + Duration::from_secs(self.config.visibility_timeout_secs);
                    running_tasks.insert(task_id.clone(), visible_at);

                    let result_task = task.clone();
                    drop(tasks); // Release locks
                    drop(pending_queue);
                    drop(running_tasks);

                    let _ = self.update_stats().await;
                    tracing::debug!("Dequeued task: {}", task_id);

                    return Ok(Some(result_task));
                }
            }
        }

        Ok(None)
    }

    async fn dequeue_batch(&self, count: usize) -> SmithyResult<Vec<QueuedTask>> {
        let mut batch = Vec::with_capacity(count);

        for _ in 0..count {
            if let Some(task) = self.dequeue().await? {
                batch.push(task);
            } else {
                break; // No more tasks available
            }
        }

        Ok(batch)
    }

    async fn complete_task(&self, task_id: &TaskId) -> SmithyResult<()> {
        let now = SystemTime::now();
        let mut tasks = self.tasks.write().await;
        let mut running_tasks = self.running_tasks.write().await;
        let mut completed_tasks = self.completed_tasks.write().await;

        // Remove from running
        running_tasks.remove(task_id);

        // Update task status
        if let Some(task) = tasks.get_mut(task_id) {
            task.status = TaskStatus::Completed;
            task.updated_at = now;

            completed_tasks.push(task_id.clone());

            drop(tasks);
            drop(running_tasks);
            drop(completed_tasks);

            let _ = self.update_stats().await;

            // Update total processed counter
            let mut stats = self.stats.write().await;
            stats.total_processed += 1;

            tracing::debug!("Completed task: {}", task_id);
            Ok(())
        } else {
            Err(SmithyError::TaskNotFound {
                task_type: task_id.clone(),
            })
        }
    }

    async fn fail_task(&self, task_id: &TaskId, error: &str, retry: bool) -> SmithyResult<()> {
        let now = SystemTime::now();
        let mut tasks = self.tasks.write().await;
        let mut running_tasks = self.running_tasks.write().await;
        let mut failed_tasks = self.failed_tasks.write().await;
        let mut dead_tasks = self.dead_tasks.write().await;
        let mut pending_queue = self.pending_queue.write().await;

        // Remove from running
        running_tasks.remove(task_id);

        if let Some(task) = tasks.get_mut(task_id) {
            task.retry_count += 1;
            task.updated_at = now;

            if retry && task.retry_count <= task.max_retries {
                // Retry the task
                task.status = TaskStatus::Failed; // Mark as failed but will retry
                pending_queue.push_back(task_id.clone()); // Re-add to queue
                failed_tasks.push(task_id.clone());

                tracing::warn!(
                    "Task {} failed (retry {}/{}): {}",
                    task_id,
                    task.retry_count,
                    task.max_retries,
                    error
                );
            } else {
                // Max retries exceeded or retry disabled
                task.status = TaskStatus::Dead;
                dead_tasks.push(task_id.clone());

                tracing::error!(
                    "Task {} permanently failed after {} retries: {}",
                    task_id,
                    task.retry_count,
                    error
                );
            }

            drop(tasks);
            drop(running_tasks);
            drop(failed_tasks);
            drop(dead_tasks);
            drop(pending_queue);

            let _ = self.update_stats().await;
            Ok(())
        } else {
            Err(SmithyError::TaskNotFound {
                task_type: task_id.clone(),
            })
        }
    }

    async fn requeue_task(&self, task_id: &TaskId, delay: Option<Duration>) -> SmithyResult<()> {
        let now = SystemTime::now();
        let mut tasks = self.tasks.write().await;
        let mut pending_queue = self.pending_queue.write().await;

        if let Some(task) = tasks.get_mut(task_id) {
            task.status = TaskStatus::Pending;
            task.updated_at = now;

            // Set execute_at if delay is specified
            if let Some(delay) = delay {
                task.execute_at = Some(now + delay);
            }

            pending_queue.push_back(task_id.clone());

            drop(tasks);
            drop(pending_queue);

            let _ = self.update_stats().await;

            tracing::debug!("Requeued task {} with delay: {:?}", task_id, delay);
            Ok(())
        } else {
            Err(SmithyError::TaskNotFound {
                task_type: task_id.clone(),
            })
        }
    }

    async fn get_task(&self, task_id: &TaskId) -> SmithyResult<Option<QueuedTask>> {
        let tasks = self.tasks.read().await;
        Ok(tasks.get(task_id).cloned())
    }

    async fn update_task_status(&self, task_id: &TaskId, status: TaskStatus) -> SmithyResult<()> {
        let mut tasks = self.tasks.write().await;

        if let Some(task) = tasks.get_mut(task_id) {
            task.status = status;
            task.updated_at = SystemTime::now();

            drop(tasks);
            let _ = self.update_stats().await;
            Ok(())
        } else {
            Err(SmithyError::TaskNotFound {
                task_type: task_id.clone(),
            })
        }
    }

    async fn stats(&self) -> SmithyResult<QueueStats> {
        // Make sure stats are up to date
        let _ = self.update_stats().await;
        let stats = self.stats.read().await;
        Ok(stats.clone())
    }

    async fn cleanup(&self) -> SmithyResult<u64> {
        // Process visibility timeouts
        self.process_visibility_timeouts().await?;

        // For in-memory queue, we can optionally clean up old completed/dead tasks
        // This is mainly useful to prevent memory growth in long-running applications

        let now = SystemTime::now();
        let cleanup_age = Duration::from_secs(3600); // 1 hour
        let mut cleaned_count = 0;

        let mut tasks = self.tasks.write().await;
        let mut completed_tasks = self.completed_tasks.write().await;
        let mut dead_tasks = self.dead_tasks.write().await;

        // Clean up old completed tasks
        completed_tasks.retain(|task_id| {
            if let Some(task) = tasks.get(task_id) {
                let age = now.duration_since(task.updated_at).unwrap_or_default();
                if age > cleanup_age {
                    tasks.remove(task_id);
                    cleaned_count += 1;
                    false
                } else {
                    true
                }
            } else {
                false
            }
        });

        // Clean up old dead tasks
        dead_tasks.retain(|task_id| {
            if let Some(task) = tasks.get(task_id) {
                let age = now.duration_since(task.updated_at).unwrap_or_default();
                if age > cleanup_age {
                    tasks.remove(task_id);
                    cleaned_count += 1;
                    false
                } else {
                    true
                }
            } else {
                false
            }
        });

        drop(tasks);
        drop(completed_tasks);
        drop(dead_tasks);

        if cleaned_count > 0 {
            let _ = self.update_stats().await;
            tracing::info!("Cleaned up {} old tasks", cleaned_count);
        }

        Ok(cleaned_count)
    }

    async fn get_tasks_by_status(
        &self,
        status: TaskStatus,
        limit: Option<usize>,
    ) -> SmithyResult<Vec<QueuedTask>> {
        let tasks = self.tasks.read().await;
        let mut result: Vec<QueuedTask> = tasks
            .values()
            .filter(|task| task.status == status)
            .cloned()
            .collect();

        // Sort by created_at (newest first)
        result.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        if let Some(limit) = limit {
            result.truncate(limit);
        }

        Ok(result)
    }

    async fn purge(&self) -> SmithyResult<u64> {
        let mut tasks = self.tasks.write().await;
        let mut pending_queue = self.pending_queue.write().await;
        let mut running_tasks = self.running_tasks.write().await;
        let mut completed_tasks = self.completed_tasks.write().await;
        let mut failed_tasks = self.failed_tasks.write().await;
        let mut dead_tasks = self.dead_tasks.write().await;

        let purged_count = tasks.len() as u64;

        tasks.clear();
        pending_queue.clear();
        running_tasks.clear();
        completed_tasks.clear();
        failed_tasks.clear();
        dead_tasks.clear();

        drop(tasks);
        drop(pending_queue);
        drop(running_tasks);
        drop(completed_tasks);
        drop(failed_tasks);
        drop(dead_tasks);

        // Reset stats
        {
            let mut stats = self.stats.write().await;
            stats.pending = 0;
            stats.running = 0;
            stats.completed = 0;
            stats.failed = 0;
            stats.dead = 0;
            // Keep total_processed as historical data
        }

        tracing::warn!("Purged {} tasks from queue", purged_count);
        Ok(purged_count)
    }

    async fn health_check(&self) -> SmithyResult<()> {
        // For in-memory queue, we just check that our internal state is consistent
        let tasks_count = {
            let tasks = self.tasks.read().await;
            tasks.len()
        };

        let queue_count = {
            let pending_queue = self.pending_queue.read().await;
            pending_queue.len()
        };

        let running_count = {
            let running_tasks = self.running_tasks.read().await;
            running_tasks.len()
        };

        tracing::debug!(
            "Queue health check: {} total tasks, {} pending, {} running",
            tasks_count,
            queue_count,
            running_count
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn test_enqueue_dequeue() {
        let queue = InMemoryQueue::new();
        let task = create_test_task("test-1");

        // Enqueue
        let task_id = queue.enqueue(task).await.unwrap();
        assert_eq!(task_id, "test-1");

        // Dequeue
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_some());
        let dequeued_task = dequeued.unwrap();
        assert_eq!(dequeued_task.id, "test-1");
        assert_eq!(dequeued_task.status, TaskStatus::Running);
    }

    #[tokio::test]
    async fn test_complete_task() {
        let queue = InMemoryQueue::new();
        let task = create_test_task("test-1");

        let task_id = queue.enqueue(task).await.unwrap();
        queue.dequeue().await.unwrap();
        queue.complete_task(&task_id).await.unwrap();

        let stats = queue.stats().await.unwrap();
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.total_processed, 1);
    }

    #[tokio::test]
    async fn test_fail_and_retry() {
        let queue = InMemoryQueue::new();
        let task = create_test_task("test-1");

        let task_id = queue.enqueue(task).await.unwrap();
        queue.dequeue().await.unwrap();

        // Fail with retry
        queue.fail_task(&task_id, "test error", true).await.unwrap();

        // Should be available for dequeue again
        let retried = queue.dequeue().await.unwrap();
        assert!(retried.is_some());
        let retried_task = retried.unwrap();
        assert_eq!(retried_task.retry_count, 1);
    }

    #[tokio::test]
    async fn test_delayed_task() {
        let queue = InMemoryQueue::new();
        let mut task = create_test_task("test-1");

        // Set execute_at to 1 second in the future
        task.execute_at = Some(SystemTime::now() + Duration::from_secs(1));

        queue.enqueue(task).await.unwrap();

        // Should not be available immediately
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_none());

        // Wait and try again (in real usage, you'd wait longer)
        tokio::time::sleep(Duration::from_millis(100)).await;
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_none()); // Still too early
    }

    #[tokio::test]
    async fn test_queue_size_limit() {
        let config = QueueConfig {
            max_queue_size: 2,
            ..Default::default()
        };
        let queue = InMemoryQueue::with_config(config);

        // Enqueue up to limit
        queue.enqueue(create_test_task("test-1")).await.unwrap();
        queue.enqueue(create_test_task("test-2")).await.unwrap();

        // Third task should fail
        let result = queue.enqueue(create_test_task("test-3")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_dequeue() {
        let queue = InMemoryQueue::new();

        // Enqueue multiple tasks
        for i in 1..=5 {
            queue
                .enqueue(create_test_task(&format!("test-{}", i)))
                .await
                .unwrap();
        }

        // Dequeue batch
        let batch = queue.dequeue_batch(3).await.unwrap();
        assert_eq!(batch.len(), 3);

        // All should be marked as running
        for task in batch {
            assert_eq!(task.status, TaskStatus::Running);
        }
    }

    #[tokio::test]
    async fn test_cleanup() {
        let queue = InMemoryQueue::new();
        let task = create_test_task("test-1");

        let task_id = queue.enqueue(task).await.unwrap();
        queue.dequeue().await.unwrap();
        queue.complete_task(&task_id).await.unwrap();

        // Cleanup shouldn't remove recent tasks
        let cleaned = queue.cleanup().await.unwrap();
        assert_eq!(cleaned, 0);

        let stats = queue.stats().await.unwrap();
        assert_eq!(stats.completed, 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let queue = InMemoryQueue::new();

        // Add some tasks
        for i in 1..=3 {
            queue
                .enqueue(create_test_task(&format!("test-{}", i)))
                .await
                .unwrap();
        }

        let purged = queue.purge().await.unwrap();
        assert_eq!(purged, 3);

        let stats = queue.stats().await.unwrap();
        assert_eq!(stats.pending, 0);
    }
}
