//! Worker implementation for SmithyQ.
//!
//! Workers are the individual "blacksmiths" that forge (execute) tasks.
//! They communicate with the dispatcher to receive tasks and report results.

use crate::config::SmithyConfig;
use crate::core::registry::get_registry;
use crate::error::{SmithyError, SmithyResult};
use crate::task::{QueuedTask, TaskId, TaskStatus};
use crate::utils::calculate_backoff;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

/// Result of task execution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskResult {
    /// Task completed successfully
    Success,
    /// Task execution timed out
    Timeout,
    /// Task panicked or had an error
    Panic(String),
}

/// Reasons why a worker might die
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerDeathCause {
    /// Communication timeout with dispatcher
    Timeout,
    /// Channel communication error
    CommunicationError,
    /// Worker panicked
    Panic,
    /// Task execution timeout
    TaskTimeout,
    /// Normal shutdown
    Normal,
}

/// Information about a dead worker
#[derive(Debug, Clone)]
pub struct DeadWorker {
    /// Worker identifier
    pub worker_id: usize,
    /// Why the worker died
    pub cause: WorkerDeathCause,
    /// When the worker died
    pub died_at: SystemTime,
}

/// Request from worker to dispatcher for a task
#[derive(Debug)]
pub struct WorkerRequest {
    /// Channel to send the task response back to worker
    pub response_tx: oneshot::Sender<Option<QueuedTask>>,
    /// Worker ID making the request
    pub worker_id: usize,
    /// When the request was made
    pub requested_at: SystemTime,
}

/// Statistics about worker performance
#[derive(Debug, Clone)]
pub struct WorkerStats {
    /// Number of active workers
    pub active_workers: usize,
    /// Total number of workers spawned
    pub total_workers: usize,
    /// Number of tasks processed successfully
    pub tasks_completed: u64,
    /// Number of tasks that failed
    pub tasks_failed: u64,
    /// Number of worker restarts
    pub worker_restarts: u64,
    /// Average task processing time
    pub avg_task_duration: Option<Duration>,
}

/// Individual worker that processes tasks
pub struct Worker {
    id: usize,
    config: SmithyConfig,
    stats: Arc<Mutex<WorkerStats>>,
}

impl Worker {
    /// Create a new worker with the given ID
    pub fn new(id: usize, config: SmithyConfig, stats: Arc<Mutex<WorkerStats>>) -> Self {
        Self { id, config, stats }
    }

    /// Run the worker loop
    pub async fn run(
        &self,
        worker_ready_tx: mpsc::Sender<WorkerRequest>,
        death_tx: mpsc::Sender<DeadWorker>,
    ) -> WorkerDeathCause {
        tracing::info!("🔨 Worker {} starting forge", self.id);

        let mut empty_count = 0u32;
        let worker_id = self.id;

        let death_cause = loop {
            // Request a task from the dispatcher
            let (response_tx, response_rx) = oneshot::channel();
            let request = WorkerRequest {
                response_tx,
                worker_id,
                requested_at: SystemTime::now(),
            };

            // Send request with timeout
            let send_timeout = Duration::from_secs(5);
            if timeout(send_timeout, worker_ready_tx.send(request))
                .await
                .is_err()
            {
                tracing::warn!("🔨 Worker {}: dispatcher timeout, shutting down", worker_id);
                break WorkerDeathCause::Timeout;
            }

            // Wait for task response
            let response_timeout = Duration::from_secs(10);
            match timeout(response_timeout, response_rx).await {
                Ok(Ok(Some(task))) => {
                    empty_count = 0;
                    let task_id = task.id.clone();

                    tracing::info!("🔨 Worker {} forging task {}", worker_id, task_id);

                    let start_time = std::time::Instant::now();
                    let task_result = self.execute_task(task).await;
                    let duration = start_time.elapsed();

                    // Update stats
                    self.update_stats(task_result.clone(), duration).await;

                    match task_result {
                        TaskResult::Success => {
                            tracing::info!(
                                "🔨 Worker {} completed task {} in {:?}",
                                worker_id,
                                task_id,
                                duration
                            );
                        }
                        TaskResult::Timeout => {
                            tracing::error!("🔨 Worker {} task {} timed out", worker_id, task_id);
                            break WorkerDeathCause::TaskTimeout;
                        }
                        TaskResult::Panic(error) => {
                            tracing::error!(
                                "🔨 Worker {} task {} panicked: {}",
                                worker_id,
                                task_id,
                                error
                            );
                            break WorkerDeathCause::Panic;
                        }
                    }
                }
                Ok(Ok(None)) => {
                    // No tasks available, back off
                    empty_count += 1;
                    let backoff_ms = calculate_backoff(empty_count, worker_id as u64);

                    tracing::debug!(
                        "🔨 Worker {} no tasks available, backing off {}ms",
                        worker_id,
                        backoff_ms
                    );
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
                Ok(Err(_)) | Err(_) => {
                    tracing::error!("🔨 Worker {} communication error, shutting down", worker_id);
                    break WorkerDeathCause::CommunicationError;
                }
            }
        };

        tracing::info!(
            "🔨 Worker {} finished with cause: {:?}",
            worker_id,
            death_cause
        );

        // Report death to supervisor
        let dead_worker = DeadWorker {
            worker_id,
            cause: death_cause.clone(),
            died_at: SystemTime::now(),
        };

        if !matches!(death_cause, WorkerDeathCause::Normal) {
            let _ = death_tx.send(dead_worker).await;
        }

        death_cause
    }

    /// Execute a single task
    async fn execute_task(&self, task: QueuedTask) -> TaskResult {
        let timeout_duration =
            Duration::from_secs(self.config.workers.task_timeout_secs.unwrap_or(300));

        let task_id = task.id.clone();
        let task_id_clone = task_id.clone();
        let task_type = task.task_type.clone();

        let handle = tokio::spawn(async move {
            tracing::debug!(
                "🔨 Executing task {} of type {}",
                task_id.clone(),
                task_type
            );

            let registry = get_registry();
            registry.execute_task(&task).await
        });

        match timeout(timeout_duration, handle).await {
            Ok(Ok(Ok(_result))) => {
                tracing::debug!("🔨 Task {} executed successfully", task_id_clone);
                TaskResult::Success
            }
            Ok(Ok(Err(error))) => {
                let error_msg = error.to_string();
                tracing::error!("🔨 Task {} failed: {}", task_id_clone, error_msg);
                TaskResult::Panic(error_msg)
            }
            Ok(Err(join_error)) => {
                let error_msg = format!("Task panicked: {}", join_error);
                tracing::error!("🔨 Task {} panicked: {}", task_id_clone, join_error);
                TaskResult::Panic(error_msg)
            }
            Err(_) => {
                tracing::error!(
                    "🔨 Task {} timed out after {:?}",
                    task_id_clone,
                    timeout_duration
                );
                TaskResult::Timeout
            }
        }
    }

    /// Update worker statistics
    async fn update_stats(&self, result: TaskResult, duration: Duration) {
        let mut stats = self.stats.lock().await;

        match result {
            TaskResult::Success => {
                stats.tasks_completed += 1;
            }
            TaskResult::Timeout | TaskResult::Panic(_) => {
                stats.tasks_failed += 1;
            }
        }

        // Update average duration (simple moving average)
        if let Some(avg) = stats.avg_task_duration {
            stats.avg_task_duration = Some(Duration::from_nanos(
                (avg.as_nanos() as u64 + duration.as_nanos() as u64) / 2,
            ));
        } else {
            stats.avg_task_duration = Some(duration);
        }
    }
}

/// Manages the worker pool
#[derive(Clone)]
pub struct WorkerManager {
    /// Number of currently active workers
    pub active_workers: Arc<AtomicUsize>,
    /// Handles to all worker tasks
    pub worker_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// Channel to send dead worker notifications
    supervisor_tx: mpsc::Sender<DeadWorker>,
    /// Configuration
    config: SmithyConfig,
    /// Worker statistics
    stats: Arc<Mutex<WorkerStats>>,
}

impl WorkerManager {
    /// Create a new worker manager
    pub fn new(supervisor_tx: mpsc::Sender<DeadWorker>, config: SmithyConfig) -> Self {
        let stats = WorkerStats {
            active_workers: 0,
            total_workers: 0,
            tasks_completed: 0,
            tasks_failed: 0,
            worker_restarts: 0,
            avg_task_duration: None,
        };

        Self {
            active_workers: Arc::new(AtomicUsize::new(0)),
            worker_handles: Arc::new(Mutex::new(Vec::new())),
            supervisor_tx,
            config,
            stats: Arc::new(Mutex::new(stats)),
        }
    }

    /// Spawn a new worker
    pub async fn spawn_worker(
        &self,
        worker_id: usize,
        worker_ready_tx: mpsc::Sender<WorkerRequest>,
    ) -> SmithyResult<()> {
        let active_workers = Arc::clone(&self.active_workers);
        let config = self.config.clone();
        let supervisor_tx = self.supervisor_tx.clone();
        let stats = Arc::clone(&self.stats);

        // Update total workers count
        {
            let mut stats_guard = stats.lock().await;
            stats_guard.total_workers += 1;
        }

        let handle = tokio::spawn(async move {
            // Increment active worker count
            active_workers.fetch_add(1, Ordering::Relaxed);

            // Update active workers in stats
            {
                let mut stats_guard = stats.lock().await;
                stats_guard.active_workers = active_workers.load(Ordering::Relaxed);
            }

            // Create and run worker
            let worker = Worker::new(worker_id, config, stats.clone());
            let _death_cause = worker.run(worker_ready_tx, supervisor_tx).await;

            // Decrement active worker count
            active_workers.fetch_sub(1, Ordering::Relaxed);

            // Update active workers in stats
            {
                let mut stats_guard = stats.lock().await;
                stats_guard.active_workers = active_workers.load(Ordering::Relaxed);
            }
        });

        // Store the handle
        let mut handles = self.worker_handles.lock().await;
        handles.push(handle);

        tracing::debug!("🔨 Spawned worker {}", worker_id);
        Ok(())
    }

    /// Get current worker statistics
    pub async fn stats(&self) -> WorkerStats {
        let stats = self.stats.lock().await;
        stats.clone()
    }

    /// Shutdown all workers gracefully
    pub async fn shutdown(&self, timeout_duration: Duration) -> SmithyResult<String> {
        let handles = {
            let mut guard = self.worker_handles.lock().await;
            std::mem::take(&mut *guard)
        };

        if handles.is_empty() {
            return Ok("No workers to shutdown".to_string());
        }

        tracing::info!(
            "🔨 Shutting down {} workers with timeout {:?}",
            handles.len(),
            timeout_duration
        );

        let mut error_count = 0;
        let mut timeout_count = 0;

        for (i, handle) in handles.into_iter().enumerate() {
            match timeout(timeout_duration, handle).await {
                Ok(Ok(_)) => {
                    tracing::debug!("🔨 Worker handle {} shut down cleanly", i);
                }
                Ok(Err(e)) => {
                    error_count += 1;
                    tracing::error!("🔨 Worker handle {} error: {}", i, e);
                }
                Err(_) => {
                    timeout_count += 1;
                    tracing::warn!("🔨 Worker handle {} timed out", i);
                }
            }
        }

        let result = format!("{} errors, {} timeouts", error_count, timeout_count);
        tracing::info!("🔨 Worker shutdown complete: {}", result);
        Ok(result)
    }

    /// Get the number of active workers
    pub fn active_worker_count(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }

    /// Restart a specific worker (used by supervisor)
    pub async fn restart_worker(
        &self,
        worker_id: usize,
        worker_ready_tx: mpsc::Sender<WorkerRequest>,
    ) -> SmithyResult<()> {
        // Update restart count
        {
            let mut stats = self.stats.lock().await;
            stats.worker_restarts += 1;
        }

        self.spawn_worker(worker_id, worker_ready_tx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_worker_stats() {
        let config = SmithyConfig::default();
        let (supervisor_tx, _supervisor_rx) = mpsc::channel(10);

        let manager = WorkerManager::new(supervisor_tx, config);
        let stats = manager.stats().await;

        assert_eq!(stats.active_workers, 0);
        assert_eq!(stats.total_workers, 0);
        assert_eq!(stats.tasks_completed, 0);
    }

    #[tokio::test]
    async fn test_worker_manager_creation() {
        let config = SmithyConfig::default();
        let (supervisor_tx, _supervisor_rx) = mpsc::channel(10);

        let manager = WorkerManager::new(supervisor_tx, config);
        assert_eq!(manager.active_worker_count(), 0);
    }
}
