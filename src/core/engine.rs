//! The SmithyQ engine - orchestrates workers, dispatcher, and supervisor.
//!
//! The engine is responsible for coordinating all the moving parts of SmithyQ:
//! - Worker pool management
//! - Task dispatching
//! - Worker supervision and recovery
//! - Health monitoring

use crate::config::SmithyConfig;
use crate::core::worker::{DeadWorker, WorkerManager, WorkerRequest, WorkerStats};
use crate::error::{SmithyError, SmithyResult};
use crate::queue::QueueBackend;
use crate::utils::calculate_backoff;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};

/// The main SmithyQ engine that orchestrates all components
pub struct SmithyEngine {
    /// Configuration
    config: SmithyConfig,
    /// Queue backend
    queue: Arc<dyn QueueBackend>,
    /// Worker manager
    worker_manager: WorkerManager,
    /// Communication channels
    worker_ready_tx: mpsc::Sender<WorkerRequest>,
    worker_ready_rx: Option<mpsc::Receiver<WorkerRequest>>,
    supervisor_rx: Arc<Mutex<mpsc::Receiver<DeadWorker>>>,
    /// Control flags
    is_running: Arc<AtomicBool>,
    is_shutting_down: Arc<AtomicBool>,
    /// Component handles
    dispatcher_handle: Option<JoinHandle<()>>,
    supervisor_handle: Option<JoinHandle<()>>,
    monitor_handle: Option<JoinHandle<()>>,
    /// Engine start time for uptime tracking
    start_time: Option<Instant>,
}

impl SmithyEngine {
    /// Create a new SmithyQ engine
    pub fn new(config: SmithyConfig, queue: Arc<dyn QueueBackend>) -> Self {
        let (worker_ready_tx, worker_ready_rx) =
            mpsc::channel::<WorkerRequest>(config.workers.num_workers);
        let (supervisor_tx, supervisor_rx) =
            mpsc::channel::<DeadWorker>(config.workers.num_workers);

        let worker_manager = WorkerManager::new(supervisor_tx, config.clone());

        Self {
            config,
            queue,
            worker_manager,
            worker_ready_tx,
            worker_ready_rx: Some(worker_ready_rx),
            supervisor_rx: Arc::new(Mutex::new(supervisor_rx)),
            is_running: Arc::new(AtomicBool::new(false)),
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            dispatcher_handle: None,
            supervisor_handle: None,
            monitor_handle: None,
            start_time: None,
        }
    }

    /// Start the engine and all its components
    pub async fn start(&mut self) -> SmithyResult<()> {
        if self.is_running.load(Ordering::Relaxed) {
            return Err(SmithyError::AlreadyRunning);
        }

        tracing::info!(
            "Starting SmithyQ engine with {} workers",
            self.config.workers.num_workers
        );

        // Start dispatcher
        let worker_ready_rx = self
            .worker_ready_rx
            .take()
            .ok_or_else(|| SmithyError::config("Engine already started"))?;
        self.dispatcher_handle = Some(self.start_dispatcher(worker_ready_rx).await);

        // Start supervisor
        self.supervisor_handle = Some(self.start_supervisor().await);

        // Start monitor
        self.monitor_handle = Some(self.start_monitor().await);

        // Spawn initial workers
        for worker_id in 0..self.config.workers.num_workers {
            self.worker_manager
                .spawn_worker(worker_id, self.worker_ready_tx.clone())
                .await?;
        }

        self.is_running.store(true, Ordering::Relaxed);
        self.start_time = Some(Instant::now());

        tracing::info!("SmithyQ engine started successfully");
        Ok(())
    }

    /// Shutdown the engine gracefully
    pub async fn shutdown(&self) -> SmithyResult<()> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Err(SmithyError::NotRunning);
        }

        tracing::info!("Shutting down SmithyQ engine...");
        self.is_shutting_down.store(true, Ordering::Relaxed);

        // Close worker ready channel to stop accepting new work
        drop(self.worker_ready_tx.clone());

        // Shutdown workers
        let shutdown_timeout =
            Duration::from_secs(self.config.workers.shutdown_timeout_secs.unwrap_or(30));
        let shutdown_result = self.worker_manager.shutdown(shutdown_timeout).await?;
        tracing::info!("Worker shutdown result: {}", shutdown_result);

        // Stop components
        if let Some(handle) = &self.dispatcher_handle {
            handle.abort();
        }
        if let Some(handle) = &self.supervisor_handle {
            handle.abort();
        }
        if let Some(handle) = &self.monitor_handle {
            handle.abort();
        }

        self.is_running.store(false, Ordering::Relaxed);
        self.is_shutting_down.store(false, Ordering::Relaxed);

        tracing::info!("SmithyQ engine shutdown complete");
        Ok(())
    }

    /// Wait for the engine to shutdown (blocks until shutdown)
    pub async fn wait_for_shutdown(&self) -> SmithyResult<()> {
        // Wait for Ctrl+C or other shutdown signal
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
            }
            _ = self.wait_for_component_failure() => {
                tracing::warn!("Component failure detected, initiating shutdown...");
            }
        }

        self.shutdown().await
    }

    /// Start the task dispatcher
    async fn start_dispatcher(
        &self,
        mut worker_ready_rx: mpsc::Receiver<WorkerRequest>,
    ) -> JoinHandle<()> {
        let queue = Arc::clone(&self.queue);
        let is_shutting_down = Arc::clone(&self.is_shutting_down);

        tokio::spawn(async move {
            tracing::info!("Task dispatcher started");

            while let Some(worker_request) = worker_ready_rx.recv().await {
                if is_shutting_down.load(Ordering::Relaxed) {
                    break;
                }

                // Try to get a task from the queue
                let maybe_task =
                    match tokio::time::timeout(Duration::from_millis(500), queue.dequeue()).await {
                        Ok(Ok(task)) => task,
                        Ok(Err(e)) => {
                            tracing::error!("Queue dequeue error: {}", e);

                            // TODO: Handle specific queue errors (e.g., retry logic)
                            None
                        }
                        Err(e) => {
                            tracing::error!("Queue dequeue timeout: {}", e);
                            None
                        }
                    };

                // Send task to worker (or None if no tasks available)
                if let Err(_) = worker_request.response_tx.send(maybe_task) {
                    tracing::warn!("Failed to send task to worker {}", worker_request.worker_id);
                }
            }

            tracing::info!("Task dispatcher stopped");
        })
    }

    /// Start the worker supervisor
    async fn start_supervisor(&self) -> JoinHandle<()> {
        let supervisor_rx = Arc::clone(&self.supervisor_rx);
        let worker_ready_tx = self.worker_ready_tx.clone();
        let worker_manager = self.worker_manager.clone();
        let is_shutting_down = Arc::clone(&self.is_shutting_down);

        tokio::spawn(async move {
            tracing::info!("Worker supervisor started");

            let mut restart_counts: std::collections::HashMap<usize, u32> =
                std::collections::HashMap::new();

            loop {
                let dead_worker = {
                    let mut rx = supervisor_rx.lock().await;
                    rx.recv().await
                };

                let dead_worker = match dead_worker {
                    Some(dw) => dw,
                    None => break, // Channel closed
                };

                if is_shutting_down.load(Ordering::Relaxed) {
                    break;
                }

                let worker_id = dead_worker.worker_id;
                let restart_count = restart_counts.entry(worker_id).or_insert(0);
                *restart_count += 1;

                tracing::warn!(
                    "Worker {} died ({:?}), restarting... (restart #{})",
                    worker_id,
                    dead_worker.cause,
                    restart_count
                );

                // Exponential backoff for restarts
                if *restart_count > 1 {
                    let backoff_secs = calculate_backoff(*restart_count, 1);
                    tracing::info!("Backing off {}ms before restart", backoff_secs);
                    sleep(Duration::from_millis(backoff_secs)).await;
                }

                // Restart the worker
                if let Err(e) = worker_manager
                    .restart_worker(worker_id, worker_ready_tx.clone())
                    .await
                {
                    tracing::error!("Failed to restart worker {}: {}", worker_id, e);
                }

                // Reset restart count if too high (circuit breaker)
                if *restart_count > 100 {
                    tracing::error!(
                        "Too many restarts for worker {} ({}), pausing supervision",
                        worker_id,
                        restart_count
                    );
                    sleep(Duration::from_secs(60)).await;
                    *restart_count = 0;
                }
            }

            tracing::info!("Worker supervisor stopped");
        })
    }

    /// Start the health monitor
    async fn start_monitor(&self) -> JoinHandle<()> {
        let worker_manager = self.worker_manager.clone();
        let queue = Arc::clone(&self.queue);
        let expected_workers = self.config.workers.num_workers;
        let is_shutting_down = Arc::clone(&self.is_shutting_down);
        let start_time = Instant::now();

        tokio::spawn(async move {
            tracing::info!("Health monitor started");

            let mut interval = interval(Duration::from_secs(30));

            loop {
                interval.tick().await;

                if is_shutting_down.load(Ordering::Relaxed) {
                    break;
                }

                let active_count = worker_manager.active_worker_count();
                let worker_stats = worker_manager.stats().await;

                let queue_stats = match queue.stats().await {
                    Ok(stats) => stats,
                    Err(e) => {
                        tracing::error!("Failed to get queue stats: {}", e);
                        continue;
                    }
                };

                let health_status = if active_count == expected_workers {
                    "HEALTHY"
                } else if active_count > 0 {
                    "DEGRADED"
                } else {
                    "CRITICAL"
                };

                tracing::info!(
                    "HEALTH CHECK - Status: {} | Workers: {}/{} | Queue: P:{} R:{} C:{} F:{} D:{} | Uptime: {:?}",
                    health_status,
                    active_count,
                    expected_workers,
                    queue_stats.pending,
                    queue_stats.running,
                    queue_stats.completed,
                    queue_stats.failed,
                    queue_stats.dead,
                    start_time.elapsed()
                );

                if active_count == 0 {
                    tracing::error!("CRITICAL: All workers are dead!");
                }

                // Log worker performance stats
                if worker_stats.tasks_completed > 0 {
                    tracing::debug!(
                        "Worker performance - Completed: {} | Failed: {} | Restarts: {} | Avg duration: {:?}",
                        worker_stats.tasks_completed,
                        worker_stats.tasks_failed,
                        worker_stats.worker_restarts,
                        worker_stats.avg_task_duration
                    );
                }
            }

            tracing::info!("Health monitor stopped");
        })
    }

    /// Wait for any critical component to fail
    async fn wait_for_component_failure(&self) {
        tokio::select! {
            _ = async {
                if let Some(handle) = &self.dispatcher_handle {
                    let _ = handle;
                }
            } => {
                tracing::error!("Dispatcher failed");
            }
            _ = async {
                if let Some(handle) = &self.supervisor_handle {
                    let _ = handle;
                }
            } => {
                tracing::error!("Supervisor failed");
            }
        }
    }

    /// Get worker statistics
    pub async fn worker_stats(&self) -> WorkerStats {
        self.worker_manager.stats().await
    }

    /// Perform a health check
    pub async fn health_check(&self) -> SmithyResult<()> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Err(SmithyError::NotRunning);
        }

        // Check if we have the expected number of workers
        let active_workers = self.worker_manager.active_worker_count();
        let expected_workers = self.config.workers.num_workers;

        if active_workers == 0 {
            return Err(SmithyError::config("No active workers"));
        }

        if active_workers < expected_workers / 2 {
            tracing::warn!(
                "Less than half of expected workers active: {}/{}",
                active_workers,
                expected_workers
            );
        }

        Ok(())
    }

    /// Get engine uptime
    pub fn uptime(&self) -> Option<Duration> {
        self.start_time.map(|start| start.elapsed())
    }

    /// Check if engine is running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    /// Check if engine is shutting down
    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::memory::InMemoryQueue;

    #[tokio::test]
    async fn test_engine_creation() {
        let config = SmithyConfig::default();
        let queue = Arc::new(InMemoryQueue::new());

        let engine = SmithyEngine::new(config, queue);
        assert!(!engine.is_running());
        assert!(!engine.is_shutting_down());
    }

    #[tokio::test]
    async fn test_engine_lifecycle() {
        let mut config = SmithyConfig::default();
        config.workers.num_workers = 1; // Single worker for test

        let queue = Arc::new(InMemoryQueue::new());
        let mut engine = SmithyEngine::new(config, queue);

        // Start engine
        engine.start().await.unwrap();
        assert!(engine.is_running());

        // Give it a moment to fully start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check health
        assert!(engine.health_check().await.is_ok());

        // Shutdown engine
        engine.shutdown().await.unwrap();
        assert!(!engine.is_running());
    }
}
