//! Redis queue implementation for SmithyQ.
//!
//! This queue backend uses Redis as the storage layer, providing distributed
//! queue capabilities suitable for multi-process and multi-server deployments.
//!
//! # Features
//!
//! - **Distributed**: Multiple processes can share the same queue
//! - **Persistent**: Tasks survive process restarts
//! - **Scalable**: Redis handles high throughput efficiently
//! - **Atomic operations**: Uses Redis transactions for consistency

use super::{QueueBackend, QueueConfig, QueueStats};
use crate::config::RedisConfig;
use crate::error::{SmithyError, SmithyResult};
use crate::task::{QueuedTask, TaskId, TaskStatus};
use async_trait::async_trait;

use redis::{AsyncCommands, Client, Script, aio::ConnectionManager};

use serde_json::{self, Value as JsonValue};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Redis queue backend implementation
pub struct RedisQueue {
    /// Redis connection manager
    conn: ConnectionManager,
    /// Redis configuration
    config: RedisConfig,
    /// Queue configuration
    queue_config: QueueConfig,
    /// Cached statistics (updated periodically to avoid frequent Redis calls)
    cached_stats: RwLock<Option<(QueueStats, SystemTime)>>,
    /// Pre-compiled Lua scripts for atomic operations
    scripts: RedisScripts,
}

impl std::fmt::Debug for RedisQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisQueue")
            .field("config", &self.config)
            .field("queue_config", &self.queue_config)
            .field("cached_stats", &self.cached_stats)
            .field("scripts", &self.scripts)
            .finish_non_exhaustive()
    }
}

/// Pre-compiled Lua scripts for atomic Redis operations
#[derive(Debug)]
struct RedisScripts {
    /// Script for atomic dequeue operation
    dequeue_script: Script,
    /// Script for atomic fail operation with retry logic
    fail_script: Script,
    /// Script for cleanup of expired tasks
    cleanup_script: Script,
    /// Script for updating task status atomically
    update_status_script: Script,
}

impl RedisScripts {
    fn new() -> Self {
        Self {
            dequeue_script: Script::new(include_str!("scripts/dequeue.lua")),
            fail_script: Script::new(include_str!("scripts/fail_task.lua")),
            cleanup_script: Script::new(include_str!("scripts/cleanup.lua")),
            update_status_script: Script::new(include_str!("scripts/update_status.lua")),
        }
    }
}

impl RedisQueue {
    /// Create a new Redis queue with the given connection string and configuration
    pub async fn new(connection_string: &str, queue_config: QueueConfig) -> SmithyResult<Self> {
        let redis_config = match &queue_config.backend {
            crate::config::QueueBackendConfig::Redis(config) => config.clone(),
            _ => {
                return Err(SmithyError::ConfigError {
                    message: "Redis configuration not found in queue config".to_string(),
                });
            }
        };

        let client = Client::open(connection_string).map_err(|e| SmithyError::QueueError {
            message: format!("Failed to create Redis client: {}", e),
            source: Some(Box::new(e)),
        })?;

        println!(
            "🔧 Configuring Redis queue with prefix: {}",
            redis_config.key_prefix
        );

        let conn = timeout(Duration::from_secs(10), client.get_connection_manager())
            .await
            .map_err(|e| SmithyError::QueueError {
                message: "Timed out connecting to Redis".to_string(),
                source: Some(Box::new(e)),
            })?
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to create Redis connection manager: {}", e),
                source: Some(Box::new(e)),
            })?;

        println!("✅ Connected to Redis");

        Ok(Self {
            conn,
            config: redis_config,
            queue_config,
            cached_stats: RwLock::new(None),
            scripts: RedisScripts::new(),
        })
    }

    /// Create a new Redis queue from existing configuration
    pub async fn from_config(
        redis_config: RedisConfig,
        queue_config: QueueConfig,
    ) -> SmithyResult<Self> {
        Self::new(&redis_config.connection_string, queue_config).await
    }

    /// Get Redis key for pending tasks queue
    fn pending_key(&self) -> String {
        format!("{}:pending", self.config.key_prefix)
    }

    /// Get Redis key for running tasks hash
    fn running_key(&self) -> String {
        format!("{}:running", self.config.key_prefix)
    }

    /// Get Redis key for task data hash
    fn tasks_key(&self) -> String {
        format!("{}:tasks", self.config.key_prefix)
    }

    /// Get Redis key for completed tasks set
    fn completed_key(&self) -> String {
        format!("{}:completed", self.config.key_prefix)
    }

    /// Get Redis key for failed tasks set
    fn failed_key(&self) -> String {
        format!("{}:failed", self.config.key_prefix)
    }

    /// Get Redis key for dead tasks set
    fn dead_key(&self) -> String {
        format!("{}:dead", self.config.key_prefix)
    }

    /// Get Redis key for delayed tasks sorted set
    fn delayed_key(&self) -> String {
        format!("{}:delayed", self.config.key_prefix)
    }

    /// Get Redis key for queue statistics
    fn stats_key(&self) -> String {
        format!("{}:stats", self.config.key_prefix)
    }

    /// Convert SystemTime to Unix timestamp in seconds
    fn system_time_to_timestamp(time: SystemTime) -> u64 {
        time.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Convert Unix timestamp to SystemTime
    fn timestamp_to_system_time(timestamp: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(timestamp)
    }

    /// Serialize task to JSON for Redis storage
    fn serialize_task(task: &QueuedTask) -> SmithyResult<String> {
        let mut task_data =
            serde_json::to_value(task).map_err(|e| SmithyError::SerializationError(e))?;

        // Convert SystemTime to timestamp for JSON serialization
        if let Some(obj) = task_data.as_object_mut() {
            obj.insert(
                "created_at_timestamp".to_string(),
                JsonValue::Number(Self::system_time_to_timestamp(task.created_at).into()),
            );
            obj.insert(
                "updated_at_timestamp".to_string(),
                JsonValue::Number(Self::system_time_to_timestamp(task.updated_at).into()),
            );
            if let Some(execute_at) = task.execute_at {
                obj.insert(
                    "execute_at_timestamp".to_string(),
                    JsonValue::Number(Self::system_time_to_timestamp(execute_at).into()),
                );
            }
        }

        serde_json::to_string(&task_data).map_err(|e| SmithyError::SerializationError(e))
    }

    // / Deserialize task from JSON stored in Redis
    fn deserialize_task(data: &str) -> SmithyResult<QueuedTask> {
        let task_data: JsonValue =
            serde_json::from_str(data).map_err(|e| SmithyError::SerializationError(e))?;

        let task: QueuedTask =
            serde_json::from_value(task_data).map_err(|e| SmithyError::SerializationError(e))?;

        Ok(task)
    }

    /// Update cached statistics
    async fn update_cached_stats(&self) -> SmithyResult<()> {
        let mut conn = self.conn.clone();

        // Get counts from Redis using pipeline for efficiency
        let (pending, running, completed, failed, dead): (u64, u64, u64, u64, u64) = redis::pipe()
            .llen(&self.pending_key())
            .hlen(&self.running_key())
            .scard(&self.completed_key())
            .scard(&self.failed_key())
            .scard(&self.dead_key())
            .query_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to get queue statistics: {}", e),
                source: Some(Box::new(e)),
            })?;

        // Get total processed from stats hash or calculate it
        let total_processed: u64 = conn
            .hget(&self.stats_key(), "total_processed")
            .await
            .unwrap_or(completed);

        let stats = QueueStats {
            pending,
            running,
            completed,
            failed,
            dead,
            total_processed,
        };

        let mut cached_stats = self.cached_stats.write().await;
        *cached_stats = Some((stats, SystemTime::now()));

        Ok(())
    }

    /// Get cached statistics or update if stale
    async fn get_stats_cached(&self) -> SmithyResult<QueueStats> {
        const CACHE_DURATION: Duration = Duration::from_secs(5);

        let cached_stats = self.cached_stats.read().await;
        if let Some((stats, timestamp)) = &*cached_stats {
            if timestamp.elapsed().unwrap_or(CACHE_DURATION) < CACHE_DURATION {
                return Ok(stats.clone());
            }
        }
        drop(cached_stats);

        // Cache is stale or empty, update it
        self.update_cached_stats().await?;

        let cached_stats = self.cached_stats.read().await;
        Ok(cached_stats.as_ref().unwrap().0.clone())
    }

    /// Process expired visibility timeouts by moving tasks back to pending
    async fn process_visibility_timeouts(&self) -> SmithyResult<()> {
        let mut conn = self.conn.clone();
        let now = Self::system_time_to_timestamp(SystemTime::now());

        // Use Lua script for atomic operation
        let script = Script::new(
            r#"
            local running_key = KEYS[1]
            local pending_key = KEYS[2]
            local tasks_key = KEYS[3]
            local now = tonumber(ARGV[1])

            local expired_tasks = {}
            local running_tasks = redis.call('HGETALL', running_key)

            for i = 1, #running_tasks, 2 do
                local task_id = running_tasks[i]
                local visible_at = tonumber(running_tasks[i + 1])

                if visible_at <= now then
                    table.insert(expired_tasks, task_id)
                    redis.call('HDEL', running_key, task_id)
                    redis.call('LPUSH', pending_key, task_id)

                    -- Update task status to Pending
                    local task_data = redis.call('HGET', tasks_key, task_id)
                    if task_data then
                        local task = cjson.decode(task_data)
                        task.status = 'Pending'
                        task.updated_at_timestamp = now
                        redis.call('HSET', tasks_key, task_id, cjson.encode(task))
                    end
                end
            end

            return #expired_tasks
        "#,
        );

        let expired_count: i32 = script
            .key(&self.running_key())
            .key(&self.pending_key())
            .key(&self.tasks_key())
            .arg(now)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to process visibility timeouts: {}", e),
                source: Some(Box::new(e)),
            })?;

        if expired_count > 0 {
            debug!("Processed {} expired visibility timeouts", expired_count);
        }

        Ok(())
    }

    /// Move delayed tasks that are ready to execute to the pending queue
    async fn process_delayed_tasks(&self) -> SmithyResult<()> {
        let mut conn = self.conn.clone();
        let now = Self::system_time_to_timestamp(SystemTime::now());

        // Get tasks that are ready to execute
        let ready_tasks: Vec<String> = conn
            .zrangebyscore_limit(&self.delayed_key(), 0, now as isize, 0, 100)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to get ready delayed tasks: {}", e),
                source: Some(Box::new(e)),
            })?;

        if !ready_tasks.is_empty() {
            // Move tasks from delayed to pending queue atomically
            let script = Script::new(
                r#"
                local delayed_key = KEYS[1]
                local pending_key = KEYS[2]
                local tasks_key = KEYS[3]
                local now = tonumber(ARGV[1])

                local ready_tasks = redis.call('ZRANGEBYSCORE', delayed_key, 0, now, 'LIMIT', 0, 100)
                local moved_count = 0

                for i = 1, #ready_tasks do
                    local task_id = ready_tasks[i]
                    redis.call('ZREM', delayed_key, task_id)
                    redis.call('LPUSH', pending_key, task_id)

                    -- Update task status to Pending
                    local task_data = redis.call('HGET', tasks_key, task_id)
                    if task_data then
                        local task = cjson.decode(task_data)
                        task.status = 'Pending'
                        task.updated_at_timestamp = now
                        redis.call('HSET', tasks_key, task_id, cjson.encode(task))
                    end

                    moved_count = moved_count + 1
                end

                return moved_count
            "#,
            );

            let moved_count: i32 = script
                .key(&self.delayed_key())
                .key(&self.pending_key())
                .key(&self.tasks_key())
                .arg(now)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| SmithyError::QueueError {
                    message: format!("Failed to process delayed tasks: {}", e),
                    source: Some(Box::new(e)),
                })?;

            if moved_count > 0 {
                debug!("Moved {} delayed tasks to pending queue", moved_count);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl QueueBackend for RedisQueue {
    async fn enqueue(&self, mut task: QueuedTask) -> SmithyResult<TaskId> {
        let mut conn = self.conn.clone();

        // Check queue size limit if configured
        if self.queue_config.max_queue_size > 0 {
            let current_size: u64 =
                conn.hlen(&self.tasks_key())
                    .await
                    .map_err(|e| SmithyError::QueueError {
                        message: format!("Failed to check queue size: {}", e),
                        source: Some(Box::new(e)),
                    })?;

            if current_size >= self.queue_config.max_queue_size as u64 {
                return Err(SmithyError::QueueError {
                    message: format!(
                        "Queue is full (max size: {})",
                        self.queue_config.max_queue_size
                    ),
                    source: None,
                });
            }
        }

        let task_id = task.id.clone();
        let now = SystemTime::now();

        task.status = TaskStatus::Pending;
        task.created_at = now;
        task.updated_at = now;

        let task_json = Self::serialize_task(&task)?;

        // Store task and add to appropriate queue
        if let Some(execute_at) = task.execute_at {
            if execute_at > now {
                // Delayed task - add to delayed queue
                let execute_timestamp = Self::system_time_to_timestamp(execute_at);

                redis::pipe()
                    .atomic()
                    .hset(&self.tasks_key(), &task_id, &task_json)
                    .zadd(&self.delayed_key(), &task_id, execute_timestamp as isize)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| SmithyError::QueueError {
                        message: format!("Failed to enqueue delayed task: {}", e),
                        source: Some(Box::new(e)),
                    })?;
            } else {
                // Execute time has passed - add to pending queue
                redis::pipe()
                    .atomic()
                    .hset(&self.tasks_key(), &task_id, &task_json)
                    .lpush(&self.pending_key(), &task_id)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| SmithyError::QueueError {
                        message: format!("Failed to enqueue task: {}", e),
                        source: Some(Box::new(e)),
                    })?;
            }
        } else {
            // Immediate task - add to pending queue
            redis::pipe()
                .atomic()
                .hset(&self.tasks_key(), &task_id, &task_json)
                .lpush(&self.pending_key(), &task_id)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| SmithyError::QueueError {
                    message: format!("Failed to enqueue task: {}", e),
                    source: Some(Box::new(e)),
                })?;
        }

        debug!("Enqueued task: {}", task_id);
        Ok(task_id)
    }

    async fn dequeue(&self) -> SmithyResult<Option<QueuedTask>> {
        // Process expired timeouts and delayed tasks first
        self.process_visibility_timeouts().await?;
        self.process_delayed_tasks().await?;

        let mut conn = self.conn.clone();
        let now = SystemTime::now();
        let visible_at = now + Duration::from_secs(self.queue_config.visibility_timeout_secs);
        let visible_timestamp = Self::system_time_to_timestamp(visible_at);

        // Use Lua script for atomic dequeue operation
        let script = Script::new(
            r#"
            local pending_key = KEYS[1]
            local running_key = KEYS[2]
            local tasks_key = KEYS[3]
            local visible_timestamp = tonumber(ARGV[1])
            local now = tonumber(ARGV[2])

            local task_id = redis.call('RPOP', pending_key)
            if not task_id then
                return nil
            end

            local task_data = redis.call('HGET', tasks_key, task_id)
            if not task_data then
                return nil
            end

            -- Set task as running with visibility timeout
            redis.call('HSET', running_key, task_id, visible_timestamp)

            -- Update task status
            local task = cjson.decode(task_data)
            task.status = 'Running'
            task.updated_at_timestamp = now
            local updated_task_data = cjson.encode(task)
            redis.call('HSET', tasks_key, task_id, updated_task_data)

            return updated_task_data
        "#,
        );

        let result: Option<String> = script
            .key(&self.pending_key())
            .key(&self.running_key())
            .key(&self.tasks_key())
            .arg(visible_timestamp)
            .arg(Self::system_time_to_timestamp(now))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to dequeue task: {}", e),
                source: Some(Box::new(e)),
            })?;

        if let Some(task_json) = result {
            let task = Self::deserialize_task(&task_json)?;
            debug!("Dequeued task: {}", task.id);
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    async fn dequeue_batch(&self, count: usize) -> SmithyResult<Vec<QueuedTask>> {
        let mut batch = Vec::with_capacity(count);

        for _ in 0..count {
            if let Some(task) = self.dequeue().await? {
                batch.push(task);
            } else {
                break;
            }
        }

        Ok(batch)
    }

    async fn complete_task(&self, task_id: &TaskId) -> SmithyResult<()> {
        let mut conn = self.conn.clone();
        let now = SystemTime::now();
        let now_timestamp = Self::system_time_to_timestamp(now);

        // Use Lua script for atomic completion
        let script = Script::new(
            r#"
            local running_key = KEYS[1]
            local completed_key = KEYS[2]
            local tasks_key = KEYS[3]
            local stats_key = KEYS[4]
            local task_id = ARGV[1]
            local now = tonumber(ARGV[2])

            -- Remove from running
            local was_running = redis.call('HDEL', running_key, task_id)
            if was_running == 0 then
                return 0  -- Task was not running
            end

            -- Add to completed set
            redis.call('SADD', completed_key, task_id)

            -- Update task status
            local task_data = redis.call('HGET', tasks_key, task_id)
            if task_data then
                local task = cjson.decode(task_data)
                task.status = 'Completed'
                task.updated_at_timestamp = now
                redis.call('HSET', tasks_key, task_id, cjson.encode(task))
            end

            -- Increment total processed counter
            redis.call('HINCRBY', stats_key, 'total_processed', 1)

            return 1
        "#,
        );

        let result: i32 = script
            .key(&self.running_key())
            .key(&self.completed_key())
            .key(&self.tasks_key())
            .key(&self.stats_key())
            .arg(task_id)
            .arg(now_timestamp)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to complete task: {}", e),
                source: Some(Box::new(e)),
            })?;

        if result == 0 {
            return Err(SmithyError::TaskNotFound {
                task_type: task_id.clone(),
            });
        }

        debug!("Completed task: {}", task_id);
        Ok(())
    }

    async fn fail_task(&self, task_id: &TaskId, error: &str, retry: bool) -> SmithyResult<()> {
        let mut conn = self.conn.clone();
        let now = SystemTime::now();
        let now_timestamp = Self::system_time_to_timestamp(now);

        // Use Lua script for atomic fail operation
        let script = Script::new(
            r#"
            local running_key = KEYS[1]
            local pending_key = KEYS[2]
            local failed_key = KEYS[3]
            local dead_key = KEYS[4]
            local tasks_key = KEYS[5]
            local task_id = ARGV[1]
            local now = tonumber(ARGV[2])
            local should_retry = ARGV[3] == 'true'
            local error_msg = ARGV[4]

            -- Remove from running
            local was_running = redis.call('HDEL', running_key, task_id)
            if was_running == 0 then
                return 0  -- Task was not running
            end

            local task_data = redis.call('HGET', tasks_key, task_id)
            if not task_data then
                return 0
            end

            local task = cjson.decode(task_data)
            task.retry_count = (task.retry_count or 0) + 1
            task.updated_at_timestamp = now

            if should_retry and task.retry_count <= task.max_retries then
                -- Retry the task
                task.status = 'Failed'
                redis.call('LPUSH', pending_key, task_id)
                redis.call('SADD', failed_key, task_id)
            else
                -- Max retries exceeded
                task.status = 'Dead'
                redis.call('SADD', dead_key, task_id)
            end

            redis.call('HSET', tasks_key, task_id, cjson.encode(task))
            return 1
        "#,
        );

        let result: i32 = script
            .key(&self.running_key())
            .key(&self.pending_key())
            .key(&self.failed_key())
            .key(&self.dead_key())
            .key(&self.tasks_key())
            .arg(task_id)
            .arg(now_timestamp)
            .arg(if retry { "true" } else { "false" })
            .arg(error)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to fail task: {}", e),
                source: Some(Box::new(e)),
            })?;

        if result == 0 {
            return Err(SmithyError::TaskNotFound {
                task_type: task_id.clone(),
            });
        }

        if retry {
            warn!("Task {} failed and will be retried: {}", task_id, error);
        } else {
            error!("Task {} permanently failed: {}", task_id, error);
        }

        Ok(())
    }

    async fn requeue_task(&self, task_id: &TaskId, delay: Option<Duration>) -> SmithyResult<()> {
        let mut conn = self.conn.clone();
        let now = SystemTime::now();
        let now_timestamp = Self::system_time_to_timestamp(now);

        let task_data: Option<String> =
            conn.hget(&self.tasks_key(), task_id)
                .await
                .map_err(|e| SmithyError::QueueError {
                    message: format!("Failed to get task for requeue: {}", e),
                    source: Some(Box::new(e)),
                })?;

        let task_json = task_data.ok_or_else(|| SmithyError::TaskNotFound {
            task_type: task_id.clone(),
        })?;

        if let Some(delay) = delay {
            let execute_at = now + delay;
            let execute_timestamp = Self::system_time_to_timestamp(execute_at);

            // Add to delayed queue
            redis::pipe()
                .atomic()
                .zadd(&self.delayed_key(), task_id, execute_timestamp as isize)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| SmithyError::QueueError {
                    message: format!("Failed to requeue delayed task: {}", e),
                    source: Some(Box::new(e)),
                })?;
        } else {
            // Add to pending queue immediately
            conn.lpush::<_, _, ()>(&self.pending_key(), task_id)
                .await
                .map_err(|e| SmithyError::QueueError {
                    message: format!("Failed to requeue task: {}", e),
                    source: Some(Box::new(e)),
                })?;
        }

        // Update task status
        let script = Script::new(
            r#"
            local tasks_key = KEYS[1]
            local task_id = ARGV[1]
            local now = tonumber(ARGV[2])

            local task_data = redis.call('HGET', tasks_key, task_id)
            if task_data then
                local task = cjson.decode(task_data)
                task.status = 'Pending'
                task.updated_at_timestamp = now
                redis.call('HSET', tasks_key, task_id, cjson.encode(task))
            end

            return 1
        "#,
        );

        script
            .key(&self.tasks_key())
            .arg(task_id)
            .arg(now_timestamp)
            .invoke_async::<()>(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to update requeued task status: {}", e),
                source: Some(Box::new(e)),
            })?;

        debug!("Requeued task {} with delay: {:?}", task_id, delay);
        Ok(())
    }

    async fn get_task(&self, task_id: &TaskId) -> SmithyResult<Option<QueuedTask>> {
        let mut conn = self.conn.clone();

        let task_data: Option<String> =
            conn.hget(&self.tasks_key(), task_id)
                .await
                .map_err(|e| SmithyError::QueueError {
                    message: format!("Failed to get task: {}", e),
                    source: Some(Box::new(e)),
                })?;

        if let Some(json) = task_data {
            let task = Self::deserialize_task(&json)?;
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    async fn update_task_status(&self, task_id: &TaskId, status: TaskStatus) -> SmithyResult<()> {
        let mut conn = self.conn.clone();
        let now_timestamp = Self::system_time_to_timestamp(SystemTime::now());

        let script = Script::new(
            r#"
            local tasks_key = KEYS[1]
            local task_id = ARGV[1]
            local new_status = ARGV[2]
            local now = tonumber(ARGV[3])

            local task_data = redis.call('HGET', tasks_key, task_id)
            if not task_data then
                return 0
            end

            local task = cjson.decode(task_data)
            task.status = new_status
            task.updated_at_timestamp = now
            redis.call('HSET', tasks_key, task_id, cjson.encode(task))

            return 1
        "#,
        );

        let result: i32 = script
            .key(&self.tasks_key())
            .arg(task_id)
            .arg(format!("{:?}", status))
            .arg(now_timestamp)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to update task status: {}", e),
                source: Some(Box::new(e)),
            })?;

        if result == 0 {
            return Err(SmithyError::TaskNotFound {
                task_type: task_id.clone(),
            });
        }

        Ok(())
    }

    async fn stats(&self) -> SmithyResult<QueueStats> {
        self.get_stats_cached().await
    }

    async fn cleanup(&self) -> SmithyResult<u64> {
        // Process visibility timeouts first
        self.process_visibility_timeouts().await?;

        let mut conn = self.conn.clone();
        let now = SystemTime::now();
        let cleanup_age = Duration::from_secs(3600); // 1 hour
        let cleanup_threshold = Self::system_time_to_timestamp(now - cleanup_age);

        // Clean up old completed and dead tasks
        let script = Script::new(
            r#"
            local tasks_key = KEYS[1]
            local completed_key = KEYS[2]
            local dead_key = KEYS[3]
            local cleanup_threshold = tonumber(ARGV[1])

            local cleaned_count = 0

            -- Clean completed tasks
            local completed_tasks = redis.call('SMEMBERS', completed_key)
            for i = 1, #completed_tasks do
                local task_id = completed_tasks[i]
                local task_data = redis.call('HGET', tasks_key, task_id)
                if task_data then
                    local task = cjson.decode(task_data)
                    if task.updated_at_timestamp and task.updated_at_timestamp < cleanup_threshold then
                        redis.call('HDEL', tasks_key, task_id)
                        redis.call('SREM', completed_key, task_id)
                        cleaned_count = cleaned_count + 1
                    end
                end
            end

            -- Clean dead tasks
            local dead_tasks = redis.call('SMEMBERS', dead_key)
            for i = 1, #dead_tasks do
                local task_id = dead_tasks[i]
                local task_data = redis.call('HGET', tasks_key, task_id)
                if task_data then
                    local task = cjson.decode(task_data)
                    if task.updated_at_timestamp and task.updated_at_timestamp < cleanup_threshold then
                        redis.call('HDEL', tasks_key, task_id)
                        redis.call('SREM', dead_key, task_id)
                        cleaned_count = cleaned_count + 1
                    end
                end
            end

            return cleaned_count
        "#,
        );

        let cleaned_count: u64 = script
            .key(&self.tasks_key())
            .key(&self.completed_key())
            .key(&self.dead_key())
            .arg(cleanup_threshold)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to cleanup old tasks: {}", e),
                source: Some(Box::new(e)),
            })?;

        if cleaned_count > 0 {
            info!("Cleaned up {} old tasks", cleaned_count);
        }

        Ok(cleaned_count)
    }

    async fn get_tasks_by_status(
        &self,
        status: TaskStatus,
        limit: Option<usize>,
    ) -> SmithyResult<Vec<QueuedTask>> {
        let mut conn = self.conn.clone();

        // Get task IDs based on status
        let task_ids: Vec<String> =
            match status {
                TaskStatus::Pending => {
                    let pending: Vec<String> = conn
                        .lrange(&self.pending_key(), 0, -1)
                        .await
                        .map_err(|e| SmithyError::QueueError {
                            message: format!("Failed to get pending tasks: {}", e),
                            source: Some(Box::new(e)),
                        })?;
                    let delayed: Vec<String> = conn
                        .zrange(&self.delayed_key(), 0, -1)
                        .await
                        .map_err(|e| SmithyError::QueueError {
                            message: format!("Failed to get delayed tasks: {}", e),
                            source: Some(Box::new(e)),
                        })?;
                    [pending, delayed].concat()
                }
                TaskStatus::Running => {
                    let running_hash: HashMap<String, String> = conn
                        .hgetall(&self.running_key())
                        .await
                        .map_err(|e| SmithyError::QueueError {
                            message: format!("Failed to get running tasks: {}", e),
                            source: Some(Box::new(e)),
                        })?;
                    running_hash.into_keys().collect()
                }
                TaskStatus::Completed => {
                    conn.smembers(&self.completed_key()).await.map_err(|e| {
                        SmithyError::QueueError {
                            message: format!("Failed to get completed tasks: {}", e),
                            source: Some(Box::new(e)),
                        }
                    })?
                }
                TaskStatus::Failed => conn.smembers(&self.failed_key()).await.map_err(|e| {
                    SmithyError::QueueError {
                        message: format!("Failed to get failed tasks: {}", e),
                        source: Some(Box::new(e)),
                    }
                })?,
                TaskStatus::Dead => {
                    conn.smembers(&self.dead_key())
                        .await
                        .map_err(|e| SmithyError::QueueError {
                            message: format!("Failed to get dead tasks: {}", e),
                            source: Some(Box::new(e)),
                        })?
                }
            };

        // Apply limit if specified
        let limited_ids = if let Some(limit) = limit {
            task_ids.into_iter().take(limit).collect()
        } else {
            task_ids
        };

        // Fetch task data
        let mut tasks = Vec::new();
        for task_id in limited_ids {
            if let Some(task_data) = conn.hget(&self.tasks_key(), &task_id).await.map_err(|e| {
                SmithyError::QueueError {
                    message: format!("Failed to get task data: {}", e),
                    source: Some(Box::new(e)),
                }
            })? {
                let task: String = task_data;
                if let Ok(parsed_task) = Self::deserialize_task(&task) {
                    if parsed_task.status == status {
                        tasks.push(parsed_task);
                    }
                }
            }
        }

        // Sort by created_at (newest first)
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok(tasks)
    }

    async fn purge(&self) -> SmithyResult<u64> {
        let mut conn = self.conn.clone();

        let task_count: u64 =
            conn.hlen(&self.tasks_key())
                .await
                .map_err(|e| SmithyError::QueueError {
                    message: format!("Failed to get task count for purge: {}", e),
                    source: Some(Box::new(e)),
                })?;

        // Delete all queue-related keys
        let keys = vec![
            self.pending_key(),
            self.running_key(),
            self.tasks_key(),
            self.completed_key(),
            self.failed_key(),
            self.dead_key(),
            self.delayed_key(),
        ];

        redis::pipe()
            .atomic()
            .del(&keys)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to purge queue: {}", e),
                source: Some(Box::new(e)),
            })?;

        // Reset stats except total_processed
        let total_processed: u64 = conn
            .hget(&self.stats_key(), "total_processed")
            .await
            .unwrap_or(0);

        redis::pipe()
            .atomic()
            .del(&self.stats_key())
            .hset(&self.stats_key(), "total_processed", total_processed)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to reset stats: {}", e),
                source: Some(Box::new(e)),
            })?;

        warn!("Purged {} tasks from Redis queue", task_count);
        Ok(task_count)
    }

    async fn health_check(&self) -> SmithyResult<()> {
        let mut conn = self.conn.clone();

        // Simple ping to check Redis connectivity
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Redis health check failed: {}", e),
                source: Some(Box::new(e)),
            })?;

        if pong != "PONG" {
            return Err(SmithyError::QueueError {
                message: "Redis health check failed: unexpected response".to_string(),
                source: None,
            });
        }

        // Check queue integrity
        let (pending_count, running_count, tasks_count): (u64, u64, u64) = redis::pipe()
            .llen(&self.pending_key())
            .hlen(&self.running_key())
            .hlen(&self.tasks_key())
            .query_async(&mut conn)
            .await
            .map_err(|e| SmithyError::QueueError {
                message: format!("Failed to check queue integrity: {}", e),
                source: Some(Box::new(e)),
            })?;

        debug!(
            "Redis queue health check passed: {} total tasks, {} pending, {} running",
            tasks_count, pending_count, running_count
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::QueueConfig;
    use serde_json::json;
    use std::time::SystemTime;

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

    // Note: These tests require a running Redis instance
    // They are integration tests and should be run with: cargo test --features redis-queue

    #[tokio::test]
    #[ignore] // Ignore by default since it requires Redis
    async fn test_redis_queue_basic_operations() {
        let config = QueueConfig::redis("redis://localhost:6379".to_string());
        let queue = RedisQueue::new("redis://localhost:6379", config)
            .await
            .expect("Failed to create Redis queue");

        // Clean up any existing data
        let _ = queue.purge().await;

        let task = create_test_task("test-redis-1");

        // Test enqueue
        let task_id = queue.enqueue(task.clone()).await.unwrap();
        assert_eq!(task_id, "test-redis-1");

        // Test dequeue
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_some());
        let dequeued_task = dequeued.unwrap();
        assert_eq!(dequeued_task.id, "test-redis-1");
        assert_eq!(dequeued_task.status, TaskStatus::Running);

        // Test complete
        queue.complete_task(&task_id).await.unwrap();

        // Test stats
        let stats = queue.stats().await.unwrap();
        assert_eq!(stats.completed, 1);

        // Clean up
        let _ = queue.purge().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_queue_delayed_tasks() {
        let config = QueueConfig::redis("redis://localhost:6379".to_string());
        let queue = RedisQueue::new("redis://localhost:6379", config)
            .await
            .expect("Failed to create Redis queue");

        // Clean up
        let _ = queue.purge().await;

        let mut task = create_test_task("test-delayed-1");
        task.execute_at = Some(SystemTime::now() + Duration::from_secs(1));

        // Enqueue delayed task
        queue.enqueue(task).await.unwrap();

        // Should not be available immediately
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_none());

        // Wait for the delay (in real tests, you might mock time)
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Should be available now
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_some());

        // Clean up
        let _ = queue.purge().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_queue_visibility_timeout() {
        let mut config = QueueConfig::redis("redis://localhost:6379".to_string());
        config.visibility_timeout_secs = 1; // Very short timeout for testing

        let queue = RedisQueue::new("redis://localhost:6379", config)
            .await
            .expect("Failed to create Redis queue");

        // Clean up
        let _ = queue.purge().await;

        let task = create_test_task("test-visibility-1");
        queue.enqueue(task).await.unwrap();

        // Dequeue task
        let dequeued = queue.dequeue().await.unwrap();
        assert!(dequeued.is_some());

        // Wait for visibility timeout
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Task should be available again after timeout
        let dequeued_again = queue.dequeue().await.unwrap();
        assert!(dequeued_again.is_some());
        assert_eq!(dequeued_again.unwrap().id, "test-visibility-1");

        // Clean up
        let _ = queue.purge().await;
    }
}
