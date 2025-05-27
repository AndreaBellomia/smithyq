//! Configuration types for SmithyQ.
//!
//! This module contains all configuration structures used throughout SmithyQ,
//! including worker settings, queue configuration, and engine parameters.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Main configuration for SmithyQ.
///
/// This is the primary configuration structure that contains all settings
/// needed to run a SmithyQ instance.
///
/// # Examples
///
/// ```rust
/// use smithyq::config::SmithyConfig;
///
/// // Use default configuration
/// let config = SmithyConfig::default();
///
/// // Custom configuration
/// let config = SmithyConfig {
///     workers: WorkerConfig {
///         num_workers: 8,
///         task_timeout_secs: Some(600),
///         ..Default::default()
///     },
///     queue: QueueConfig {
///         batch_size: 20,
///         max_queue_size: 10000,
///         ..Default::default()
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmithyConfig {
    /// Worker-related configuration
    pub workers: WorkerConfig,

    /// Queue backend configuration
    pub queue: QueueConfig,

    /// Engine-level configuration
    pub engine: EngineConfig,

    /// Monitoring and observability configuration
    #[cfg(feature = "metrics")]
    #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
    pub metrics: MetricsConfig,

    /// Logging configuration
    pub logging: LoggingConfig,
}

impl Default for SmithyConfig {
    fn default() -> Self {
        Self {
            workers: WorkerConfig::default(),
            queue: QueueConfig::default(),
            engine: EngineConfig::default(),
            #[cfg(feature = "metrics")]
            metrics: MetricsConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

/// Worker pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Number of worker threads to spawn
    pub num_workers: usize,

    /// Maximum time a task can run before being killed (in seconds)
    pub task_timeout_secs: Option<u64>,

    /// Time to wait for workers to shutdown gracefully (in seconds)
    pub shutdown_timeout_secs: Option<u64>,

    /// Maximum number of consecutive restarts for a single worker before circuit breaking
    pub max_restart_count: u32,

    /// Time to wait between worker restarts (in milliseconds)
    pub restart_delay_ms: u64,

    /// Whether to enable worker performance tracking
    pub enable_performance_tracking: bool,

    /// Worker idle timeout - how long a worker waits before polling again when no tasks (in milliseconds)
    pub idle_timeout_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get().max(1),
            task_timeout_secs: Some(300),    // 5 minutes
            shutdown_timeout_secs: Some(30), // 30 seconds
            max_restart_count: 100,
            restart_delay_ms: 1000, // 1 second
            enable_performance_tracking: true,
            idle_timeout_ms: 5000, // 5 seconds
        }
    }
}

impl WorkerConfig {
    /// Create a new worker configuration with a specific number of workers.
    pub fn with_workers(num_workers: usize) -> Self {
        Self {
            num_workers,
            ..Default::default()
        }
    }

    /// Set the task timeout.
    pub fn with_task_timeout(mut self, timeout_secs: u64) -> Self {
        self.task_timeout_secs = Some(timeout_secs);
        self
    }

    /// Set the shutdown timeout.
    pub fn with_shutdown_timeout(mut self, timeout_secs: u64) -> Self {
        self.shutdown_timeout_secs = Some(timeout_secs);
        self
    }

    /// Enable or disable performance tracking.
    pub fn with_performance_tracking(mut self, enabled: bool) -> Self {
        self.enable_performance_tracking = enabled;
        self
    }

    /// Set the idle timeout.
    pub fn with_idle_timeout(mut self, timeout_ms: u64) -> Self {
        self.idle_timeout_ms = timeout_ms;
        self
    }
}

/// Queue backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Maximum number of tasks to fetch in a single batch
    pub batch_size: usize,

    /// Timeout for queue operations (in seconds)
    pub operation_timeout_secs: u64,

    /// Visibility timeout for tasks - how long a task is invisible after being dequeued (in seconds)
    pub visibility_timeout_secs: u64,

    /// Cleanup interval for expired tasks (in seconds)
    pub cleanup_interval_secs: u64,

    /// Maximum queue size (0 = unlimited)
    pub max_queue_size: usize,

    /// Default retry policy for failed tasks
    pub default_retry_policy: RetryPolicy,

    /// Whether to enable delayed task execution
    pub enable_delayed_tasks: bool,

    /// Whether to enable task persistence (for in-memory queue, saves to disk)
    pub enable_persistence: bool,

    /// Persistence file path (only used if enable_persistence is true)
    pub persistence_path: Option<String>,

    /// Queue-specific backend configuration
    pub backend: QueueBackendConfig,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            operation_timeout_secs: 30,
            visibility_timeout_secs: 300, // 5 minutes
            cleanup_interval_secs: 60,    // 1 minute
            max_queue_size: 0,            // unlimited
            default_retry_policy: RetryPolicy::default(),
            enable_delayed_tasks: true,
            enable_persistence: false,
            persistence_path: None,
            backend: QueueBackendConfig::InMemory,
        }
    }
}

impl QueueConfig {
    /// Create configuration for in-memory queue.
    pub fn in_memory() -> Self {
        Self {
            backend: QueueBackendConfig::InMemory,
            ..Default::default()
        }
    }

    /// Create configuration for Redis queue.
    #[cfg(feature = "redis-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
    pub fn redis(connection_string: String) -> Self {
        Self {
            backend: QueueBackendConfig::Redis(RedisConfig {
                connection_string,
                pool_size: 10,
                key_prefix: "smithyq".to_string(),
                db: 0,
            }),
            ..Default::default()
        }
    }

    /// Create configuration for PostgreSQL queue.
    #[cfg(feature = "postgres-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
    pub fn postgres(connection_string: String) -> Self {
        Self {
            backend: QueueBackendConfig::Postgres(PostgresConfig {
                connection_string,
                pool_size: 10,
                table_name: "smithyq_tasks".to_string(),
                schema: "public".to_string(),
            }),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the maximum queue size.
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_queue_size = max_size;
        self
    }

    /// Set the visibility timeout.
    pub fn with_visibility_timeout(mut self, timeout_secs: u64) -> Self {
        self.visibility_timeout_secs = timeout_secs;
        self
    }

    /// Enable persistence for in-memory queue.
    pub fn with_persistence(mut self, path: Option<String>) -> Self {
        self.enable_persistence = true;
        self.persistence_path = path;
        self
    }

    /// Set the default retry policy.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.default_retry_policy = policy;
        self
    }
}

/// Queue backend-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueBackendConfig {
    /// In-memory queue (default)
    InMemory,

    /// Redis queue configuration
    #[cfg(feature = "redis-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
    Redis(RedisConfig),

    /// PostgreSQL queue configuration
    #[cfg(feature = "postgres-queue")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
    Postgres(PostgresConfig),
}

/// Redis queue configuration.
#[cfg(feature = "redis-queue")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis-queue")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis connection string (e.g., "redis://localhost:6379")
    pub connection_string: String,

    /// Connection pool size
    pub pool_size: u32,

    /// Key prefix for Redis keys
    pub key_prefix: String,

    /// Redis database number
    pub db: i64,
}

/// PostgreSQL queue configuration.
#[cfg(feature = "postgres-queue")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres-queue")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// PostgreSQL connection string
    pub connection_string: String,

    /// Connection pool size
    pub pool_size: u32,

    /// Table name for storing tasks
    pub table_name: String,

    /// Database schema
    pub schema: String,
}

/// Retry policy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_attempts: u32,

    /// Base delay between retries (in milliseconds)
    pub base_delay_ms: u64,

    /// Maximum delay between retries (in milliseconds)
    pub max_delay_ms: u64,

    /// Backoff strategy
    pub backoff_strategy: BackoffStrategy,

    /// Whether to enable jitter in delay calculations
    pub enable_jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 1000, // 1 second
            max_delay_ms: 60000, // 1 minute
            backoff_strategy: BackoffStrategy::Exponential { multiplier: 2.0 },
            enable_jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Create a retry policy with exponential backoff.
    pub fn exponential(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            backoff_strategy: BackoffStrategy::Exponential { multiplier: 2.0 },
            ..Default::default()
        }
    }

    /// Create a retry policy with linear backoff.
    pub fn linear(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            backoff_strategy: BackoffStrategy::Linear { increment_ms: 1000 },
            ..Default::default()
        }
    }

    /// Create a retry policy with fixed delays.
    pub fn fixed(max_attempts: u32, delay_ms: u64) -> Self {
        Self {
            max_attempts,
            base_delay_ms: delay_ms,
            max_delay_ms: delay_ms,
            backoff_strategy: BackoffStrategy::Fixed,
            enable_jitter: false,
        }
    }

    /// Disable retries completely.
    pub fn none() -> Self {
        Self {
            max_attempts: 0,
            ..Default::default()
        }
    }
}

/// Backoff strategy for retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed,

    /// Linear backoff (base + attempt * increment)
    Linear {
        /// Increment per attempt (in milliseconds)
        increment_ms: u64,
    },

    /// Exponential backoff (base * multiplier^attempt)
    Exponential {
        /// Multiplier for exponential growth
        multiplier: f64,
    },
}

/// Engine-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Health check interval (in seconds)
    pub health_check_interval_secs: u64,

    /// Enable automatic cleanup of old completed tasks
    pub enable_auto_cleanup: bool,

    /// Age threshold for automatic cleanup (in seconds)
    pub cleanup_age_threshold_secs: u64,

    /// Enable graceful shutdown handling
    pub enable_graceful_shutdown: bool,

    /// Maximum time to wait for graceful shutdown (in seconds)
    pub graceful_shutdown_timeout_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            health_check_interval_secs: 30,
            enable_auto_cleanup: true,
            cleanup_age_threshold_secs: 3600, // 1 hour
            enable_graceful_shutdown: true,
            graceful_shutdown_timeout_secs: 60, // 1 minute
        }
    }
}

/// Metrics and monitoring configuration.
#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable Prometheus metrics export
    pub enable_prometheus: bool,

    /// Prometheus metrics endpoint port
    pub prometheus_port: u16,

    /// Prometheus metrics endpoint path
    pub prometheus_path: String,

    /// Enable detailed worker metrics
    pub enable_worker_metrics: bool,

    /// Enable queue depth metrics
    pub enable_queue_metrics: bool,

    /// Enable task timing metrics
    pub enable_timing_metrics: bool,

    /// Metrics collection interval (in seconds)
    pub collection_interval_secs: u64,
}

#[cfg(feature = "metrics")]
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enable_prometheus: true,
            prometheus_port: 9090,
            prometheus_path: "/metrics".to_string(),
            enable_worker_metrics: true,
            enable_queue_metrics: true,
            enable_timing_metrics: true,
            collection_interval_secs: 10,
        }
    }
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level filter
    pub level: LogLevel,

    /// Enable structured JSON logging
    pub json_format: bool,

    /// Enable colored output (ignored if json_format is true)
    pub colored: bool,

    /// Include timestamps in logs
    pub include_timestamps: bool,

    /// Include target module in logs
    pub include_targets: bool,

    /// Log file path (None = stdout only)
    pub file_path: Option<String>,

    /// Maximum log file size before rotation (in bytes)
    pub max_file_size: Option<u64>,

    /// Number of rotated log files to keep
    pub max_files: Option<usize>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            json_format: false,
            colored: true,
            include_timestamps: true,
            include_targets: false,
            file_path: None,
            max_file_size: Some(100 * 1024 * 1024), // 100MB
            max_files: Some(5),
        }
    }
}

/// Log level enumeration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    /// Trace level
    Trace,
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warn level
    Warn,
    /// Error level
    Error,
}

impl From<LogLevel> for tracing::Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => tracing::Level::TRACE,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
        }
    }
}

/// Helper trait for converting durations in configuration.
pub trait DurationExt {
    /// Convert seconds to Duration
    fn secs(self) -> Duration;
    /// Convert milliseconds to Duration
    fn millis(self) -> Duration;
}

impl DurationExt for u64 {
    fn secs(self) -> Duration {
        Duration::from_secs(self)
    }

    fn millis(self) -> Duration {
        Duration::from_millis(self)
    }
}

impl SmithyConfig {
    /// Create a new configuration optimized for development.
    pub fn development() -> Self {
        Self {
            workers: WorkerConfig {
                num_workers: 2,
                task_timeout_secs: Some(60),
                enable_performance_tracking: true,
                ..Default::default()
            },
            queue: QueueConfig {
                batch_size: 5,
                max_queue_size: 1000,
                visibility_timeout_secs: 60,
                enable_persistence: false,
                ..Default::default()
            },
            engine: EngineConfig {
                health_check_interval_secs: 10,
                enable_auto_cleanup: true,
                cleanup_age_threshold_secs: 300, // 5 minutes
                ..Default::default()
            },
            #[cfg(feature = "metrics")]
            metrics: MetricsConfig {
                enable_prometheus: false,
                collection_interval_secs: 30,
                ..Default::default()
            },
            logging: LoggingConfig {
                level: LogLevel::Debug,
                colored: true,
                include_targets: true,
                ..Default::default()
            },
        }
    }

    /// Create a new configuration optimized for production.
    pub fn production() -> Self {
        Self {
            workers: WorkerConfig {
                num_workers: num_cpus::get() * 2,
                task_timeout_secs: Some(300),
                shutdown_timeout_secs: Some(60),
                enable_performance_tracking: true,
                ..Default::default()
            },
            queue: QueueConfig {
                batch_size: 20,
                max_queue_size: 0,            // unlimited
                visibility_timeout_secs: 600, // 10 minutes
                enable_persistence: true,
                default_retry_policy: RetryPolicy::exponential(5),
                ..Default::default()
            },
            engine: EngineConfig {
                health_check_interval_secs: 60,
                enable_auto_cleanup: true,
                cleanup_age_threshold_secs: 86400, // 24 hours
                graceful_shutdown_timeout_secs: 120,
                ..Default::default()
            },
            #[cfg(feature = "metrics")]
            metrics: MetricsConfig {
                enable_prometheus: true,
                enable_worker_metrics: true,
                enable_queue_metrics: true,
                enable_timing_metrics: true,
                collection_interval_secs: 15,
                ..Default::default()
            },
            logging: LoggingConfig {
                level: LogLevel::Info,
                json_format: true,
                colored: false,
                include_timestamps: true,
                include_targets: false,
                file_path: Some("/var/log/smithyq/smithyq.log".to_string()),
                ..Default::default()
            },
        }
    }

    /// Create a configuration for testing.
    pub fn testing() -> Self {
        Self {
            workers: WorkerConfig {
                num_workers: 1,
                task_timeout_secs: Some(10),
                shutdown_timeout_secs: Some(5),
                enable_performance_tracking: false,
                idle_timeout_ms: 100,
                ..Default::default()
            },
            queue: QueueConfig {
                batch_size: 1,
                max_queue_size: 100,
                visibility_timeout_secs: 10,
                cleanup_interval_secs: 1,
                enable_persistence: false,
                default_retry_policy: RetryPolicy::fixed(1, 100),
                ..Default::default()
            },
            engine: EngineConfig {
                health_check_interval_secs: 1,
                enable_auto_cleanup: false,
                graceful_shutdown_timeout_secs: 5,
                ..Default::default()
            },
            #[cfg(feature = "metrics")]
            metrics: MetricsConfig {
                enable_prometheus: false,
                collection_interval_secs: 1,
                ..Default::default()
            },
            logging: LoggingConfig {
                level: LogLevel::Debug,
                colored: false,
                include_timestamps: false,
                include_targets: true,
                ..Default::default()
            },
        }
    }

    /// Validate the configuration and return any errors.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Validate worker config
        if self.workers.num_workers == 0 {
            errors.push("Number of workers must be greater than 0".to_string());
        }

        if self.workers.num_workers > 1000 {
            errors.push("Number of workers should not exceed 1000".to_string());
        }

        // Validate queue config
        if self.queue.batch_size == 0 {
            errors.push("Queue batch size must be greater than 0".to_string());
        }

        if self.queue.visibility_timeout_secs == 0 {
            errors.push("Visibility timeout must be greater than 0".to_string());
        }

        // Validate retry policy
        if self.queue.default_retry_policy.base_delay_ms == 0 {
            errors.push("Retry base delay must be greater than 0".to_string());
        }

        if self.queue.default_retry_policy.max_delay_ms
            < self.queue.default_retry_policy.base_delay_ms
        {
            errors.push("Retry max delay must be greater than or equal to base delay".to_string());
        }

        // Validate engine config
        if self.engine.health_check_interval_secs == 0 {
            errors.push("Health check interval must be greater than 0".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SmithyConfig::default();
        assert!(config.workers.num_workers > 0);
        assert_eq!(config.queue.batch_size, 10);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_development_config() {
        let config = SmithyConfig::development();
        assert_eq!(config.workers.num_workers, 2);
        assert_eq!(config.queue.batch_size, 5);
        assert!(matches!(config.logging.level, LogLevel::Debug));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_production_config() {
        let config = SmithyConfig::production();
        assert!(config.workers.num_workers >= 2);
        assert_eq!(config.queue.batch_size, 20);
        assert!(matches!(config.logging.level, LogLevel::Info));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_testing_config() {
        let config = SmithyConfig::testing();
        assert_eq!(config.workers.num_workers, 1);
        assert_eq!(config.queue.batch_size, 1);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        let mut config = SmithyConfig::default();

        // Valid config
        assert!(config.validate().is_ok());

        // Invalid worker count
        config.workers.num_workers = 0;
        assert!(config.validate().is_err());

        config.workers.num_workers = 1;

        // Invalid batch size
        config.queue.batch_size = 0;
        let errors = config.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("batch size")));
    }

    #[test]
    fn test_retry_policies() {
        let exponential = RetryPolicy::exponential(5);
        assert_eq!(exponential.max_attempts, 5);
        assert!(matches!(
            exponential.backoff_strategy,
            BackoffStrategy::Exponential { .. }
        ));

        let linear = RetryPolicy::linear(3);
        assert_eq!(linear.max_attempts, 3);
        assert!(matches!(
            linear.backoff_strategy,
            BackoffStrategy::Linear { .. }
        ));

        let fixed = RetryPolicy::fixed(2, 1000);
        assert_eq!(fixed.max_attempts, 2);
        assert_eq!(fixed.base_delay_ms, 1000);
        assert!(matches!(fixed.backoff_strategy, BackoffStrategy::Fixed));

        let none = RetryPolicy::none();
        assert_eq!(none.max_attempts, 0);
    }

    #[test]
    fn test_queue_config_builders() {
        let in_memory = QueueConfig::in_memory();
        assert!(matches!(in_memory.backend, QueueBackendConfig::InMemory));

        #[cfg(feature = "redis-queue")]
        {
            let redis = QueueConfig::redis("redis://localhost:6379".to_string());
            assert!(matches!(redis.backend, QueueBackendConfig::Redis(_)));
        }

        #[cfg(feature = "postgres-queue")]
        {
            let postgres = QueueConfig::postgres("postgresql://localhost/test".to_string());
            assert!(matches!(postgres.backend, QueueBackendConfig::Postgres(_)));
        }
    }

    #[test]
    fn test_worker_config_builders() {
        let config = WorkerConfig::with_workers(8)
            .with_task_timeout(600)
            .with_performance_tracking(true)
            .with_idle_timeout(1000);

        assert_eq!(config.num_workers, 8);
        assert_eq!(config.task_timeout_secs, Some(600));
        assert!(config.enable_performance_tracking);
        assert_eq!(config.idle_timeout_ms, 1000);
    }

    #[test]
    fn test_duration_ext() {
        assert_eq!(5u64.secs(), Duration::from_secs(5));
        assert_eq!(1500u64.millis(), Duration::from_millis(1500));
    }
}
