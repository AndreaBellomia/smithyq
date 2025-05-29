//! Task registry system for SmithyQ.
//!
//! This module provides the core task registration and execution system that allows
//! automatic registration of task types and their execution through a unified interface.
//!
//! # Overview
//!
//! The task registry system consists of:
//! - `SmithyTask` trait: Defines how tasks are executed
//! - `TaskExecutor` trait: Handles task execution from serialized data
//! - `TaskCaller` trait: Creates tasks for enqueueing
//! - `TaskRegistry`: Global registry for all task types
//! - `forge_task!` macro: Automatically generates registration code
//!
//! # Examples
//!
//! ```rust
//! use smithyq::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! // Define your task
//! #[derive(Debug, Serialize, Deserialize, Default)]
//! struct EmailTask {
//!     to: String,
//!     subject: String,
//!     body: String,
//! }
//!
//! // Implement the task logic
//! #[async_trait::async_trait]
//! impl SmithyTask for EmailTask {
//!     type Output = String;
//!     
//!     async fn forge(self) -> SmithyResult<Self::Output> {
//!         // Your email sending logic
//!         send_email(&self.to, &self.subject, &self.body).await?;
//!         Ok(format!("Email sent to {}", self.to))
//!     }
//! }
//!
//! // Register the task automatically - no task_type parameter needed!
//! forge_task!(EmailTask);
//!
//! # async fn send_email(to: &str, subject: &str, body: &str) -> SmithyResult<()> { Ok(()) }
//! ```

use crate::error::{SmithyError, SmithyResult};
use crate::task::QueuedTask;
use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::SystemTime;
use tokio::sync::RwLock;

/// Trait for executing tasks from serialized payloads.
///
/// This trait is implemented by the generated executor structs
/// and handles the deserialization and execution of tasks.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Execute a task with the given serialized payload.
    ///
    /// The payload should be deserializable into the task type
    /// that this executor handles.
    async fn execute(&self, payload: serde_json::Value) -> SmithyResult<serde_json::Value>;

    /// Get the task type identifier that this executor handles.
    fn task_type(&self) -> &'static str;

    /// Get metadata about this task type (optional).
    fn metadata(&self) -> TaskMetadata {
        TaskMetadata::default()
    }
}

/// Trait for creating tasks to be enqueued.
///
/// This trait is implemented by the generated caller structs
/// and handles the creation of `QueuedTask` instances.
pub trait TaskCaller: Send + Sync {
    /// Create a queued task from a serialized payload.
    fn create_task(&self, payload: serde_json::Value) -> QueuedTask;

    /// Get the task type identifier that this caller creates.
    fn task_type(&self) -> &'static str;

    /// Get the default configuration for tasks of this type.
    fn default_config(&self) -> TaskConfig {
        TaskConfig::default()
    }
}

/// Metadata about a task type.
#[derive(Debug, Clone)]
pub struct TaskMetadata {
    /// Human-readable description of what this task does
    pub description: Option<String>,
    /// Estimated average execution time
    pub estimated_duration: Option<std::time::Duration>,
    /// Whether this task type is CPU intensive
    pub cpu_intensive: bool,
    /// Whether this task type performs I/O operations
    pub io_intensive: bool,
    /// Priority level for this task type
    pub priority: TaskPriority,
    /// Tags for categorizing this task type
    pub tags: Vec<String>,
}

impl Default for TaskMetadata {
    fn default() -> Self {
        Self {
            description: None,
            estimated_duration: None,
            cpu_intensive: false,
            io_intensive: true, // Most tasks do I/O
            priority: TaskPriority::Normal,
            tags: Vec::new(),
        }
    }
}

/// Priority levels for tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    /// Low priority tasks (background processing)
    Low = 0,
    /// Normal priority tasks (default)
    Normal = 1,
    /// High priority tasks (user-facing operations)
    High = 2,
    /// Critical priority tasks (system operations)
    Critical = 3,
}

impl Default for TaskPriority {
    fn default() -> Self {
        TaskPriority::Normal
    }
}

/// Configuration for individual task types.
#[derive(Debug, Clone)]
pub struct TaskConfig {
    /// Maximum number of retries for this task type
    pub max_retries: u32,
    /// Custom timeout for this task type
    pub timeout: Option<std::time::Duration>,
    /// Priority for this task type
    pub priority: TaskPriority,
    /// Whether this task can be executed concurrently with itself
    pub concurrent: bool,
    /// Rate limit: maximum executions per second (0 = unlimited)
    pub rate_limit: u32,
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            timeout: None, // Use global default
            priority: TaskPriority::Normal,
            concurrent: true,
            rate_limit: 0, // Unlimited
        }
    }
}

/// Statistics for a registered task type.
#[derive(Debug, Clone, Default)]
pub struct TaskTypeStats {
    /// Total number of tasks executed
    pub total_executed: u64,
    /// Number of successful executions
    pub successful: u64,
    /// Number of failed executions
    pub failed: u64,
    /// Number of timed out executions
    pub timed_out: u64,
    /// Average execution time
    pub avg_duration: Option<std::time::Duration>,
    /// Last execution time
    pub last_executed: Option<SystemTime>,
}

/// Global registry for all task types.
///
/// The registry maintains mappings between task type identifiers and their
/// corresponding executors and callers. It also tracks statistics and metadata
/// for each registered task type.
pub struct TaskRegistry {
    /// Registered task executors by type
    executors: RwLock<HashMap<String, Box<dyn TaskExecutor>>>,
    /// Registered task callers by type
    callers: RwLock<HashMap<String, Box<dyn TaskCaller>>>,
    /// Task type metadata
    metadata: RwLock<HashMap<String, TaskMetadata>>,
    /// Task type statistics
    stats: RwLock<HashMap<String, TaskTypeStats>>,
    /// Task type configurations
    configs: RwLock<HashMap<String, TaskConfig>>,
}

impl TaskRegistry {
    /// Create a new empty task registry.
    fn new() -> Self {
        tracing::debug!("Creating new task registry");
        Self {
            executors: RwLock::new(HashMap::new()),
            callers: RwLock::new(HashMap::new()),
            metadata: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
        }
    }

    /// Register a task executor.
    ///
    /// This method is typically called by the auto-generated registration functions
    /// created by the `forge_task!` macro.
    pub async fn register_executor(&self, executor: Box<dyn TaskExecutor>) {
        let task_type = executor.task_type().to_string();
        let metadata = executor.metadata();

        tracing::info!("Registering task executor: {}", task_type);

        // Store executor
        self.executors
            .write()
            .await
            .insert(task_type.clone(), executor);

        // Store metadata
        self.metadata
            .write()
            .await
            .insert(task_type.clone(), metadata);

        // Initialize stats
        self.stats
            .write()
            .await
            .entry(task_type)
            .or_insert_with(TaskTypeStats::default);
    }

    /// Register a task caller.
    pub async fn register_caller(&self, caller: Box<dyn TaskCaller>) {
        let task_type = caller.task_type().to_string();
        let config = caller.default_config();

        tracing::debug!("Registering task caller: {}", task_type);

        // Store caller
        self.callers.write().await.insert(task_type.clone(), caller);

        // Store configuration
        self.configs.write().await.insert(task_type, config);
    }

    /// Execute a task using its registered executor.
    ///
    /// This method looks up the executor for the task's type and delegates
    /// execution to it. It also updates execution statistics.
    pub async fn execute_task(&self, task: &QueuedTask) -> SmithyResult<serde_json::Value> {
        let start_time = std::time::Instant::now();

        // Get executor
        let executor = {
            let executors = self.executors.read().await;
            match executors.get(&task.task_type) {
                Some(_) => {} // Executor exists, continue
                None => {
                    let error = format!("No executor found for task type: {}", task.task_type);
                    tracing::error!("{}", error);
                    return Err(SmithyError::TaskNotFound {
                        task_type: task.task_type.clone(),
                    });
                }
            }
        };

        tracing::debug!("Executing task {} of type {}", task.id, task.task_type);

        // Execute task
        let result = {
            let executors = self.executors.read().await;
            let executor = executors.get(&task.task_type).unwrap(); // Safe because we checked above
            executor.execute(task.payload.clone()).await
        };

        // Update statistics
        let duration = start_time.elapsed();
        self.update_stats(&task.task_type, &result, duration).await;

        match &result {
            Ok(_) => {
                tracing::debug!("Task {} completed successfully in {:?}", task.id, duration);
            }
            Err(e) => {
                tracing::error!("Task {} failed after {:?}: {}", task.id, duration, e);
            }
        }

        result
    }

    /// Create a task using its registered caller.
    pub async fn create_task(
        &self,
        task_type: &str,
        payload: serde_json::Value,
    ) -> Option<QueuedTask> {
        let callers = self.callers.read().await;
        callers
            .get(task_type)
            .map(|caller| caller.create_task(payload))
    }

    /// Get all registered task types.
    pub async fn get_registered_types(&self) -> Vec<String> {
        let executors = self.executors.read().await;
        let mut types: Vec<String> = executors.keys().cloned().collect();
        types.sort();
        types
    }

    /// Get the number of registered executors.
    pub async fn executor_count(&self) -> usize {
        let executors = self.executors.read().await;
        executors.len()
    }

    /// Get the number of registered callers.
    pub async fn caller_count(&self) -> usize {
        let callers = self.callers.read().await;
        callers.len()
    }

    /// Check if a task type is registered.
    pub async fn is_registered(&self, task_type: &str) -> bool {
        let executors = self.executors.read().await;
        executors.contains_key(task_type)
    }

    /// Get metadata for a task type.
    pub async fn get_metadata(&self, task_type: &str) -> Option<TaskMetadata> {
        let metadata = self.metadata.read().await;
        metadata.get(task_type).cloned()
    }

    /// Get configuration for a task type.
    pub async fn get_config(&self, task_type: &str) -> Option<TaskConfig> {
        let configs = self.configs.read().await;
        configs.get(task_type).cloned()
    }

    /// Get statistics for a task type.
    pub async fn get_stats(&self, task_type: &str) -> Option<TaskTypeStats> {
        let stats = self.stats.read().await;
        stats.get(task_type).cloned()
    }

    /// Get statistics for all registered task types.
    pub async fn get_all_stats(&self) -> HashMap<String, TaskTypeStats> {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Update statistics for a task execution.
    async fn update_stats(
        &self,
        task_type: &str,
        result: &SmithyResult<serde_json::Value>,
        duration: std::time::Duration,
    ) {
        let mut stats = self.stats.write().await;
        let task_stats = stats
            .entry(task_type.to_string())
            .or_insert_with(TaskTypeStats::default);

        task_stats.total_executed += 1;
        task_stats.last_executed = Some(SystemTime::now());

        match result {
            Ok(_) => task_stats.successful += 1,
            Err(SmithyError::Timeout { .. }) => task_stats.timed_out += 1,
            Err(_) => task_stats.failed += 1,
        }

        // Update average duration (exponential moving average)
        if let Some(avg) = task_stats.avg_duration {
            let alpha = 0.1; // Weight for new sample
            let new_avg_nanos =
                (alpha * duration.as_nanos() as f64) + ((1.0 - alpha) * avg.as_nanos() as f64);
            task_stats.avg_duration = Some(std::time::Duration::from_nanos(new_avg_nanos as u64));
        } else {
            task_stats.avg_duration = Some(duration);
        }
    }

    /// Reset statistics for all task types.
    ///
    /// This is mainly useful for testing or when you want to start fresh.
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        for task_stats in stats.values_mut() {
            *task_stats = TaskTypeStats::default();
        }
        tracing::info!("Task registry statistics reset");
    }

    /// Get a summary of the registry state.
    pub async fn summary(&self) -> RegistrySummary {
        let executor_count = self.executor_count().await;
        let caller_count = self.caller_count().await;
        let registered_types = self.get_registered_types().await;
        let all_stats = self.get_all_stats().await;

        let total_executed: u64 = all_stats.values().map(|s| s.total_executed).sum();
        let total_successful: u64 = all_stats.values().map(|s| s.successful).sum();
        let total_failed: u64 = all_stats.values().map(|s| s.failed).sum();

        RegistrySummary {
            executor_count,
            caller_count,
            registered_types,
            total_executed,
            total_successful,
            total_failed,
        }
    }

    /// Clear all registered tasks.
    ///
    /// This is mainly useful for testing.
    #[cfg(test)]
    pub async fn clear(&self) {
        let mut executors = self.executors.write().await;
        let mut callers = self.callers.write().await;
        let mut metadata = self.metadata.write().await;
        let mut stats = self.stats.write().await;
        let mut configs = self.configs.write().await;

        executors.clear();
        callers.clear();
        metadata.clear();
        stats.clear();
        configs.clear();

        tracing::debug!("Task registry cleared");
    }
}

/// Summary of the registry state.
#[derive(Debug, Clone)]
pub struct RegistrySummary {
    /// Number of registered executors
    pub executor_count: usize,
    /// Number of registered callers
    pub caller_count: usize,
    /// List of all registered task types
    pub registered_types: Vec<String>,
    /// Total number of tasks executed
    pub total_executed: u64,
    /// Total number of successful executions
    pub total_successful: u64,
    /// Total number of failed executions
    pub total_failed: u64,
}

/// Global singleton registry instance.
static TASK_REGISTRY: OnceLock<TaskRegistry> = OnceLock::new();

/// Get the global task registry instance.
///
/// This function returns a reference to the global task registry singleton.
/// The registry is initialized on first access.
pub fn get_registry() -> &'static TaskRegistry {
    TASK_REGISTRY.get_or_init(|| {
        tracing::debug!("Initializing global task registry");
        TaskRegistry::new()
    })
}

// Auto-registration system
type AutoRegisterFn = fn() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

static AUTO_REGISTER_FNS: OnceLock<std::sync::Mutex<Vec<AutoRegisterFn>>> = OnceLock::new();

/// Add a function to be called during auto-registration
pub fn add_auto_register_fn(f: AutoRegisterFn) {
    let fns = AUTO_REGISTER_FNS.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    fns.lock().unwrap().push(f);
}

/// Execute all auto-registration functions
pub async fn execute_auto_registrations() {
    let fns = AUTO_REGISTER_FNS.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    let fns = fns.lock().unwrap().clone();

    for f in fns {
        f().await;
    }
}

/// Extended macro for registering tasks with custom configuration.
#[macro_export]
macro_rules! forge_task_with_config {
    ($task_struct:ident, {
        $(description = $description:literal,)?
        $(estimated_duration = $duration:expr,)?
        $(cpu_intensive = $cpu_intensive:expr,)?
        $(io_intensive = $io_intensive:expr,)?
        $(priority = $priority:expr,)?
        $(tags = [$($tag:literal),*],)?
        $(max_retries = $max_retries:expr,)?
        $(timeout = $timeout:expr,)?
        $(concurrent = $concurrent:expr,)?
        $(rate_limit = $rate_limit:expr,)?
    }) => {
        paste::paste! {
            /// Generated executor for the registered task type
            #[derive(Debug)]
            pub struct [<$task_struct Executor>];

            /// Generated caller for creating instances of the registered task type
            #[derive(Debug)]
            pub struct [<$task_struct Caller>];

            #[async_trait::async_trait]
            impl $crate::core::registry::TaskExecutor for [<$task_struct Executor>] {
                async fn execute(&self, payload: serde_json::Value) -> $crate::error::SmithyResult<serde_json::Value> {
                    let task_data: $task_struct = serde_json::from_value(payload)
                        .map_err(|e| $crate::error::SmithyError::SerializationError(e))?;
                    let result = task_data.forge().await?;
                    serde_json::to_value(result)
                        .map_err(|e| $crate::error::SmithyError::SerializationError(e))
                }

                fn task_type(&self) -> &'static str {
                    // Use type_name as default implementation
                    std::any::type_name::<$task_struct>()
                }

                fn metadata(&self) -> $crate::core::registry::TaskMetadata {
                    $crate::core::registry::TaskMetadata {
                        description: $crate::forge_task_with_config!(@description $($description)?),
                        estimated_duration: $crate::forge_task_with_config!(@duration $($duration)?),
                        cpu_intensive: $crate::forge_task_with_config!(@cpu_intensive $($cpu_intensive)?),
                        io_intensive: $crate::forge_task_with_config!(@io_intensive $($io_intensive)?),
                        priority: $crate::forge_task_with_config!(@priority $($priority)?),
                        tags: vec![$($(String::from($tag)),*)?],
                    }
                }
            }

            impl $crate::core::registry::TaskCaller for [<$task_struct Caller>] {
                fn create_task(&self, payload: serde_json::Value) -> $crate::task::QueuedTask {
                    use std::time::SystemTime;

                    $crate::task::QueuedTask {
                        id: uuid::Uuid::new_v4().to_string(),
                        task_type: std::any::type_name::<$task_struct>().to_string(),
                        payload,
                        status: $crate::task::TaskStatus::Pending,
                        retry_count: 0,
                        max_retries: $crate::forge_task_with_config!(@max_retries $($max_retries)?),
                        created_at: SystemTime::now(),
                        updated_at: SystemTime::now(),
                        execute_at: None,
                    }
                }

                fn task_type(&self) -> &'static str {
                    // Use type_name as default implementation
                    std::any::type_name::<$task_struct>()
                }

                fn default_config(&self) -> $crate::core::registry::TaskConfig {
                    $crate::core::registry::TaskConfig {
                        max_retries: $crate::forge_task_with_config!(@max_retries $($max_retries)?),
                        timeout: $crate::forge_task_with_config!(@timeout $($timeout)?),
                        priority: $crate::forge_task_with_config!(@priority $($priority)?),
                        concurrent: $crate::forge_task_with_config!(@concurrent $($concurrent)?),
                        rate_limit: $crate::forge_task_with_config!(@rate_limit $($rate_limit)?),
                    }
                }
            }

            /// Helper for creating and enqueuing tasks of this type
            impl [<$task_struct Caller>] {
                /// Create a new caller instance
                pub fn new() -> Self {
                    Self
                }

                /// Create a task from the given data
                #[allow(dead_code)]
                pub(crate) async fn call(&self, task_data: $task_struct) -> $crate::error::SmithyResult<$crate::task::QueuedTask> {
                    let payload = serde_json::to_value(&task_data)
                        .map_err(|e| $crate::error::SmithyError::SerializationError(e))?;
                    let task = self.create_task(payload);
                    Ok(task)
                }
            }

            impl Default for [<$task_struct Caller>] {
                fn default() -> Self {
                    Self::new()
                }
            }

            /// Auto-registration function
            pub async fn [<auto_register_ $task_struct:snake>]() {
                let registry = $crate::core::registry::get_registry();
                registry.register_executor(Box::new([<$task_struct Executor>])).await;
                registry.register_caller(Box::new([<$task_struct Caller>])).await;

                let task_type = std::any::type_name::<$task_struct>();
                tracing::info!("Auto-registered task type: {} ({})", task_type, stringify!($task_struct));
            }

            /// Manual registration function (for backward compatibility)
            #[deprecated(note = "Use auto-registration instead")]
            pub async fn [<register_ $task_struct:snake _task>]() {
                [<auto_register_ $task_struct:snake>]().await;
            }

            /// Function to be called during initialization
            #[ctor::ctor]
            fn [<mark_ $task_struct:snake _for_registration>]() {
                $crate::core::registry::add_auto_register_fn(|| {
                    Box::pin([<auto_register_ $task_struct:snake>]())
                });
            }
        }
    };

    // Helper rules for extracting optional configuration values
    (@description) => { None };
    (@description $description:literal) => { Some($description.to_string()) };

    (@duration) => { None };
    (@duration $duration:expr) => { Some($duration) };

    (@cpu_intensive) => { false };
    (@cpu_intensive $cpu_intensive:expr) => { $cpu_intensive };

    (@io_intensive) => { true };
    (@io_intensive $io_intensive:expr) => { $io_intensive };

    (@priority) => { $crate::core::registry::TaskPriority::Normal };
    (@priority $priority:expr) => { $priority };

    (@max_retries) => { 3 };
    (@max_retries $max_retries:expr) => { $max_retries };

    (@timeout) => { None };
    (@timeout $timeout:expr) => { Some($timeout) };

    (@concurrent) => { true };
    (@concurrent $concurrent:expr) => { $concurrent };

    (@rate_limit) => { 0 };
    (@rate_limit $rate_limit:expr) => { $rate_limit };
}

/// Macro to automatically register a task type with SmithyQ.
///
/// This macro generates the necessary boilerplate code to register a task type
/// with the global task registry. It creates both an executor and a caller
/// for the task type. The task_type is automatically derived from the SmithyTask trait.
///
/// # Arguments
///
/// * `$task_struct` - The struct that implements `SmithyTask`
///
/// # Examples
///
/// ```rust, ignore
/// use smithyq::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, Default)]
/// struct MyTask {
///     data: String,
/// }
///
/// #[async_trait::async_trait]
/// impl SmithyTask for MyTask {
///     type Output = String;
///     
///     async fn forge(self) -> SmithyResult<Self::Output> {
///         Ok(format!("Processed: {}", self.data))
///     }
/// }
///
/// // Register the task - task_type is automatic!
/// forge_task!(MyTask);
///
/// // This generates:
/// // - MyTaskExecutor struct
/// // - MyTaskCaller struct  
/// // - auto_register_my_task() function
/// // - Automatic registration on startup
/// ```
#[macro_export]
macro_rules! forge_task {
    ($task_struct:ident) => {
        $crate::forge_task_with_config!($task_struct, {});
    };
    ($task_struct:ident, { $($config:tt)* }) => {
        $crate::forge_task_with_config!($task_struct, { $($config)* });
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TaskStatus, task::SmithyTask};
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    #[derive(Debug, Serialize, Deserialize, Default)]
    struct TestTask {
        data: String,
    }

    #[async_trait]
    impl SmithyTask for TestTask {
        type Output = String;

        async fn forge(self) -> SmithyResult<Self::Output> {
            Ok(format!("Processed: {}", self.data))
        }

        // Override per avere un task_type personalizzato nei test
        fn task_type(&self) -> &'static str {
            "test_task"
        }
    }

    // Test the basic forge_task! macro
    forge_task!(TestTask);

    #[derive(Debug, Serialize, Deserialize, Default)]
    struct AdvancedTask {
        value: i32,
    }

    #[async_trait]
    impl SmithyTask for AdvancedTask {
        type Output = i32;

        async fn forge(self) -> SmithyResult<Self::Output> {
            Ok(self.value * 2)
        }

        // Override per avere un task_type personalizzato nei test
        fn task_type(&self) -> &'static str {
            "advanced_task"
        }
    }

    // Test the advanced forge_task! macro with configuration
    forge_task!(AdvancedTask, {
        description = "A task that doubles a value",
        estimated_duration = Duration::from_millis(100),
        cpu_intensive = true,
        io_intensive = false,
        priority = TaskPriority::High,
        tags = ["math", "calculation"],
        max_retries = 5,
        timeout = Duration::from_secs(30),
        concurrent = false,
        rate_limit = 10,
    });

    #[derive(Debug, Serialize, Deserialize, Default)]
    struct AutoTypeTask {
        name: String,
    }

    #[cfg(test)]
    mod comprehensive_tests {
        use super::*;

        // Test tasks that use the default type_name behavior
        #[derive(Debug, Serialize, Deserialize, Default)]
        struct DefaultTypeTask {
            message: String,
        }

        #[async_trait]
        impl SmithyTask for DefaultTypeTask {
            type Output = String;

            async fn forge(self) -> SmithyResult<Self::Output> {
                Ok(format!("Default: {}", self.message))
            }
            // No task_type override - uses std::any::type_name
        }

        forge_task!(DefaultTypeTask);

        #[derive(Debug, Serialize, Deserialize, Default)]
        struct CustomTypeTask {
            value: i32,
        }

        #[async_trait]
        impl SmithyTask for CustomTypeTask {
            type Output = i32;

            async fn forge(self) -> SmithyResult<Self::Output> {
                Ok(self.value + 10)
            }

            fn task_type(&self) -> &'static str {
                "custom_task"
            }
        }

        forge_task!(CustomTypeTask);

        #[derive(Debug, Serialize, Deserialize, Default)]
        struct ConfiguredTypeTask {
            data: String,
        }

        #[async_trait]
        impl SmithyTask for ConfiguredTypeTask {
            type Output = String;

            async fn forge(self) -> SmithyResult<Self::Output> {
                Ok(format!("Configured: {}", self.data))
            }
        }

        forge_task!(ConfiguredTypeTask, {
            description = "A configured task",
            cpu_intensive = true,
            priority = TaskPriority::High,
            max_retries = 2,
            timeout = Duration::from_secs(10),
        });

        #[tokio::test]
        async fn test_default_type_registration() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;
            registry
                .register_caller(Box::new(DefaultTypeTaskCaller))
                .await;

            let expected_type = std::any::type_name::<DefaultTypeTask>();
            assert!(registry.is_registered(expected_type).await);
            assert_eq!(registry.executor_count().await, 1);
            assert_eq!(registry.caller_count().await, 1);
        }

        #[tokio::test]
        async fn test_default_type_execution() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;

            let task_type = std::any::type_name::<DefaultTypeTask>();
            let task = QueuedTask {
                id: "default-123".to_string(),
                task_type: task_type.to_string(),
                payload: serde_json::json!({"message": "hello"}),
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            let result = registry.execute_task(&task).await.unwrap();
            assert_eq!(result, "Default: hello");

            let stats = registry.get_stats(task_type).await.unwrap();
            assert_eq!(stats.total_executed, 1);
            assert_eq!(stats.successful, 1);
        }

        #[tokio::test]
        async fn test_custom_type_consistency() {
            // The macro generates executors/callers that use type_name,
            // but the SmithyTask implementation might override task_type
            let executor = CustomTypeTaskExecutor;
            let caller = CustomTypeTaskCaller;

            let expected_type = std::any::type_name::<CustomTypeTask>();
            assert_eq!(executor.task_type(), expected_type);
            assert_eq!(caller.task_type(), expected_type);

            // The SmithyTask implementation has a different task_type
            let task_instance = CustomTypeTask::default();
            assert_eq!(task_instance.task_type(), "custom_task");

            // But the generated code uses type_name, not the instance method
            assert_ne!(executor.task_type(), task_instance.task_type());
        }

        #[tokio::test]
        async fn test_task_caller_creation() {
            let caller = DefaultTypeTaskCaller::new();
            let task_data = DefaultTypeTask {
                message: "test message".to_string(),
            };

            let queued_task = caller.call(task_data).await.unwrap();
            let expected_type = std::any::type_name::<DefaultTypeTask>();

            assert_eq!(queued_task.task_type, expected_type);
            assert_eq!(queued_task.status, TaskStatus::Pending);
            assert_eq!(queued_task.retry_count, 0);
        }

        #[tokio::test]
        async fn test_configured_task_metadata() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(ConfiguredTypeTaskExecutor))
                .await;
            registry
                .register_caller(Box::new(ConfiguredTypeTaskCaller))
                .await;

            let task_type = std::any::type_name::<ConfiguredTypeTask>();
            let metadata = registry.get_metadata(task_type).await.unwrap();

            assert_eq!(metadata.description, Some("A configured task".to_string()));
            assert!(metadata.cpu_intensive);
            assert_eq!(metadata.priority, TaskPriority::High);

            let config = registry.get_config(task_type).await.unwrap();
            assert_eq!(config.max_retries, 2);
            assert_eq!(config.timeout, Some(Duration::from_secs(10)));
            assert_eq!(config.priority, TaskPriority::High);
        }

        #[tokio::test]
        async fn test_multiple_task_types() {
            let registry = TaskRegistry::new();

            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;
            registry
                .register_caller(Box::new(DefaultTypeTaskCaller))
                .await;
            registry
                .register_executor(Box::new(ConfiguredTypeTaskExecutor))
                .await;
            registry
                .register_caller(Box::new(ConfiguredTypeTaskCaller))
                .await;

            assert_eq!(registry.executor_count().await, 2);
            assert_eq!(registry.caller_count().await, 2);

            let types = registry.get_registered_types().await;
            assert_eq!(types.len(), 2);
            assert!(types.contains(&std::any::type_name::<DefaultTypeTask>().to_string()));
            assert!(types.contains(&std::any::type_name::<ConfiguredTypeTask>().to_string()));
        }

        #[tokio::test]
        async fn test_registry_summary_with_type_names() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;
            registry
                .register_caller(Box::new(DefaultTypeTaskCaller))
                .await;

            let summary = registry.summary().await;
            assert_eq!(summary.executor_count, 1);
            assert_eq!(summary.caller_count, 1);

            let expected_type = std::any::type_name::<DefaultTypeTask>();
            assert_eq!(summary.registered_types, vec![expected_type]);
            assert_eq!(summary.total_executed, 0);
        }

        #[tokio::test]
        async fn test_task_execution_with_stats() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(ConfiguredTypeTaskExecutor))
                .await;

            let task_type = std::any::type_name::<ConfiguredTypeTask>();

            // Execute a successful task
            let task1 = QueuedTask {
                id: "config-1".to_string(),
                task_type: task_type.to_string(),
                payload: serde_json::json!({"data": "test1"}),
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            let result1 = registry.execute_task(&task1).await.unwrap();
            assert_eq!(result1, "Configured: test1");

            // Execute another task
            let task2 = QueuedTask {
                id: "config-2".to_string(),
                task_type: task_type.to_string(),
                payload: serde_json::json!({"data": "test2"}),
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            registry.execute_task(&task2).await.unwrap();

            // Check stats
            let stats = registry.get_stats(task_type).await.unwrap();
            assert_eq!(stats.total_executed, 2);
            assert_eq!(stats.successful, 2);
            assert_eq!(stats.failed, 0);
            assert!(stats.avg_duration.is_some());
            assert!(stats.last_executed.is_some());
        }

        #[tokio::test]
        async fn test_failing_task_stats() {
            #[derive(Debug, Serialize, Deserialize, Default)]
            struct FailingTask {
                should_fail: bool,
            }

            #[async_trait]
            impl SmithyTask for FailingTask {
                type Output = String;

                async fn forge(self) -> SmithyResult<Self::Output> {
                    if self.should_fail {
                        Err(SmithyError::TaskExecutionFailed {
                            message: "Task failed".to_string(),
                            source: None,
                        })
                    } else {
                        Ok("Success".to_string())
                    }
                }
            }

            forge_task!(FailingTask);

            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(FailingTaskExecutor))
                .await;

            let task_type = std::any::type_name::<FailingTask>();

            // Execute a failing task
            let task = QueuedTask {
                id: "fail-123".to_string(),
                task_type: task_type.to_string(),
                payload: serde_json::json!({"should_fail": true}),
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            let result = registry.execute_task(&task).await;
            assert!(result.is_err());

            let stats = registry.get_stats(task_type).await.unwrap();
            assert_eq!(stats.total_executed, 1);
            assert_eq!(stats.successful, 0);
            assert_eq!(stats.failed, 1);
        }

        #[tokio::test]
        async fn test_create_task_with_registry() {
            let registry = TaskRegistry::new();
            registry
                .register_caller(Box::new(DefaultTypeTaskCaller))
                .await;

            let task_type = std::any::type_name::<DefaultTypeTask>();
            let payload = serde_json::json!({"message": "registry test"});

            let task = registry.create_task(task_type, payload).await;
            assert!(task.is_some());

            let task = task.unwrap();
            assert_eq!(task.task_type, task_type);
            assert_eq!(task.status, TaskStatus::Pending);
        }

        #[tokio::test]
        async fn test_create_task_unknown_type() {
            let registry = TaskRegistry::new();
            let payload = serde_json::json!({"data": "test"});

            let task = registry.create_task("unknown_type", payload).await;
            assert!(task.is_none());
        }

        #[tokio::test]
        async fn test_stats_reset() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;

            let task_type = std::any::type_name::<DefaultTypeTask>();
            let task = QueuedTask {
                id: "stats-123".to_string(),
                task_type: task_type.to_string(),
                payload: serde_json::json!({"message": "test"}),
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            // Execute task to generate stats
            registry.execute_task(&task).await.unwrap();

            let stats_before = registry.get_stats(task_type).await.unwrap();
            assert_eq!(stats_before.total_executed, 1);

            // Reset stats
            registry.reset_stats().await;

            let stats_after = registry.get_stats(task_type).await.unwrap();
            assert_eq!(stats_after.total_executed, 0);
            assert_eq!(stats_after.successful, 0);
            assert_eq!(stats_after.failed, 0);
        }

        #[tokio::test]
        async fn test_registry_clear() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;
            registry
                .register_caller(Box::new(DefaultTypeTaskCaller))
                .await;

            assert_eq!(registry.executor_count().await, 1);
            assert_eq!(registry.caller_count().await, 1);

            registry.clear().await;

            assert_eq!(registry.executor_count().await, 0);
            assert_eq!(registry.caller_count().await, 0);
            assert!(registry.get_registered_types().await.is_empty());
        }

        #[tokio::test]
        async fn test_all_stats_aggregation() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;
            registry
                .register_executor(Box::new(ConfiguredTypeTaskExecutor))
                .await;

            // Execute tasks of different types
            let task1_type = std::any::type_name::<DefaultTypeTask>();
            let task1 = QueuedTask {
                id: "agg-1".to_string(),
                task_type: task1_type.to_string(),
                payload: serde_json::json!({"message": "test1"}),
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            let task2_type = std::any::type_name::<ConfiguredTypeTask>();
            let task2 = QueuedTask {
                id: "agg-2".to_string(),
                task_type: task2_type.to_string(),
                payload: serde_json::json!({"data": "test2"}),
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            registry.execute_task(&task1).await.unwrap();
            registry.execute_task(&task2).await.unwrap();

            let all_stats = registry.get_all_stats().await;
            assert_eq!(all_stats.len(), 2);

            let summary = registry.summary().await;
            assert_eq!(summary.total_executed, 2);
            assert_eq!(summary.total_successful, 2);
            assert_eq!(summary.total_failed, 0);
        }

        #[tokio::test]
        async fn test_executor_caller_type_consistency() {
            let executor = DefaultTypeTaskExecutor;
            let caller = DefaultTypeTaskCaller::new();

            // Both should return the same task type
            assert_eq!(executor.task_type(), caller.task_type());

            let expected_type = std::any::type_name::<DefaultTypeTask>();
            assert_eq!(executor.task_type(), expected_type);
            assert_eq!(caller.task_type(), expected_type);
        }

        #[tokio::test]
        async fn test_metadata_with_defaults() {
            let executor = DefaultTypeTaskExecutor;
            let metadata = executor.metadata();

            assert_eq!(metadata.description, None);
            assert!(!metadata.cpu_intensive);
            assert!(metadata.io_intensive); // Default is true
            assert_eq!(metadata.priority, TaskPriority::Normal);
            assert!(metadata.tags.is_empty());
        }

        #[tokio::test]
        async fn test_caller_default_config() {
            let caller = DefaultTypeTaskCaller::new();
            let config = caller.default_config();

            assert_eq!(config.max_retries, 3);
            assert_eq!(config.timeout, None);
            assert_eq!(config.priority, TaskPriority::Normal);
            assert!(config.concurrent);
            assert_eq!(config.rate_limit, 0);
        }

        #[tokio::test]
        async fn test_task_serialization_error() {
            let registry = TaskRegistry::new();
            registry
                .register_executor(Box::new(DefaultTypeTaskExecutor))
                .await;

            let task_type = std::any::type_name::<DefaultTypeTask>();
            let task = QueuedTask {
                id: "invalid-123".to_string(),
                task_type: task_type.to_string(),
                payload: serde_json::json!({"invalid_field": "value"}), // Missing required field
                status: TaskStatus::Pending,
                retry_count: 0,
                max_retries: 3,
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
                execute_at: None,
            };

            let result = registry.execute_task(&task).await;
            assert!(result.is_err());

            match result.unwrap_err() {
                SmithyError::SerializationError(_) => {
                    // Expected error type
                }
                other => panic!("Expected SerializationError, got: {:?}", other),
            }
        }
    }

    // #[tokio::test]
    // async fn test_task_type_override() {
    //     // TestTask ha un override del task_type
    //     let executor = TestTaskExecutor;
    //     assert_eq!(executor.task_type(), "test_task");

    //     let caller = TestTaskCaller;
    //     assert_eq!(caller.task_type(), "test_task");
    // }

    // #[tokio::test]
    // async fn test_task_caller_functionality() {
    //     let caller = TestTaskCaller::new();
    //     let test_task = TestTask {
    //         data: "test data".to_string(),
    //     };

    //     let queued_task = caller.call(test_task).await.unwrap();
    //     assert_eq!(queued_task.task_type, "test_task");
    //     assert_eq!(queued_task.status, TaskStatus::Pending);
    //     assert_eq!(queued_task.retry_count, 0);
    // }

    // #[tokio::test]
    // async fn test_registry_summary() {
    //     let registry = TaskRegistry::new();
    //     registry.register_executor(Box::new(TestTaskExecutor)).await;
    //     registry.register_caller(Box::new(TestTaskCaller)).await;

    //     let summary = registry.summary().await;
    //     assert_eq!(summary.executor_count, 1);
    //     assert_eq!(summary.caller_count, 1);
    //     assert_eq!(summary.registered_types, vec!["test_task"]);
    //     assert_eq!(summary.total_executed, 0);
    // }

    #[tokio::test]
    async fn test_unknown_task_type() {
        let registry = TaskRegistry::new();

        let task = QueuedTask {
            id: "test-123".to_string(),
            task_type: "unknown_task".to_string(),
            payload: serde_json::json!({}),
            status: TaskStatus::Pending,
            retry_count: 0,
            max_retries: 3,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            execute_at: None,
        };

        let result = registry.execute_task(&task).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            SmithyError::TaskNotFound { task_type } => {
                assert_eq!(task_type, "unknown_task");
            }
            _ => panic!("Expected TaskNotFound error"),
        }
    }

    #[tokio::test]
    async fn test_auto_registration_system() {
        // Test che il sistema di auto-registrazione funzioni
        execute_auto_registrations().await;

        let registry = get_registry();
        // Dopo l'auto-registrazione, dovremmo avere i task registrati
        let types = registry.get_registered_types().await;
        assert!(
            !types.is_empty(),
            "Auto-registration should register some tasks"
        );
    }
}
