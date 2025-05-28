//! # SmithyQ - A High-Performance Task Queue Library
//!
//! SmithyQ is a Rust library for managing high-performance task queues,
//! designed to be simple to use yet powerful and flexible.
//!
//! ## Key Features
//!
//! - **Async tasks**: Full support for asynchronous tasks with `async/await`
//! - **Multiple queues**: Support for different queue implementations (memory, database)
//! - **Worker pool**: Configurable worker system for parallel processing
//! - **Task registry**: Centralized registration and management of task types
//! - **Error handling**: Robust error handling with automatic retry
//! - **Flexible configuration**: Complete configuration system
//!
//! ## Architecture
//!
//! The library is organized into the following main modules:
//!
//! ### Core
//! - [`Engine`](crate::core::engine): The main engine that coordinates task execution
//! - [`Worker`](crate::core::worker): Workers that process tasks from the queue
//! - [`Registry`](crate::core::registry): Registry for registering task types
//!
//! ### Queue
//! - [`MemoryQueue`](crate::queue::memory): In-memory queue implementation
//!
//! ### Task and Configuration
//! - [`Task`](crate::task): Task definitions and traits
//! - [`Config`](crate::config): Configuration system
//! - [`Error`](crate::error): Library error types
//! - [`Utils`](crate::utils): Utilities and support functions
//!
//! ## Quick Start Example
//!
//! ```rust
//! use smithyq::{Engine, Task, Config};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct EmailTask {
//!     to: String,
//!     subject: String,
//!     body: String,
//! }
//!
//! impl Task for EmailTask {
//!     type Output = ();
//!     type Error = Box<dyn std::error::Error>;
//!
//!     async fn execute(&self) -> Result<Self::Output, Self::Error> {
//!         // Logic to send email
//!         println!("Sending email to: {}", self.to);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::default();
//!     let mut engine = Engine::new(config).await?;
//!     
//!     // Register the task type
//!     engine.register_task::<EmailTask>().await?;
//!     
//!     // Start the engine
//!     engine.start().await?;
//!     
//!     // Enqueue a task
//!     let task = EmailTask {
//!         to: "user@example.com".to_string(),
//!         subject: "Welcome".to_string(),
//!         body: "Thank you for signing up!".to_string(),
//!     };
//!     
//!     engine.enqueue(task).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Advanced Configuration
//!
//! ```rust
//! use smithyq::{Config, Engine};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::builder()
//!         .worker_count(10)
//!         .max_retries(3)
//!         .retry_delay(std::time::Duration::from_secs(5))
//!         .build();
//!         
//!     let engine = Engine::new(config).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Usage Patterns
//!
//! ### Tasks with Retry Logic
//!
//! ```rust
//! use smithyq::Task;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct ApiCallTask {
//!     url: String,
//!     retry_count: u32,
//! }
//!
//! impl Task for ApiCallTask {
//!     type Output = String;
//!     type Error = Box<dyn std::error::Error>;
//!
//!     async fn execute(&self) -> Result<Self::Output, Self::Error> {
//!         // Simulate API call that might fail
//!         if self.retry_count < 3 {
//!             return Err("API temporarily unavailable".into());
//!         }
//!         
//!         Ok("API response".to_string())
//!     }
//!
//!     fn max_retries(&self) -> u32 {
//!         5
//!     }
//! }
//! ```
//!
//! ### Tasks with Priority
//!
//! ```rust
//! use smithyq::Task;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct UrgentTask {
//!     data: String,
//! }
//!
//! impl Task for UrgentTask {
//!     type Output = ();
//!     type Error = Box<dyn std::error::Error>;
//!
//!     async fn execute(&self) -> Result<Self::Output, Self::Error> {
//!         println!("Processing urgent: {}", self.data);
//!         Ok(())
//!     }
//!
//!     fn priority(&self) -> u8 {
//!         255 // Maximum priority
//!     }
//! }
//! ```
//!
//! ## Database Integration
//!
//! SmithyQ supports different queue implementations. To use a database:
//!
//! ```rust
//! use smithyq::{Config, Engine, queue::DatabaseQueue};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = Config::builder()
//!         .database_url("postgresql://user:pass@localhost/smithyq")
//!         .build();
//!         
//!     let engine = Engine::with_database_queue(config).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Monitoring and Metrics
//!
//! ```rust
//! use smithyq::{Engine, metrics::Metrics};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let engine = Engine::new(Config::default()).await?;
//!     
//!     // Get metrics
//!     let metrics = engine.metrics().await;
//!     println!("Queued tasks: {}", metrics.queued_tasks);
//!     println!("Completed tasks: {}", metrics.completed_tasks);
//!     println!("Failed tasks: {}", metrics.failed_tasks);
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Best Practices
//!
//! ### 1. Error Handling
//! - Always implement appropriate retry logic
//! - Use structured logging to trace task execution
//! - Handle timeouts for long-running tasks
//!
//! ### 2. Performance
//! - Configure worker count based on available resources
//! - Use batch tasks for multiple similar operations
//! - Monitor metrics to optimize performance
//!
//! ### 3. Serialization
//! - Keep tasks serializable and lightweight
//! - Avoid including large amounts of data in tasks
//! - Use references or IDs for external data
//!
//! ## Task Lifecycle
//!
//! 1. **Enqueue**: Task is added to the queue
//! 2. **Pick**: Worker picks up the task
//! 3. **Execute**: Task logic is executed
//! 4. **Complete**: Task is marked as completed or failed
//! 5. **Retry**: Failed tasks are retried if configured
//!
//! ## Worker Management
//!
//! Workers are automatically managed by the engine:
//! - Workers request tasks from the dispatcher
//! - Failed workers are automatically restarted
//! - Worker statistics are tracked for monitoring
//!
//! ## Contributing
//!
//! SmithyQ is an open source project. To contribute:
//!
//! 1. Fork the repository
//! 2. Create a branch for your feature
//! 3. Add tests for new functionality
//! 4. Submit a pull request
//!
//! ## License
//!
//! This project is released under the MIT license.
