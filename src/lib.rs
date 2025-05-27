//! # SmithyQ Worker
//!
//! A high-performance async task worker manager for Rust applications.
//!
//! ## Features
//!
//! - **Automatic Task Registration**: Use macros to automatically register tasks
//! - **Type-Safe Execution**: Compile-time safety for task payloads
//! - **Graceful Shutdown**: Proper cleanup and timeout handling
//! - **Backpressure Handling**: Intelligent worker scaling
//! - **Observability**: Built-in logging and metrics
//!
//! ## Quick Start
//!
//! ```rust
//! use rusty_worker::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct EmailTask {
//!     to: String,
//!     subject: String,
//! }
//!
//! #[async_trait::async_trait]
//! impl ExecutableTask for EmailTask {
//!     type Output = String;
//!     
//!     async fn execute(self) -> TaskResult<Self::Output> {
//!         // Your task logic here
//!         Ok(format!("Email sent to {}", self.to))
//!     }
//! }
//!
//! register_task!(EmailTask, "email");
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = WorkerConfig::default();
//!     let mut manager = WorkerManager::new(config).await?;
//!     
//!     // Register your tasks
//!     register_email_task().await;
//!     
//!     // Start workers
//!     manager.start().await?;
//!     
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod core;
pub mod error;
pub mod queue;
pub mod task;
pub mod utils;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;

pub mod prelude {
    pub use crate::config::*;
    pub use crate::core::Smithy;
    pub use crate::core::registry::{TaskCaller, TaskExecutor, TaskPriority, get_registry};
    pub use crate::error::{SmithyError, SmithyResult};
    pub use crate::forge_task;
    pub use crate::queue::{QueueBackend, TaskQueue};
    pub use crate::task::{QueuedTask, SmithyTask, TaskId, TaskStatus};
    pub use async_trait::async_trait;

    #[cfg(feature = "metrics")]
    #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
    pub use crate::metrics::SmithyMetrics;
}

pub use crate::config::*;
pub use crate::core::Smithy;
pub use crate::core::registry::{TaskCaller, TaskExecutor, TaskPriority, get_registry};
pub use crate::error::{SmithyError, SmithyResult};
pub use crate::queue::{QueueBackend, TaskQueue};
pub use crate::task::{QueuedTask, SmithyTask, TaskId, TaskStatus};
pub use async_trait::async_trait;
