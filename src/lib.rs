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
//! ```rust, ignore
//! use smithyq::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Example {
//!     data: String,
//! }
//!
//! #[async_trait::async_trait]
//! impl SmithyTask for Example {
//!     type Output = String;
//!     
//!     async fn forge(self) -> SmithyResult<Self::Output> {
//!         // Your task logic here
//!         Ok(format!("Email sent to {}", self.data))
//!     }
//! }
//!
//! register_task!(MyTask, "example");
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = SmithyConfig::default();
//!     let mut manager = Smithy::new(config).await?;
//!     
//!     // Register your tasks
//!     register_example_task().await;
//!     
//!     // Start workers
//!     manager.start_forging().await?;
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

// #[cfg(feature = "metrics")]
// #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
// pub mod metrics;

pub mod prelude {
    pub use crate::config::*;
    pub use crate::core::SmithyQ;
    pub use crate::core::registry::{TaskCaller, TaskExecutor, TaskPriority, get_registry};
    pub use crate::error::{SmithyError, SmithyResult};
    pub use crate::forge_task;
    pub use crate::queue::{InMemoryQueue, QueueBackend, TaskQueue};
    pub use crate::task::{QueuedTask, SmithyTask, TaskId, TaskStatus};
    pub use async_trait::async_trait;

    // #[cfg(feature = "metrics")]
    // #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
    // pub use crate::metrics::SmithyMetrics;
}

pub use crate::config::*;
pub use crate::core::SmithyQ;
pub use crate::core::registry::{TaskCaller, TaskExecutor, TaskPriority, get_registry};
pub use crate::error::{SmithyError, SmithyResult};
pub use crate::queue::{QueueBackend, TaskQueue};
pub use crate::task::{QueuedTask, SmithyTask, TaskId, TaskStatus};
pub use async_trait::async_trait;
