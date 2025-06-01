use serde::{Deserialize, Serialize};
use smithyq::QueueConfig;
use smithyq::config::{QueueBackendConfig, RedisConfig, SmithyConfig};
use smithyq::prelude::*;
use smithyq::queue::RedisQueue;
use std::time::Duration;
use tokio::signal;

#[derive(Debug, Serialize, Deserialize)]
struct HelloTask {
    pub name: String,
    pub message: String,
}

#[async_trait::async_trait]
impl SmithyTask for HelloTask {
    type Output = ();

    async fn forge(self) -> SmithyResult<Self::Output> {
        println!("👋 Hello {}: {}", self.name, self.message);
        tokio::time::sleep(Duration::from_millis(5000)).await;
        println!("✅ Task completed for {}", self.name);
        Ok(())
    }
}

forge_task!(HelloTask);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("🚀 SmithyQ Redis Simple Example");

    let redis_config = RedisConfig {
        connection_string: "redis://localhost:6379".to_string(),
        pool_size: 10,
        key_prefix: "smithyq".to_string(),
        db: 0,
    };

    println!(
        "🔗 Connecting to Redis at {}",
        redis_config.connection_string
    );

    let queue_config = QueueConfig {
        backend: QueueBackendConfig::Redis(redis_config.clone()),
        ..QueueConfig::default()
    };

    println!("🔧 Configuring Redis queue with prefix");

    let queue = RedisQueue::from_config(redis_config, queue_config.clone()).await?;
    queue.purge().await?;

    println!("🧹 Purged existing tasks from Redis queue");

    let config = SmithyConfig {
        queue: queue_config,
        ..SmithyConfig::development()
    };

    println!("⚙️ Initializing SmithyQ with Redis backend");

    let smithy = SmithyQ::with_queue(config, queue).await?;

    println!("🔨 Starting SmithyQ with Redis backend...");

    smithy.start_forging().await?;
    println!("✅ SmithyQ started with Redis backend!");
    println!("🔨 Ready to enqueue tasks...");

    let tasks = vec![
        HelloTask {
            name: "User".to_string(),
            message: "Welcome to SmithyQ".to_string(),
        },
        HelloTask {
            name: "Redis".to_string(),
            message: "Task successfully enqueued!".to_string(),
        },
        HelloTask {
            name: "SmithyQ".to_string(),
            message: "Fantastic Rust queuing system!".to_string(),
        },
        HelloTask {
            name: "Alice".to_string(),
            message: "Hello from Wonderland!".to_string(),
        },
        HelloTask {
            name: "Bob".to_string(),
            message: "Greetings from the Builder!".to_string(),
        },
        HelloTask {
            name: "Charlie".to_string(),
            message: "Salutations from the Chocolate Factory!".to_string(),
        },
        HelloTask {
            name: "Dave".to_string(),
            message: "Hey there from the Data Center!".to_string(),
        },
        HelloTask {
            name: "Eve".to_string(),
            message: "Hi from the Garden of Eden!".to_string(),
        },
    ];

    for task in tasks {
        let task_name = task.name.clone();
        smithy.enqueue(task).await?;
        println!("📥 Task enqueued: {}", task_name);
    }

    signal::ctrl_c().await.expect("Failed to listen for ctrl+c");

    smithy.stop_forging().await?;
    Ok(())
}
