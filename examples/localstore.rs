use std::time::Duration;

use smithyq::prelude::*;
use tokio::{signal, time::sleep};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct PingTask {}

#[async_trait::async_trait]
impl SmithyTask for PingTask {
    type Output = ();

    async fn forge(self) -> SmithyResult<Self::Output> {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        Ok(())
    }
}

forge_task!(PingTask);

#[tokio::main]
async fn main() -> SmithyResult<()> {
    tracing_subscriber::fmt::init();

    let smithy = SmithyQ::new(SmithyConfig::default()).await?;

    smithy.start_forging().await?;

    println!("🔨 Smithy is forging! Press Ctrl+C to stop...");

    for _ in 0..4 {
        smithy.enqueue(PingTask {}).await?;
    }

    sleep(Duration::from_secs(5)).await;

    for _ in 0..4 {
        smithy.enqueue(PingTask {}).await?;
    }

    signal::ctrl_c().await.expect("Failed to listen for ctrl+c");

    smithy.stop_forging().await?;

    println!("🔨 Smithy stopped forging");
    Ok(())
}
