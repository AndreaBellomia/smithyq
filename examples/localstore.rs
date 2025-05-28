use smithyq::prelude::*;
use tokio::signal;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct EmailTask {
    to: String,
    subject: String,
    body: String,
}

#[async_trait::async_trait]
impl SmithyTask for EmailTask {
    type Output = String;

    async fn forge(self) -> SmithyResult<Self::Output> {
        // La tua logica del task qui
        println!("Sending email to: {}", self.to);
        // Simula l'invio dell'email
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(format!("Email sent to {}", self.to))
    }
}

forge_task!(EmailTask, "email_task");

#[tokio::main]
async fn main() -> SmithyResult<()> {
    // Inizializza il logging
    tracing_subscriber::fmt::init();

    // Crea il Smithy con configurazione di default
    let smithy = Smithy::new(SmithyConfig::default()).await?;

    register_email_task_task().await;

    // Avvia il "forging" (elaborazione dei task)
    smithy.start_forging().await?;

    println!("🔨 Smithy is forging! Press Ctrl+C to stop...");

    // Aspetta il segnale di interruzione
    signal::ctrl_c().await.expect("Failed to listen for ctrl+c");

    // Ferma gracefully
    smithy.stop_forging().await?;

    // Replace `MyTask` with the actual task struct identifier defined in your project
    // forge_task!(MyTask, "Task 1".to_string());

    println!("🔨 Smithy stopped forging");
    Ok(())
}
