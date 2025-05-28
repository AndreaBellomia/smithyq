use smithyq::prelude::*;
use tokio::signal;

#[tokio::main]
async fn main() -> SmithyResult<()> {
    // Inizializza il logging
    tracing_subscriber::fmt::init();

    // Crea il Smithy con configurazione di default
    let smithy = Smithy::new(SmithyConfig::default()).await?;

    // Avvia il "forging" (elaborazione dei task)
    smithy.start_forging().await?;

    println!("🔨 Smithy is forging! Press Ctrl+C to stop...");

    // Aspetta il segnale di interruzione
    signal::ctrl_c().await.expect("Failed to listen for ctrl+c");

    // Ferma gracefully
    smithy.stop_forging().await?;

    println!("🔨 Smithy stopped forging");
    Ok(())
}
