use std::sync::Arc;

use log::info;
use middleman::error::Result;
use middleman::ingestion::sql::SqlIngestor;
use middleman::util::sleep_until_next_tick;
use middleman::{init_logging, Application};
use tracing::error;

#[tokio::main]
async fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        dotenvy::dotenv().unwrap();
    }

    init_logging();

    let config = middleman::config::load_config()?;
    let app = Arc::new(Application::new(config.clone())?);
    let router = middleman::api::router(Arc::clone(&app));

    let ref_time = tokio::time::Instant::now();
    let period = 0.2;

    tokio::spawn({
        let app = Arc::clone(&app);
        async move {
            loop {
                // TODO maybe? Catch panics. Would require testing.
                let res = app.schedule_deliveries();
                if let Err(e) = res {
                    error!("error scheduling deliveries: {}", e);
                }
                sleep_until_next_tick(ref_time, period).await;
            }
        }
    });

    if let Some(options) = config.sql_ingestion_options() {
        let mut ingestor = SqlIngestor::new(Arc::clone(&app), options).await?;
        tokio::spawn(async move {
            loop {
                let res = ingestor.consume_events().await;
                if let Err(e) = res {
                    error!("error consuming events: {}", e);
                }
                sleep_until_next_tick(ref_time, period).await;
            }
        });
    }

    let listener = tokio::net::TcpListener::bind((config.host, config.port)).await?;
    info!("Listening on {}:{}", config.host, config.port);
    axum::serve(listener, router).await?;

    Ok(())
}
