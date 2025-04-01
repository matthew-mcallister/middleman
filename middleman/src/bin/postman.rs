use std::sync::Arc;

use log::info;
use middleman::error::Result;
use middleman::ingestion::sql::SqlIngestor;
use middleman::{init_logging, Application};
use tracing::warn;

#[tokio::main]
async fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        dotenvy::dotenv().unwrap();
    }

    init_logging();

    let config = middleman::config::load_config()?;
    let app = Arc::new(Application::new(config.clone())?);
    let router = middleman::api::router(Arc::clone(&app));

    tokio::spawn({
        let app = Arc::clone(&app);
        async move {
            // FIXME: catch panic inside loop
            loop {
                let res = app.schedule_deliveries();
                if let Err(e) = res {
                    // XXX: Logging
                    eprintln!("{}", e);
                }
                // FIXME: Calculate sleep end time at top of loop
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        }
    });

    if let Some(options) = config.ingestion_db_options() {
        let mut ingestor = SqlIngestor::new(Arc::clone(&app), options).await?;
        tokio::spawn(async move {
            loop {
                let res = ingestor.consume_events().await;
                if let Err(e) = res {
                    warn!("error consuming events: {}", e);
                }
                // FIXME: Calculate sleep end time at top of loop
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        });
    }

    let listener = tokio::net::TcpListener::bind((config.host, config.port)).await?;
    info!("Listening on {}:{}", config.host, config.port);
    axum::serve(listener, router).await?;

    Ok(())
}
