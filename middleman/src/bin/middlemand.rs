use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing::{error, info, warn};

use middleman::error::{Error, Result};
use middleman::ingestion::sql::SqlIngestor;
use middleman::util::sleep_until_next_tick;
use middleman::{init_logging, Application};

/// Middleman event distribution daemon.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to a config file to load.
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        dotenvy::dotenv().unwrap();
    }

    init_logging();

    let args = Args::parse();
    let config = middleman::config::load_config(args.config)?;
    let app = Arc::new(Application::new(config.clone())?);

    let ref_time = tokio::time::Instant::now();
    let period = 0.2;

    tokio::spawn({
        let app = Arc::clone(&app);
        async move {
            loop {
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

    if let Some(options) = config.consumer_api_options() {
        let app = Arc::clone(&app);
        tokio::spawn(async move {
            let router = middleman::api::consumer::router(app)?;
            let listener = tokio::net::TcpListener::bind((options.host, options.port)).await?;
            info!("consumer API listening on {}:{}", options.host, options.port);
            axum::serve(listener, router).await?;
            Ok::<_, Box<Error>>(())
        });
    } else {
        warn!("consumer API not configured");
    }

    let router = middleman::api::producer::router(Arc::clone(&app));
    let producer_listener =
        tokio::net::TcpListener::bind((config.producer_api_host, config.producer_api_port)).await?;
    info!("producer API listening on {}:{}", config.producer_api_host, config.producer_api_port);
    axum::serve(producer_listener, router).await?;

    Ok(())
}
