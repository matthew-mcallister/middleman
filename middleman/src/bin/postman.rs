use std::sync::Arc;

use log::info;
use middleman::error::Result;
use middleman::{init_logging, Application};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config = middleman::config::load_config()?;
    let app = Arc::new(Application::new(config.clone())?);
    let router = middleman::api::router(app);

    let listener = tokio::net::TcpListener::bind((config.host, config.port)).await?;
    info!("Listening on {}:{}", config.host, config.port);
    axum::serve(listener, router).await?;

    Ok(())
}
