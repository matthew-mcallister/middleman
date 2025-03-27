//! Sample webhook responder.
use std::str::FromStr;

use axum::routing::post;
use axum::{Json, Router};
use http::StatusCode;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use tower_http::trace::TraceLayer;
use tracing::{info, trace};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Deserialize, Serialize)]
struct Event {
    id: u64,
    stream: String,
    payload: Value,
}

fn router() -> Router {
    Router::new()
        .route(
            "/",
            post(async move |Json(event): Json<Event>| {
                trace!(?event);
                StatusCode::OK
            }),
        )
        .layer(TraceLayer::new_for_http())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();
    let host = std::env::var("HOST").unwrap_or("127.0.0.1".to_owned());
    let port = std::env::var("PORT").map(|x| u16::from_str(&x).unwrap()).unwrap_or(8090);
    let router = router();
    let listener = tokio::net::TcpListener::bind((&host[..], port)).await?;
    info!(host, port, "listening");
    axum::serve(listener, router).await?;
    Ok(())
}
