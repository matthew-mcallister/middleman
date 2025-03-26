//! Sample webhook responder.

use axum::routing::get;
use axum::Router;
use http::StatusCode;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct Event {
    id: u64,
    tag: Uuid,
    stream: (),
    payload: (),
}

fn router() -> Router {
    Router::new().route(
        "/",
        get(async move |query: Query<ListEvents>| -> Result<_> { Ok(StatusCode::OK) }),
    )
}
