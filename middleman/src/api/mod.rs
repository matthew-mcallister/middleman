use std::sync::Arc;

use axum::routing::{post, Router};
use axum::Json;
use hyper::StatusCode;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;
use crate::event::{Event, EventBuilder};
use crate::types::ContentType;
use crate::Application;

#[derive(Serialize, Deserialize)]
struct PutEvent {
    tag: Uuid,
    idempotency_key: Uuid,
    content_type: ContentType,
    stream: String,
    payload: String,
}

impl From<PutEvent> for Box<Event> {
    fn from(value: PutEvent) -> Self {
        let mut builder = EventBuilder::new();
        builder
            .idempotency_key(value.idempotency_key)
            .tag(value.tag)
            .content_type(value.content_type)
            .stream(&value.stream)
            .payload(value.payload.as_bytes());
        builder.build()
    }
}

pub fn router(app: Arc<Application>) -> Router {
    Router::new().route(
        "/events",
        post({
            let app = Arc::clone(&app);
            async move |Json(event): Json<PutEvent>| -> Result<_, Error> {
                let event: Box<Event> = event.into();
                app.create_event(&event)?;
                Ok(StatusCode::CREATED)
            }
        }),
    )
}
