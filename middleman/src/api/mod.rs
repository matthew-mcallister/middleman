use std::sync::Arc;

use axum::extract::Query;
use axum::routing::{post, Router};
use axum::Json;
use hyper::StatusCode;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;
use crate::event::EventBuilder;
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

#[derive(Serialize, Deserialize)]
struct ListEvents {
    tag: Uuid,
    starting_id: Option<u64>,
    stream: Option<String>,
    max_results: Option<u64>,
}

impl<'a> From<&'a PutEvent> for EventBuilder<'a> {
    fn from(value: &'a PutEvent) -> Self {
        let mut builder = EventBuilder::new();
        builder
            .idempotency_key(value.idempotency_key)
            .tag(value.tag)
            .content_type(value.content_type)
            .stream(&value.stream)
            .payload(value.payload.as_bytes());
        builder
    }
}

pub fn router(app: Arc<Application>) -> Router {
    Router::new().route(
        "/events",
        post({
            let app = Arc::clone(&app);
            async move |Json(event): Json<PutEvent>| -> Result<_> {
                let event: EventBuilder<'_> = (&event).into();
                app.create_event(event)?;
                Ok(StatusCode::CREATED)
            }
        })
        .get({
            let app = Arc::clone(&app);
            async move |query: Query<ListEvents>| -> Result<_> {
                let max_results = query.max_results.unwrap_or(100).max(100);
                let starting_id = query.starting_id.unwrap_or(0);
                let events: Result<Vec<_>> = if let Some(stream) = query.stream.as_ref() {
                    app.events
                        .iter_by_stream(query.tag, stream, starting_id)
                        .take(max_results as _)
                        .collect()
                } else {
                    app.events.iter_by_tag(query.tag, starting_id).take(max_results as _).collect()
                };
                let events = events?;
                Ok(Json(events))
            }
        }),
    )
}
