use std::sync::Arc;

use axum::extract::Query;
use axum::response::IntoResponse;
use axum::routing::{put, Router};
use axum::Json;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};
use to_json::{JsonFormatter, ProducerApiSerializer};
use url::Url;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::event::EventBuilder;
use crate::subscriber::SubscriberBuilder;
use crate::Application;

pub(crate) mod to_json;

#[derive(Serialize, Deserialize)]
struct PutEvent {
    tag: Uuid,
    idempotency_key: Uuid,
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
            .stream(&value.stream)
            .payload(&value.payload);
        builder
    }
}

#[derive(Serialize, Deserialize)]
struct PutSubscriber {
    tag: Uuid,
    id: Uuid,
    stream_regex: String,
    destination_url: String,
    max_connections: Option<u16>,
    hmac_key: String,
}

#[derive(Serialize, Deserialize)]
struct ListSubscribers {
    tag: Option<Uuid>,
}

impl TryFrom<PutSubscriber> for SubscriberBuilder {
    type Error = Box<Error>;

    fn try_from(value: PutSubscriber) -> Result<Self> {
        let mut builder = SubscriberBuilder::new();
        builder
            .tag(value.tag)
            .id(value.id)
            .destination_url(Url::parse(&value.destination_url)?)
            .stream_regex(Regex::new(&value.stream_regex)?)
            .hmac_key(value.hmac_key);
        if let Some(max_connections) = value.max_connections {
            builder.max_connections(max_connections);
        }
        Ok(builder)
    }
}

pub fn router(app: Arc<Application>) -> Router {
    Router::new()
        .route(
            "/events",
            put({
                let app = Arc::clone(&app);
                async move |Json(event): Json<PutEvent>| -> Result<_> {
                    let event: EventBuilder<'_> = (&event).into();
                    let event = app.create_event(event)?;
                    Ok(JsonFormatter(ProducerApiSerializer::from_box(event)).into_response())
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
                            .map(|x| x.map(|x| ProducerApiSerializer::from_box(x)))
                            .collect()
                    } else {
                        app.events
                            .iter_by_tag(query.tag, starting_id)
                            .take(max_results as _)
                            .map(|x| x.map(|x| ProducerApiSerializer::from_box(x)))
                            .collect()
                    };
                    Ok(JsonFormatter(events?))
                }
            }),
        )
        .route(
            "/subscribers",
            put({
                let app = Arc::clone(&app);
                async move |Json(request): Json<PutSubscriber>| -> Result<_> {
                    let builder: SubscriberBuilder = request.try_into()?;
                    let subscriber = app.create_subscriber(builder)?;
                    Ok(Json(ProducerApiSerializer(subscriber)))
                }
            })
            .get({
                let app = Arc::clone(&app);
                async move |query: Query<ListSubscribers>| -> Result<_> {
                    let subscribers: Result<Vec<_>> = if let Some(tag) = query.tag {
                        app.subscribers.iter_by_tag(tag).collect()
                    } else {
                        app.subscribers.iter().collect()
                    };
                    Ok(Json(ProducerApiSerializer::from_vec(subscribers?)))
                }
            }),
        )
}
