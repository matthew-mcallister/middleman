use std::sync::Arc;

use crate::api::to_json::{JsonFormatter, ProducerApiSerializer};
use axum::extract::{Query, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put, Router};
use axum::Json;
use cast::cast_from;
use http::StatusCode;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;
use url::Url;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::event::EventBuilder;
use crate::subscriber::SubscriberBuilder;
use crate::Application;

fn validate_bearer_key(key: &str, request: &Request) -> Result<()> {
    let auth_header_value = request
        .headers()
        .get("Authorization")
        .ok_or(ErrorKind::Unauthenticated)?
        .to_str()?;
    if !auth_header_value.starts_with("Bearer ") {
        Err(ErrorKind::Unauthenticated)?;
    }
    let request_key = &auth_header_value["Bearer ".len()..];
    if request_key != key {
        Err(ErrorKind::Unauthenticated)?;
    }
    Ok(())
}

async fn bearer_auth_middleware(
    State(app): State<Arc<Application>>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(ref key) = app.config.producer_api_bearer_token {
        if let Err(e) = validate_bearer_key(key, &request) {
            return e.into_response();
        }
    }
    next.run(request).await
}

#[derive(Serialize, Deserialize)]
struct PostEvent {
    tag: Uuid,
    idempotency_key: Uuid,
    stream: String,
    payload: Value,
}

#[derive(Serialize, Deserialize)]
struct ListEvents {
    tag: Uuid,
    starting_id: Option<u64>,
    stream: Option<String>,
    max_results: Option<u64>,
}

impl<'a> From<&'a PostEvent> for EventBuilder<'a> {
    fn from(value: &'a PostEvent) -> Self {
        let mut builder = EventBuilder::new();
        // XXX: Can this actually fail...?
        let payload = serde_json::to_string(&value.payload).unwrap();
        builder
            .idempotency_key(value.idempotency_key)
            .tag(value.tag)
            .stream(&value.stream)
            .payload(payload);
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
    id: Option<Uuid>,
}

#[derive(Serialize, Deserialize)]
struct DeleteSubscriber {
    tag: Uuid,
    id: Uuid,
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
    let mut router = Router::new()
        .route(
            "/events",
            post({
                let app = Arc::clone(&app);
                async move |Json(event): Json<PostEvent>| -> Result<_> {
                    let event: EventBuilder<'_> = (&event).into();
                    let event = app.create_event(event)?;
                    Ok(JsonFormatter(cast_from::<Box<ProducerApiSerializer<_>>, _>(event))
                        .into_response())
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
                        app.events
                            .iter_by_tag(query.tag, starting_id)
                            .take(max_results as _)
                            .collect()
                    };
                    let events: Vec<Box<ProducerApiSerializer<_>>> = cast_from(events?);
                    Ok(JsonFormatter(events))
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
                    let subscriber: Box<ProducerApiSerializer<_>> = cast_from(subscriber);
                    Ok(Json(subscriber))
                }
            })
            .get({
                let app = Arc::clone(&app);
                async move |query: Query<ListSubscribers>| -> Result<_> {
                    let subscribers: Result<Vec<_>> = if let Some(id) = query.id {
                        let tag = query
                            .tag
                            .ok_or(Error::with_cause(ErrorKind::InvalidInput, "missing tag"))?;
                        Ok(app.subscribers.get(tag, id)?.into_iter().collect())
                    } else {
                        if let Some(tag) = query.tag {
                            app.subscribers.iter_by_tag(tag).collect()
                        } else {
                            app.subscribers.iter().collect()
                        }
                    };
                    let subscribers: Vec<Box<ProducerApiSerializer<_>>> = cast_from(subscribers?);
                    Ok(Json(subscribers))
                }
            })
            .delete({
                let app = Arc::clone(&app);
                async move |Query(query): Query<DeleteSubscriber>| -> Result<_> {
                    app.delete_subscriber(query.tag, query.id)?;
                    Ok(StatusCode::from_u16(200).unwrap())
                }
            }),
        );

    if app.config.producer_api_bearer_token.is_some() {
        router = router
            .layer(axum::middleware::from_fn_with_state(Arc::clone(&app), bearer_auth_middleware));
    } else {
        warn!("producer API bearer token not present; requests will not be authenticated")
    }

    router
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::uuid;

    use crate::testing::{check_status, TestHarness};

    #[tokio::test(flavor = "current_thread")]
    async fn test_events() {
        let mut harness = TestHarness::new();
        let url = harness.producer_api().await;
        let client = reqwest::Client::new();

        let body = json!({
            "tag": "efd9fd38-73e2-4bb3-a5ab-f948738568af",
            "idempotency_key": "3ed5a219-e0e0-46af-bd8d-86d7380dc9da",
            "stream": "stream:0",
            "payload": {"hello": "world"},
        });
        check_status!(
            200,
            client
                .post(format!("{url}/events"))
                .body(body.to_string())
                .header("Content-Type", "application/json"),
        );

        let response = check_status!(
            200,
            client.get(format!("{url}/events?tag=efd9fd38-73e2-4bb3-a5ab-f948738568af")),
        );
        let expected = json!([{
            "id": 1,
            "tag": "efd9fd38-73e2-4bb3-a5ab-f948738568af",
            "idempotency_key": "3ed5a219-e0e0-46af-bd8d-86d7380dc9da",
            "stream": "stream:0",
            "payload": {"hello": "world"},
        }]);
        assert_eq!(response, Some(expected));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_bearer_auth() {
        let mut harness = TestHarness::new();
        let secret = "notsecret1";
        harness.config().producer_api_bearer_token = Some(secret.to_owned());

        let url = harness.producer_api().await;
        let url = format!("{url}/events?tag=3c9bd902-cf3d-4ba0-b757-acce3bb8c345");
        let client = reqwest::Client::new();

        check_status!(200, client.get(&url).header("Authorization", format!("Bearer {secret}")));
        check_status!(401, client.get(&url));
        check_status!(401, client.get(&url).header("Authorization", "blah"));
        check_status!(401, client.get(&url).header("Authorization", "Bearer wrongkey"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_subscribers() {
        let mut harness = TestHarness::new();
        let url = harness.producer_api().await;
        let client = reqwest::Client::new();

        // Create two subscribers
        let tag = uuid!("094ce043-4789-43d7-b9a8-dbee7b658276");
        let id1 = uuid!("8187c0ef-7d63-4dd9-9dee-4b48730cd10a");
        let body = json!({
            "tag": tag,
            "id": id1,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook",
            "max_connections": 5,
            "hmac_key": "nosecret",
        });
        check_status!(
            200,
            client
                .put(format!("{url}/subscribers"))
                .body(body.to_string())
                .header("Content-Type", "application/json"),
        );

        let id2 = uuid!("de9d7723-1a4d-48d4-8c6f-2a481fc8680b");
        let body = json!({
            "tag": tag,
            "id": id2,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook",
            "max_connections": 5,
            "hmac_key": "nosecret",
        });
        check_status!(
            200,
            client
                .put(format!("{url}/subscribers"))
                .body(body.to_string())
                .header("Content-Type", "application/json"),
        );

        // List subscribers
        let response = check_status!(200, client.get(format!("{url}/subscribers"))).unwrap();
        let expected = json!([
            {
                "tag": tag,
                "id": id1,
                "stream_regex": ".*",
                "destination_url": "https://example.com/webhook",
                //"max_connections": 5,
            },
            {
                "tag": tag,
                "id": id2,
                "stream_regex": ".*",
                "destination_url": "https://example.com/webhook",
                //"max_connections": 5,
            },
        ]);
        assert_eq!(response, expected);

        // Modify subscriber
        let id2 = uuid!("de9d7723-1a4d-48d4-8c6f-2a481fc8680b");
        let body = json!({
            "tag": tag,
            "id": id2,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook2",
            "max_connections": 5,
            "hmac_key": "nosecret",
        });
        check_status!(
            200,
            client
                .put(format!("{url}/subscribers"))
                .body(body.to_string())
                .header("Content-Type", "application/json")
        );

        // Get subscriber
        let response =
            check_status!(200, client.get(format!("{url}/subscribers?tag={tag}&id={id2}")))
                .unwrap();
        let expected = json!([{
            "tag": tag,
            "id": id2,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook2",
            //"max_connections": 5,
        }]);
        assert_eq!(response, expected);

        // (with missing tag)
        check_status!(400, client.get(format!("{url}/subscribers?id={id2}")));

        // List by tag
        let response =
            check_status!(200, client.get(format!("{url}/subscribers?tag={tag}"))).unwrap();
        let expected = json!([
            {
                "tag": tag,
                "id": id1,
                "stream_regex": ".*",
                "destination_url": "https://example.com/webhook",
                //"max_connections": 5,
            },
            {
                "tag": tag,
                "id": id2,
                "stream_regex": ".*",
                "destination_url": "https://example.com/webhook2",
                //"max_connections": 5,
            },
        ]);
        assert_eq!(response, expected);

        let response = check_status!(
            200,
            client.get(format!("{url}/subscribers?tag=00000000-0000-0000-0000-000000000000"))
        );
        assert_eq!(response.unwrap(), json!([]));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_delete_subscribers() {
        let mut harness = TestHarness::new();
        let url = harness.producer_api().await;
        let client = reqwest::Client::new();

        let tag = uuid!("094ce043-4789-43d7-b9a8-dbee7b658276");
        let id1 = uuid!("8187c0ef-7d63-4dd9-9dee-4b48730cd10a");
        let id2 = uuid!("de9d7723-1a4d-48d4-8c6f-2a481fc8680b");
        for id in [id1, id2].into_iter() {
            let body = json!({
                "tag": tag,
                "id": id,
                "stream_regex": ".*",
                "destination_url": "https://example.com/webhook",
                "max_connections": 5,
                "hmac_key": "nosecret",
            });
            check_status!(
                200,
                client
                    .put(format!("{url}/subscribers"))
                    .body(body.to_string())
                    .header("Content-Type", "application/json")
            );
        }

        let response = check_status!(200, client.get(format!("{url}/subscribers"))).unwrap();
        let expected_body = json!([
            {
                "tag": tag,
                "id": id1,
                "stream_regex": ".*",
                "destination_url": "https://example.com/webhook",
                //"max_connections": 5,
            },
            {
                "tag": tag,
                "id": id2,
                "stream_regex": ".*",
                "destination_url": "https://example.com/webhook",
                //"max_connections": 5,
            },
        ]);
        assert_eq!(response, expected_body);

        // Delete
        check_status!(200, client.delete(format!("{url}/subscribers?tag={tag}&id={id2}")));

        let response = check_status!(200, client.get(format!("{url}/subscribers"))).unwrap();
        let expected = json!([{
            "tag": tag,
            "id": id1,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook",
            //"max_connections": 5,
        }]);
        assert_eq!(response, expected);

        // (delete twice for good measure)
        check_status!(200, client.delete(format!("{url}/subscribers?tag={tag}&id={id2}")));

        let response = check_status!(200, client.get(format!("{url}/subscribers"))).unwrap();
        let expected = json!([{
            "tag": tag,
            "id": id1,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook",
            //"max_connections": 5,
        }]);
        assert_eq!(response, expected);

        // Can't delete without both tag and ID
        check_status!(400, client.delete(format!("{url}/subscribers")));
        check_status!(400, client.delete(format!("{url}/subscribers?id={id1}")));
        check_status!(400, client.delete(format!("{url}/subscribers?tag={tag}")));
        check_status!(200, client.delete(format!("{url}/subscribers?id={id1}&tag={tag}")));
    }
}
