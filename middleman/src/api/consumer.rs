use std::str::FromStr;
use std::sync::Arc;

use crate::api::to_json::{ConsumerApiSerializer, JsonFormatter};
use axum::extract::{Path, Query, State};
use axum::routing::{delete, get, put, Router};
use axum::Json;
use cast::cast_from;
use http::StatusCode;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::error::{Error, ErrorKind, Result};
use crate::subscriber::SubscriberBuilder;
use crate::Application;

use super::auth::{ConsumerAuth, TokenValidator};

#[derive(Debug)]
pub(crate) struct ConsumerApi {
    pub(crate) app: Arc<Application>,
    pub(crate) token_validator: TokenValidator,
}

#[derive(Serialize, Deserialize)]
struct ListEvents {
    starting_id: Option<u64>,
    stream: Option<String>,
    max_results: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct PutSubscriber {
    id: Uuid,
    stream_regex: String,
    destination_url: String,
    max_connections: Option<u16>,
    hmac_key: String,
}

impl TryFrom<PutSubscriber> for SubscriberBuilder {
    type Error = Box<Error>;

    fn try_from(value: PutSubscriber) -> Result<Self> {
        let mut builder = SubscriberBuilder::new();
        builder
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

pub fn router(app: Arc<Application>) -> Result<Router> {
    let token_validator = TokenValidator::new(&app)?;
    let api = ConsumerApi {
        app,
        token_validator,
    };

    let router = Router::<Arc<ConsumerApi>>::new()
        .route(
            "/events",
            get(
                // TODO: Reduce redundancy between producer and consumer APIs somewhat
                async move |query: Query<ListEvents>,
                            ConsumerAuth(tag),
                            State(api): State<Arc<ConsumerApi>>|
                            -> Result<_> {
                    let app = &api.app;
                    let max_results = query.max_results.unwrap_or(100).max(100);
                    let starting_id = query.starting_id.unwrap_or(0);
                    let events: Result<Vec<_>> = if let Some(stream) = query.stream.as_ref() {
                        app.events
                            .iter_by_stream(tag, stream, starting_id)
                            .take(max_results as _)
                            .collect()
                    } else {
                        app.events.iter_by_tag(tag, starting_id).take(max_results as _).collect()
                    };
                    let events: Vec<Box<ConsumerApiSerializer<_>>> = cast_from(events?);
                    Ok(JsonFormatter(events))
                },
            ),
        )
        .route(
            "/subscribers",
            put(
                async move |ConsumerAuth(tag),
                            State(api): State<Arc<ConsumerApi>>,
                            Json(request): Json<PutSubscriber>|
                            -> Result<_> {
                    let app = &api.app;
                    let mut builder: SubscriberBuilder = request.try_into()?;
                    builder.tag(tag);
                    let subscriber = app.create_subscriber(builder)?;
                    let subscriber: Box<ConsumerApiSerializer<_>> = cast_from(subscriber);
                    Ok(Json(subscriber))
                },
            )
            .get(
                async move |ConsumerAuth(tag), State(api): State<Arc<ConsumerApi>>| -> Result<_> {
                    let app = &api.app;
                    let subscribers: Result<Vec<_>> = app.subscribers.iter_by_tag(tag).collect();
                    let subscribers: Vec<Box<ConsumerApiSerializer<_>>> = cast_from(subscribers?);
                    Ok(Json(subscribers))
                },
            ),
        )
        .route(
            "/subscribers/{*id}",
            get(
                async move |ConsumerAuth(tag),
                            Path(id): Path<String>,

                            State(api): State<Arc<ConsumerApi>>|
                            -> Result<_> {
                    let id = Uuid::from_str(&id).map_err(|_| ErrorKind::NotFound)?;
                    let subscriber =
                        api.app.subscribers.get(tag, id)?.ok_or(ErrorKind::NotFound)?;
                    let subscriber: Box<ConsumerApiSerializer<_>> = cast_from(subscriber);
                    Ok(Json(subscriber))
                },
            )
            .delete(
                async move |ConsumerAuth(tag),
                            Path(id): Path<String>,
                            State(api): State<Arc<ConsumerApi>>|
                            -> Result<_> {
                    let id = Uuid::from_str(&id).map_err(|_| ErrorKind::NotFound)?;
                    api.app.delete_subscriber(tag, id)?;
                    Ok(StatusCode::from_u16(200).unwrap())
                },
            ),
        )
        .with_state(Arc::new(api));

    Ok(router)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::uuid;

    use crate::testing::TestHarness;

    #[tokio::test(flavor = "current_thread")]
    async fn test_jwt_auth() {
        let mut harness = TestHarness::new();

        let tag = uuid!("b8438ccb-59a0-4f9f-ba07-35914190f68e");
        let url = harness.consumer_api().await;
        let url = format!("{url}/events");
        let token = harness.consumer_auth_token(tag.to_string());
        let client = reqwest::Client::new();

        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);

        let response = client.get(&url).send().await.unwrap();
        assert_eq!(response.status().as_u16(), 401);

        let response = client.get(&url).header("Authorization", "blah").send().await.unwrap();
        assert_eq!(response.status().as_u16(), 401);

        let response = client
            .get(&url)
            .header("Authorization", "Bearer wrongkey")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);

        // Make sure that we don't choke on invalid UUID
        let bad_token = harness.consumer_auth_token("invalidsub".to_owned());
        let response = client
            .get(&url)
            .header("Authorization", format!("Bearer {bad_token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 401);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_events() {
        let mut harness = TestHarness::new();

        let producer_url = harness.producer_api().await;
        let client = reqwest::Client::new();
        let body = json!({
            "tag": "efd9fd38-73e2-4bb3-a5ab-f948738568af",
            "idempotency_key": "3ed5a219-e0e0-46af-bd8d-86d7380dc9da",
            "stream": "stream:0",
            "payload": {"hello": "world"},
        });
        let response = client
            .post(format!("{producer_url}/events"))
            .body(body.to_string())
            .header("Content-Type", "application/json")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);

        let tag = uuid!("efd9fd38-73e2-4bb3-a5ab-f948738568af");
        let token = harness.consumer_auth_token(tag.to_string());
        let consumer_url = harness.consumer_api().await;

        let response = client
            .get(format!("{consumer_url}/events"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let body: serde_json::Value =
            serde_json::from_str(&response.text().await.unwrap()).unwrap();
        let expected_body = json!([{
            "id": 1,
            "stream": "stream:0",
            "payload": {"hello": "world"},
        }]);
        assert_eq!(body, expected_body);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_subscribers() {
        let mut harness = TestHarness::new();

        let tag = uuid!("efd9fd38-73e2-4bb3-a5ab-f948738568af");
        let token = harness.consumer_auth_token(tag.to_string());
        let consumer_url = harness.consumer_api().await;
        let client = reqwest::Client::new();

        // Create
        let subscriber_id = "481b7576-bced-4d6b-a22a-9ee67f0c63f6";
        let body = json!({
            "id": subscriber_id,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook",
            "max_connections": 6,
            "hmac_key": "fakesecret",
        });
        let response = client
            .put(format!("{consumer_url}/subscribers"))
            .body(body.to_string())
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);

        // List
        let response = client
            .get(format!("{consumer_url}/subscribers"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let body: serde_json::Value =
            serde_json::from_str(&response.text().await.unwrap()).unwrap();
        let expected_body = json!([{
            "id": subscriber_id,
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook",
            // TODO
            //"max_connections": 6,
        }]);
        assert_eq!(body, expected_body);

        // Get by ID
        let response = client
            .get(format!("{consumer_url}/subscribers/{subscriber_id}"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let body: serde_json::Value =
            serde_json::from_str(&response.text().await.unwrap()).unwrap();
        let expected_body = json!({
            "id": "481b7576-bced-4d6b-a22a-9ee67f0c63f6",
            "stream_regex": ".*",
            "destination_url": "https://example.com/webhook",
            //"max_connections": 6,
        });
        assert_eq!(body, expected_body);

        // Delete
        let response = client
            .delete(format!("{consumer_url}/subscribers/{subscriber_id}"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);

        let response = client
            .get(format!("{consumer_url}/subscribers/{subscriber_id}"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 404);
    }
}
