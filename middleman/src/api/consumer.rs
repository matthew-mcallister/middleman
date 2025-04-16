use std::sync::Arc;

use crate::api::to_json::{JsonFormatter, ProducerApiSerializer};
use axum::extract::{Query, State};
use axum::routing::{get, put, Router};
use axum::Json;
use cast::cast_from;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::error::{Error, Result};
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
                        app.events
                            .iter_by_tag(tag, starting_id)
                            .take(max_results as _)
                            .collect()
                    };
                    let events: Vec<Box<ProducerApiSerializer<_>>> = cast_from(events?);
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
                    let subscriber: Box<ProducerApiSerializer<_>> = cast_from(subscriber);
                    Ok(Json(subscriber))
                },
            )
            .get(
                async move |ConsumerAuth(tag), State(api): State<Arc<ConsumerApi>>| -> Result<_> {
                    let app = &api.app;
                    let subscribers: Result<Vec<_>> = app.subscribers.iter_by_tag(tag).collect();
                    let subscribers: Vec<Box<ProducerApiSerializer<_>>> = cast_from(subscribers?);
                    Ok(Json(subscribers))
                },
            ),
            // XXX: delete
        )
        .with_state(Arc::new(api));

    Ok(router)
}

#[cfg(test)]
mod tests {
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

        let response = client
            .get(&url)
            .header("Authorization", "blah")
            .send()
            .await
            .unwrap();
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
    async fn test_events() {}
}
