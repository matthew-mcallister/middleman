use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use http::{HeaderValue, Request};
use middleman_db::Transaction;
use tracing::debug;
use uuid::Uuid;

use crate::api::to_json::{cast, ConsumerApiSerializer, JsonFormatter};
use crate::delivery::Delivery;
use crate::error::{Error, ErrorKind, Result};
use crate::event::Event;
use crate::http::timestamp_and_sign_request;
use crate::scheduler::TaskShared;
use crate::subscriber::Subscriber;

fn between(x: u16, min: u16, max: u16) -> bool {
    x >= min && x < max
}

#[derive(Debug)]
pub(crate) struct DeliveryTask {
    shared: Arc<TaskShared>,
    transaction: Transaction,
    subscriber: Box<Subscriber>,
    delivery: Delivery,
    event: Box<Event>,
}

impl DeliveryTask {
    pub(crate) fn new(
        shared: Arc<TaskShared>,
        transaction: Transaction,
        subscriber_id: Uuid,
        event_id: u64,
    ) -> Result<Self> {
        let subscriber = shared.subscribers.get(subscriber_id)?.ok_or(ErrorKind::Unexpected)?;
        let event = shared.events.get(subscriber.tag(), event_id)?.ok_or(ErrorKind::Unexpected)?;
        let delivery =
            shared.deliveries.get(subscriber_id, event_id)?.ok_or(ErrorKind::Unexpected)?;
        Ok(Self {
            shared,
            transaction,
            subscriber,
            delivery,
            event,
        })
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        debug!(
            "delivering event {} to subscriber {} attempt {}",
            self.delivery.event_id(),
            self.delivery.subscriber_id(),
            self.delivery.attempts_made() + 1,
        );

        let subscriber_id = self.delivery.subscriber_id();
        let mut txn = self.transaction;

        let body = cast::<_, &JsonFormatter<_>>(cast::<_, &ConsumerApiSerializer<_>>(&*self.event));
        let mut request = Request::new(body.to_string());
        *request.method_mut() = http::Method::POST;
        request.headers_mut().insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        let timestamp: chrono::DateTime<chrono::Utc> = SystemTime::now().into();
        timestamp_and_sign_request(timestamp.into(), self.subscriber.hmac_key(), &mut request);

        let connection = self.shared.connections.connect(&subscriber_id).await;
        // Mark the task as started whether or not the connection succeeded
        self.shared.stats.tasks_started_last_tick.fetch_add(1, Ordering::Acquire);

        let response = match connection {
            Ok(mut connection) => connection.send_request(request).await,
            Err(e) => Err(e),
        };
        match response {
            Ok(response) if between(u16::from(response.status()), 200, 300) => {
                debug!(
                    delivery = ?self.delivery,
                    "delivery succeeded for event {}, subscriber {}",
                    self.event.id(),
                    self.subscriber.id(),
                );
                self.shared.deliveries.delete(&mut txn, &mut self.delivery);
            },
            // TODO: Handle 300
            Err(e) => {
                debug!(delivery = ?self.delivery, "delivery failed: {}", e);
                self.shared.deliveries.update_for_next_attempt(&mut txn, &mut self.delivery);
            },
            _ => {
                self.shared.deliveries.update_for_next_attempt(&mut txn, &mut self.delivery);
            },
        }

        txn.commit()?;

        Ok::<(), Box<Error>>(())
    }
}
