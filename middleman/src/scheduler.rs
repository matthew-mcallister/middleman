use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use dashmap::DashMap;
use http::{HeaderValue, Request};
use middleman_db::{Db, Transaction};
use tracing::debug;
use uuid::Uuid;

use crate::api::to_json::{cast, ConsumerApiSerializer, JsonFormatter};
use crate::delivery::{Delivery, DeliveryTable};
use crate::error::{Error, ErrorKind, Result};
use crate::event::{Event, EventTable};
use crate::http::{timestamp_and_sign_request, SubscriberConnectionPool};
use crate::subscriber::{Subscriber, SubscriberTable};

fn between(x: u16, min: u16, max: u16) -> bool {
    x >= min && x < max
}

#[derive(Clone, Copy, Debug)]
struct RingBuffer<T, const N: usize> {
    head: usize,
    elems: [T; N],
}

impl<T, const N: usize> Default for RingBuffer<T, N>
where
    [T; N]: Default,
{
    fn default() -> Self {
        Self {
            head: 0,
            elems: Default::default(),
        }
    }
}

impl<T, const N: usize> Index<usize> for RingBuffer<T, N> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.elems[(self.head + index) % N]
    }
}

impl<T, const N: usize> IndexMut<usize> for RingBuffer<T, N> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.elems[(self.head + index) % N]
    }
}

impl<T, const N: usize> RingBuffer<T, N> {
    fn push(&mut self, value: T) {
        self.elems[self.head] = value;
        self.head = (self.head + 1) % N;
    }
}

#[derive(Debug)]
struct TaskShared {
    db: Arc<Db>,
    subscriber_id: Uuid,
    stats: Stats,
    subscribers: Arc<SubscriberTable>,
    events: Arc<EventTable>,
    deliveries: Arc<DeliveryTable>,
    connections: Arc<SubscriberConnectionPool>,
}

struct Task {
    shared: Arc<TaskShared>,
    transaction: Transaction,
    subscriber: Box<Subscriber>,
    delivery: Delivery,
    event: Box<Event>,
}

impl Task {
    fn new(
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

    async fn run(mut self) -> Result<()> {
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

#[derive(Debug, Default)]
struct Stats {
    queued_tasks: AtomicU32,
    tasks_started_last_tick: AtomicU32,
}

#[derive(Debug)]
pub(crate) struct UnitScheduler {
    shared: Arc<TaskShared>,
    tasks_started_history: RingBuffer<u32, 3>,
}

impl UnitScheduler {
    fn stats(&self) -> &Stats {
        &self.shared.stats
    }

    fn update_stats(&mut self) {
        let tasks_started = self.shared.stats.tasks_started_last_tick.swap(0, Ordering::Relaxed);
        self.tasks_started_history.push(tasks_started);
        self.stats().queued_tasks.fetch_sub(tasks_started, Ordering::Relaxed);
    }

    /// Predicts the number of tasks that will be started this ticks by
    /// autoregression on the last three ticks of measurements
    fn predict_tasks_started(&self) -> u32 {
        let [y0, y1, y2] = self.tasks_started_history.elems;
        let (y0, y1, y2) = (y0 as f32, y1 as f32, y2 as f32);
        let m = (y2 - y0) as f32 / 2.0;
        let b = (y0 + y1 + y2) as f32 / 3.0;
        (2f32 * m + b).ceil().min(0.0) as u32
    }

    fn num_tasks_to_spawn(&mut self) -> u32 {
        self.update_stats();

        let prediction = self.predict_tasks_started();
        let mut num = prediction;

        // Tweak spawn rate to maintain a reasonably sized queue
        let num_queued = self.stats().queued_tasks.load(Ordering::Relaxed);
        if num_queued < 30 {
            num += 1;
        } else if num_queued > 60 {
            num -= 1;
        }

        if num < 4 {
            num = 4;
        }

        num
    }

    fn spawn_task<'st>(
        &self,
        transaction: Transaction,
        subscriber_id: Uuid,
        event_id: u64,
    ) -> Result<()> {
        let shared = Arc::clone(&self.shared);
        shared.stats.queued_tasks.fetch_add(1, Ordering::Acquire);

        let task = Task::new(shared, transaction, subscriber_id, event_id)?;
        tokio::spawn(task.run());
        Ok(())
    }

    fn schedule(&mut self) -> Result<()> {
        let num_tasks = self.num_tasks_to_spawn();
        let max_time = chrono::Utc::now();
        for result in self
            .shared
            .deliveries
            .iter_by_next_attempt_skip_locked(self.shared.subscriber_id, max_time)
            .take(num_tasks as usize)
        {
            let (transaction, subscriber_id, event_id) = result?;
            self.spawn_task(transaction, subscriber_id, event_id)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Scheduler {
    subscribers: Arc<SubscriberTable>,
    events: Arc<EventTable>,
    deliveries: Arc<DeliveryTable>,
    connections: Arc<SubscriberConnectionPool>,
    unit_schedulers: DashMap<Uuid, UnitScheduler>,
}

impl Scheduler {
    pub fn new(
        subscribers: Arc<SubscriberTable>,
        events: Arc<EventTable>,
        deliveries: Arc<DeliveryTable>,
        connections: Arc<SubscriberConnectionPool>,
    ) -> Result<Self> {
        let this = Self {
            subscribers,
            events,
            deliveries,
            connections,
            unit_schedulers: Default::default(),
        };

        this.scan_subscribers()?;

        Ok(this)
    }

    // Registers all subscribers in the database to memory for scheduling.
    pub fn scan_subscribers(&self) -> Result<()> {
        for subscriber in self.subscribers.iter() {
            self.register_subscriber(&subscriber?);
        }
        Ok(())
    }

    // Adds a newly created subscriber to the scheduler without scanning.
    pub fn register_subscriber(&self, subscriber: &Subscriber) {
        let shared = Arc::new(TaskShared {
            db: Arc::clone(self.subscribers.db()),
            subscriber_id: subscriber.id(),
            stats: Default::default(),
            subscribers: Arc::clone(&self.subscribers),
            events: Arc::clone(&self.events),
            deliveries: Arc::clone(&self.deliveries),
            connections: Arc::clone(&self.connections),
        });
        self.unit_schedulers.insert(
            subscriber.id(),
            UnitScheduler {
                shared,
                tasks_started_history: Default::default(),
            },
        );
    }

    pub fn schedule_all(&self) -> Result<()> {
        for mut unit in self.unit_schedulers.iter_mut() {
            unit.schedule()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use http::{Request, Response};
    use regex::Regex;
    use serde_json::json;
    use tokio::time::timeout;
    use url::Url;

    use crate::event::EventBuilder;
    use crate::subscriber::SubscriberBuilder;
    use crate::testing::{http_server, TestHarness};

    #[tokio::test(flavor = "current_thread")]
    async fn test_schedule_event() {
        let mut harness = TestHarness::new();
        harness.application();
        let TestHarness { application, .. } = harness;
        let app = Arc::new(application.unwrap());

        // Set up webhook responder
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Request<String>>(10);
        let sender = Arc::new(sender);
        let (port, responder) = http_server(move |request| {
            let sender = Arc::clone(&sender);
            async move {
                let _ = sender.send(request).await;
                Ok(Response::new("".to_owned()))
            }
        })
        .await
        .unwrap();
        tokio::spawn(responder);

        let tag = uuid::uuid!("00000000-0000-8000-8000-000000000000");

        // Create a subscriber
        let url = format!("http://127.0.0.1:{port}/");
        let mut subscriber = SubscriberBuilder::new();
        subscriber
            .id(uuid::uuid!("10000000-0000-8000-8000-000000000001"))
            .tag(tag)
            .destination_url(Url::parse(&url).unwrap())
            .stream_regex(Regex::new(".*").unwrap())
            .hmac_key("key".to_owned());
        app.create_subscriber(subscriber).unwrap();

        // Create an event
        let idempotency_key = uuid::uuid!("00000000-0000-8000-8000-000000000000");
        let mut event = EventBuilder::new();
        event.tag(tag).stream("asdf:1234").payload("[1, 2, 3]").idempotency_key(idempotency_key);
        app.create_event(event).unwrap();
        assert_eq!(app.deliveries.iter().map(Result::unwrap).count(), 1);

        // Deliver event
        app.schedule_deliveries().unwrap();
        let request = timeout(Duration::from_secs(1), receiver.recv()).await.unwrap().unwrap();
        let request_json: serde_json::Value = serde_json::from_str(request.body()).unwrap();
        assert_eq!(
            request_json,
            json!({
                "id": 1,
                "stream": "asdf:1234",
                "payload": [1, 2, 3],
            })
        );

        // Wait until the delivery is deleted
        for i in 0.. {
            if i >= 5 {
                panic!("timed out waiting for delivery to be deleted");
            }
            let count = app.deliveries.iter().map(Result::unwrap).count();
            if count == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
