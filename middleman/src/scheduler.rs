use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use http::Request;
use middleman_db::bytes::AsBytes;
use middleman_db::{packed, Db, Transaction};
use uuid::Uuid;

use crate::api::to_json::ToJson;
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
    fn new(shared: Arc<TaskShared>, subscriber_id: Uuid, event_id: u64) -> Result<Self> {
        let mut transaction = shared.db.clone().begin_transaction();
        let key = packed!(subscriber_id, event_id);
        transaction.lock_key(&shared.deliveries.cf, AsBytes::as_bytes(&key))?;
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
        let subscriber_id = self.delivery.subscriber_id();
        let mut txn = self.transaction;

        let body = ToJson(&self.event).to_string();
        let mut request = Request::new(body);
        let timestamp: chrono::DateTime<chrono::Utc> = SystemTime::now().into();
        timestamp_and_sign_request(timestamp.into(), self.subscriber.hmac_key(), &mut request);

        let result = self.shared.connections.connect(&subscriber_id).await;
        // Mark the task as started whether or not the connection succeeded
        self.shared.stats.tasks_started_last_tick.fetch_add(1, Ordering::Acquire);
        let mut connection = result?;

        let response = connection.send_request(request).await;
        match response {
            Ok(response) if between(u16::from(response.status()), 200, 300) => {
                self.shared.deliveries.delete(&mut txn, &mut self.delivery);
            },
            // TODO: Handle 300
            Err(_) | Ok(_) => {
                self.shared.deliveries.update_for_next_attempt(&mut txn, &mut self.delivery);
            },
        }

        txn.commit()?;

        Ok::<(), Box<Error>>(())
    }
}

#[derive(Debug)]
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

    fn spawn_task<'st>(&self, subscriber_id: Uuid, event_id: u64) -> Result<()> {
        let shared = Arc::clone(&self.shared);
        shared.stats.queued_tasks.fetch_add(1, Ordering::Acquire);

        let task = Task::new(shared, subscriber_id, event_id)?;
        tokio::spawn(task.run());
        Ok(())
    }
}
