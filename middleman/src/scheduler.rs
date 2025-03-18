use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use http::Request;
use uuid::Uuid;

use crate::api::to_json::ToJson;
use crate::error::{Error, ErrorKind};
use crate::event::Event;
use crate::http::{timestamp_and_sign_request, SubscriberConnectionPool};
use crate::subscriber::SubscriberTable;

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
struct Stats {
    queued_tasks: AtomicU32,
    tasks_started_last_tick: AtomicU32,
}

#[derive(Debug)]
struct UnitSchedulerShared {
    subscriber_id: Uuid,
    stats: Stats,
    subscribers: Arc<SubscriberTable>,
    connections: Arc<SubscriberConnectionPool>,
}

#[derive(Debug)]
pub(crate) struct UnitScheduler {
    shared: Arc<UnitSchedulerShared>,
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

    fn spawn_task<'st>(&self, subscriber_id: Uuid, event: Box<Event>) {
        let shared = Arc::clone(&self.shared);
        shared.stats.queued_tasks.fetch_add(1, Ordering::Acquire);
        tokio::spawn(async move {
            let subscriber = shared.subscribers.get(subscriber_id)?.ok_or(ErrorKind::Unexpected)?;

            let body = ToJson(&event).to_string();
            let mut request = Request::new(body);
            let timestamp: chrono::DateTime<chrono::Utc> = SystemTime::now().into();
            timestamp_and_sign_request(timestamp.into(), subscriber.hmac_key(), &mut request);

            let result = shared.connections.connect(&subscriber_id).await;
            // Mark the task as started whether or not the connection succeeded
            shared.stats.tasks_started_last_tick.fetch_add(1, Ordering::Acquire);
            let mut connection = result?;

            todo!();

            Ok::<(), Box<Error>>(())
        });
    }
}
