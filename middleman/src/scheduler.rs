use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use uuid::Uuid;

use crate::connection::Http11ConnectionPool;
use crate::error::Error;
use crate::event::Event;
use crate::Application;

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
pub(crate) struct UnitScheduler {
    connections: Arc<Http11ConnectionPool<Uuid, >>,
    stats: Arc<Stats>,
    subscriber_id: Uuid,
    tasks_started_history: RingBuffer<u32, 3>,
}

impl UnitScheduler {
    fn update_stats(&mut self) {
        let tasks_started = self.stats.tasks_started_last_tick.swap(0, Ordering::Relaxed);
        self.tasks_started_history.push(tasks_started);
        self.stats.queued_tasks.fetch_sub(tasks_started, Ordering::Relaxed);
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
        let num_queued = self.stats.queued_tasks.load(Ordering::Relaxed);
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
        let stats = Arc::clone(&self.stats);
        let app = Arc::clone(&self.app);

        stats.queued_tasks.fetch_add(1, Ordering::Acquire);
        app.

        tokio::spawn(async move {
            let lease_id = rand::random();
            let result = app.connections.connect(lease_id, &subscriber_id).await;
            // Mark the task as started whether or not the connection succeeded
            stats.tasks_started_last_tick.fetch_add(1, Ordering::Acquire);
            let mut connection = result?;

            let content = serde_json::to_string(&event);
            // TODO: Sign the request
            let signature = b"123456789abcdef";

            Ok::<(), Error>(())
        });
        todo!()
    }
}
