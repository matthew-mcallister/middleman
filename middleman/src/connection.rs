use std::collections::VecDeque;
use std::future::Future;
use std::hash::Hash;
use std::mem::ManuallyDrop;
use std::num::{NonZeroU16, NonZeroU64};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::error::Result;

pub trait Key: Clone + Eq + Hash + Send + Sync + 'static {}
pub trait Connection: Send + Sync + 'static {}

pub trait ConnectionFactory {
    type Key: Key;
    type Connection: Connection;

    fn connect(
        &self,
        key: &Self::Key,
        keep_alive_secs: u16,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection>> + Send>>;
}

/// Index into the pool of slots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SlotHandle(NonZeroU16);

impl From<usize> for SlotHandle {
    fn from(value: usize) -> SlotHandle {
        Self(NonZeroU16::new((value + 1) as u16).unwrap())
    }
}

impl From<SlotHandle> for usize {
    fn from(SlotHandle(value): SlotHandle) -> usize {
        u16::from(value) as usize - 1
    }
}

impl SlotHandle {
    fn to_index(self) -> usize {
        self.into()
    }
}

/// Lease on a connection. Whatever task owns the lease on a connection has
/// exclusive access. The lease will expire after a timeout to prevent dead
/// threads from permanently squatting on connections.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Lease<K> {
    /// Identifies the current owner of the lease
    // We use NonZeroU64 to get the free niche optimization
    id: NonZeroU64,
    key: K,
}

impl<K> Lease<K> {
    fn new(id: NonZeroU64, key: K) -> Self {
        Self { id, key }
    }
}

/// A slot can be in one of three states:
///
/// 1. Free
/// 2. Leased
/// 3. Idle
///
/// A slot is free until it is leased and a connection is opened. After the
/// lease is released, it will be recycled to the idle pool for that host so
/// the connection can be reused. Once the idle timeout ends, the connection
/// will be closed and the slot will be recycled to the free pool.
///
/// When a slot is leased, the owner of the lease has exclusive access to the
/// connection, regardless of whether the slot is locked or not. If the lease
/// is not released before it expires, it is assumed that the owning task is
/// dead and the connection is invalid, and the slot will be recycled to the
/// free pool.
enum Slot<K, C> {
    Leased(Lease<K>),
    IdleSince(Instant, Box<C>),
    Free,
}

impl<K, C> Default for Slot<K, C> {
    fn default() -> Self {
        Self::Free
    }
}

impl<K, C> Slot<K, C> {
    // Returns an idle state with the current timestamp
    fn idle(connection: Box<C>) -> Self {
        Self::IdleSince(Instant::now(), connection)
    }

    fn lease(&self) -> Option<&Lease<K>> {
        match self {
            Self::Leased(ref lease) => Some(lease),
            _ => None,
        }
    }

    fn lease_id(&self) -> Option<NonZeroU64> {
        self.lease().map(|l| l.id)
    }
}

#[derive(Debug)]
struct PerHostPool {
    max_connections: u16,
    /// How long to hold onto idle connections. Should be equal to the
    /// keepalive header value.
    idle_timeout: u16,
    total_connections: u16,
    idle_connections: VecDeque<SlotHandle>,
    // XXX: This really shouldn't be behind a lock
    notify_idle: Arc<Notify>,
}

impl PerHostPool {
    fn idle_timeout(&self) -> Duration {
        // We subtract 2 sec since it's better to reap timed out connections
        // slightly early rather than too late
        Duration::from_secs(self.idle_timeout as u64 - 2)
    }
}

// Provides the destructor for ConnectionHandle
struct LeasedSlot<'pool, K: Key, C: Connection> {
    id: NonZeroU64,
    pool: &'pool Http11ConnectionPool<K, C>,
    handle: SlotHandle,
}

impl<'pool, K: Key, C: Connection> LeasedSlot<'pool, K, C> {
    fn slot(&self) -> &'pool Mutex<Slot<K, C>> {
        &self.pool.slots[self.handle.to_index()]
    }

    fn release(&mut self, connection: Option<Box<C>>) {
        if let Some(c) = connection {
            let mut slot = self.slot().lock();
            match slot.lease() {
                // Lease was already reclaimed; nothing to do
                None => return,
                Some(lease) if lease.id != self.id => return,
                // We still have the lease
                _ => {},
            };

            let old_state = std::mem::replace(&mut *slot, Slot::idle(c));
            drop(slot);

            // Return to idle pool
            let key = &old_state.lease().unwrap().key;
            self.pool.push_idle(key, self.handle);
        } else {
            *self.slot().lock() = Slot::Free;
            self.pool.push_free(self.handle);
        };
    }
}

#[repr(transparent)]
struct TmpLeasedSlot<'pool, K: Key, C: Connection>(LeasedSlot<'pool, K, C>);

impl<'pool, K: Key, C: Connection> Drop for TmpLeasedSlot<'pool, K, C> {
    fn drop(&mut self) {
        self.0.release(None);
    }
}

impl<'pool, K: Key, C: Connection> TmpLeasedSlot<'pool, K, C> {
    fn into_inner(self) -> LeasedSlot<'pool, K, C> {
        // XXX: Is this really the only way to implement into_inner...?
        unsafe { std::mem::transmute(self) }
    }
}

/// Exclusive handle to a connection within the pool.
pub struct ConnectionHandle<'pool, K: Key, C: Connection> {
    slot: LeasedSlot<'pool, K, C>,
    connection: ManuallyDrop<Box<C>>,
}

impl<'pool, K: Key, C: Connection> std::ops::Deref for ConnectionHandle<'pool, K, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &*self.connection
    }
}

impl<'pool, K: Key, C: Connection> std::ops::DerefMut for ConnectionHandle<'pool, K, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.connection
    }
}

impl<'pool, K: Key, C: Connection> Drop for ConnectionHandle<'pool, K, C> {
    fn drop(&mut self) {
        let cxn = unsafe { ManuallyDrop::take(&mut self.connection) };
        self.slot.release(Some(cxn));
    }
}

#[derive(Clone, Debug)]
pub struct Http11ConnectionPoolSettings {
    max_connections: u16,
    max_connections_per_host: u16,
    idle_timeout_seconds: u16,
}

impl Default for Http11ConnectionPoolSettings {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_connections_per_host: 16,
            idle_timeout_seconds: 60,
        }
    }
}

/// Connection pool for subscribers. Assumes a one-connection-per-request rule
/// for compatibility with HTTP/1.1. Supports global and per-host connection
/// limits.
pub struct Http11ConnectionPool<K, C: Connection> {
    settings: Http11ConnectionPoolSettings,
    connection_factory: Box<dyn ConnectionFactory<Key = K, Connection = C>>,
    slots: Box<[Mutex<Slot<K, C>>]>,
    hosts: DashMap<K, PerHostPool>,
    // Notifies waiters that there is a free slot available
    notify_free: Notify,
    free_slots: Mutex<VecDeque<SlotHandle>>,
}

unsafe impl<K: Send, C: Connection> Send for Http11ConnectionPool<K, C> {}
unsafe impl<K: Sync, C: Connection> Sync for Http11ConnectionPool<K, C> {}

impl<K: Key, C: Connection> Http11ConnectionPool<K, C> {
    pub fn new(
        settings: Http11ConnectionPoolSettings,
        connection_factory: Box<dyn ConnectionFactory<Key = K, Connection = C>>,
    ) -> Self {
        let free_slots: VecDeque<SlotHandle> =
            (0..settings.max_connections as usize).map(Into::into).collect();
        let mut slots = Vec::new();
        slots.resize_with(settings.max_connections as _, Default::default);
        Self {
            slots: slots.into_boxed_slice(),
            settings,
            connection_factory,
            hosts: Default::default(),
            notify_free: Default::default(),
            free_slots: Mutex::new(free_slots),
        }
    }

    fn push_idle(&self, host: &K, handle: SlotHandle) {
        let mut pool = self.hosts.get_mut(host).unwrap();
        pool.idle_connections.push_back(handle);
        let notify = Arc::clone(&pool.notify_idle);
        drop(pool);
        notify.notify_one();
    }

    fn push_free(&self, handle: SlotHandle) {
        self.free_slots.lock().push_back(handle);
        self.notify_free.notify_one();
    }

    /// Acquires a lease on a slot. Newly allocated slots will not have a
    /// connection attached. The caller is responsible for creating and
    /// assigning a connection.
    fn acquire_lease<'a>(
        &'a self,
        lease_id: NonZeroU64,
        key: &'a K,
    ) -> impl Future<Output = Result<(SlotHandle, Slot<K, C>)>> + Send + 'a {
        let key = key.clone();
        async move {
            loop {
                let mut host_pool = self.hosts.entry(key.clone()).or_insert(PerHostPool {
                    max_connections: self.settings.max_connections_per_host,
                    idle_timeout: self.settings.idle_timeout_seconds,
                    total_connections: 0,
                    idle_connections: Default::default(),
                    notify_idle: Default::default(),
                });

                // Try to acquire an idle connection
                if let Some(handle) = host_pool.idle_connections.pop_front() {
                    let slot = &self.slots[handle.to_index()];
                    let lease = Lease::new(lease_id, key);
                    let old_slot = std::mem::replace(&mut *slot.lock(), Slot::Leased(lease));
                    return Ok((handle, old_slot));
                }

                if host_pool.total_connections < host_pool.max_connections {
                    // Try to acquire a free slot
                    if let Some(handle) = self.free_slots.lock().pop_front() {
                        let slot = &self.slots[handle.to_index()];
                        let lease = Lease::new(lease_id, key);
                        let old_slot = std::mem::replace(&mut *slot.lock(), Slot::Leased(lease));
                        host_pool.total_connections += 1;
                        return Ok((handle, old_slot));
                    }
                }

                if host_pool.total_connections < host_pool.max_connections
                    && host_pool.total_connections == 0
                {
                    // Wait for a free slot to become available
                    drop(host_pool);
                    self.notify_free.notified().await;
                } else {
                    // Wait for an idle connection
                    let notify = Arc::clone(&host_pool.notify_idle);
                    drop(host_pool);
                    notify.notified().await;
                }

                // Loop will try again after we are notified
            }
        }
    }

    pub fn connect<'a: 'b, 'b>(
        &'a self,
        lease_id: NonZeroU64,
        key: &'a K,
    ) -> impl Future<Output = Result<ConnectionHandle<'a, K, C>>> + Send + 'b {
        async move {
            let (handle, old_slot) = self.acquire_lease(lease_id, key).await?;
            let leased_slot = TmpLeasedSlot(LeasedSlot {
                id: lease_id,
                pool: self,
                handle,
            });

            let keepalive = self.settings.idle_timeout_seconds;
            let connection = match old_slot {
                Slot::Leased(_) => unreachable!(),
                Slot::IdleSince(_, connection) => connection,
                Slot::Free => Box::new(self.connection_factory.connect(key, keepalive).await?),
            };

            Ok(ConnectionHandle {
                slot: leased_slot.into_inner(),
                connection: ManuallyDrop::new(connection),
            })
        }
    }

    /// Recycles any idle connections that have outlived their timeout.
    pub fn recycle_idle(&self) {
        for mut host in self.hosts.iter_mut() {
            loop {
                let Some(&handle) = host.idle_connections.front() else { break };
                let slot = &self.slots[handle.to_index()];
                let mut state = slot.lock();
                let idle_since = match *state {
                    Slot::Leased(_) | Slot::Free => {
                        panic!("non-idle connection in idle pool")
                    },
                    Slot::IdleSince(time, _) => time,
                };
                let elapsed = Instant::now().duration_since(idle_since);
                if elapsed < host.idle_timeout() {
                    // Because idle connections are ordered from oldest to
                    // newest, we can stop iterating early
                    break;
                }

                *state = Slot::Free;
                host.idle_connections.pop_front();
                host.total_connections -= 1;
                drop(state);

                // Push to free pool and notify now that lock is released
                self.push_free(handle);
            }
        }
    }

    // Unused code for hypothetical future leak detection
    #[allow(dead_code)]
    pub fn recycle_by_id(&self, lease_id: NonZeroU64) {
        for (index, slot) in self.slots.iter().enumerate() {
            let mut state = slot.lock();
            if state.lease_id() != Some(lease_id) {
                continue;
            }

            // Free the slot
            let key = match std::mem::take(&mut *state) {
                Slot::Leased(lease) => lease.key,
                _ => unreachable!(),
            };
            drop(state);

            // Decrement connection quota
            self.hosts.get_mut(&key).unwrap().total_connections -= 1;

            // Add to free list and notify waiters
            self.push_free(index.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use compact_str::CompactString;

    use crate::connection::{Http11ConnectionPool, Http11ConnectionPoolSettings};
    use crate::testing::{TestConnectionFactory, TestHarness};

    fn random_u64() -> NonZeroU64 {
        loop {
            let x: u64 = rand::random();
            if let Ok(x) = NonZeroU64::try_from(x) {
                return x;
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_connect() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = CompactString::from("example.com:1234");
        let handle = pool.connect(random_u64(), &host).await.unwrap();
        assert_eq!(handle.host_string(), host);
        assert_eq!(handle.keep_alive_secs, 60);
        assert_eq!(handle.id, 0);
        drop(handle);

        // Connection is reused
        let handle = pool.connect(random_u64(), &host).await.unwrap();
        assert_eq!(handle.id, 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_idle_timeout() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = CompactString::from("example.com:1234");
        let handle = pool.connect(random_u64(), &host).await.unwrap();
        let num_connections = Arc::clone(&handle.num_connections);
        assert_eq!(handle.id, 0);
        drop(handle);

        // No slots are recycled before timeout has passed
        pool.recycle_idle();
        tokio::task::yield_now().await;
        assert_eq!(pool.free_slots.lock().len(), 255);
        assert_eq!(num_connections.load(Ordering::Relaxed), 1);

        tokio::time::advance(Duration::from_secs(120)).await;

        pool.recycle_idle();
        tokio::task::yield_now().await; // Allow destructor to run
        assert_eq!(pool.free_slots.lock().len(), 256); // Slot returned to free pool
        assert_eq!(num_connections.load(Ordering::Relaxed), 0); // Connection async destroyed

        let handle = pool.connect(random_u64(), &host).await.unwrap();
        assert_eq!(handle.id, 1);
        assert_eq!(pool.free_slots.lock().len(), 255);
        assert_eq!(num_connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_recycle_by_id() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = CompactString::from("example.com:1234");
        let lease_id = NonZeroU64::new(1).unwrap();
        let handle = pool.connect(lease_id, &host).await.unwrap();
        let num_connections = Arc::clone(&handle.num_connections);

        // Slot is reclaimed
        pool.recycle_by_id(lease_id);
        tokio::task::yield_now().await;
        assert_eq!(pool.free_slots.lock().len(), 256);
        drop(handle);
        assert_eq!(num_connections.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_max_host_connections() {
        let pool = Arc::new(Http11ConnectionPool::new(
            Http11ConnectionPoolSettings {
                max_connections_per_host: 1,
                ..Default::default()
            },
            Box::new(TestConnectionFactory::default()),
        ));

        // We can allocate one handle for each host
        let host1 = CompactString::from("example.com:1234");
        let host2 = CompactString::from("example.com:4321");
        let handle1 = pool.connect(random_u64(), &host1).await.unwrap();
        let _handle2 = pool.connect(random_u64(), &host2).await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        tokio::spawn(async move {
            pool2.connect(random_u64(), &CompactString::from("example.com:1234")).await.unwrap();
            send.send(()).unwrap();
        });

        // Task is waiting
        tokio::task::yield_now().await;
        assert!(recv.try_recv().is_err());

        // Task is completed
        drop(handle1);
        tokio::task::yield_now().await;
        assert_eq!(recv.try_recv().unwrap(), ());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_max_connections() {
        let pool = Arc::new(Http11ConnectionPool::new(
            Http11ConnectionPoolSettings {
                max_connections: 1,
                ..Default::default()
            },
            Box::new(TestConnectionFactory::default()),
        ));

        let host = CompactString::from("example.com:1234");
        let handle = pool.connect(random_u64(), &host).await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        let host2 = CompactString::from("example.com:4321");
        tokio::spawn(async move {
            pool2.connect(random_u64(), &host2).await.unwrap();
            send.send(()).unwrap();
        });

        // Connection is idle but task is waiting
        drop(handle);
        tokio::task::yield_now().await;
        assert!(recv.try_recv().is_err());

        // Have to wait for idle connection to time out
        tokio::time::advance(Duration::from_secs(301)).await;
        pool.recycle_idle();

        // Task is completed
        tokio::task::yield_now().await;
        assert_eq!(recv.try_recv().unwrap(), ());
    }

    // Same as test_max_connections but acquire an idle connection instead of a
    // free slot
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_max_connections_wait_idle() {
        let pool = Arc::new(Http11ConnectionPool::new(
            Http11ConnectionPoolSettings {
                max_connections: 1,
                ..Default::default()
            },
            Box::new(TestConnectionFactory::default()),
        ));

        let host = CompactString::from("example.com:1234");
        let handle = pool.connect(random_u64(), &host).await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        tokio::spawn(async move {
            let host = CompactString::from("example.com:1234");
            pool2.connect(random_u64(), &host).await.unwrap();
            send.send(()).unwrap();
        });

        // Task is waiting
        tokio::task::yield_now().await;
        assert!(recv.try_recv().is_err());

        // Task is completed
        drop(handle);
        tokio::task::yield_now().await;
        assert_eq!(recv.try_recv().unwrap(), ());
    }
}
