use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use dashmap::DashMap;
use parking_lot::Mutex;
use rand::random;
use tokio::sync::Notify;
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::Result;

pub trait Connection: Send + Sync + 'static {}

pub trait ConnectionFactory {
    type Connection: Connection;

    fn connect(
        &self,
        host_string: &str,
        keep_alive_secs: u16,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection>> + Send>>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ConnectionPoolError {
    /// Occurs when the lease on a connection expires.
    LeaseExpired,
}

impl std::fmt::Display for ConnectionPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::LeaseExpired => write!(f, "lease expired"),
        }
    }
}

impl std::error::Error for ConnectionPoolError {}

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct HostKey {
    tag: Uuid,
    host_string: CompactString,
}

impl HostKey {
    fn new(tag: Uuid, host_string: impl Into<CompactString>) -> Self {
        Self {
            tag,
            host_string: host_string.into(),
        }
    }
}

/// Lease on a connection. Whatever task owns the lease on a connection has
/// exclusive access. The lease will expire after a timeout to prevent dead
/// threads from permanently squatting on connections.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Lease {
    /// Identifies the current owner of the lease
    // We use NonZeroU64 to get the free niche optimization
    id: NonZeroU64,
    /// Tracks lease lifespan for recycling
    acquired_at: Instant,
    host_key: HostKey,
}

impl Lease {
    fn new(host_key: HostKey) -> Self {
        Self {
            id: NonZeroU64::new(std::cmp::max(1, random::<u64>())).unwrap(),
            acquired_at: Instant::now(),
            host_key,
        }
    }

    fn held_duration(&self) -> Duration {
        Instant::now().duration_since(self.acquired_at)
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
struct Slot<C: Connection> {
    state: Mutex<SlotState>,
    connection: UnsafeCell<Option<Box<C>>>,
}

unsafe impl<C: Connection> Send for Slot<C> {}
unsafe impl<C: Connection> Sync for Slot<C> {}

#[derive(Clone, Debug)]
enum SlotState {
    Leased(Lease),
    IdleSince(Instant),
    Free,
}

impl Default for SlotState {
    fn default() -> Self {
        Self::Free
    }
}

impl SlotState {
    // Returns an idle state with the current timestamp
    fn idle() -> Self {
        Self::IdleSince(Instant::now())
    }

    fn lease(&self) -> Option<&Lease> {
        match self {
            Self::Leased(ref lease) => Some(lease),
            _ => None,
        }
    }

    fn lease_id(&self) -> Option<NonZeroU64> {
        self.lease().map(|l| l.id)
    }
}

impl<C: Connection> Default for Slot<C> {
    fn default() -> Self {
        Self {
            state: Default::default(),
            connection: Default::default(),
        }
    }
}

impl<C: Connection> Drop for Slot<C> {
    fn drop(&mut self) {
        drop(self.connection.get_mut().take());
    }
}

impl<C: Connection> Slot<C> {
    /// Unsafely disposes of the connection in this slot. This is unsafe as
    /// another thread may be using the connection when this occurs. The state
    /// lock should be acquired to prevent racing. The connection will be
    /// disposed of asynchronously.
    unsafe fn dispose_connection(&self) {
        let connection = (*self.connection.get()).take();
        // Fire the connection off into outer space
        // TODO: This makes the connection cap into a soft cap rather than a
        // hard cap, so maybe it should be done synchronously?
        tokio::spawn(async move { drop(connection) });
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
    notify_idle: Arc<Notify>,
}

impl PerHostPool {
    fn idle_timeout(&self) -> Duration {
        // We subtract 3 sec since it's better to reap timed out connections
        // slightly early rather than after it's too late
        Duration::from_secs(self.idle_timeout as u64 - 3)
    }
}

// Provides the destructor for ConnectionHandle
struct LeasedSlot<'pool, C: Connection> {
    pool: &'pool Http11ConnectionPool<C>,
    handle: SlotHandle,
}

impl<'pool, C: Connection> LeasedSlot<'pool, C> {
    fn slot(&self) -> &'pool Slot<C> {
        &self.pool.slots[self.handle.to_index()]
    }
}

impl<'pool, C: Connection> Drop for LeasedSlot<'pool, C> {
    fn drop(&mut self) {
        // Add to idle connection pool and release lease
        let mut state = self.slot().state.lock();
        let host = &state.lease().unwrap().host_key;
        let mut pool = self.pool.hosts.get_mut(host).unwrap();
        pool.idle_connections.push_back(self.handle);
        *state = SlotState::idle();
        drop(state);

        // Notify waiters after unlocking pool
        let notify = Arc::clone(&pool.notify_idle);
        drop(pool);
        notify.notify_one();
    }
}

/// Exclusive handle to a connection within the pool.
pub struct ConnectionHandle<'pool, C: Connection> {
    _slot: LeasedSlot<'pool, C>,
    connection: *mut C,
}

unsafe impl<'pool, C: Connection> Send for ConnectionHandle<'pool, C> {}
unsafe impl<'pool, C: Connection> Sync for ConnectionHandle<'pool, C> {}

impl<'pool, C: Connection> std::ops::Deref for ConnectionHandle<'pool, C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.connection }
    }
}

impl<'pool, C: Connection> std::ops::DerefMut for ConnectionHandle<'pool, C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.connection }
    }
}

#[derive(Clone, Debug)]
pub struct Http11ConnectionPoolSettings {
    max_connections: u16,
    max_connections_per_host: u16,
    idle_timeout_seconds: u16,
    lease_expiration_seconds: u16,
}

impl Default for Http11ConnectionPoolSettings {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_connections_per_host: 16,
            idle_timeout_seconds: 60,
            // It is the user's responsibility to set a response timeout less
            // than this value.
            lease_expiration_seconds: 300,
        }
    }
}

/// Connection pool for subscribers. Assumes a one-connection-per-request rule
/// for compatibility with HTTP/1.1. Supports global and per-host connection
/// limits.
pub struct Http11ConnectionPool<C: Connection> {
    settings: Http11ConnectionPoolSettings,
    connection_factory: Box<dyn ConnectionFactory<Connection = C>>,
    slots: Box<[Slot<C>]>,
    hosts: DashMap<HostKey, PerHostPool>,
    // Notifies waiters that there is a free slot available
    notify_free: Notify,
    free_slots: Mutex<VecDeque<SlotHandle>>,
}

unsafe impl<C: Connection> Send for Http11ConnectionPool<C> {}
unsafe impl<C: Connection> Sync for Http11ConnectionPool<C> {}

impl<C: Connection> Http11ConnectionPool<C> {
    pub fn new(
        settings: Http11ConnectionPoolSettings,
        connection_factory: Box<dyn ConnectionFactory<Connection = C>>,
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

    fn lease_expiration(&self) -> Duration {
        Duration::from_secs(self.settings.lease_expiration_seconds as u64)
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
        tag: Uuid,
        host_string: &'a str,
    ) -> impl Future<Output = Result<(NonZeroU64, SlotHandle)>> + Send + 'a {
        let host_key = HostKey::new(tag, host_string);
        async move {
            loop {
                let mut host_pool = self.hosts.entry(host_key.clone()).or_insert(PerHostPool {
                    max_connections: self.settings.max_connections_per_host,
                    idle_timeout: self.settings.idle_timeout_seconds,
                    total_connections: 0,
                    idle_connections: Default::default(),
                    notify_idle: Default::default(),
                });

                // Try to acquire an idle connection
                if let Some(handle) = host_pool.idle_connections.pop_front() {
                    let slot = &self.slots[handle.to_index()];
                    let lease = Lease::new(host_key);
                    let lease_id = lease.id;
                    *slot.state.lock() = SlotState::Leased(lease);
                    return Ok((lease_id, handle));
                }

                if host_pool.total_connections < host_pool.max_connections {
                    // Try to acquire a free slot
                    if let Some(handle) = self.free_slots.lock().pop_front() {
                        let slot = &self.slots[handle.to_index()];
                        let lease = Lease::new(host_key);
                        let lease_id = lease.id;
                        *slot.state.lock() = SlotState::Leased(lease);
                        host_pool.total_connections += 1;
                        return Ok((lease_id, handle));
                    }

                    // TODO: Stochastically steal an idle slot from another host pool
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
        tag: Uuid,
        host_string: &'b str,
    ) -> impl Future<Output = Result<ConnectionHandle<'a, C>>> + Send + 'b {
        async move {
            let (lease_id, handle) = self.acquire_lease(tag, host_string).await?;
            let leased_slot = LeasedSlot { pool: self, handle };

            let slot = &self.slots[handle.to_index()];
            let keepalive = self.settings.idle_timeout_seconds;

            {
                let state = slot.state.lock();
                if state.lease_id() != Some(lease_id) {
                    return Err(ConnectionPoolError::LeaseExpired.into());
                }
            }

            // safety: We hold the lock and the lease; access is exclusive
            let connection = unsafe { &mut *slot.connection.get() };
            if connection.is_none() {
                let inner = self.connection_factory.connect(host_string, keepalive).await?;

                // Double-check the lease in the unlikely event it expired
                // during .await
                let state = slot.state.lock();
                if state.lease_id() != Some(lease_id) {
                    return Err(ConnectionPoolError::LeaseExpired.into());
                }
                *connection = Some(Box::new(inner));
            }
            let connection = &mut **connection.as_mut().unwrap() as *mut C;

            Ok(ConnectionHandle {
                _slot: leased_slot,
                connection,
            })
        }
    }

    /// Recycles any idle connections that have outlived their timeout.
    pub fn recycle_idle(&self) {
        for mut host in self.hosts.iter_mut() {
            loop {
                let Some(&handle) = host.idle_connections.front() else { break };
                let slot = &self.slots[handle.to_index()];
                let mut state = slot.state.lock();
                let idle_since = match *state {
                    SlotState::Leased(_) | SlotState::Free => {
                        panic!("non-idle connection in idle pool")
                    },
                    SlotState::IdleSince(time) => time,
                };
                let elapsed = Instant::now().duration_since(idle_since);
                if elapsed < host.idle_timeout() {
                    // Stop scanning and go to next host pool
                    break;
                }

                // Dispose of connection and pop from host pool
                // safety: We hold the lock and the slot is unleased
                *state = SlotState::Free;
                unsafe { slot.dispose_connection() }
                host.idle_connections.pop_front();
                drop(state);

                // Push to free pool and notify now that lock is released
                self.push_free(handle);
            }
        }
    }

    /// Recycles connections which have expired leases. We assume that any
    /// connection that is held for longer than the lease duration is a leak.
    ///
    /// # Safety
    ///
    /// This method is unsafe if any of the connections with expired leases are
    /// still in use.
    // TODO: Instead of tracking time, just attach a reference ID and
    // invalidate by ID. This passes the actual leak detection to the caller.
    pub unsafe fn recycle_leaks(&self) {
        for (index, slot) in self.slots.iter().enumerate() {
            let mut state = slot.state.lock();
            if let Some(lease) = state.lease() {
                if lease.held_duration() < self.lease_expiration() {
                    continue;
                }
            } else {
                continue;
            }

            // Dispose of connection
            unsafe { slot.dispose_connection() }

            // Free the slot
            let host_key = match std::mem::take(&mut *state) {
                SlotState::Leased(lease) => lease.host_key,
                _ => unreachable!(),
            };
            drop(state);

            // Decrement connection quota
            self.hosts.get_mut(&host_key).unwrap().total_connections -= 1;

            // Add to free list and notify waiters
            self.push_free(index.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use uuid::{uuid, Uuid};

    use crate::connection::{Http11ConnectionPool, Http11ConnectionPoolSettings};
    use crate::testing::{TestConnectionFactory, TestHarness};

    const TAG: Uuid = uuid!("00000000-0000-0000-0000-000000000000");

    #[tokio::test(flavor = "current_thread")]
    async fn test_connect() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = "example.com:1234";
        let handle = pool.connect(TAG, host).await.unwrap();
        assert_eq!(handle.host_string(), host);
        assert_eq!(handle.keep_alive_secs, 60);
        assert_eq!(handle.id, 0);
        drop(handle);

        // Connection is reused
        let handle = pool.connect(TAG, host).await.unwrap();
        assert_eq!(handle.id, 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_idle_timeout() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = "example.com:1234";
        let handle = pool.connect(TAG, host).await.unwrap();
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

        let handle = pool.connect(TAG, host).await.unwrap();
        assert_eq!(handle.id, 1);
        assert_eq!(pool.free_slots.lock().len(), 255);
        assert_eq!(num_connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_expired_lease() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = "example.com:1234";
        let handle = pool.connect(TAG, host).await.unwrap();
        let num_connections = Arc::clone(&handle.num_connections);
        // Leak the handle
        std::mem::forget(handle);

        // Slot is not reclaimed prematurely
        unsafe { pool.recycle_leaks() };
        tokio::task::yield_now().await;
        assert_eq!(pool.free_slots.lock().len(), 255);
        assert_eq!(num_connections.load(Ordering::Relaxed), 1);

        // Slot is reclaimed, connection destroyed
        tokio::time::advance(Duration::from_secs(301)).await;
        unsafe { pool.recycle_leaks() };
        tokio::task::yield_now().await;
        assert_eq!(pool.free_slots.lock().len(), 256);
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
        let handle1 = pool.connect(TAG, "example.com:1234").await.unwrap();
        let _handle2 = pool.connect(TAG, "example.com:4321").await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        tokio::spawn(async move {
            pool2.connect(TAG, "example.com:1234").await.unwrap();
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

        let handle = pool.connect(TAG, "example.com:1234").await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        tokio::spawn(async move {
            pool2.connect(TAG, "example.com:4321").await.unwrap();
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

        let handle = pool.connect(TAG, "example.com:1234").await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        tokio::spawn(async move {
            pool2.connect(TAG, "example.com:1234").await.unwrap();
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

    // Per-host connection quota is isolated by tag
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_tag_isolation() {
        let pool = Arc::new(Http11ConnectionPool::new(
            Http11ConnectionPoolSettings {
                max_connections_per_host: 1,
                ..Default::default()
            },
            Box::new(TestConnectionFactory::default()),
        ));

        let tag_2 = uuid!("00000000-0000-0000-0000-000000000001");
        let _handle1 = pool.connect(TAG, "example.com:1234").await.unwrap();
        let _handle2 = pool.connect(tag_2, "example.com:1234").await.unwrap();
    }
}
