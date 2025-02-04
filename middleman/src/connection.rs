use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU64};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use rand::random;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::error::DynResult;

pub trait Connection: Send + Sync + 'static {
    fn host_string(&self) -> impl AsRef<str>;
}

pub trait ConnectionFactory {
    type Connection: Connection;

    fn connect(
        &self,
        host_string: &str,
        keep_alive_secs: u16,
    ) -> Pin<Box<dyn Future<Output = DynResult<Self::Connection>>>>;
}

#[derive(Clone, Copy, Debug)]
enum ConnectionPoolError {
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
        unsafe { Self(NonZeroU16::new_unchecked((value + 1) as u16)) }
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Lease {
    /// Identifies the current owner of the lease
    // We use NonZeroU64 to get the free niche optimization
    id: NonZeroU64,
    /// Tracks lease lifespan for recycling
    acquired_at: Instant,
}

impl Lease {
    fn new() -> Self {
        Self {
            id: NonZeroU64::new(std::cmp::max(1, random::<u64>())).unwrap(),
            acquired_at: Instant::now(),
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

#[derive(Clone, Copy, Debug)]
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
        std::mem::drop(self.connection.get_mut().take());
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
        tokio::spawn(async move { std::mem::drop(connection) });
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
        // We subtract 1s since it's better to reap timed out connections
        // slightly early rather than slightly late
        Duration::from_secs(self.idle_timeout as u64 - 1)
    }
}

/// Exclusive handle to a connection within the pool.
pub struct ConnectionHandle<'pool, C: Connection> {
    pool: &'pool Http11ConnectionPool<C>,
    connection: *mut C,
    handle: SlotHandle,
}

impl<'pool, C: Connection> ConnectionHandle<'pool, C> {
    fn slot(&self) -> &'pool Slot<C> {
        &self.pool.slots[self.handle.to_index()]
    }
}

// XXX: This deref impl isn't *truly* safe. If the lease is held past its
// expiration, then the connection could be destroyed by another thread while
// we are still using it, the premise being that a connection will never be
// held past the lease expiration unless the owner is dead.
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

impl<'pool, C: Connection> Drop for ConnectionHandle<'pool, C> {
    fn drop(&mut self) {
        let host = self.host_string();

        // Add to idle connection pool and release lease
        let mut pool = self.pool.hosts.get_mut(host.as_ref()).unwrap();
        pool.idle_connections.push_back(self.handle);
        *self.slot().state.lock() = SlotState::idle();

        // Notify waiters after unlocking pool
        let notify = Arc::clone(&pool.notify_idle);
        std::mem::drop(pool);
        notify.notify_one();
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
            max_connections_per_host: 8,
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
    hosts: DashMap<String, PerHostPool>,
    // Notifies waiters that there is a free slot available
    notify_free: Notify,
    free_slots: Mutex<VecDeque<SlotHandle>>,
}

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
    async fn acquire_lease(&self, host_string: &str) -> DynResult<(NonZeroU64, SlotHandle)> {
        loop {
            let mut host_pool = self.hosts.entry(host_string.to_owned()).or_insert(PerHostPool {
                max_connections: self.settings.max_connections_per_host,
                idle_timeout: self.settings.idle_timeout_seconds,
                total_connections: 0,
                idle_connections: Default::default(),
                notify_idle: Default::default(),
            });

            // Try to acquire an idle connection
            if let Some(handle) = host_pool.idle_connections.pop_front() {
                let slot = &self.slots[handle.to_index()];
                let lease = Lease::new();
                *slot.state.lock() = SlotState::Leased(lease);
                return Ok((lease.id, handle));
            }

            if host_pool.total_connections < host_pool.max_connections {
                // Try to acquire a free slot
                if let Some(handle) = self.free_slots.lock().pop_front() {
                    let slot = &self.slots[handle.to_index()];
                    let lease = Lease::new();
                    *slot.state.lock() = SlotState::Leased(lease);
                    host_pool.total_connections += 1;
                    return Ok((lease.id, handle));
                }

                // TODO: Stochastically steal an idle slot from another host pool

                // Wait for a free slot to become available
                std::mem::drop(host_pool);
                self.notify_free.notified().await;
            } else {
                // Wait for an idle connection
                debug_assert_eq!(host_pool.total_connections, host_pool.max_connections);
                let notify = Arc::clone(&host_pool.notify_idle);
                std::mem::drop(host_pool);
                notify.notified().await;
            }

            // Loop will try again after we are notified
        }
    }

    pub async fn connect<'a>(&'a self, host_string: &str) -> DynResult<ConnectionHandle<'a, C>> {
        let (lease_id, handle) = self.acquire_lease(host_string).await?;
        let slot = &self.slots[handle.to_index()];
        let keepalive = self.settings.idle_timeout_seconds;

        let state = slot.state.lock();
        if state.lease().map(|l| l.id) != Some(lease_id) {
            return Err(ConnectionPoolError::LeaseExpired.into());
        }

        // safety: We have hold the lock and the lease; access is exclusive
        let connection = unsafe { &mut *slot.connection.get() };
        if connection.is_none() {
            let inner = self.connection_factory.connect(host_string, keepalive).await?;
            *connection = Some(Box::new(inner));
        }
        let connection = &mut **connection.as_mut().unwrap() as *mut C;

        Ok(ConnectionHandle {
            pool: self,
            connection,
            handle,
        })
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
                *state = SlotState::Free;
                unsafe { slot.dispose_connection() }
                host.idle_connections.pop_front();
                std::mem::drop(state);

                // Push to free pool and notify now that lock is released
                self.push_free(handle);
            }
        }
    }

    /// Recycles connections which have expired leases. We assume that any
    /// connection that is held for longer than the lease duration is a leak.
    pub async fn recycle_leaks(&self) {
        for (index, slot) in self.slots.iter().enumerate() {
            let state = slot.state.lock();
            if let Some(lease) = state.lease() {
                if lease.held_duration() < self.lease_expiration() {
                    continue;
                }
            }

            // Dispose of connection
            let host_string = unsafe {
                let cxn = (*slot.connection.get()).as_ref().unwrap();
                cxn.host_string().as_ref().to_owned()
            };
            unsafe { slot.dispose_connection() }
            std::mem::drop(state);

            // Decrement connection quota
            self.hosts.get_mut(&host_string).unwrap().total_connections -= 1;

            // Add to free list and notify waiters
            self.push_free(index.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::Connection;
    use crate::testing::{TestConnection, TestHarness};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn smoke_test() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = "example.com:1234";
        let handle = pool.connect(host).await.unwrap();
        assert_eq!(handle.host_string().as_ref(), host);
        assert_eq!(handle.keep_alive_secs, 60);
        let pointer = &*handle as *const TestConnection;
        std::mem::drop(handle);

        // Connection is reused
        let handle = pool.connect(host).await.unwrap();
        assert_eq!(pointer, &*handle as *const TestConnection);
    }
}
