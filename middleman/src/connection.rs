// TODO: Support 3xx redirects by caching the redirect and invalidating
// connections
use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Notify;
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::Result;

pub trait Key: Clone + Eq + Hash + Debug + Send + Sync + 'static {}
pub trait Connection: Debug + Send + Sync + 'static {}

impl Key for Uuid {}

pub trait ConnectionFactory: Debug + Send + Sync + 'static {
    type Key: Key;
    type Connection: Connection;

    fn connect<'a>(
        &'a self,
        key: &'a Self::Key,
        keep_alive_secs: u16,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection>> + Send + 'a>>;
}

#[derive(Debug)]
pub struct ConnectionHandle<'pl, K: Key, C: Connection> {
    pool: &'pl Http11ConnectionPool<K, C>,
    key: K,
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
        // Return to idle pool
        let connection = unsafe { ManuallyDrop::take(&mut self.connection) };
        self.pool.push_idle(&self.key, connection);
    }
}

#[derive(Debug)]
struct IdleConnection<C> {
    idle_since: Instant,
    connection: Box<C>,
}

#[derive(Debug)]
struct PerHostPool<C: Connection> {
    max_connections: u16,
    /// How long to hold onto idle connections. Should be equal to the
    /// keepalive header value.
    idle_timeout: u16,
    total_connections: u16,
    idle_connections: VecDeque<IdleConnection<C>>,
    // XXX: This really shouldn't be behind a lock
    notify_idle: Arc<Notify>,
}

impl<C: Connection> PerHostPool<C> {
    fn idle_timeout(&self) -> Duration {
        // We subtract 2 sec since it's better to reap timed out connections
        // slightly early rather than too late
        Duration::from_secs(self.idle_timeout as u64 - 2)
    }
}

#[derive(Clone, Debug)]
pub struct Http11ConnectionPoolSettings {
    pub(crate) max_connections: u16,
    pub(crate) max_connections_per_host: u16,
    pub(crate) idle_timeout_seconds: u16,
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
#[derive(Debug)]
pub struct Http11ConnectionPool<K: Key, C: Connection> {
    settings: Http11ConnectionPoolSettings,
    connection_factory: Box<dyn ConnectionFactory<Key = K, Connection = C>>,
    hosts: DashMap<K, PerHostPool<C>>,
    // Notifies waiters that there is a free slot available
    notify_free: Notify,
    total_connections: AtomicU64,
}

unsafe impl<K: Key, C: Connection> Send for Http11ConnectionPool<K, C> {}
unsafe impl<K: Key, C: Connection> Sync for Http11ConnectionPool<K, C> {}

struct Guard<F: FnOnce()>(ManuallyDrop<F>);

impl<F: FnOnce()> Drop for Guard<F> {
    fn drop(&mut self) {
        unsafe { (ManuallyDrop::take(&mut self.0))() }
    }
}

fn guard<F: FnOnce()>(f: F) -> Guard<F> {
    Guard(ManuallyDrop::new(f))
}

impl<K: Key, C: Connection> Http11ConnectionPool<K, C> {
    pub fn new(
        settings: Http11ConnectionPoolSettings,
        connection_factory: Box<dyn ConnectionFactory<Key = K, Connection = C>>,
    ) -> Self {
        Self {
            settings,
            connection_factory,
            hosts: Default::default(),
            notify_free: Default::default(),
            total_connections: Default::default(),
        }
    }

    fn push_idle(&self, host: &K, connection: Box<C>) {
        let connection = IdleConnection {
            idle_since: Instant::now(),
            connection,
        };
        let mut pool = self.hosts.get_mut(host).unwrap();
        pool.idle_connections.push_back(connection);
        let notify = Arc::clone(&pool.notify_idle);
        drop(pool);
        notify.notify_one();
    }

    fn push_free(&self) {
        self.total_connections.fetch_sub(1, Ordering::Acquire);
        self.notify_free.notify_one();
    }

    fn allocate_free(&self) -> Option<()> {
        loop {
            let count = self.total_connections.load(Ordering::Relaxed);
            if count < self.settings.max_connections as u64 {
                if self
                    .total_connections
                    .compare_exchange_weak(count, count + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(());
                }
            } else {
                return None;
            }
        }
    }

    /// Acquires a lease on a connection. If an idle connection was acquired,
    /// it is returned. If a new lease was allocated, then returns `Ok(None)`.
    async fn acquire_lease<'a>(&self, key: &K) -> Result<Option<Box<C>>> {
        let key = key.clone();
        loop {
            let mut host_pool = self.hosts.entry(key.clone()).or_insert_with(|| PerHostPool {
                max_connections: self.settings.max_connections_per_host,
                idle_timeout: self.settings.idle_timeout_seconds,
                total_connections: 0,
                idle_connections: Default::default(),
                notify_idle: Default::default(),
            });

            // Try to acquire an idle connection
            if let Some(idle) = host_pool.idle_connections.pop_front() {
                return Ok(Some(idle.connection));
            }

            if host_pool.total_connections < host_pool.max_connections {
                // Try to allocate a new connection
                if let Some(()) = self.allocate_free() {
                    host_pool.total_connections += 1;
                    return Ok(None);
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

    pub async fn connect<'a>(&'a self, key: &'a K) -> Result<ConnectionHandle<'a, K, C>> {
        let connection = self.acquire_lease(key).await?;

        let keepalive = self.settings.idle_timeout_seconds;
        let connection = if let Some(connection) = connection {
            connection
        } else {
            let g = guard(|| self.push_free()); // Release if the connection fails
            let connection = self.connection_factory.connect(key, keepalive).await?;
            std::mem::forget(g);
            Box::new(connection)
        };

        Ok(ConnectionHandle {
            pool: self,
            key: key.clone(),
            connection: ManuallyDrop::new(connection),
        })
    }

    /// Recycles any idle connections that have outlived their timeout.
    pub fn recycle_idle(&self) {
        for mut host in self.hosts.iter_mut() {
            loop {
                let Some(ref idle) = host.idle_connections.front() else { break };
                let elapsed = Instant::now().duration_since(idle.idle_since);
                if elapsed < host.idle_timeout() {
                    // Because idle connections are ordered from oldest to
                    // newest, we can stop iterating early
                    break;
                }

                host.idle_connections.pop_front();
                host.total_connections -= 1;

                // Push to free pool and notify now that lock is released
                self.push_free();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use compact_str::CompactString;

    use crate::connection::{Http11ConnectionPool, Http11ConnectionPoolSettings};
    use crate::testing::{TestConnectionFactory, TestHarness};

    #[tokio::test(flavor = "current_thread")]
    async fn test_connect() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = CompactString::from("example.com:1234");
        let handle = pool.connect(&host).await.unwrap();
        assert_eq!(handle.host_string(), host);
        assert_eq!(handle.keep_alive_secs, 60);
        assert_eq!(handle.id, 0);
        drop(handle);

        // Connection is reused
        let handle = pool.connect(&host).await.unwrap();
        assert_eq!(handle.id, 0);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_idle_timeout() {
        let mut harness = TestHarness::new();
        let pool = harness.connection_pool();

        let host = CompactString::from("example.com:1234");
        let handle = pool.connect(&host).await.unwrap();
        let num_connections = Arc::clone(&handle.num_connections);
        assert_eq!(handle.id, 0);
        drop(handle);

        // No connections are recycled before timeout has passed
        pool.recycle_idle();
        tokio::task::yield_now().await;
        assert_eq!(pool.total_connections.load(Ordering::Relaxed), 1);
        assert_eq!(num_connections.load(Ordering::Relaxed), 1);

        tokio::time::advance(Duration::from_secs(120)).await;

        pool.recycle_idle();
        tokio::task::yield_now().await; // Allow destructor to run
        assert_eq!(pool.total_connections.load(Ordering::Relaxed), 0); // Free pool increased
        assert_eq!(num_connections.load(Ordering::Relaxed), 0); // Connection async destroyed

        let handle = pool.connect(&host).await.unwrap();
        assert_eq!(handle.id, 1);
        assert_eq!(pool.total_connections.load(Ordering::Relaxed), 1);
        assert_eq!(num_connections.load(Ordering::Relaxed), 1);
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
        let handle1 = pool.connect(&host1).await.unwrap();
        let _handle2 = pool.connect(&host2).await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        tokio::spawn(async move {
            pool2.connect(&CompactString::from("example.com:1234")).await.unwrap();
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
        let handle = pool.connect(&host).await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        let host2 = CompactString::from("example.com:4321");
        tokio::spawn(async move {
            pool2.connect(&host2).await.unwrap();
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
        let handle = pool.connect(&host).await.unwrap();

        let (send, recv) = std::sync::mpsc::channel::<()>();
        let pool2 = Arc::clone(&pool);
        tokio::spawn(async move {
            let host = CompactString::from("example.com:1234");
            pool2.connect(&host).await.unwrap();
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
