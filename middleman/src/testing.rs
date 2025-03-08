use std::future::Future;
use std::net::IpAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use compact_str::CompactString;

use crate::config::Config;
use crate::connection::{Connection, ConnectionFactory, Http11ConnectionPool, Key};
use crate::error::Result;
use crate::Application;

#[derive(Default)]
pub struct TestHarness {
    db_dir: Option<tempfile::TempDir>,
    application: Option<Application>,
    connection_pool: Option<Http11ConnectionPool<CompactString, TestConnection>>,
}

impl TestHarness {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn db_dir(&mut self) -> &std::path::Path {
        if self.db_dir.is_some() {
            return self.db_dir.as_ref().map(|d| d.path()).unwrap();
        }

        // Create temp dir
        let system_temp_dir = std::env::temp_dir();
        let root = PathBuf::from(system_temp_dir).join("middleman");
        match std::fs::create_dir(&root) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {},
            _ => panic!(),
        }

        self.db_dir = Some(tempfile::tempdir_in(&root).unwrap());
        self.db_dir.as_ref().map(|d| d.path()).unwrap()
    }

    pub fn application(&mut self) -> &mut Application {
        if self.application.is_some() {
            return self.application.as_mut().unwrap();
        }

        let db_dir = self.db_dir().to_owned();
        let config = Box::new(Config {
            db_dir,
            host: IpAddr::from_str("127.0.0.1").unwrap(),
            port: Default::default(),
        });
        self.application = Some(Application::new(config).unwrap());

        self.application.as_mut().unwrap()
    }

    pub fn connection_pool(&mut self) -> &mut Http11ConnectionPool<CompactString, TestConnection> {
        if self.connection_pool.is_some() {
            return self.connection_pool.as_mut().unwrap();
        }

        let pool = Http11ConnectionPool::new(
            Default::default(),
            Box::new(TestConnectionFactory::default()),
        );
        self.connection_pool = Some(pool);
        self.connection_pool.as_mut().unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct TestConnection {
    pub num_connections: Arc<AtomicU64>,
    pub id: u64,
    pub host: CompactString,
    pub keep_alive_secs: u16,
}

#[derive(Debug, Default)]
pub struct TestConnectionFactory {
    // Also doubles as total number of connections created
    pub id: AtomicU64,
    pub num_connections: Arc<AtomicU64>,
}

impl TestConnection {
    pub fn host_string(&self) -> &str {
        &self.host[..]
    }
}

impl Drop for TestConnection {
    fn drop(&mut self) {
        self.num_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Key for CompactString {}
impl Connection for TestConnection {}

impl ConnectionFactory for TestConnectionFactory {
    type Key = CompactString;
    type Connection = TestConnection;

    fn connect(
        &self,
        key: &Self::Key,
        keep_alive_secs: u16,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Connection>> + Send>> {
        let num_connections = Arc::clone(&self.num_connections);
        num_connections.fetch_add(1, Ordering::Relaxed);
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let host = key.clone();
        Box::pin(async move {
            Ok(TestConnection {
                num_connections,
                id,
                host,
                keep_alive_secs,
            })
        })
    }
}
