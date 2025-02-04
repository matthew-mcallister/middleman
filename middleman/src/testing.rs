use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use crate::config::Config;
use crate::connection::{Connection, ConnectionFactory, Http11ConnectionPool};
use crate::error::DynResult;
use crate::types::Db;
use crate::Application;

#[derive(Default)]
pub struct TestHarness {
    db_dir: Option<tempfile::TempDir>,
    db: Option<Arc<Db>>,
    application: Option<Application>,
    connection_pool: Option<Http11ConnectionPool<TestConnection>>,
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

    /// Minimal DB instance for low-level tests
    pub fn db(&mut self) -> &Arc<Db> {
        assert!(!self.application.is_some());
        let db_dir = self.db_dir().to_owned();
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        self.db = Some(Arc::new(Db::open(&options, &db_dir).unwrap()));
        self.db.as_ref().unwrap()
    }

    pub fn application(&mut self) -> &mut Application {
        assert!(!self.db.is_some());
        if self.application.is_some() {
            return self.application.as_mut().unwrap();
        }

        let db_dir = self.db_dir().to_owned();
        let config = Box::new(Config { db_dir });
        self.application = Some(Application::new(config).unwrap());

        self.application.as_mut().unwrap()
    }

    pub fn connection_pool(&mut self) -> &mut Http11ConnectionPool<TestConnection> {
        if self.connection_pool.is_some() {
            return self.connection_pool.as_mut().unwrap();
        }

        let pool = Http11ConnectionPool::new(Default::default(), Box::new(TestConnectionFactory));
        self.connection_pool = Some(pool);
        self.connection_pool.as_mut().unwrap()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TestConnection {
    pub host: String,
    pub keep_alive_secs: u16,
}

pub struct TestConnectionFactory;

impl Connection for TestConnection {
    fn host_string(&self) -> impl AsRef<str> {
        &self.host[..]
    }
}

impl ConnectionFactory for TestConnectionFactory {
    type Connection = TestConnection;

    fn connect(
        &self,
        host_string: &str,
        keep_alive_secs: u16,
    ) -> Pin<Box<dyn Future<Output = DynResult<Self::Connection>> + 'static>> {
        let host = host_string.to_owned();
        Box::pin(async move {
            Ok(TestConnection {
                host,
                keep_alive_secs,
            })
        })
    }
}
