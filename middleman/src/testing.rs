use std::path::PathBuf;
use std::sync::Arc;

use crate::config::Config;
use crate::types::Db;
use crate::Application;

#[derive(Default)]
pub(crate) struct TestHarness {
    db_dir: Option<tempfile::TempDir>,
    db: Option<Arc<Db>>,
    application: Option<Application>,
}

impl TestHarness {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn db_dir(&mut self) -> &std::path::Path {
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
    pub(crate) fn db(&mut self) -> &Arc<Db> {
        assert!(!self.application.is_some());
        let db_dir = self.db_dir().to_owned();
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        self.db = Some(Arc::new(Db::open(&options, &db_dir).unwrap()));
        self.db.as_ref().unwrap()
    }

    pub(crate) fn application(&mut self) -> &mut Application {
        assert!(!self.db.is_some());
        if self.application.is_some() {
            return self.application.as_mut().unwrap();
        }

        let db_dir = self.db_dir().to_owned();
        let config = Box::new(Config { db_dir });
        self.application = Some(Application::new(config).unwrap());

        self.application.as_mut().unwrap()
    }
}
