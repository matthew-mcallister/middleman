use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;

use crate::db::{Db, DbOptions};

#[derive(Default)]
pub struct TestDb {
    db_dir: Option<TempDir>,
    db: Option<Arc<Db>>,
}

impl TestDb {
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

    pub fn db(&mut self) -> &mut Arc<Db> {
        if self.db.is_some() {
            return self.db.as_mut().unwrap();
        }

        let db_dir = self.db_dir().to_owned();
        let mut options = DbOptions::default();
        options.create_if_missing = true;
        let empty: &[(&str, &rocksdb::Options, rocksdb::ColumnFamilyTtl)] = &[];
        self.db = Some(Arc::new(
            Db::open(&db_dir, &options, empty.iter().cloned()).unwrap(),
        ));

        self.db.as_mut().unwrap()
    }
}
