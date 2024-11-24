use std::path::PathBuf;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct TestHarness {
    db_dir: Option<tempfile::TempDir>,
    db: Option<Arc<rocksdb::OptimisticTransactionDB>>,
}

impl TestHarness {
    pub(crate) fn new() -> Self {
        Default::default()
    }

    pub(crate) fn db(&mut self) -> Arc<rocksdb::OptimisticTransactionDB> {
        if let Some(db) = &self.db {
            return Arc::clone(db);
        }

        let system_temp_dir = std::env::temp_dir();
        let root = PathBuf::from(system_temp_dir).join("middleman");
        match std::fs::create_dir(&root) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {},
            _ => panic!(),
        }
        let temp_dir = tempfile::tempdir_in(&root).unwrap();

        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = Arc::new(rocksdb::OptimisticTransactionDB::open(&options, &temp_dir).unwrap());

        self.db = Some(Arc::clone(&db));
        self.db_dir = Some(temp_dir);

        db
    }
}
