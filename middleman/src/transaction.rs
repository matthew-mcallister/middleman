use std::time::Duration;

use byteview::ByteView;
use dashmap::{DashMap, Entry};
use rocksdb::WriteBatchWithTransaction;
use tokio::time::Instant;

use crate::error::{ErrorKind, Result};
use crate::types::{Db, DbColumnFamily};
use crate::Application;

/// This is a lightweight lock that serves as a pessimistic transaction
/// conflict resolution mechanism. Attempting to acquire a lock held by an
/// existing transaction will simply fail.
///
/// To prevent resource leaks, locks will expire after a configurable timeout.
// TODO: Unified resource leak detection
#[derive(Debug)]
pub(crate) struct TransactionLock {
    lock_expiration: std::time::Duration,
    keys: DashMap<ByteView, Instant>,
}

impl TransactionLock {
    pub(crate) fn new(lock_expiration: Duration) -> Self {
        Self {
            lock_expiration,
            keys: Default::default(),
        }
    }

    pub(crate) fn acquire(&self, key: ByteView) -> bool {
        match self.keys.entry(key) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                e.insert(Instant::now());
                true
            },
        }
    }

    pub(crate) fn release(&self, key: &ByteView) {
        // FIXME: Lock could be held by a different transaction if it leaked
        self.keys.remove(key);
    }

    pub(crate) fn recycle_leaks(&self) {
        let now = Instant::now();
        self.keys.retain(|_, v| now.duration_since(*v) < self.lock_expiration);
    }
}

// Wrapper to handle destructor
struct Locks<'app> {
    app: &'app Application,
    keys: Vec<ByteView>,
}

impl<'app> Drop for Locks<'app> {
    fn drop(&mut self) {
        for key in self.keys.iter() {
            self.app.transaction_lock.release(key);
        }
    }
}

/// An atomic DB transaction with optional locking.
// XXX: Instead of locking a single key, just have a "lock for update" method
pub(crate) struct Transaction<'app> {
    db: &'app Db,
    locks: Locks<'app>,
    write_batch: rocksdb::WriteBatchWithTransaction<true>,
}

impl<'app> Transaction<'app> {
    pub(crate) fn new(app: &'app Application) -> Self {
        Self {
            db: &app.db,
            write_batch: WriteBatchWithTransaction::new(),
            locks: Locks {
                app,
                keys: Vec::new(),
            },
        }
    }

    pub(crate) fn app(&self) -> &Application {
        self.locks.app
    }

    pub(crate) fn lock_key(&mut self, key: ByteView) -> Result<()> {
        if self.app().transaction_lock.acquire(key.clone()) {
            self.locks.keys.push(key);
            Ok(())
        } else {
            Err(ErrorKind::Busy)?
        }
    }

    pub(crate) fn get_cf(
        &self,
        cf: &DbColumnFamily,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(&**cf, key)?)
    }

    pub(crate) fn put_cf(
        &mut self,
        cf: &DbColumnFamily,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        self.write_batch.put_cf(&**cf, key, value);
    }

    pub(crate) fn put_cf_locked(
        &mut self,
        cf: &DbColumnFamily,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.lock_key(key.as_ref().into())?;
        self.put_cf(&cf, key, value);
        Ok(())
    }

    pub(crate) fn raw_iterator_cf<'a>(
        &'a self,
        cf: &DbColumnFamily,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'a, Db> {
        self.db.raw_iterator_cf(&**cf)
    }

    pub(crate) fn commit(self) -> Result<()> {
        Ok(self.db.write(self.write_batch)?)
    }
}
