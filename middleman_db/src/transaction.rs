use std::time::Duration;

use byteview::ByteView;
use dashmap::{DashMap, Entry};
use rocksdb::WriteBatchWithTransaction;
use std::time::Instant;

use crate::column_family::ColumnFamily;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::RawDb;

/// This is a lightweight lock that serves as a pessimistic transaction
/// conflict resolution mechanism. Attempting to acquire a lock held by an
/// existing transaction will simply fail.
///
/// To prevent resource leaks, locks will expire after a configurable timeout.
// TODO: Instead of an expiration, support deleting by transaction ID.
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
struct Locks<'db> {
    db: &'db Db,
    keys: Vec<ByteView>,
}

impl<'db> Drop for Locks<'db> {
    fn drop(&mut self) {
        for key in self.keys.iter() {
            self.db.transaction_lock.release(key);
        }
    }
}

/// An atomic DB transaction with optional locking.
// XXX: Instead of locking a single key, just have a "lock for update" method
pub struct Transaction<'db> {
    db: &'db Db,
    locks: Locks<'db>,
    write_batch: rocksdb::WriteBatchWithTransaction<true>,
}

impl<'db> Transaction<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self {
            db,
            write_batch: WriteBatchWithTransaction::new(),
            locks: Locks {
                db,
                keys: Vec::new(),
            },
        }
    }

    pub fn db(&self) -> &Db {
        self.locks.db
    }

    pub fn lock_key(&mut self, key: ByteView) -> Result<()> {
        if self.db().transaction_lock.acquire(key.clone()) {
            self.locks.keys.push(key);
            Ok(())
        } else {
            Err(Error::transaction_conflict())?
        }
    }

    pub(crate) fn get_cf(
        &self,
        cf: &ColumnFamily,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>> {
        Ok(self.db.raw.get_cf(cf.raw(), key)?)
    }

    pub(crate) fn put_cf(
        &mut self,
        cf: &ColumnFamily,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        self.write_batch.put_cf(cf.raw(), key, value);
    }

    pub(crate) fn put_cf_locked(
        &mut self,
        cf: &ColumnFamily,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.lock_key(key.as_ref().into())?;
        self.put_cf(&cf, key, value);
        Ok(())
    }

    pub(crate) fn raw_iterator_cf<'a>(
        &'a self,
        cf: &ColumnFamily,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'a, RawDb> {
        self.db.raw.raw_iterator_cf(cf.raw())
    }

    pub fn commit(self) -> Result<()> {
        Ok(self.db.raw.write(self.write_batch)?)
    }
}
