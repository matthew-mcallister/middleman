use std::sync::Arc;

use byteview::ByteView;
use dashmap::{DashMap, Entry};
use rocksdb::WriteBatchWithTransaction;
use std::time::Instant;

use crate::column_family::ColumnFamily;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::RawDb;

type LockKey = (Arc<String>, ByteView);

/// This is a lightweight lock that serves as a pessimistic transaction
/// conflict resolution mechanism. Attempting to acquire a lock held by an
/// existing transaction will simply fail.
///
/// To prevent resource leaks, locks will expire after a configurable timeout.
#[derive(Debug)]
pub(crate) struct TransactionLock {
    keys: DashMap<LockKey, Instant>,
}

impl TransactionLock {
    pub(crate) fn new() -> Self {
        Self {
            keys: Default::default(),
        }
    }

    pub(crate) fn acquire(&self, key: LockKey) -> bool {
        match self.keys.entry(key) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                e.insert(Instant::now());
                true
            },
        }
    }

    pub(crate) fn release(&self, key: &LockKey) {
        // FIXME: Lock could be held by a different transaction if it leaked
        self.keys.remove(key);
    }
}

// Wrapper to handle destructor
struct Locks {
    db: Arc<Db>,
    keys: Vec<LockKey>,
}

impl Drop for Locks {
    fn drop(&mut self) {
        for key in self.keys.iter() {
            self.db.transaction_lock.release(key);
        }
    }
}

/// An atomic DB transaction with optional locking.
pub struct Transaction {
    locks: Locks,
    write_batch: rocksdb::WriteBatchWithTransaction<true>,
}

impl Transaction {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            write_batch: WriteBatchWithTransaction::new(),
            locks: Locks {
                db,
                keys: Vec::new(),
            },
        }
    }

    pub fn db(&self) -> &Db {
        &self.locks.db
    }

    pub fn lock_key(&mut self, cf: &ColumnFamily, key: impl Into<ByteView>) -> Result<()> {
        let key = (Arc::clone(&cf.name), key.into());
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
        Ok(self.db().raw.get_cf(cf.raw(), key)?)
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
        // XXX: Ideally there would be a CowByteView type that is either
        // &'a [u8] or ByteView which we can convert to.
        key: impl Into<ByteView>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let key = key.into();
        self.lock_key(cf, key.clone())?;
        self.put_cf(&cf, &key[..], value);
        Ok(())
    }

    pub(crate) fn delete(&mut self, cf: &ColumnFamily, key: impl AsRef<[u8]>) {
        self.write_batch.delete_cf(cf.raw(), key.as_ref());
    }

    pub(crate) fn raw_iterator_cf<'a>(
        &'a self,
        cf: &ColumnFamily,
    ) -> rocksdb::DBRawIteratorWithThreadMode<'a, RawDb> {
        self.db().raw.raw_iterator_cf(cf.raw())
    }

    pub fn commit(self) -> Result<()> {
        let Self { write_batch, locks } = self;
        Ok(locks.db.raw.write(write_batch)?)
    }
}
