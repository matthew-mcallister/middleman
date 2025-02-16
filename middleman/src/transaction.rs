use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

use byteview::ByteView;
use parking_lot::Mutex;
use rocksdb::WriteBatchWithTransaction;
use tokio::time::Instant;

use crate::error::{ErrorKind, Result};
use crate::types::{Db, DbColumnFamily};
use crate::Application;

// Align to half a cache line to reduce false sharing.
#[derive(Debug)]
#[repr(align(32))]
struct Shard {
    /// Map transaction IDs to acquisition time. Acquisition time is only
    /// needed to perform leak detection.
    transactions: Mutex<HashMap<ByteView, Instant>>,
    lock_expiration: std::time::Duration,
}

#[derive(Debug)]
pub(crate) struct LockGuard<'a> {
    transaction_id: ByteView,
    shard: &'a Shard,
}

impl<'a> Drop for LockGuard<'a> {
    fn drop(&mut self) {
        self.shard.transactions.lock().remove(&self.transaction_id);
    }
}

impl Shard {
    fn new(lock_expiration: Duration) -> Self {
        Self {
            transactions: Default::default(),
            lock_expiration,
        }
    }

    fn acquire<'a>(self: &'a Self, transaction_id: ByteView) -> Option<LockGuard<'a>> {
        let mut transactions = self.transactions.lock();
        match transactions.entry(transaction_id.clone()) {
            Entry::Occupied(_) => None,
            Entry::Vacant(e) => {
                e.insert(Instant::now());
                Some(LockGuard {
                    transaction_id,
                    shard: self,
                })
            },
        }
    }

    fn recycle_leaks(&self) {
        let now = Instant::now();
        self.transactions.lock().retain(|_, v| now.duration_since(*v) < self.lock_expiration);
    }
}

const NUM_SHARDS: usize = 8;

/// This is a lightweight lock that serves as a pessimistic transaction
/// conflict resolution mechanism. Attempting to acquire a lock held by an
/// existing transaction will simply fail.
///
/// To prevent resource leaks, locks will expire after a configurable timeout.
// XXX: Instead of reimplementing leak detection in multiple places, maybe
// register them inside a "session" struct and do leak detection on that.
// This is basically how a database does resource management with connections.
#[derive(Debug)]
pub(crate) struct TransactionLock {
    shards: [Shard; NUM_SHARDS],
}

impl TransactionLock {
    pub(crate) fn new(lock_expiration: Duration) -> Self {
        Self {
            shards: std::array::from_fn(|_| Shard::new(lock_expiration)),
        }
    }

    /// Acquires a transaction lock. Any other transaction which try to acquire
    /// this key while it is locked will fail.
    pub(crate) fn acquire(&self, key: ByteView) -> Option<LockGuard<'_>> {
        let mut hasher: DefaultHasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let shard_index = hash as usize % NUM_SHARDS;
        self.shards[shard_index].acquire(key)
    }

    pub(crate) fn recycle_leaks(&self) {
        self.shards.iter().for_each(Shard::recycle_leaks);
    }
}

/// An atomic DB transaction with optional locking.
// XXX: Instead of locking a single key, just have a "lock for update" method
pub(crate) struct Transaction<'t> {
    pub(crate) app: &'t Application,
    pub(crate) db: &'t Db,
    pub(crate) write_batch: rocksdb::WriteBatchWithTransaction<true>,
    pub(crate) guards: Vec<LockGuard<'t>>,
}

impl<'t> Transaction<'t> {
    pub(crate) fn new(app: &'t Application) -> Self {
        Self {
            db: &app.db,
            app,
            write_batch: WriteBatchWithTransaction::new(),
            guards: Vec::new(),
        }
    }

    pub(crate) fn lock_key(&mut self, key: ByteView) -> Result<()> {
        let guard = self.app.transaction_lock.acquire(key).ok_or(ErrorKind::Busy)?;
        self.guards.push(guard);
        Ok(())
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
