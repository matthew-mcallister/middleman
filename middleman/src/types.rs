use std::sync::Arc;

/// Binary key prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Prefix {
    Event = 0,
    EventIdempotencyKeyIndex = 1,
    EventStreamIndex = 2,
    Delivery = 3,
    Subscriber = 4,
    SubscriberTagIndex = 5,
}

pub(crate) type Db = rocksdb::OptimisticTransactionDB;
// XXX: Can we convert some uses of Transaction to WriteBatch?
pub(crate) type DbTransaction<'db> = rocksdb::Transaction<'db, Db>;
pub(crate) type DbColumnFamily = Arc<rocksdb::BoundColumnFamily<'static>>;
