/// Binary key prefix.
#[derive(Clone, Copy, Debug)]
pub enum Prefix {
    Event = 0,
    EventIdempotencyKeyIndex = 1,
    EventStreamIndex = 2,
    Delivery = 3,
    Subscriber = 4,
}

pub(crate) type DbTransaction<'db> = rocksdb::Transaction<'db, rocksdb::OptimisticTransactionDB>;

// XXX: Enforce that both of these are valid UUIDs. This way, we are
// guaranteed to be able to format them as UUIDs later. Version 8 UUIDs
// are totally allowed if the application wants to use e.g. int IDs.
pub type Tag = u128;
pub type IdempotencyKey = u128;
