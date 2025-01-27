use std::sync::Arc;

pub type Owned<T> = <T as ToOwned>::Owned;
pub type Owned2<T, U> = (<T as ToOwned>::Owned, <U as ToOwned>::Owned);

pub(crate) type Db = rocksdb::OptimisticTransactionDB;
// XXX: Can we convert some uses of Transaction to WriteBatch?
pub(crate) type DbTransaction<'db> = rocksdb::Transaction<'db, Db>;
pub(crate) type DbColumnFamily = Arc<rocksdb::BoundColumnFamily<'static>>;
