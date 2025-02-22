pub mod accessor;
pub mod big_tuple;
pub mod bytes;
pub mod column_family;
pub mod comparator;
pub mod cursor;
pub mod db;
pub mod error;
pub mod key;
pub mod model;
pub mod prefix;
#[cfg(test)]
pub mod testing;
pub mod transaction;

type RawDb = rocksdb::OptimisticTransactionDB<rocksdb::SingleThreaded>;

pub type Owned<T> = <T as ToOwned>::Owned;
pub type Owned2<T, U> = (<T as ToOwned>::Owned, <U as ToOwned>::Owned);
