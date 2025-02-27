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
pub mod sequence;
#[cfg(test)]
pub mod testing;
pub mod transaction;

// FIXME: Have to replace OptimisticTransactionDB with DBWithTTL :'(
type RawDb = rocksdb::OptimisticTransactionDB<rocksdb::SingleThreaded>;

pub type Owned<T> = <T as ToOwned>::Owned;
pub type Owned2<T, U> = (<T as ToOwned>::Owned, <U as ToOwned>::Owned);

pub use accessor::Accessor;
pub use column_family::{ColumnFamily, ColumnFamilyDescriptor};
pub use cursor::Cursor;
pub use db::{Db, DbOptions};
pub use error::{Error, ErrorKind, Result};
pub use sequence::Sequence;
pub use transaction::Transaction;
