use std::sync::Arc;

use owning_ref::OwningRef;
use serde_derive::{Deserialize, Serialize};
use strum_macros::{IntoStaticStr, VariantNames};

// XXX: Support other content types
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ContentType {
    #[serde(rename = "application/json")]
    Json,
}

impl std::fmt::Display for ContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                ContentType::Json => "application/json",
            }
        )
    }
}

pub type Owned<T> = <T as ToOwned>::Owned;
pub type Owned2<T, U> = (<T as ToOwned>::Owned, <U as ToOwned>::Owned);

pub(crate) type Db = rocksdb::OptimisticTransactionDB<rocksdb::SingleThreaded>;
pub(crate) type DbColumnFamily = OwningRef<Arc<Db>, rocksdb::ColumnFamily>;

#[derive(Clone, Copy, Debug, Eq, IntoStaticStr, PartialEq, VariantNames)]
pub(crate) enum ColumnFamilyName {
    Deliveries,
    Events,
    EventTagIdempotencyKeyIndex,
    EventTagStreamIndex,
    Subscribers,
}
