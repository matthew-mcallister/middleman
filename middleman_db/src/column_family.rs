use std::ops::Deref;
use std::sync::Arc;

use owning_ref::OwningRef;

use crate::db::Db;

#[derive(Clone)]
pub struct ColumnFamily(pub(crate) OwningRef<Arc<Db>, rocksdb::ColumnFamily>);

impl Deref for ColumnFamily {
    type Target = rocksdb::ColumnFamily;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl ColumnFamily {
    pub fn db(&self) -> &Db {
        self.0.as_owner()
    }
}

/// Identifies and defines a column family.
pub trait ColumnFamilyDescriptor {
    /// Unique name
    fn name(&self) -> &str;

    /// Column family options
    fn options(&self) -> rocksdb::Options;

    /// Time-to-live
    fn ttl(&self) -> rocksdb::ColumnFamilyTtl;
}

impl<'a> ColumnFamilyDescriptor for (&'a str, &'a rocksdb::Options, rocksdb::ColumnFamilyTtl) {
    fn name(&self) -> &str {
        &self.0
    }

    fn options(&self) -> rocksdb::Options {
        self.1.clone()
    }

    fn ttl(&self) -> rocksdb::ColumnFamilyTtl {
        self.2
    }
}
