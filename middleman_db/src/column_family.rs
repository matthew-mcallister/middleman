use std::borrow::Cow;
use std::sync::Arc;

use owning_ref::OwningRef;

use crate::db::Db;
use crate::error::Result;

#[derive(Clone)]
pub struct ColumnFamily {
    pub(crate) inner: OwningRef<Arc<Db>, rocksdb::ColumnFamily>,
    pub(crate) name: Arc<String>,
}

impl ColumnFamily {
    pub fn db(&self) -> &Arc<Db> {
        self.inner.as_owner()
    }

    pub fn raw(&self) -> &rocksdb::ColumnFamily {
        &*self.inner
    }

    pub fn name(&self) -> &Arc<String> {
        &self.name
    }

    pub(crate) fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        Ok(self.db().raw.get_cf(self.raw(), key.as_ref())?)
    }

    pub(crate) fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        Ok(self.db().raw.put_cf(self.raw(), key, value)?)
    }
}

impl std::fmt::Debug for ColumnFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamily")
            .field("owner", &self.inner.as_owner())
            .field("reference", &"<rocksdb::ColumnFamily>")
            .finish()
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
