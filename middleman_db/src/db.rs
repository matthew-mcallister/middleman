use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use owning_ref::OwningRef;

use crate::column_family::{ColumnFamily, ColumnFamilyDescriptor};
use crate::error::Result;
use crate::transaction::TransactionLock;
use crate::{RawDb, Transaction};

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct DbOptions {
    pub create_if_missing: bool,
}

impl<'a> From<&'a DbOptions> for rocksdb::Options {
    fn from(value: &'a DbOptions) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(value.create_if_missing);
        options
    }
}

#[derive(Debug)]
pub struct Db {
    pub(crate) raw: RawDb,
    pub(crate) transaction_lock: TransactionLock,
}

impl Db {
    pub fn open(
        dir: &Path,
        options: &DbOptions,
        column_families: impl IntoIterator<Item = impl ColumnFamilyDescriptor>,
    ) -> Result<Self> {
        let iter = column_families.into_iter().map(|desc| {
            rocksdb::ColumnFamilyDescriptor::new_with_ttl(desc.name(), desc.options(), desc.ttl())
        });
        let raw = RawDb::open_cf_descriptors(&options.into(), dir, iter)?;

        Ok(Self {
            raw,
            transaction_lock: TransactionLock::new(Duration::from_secs(300)),
        })
    }

    pub fn path(&self) -> &Path {
        self.raw.path()
    }

    pub fn create_column_family(&mut self, descriptor: &impl ColumnFamilyDescriptor) -> Result<()> {
        self.raw.create_cf(descriptor.name(), &descriptor.options())?;
        Ok(())
    }

    pub fn get_column_family(self: &Arc<Self>, name: &str) -> Option<ColumnFamily> {
        Some(ColumnFamily(
            OwningRef::new(Arc::clone(self)).try_map(|db| db.raw.cf_handle(name).ok_or(())).ok()?,
        ))
    }

    pub fn begin_transaction(&self) -> Transaction<'_> {
        Transaction::new(self)
    }
}
