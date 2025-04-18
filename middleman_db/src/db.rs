use std::path::Path;
use std::sync::Arc;

use byteview::ByteView;
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
            transaction_lock: TransactionLock::new(),
        })
    }

    pub fn path(&self) -> &Path {
        self.raw.path()
    }

    pub fn create_column_family(&mut self, descriptor: &impl ColumnFamilyDescriptor) -> Result<()> {
        self.raw
            .create_cf(descriptor.name(), &descriptor.options())?;
        Ok(())
    }

    pub fn get_column_family(self: &Arc<Self>, name: &str) -> Option<ColumnFamily> {
        Some(ColumnFamily {
            inner: OwningRef::new(Arc::clone(self))
                .try_map(|db| db.raw.cf_handle(name).ok_or(()))
                .ok()?,
            name: name.to_owned().into(),
        })
    }

    pub fn begin_transaction(self: &Arc<Self>) -> Transaction {
        Transaction::new(Arc::clone(self))
    }

    pub fn begin_transaction_locked(
        self: &Arc<Self>,
        cf: &ColumnFamily,
        key: impl Into<ByteView>,
    ) -> Result<Transaction> {
        Transaction::new_locked(Arc::clone(self), cf, key)
    }

    /// Prints all the key/value pairs in a table as a debugging aid.
    pub fn dump(&self, cf: &ColumnFamily) {
        struct ByteString<'a>(&'a [u8]);

        impl<'a> std::fmt::Display for ByteString<'a> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for &b in self.0.iter() {
                    match b {
                        0 => write!(f, "\\0")?,
                        b'\t' => write!(f, "\\t")?,
                        b'\r' => write!(f, "\\r")?,
                        b'\n' => write!(f, "\\n")?,
                        b'"' => write!(f, "\\\"")?,
                        b'\\' => write!(f, "\\\\")?,
                        0x20..0x7e => write!(f, "{}", b as char)?,
                        _ => write!(f, "\\x{b:02x}")?,
                    }
                }
                Ok(())
            }
        }

        for e in self.raw.iterator_cf(cf.raw(), rocksdb::IteratorMode::Start) {
            let (k, v) = e.unwrap();
            println!("\"{}\" => \"{}\"", ByteString(&k), ByteString(&v));
        }
    }
}
