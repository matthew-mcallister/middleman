use std::sync::Arc;

use crate::error::DynResult;
use crate::types::Db;
use crate::util::ByteCast;

pub struct CfAccessor {
    cf: rocksdb::ColumnFamily,
    db: Arc<Db>,
}

/// # Safety
/// 
/// Most methods here are unsafe due to the ability to cast trap values. The
/// application has to ensure that keys are accessed with appropriate type
/// information.
impl CfAccessor {
    pub unsafe fn get<K: AsRef<[u8]>, V: ByteCast>(&self, key: K) -> DynResult<Option<Box<V>>> {
        match self.db.get_cf(&self.cf, &key)? {
            Some(bytes) => Ok(Some(<V as ByteCast>::from_bytes_owned(bytes))),
            None => Ok(None),
        }
    }

    pub fn put<K: AsRef<[u8]>, V: ByteCast>(&self, key: K, value: &V) -> DynResult<()> {
        // This exposes potentially uninitialized bytes, but they will only be
        // memcpy'd by RocksDB.
        let bytes: &[u8] = unsafe { std::mem::transmute(ByteCast::as_bytes(value)) };
        Ok(self.db.put_cf(&self.cf, &key, bytes)?)
    }

    pub unsafe fn iter_keys_by_prefix<P: AsRef<[u8]>, K: ByteCast>(
        &self,
        prefix: P,
    ) -> impl Iterator<DynResult<Box<K>>> {
        let options: rocksdb::ReadOptions = Default::default();
        options.set_prefix_same_as_start(true);
        let mut raw = self.db.raw_iterator_cf_opt(&self.cf, options);
        unimplemented!()
    }
}
