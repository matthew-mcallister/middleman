use crate::bytes::{AsRawBytes, OwnedFromBytesUnchecked};
use crate::column_family::ColumnFamily;
use crate::cursor::BaseCursor;
use crate::db::Db;
use crate::error::Result;
use crate::transaction::Transaction;
use std::marker::PhantomData;

pub struct Accessor<
    'db,
    K: OwnedFromBytesUnchecked + AsRawBytes + ToOwned + ?Sized + 'static,
    V: OwnedFromBytesUnchecked + ToOwned + ?Sized + 'static,
> {
    cf: &'db ColumnFamily,
    _k: PhantomData<*const K>,
    _v: PhantomData<*const V>,
}

impl<
        'db,
        K: OwnedFromBytesUnchecked + AsRawBytes + ?Sized + 'static,
        V: OwnedFromBytesUnchecked + AsRawBytes + ?Sized + 'static,
    > Accessor<'db, K, V>
{
    pub fn new(cf: &'db ColumnFamily) -> Self {
        Self {
            cf,
            _k: Default::default(),
            _v: Default::default(),
        }
    }

    fn db(&self) -> &'db Db {
        self.cf.db()
    }

    pub unsafe fn get_unchecked(&self, key: &K) -> Result<Option<Box<V>>> {
        let key = unsafe { key.as_bytes_unchecked() };
        match self.db().raw.get_cf(self.cf.raw(), key)? {
            Some(bytes) => Ok(Some(V::box_from_bytes_unchecked(bytes))),
            None => Ok(None),
        }
    }

    pub unsafe fn get_txn_unchecked(
        &self,
        txn: &Transaction<'_>,
        key: &K,
    ) -> Result<Option<Box<V>>> {
        let key = unsafe { key.as_bytes_unchecked() };
        match txn.get_cf(self.cf, key)? {
            Some(bytes) => Ok(Some(V::box_from_bytes_unchecked(bytes))),
            None => Ok(None),
        }
    }

    pub fn put(&self, key: &K, value: &V) -> Result<()> {
        // This is technically UB; the rocksdb crate requires initialized bytes
        // though uninitialized bytes should be safe
        let key = unsafe { key.as_bytes_unchecked() };
        let value = unsafe { value.as_bytes_unchecked() };
        Ok(self.db().raw.put_cf(self.cf.raw(), key, value)?)
    }

    pub fn put_txn(&self, txn: &mut Transaction<'_>, key: &K, value: &V) {
        let key = unsafe { key.as_bytes_unchecked() };
        let value = unsafe { value.as_bytes_unchecked() };
        txn.put_cf(self.cf, key, value);
    }

    pub fn put_txn_locked(&self, txn: &mut Transaction<'_>, key: &K, value: &V) -> Result<()> {
        let key = unsafe { key.as_bytes_unchecked() };
        let value = unsafe { value.as_bytes_unchecked() };
        txn.put_cf_locked(self.cf, key, value)
    }

    pub unsafe fn cursor_unchecked(&self) -> BaseCursor<'db, K, V> {
        BaseCursor::<'_, K, V>::new(self.db().raw.raw_iterator_cf(self.cf.raw()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::accessor::Accessor;
    use crate::column_family::ColumnFamily;
    use crate::key::{BigEndianU16, FiniteString, Packed2};
    use crate::testing::TestDb;

    fn cf() -> ColumnFamily {
        let mut harness = TestDb::new();
        let db = harness.db();
        Arc::get_mut(db)
            .unwrap()
            .create_column_family(&("cf", &Default::default(), Default::default()))
            .unwrap();
        db.get_column_family("cf").unwrap()
    }

    #[test]
    fn test_get_put() {
        type Key = Packed2<BigEndianU16, FiniteString<4>>;
        type Value = (u32, i16, [u8; 2]);

        let cf = cf();
        let accessor = Accessor::<Key, Value>::new(&cf);

        let key1: Key = (1.into(), FiniteString::try_from("blah").unwrap()).into();
        let value1: Value = (1, 2, [3, 4]);
        accessor.put(&key1, &value1).unwrap();

        let key2: Key = (2.into(), FiniteString::try_from("bluh").unwrap()).into();
        let value2: Value = (4, 3, [2, 1]);
        accessor.put(&key2, &value2).unwrap();

        unsafe {
            assert_eq!(*accessor.get_unchecked(&key1).unwrap().unwrap(), value1);
            assert_eq!(*accessor.get_unchecked(&key2).unwrap().unwrap(), value2);
        }

        accessor.put(&key1, &value2).unwrap();
        unsafe {
            assert_eq!(*accessor.get_unchecked(&key1).unwrap().unwrap(), value2);
        }
    }

    #[test]
    fn test_iter_by_prefix() {
        type Key = Packed2<BigEndianU16, FiniteString<4>>;
        type Value = u32;

        let cf = cf();
        let accessor = Accessor::<Key, Value>::new(&cf);

        let key1: Key = (1.into(), FiniteString::try_from("blah").unwrap()).into();
        let value1: Value = 1;
        accessor.put(&key1, &value1).unwrap();

        let key2: Key = (2.into(), FiniteString::try_from("blah").unwrap()).into();
        let value2: Value = 2;
        accessor.put(&key2, &value2).unwrap();

        let key3: Key = (2.into(), FiniteString::try_from("bluh").unwrap()).into();
        let value3: Value = 3;
        accessor.put(&key3, &value3).unwrap();

        let prefix = 1u16.to_be_bytes();
        let mut iter =
            unsafe { accessor.cursor_unchecked().prefix_iter::<[u8; 2]>(prefix).into_iter() };
        assert_eq!(iter.next().unwrap().unwrap(), (key1, value1));
        let next = iter.next();
        assert!(next.is_none());

        let prefix = 2u16.to_be_bytes();
        let mut iter =
            unsafe { accessor.cursor_unchecked().prefix_iter::<[u8; 2]>(prefix).into_iter() };
        assert_eq!(iter.next().unwrap().unwrap(), (key2, value2));
        assert_eq!(iter.next().unwrap().unwrap(), (key3, value3));
        assert!(iter.next().is_none());
    }
}
