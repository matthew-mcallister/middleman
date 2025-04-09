use std::marker::PhantomData;

use bytecast::{box_from_bytes, FromBytes, IntoBytes};

use crate::column_family::ColumnFamily;
use crate::cursor::BaseCursor;
use crate::db::Db;
use crate::error::Result;
use crate::transaction::Transaction;

pub struct Accessor<
    'db,
    K: FromBytes + IntoBytes + ToOwned + ?Sized + 'static,
    V: FromBytes + IntoBytes + ToOwned + ?Sized + 'static,
> {
    cf: &'db ColumnFamily,
    _k: PhantomData<*const K>,
    _v: PhantomData<*const V>,
}

impl<
        'db,
        K: FromBytes + IntoBytes + ToOwned + ?Sized + 'static,
        V: FromBytes + IntoBytes + ToOwned + ?Sized + 'static,
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

    pub fn get(&self, key: &K) -> Result<Option<Box<V>>> {
        let key = key.as_bytes();
        match self.db().raw.get_cf(self.cf.raw(), key)? {
            Some(bytes) => Ok(Some(box_from_bytes(bytes.into_boxed_slice()).unwrap())),
            None => Ok(None),
        }
    }

    pub fn get_txn(&self, txn: &Transaction, key: &K) -> Result<Option<Box<V>>> {
        let key = key.as_bytes();
        match txn.get_cf(self.cf, key)? {
            Some(bytes) => Ok(Some(box_from_bytes(bytes.into_boxed_slice()).unwrap())),
            None => Ok(None),
        }
    }

    pub fn put(&self, key: &K, value: &V) -> Result<()> {
        // This is technically UB; the rocksdb crate requires initialized bytes
        // though uninitialized bytes should be safe
        let key = key.as_bytes();
        let value = value.as_bytes();
        Ok(self.db().raw.put_cf(self.cf.raw(), key, value)?)
    }

    pub fn put_txn(&self, txn: &mut Transaction, key: &K, value: &V) {
        let key = key.as_bytes();
        let value = value.as_bytes();
        txn.put_cf(self.cf, key, value);
    }

    pub fn put_txn_locked(&self, txn: &mut Transaction, key: &K, value: &V) -> Result<()> {
        let key = key.as_bytes();
        let value = value.as_bytes();
        txn.put_cf_locked(self.cf, key, value)
    }

    pub fn delete_txn(&self, txn: &mut Transaction, key: &K) {
        let key = key.as_bytes();
        txn.delete(self.cf, key);
    }

    // XXX: Take initial seek location as a parameter?
    pub fn cursor(&self) -> BaseCursor<'db, K, V> {
        BaseCursor::<'_, K, V>::new(self.db().raw.raw_iterator_cf(self.cf.raw()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::accessor::Accessor;
    use crate::column_family::ColumnFamily;
    use crate::key::{BigEndianU16, Packed2, Packed3};
    use crate::packed;
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
        type Key = Packed2<BigEndianU16, [u8; 4]>;
        type Value = Packed3<u32, i16, [u8; 2]>;

        let cf = cf();
        let accessor = Accessor::<Key, Value>::new(&cf);

        let key1: Key = packed!(1.into(), *b"blah");
        let value1: Value = packed!(1, 2, [3, 4]);
        accessor.put(&key1, &value1).unwrap();

        let key2: Key = packed!(2.into(), *b"blah");
        let value2: Value = packed!(4, 3, [2, 1]);
        accessor.put(&key2, &value2).unwrap();

        assert_eq!(*accessor.get(&key1).unwrap().unwrap(), value1);
        assert_eq!(*accessor.get(&key2).unwrap().unwrap(), value2);

        accessor.put(&key1, &value2).unwrap();
        assert_eq!(*accessor.get(&key1).unwrap().unwrap(), value2);
    }

    #[test]
    fn test_iter_by_prefix() {
        type Key = Packed2<BigEndianU16, [u8; 4]>;
        type Value = u32;

        let cf = cf();
        let accessor = Accessor::<Key, Value>::new(&cf);

        let key1: Key = (1.into(), *b"blah").into();
        let value1: Value = 1;
        accessor.put(&key1, &value1).unwrap();

        let key2: Key = (2.into(), *b"blah").into();
        let value2: Value = 2;
        accessor.put(&key2, &value2).unwrap();

        let key3: Key = (2.into(), *b"bluh").into();
        let value3: Value = 3;
        accessor.put(&key3, &value3).unwrap();

        let prefix = 1u16.to_be_bytes();
        let mut iter = accessor.cursor().prefix_iter::<[u8; 2]>(prefix).into_iter();
        assert_eq!(iter.next().unwrap().unwrap(), (key1, value1));
        let next = iter.next();
        assert!(next.is_none());

        let prefix = 2u16.to_be_bytes();
        let mut iter = accessor.cursor().prefix_iter::<[u8; 2]>(prefix).into_iter();
        assert_eq!(iter.next().unwrap().unwrap(), (key2, value2));
        assert_eq!(iter.next().unwrap().unwrap(), (key3, value3));
        assert!(iter.next().is_none());
    }
}
