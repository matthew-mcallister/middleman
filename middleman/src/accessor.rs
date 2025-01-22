use std::borrow::Borrow;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::bytes::{AsRawBytes, FromBytesUnchecked};
use crate::error::DynResult;
use crate::prefix::IsPrefixOf;
use crate::types::{Db, DbTransaction};

pub struct CfAccessor<
    'db,
    K: AsRawBytes + ToOwned + ?Sized + 'static,
    V: FromBytesUnchecked + ToOwned + ?Sized + 'static,
> {
    db: &'db Db,
    cf: &'db Arc<rocksdb::BoundColumnFamily<'db>>,
    _k: PhantomData<*const K>,
    _v: PhantomData<*const V>,
}

type Owned<T> = <T as ToOwned>::Owned;
type Owned2<T, U> = (<T as ToOwned>::Owned, <U as ToOwned>::Owned);

impl<
        'db,
        K: FromBytesUnchecked + AsRawBytes + ToOwned + ?Sized + 'static,
        V: FromBytesUnchecked + AsRawBytes + ToOwned + ?Sized + 'static,
    > CfAccessor<'db, K, V>
{
    pub fn new(db: &'db Db, cf: &'db Arc<rocksdb::BoundColumnFamily<'db>>) -> Self {
        Self {
            db,
            cf,
            _k: Default::default(),
            _v: Default::default(),
        }
    }

    pub unsafe fn get_unchecked(&self, key: &K) -> DynResult<Option<Owned<V>>> {
        let key = unsafe { key.as_bytes_unchecked() };
        match self.db.get_cf(self.cf, key)? {
            Some(bytes) => Ok(Some(V::box_from_bytes_unchecked(bytes).to_owned())),
            None => Ok(None),
        }
    }

    pub unsafe fn get_txn_unchecked(
        &self,
        txn: &DbTransaction,
        key: &K,
    ) -> DynResult<Option<Owned<V>>> {
        let key = unsafe { key.as_bytes_unchecked() };
        match txn.get_cf(self.cf, key)? {
            Some(bytes) => Ok(Some(V::box_from_bytes_unchecked(bytes).to_owned())),
            None => Ok(None),
        }
    }

    pub fn put(&self, key: &K, value: &V) -> DynResult<()> {
        // This is technically UB; the rocksdb crate requires initialized bytes
        // when uninitialized should be safe
        let key = unsafe { key.as_bytes_unchecked() };
        let value = unsafe { value.as_bytes_unchecked() };
        Ok(self.db.put_cf(self.cf, key, value)?)
    }

    pub fn put_txn(&self, txn: &DbTransaction, key: &K, value: &V) -> DynResult<()> {
        let key = unsafe { key.as_bytes_unchecked() };
        let value = unsafe { value.as_bytes_unchecked() };
        Ok(txn.put_cf(self.cf, key, value)?)
    }

    // TODO: Convert to a streaming iterator
    pub unsafe fn iter_by_prefix_unchecked<
        P: ToOwned + AsRawBytes + IsPrefixOf<K> + ?Sized + 'static,
    >(
        &self,
        // XXX: The mapping `P -> Owned<P>` is not invertible; there may be
        // multiple possible `P` with the same `Owned<P>`. Therefore, this
        // breaks type inference. However, you can't just take `prefix: P`
        // either because then this breaks the call to
        // `prefix.as_bytes_unchecked()`. I'm at a loss for how to fix type
        // inference here.
        prefix: Owned<P>,
    ) -> impl Iterator<Item = DynResult<Owned2<K, V>>> + 'db {
        let mut raw = self.db.raw_iterator_cf(self.cf);
        raw.seek(unsafe { prefix.borrow().as_bytes_unchecked() });

        struct Iter<'db, P: ToOwned + ?Sized, T: ?Sized, U: ?Sized> {
            raw: rocksdb::DBRawIteratorWithThreadMode<'db, Db>,
            prefix: Owned<P>,
            _t: std::marker::PhantomData<*const T>,
            _u: std::marker::PhantomData<*const U>,
        }

        impl<
                'db,
                P: ToOwned + IsPrefixOf<T> + ?Sized,
                T: FromBytesUnchecked + ToOwned + ?Sized,
                U: FromBytesUnchecked + ToOwned + ?Sized,
            > Iterator for Iter<'db, P, T, U>
        {
            type Item = DynResult<Owned2<T, U>>;

            fn next(&mut self) -> Option<Self::Item> {
                if !self.raw.valid() {
                    match self.raw.status() {
                        Ok(_) => return None,
                        Err(e) => return Some(Err(e.into())),
                    }
                }

                let key = unsafe { T::ref_from_bytes_unchecked(self.raw.key()?) };
                if !self.prefix.borrow().is_prefix_of(key) {
                    return None;
                }

                let key = key.to_owned();
                let value = unsafe { U::ref_from_bytes_unchecked(self.raw.value()?).to_owned() };

                self.raw.next();

                Some(Ok((key, value)))
            }
        }

        Iter::<P, K, V> {
            raw,
            prefix,
            _t: Default::default(),
            _u: Default::default(),
        }
    }

    pub unsafe fn iter_keys_by_prefix_unchecked<
        P: ToOwned + AsRawBytes + IsPrefixOf<K> + ?Sized + 'static,
    >(
        &self,
        prefix: Owned<P>,
    ) -> impl Iterator<Item = DynResult<Owned<K>>> + 'db {
        let mut raw = self.db.raw_iterator_cf(self.cf);
        raw.seek(unsafe { prefix.borrow().as_bytes_unchecked() });

        struct Iter<'db, P: ToOwned + ?Sized, T: ?Sized> {
            raw: rocksdb::DBRawIteratorWithThreadMode<'db, Db>,
            prefix: Owned<P>,
            _t: std::marker::PhantomData<*const T>,
            _p: std::marker::PhantomData<*const P>,
        }

        impl<
                'db,
                P: ToOwned + IsPrefixOf<T> + ?Sized,
                T: FromBytesUnchecked + ToOwned + ?Sized,
            > Iterator for Iter<'db, P, T>
        {
            type Item = DynResult<Owned<T>>;

            fn next(&mut self) -> Option<Self::Item> {
                if !self.raw.valid() {
                    match self.raw.status() {
                        Ok(_) => return None,
                        Err(e) => return Some(Err(e.into())),
                    }
                }

                let key = unsafe { T::ref_from_bytes_unchecked(self.raw.key()?) };
                if !self.prefix.borrow().is_prefix_of(key) {
                    return None;
                }

                let key = key.to_owned();

                self.raw.next();

                Some(Ok(key))
            }
        }

        Iter::<P, K> {
            raw,
            prefix,
            _t: Default::default(),
            _p: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::accessor::CfAccessor;
    use crate::key::{BigEndianU16, FiniteString, Packed2};
    use crate::testing::TestHarness;

    #[test]
    fn test_get_put() {
        type Key = Packed2<BigEndianU16, FiniteString<4>>;
        type Value = (u32, i16, [u8; 2]);

        let mut harness = TestHarness::new();
        let db = harness.db();
        db.create_cf("cf", &Default::default()).unwrap();
        let cf = db.cf_handle("cf").unwrap();
        let accessor = CfAccessor::<Key, Value>::new(&db, &cf);

        let key1: Key = (1.into(), FiniteString::try_from("blah").unwrap()).into();
        let value1: Value = (1, 2, [3, 4]);
        accessor.put(&key1, &value1).unwrap();

        let key2: Key = (2.into(), FiniteString::try_from("bluh").unwrap()).into();
        let value2: Value = (4, 3, [2, 1]);
        accessor.put(&key2, &value2).unwrap();

        unsafe {
            assert_eq!(accessor.get_unchecked(&key1).unwrap().unwrap(), value1);
            assert_eq!(accessor.get_unchecked(&key2).unwrap().unwrap(), value2);
        }

        accessor.put(&key1, &value2).unwrap();
        unsafe {
            assert_eq!(accessor.get_unchecked(&key1).unwrap().unwrap(), value2);
        }
    }

    #[test]
    fn test_iter_by_prefix() {
        type Key = Packed2<BigEndianU16, FiniteString<4>>;
        type Value = u32;

        let mut harness = TestHarness::new();
        let db = harness.db();
        db.create_cf("cf", &Default::default()).unwrap();
        let cf = db.cf_handle("cf").unwrap();
        let accessor = CfAccessor::<Key, Value>::new(&db, &cf);

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
        let mut iter = unsafe { accessor.iter_by_prefix_unchecked::<[u8; 2]>(prefix) };
        assert_eq!(iter.next().unwrap().unwrap(), (key1, value1));
        let next = iter.next();
        assert!(next.is_none());

        let prefix = 2u16.to_be_bytes();
        let mut iter = unsafe { accessor.iter_by_prefix_unchecked::<[u8; 2]>(prefix) };
        assert_eq!(iter.next().unwrap().unwrap(), (key2, value2));
        assert_eq!(iter.next().unwrap().unwrap(), (key3, value3));
        assert!(iter.next().is_none());
    }
}
