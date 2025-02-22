use std::borrow::Borrow;
use std::marker::PhantomData;

use crate::bytes::{AsRawBytes, OwnedFromBytesUnchecked};
use crate::error::Result;
use crate::prefix::IsPrefixOf;
use crate::{Owned, Owned2, RawDb};

/// Trait for implementing iterators and wrappers around the base DB cursor.
// XXX: I think with streaming iterators, a cursor could just be a double-ended
// iterator over temporary "entries".
pub trait Cursor: Sized {
    type Key: ToOwned + ?Sized;
    type Value: ToOwned + ?Sized;

    fn check_status(&mut self) -> Result<Option<()>>;

    fn next(&mut self);

    unsafe fn key(&self) -> &Self::Key;

    unsafe fn value(&self) -> Owned<Self::Value>;

    // FIXME: Sigh... type inference is broken because there's no way to infer
    // which implementation of ToOwned to use...
    fn prefix<P: IsPrefixOf<Self::Key> + ToOwned + ?Sized>(
        self,
        prefix: Owned<P>,
    ) -> Prefix<Self, P>
    where
        Self: Sized,
    {
        Prefix {
            prefix,
            cursor: self,
            done: false,
        }
    }

    fn iter(self) -> Iter<Self> {
        Iter { cursor: self }
    }

    fn keys(self) -> Keys<Self> {
        Keys { cursor: self }
    }

    fn values(self) -> Values<Self> {
        Values { cursor: self }
    }
}

pub struct BaseCursor<
    'db,
    K: OwnedFromBytesUnchecked + AsRawBytes + ToOwned + ?Sized,
    V: OwnedFromBytesUnchecked + ToOwned + ?Sized,
> {
    raw: rocksdb::DBRawIteratorWithThreadMode<'db, RawDb>,
    _k: PhantomData<*const K>,
    _v: PhantomData<*const V>,
}

impl<
        'db,
        K: OwnedFromBytesUnchecked + AsRawBytes + ToOwned + ?Sized,
        V: OwnedFromBytesUnchecked + ToOwned + ?Sized,
    > BaseCursor<'db, K, V>
{
    pub(crate) fn new(raw: rocksdb::DBRawIteratorWithThreadMode<'db, RawDb>) -> Self {
        Self {
            raw,
            _k: Default::default(),
            _v: Default::default(),
        }
    }

    pub fn seek_to_first(&mut self) {
        self.raw.seek_to_first();
    }

    fn raw_seek<Q: AsRawBytes + ?Sized>(&mut self, target: &Q) {
        let raw_bytes = target.as_raw_bytes();
        let bytes: &[u8] = unsafe { std::mem::transmute(raw_bytes) };
        self.raw.seek(bytes);
    }

    pub fn seek(&mut self, key: &K) {
        self.raw_seek(key)
    }

    pub fn seek_prefix<P: IsPrefixOf<K> + AsRawBytes + ToOwned + ?Sized>(&mut self, prefix: &P) {
        self.raw_seek(prefix);
    }

    pub fn prefix_iter<P: IsPrefixOf<K> + AsRawBytes + ToOwned + ?Sized>(
        mut self,
        prefix: Owned<P>,
    ) -> Prefix<Self, P> {
        self.seek_prefix(prefix.borrow());
        self.prefix(prefix)
    }
}

impl<
        'db,
        K: OwnedFromBytesUnchecked + AsRawBytes + ToOwned + ?Sized,
        V: OwnedFromBytesUnchecked + ToOwned + ?Sized,
    > Cursor for BaseCursor<'db, K, V>
{
    type Key = K;
    type Value = V;

    fn check_status(&mut self) -> Result<Option<()>> {
        if self.raw.valid() {
            Ok(Some(()))
        } else {
            match self.raw.status() {
                Ok(_) => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
    }

    fn next(&mut self) {
        self.raw.next();
    }

    unsafe fn key(&self) -> &K {
        // FIXME: There should be a static assertion that K has alignment 1
        // because we are casting unaligned memory. Need yet another trait for
        // this because K is unsized.
        let bytes = self.raw.key().unwrap();
        K::ref_from_bytes_unchecked(bytes)
    }

    unsafe fn value(&self) -> Owned<V> {
        let bytes = self.raw.value().unwrap();
        V::owned_from_bytes_unchecked(bytes)
    }
}

/// A cursor which iterates over keys that match a particular prefix.
pub struct Prefix<C: Cursor, P: IsPrefixOf<C::Key> + ToOwned + ?Sized> {
    cursor: C,
    prefix: Owned<P>,
    done: bool,
}

impl<C: Cursor, P: IsPrefixOf<C::Key> + ToOwned> Prefix<C, P> {
    pub fn new(cursor: C, prefix: Owned<P>) -> Self {
        Self {
            cursor,
            prefix,
            done: false,
        }
    }
}

impl<C: Cursor, P: IsPrefixOf<C::Key> + ToOwned + ?Sized> Cursor for Prefix<C, P> {
    type Key = C::Key;
    type Value = C::Value;

    fn check_status(&mut self) -> Result<Option<()>> {
        if self.done {
            return Ok(None);
        }
        match self.cursor.check_status()? {
            Some(()) => {
                if !self.prefix.borrow().is_prefix_of(unsafe { self.key() }) {
                    self.done = true;
                    Ok(None)
                } else {
                    Ok(Some(()))
                }
            },
            None => Ok(None),
        }
    }

    fn next(&mut self) {
        self.cursor.next();
    }

    unsafe fn key(&self) -> &Self::Key {
        self.cursor.key()
    }

    unsafe fn value(&self) -> Owned<Self::Value> {
        self.cursor.value()
    }
}

pub struct Iter<C: Cursor> {
    cursor: C,
}

impl<C: Cursor> Iterator for Iter<C> {
    type Item = Result<Owned2<C::Key, C::Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = match self.cursor.check_status() {
            Ok(Some(())) => unsafe {
                let key = self.cursor.key();
                let value = self.cursor.value();
                Some(Ok((key.to_owned(), value)))
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        };
        self.cursor.next();
        res
    }
}

pub struct Keys<C: Cursor> {
    cursor: C,
}

impl<C: Cursor> Iterator for Keys<C> {
    type Item = Result<Owned<C::Key>>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = match self.cursor.check_status() {
            Ok(Some(())) => unsafe {
                let key = self.cursor.key();
                Some(Ok(key.to_owned()))
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        };
        self.cursor.next();
        res
    }
}

pub struct Values<C: Cursor> {
    cursor: C,
}

impl<C: Cursor> Iterator for Values<C> {
    type Item = Result<Owned<C::Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = match self.cursor.check_status() {
            Ok(Some(())) => unsafe {
                let value = self.cursor.value();
                Some(Ok(value))
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        };
        self.cursor.next();
        res
    }
}

impl<
        'db,
        K: OwnedFromBytesUnchecked + AsRawBytes + ToOwned + ?Sized,
        V: OwnedFromBytesUnchecked + ToOwned + ?Sized,
    > IntoIterator for BaseCursor<'db, K, V>
{
    type IntoIter = Iter<Self>;
    type Item = <Iter<Self> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<C: Cursor, P: IsPrefixOf<C::Key> + ToOwned + ?Sized> IntoIterator for Prefix<C, P> {
    type IntoIter = Iter<Self>;
    type Item = <Iter<Self> as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
