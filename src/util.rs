pub(crate) fn join_slices<T: Copy>(slices: &[&[T]]) -> Vec<T> {
    let len = slices.iter().map(|slice| slice.len()).sum();
    let mut vec = Vec::with_capacity(len);
    for &slice in slices {
        vec.extend(slice);
    }
    vec
}

macro_rules! big_endian_int {
    ($BeInt:ident, $Int:ty) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        struct $BeInt([u8; std::mem::size_of::<$Int>()]);
        impl From<$Int> for $BeInt {
            fn from(value: $Int) -> $BeInt {
                $BeInt(value.to_be_bytes())
            }
        }
        impl From<$BeInt> for [u8; std::mem::size_of::<$Int>()] {
            fn from(value: $BeInt) -> [u8; std::mem::size_of::<$Int>()] {
                value.0
            }
        }
        impl From<[u8; std::mem::size_of::<$Int>()]> for $BeInt {
            fn from(value: [u8; std::mem::size_of::<$Int>()]) -> $BeInt {
                $BeInt(value)
            }
        }
        impl AsRef<[u8; std::mem::size_of::<$Int>()]> for $BeInt {
            fn as_ref(&self) -> &[u8; std::mem::size_of::<$Int>()] {
                &self.0
            }
        }
    };
}

big_endian_int!(BigEndianU16, u16);
big_endian_int!(BigEndianU32, u32);
big_endian_int!(BigEndianU64, u64);

#[macro_export]
macro_rules! define_key {
    (
        $Key:ident {
            prefix = $prefix_value:expr,
            $($field:ident: $FieldTy:ty),*$(,)?
        }
    ) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        #[repr(packed)]
        pub(crate) struct $Key {
            prefix: crate::types::Prefix,
            $(pub(crate) $field: $FieldTy,)*
        }

        impl $Key {
            pub(crate) fn new($($field: $FieldTy,)*) -> Self {
                Self { prefix: $prefix_value, $($field,)* }
            }

            pub(crate) fn as_bytes(&self) -> &[u8; std::mem::size_of::<Self>()] {
                unsafe { &*(self as *const Self as *const [u8; std::mem::size_of::<Self>()]) }
            }

            pub(crate) unsafe fn from_bytes(bytes: [u8; std::mem::size_of::<Self>()]) -> Self {
                unsafe { std::mem::transmute(bytes) }
            }
        }

        impl AsRef<[u8; std::mem::size_of::<$Key>()]> for $Key {
            fn as_ref(&self) -> &[u8; std::mem::size_of::<$Key>()] {
                self.as_bytes()
            }
        }

        impl AsRef<[u8]> for $Key {
            fn as_ref(&self) -> &[u8] {
                self.as_bytes().as_ref()
            }
        }

        impl From<$Key> for [u8; std::mem::size_of::<$Key>()] {
            fn from(value: $Key) -> Self {
                unsafe { std::mem::transmute(value) }
            }
        }
    };
}

pub(crate) trait ByteCast {
    /// For slice-based DSTs, this is `usize`. For sized types, this is `()`.
    type Size;

    /// Returns the alignment of `Self`.
    fn align_of() -> usize;

    /// Returns the size of an instance of `Self`, possibly excluding final
    /// padding bytes.
    // XXX: Not clear if it's UB when size is not a multiple of alignment.
    fn size_of_tight(num_elems: Self::Size) -> usize;

    unsafe fn from_bytes(bytes: &[u8]) -> &Self;

    unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self;

    /// Converts `Box<[u8]>` to `Box<T>`, assuming that the vector contains a
    /// valid `T` value.
    ///
    /// # Panics
    ///
    /// Panics if the box has insufficient size or alignment.
    unsafe fn from_bytes_owned(bytes: impl Into<Box<[u8]>>) -> Box<Self> {
        let mut bytes = bytes.into();
        let ptr = unsafe { <Self as ByteCast>::from_bytes_mut(&mut *bytes) as *mut Self };
        std::mem::forget(bytes);
        unsafe { Box::from_raw(ptr) }
    }

    /// Casts an object to a byte slice. Sadly, this is not guaranteed to be
    /// safe, as it is undefined behavior to read padding bytes in a struct as
    /// they are uninitialized. This method is only safe if all bytes in a
    /// struct are initialized.
    unsafe fn as_bytes(this: &Self) -> &[u8];
}

impl<T: Sized> ByteCast for T {
    type Size = ();

    fn align_of() -> usize {
        std::mem::align_of::<T>()
    }

    fn size_of_tight(_: Self::Size) -> usize {
        std::mem::size_of::<T>()
    }

    unsafe fn from_bytes(bytes: &[u8]) -> &Self {
        assert!(bytes.len() >= std::mem::size_of::<T>());
        assert_eq!(bytes.as_ptr() as usize % std::mem::align_of::<T>(), 0);
        unsafe { &*(bytes.as_ptr() as *const Self) }
    }

    unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        assert!(bytes.len() >= std::mem::size_of::<T>());
        assert_eq!(bytes.as_ptr() as usize % std::mem::align_of::<T>(), 0);
        unsafe { &mut *(bytes.as_mut_ptr() as *mut Self) }
    }

    unsafe fn as_bytes(this: &Self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                this as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }
}

/// Implements ByteCast for `$struct<[T]>`.
#[macro_export]
macro_rules! impl_byte_cast_unsized {
    ($struct:ident, $slice:ident) => {
        impl<T> ByteCast for $struct<[T]> {
            type Size = usize;

            fn align_of() -> usize {
                std::mem::align_of::<$struct<T>>()
            }

            fn size_of_tight(num_elems: Self::Size) -> usize {
                let slice_offset = std::mem::offset_of!($struct<T>, $slice);
                let slice_len = num_elems * std::mem::size_of::<T>();
                let len = slice_offset + slice_len;
                std::cmp::max(std::mem::size_of::<$struct<[T; 0]>>(), len)
            }

            unsafe fn from_bytes(bytes: &[u8]) -> &Self {
                assert!(bytes.len() >= <Self as ByteCast>::size_of_tight(0));
                assert_eq!(bytes.as_ptr() as usize % <Self as ByteCast>::align_of(), 0);
                let slice_offset = std::mem::offset_of!($struct<T>, $slice);
                let num_elems = (bytes.len() - slice_offset) / std::mem::size_of::<T>();
                unsafe { std::mem::transmute((bytes.as_ptr(), num_elems)) }
            }

            unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
                assert!(bytes.len() >= <Self as ByteCast>::size_of_tight(0));
                assert_eq!(bytes.as_ptr() as usize % <Self as ByteCast>::align_of(), 0);
                let slice_offset = std::mem::offset_of!($struct<T>, $slice);
                let num_elems = (bytes.len() - slice_offset) / std::mem::size_of::<T>();
                unsafe { std::mem::transmute((bytes.as_mut_ptr(), num_elems)) }
            }

            unsafe fn as_bytes(this: &Self) -> &[u8] {
                unsafe {
                    let (_, num_elems): (usize, usize) = std::mem::transmute(this);
                    let size = <Self as ByteCast>::size_of_tight(num_elems);
                    std::slice::from_raw_parts(this as *const Self as *const u8, size)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! make_dst {
    (
        $struct:ident[$slice_type:ty] {
            $($field:ident: $value:expr),+
            $(, [$slice:ident]: ($($payload:expr),+$(,)?))?$(,)?
        }
    ) => {
        {
            let num_elems = 0 $($(+ $payload.len())*)?;
            let len = <$struct<[$slice_type]> as ByteCast>::size_of_tight(num_elems);
            let mut buffer: Vec<u8> = Vec::with_capacity(len);

            let ptr: *mut $struct<$slice_type> = buffer.as_mut_ptr() as *mut _;
            // XXX: (*ptr) is potentially UB...
            $(std::ptr::addr_of_mut!((*ptr).$field).write($value);)*

            $(
                let mut _ptr = std::ptr::addr_of_mut!((*ptr).$slice);
                $(
                    let expr = $payload;
                    _ptr.copy_from(expr.as_ptr(), expr.len());
                    _ptr = _ptr.offset(expr.len() as _);
                )*
            )?

            buffer.set_len(len);
            <$struct<[$slice_type]> as ByteCast>::from_bytes_owned(buffer)
        }
    };
}

pub struct DbSlice<'db, T: ?Sized> {
    _inner: rocksdb::DBPinnableSlice<'db>,
    ptr: *const T,
}

impl<'db, T: ByteCast + ?Sized> DbSlice<'db, T> {
    /// Casts an untyped DBPinnableSlice to a typed DbSlice.
    pub unsafe fn new(slice: rocksdb::DBPinnableSlice<'db>) -> Self {
        Self {
            ptr: unsafe { <T as ByteCast>::from_bytes(&*slice) as *const T },
            _inner: slice,
        }
    }
}

impl<'db, T: ByteCast + ?Sized> std::ops::Deref for DbSlice<'db, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

pub(crate) unsafe fn with_types<K, V>(
    iter: impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
) -> impl Iterator<Item = Result<(Box<K>, Box<V>), rocksdb::Error>>
where
    K: ByteCast + ?Sized,
    V: ByteCast + ?Sized,
{
    iter.map(|item| {
        let (k, v) = item?;
        unsafe { Ok((ByteCast::from_bytes_owned(k), ByteCast::from_bytes_owned(v))) }
    })
}

#[cfg(test)]
mod tests {
    use crate::define_key;
    use crate::types::Prefix;
    use crate::util::{BigEndianU32, ByteCast};

    #[test]
    fn test_big_endian() {
        assert_eq!(
            <[u8; 4]>::from(BigEndianU32::from(0x01020304)),
            [1, 2, 3, 4],
        );
    }

    #[test]
    fn test_define_key() {
        define_key!(Key {
            prefix = Prefix::Event,
            b: u32,
            c: u8,
        });
        let key = Key::new(1, 2);
        let bytes = [0, 1, 0, 0, 0, 2];
        assert_eq!(key.as_bytes(), &bytes);
        assert_eq!(<[u8; 6]>::from(key), bytes);
    }

    #[test]
    fn test_from_to_bytes() {
        struct S<T: ?Sized> {
            a: u32,
            b: u8,
            c: T,
        }
        impl_byte_cast_unsized!(S, c);
        let bytes = [1, 0, 0, 0, 2, 0, 3, 0, 4, 0];
        let s: Box<S<[u16]>> = unsafe { ByteCast::from_bytes_owned(bytes) };
        assert_eq!(s.a, 1);
        assert_eq!(s.b, 2);
        assert_eq!(&s.c[..], [3, 4]);
    }
}
