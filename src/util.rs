use std::mem::MaybeUninit;

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

pub trait ByteCast {
    fn as_bytes(this: &Self) -> &[MaybeUninit<u8>];

    /// # Safety
    ///
    /// `bytes` must contain a valid bit pattern for this type. Size
    /// and alignment will be validated, but field values will not be.
    /// This is a particular hazard for types with trap
    /// representations, such as a struct containing a `bool` or enum.
    unsafe fn from_bytes(bytes: &[u8]) -> &Self;

    /// # Safety
    ///
    /// See `from_bytes`.
    unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self;

    unsafe fn from_bytes_owned(bytes: impl Into<Box<[u8]>>) -> Box<Self> {
        let mut bytes = bytes.into();
        let ptr = unsafe { Self::from_bytes_mut(&mut *bytes) as *mut Self };
        std::mem::forget(bytes);
        unsafe { Box::from_raw(ptr) }
    }
}

impl ByteCast for [u8] {
    fn as_bytes(this: &Self) -> &[MaybeUninit<u8>] {
        unsafe { std::mem::transmute(this) }
    }

    unsafe fn from_bytes(bytes: &[u8]) -> &Self {
        bytes
    }

    unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        bytes
    }
}

impl ByteCast for str {
    fn as_bytes(this: &Self) -> &[MaybeUninit<u8>] {
        unsafe { std::mem::transmute(this.as_bytes()) }
    }

    unsafe fn from_bytes(bytes: &[u8]) -> &Self {
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        unsafe { std::str::from_utf8_unchecked_mut(bytes) }
    }
}

#[macro_export]
macro_rules! impl_byte_cast {
    ($Type:ident) => {
        impl crate::util::ByteCast for $Type {
            fn as_bytes(this: &Self) -> &[std::mem::MaybeUninit<u8>] {
                unsafe {
                    std::slice::from_raw_parts(
                        this as *const Self as *const std::mem::MaybeUninit<u8>,
                        std::mem::size_of::<Self>(),
                    )
                }
            }

            unsafe fn from_bytes(bytes: &[u8]) -> &Self {
                assert_eq!(bytes.len(), std::mem::size_of::<$Type>());
                assert_eq!(bytes.as_ptr() as usize % std::mem::align_of::<$Type>(), 0);
                unsafe { &*(bytes.as_ptr() as *const Self) }
            }

            unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
                assert_eq!(bytes.len(), std::mem::size_of::<$Type>());
                assert_eq!(bytes.as_ptr() as usize % std::mem::align_of::<$Type>(), 0);
                unsafe { &mut *(bytes.as_mut_ptr() as *mut Self) }
            }
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

impl<'db, T: ?Sized> std::ops::Deref for DbSlice<'db, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

#[cfg(test)]
mod tests {
    use crate::define_key;
    use crate::types::Prefix;
    use crate::util::BigEndianU32;

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
}
