//! Implements some helpers for working with key data.

use bytecast::{FromBytes, HasLayout, IntoBytes};

use crate::prefix::IsPrefixOf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StringOverflowError;

impl std::fmt::Display for StringOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "String overflow")
    }
}

impl std::error::Error for StringOverflowError {}

macro_rules! big_endian_int {
    ($BeInt:ident, $Int:ty) => {
        #[derive(
            Clone, Copy, Debug, Eq, FromBytes, HasLayout, IntoBytes, Ord, PartialEq, PartialOrd,
        )]
        #[repr(C)]
        pub struct $BeInt([u8; std::mem::size_of::<$Int>()]);
        impl From<$Int> for $BeInt {
            fn from(value: $Int) -> $BeInt {
                $BeInt(value.to_be_bytes())
            }
        }
        impl From<$BeInt> for $Int {
            fn from(value: $BeInt) -> $Int {
                <$Int>::from_be_bytes(value.0)
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
big_endian_int!(BigEndianI16, i16);
big_endian_int!(BigEndianI32, i32);
big_endian_int!(BigEndianI64, i64);

macro_rules! impl_packed_tuple {
    ($PackedN:ident, $($T:ident),*$(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, FromBytes, HasLayout, IntoBytes, Ord, PartialEq, PartialOrd)]
        #[repr(packed)]
        pub struct $PackedN<$($T,)*>($(pub $T,)*);

        #[allow(non_snake_case)]
        impl<$($T,)*> From<($($T,)*)> for $PackedN<$($T,)*> {
            fn from(($($T),*): ($($T),*)) -> Self {
                Self($($T),*)
            }
        }

        #[allow(non_snake_case)]
        impl<$($T,)*> From<$PackedN<$($T,)*>> for ($($T),*) {
            fn from($PackedN($($T),*): $PackedN<$($T,)*>) -> Self {
                ($($T),*)
            }
        }
    };
}

#[derive(Clone, Copy, Debug, Eq, FromBytes, HasLayout, IntoBytes, Ord, PartialEq, PartialOrd)]
#[repr(packed)]
pub struct Packed2<T1, T2>(pub T1, pub T2);

#[allow(non_snake_case)]
impl<T1, T2> From<(T1, T2)> for Packed2<T1, T2> {
    fn from((t1, t2): (T1, T2)) -> Self {
        Self(t1, t2)
    }
}

#[allow(non_snake_case)]
impl<T1, T2> From<Packed2<T1, T2>> for (T1, T2) {
    fn from(Packed2(t1, t2): Packed2<T1, T2>) -> Self {
        (t1, t2)
    }
}

impl_packed_tuple!(Packed3, T, U, V);
impl_packed_tuple!(Packed4, T, U, V, W);
impl_packed_tuple!(Packed5, T, U, V, W, X);
impl_packed_tuple!(Packed6, T, U, V, W, X, Y);

#[macro_export]
macro_rules! packed {
    ($_0:expr) => {
        $_0
    };
    ($_1:expr, $_2:expr) => {
        $crate::key::Packed2($_1, $_2)
    };
    ($_1:expr, $_2:expr, $_3:expr) => {
        $crate::key::Packed3($_1, $_2, $_3)
    };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr) => {
        $crate::key::Packed4($_1, $_2, $_3, $_4)
    };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr, $_5:expr) => {
        $crate::key::Packed5($_1, $_2, $_3, $_4, $_5)
    };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr, $_5:expr, $_6:expr) => {
        $crate::key::Packed6($_1, $_2, $_3, $_4, $_5, $_6)
    };
}

pub use packed;

macro_rules! impl_bytes_as_prefix_of_packed_tuple {
    ($PackedN:ident<$($T:ident),*>) => {
        impl<$($T: HasLayout + IntoBytes),*> IsPrefixOf<$PackedN<$($T),*>> for [u8] {
            fn is_prefix_of(&self, other: &$PackedN<$($T),*>) -> bool {
                self.is_prefix_of(IntoBytes::as_bytes(other))
            }
        }

        impl<T0: HasLayout + IntoBytes, $($T: HasLayout + IntoBytes),*> IsPrefixOf<T0> for $PackedN<$($T),*> {
            fn is_prefix_of(&self, other: &T0) -> bool {
                IntoBytes::as_bytes(self).is_prefix_of(IntoBytes::as_bytes(other))
            }
        }
    };
}

impl_bytes_as_prefix_of_packed_tuple!(Packed2<T, U>);
impl_bytes_as_prefix_of_packed_tuple!(Packed3<T, U, V>);
impl_bytes_as_prefix_of_packed_tuple!(Packed4<T, U, V, W>);
impl_bytes_as_prefix_of_packed_tuple!(Packed5<T, U, V, W, X>);
impl_bytes_as_prefix_of_packed_tuple!(Packed6<T, U, V, W, X, Y>);

#[cfg(test)]
mod tests {
    use bytecast::{FromBytes, HasLayout, IntoBytes};

    use super::*;

    #[test]
    fn test_key() {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, FromBytes, HasLayout, IntoBytes)]
        #[repr(packed)]
        struct MyKey {
            a: BigEndianU16,
            b: Packed4<u8, u8, u8, u8>,
            c: BigEndianU16,
        }

        let key = MyKey {
            a: 1.into(),
            b: packed!('b' as _, 'l' as _, 'a' as _, 'h' as _),
            c: 2.into(),
        };
        let bytes: [u8; 8] = [0, 1, 'b' as _, 'l' as _, 'a' as _, 'h' as _, 0, 2];
        assert_eq!(MyKey::as_bytes(&key), bytes);
        assert_eq!(*MyKey::ref_from_bytes(&bytes).unwrap(), key);
    }

    #[test]
    fn test_packed_prefix() {
        let tuple = Packed3(1u32, 2u32, 3u32);
        assert!(1u32.to_ne_bytes().is_prefix_of(&tuple));
        assert!(Packed2(1u32, 2u32).as_bytes().is_prefix_of(&tuple));
        assert!(tuple.as_bytes().is_prefix_of(&tuple));
        assert!(!2u32.to_ne_bytes().is_prefix_of(&tuple));
        assert!(!Packed3(1u32, 2u32, 4u32).as_bytes().is_prefix_of(&tuple));
        assert!(!Packed3(2u32, 2u32, 3u32).as_bytes().is_prefix_of(&tuple));
    }
}
