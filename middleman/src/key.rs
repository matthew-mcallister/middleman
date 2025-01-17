use std::convert::TryFrom;

use crate::bytes::AsBytes;
use crate::prefix::IsPrefixOf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StringOverflowError;

impl std::fmt::Display for StringOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "String overflow")
    }
}

impl std::error::Error for StringOverflowError {}

// This byte is not allowed by UTF-8 because it begins a two-byte sequence that
// is out of the valid codepoint range (0x80-0x7ff).
const UTF8_INVALID_BYTE: u8 = 0xc0;

// XXX: Sigh... this is obsoleted by using comparators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FiniteString<const N: usize> {
    bytes: [u8; N],
}

impl<const N: usize> FiniteString<N> {
    pub fn as_bytes(&self) -> &[u8; N] {
        &self.bytes
    }
}

impl<const N: usize> AsRef<[u8; N]> for FiniteString<N> {
    fn as_ref(&self) -> &[u8; N] {
        &self.bytes
    }
}

impl<const N: usize> AsRef<str> for FiniteString<N> {
    fn as_ref(&self) -> &str {
        let len = match self.bytes.iter().position(|&c| c == UTF8_INVALID_BYTE) {
            Some(n) => n,
            None => N,
        };
        let ptr = self.bytes.as_ptr();
        unsafe {
            let bytes: &[u8] = std::slice::from_raw_parts(ptr, len);
            std::str::from_utf8_unchecked(bytes)
        }
    }
}

impl<'s, const N: usize> TryFrom<&'s str> for FiniteString<N> {
    type Error = StringOverflowError;

    fn try_from(value: &'s str) -> Result<Self, Self::Error> {
        if value.len() > N {
            Err(StringOverflowError)
        } else {
            let mut bytes = [UTF8_INVALID_BYTE; N];
            bytes[..value.len()].copy_from_slice(value.as_bytes());
            Ok(Self { bytes })
        }
    }
}

unsafe impl<const N: usize> AsBytes for FiniteString<N> {}

macro_rules! big_endian_int {
    ($BeInt:ident, $Int:ty) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
        unsafe impl AsBytes for $BeInt {}
    };
}

big_endian_int!(BigEndianU16, u16);
big_endian_int!(BigEndianU32, u32);
big_endian_int!(BigEndianU64, u64);

macro_rules! impl_packed_tuple {
    ($PackedN:ident, $($T:ident),*$(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

        unsafe impl<$($T: AsBytes,)*> AsBytes for $PackedN<$($T,)*> {}
    };
}

impl_packed_tuple!(Packed2, T, U);
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
        Packed2($_1, $_2)
    };
    ($_1:expr, $_2:expr, $_3:expr) => {
        Packed3($_1, $_2, $_3)
    };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr) => {
        Packed4($_1, $_2, $_3, $_4)
    };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr, $_5:expr) => {
        Packed5($_1, $_2, $_3, $_4, $_5)
    };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr, $_5:expr, $_6:expr) => {
        Packed6($_1, $_2, $_3, $_4, $_5, $_6)
    };
}

macro_rules! impl_bytes_as_prefix_of_packed_tuple {
    ($PackedN:ident<$($T:ident),*>) => {
        impl<$($T: AsBytes),*> IsPrefixOf<$PackedN<$($T),*>> for [u8] {
            fn is_prefix_of(&self, other: &$PackedN<$($T),*>) -> bool {
                self.is_prefix_of(AsBytes::as_bytes(other))
            }
        }
    };
}

impl_bytes_as_prefix_of_packed_tuple!(Packed2<T, U>);
impl_bytes_as_prefix_of_packed_tuple!(Packed3<T, U, V>);
impl_bytes_as_prefix_of_packed_tuple!(Packed4<T, U, V, W>);
impl_bytes_as_prefix_of_packed_tuple!(Packed5<T, U, V, W, X>);
impl_bytes_as_prefix_of_packed_tuple!(Packed6<T, U, V, W, X, Y>);

/*
macro_rules! impl_packed_tuple_is_prefix_of {
    ($Lhs:ident, $Rhs:ident; $($index:tt: $T:ident),*; $index_u:tt: $U:ident; $($V:ident),*) => {
        impl<$($T: Copy + Eq),*, $U: Copy + IsPrefixOf, $($V),*> IsPrefixOf<$Rhs<$($T),*, $U, $($V),*>> for $Lhs<$($T),*, $U> {
            fn is_prefix_of(&self, other: &$Rhs<$($T),*, $U, $($V),*>) -> bool {
                true
                    $(&& {
                        let lhs = self.$index;
                        let rhs = other.$index;
                        lhs == rhs
                    })*
                    && {
                        let lhs = self.$index_u;
                        let rhs = other.$index_u;
                        lhs.is_prefix_of(&rhs)
                    }
            }
        }
    };
    ($Tuple:ident; $T:ident, $($U:ident),*) => {
        impl<$T: Copy + IsPrefixOf, $($U),*> IsPrefixOf<$Tuple<$T, $($U),*>> for $T {
            fn is_prefix_of(&self, other: &$Tuple<$T, $($U),*>) -> bool {
                let rhs = other.0;
                self.is_prefix_of(&rhs)
            }
        }
    }
}

impl_packed_tuple_is_prefix_of!(Packed2; T, U);
impl_packed_tuple_is_prefix_of!(Packed3; T, U, V);
impl_packed_tuple_is_prefix_of!(Packed4; T, U, V, W);
impl_packed_tuple_is_prefix_of!(Packed5; T, U, V, W, X);
impl_packed_tuple_is_prefix_of!(Packed6; T, U, V, W, X, Y);

impl_packed_tuple_is_prefix_of!(Packed2, Packed2; 0: T; 1: U;);
impl_packed_tuple_is_prefix_of!(Packed2, Packed3; 0: T; 1: U; V);
impl_packed_tuple_is_prefix_of!(Packed2, Packed4; 0: T; 1: U; V, W);
impl_packed_tuple_is_prefix_of!(Packed2, Packed5; 0: T; 1: U; V, W, X);
impl_packed_tuple_is_prefix_of!(Packed2, Packed6; 0: T; 1: U; V, W, X, Y);

impl_packed_tuple_is_prefix_of!(Packed3, Packed3; 0: T, 1: U; 2: V;);
impl_packed_tuple_is_prefix_of!(Packed3, Packed4; 0: T, 1: U; 2: V; W);
impl_packed_tuple_is_prefix_of!(Packed3, Packed5; 0: T, 1: U; 2: V; W, X);
impl_packed_tuple_is_prefix_of!(Packed3, Packed6; 0: T, 1: U; 2: V; W, X, Y);

impl_packed_tuple_is_prefix_of!(Packed4, Packed4; 0: T, 1: U, 2: V; 3: W;);
impl_packed_tuple_is_prefix_of!(Packed4, Packed5; 0: T, 1: U, 2: V; 3: W; X);
impl_packed_tuple_is_prefix_of!(Packed4, Packed6; 0: T, 1: U, 2: V; 3: W; X, Y);

impl_packed_tuple_is_prefix_of!(Packed5, Packed5; 0: T, 1: U, 2: V, 3: W; 4: X;);
impl_packed_tuple_is_prefix_of!(Packed5, Packed6; 0: T, 1: U, 2: V, 3: W; 4: X; Y);

impl_packed_tuple_is_prefix_of!(Packed6, Packed6; 0: T, 1: U, 2: V, 3: W, 4: X; 5: Y;);
*/

#[cfg(test)]
mod tests {
    use crate::bytes::{AsBytes, FromBytesUnchecked};
    use middleman_macros::db_key;

    use super::*;

    #[test]
    fn test_key_macro() {
        #[db_key]
        struct MyKey {
            a: BigEndianU16,
            b: FiniteString<4>,
            c: BigEndianU16,
        }

        let key = MyKey {
            a: 1.into(),
            b: FiniteString::try_from("blah").unwrap(),
            c: 2.into(),
        };
        let bytes: [u8; 8] = [0, 1, 'b' as _, 'l' as _, 'a' as _, 'h' as _, 0, 2];
        assert_eq!(MyKey::as_bytes(&key), bytes);
        unsafe {
            assert_eq!(*MyKey::ref_from_bytes_unchecked(&bytes), key);
        }
    }

    #[test]
    fn test_packed_prefix() {
        let tuple = Packed3(1u32, 2u32, 3u32);
        assert!(1u32.to_ne_bytes().is_prefix_of(&tuple));
        assert!(AsBytes::as_bytes(&Packed2(1u32, 2u32)).is_prefix_of(&tuple));
        assert!(AsBytes::as_bytes(&tuple).is_prefix_of(&tuple));
        assert!(!2u32.to_ne_bytes().is_prefix_of(&tuple));
        assert!(!AsBytes::as_bytes(&Packed3(1u32, 2u32, 4u32)).is_prefix_of(&tuple));
        assert!(!AsBytes::as_bytes(&Packed3(2u32, 2u32, 3u32)).is_prefix_of(&tuple));
    }
}
