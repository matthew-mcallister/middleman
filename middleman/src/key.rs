use std::convert::TryFrom;

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
    };
}

big_endian_int!(BigEndianU16, u16);
big_endian_int!(BigEndianU32, u32);
big_endian_int!(BigEndianU64, u64);

/// Marks a type as safe to reinterpret as a byte slice (`&[u8]`). This means
/// the type must not contain uninitialized data, e.g. interior padding bytes,
/// trailing padding bytes, enums with fields, unions in general.
pub unsafe trait ByteSafe {
    fn as_bytes(this: &Self) -> &[u8] {
        let ptr = this as *const Self as *const u8;
        let len = std::mem::size_of_val(this);
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

unsafe impl ByteSafe for u8 {}
unsafe impl ByteSafe for u16 {}
unsafe impl ByteSafe for u32 {}
unsafe impl ByteSafe for u64 {}
unsafe impl ByteSafe for u128 {}
unsafe impl ByteSafe for usize {}
unsafe impl ByteSafe for i8 {}
unsafe impl ByteSafe for i16 {}
unsafe impl ByteSafe for i32 {}
unsafe impl ByteSafe for i64 {}
unsafe impl ByteSafe for i128 {}
unsafe impl ByteSafe for isize {}
unsafe impl<T: ByteSafe, const N: usize> ByteSafe for [T; N] {}
unsafe impl<T: ByteSafe> ByteSafe for [T] {}
unsafe impl ByteSafe for str {}
unsafe impl<const N: usize> ByteSafe for FiniteString<N> {}

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

        unsafe impl<$($T: ByteSafe,)*> ByteSafe for $PackedN<$($T,)*> {}

        impl<$($T: ByteSafe,)*> AsRef<[u8]> for $PackedN<$($T,)*> {
            fn as_ref(&self) -> &[u8] {
                let ptr = self as *const Self as *const u8;
                let len = 0 $(+ std::mem::size_of::<$T>())*;
                unsafe { std::slice::from_raw_parts(ptr, len) }
            }
        }
    };
}

impl_packed_tuple!(Packed2, T, U);
impl_packed_tuple!(Packed3, T, U, V);
impl_packed_tuple!(Packed4, T, U, V, W);
impl_packed_tuple!(Packed5, T, U, V, W, X);
impl_packed_tuple!(Packed6, T, U, V, W, X, Y);

#[macro_export]
macro_rules! packed {
    ($_0:expr) => { $_0 };
    ($_1:expr, $_2:expr) => { Packed2($_1, $_2) };
    ($_1:expr, $_2:expr, $_3:expr) => { Packed3($_1, $_2, $_3) };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr) => { Packed4($_1, $_2, $_3, $_4) };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr, $_5:expr) => { Packed5($_1, $_2, $_3, $_4, $_5) };
    ($_1:expr, $_2:expr, $_3:expr, $_4:expr, $_5:expr, $_6:expr) => { Packed6($_1, $_2, $_3, $_4, $_5, $_6) };
}

/// Trait for automatically preparing a value for use in a key. Mainly, this is
/// used to convert integers to big-endian format so they can be sorted.
// XXX: Really this should just return [u8; N] but the compiler doesn't support
// it...
pub trait AsKey {
    fn as_key(self) -> impl ByteSafe;
}

macro_rules! impl_as_key_int {
    ($($Int:ty),*$(,)?) => {
        $(
            impl AsKey for $Int {
                fn as_key(self) -> impl ByteSafe {
                    self.to_be_bytes()
                }
            }
        )*
    }
}

impl_as_key_int!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

macro_rules! impl_as_key_tuple {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: AsKey,)*> AsKey for ($($T,)*) {
            fn as_key(self) -> impl ByteSafe {
                let ($($T),*) = self;
                $crate::packed!($($T.as_key()),*)
            }
        }
    }
}

impl_as_key_tuple!(T, U);
impl_as_key_tuple!(T, U, V);
impl_as_key_tuple!(T, U, V, W);
impl_as_key_tuple!(T, U, V, W, X);
impl_as_key_tuple!(T, U, V, W, X, Y);

impl<const N: usize> AsKey for FiniteString<N> {
    fn as_key(self) -> impl ByteSafe {
        self
    }
}
