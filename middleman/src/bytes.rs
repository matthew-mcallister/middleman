use std::mem::MaybeUninit;

use uuid::Uuid;

pub trait FromBytesUnchecked {
    /// # Safety
    ///
    /// `bytes` must contain a valid bit pattern for this type. Size
    /// and alignment will be validated, but field values will not be.
    /// This is a particular hazard for types with trap
    /// representations, such as a struct containing a `bool` or enum.
    unsafe fn ref_from_bytes_unchecked(bytes: &[u8]) -> &Self;

    /// # Safety
    ///
    /// See `from_bytes_unchecked`.
    unsafe fn mut_from_bytes_unchecked(bytes: &mut [u8]) -> &mut Self;

    /// Reinterprets a byte buffer as a new type in-place.
    unsafe fn box_from_bytes_unchecked(bytes: impl Into<Box<[u8]>>) -> Box<Self> {
        let mut bytes = bytes.into();
        let ptr = Self::mut_from_bytes_unchecked(&mut *bytes) as *mut Self;
        std::mem::forget(bytes);
        unsafe { Box::from_raw(ptr) }
    }
}

/// Marks a type as safe to reinterpret as a byte slice (`&[u8]`). This means
/// the type must not contain uninitialized data, e.g. interior padding bytes,
/// trailing padding bytes, enums with fields, unions in general.
pub unsafe trait AsBytes {
    fn as_bytes(this: &Self) -> &[u8] {
        let ptr = this as *const Self as *const u8;
        let len = std::mem::size_of_val(this);
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

unsafe impl AsBytes for u8 {}
unsafe impl AsBytes for u16 {}
unsafe impl AsBytes for u32 {}
unsafe impl AsBytes for u64 {}
unsafe impl AsBytes for u128 {}
unsafe impl AsBytes for usize {}
unsafe impl AsBytes for i8 {}
unsafe impl AsBytes for i16 {}
unsafe impl AsBytes for i32 {}
unsafe impl AsBytes for i64 {}
unsafe impl AsBytes for i128 {}
unsafe impl AsBytes for isize {}
unsafe impl<T: AsBytes, const N: usize> AsBytes for [T; N] {}
unsafe impl<T: AsBytes> AsBytes for [T] {}
unsafe impl AsBytes for str {}
unsafe impl AsBytes for Uuid {}

/// Converts an object reference into a slice of *possibly uninitialized*
/// bytes. Any type with padding bytes will likely expose uninitialized memory
/// with this method.
pub trait AsRawBytes {
    fn as_raw_bytes(&self) -> &[MaybeUninit<u8>];

    /// Converts a reference into a byte slice, assuming that all bytes
    /// are initialized.
    ///
    /// # Safety
    ///
    /// It is unsafe to operate on any returned bytes that are
    /// uninitialized.
    unsafe fn as_bytes_unchecked(&self) -> &[u8] {
        std::mem::transmute(self.as_raw_bytes())
    }
}

impl FromBytesUnchecked for [u8] {
    unsafe fn ref_from_bytes_unchecked(bytes: &[u8]) -> &Self {
        bytes
    }

    unsafe fn mut_from_bytes_unchecked(bytes: &mut [u8]) -> &mut Self {
        bytes
    }
}

impl<T> AsRawBytes for [T] {
    fn as_raw_bytes(&self) -> &[MaybeUninit<u8>] {
        let ptr = self.as_ptr();
        let len = std::mem::size_of_val(self);
        unsafe { std::mem::transmute((ptr, len)) }
    }
}

impl FromBytesUnchecked for str {
    unsafe fn ref_from_bytes_unchecked(bytes: &[u8]) -> &Self {
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    unsafe fn mut_from_bytes_unchecked(bytes: &mut [u8]) -> &mut Self {
        unsafe { std::str::from_utf8_unchecked_mut(bytes) }
    }
}

impl AsRawBytes for str {
    fn as_raw_bytes(&self) -> &[MaybeUninit<u8>] {
        unsafe { std::mem::transmute(self.as_bytes()) }
    }
}

impl<T: Sized> FromBytesUnchecked for T {
    unsafe fn ref_from_bytes_unchecked(bytes: &[u8]) -> &Self {
        debug_assert!(bytes.len() >= std::mem::size_of::<Self>());
        debug_assert_eq!((bytes.as_ptr() as usize) % std::mem::align_of::<Self>(), 0);
        unsafe { &*(bytes.as_ptr() as *const Self) }
    }

    unsafe fn mut_from_bytes_unchecked(bytes: &mut [u8]) -> &mut Self {
        debug_assert!(bytes.len() >= std::mem::size_of::<Self>());
        debug_assert_eq!((bytes.as_ptr() as usize) % std::mem::align_of::<Self>(), 0);
        unsafe { &mut *(bytes.as_mut_ptr() as *mut Self) }
    }
}

impl<T: Sized> AsRawBytes for T {
    fn as_raw_bytes(&self) -> &[MaybeUninit<u8>] {
        let ptr = self as *const Self as *const MaybeUninit<u8>;
        let len = std::mem::size_of::<Self>();
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes::FromBytesUnchecked;

    #[test]
    fn test_struct_from_bytes() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(C)]
        struct Thing {
            flag: bool,
            int: u32,
        }
        let bytes: [u8; std::mem::size_of::<Thing>()] = [1, 2, 3, 4, 42, 0, 0, 0];
        let thing = unsafe { Thing::ref_from_bytes_unchecked(&bytes[..]) };
        let expected = Thing {
            flag: true,
            int: 42,
        };
        assert_eq!(thing, &expected);
    }

    #[test]
    fn test_str_from_bytes() {
        let s = "Hello, world!";
        let round_trip = unsafe { str::ref_from_bytes_unchecked(s.as_bytes()) };
        assert_eq!(s, round_trip);
    }
}
