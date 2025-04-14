pub mod layout;

pub use bytecast_derive::{FromBytes, HasLayout, IntoBytes};
pub use layout::{HasLayout, Layout};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FromBytesError {
    InvalidSize,
    InvalidAlignment,
}

// XXX: This API unfortunately can't be made sound as-is on slice DSTs that may
// have padding after the tail due to alignment. Some compromises or
// workarounds are necessary, such as enriching byte slices with alignment
// information. For now, the derive macro simply fails on any type with this
// issue, leaving the user to sort out the alignment.
pub trait FromBytes {
    fn ref_from_bytes(bytes: &[u8]) -> Result<&Self, FromBytesError>;

    fn mut_from_bytes(bytes: &mut [u8]) -> Result<&mut Self, FromBytesError>;
}

/// Allows reading the individual bytes of a type. The type itself must have no
/// padding bytes, which cause undefined behavior if read.
pub trait IntoBytes {
    fn as_bytes(&self) -> &[u8];
}

/// Allows mutating the individual bytes of a type. This is a stricter
/// requirement than `IntoBytes`, as the type must not possess any trap
/// representations that would cause undefined behavior if you were to write
/// them to the underlying byte buffer.
pub trait IntoBytesMut: IntoBytes + FromBytes {
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        let ptr = self.as_bytes() as *const [u8] as *mut [u8];
        unsafe { &mut (*ptr)[..] }
    }
}

impl<T: ?Sized> IntoBytesMut for T
where
    T: IntoBytes + FromBytes,
{
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        let ptr = self.as_bytes() as *const [u8] as *mut [u8];
        // SAFETY: FromBytes guarantees that all possible byte strings are
        // valid representations of `Self`, so we know that no harm can come
        // from writing to this raw byte slice.
        unsafe { &mut (*ptr)[..] }
    }
}

pub fn box_from_bytes<T: FromBytes + ?Sized>(
    mut bytes: Box<[u8]>,
) -> std::result::Result<Box<T>, (FromBytesError, Box<[u8]>)> {
    let ptr = match T::mut_from_bytes(&mut bytes[..]) {
        Ok(r) => r as *mut T,
        Err(e) => return Err((e, bytes)),
    };
    std::mem::forget(bytes);
    unsafe { Ok(Box::from_raw(ptr as *mut T)) }
}

macro_rules! from_bytes_impl {
    () => {
        fn ref_from_bytes(bytes: &[u8]) -> Result<&Self, FromBytesError> {
            let address = bytes.as_ptr() as usize;
            let layout = &<Self as HasLayout>::LAYOUT;
            if address % layout.alignment != 0 {
                return Err(FromBytesError::InvalidAlignment);
            }
            layout::validate_size(layout, bytes.len())?;
            Ok(unsafe { &*(address as *const Self) })
        }

        fn mut_from_bytes(bytes: &mut [u8]) -> Result<&mut Self, FromBytesError> {
            let address = bytes.as_ptr() as usize;
            let layout = &<Self as HasLayout>::LAYOUT;
            if address % layout.alignment != 0 {
                return Err(FromBytesError::InvalidAlignment);
            }
            layout::validate_size(layout, bytes.len())?;
            Ok(unsafe { &mut *(address as *mut Self) })
        }
    };
}

macro_rules! from_bytes_primitive {
    ($Type:ty) => {
        impl FromBytes for $Type {
            from_bytes_impl!();
        }
    };
}

macro_rules! into_bytes_impl {
    () => {
        fn as_bytes(&self) -> &[u8] {
            unsafe {
                std::slice::from_raw_parts(
                    self as *const Self as *const u8,
                    std::mem::size_of::<Self>(),
                )
            }
        }
    };
}

macro_rules! into_bytes_primitive {
    ($Type:ty) => {
        impl IntoBytes for $Type {
            into_bytes_impl!();
        }
    };
}

from_bytes_primitive!(u8);
from_bytes_primitive!(u16);
from_bytes_primitive!(u32);
from_bytes_primitive!(u64);
from_bytes_primitive!(u128);
from_bytes_primitive!(usize);
from_bytes_primitive!(i8);
from_bytes_primitive!(i16);
from_bytes_primitive!(i32);
from_bytes_primitive!(i64);
from_bytes_primitive!(i128);
from_bytes_primitive!(isize);
into_bytes_primitive!(u8);
into_bytes_primitive!(u16);
into_bytes_primitive!(u32);
into_bytes_primitive!(u64);
into_bytes_primitive!(u128);
into_bytes_primitive!(usize);
into_bytes_primitive!(i8);
into_bytes_primitive!(i16);
into_bytes_primitive!(i32);
into_bytes_primitive!(i64);
into_bytes_primitive!(i128);
into_bytes_primitive!(isize);
into_bytes_primitive!(bool);

impl<const N: usize, T: FromBytes> FromBytes for [T; N]
where
    Self: HasLayout,
{
    from_bytes_impl!();
}

impl<const N: usize, T: IntoBytes> IntoBytes for [T; N]
where
    Self: HasLayout,
{
    into_bytes_impl!();
}

impl FromBytes for () {
    from_bytes_impl!();
}

impl IntoBytes for ()
where
    Self: HasLayout,
{
    into_bytes_impl!();
}

macro_rules! impl_tuple {
    ($($Tn:ident),*) => {
        impl<$($Tn: HasLayout + FromBytes,)*> FromBytes for ($($Tn,)*) {
            from_bytes_impl!();
        }

        impl<$($Tn: HasLayout + IntoBytes,)*> IntoBytes for ($($Tn,)*) {
            into_bytes_impl!();
        }
    }
}

impl_tuple!(T1, T2);
impl_tuple!(T1, T2, T3);
impl_tuple!(T1, T2, T3, T4);
impl_tuple!(T1, T2, T3, T4, T5);
impl_tuple!(T1, T2, T3, T4, T5, T6);

impl<T: HasLayout + FromBytes> FromBytes for [T] {
    fn ref_from_bytes(bytes: &[u8]) -> Result<&Self, FromBytesError> {
        let address = bytes.as_ptr() as usize;
        let layout = &<Self as HasLayout>::LAYOUT;
        if address % layout.alignment != 0 {
            return Err(FromBytesError::InvalidAlignment);
        }
        let len = layout::validate_size(layout, bytes.len())?;
        Ok(unsafe { std::slice::from_raw_parts(address as *const T, len) })
    }

    fn mut_from_bytes(bytes: &mut [u8]) -> Result<&mut Self, FromBytesError> {
        let address = bytes.as_ptr() as usize;
        let layout = &<Self as HasLayout>::LAYOUT;
        if address % layout.alignment != 0 {
            return Err(FromBytesError::InvalidAlignment);
        }
        let len = layout::validate_size(layout, bytes.len())?;
        Ok(unsafe { std::slice::from_raw_parts_mut(address as *mut T, len) })
    }
}

impl<T: HasLayout + IntoBytes> IntoBytes for [T] {
    fn as_bytes(&self) -> &[u8] {
        let len = self.len() * std::mem::size_of::<T>();
        unsafe { std::slice::from_raw_parts(self.as_ptr() as *const u8, len) }
    }
}
