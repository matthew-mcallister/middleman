pub mod layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TryFromBytesError {
    InvalidSize,
    InvalidAlignment,
    InvalidRepresentation,
}

impl TryFromBytesError {
    fn assert_valid_representation(self) -> FromBytesError {
        match self {
            Self::InvalidSize => FromBytesError::InvalidSize,
            Self::InvalidAlignment => FromBytesError::InvalidAlignment,
            Self::InvalidRepresentation => unreachable!(),
        }
    }
}

impl From<FromBytesError> for TryFromBytesError {
    fn from(value: FromBytesError) -> Self {
        match value {
            FromBytesError::InvalidAlignment => TryFromBytesError::InvalidAlignment,
            FromBytesError::InvalidSize => TryFromBytesError::InvalidSize,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FromBytesError {
    InvalidSize,
    InvalidAlignment,
}

pub trait TryFromBytes {
    // XXX: This API has the same underlying problem as `try_box_from_bytes`,
    // and the padding bytes of `&Self` could overlap with adjacent initialized
    // bytes, which could cause UB in LLVM. We work around this in the derive
    // crate by asserting that the tail has no padding.
    fn try_ref_from_bytes(bytes: &[u8]) -> Result<&Self, TryFromBytesError>;
    fn try_mut_from_bytes(bytes: &mut [u8]) -> Result<&mut Self, TryFromBytesError>;
}

pub trait FromBytes: TryFromBytes {
    fn ref_from_bytes(bytes: &[u8]) -> Result<&Self, FromBytesError> {
        Self::try_ref_from_bytes(bytes).map_err(|e| e.assert_valid_representation())
    }

    fn mut_from_bytes(bytes: &mut [u8]) -> Result<&mut Self, FromBytesError> {
        Self::try_mut_from_bytes(bytes).map_err(|e| e.assert_valid_representation())
    }
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

// XXX: Unfortunately, for slice DSTs, this API is not technically sound in
// that `bytes` was not allocated with the same alignment as `T`, meaning the
// `T` value might overflow the allocation after its size is padded to a
// multiple of its alignment. In practice, memory allocators must guarantee
// allocations are a multiple of the coarsest possible alignment for a system
// (generally 8/16 bytes) in order to be compatible with C, preventing such
// overflow.
pub fn try_box_from_bytes<T: TryFromBytes + ?Sized>(
    mut bytes: Box<[u8]>,
) -> std::result::Result<Box<T>, (TryFromBytesError, Box<[u8]>)> {
    let ptr = match T::try_mut_from_bytes(&mut bytes[..]) {
        Ok(r) => r as *mut T,
        Err(e) => return Err((e, bytes)),
    };
    std::mem::forget(bytes);
    unsafe { Ok(Box::from_raw(ptr)) }
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

#[cfg(test)]
mod tests {
    use crate::layout::{HasLayout, compute_layout};

    #[test]
    fn test_layout_with_padding() {
        let layout = compute_layout(&[u32::LAYOUT, u16::LAYOUT, u32::LAYOUT]);
        assert_eq!(layout.size, 12);
        assert_eq!(layout.alignment.get(), 4);
        assert_eq!(layout.has_trap_values, false);
        assert_eq!(layout.has_padding, true);
        assert_eq!(layout.tail_stride, 0);
    }

    #[test]
    fn test_layout_with_trap() {
        let layout = compute_layout(&[bool::LAYOUT]);
        assert_eq!(layout.size, 1);
        assert_eq!(layout.alignment.get(), 1);
        assert_eq!(layout.has_trap_values, true);
        assert_eq!(layout.has_padding, false);
        assert_eq!(layout.tail_stride, 0);
    }

    #[test]
    fn test_slice_dst_layout() {
        let layout = compute_layout(&[u32::LAYOUT, u16::LAYOUT, <[u8]>::LAYOUT]);
        assert_eq!(layout.size, 6);
        assert_eq!(layout.alignment.get(), 4);
        assert_eq!(layout.has_trap_values, false);
        assert_eq!(layout.has_padding, false);
        assert_eq!(layout.tail_stride, 1);

        let layout = compute_layout(&[u32::LAYOUT, u16::LAYOUT, <[u64]>::LAYOUT]);
        assert_eq!(layout.size, 8);
        assert_eq!(layout.alignment.get(), 8);
        assert_eq!(layout.has_trap_values, false);
        assert_eq!(layout.has_padding, true);
        assert_eq!(layout.tail_stride, 8);
    }

    #[test]
    fn test_transparent_layout() {
        let layout1 = compute_layout(&[u32::LAYOUT, u16::LAYOUT, u32::LAYOUT]);
        let layout2 = compute_layout(&[layout1]);
        assert_eq!(layout1, layout2);
    }
}
