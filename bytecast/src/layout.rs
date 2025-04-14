use std::num::NonZeroUsize;

use crate::FromBytesError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Layout {
    /// For unsized types, this is the tail offset, which may not be a
    /// multiple of alignment.
    pub size: usize,
    pub alignment: NonZeroUsize,
    pub has_trap_values: bool,
    pub has_padding: bool,
    /// For slice types, this is the element size of the trailing slice.
    /// Otherwise this is zero.
    pub tail_stride: usize,
}

/// Allows constructing a pointer in a way that is generic over the pointer
/// layout. This is implemented by `usize` for thin pointers and `(usize,
/// usize)` for fat pointers.
pub trait DestructuredPointer: Copy {
    fn to_address_and_len(self) -> (usize, usize);
    fn from_address_and_len(address: usize, len: usize) -> Self;
}

impl DestructuredPointer for usize {
    fn to_address_and_len(self) -> (usize, usize) {
        (self, 0)
    }

    fn from_address_and_len(address: usize, _len: usize) -> Self {
        address
    }
}

impl DestructuredPointer for (usize, usize) {
    fn to_address_and_len(self) -> (usize, usize) {
        self
    }

    fn from_address_and_len(address: usize, len: usize) -> Self {
        (address, len)
    }
}

/// # Safety
///
/// While implementing this trait cannot directly lead to undefined behavior,
/// the derive macros for `FromBytes` and `IntoBytes` are safe, yet they will
/// generate incorrect code if a type's `HasLayout` implementation is
/// incorrect.
pub unsafe trait HasLayout {
    type DestructuredPointer: DestructuredPointer;

    const LAYOUT: Layout;
}

pub const fn compute_layout_packed(fields: &[Layout]) -> Layout {
    let mut trap: bool = false;
    let mut has_padding: bool = false;
    let mut offset = 0;
    let mut i = 0;
    while i < fields.len() {
        let field = &fields[i];
        trap = trap || field.has_trap_values;
        has_padding = has_padding || field.has_padding;
        offset += field.size;
        assert!(field.tail_stride == 0, "unsized field not allowed in packed struct");
        i += 1;
    }
    Layout {
        size: offset,
        alignment: NonZeroUsize::new(1).unwrap(),
        has_trap_values: trap,
        has_padding,
        tail_stride: 0,
    }
}

/// Calculates the layout of a type from its member fields.
pub const fn compute_layout(fields: &[Layout]) -> Layout {
    const fn align_up(alignment: usize, offset: usize) -> usize {
        ((offset + alignment - 1) / alignment) * alignment
    }

    let mut trap: bool = false;
    let mut has_padding: bool = false;
    let mut offset = 0;
    let mut alignment = 1;
    let mut i = 0;
    let mut tail_stride = 0;
    while i < fields.len() {
        let field = &fields[i];
        trap = trap || field.has_trap_values;
        let field_alignment = field.alignment.get();
        if offset % field_alignment != 0 {
            has_padding = true;
            offset = align_up(field_alignment, offset) + field.size;
        } else {
            has_padding = has_padding || field.has_padding;
            offset += field.size;
        }
        alignment = if field_alignment > alignment { field_alignment } else { alignment };
        if i == fields.len() - 1 {
            tail_stride = field.tail_stride;
        } else {
            assert!(field.tail_stride == 0, "unsized type must be in tail position");
        }
        i += 1;
    }
    let size = if tail_stride > 0 {
        // Size is just tail offset
        offset
    } else {
        if offset % alignment != 0 {
            offset = align_up(alignment, offset);
            has_padding = true;
        }
        offset
    };
    Layout {
        size,
        alignment: NonZeroUsize::new(alignment).unwrap(),
        has_trap_values: trap,
        has_padding,
        tail_stride,
    }
}

pub(crate) const fn validate_size(layout: &Layout, size: usize) -> Result<usize, FromBytesError> {
    if size % layout.alignment.get() != 0 || size < layout.size {
        return Err(FromBytesError::InvalidSize);
    }
    if layout.tail_stride == 0 {
        if size > layout.size {
            return Err(FromBytesError::InvalidSize);
        }
        Ok(0)
    } else {
        let tail_len = size - layout.size;
        if tail_len % layout.tail_stride != 0 {
            return Err(FromBytesError::InvalidSize);
        }
        Ok(tail_len / layout.tail_stride)
    }
}

pub fn destructured_pointer_from_bytes<T: HasLayout + ?Sized>(
    bytes: &[u8],
) -> Result<T::DestructuredPointer, crate::FromBytesError> {
    let address = bytes.as_ptr() as usize;
    let layout = &T::LAYOUT;
    if address % layout.alignment != 0 {
        return Err(FromBytesError::InvalidAlignment);
    }
    let len = validate_size(layout, bytes.len())?;
    Ok(T::DestructuredPointer::from_address_and_len(address, len))
}

pub fn bytes_from_destructured_pointer<'a, T: HasLayout + ?Sized>(
    destructured: T::DestructuredPointer,
) -> *const [u8] {
    let layout = &T::LAYOUT;
    let (address, len) = destructured.to_address_and_len();
    // Slice DSTs are a little bit weird because this is *not* the size
    // reported by `std::mem::size_of_val`, as the latter is always rounded up
    // to the nearest multiple of the type's alignment.
    let size = layout.size + layout.tail_stride * len;
    std::ptr::slice_from_raw_parts(address as *const u8, size)
}

macro_rules! has_layout_primitive {
    ($Type:ty) => {
        has_layout_primitive!($Type; @trap false);
    };
    ($Type:ty; @trap) => {
        has_layout_primitive!($Type; @trap true);
    };
    ($Type:ty; @trap $trap:expr) => {
        unsafe impl HasLayout for $Type {
            type DestructuredPointer = usize;

            const LAYOUT: Layout = Layout {
                size: std::mem::size_of::<$Type>(),
                alignment: NonZeroUsize::new(std::mem::align_of::<$Type>()).unwrap(),
                has_trap_values: $trap,
                has_padding: false,
                tail_stride: 0,
            };
        }
    };
}

has_layout_primitive!(u8);
has_layout_primitive!(u16);
has_layout_primitive!(u32);
has_layout_primitive!(u64);
has_layout_primitive!(u128);
has_layout_primitive!(usize);
has_layout_primitive!(i8);
has_layout_primitive!(i16);
has_layout_primitive!(i32);
has_layout_primitive!(i64);
has_layout_primitive!(i128);
has_layout_primitive!(isize);
has_layout_primitive!(bool; @trap);

unsafe impl<const N: usize, T: HasLayout> HasLayout for [T; N] {
    type DestructuredPointer = usize;

    const LAYOUT: Layout = Layout {
        size: N * T::LAYOUT.size,
        alignment: T::LAYOUT.alignment,
        has_trap_values: T::LAYOUT.has_trap_values,
        has_padding: T::LAYOUT.has_padding,
        tail_stride: 0,
    };
}

unsafe impl<T: HasLayout> HasLayout for [T] {
    type DestructuredPointer = (usize, usize);

    const LAYOUT: Layout = Layout {
        size: 0,
        alignment: T::LAYOUT.alignment,
        has_trap_values: T::LAYOUT.has_trap_values,
        has_padding: T::LAYOUT.has_padding,
        tail_stride: T::LAYOUT.size,
    };
}

unsafe impl HasLayout for () {
    type DestructuredPointer = usize;

    const LAYOUT: Layout = Layout {
        size: 0,
        alignment: NonZeroUsize::new(1).unwrap(),
        has_trap_values: false,
        has_padding: false,
        tail_stride: 0,
    };
}

macro_rules! impl_has_layout_tuple {
    ($($Tn:ident),*) => {
        unsafe impl<$($Tn: HasLayout,)*> HasLayout for ($($Tn,)*) {
            type DestructuredPointer = usize;

            const LAYOUT: Layout = compute_layout(&[$($Tn::LAYOUT,)*]);
        }
    };
}

impl_has_layout_tuple!(T1, T2);
impl_has_layout_tuple!(T1, T2, T3);
impl_has_layout_tuple!(T1, T2, T3, T4);
impl_has_layout_tuple!(T1, T2, T3, T4, T5);
impl_has_layout_tuple!(T1, T2, T3, T4, T5, T6);

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
