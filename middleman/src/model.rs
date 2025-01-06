use std::mem::MaybeUninit;

use crate::util::ByteCast;

macro_rules! addr_of_field_mut {
    ($ptr:expr, $field:ident) => {
        std::ptr::addr_of_mut!((*$ptr).$field)
    };
}

fn compute_offsets<const N: usize>(fields: &[&[u8]; N]) -> [u32; N] {
    let mut offsets = [0u32; N];
    let mut running_offset = 0;
    for (&field, offset) in fields.iter().zip(offsets.iter_mut()) {
        running_offset += field.len() as u32;
        *offset = running_offset;
    }
    offsets
}

/// Models structured data with a fixed-size header `H` and a fixed number
/// `N + 1` of variable-sized fields. There is always at least 1 variable-sized
/// field.
// XXX: Investigate using rocksdb wide column support
#[derive(Debug, Eq, PartialEq)]
#[repr(C)]
pub struct VariableSizeModel<H, const N: usize> {
    pub header: H,
    // Allows another header to be placed after the tail for future
    // extensions and optional extra data.
    next_header: u32,
    offsets: [u32; N],
    tail_start: [u8; 0],
    tail: [u8],
}

impl<H, const N: usize> VariableSizeModel<H, N> {
    pub fn new(header: H, fields: &[&[u8]; N]) -> Box<Self> {
        unsafe {
            let tail_len: usize = fields.iter().map(|f| f.len()).sum();
            // XXX: Probably should return an error here instead of panicking
            assert!(tail_len <= u32::MAX as _);
            let len = std::mem::offset_of!(Self, tail_start) + tail_len;
            let mut buffer: Vec<u8> = Vec::with_capacity(len);

            let ptr = buffer.as_mut_ptr();
            let ptr: *mut Self = std::mem::transmute((ptr, 0usize));

            addr_of_field_mut!(ptr, header).write(header);
            addr_of_field_mut!(ptr, next_header).write(0);
            addr_of_field_mut!(ptr, offsets).write(compute_offsets(fields));

            let mut ptr = addr_of_field_mut!(ptr, tail_start) as *mut u8;
            for field in fields.iter() {
                ptr.copy_from(field.as_ptr(), field.len());
                ptr = ptr.offset(field.len() as _);
            }

            buffer.set_len(len);
            <Self as ByteCast>::from_bytes_owned(buffer)
        }
    }

    pub fn header(&self) -> &H {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut H {
        &mut self.header
    }

    pub fn get<const I: usize>(&self) -> &[u8] {
        if N == 0 {
            &self.tail[..]
        } else if I == 0 {
            &self.tail[..self.offsets[I] as _]
        } else if I == N {
            &self.tail[self.offsets[I] as _..]
        } else {
            &self.tail[self.offsets[I - 1] as _..self.offsets[I] as _]
        }
    }

    /// Work around `std::mem::align_of` being broken for DSTs.
    const fn align_of() -> usize {
        let (a1, a2) = (std::mem::align_of::<H>(), std::mem::align_of::<u32>());
        if a1 > a2 {
            a1
        } else {
            a2
        }
    }

    /// Validate the length and alignment of a byte slice and returns the
    /// length of the tail.
    fn validate_bytes(bytes: &[u8]) -> usize {
        assert!(bytes.len() >= std::mem::offset_of!(Self, tail_start));
        assert_eq!(bytes.as_ptr() as usize % Self::align_of(), 0);
        bytes.len() - std::mem::offset_of!(Self, tail_start)
    }
}

impl<H, const N: usize> ByteCast for VariableSizeModel<H, N> {
    fn as_bytes(this: &Self) -> &[MaybeUninit<u8>] {
        unsafe {
            let start = this as *const Self as *const u8;
            let end = this.tail.as_ptr().offset(this.tail.len() as _);
            let len = end.offset_from(start) as usize;
            std::slice::from_raw_parts(start as *const MaybeUninit<u8>, len)
        }
    }

    unsafe fn from_bytes(bytes: &[u8]) -> &Self {
        let len = Self::validate_bytes(bytes);
        unsafe { std::mem::transmute((bytes.as_ptr(), len)) }
    }

    unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
        let len = Self::validate_bytes(bytes);
        unsafe { std::mem::transmute((bytes.as_mut_ptr(), len)) }
    }
}

#[macro_export]
macro_rules! variable_size_model {
    (
        $(#[$($meta:tt)*])*
        $vis:vis struct $Struct:ident {
            $($field:ident: $FieldTy:ty,)*
            $([$var_field_vis:vis $var_field:ident: $var_index:expr]: $VarFieldTy:ty),+$(,)?
        }
    ) => {
        paste::paste! {
            $(#[$($meta)*])*
            #[repr(C)]
            struct [<$Struct Header>] {
                $($field: $FieldTy,)*
            }
        }

        paste::paste! {
            type [<$Struct Inner>] = crate::model::VariableSizeModel<
                [<$Struct Header>],
                { 0 $(+ $crate::cnst!($var_field, 1))* },
            >;
        }

        paste::paste! {
            $(#[$($meta)*])*
            #[repr(transparent)]
            $vis struct $Struct([<$Struct Inner>]);
        }

        impl $Struct {
            $(
                $var_field_vis fn $var_field(&self) -> &$VarFieldTy {
                    unsafe {
                        <$VarFieldTy as $crate::util::ByteCast>::from_bytes(self.0.get::<$var_index>())
                    }
                }
            )*
        }

        paste::paste! {
            impl AsRef<[<$Struct Inner>]> for $Struct {
                fn as_ref(&self) -> &[<$Struct Inner>] {
                    &self.0
                }
            }

            impl From<Box<[<$Struct Inner>]>> for Box<$Struct> {
                fn from(value: Box<[<$Struct Inner>]>) -> Box<$Struct> {
                    unsafe { std::mem::transmute(value) }
                }
            }
        }

        impl AsRef<[u8]> for $Struct {
            fn as_ref(&self) -> &[u8] {
                unsafe { std::mem::transmute($crate::util::ByteCast::as_bytes(&self.0)) }
            }
        }

        impl $crate::util::ByteCast for $Struct {
            fn as_bytes(this: &Self) -> &[std::mem::MaybeUninit<u8>] {
                $crate::util::ByteCast::as_bytes(&this.0)
            }

            unsafe fn from_bytes(bytes: &[u8]) -> &Self {
                unsafe { std::mem::transmute(
                    <paste::paste!([<$Struct Inner>]) as $crate::util::ByteCast>::from_bytes(bytes)
                ) }
            }

            unsafe fn from_bytes_mut(bytes: &mut [u8]) -> &mut Self {
                unsafe { std::mem::transmute(
                    <paste::paste!([<$Struct Inner>]) as $crate::util::ByteCast>::from_bytes_mut(bytes)
                ) }
            }
        }
    };
}
