/// Constructs a [u8; N] array from arbitrary inputs. Integers are
/// written in big-endian order.
#[macro_export]
macro_rules! make_key {
    ($($ty:ty: $val:expr),*$(,)?) => {{
        use std::mem::MaybeUninit;
        const SIZE: usize = 0 $(+ std::mem::size_of::<$ty>())*;
        let mut buf: MaybeUninit<[u8; SIZE]> = MaybeUninit::uninit();
        let mut _ptr = &mut buf as *mut MaybeUninit<[u8; SIZE]> as *mut u8;
        $(
            let val: $ty = $val;
            let data = val.to_be_bytes();
            unsafe {
                _ptr.copy_from(data.as_ptr(), data.len());
                _ptr = _ptr.offset(data.len() as _);
            }
        )*
        unsafe { buf.assume_init() }
    }};
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

#[cfg(test)]
mod tests {
    use crate::util::ByteCast;

    #[test]
    fn test_make_key() {
        assert_eq!(make_key!(u8: 1, u32: 2, u8: 3), [1, 0, 0, 0, 2, 3]);
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
