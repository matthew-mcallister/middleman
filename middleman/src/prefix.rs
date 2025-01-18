/// Trait for comparing keys with prefixes.
pub trait IsPrefixOf<Rhs: ?Sized = Self> {
    fn is_prefix_of(&self, other: &Rhs) -> bool;
}

macro_rules! impl_primitive_is_prefix_of {
    ($($type:ty)*) => {$(
        impl IsPrefixOf for [$type] {
            fn is_prefix_of(&self, other: &Self) -> bool {
                if other.len() < self.len() {
                    return false;
                }
                self == &other[..self.len()]
            }
        }

        impl IsPrefixOf for $type {
            #[inline(always)]
            fn is_prefix_of(&self, other: &Self) -> bool {
                *self == *other
            }
        }
    )*};
}

impl_primitive_is_prefix_of!(u8 u16 u32 u64 i8 i16 i32 i64);

impl IsPrefixOf for str {
    #[inline(always)]
    fn is_prefix_of(&self, other: &Self) -> bool {
        self.as_bytes().is_prefix_of(other.as_bytes())
    }
}

impl<P: IsPrefixOf<T> + ?Sized, T: ?Sized> IsPrefixOf<T> for Box<P> {
    #[inline(always)]
    fn is_prefix_of(&self, other: &T) -> bool {
        (**self).is_prefix_of(other)
    }
}

impl<T, U, const N: usize> IsPrefixOf<U> for [T; N]
where
    [T]: IsPrefixOf<U>,
{
    #[inline(always)]
    fn is_prefix_of(&self, other: &U) -> bool {
        self[..].is_prefix_of(other)
    }
}
