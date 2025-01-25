macro_rules! big_tuple_struct {
    (
        $(#[$($meta:tt)*])*
        $vis:vis struct $Name:ident {
            $($field_vis:vis $field:ident[$index:expr]: $Field:ty),*$(,)?
        }
    ) => {
        $(#[$($meta)*])*
        #[repr(transparent)]
        $vis struct $Name($crate::big_tuple::BigTuple);

        impl $Name {
            fn new(
                $($field: &$Field,)*
            ) -> Box<Self> {
                let info = $crate::big_tuple::BigTupleCreateInfo {
                    aligned: true,
                    fields: &[$($crate::bytes::AsBytes::as_bytes($field),)*],
                    ..Default::default()
                };
                let tuple = $crate::big_tuple::BigTuple::new(info);
                unsafe { std::mem::transmute(tuple) }
            }

            $(
                $field_vis fn $field(&self) -> &$Field {
                    unsafe {
                        <$Field as $crate::bytes::FromBytesUnchecked>::ref_from_bytes_unchecked(self.0.get($index))
                    }
                }
            )*
        }

        impl std::fmt::Debug for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("Blah")
                    $(.field(stringify!($field), &self.$field()))*
                    .finish()
            }
        }

        unsafe impl $crate::bytes::AsBytes for $Name {
            fn as_bytes(this: &Self) -> &[u8] {
                $crate::bytes::AsBytes::as_bytes(&this.0)
            }
        }

        impl $crate::bytes::AsRawBytes for $Name {
            fn as_raw_bytes(&self) -> &[std::mem::MaybeUninit<u8>] {
                self.0.as_raw_bytes()
            }
        }

        impl $crate::bytes::FromBytesUnchecked for $Name {
            unsafe fn ref_from_bytes_unchecked(bytes: &[u8]) -> &Self {
                unsafe { std::mem::transmute($crate::big_tuple::BigTuple::ref_from_bytes_unchecked(bytes)) }
            }

            unsafe fn mut_from_bytes_unchecked(bytes: &mut [u8]) -> &mut Self {
                unsafe { std::mem::transmute($crate::big_tuple::BigTuple::mut_from_bytes_unchecked(bytes)) }
            }
        }

        impl ToOwned for $Name {
            type Owned = Box<Self>;

            fn to_owned(&self) -> Self::Owned {
                let src = AsBytes::as_bytes(self);
                unsafe { <Self as $crate::bytes::FromBytesUnchecked>::box_from_bytes_unchecked(src.to_owned()) }
            }
        }

        impl std::cmp::PartialEq for $Name {
            fn eq(&self, other: &Self) -> bool {
                true $(&& self.$field() == other.$field())*
            }
        }

        impl std::cmp::Eq for $Name {}

        impl $crate::prefix::IsPrefixOf<$Name> for $crate::big_tuple::BigTuple {
            fn is_prefix_of(&self, key: &$Name) -> bool {
                self.is_prefix_of(&key.0)
            }
        }
    };
}

pub(crate) use big_tuple_struct;
