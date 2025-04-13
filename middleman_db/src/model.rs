#[macro_export]
macro_rules! big_tuple_struct {
    (
        $(#[$($meta:tt)*])*
        $vis:vis struct $Name:ident {
            $($field_vis:vis $field:ident[$index:expr]: $Field:ty),*$(,)?
        }
    ) => {
        $(#[$($meta)*])*
        #[derive(::bytecast::FromBytes, ::bytecast::IntoBytes, ::bytecast::HasLayout, ::cast::Cast)]
        #[repr(transparent)]
        $vis struct $Name($crate::big_tuple::BigTuple);

        impl $Name {
            fn new(
                $($field: &$Field,)*
            ) -> Box<Self> {
                let fields = &[$(bytecast::IntoBytes::as_bytes($field),)*];
                let mut info = $crate::big_tuple::BigTupleCreateInfo::default();
                info.aligned = true;
                info.fields = fields;
                let tuple = $crate::big_tuple::BigTuple::new(info);
                ::cast::cast_from(tuple)
            }

            $(
                $field_vis fn $field(&self) -> &$Field {
                    self.0.get_as($index)
                }
            )*
        }

        impl std::fmt::Debug for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($Name))
                    $(.field(stringify!($field), &self.$field()))*
                    .finish()
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


        impl std::borrow::ToOwned for $Name {
            type Owned = Box<Self>;

            fn to_owned(&self) -> Box<Self> {
                ::cast::cast_from(self.0.to_owned())
            }
        }
    };
}

pub use big_tuple_struct;
