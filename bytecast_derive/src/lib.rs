use quote::quote;
use syn::{Ident, ItemStruct, parse_macro_input};

enum Repr {
    Packed,
    C,
    Transparent,
}

fn get_repr(input: &ItemStruct) -> Option<Repr> {
    for attr in input.attrs.iter() {
        if attr.path().is_ident("repr") {
            let ident: Ident = attr.parse_args().ok()?;
            return match ident.to_string().as_ref() {
                "C" => Some(Repr::C),
                "packed" => Some(Repr::Packed),
                "transparent" => Some(Repr::Transparent),
                _ => None,
            };
        }
    }
    None
}

#[proc_macro_derive(HasLayout)]
pub fn derive_has_layout(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    // We could certainly support generics with some added effort
    assert!(input.generics.params.is_empty(), "generics not currently supported");
    let name = &input.ident;

    let repr = get_repr(&input).expect("repr must be one of: C, packed, transparent");
    let field_types_vec: Vec<_> = match &input.fields {
        syn::Fields::Named(fs) => fs.named.iter().map(|field| &field.ty).collect(),
        syn::Fields::Unnamed(fs) => fs.unnamed.iter().map(|field| &field.ty).collect(),
        syn::Fields::Unit => Vec::new(),
    };
    let field_types = &field_types_vec[..];
    // We could support this but it's an edge case
    assert!(!field_types.is_empty(), "empty struct not currently supported");
    let tail_type = &field_types[field_types.len() - 1];

    let impl_tokens = match repr {
        Repr::Transparent | Repr::C => quote! {
            impl ::bytecast::layout::HasLayout for #name {
                type DestructuredPointer = <#tail_type as ::bytecast::layout::HasLayout>::DestructuredPointer;

                const LAYOUT: ::bytecast::layout::Layout = ::bytecast::layout::compute_layout(&[
                    #(<#field_types as ::bytecast::layout::HasLayout>::LAYOUT,)*
                ]);
            }
        },
        Repr::Packed => quote! {
            impl ::bytecast::layout::HasLayout for #name {
                type DestructuredPointer = <#tail_type as ::bytecast::layout::HasLayout>::DestructuredPointer;

                const LAYOUT: ::bytecast::layout::Layout = ::bytecast::layout::compute_layout_packed(&[#(#field_types,)*]);
            }
        },
    };

    impl_tokens.into()
}

#[proc_macro_derive(FromBytes)]
pub fn derive_from_bytes(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    assert!(input.generics.params.is_empty(), "generics not currently supported");
    let name = &input.ident;

    let impl_tokens = quote! {
        const _: () = assert!(
            !<#name as ::bytecast::layout::HasLayout>::LAYOUT.has_trap_values,
            concat!("type ", stringify!(#name), " has trap values"),
        );

        const _: () = assert!(
            const {
                let layout = &<#name as ::bytecast::layout::HasLayout>::LAYOUT;
                layout.tail_stride == 0 || layout.tail_stride == layout.alignment.get()
            },
            // Unfortunately, tail padding may result in undefined behavior.
            concat!("type ", stringify!(#name), " has tail padding"),
        );

        impl ::bytecast::TryFromBytes for #name {
            fn try_ref_from_bytes(bytes: &[u8]) -> ::std::result::Result<&Self, ::bytecast::TryFromBytesError> {
                let destructured = ::bytecast::layout::destructured_pointer_from_bytes::<#name>(bytes)?;
                unsafe {
                    let ptr: *const Self = ::std::mem::transmute(destructured);
                    Ok(&*ptr)
                }
            }

            fn try_mut_from_bytes(bytes: &mut [u8]) -> ::std::result::Result<&mut Self, ::bytecast::TryFromBytesError> {
                let destructured = ::bytecast::layout::destructured_pointer_from_bytes::<#name>(bytes)?;
                unsafe {
                    let ptr: *mut Self = ::std::mem::transmute(destructured);
                    Ok(&mut *ptr)
                }
            }
        }

        impl ::bytecast::FromBytes for #name {}
    };

    impl_tokens.into()
}

#[proc_macro_derive(IntoBytes)]
pub fn derive_into_bytes(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    assert!(input.generics.params.is_empty(), "generics not currently supported");
    let name = &input.ident;

    let impl_tokens = quote! {
        const _: () = assert!(
            !<#name as ::bytecast::layout::HasLayout>::LAYOUT.has_padding,
            concat!("type ", stringify!(#name), " has interior padding"),
        );

        impl ::bytecast::IntoBytes for #name {
            fn as_bytes(&self) -> &[u8] {
                unsafe {
                    let destructured: <#name as ::bytecast::layout::HasLayout>::DestructuredPointer = std::mem::transmute(self);
                    &*::bytecast::layout::bytes_from_destructured_pointer::<#name>(destructured)
                }
            }
        }
    };

    impl_tokens.into()
}
