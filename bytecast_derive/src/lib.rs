use quote::quote;
use syn::{ItemStruct, LitInt, parenthesized, parse_macro_input};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Repr {
    Packed,
    C,
    Transparent,
}

fn parse_repr_attrs(input: &syn::ItemStruct) -> Option<(Repr, Option<usize>)> {
    let mut repr: Option<Repr> = None;
    let mut alignment: Option<usize> = None;

    for attr in &input.attrs {
        if attr.path().is_ident("repr") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("C") {
                    assert!(repr.is_none(), "invalid repr");
                    repr = Some(Repr::C);
                    return Ok(());
                }

                if meta.path.is_ident("transparent") {
                    assert!(repr.is_none(), "invalid repr");
                    repr = Some(Repr::Transparent);
                    return Ok(());
                }

                if meta.path.is_ident("align") {
                    assert!(alignment.is_none(), "invalid repr");
                    let content;
                    parenthesized!(content in meta.input);
                    let lit: LitInt = content.parse()?;
                    let n: usize = lit.base10_parse()?;
                    alignment = Some(n);
                    return Ok(());
                }

                if meta.path.is_ident("packed") {
                    assert!(repr.is_none(), "invalid repr");
                    if meta.input.peek(syn::token::Paren) {
                        panic!("unsupported repr");
                    } else {
                        repr = Some(Repr::Packed);
                    }
                    return Ok(());
                }

                Err(meta.error("invalid repr"))
            })
            .expect("invalid repr");
        }
    }

    let repr = repr?;
    match repr {
        Repr::Packed | Repr::Transparent if alignment.is_some() => panic!("unsupported repr"),
        _ => {},
    }

    Some((repr, alignment))
}

#[proc_macro_derive(HasLayout)]
pub fn derive_has_layout(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let type_params = input.generics.type_params().map(|p| &p.ident);
    let extra_predicates = where_clause.map(|c| &c.predicates);
    let name = &input.ident;

    let (repr, min_align) =
        parse_repr_attrs(&input).expect("repr must be one of: C, packed, transparent");
    let min_align = min_align.unwrap_or(1);
    let field_types_vec: Vec<_> = match &input.fields {
        syn::Fields::Named(fs) => fs.named.iter().map(|field| &field.ty).collect(),
        syn::Fields::Unnamed(fs) => fs.unnamed.iter().map(|field| &field.ty).collect(),
        syn::Fields::Unit => Vec::new(),
    };
    let field_types = &field_types_vec[..];
    let tail_type = &field_types[field_types.len() - 1];

    let impl_tokens = match repr {
        Repr::Transparent | Repr::C => quote! {
            unsafe impl #impl_generics ::bytecast::layout::HasLayout for #name #ty_generics
            where
                #(#type_params: ::bytecast::HasLayout,)*
                #extra_predicates
            {
                type DestructuredPointer = <#tail_type as ::bytecast::layout::HasLayout>::DestructuredPointer;

                const LAYOUT: ::bytecast::layout::Layout = ::bytecast::layout::compute_layout_with_alignment(&[
                    #(<#field_types as ::bytecast::layout::HasLayout>::LAYOUT,)*
                ], #min_align);
            }
        },
        Repr::Packed => quote! {
            unsafe impl #impl_generics ::bytecast::layout::HasLayout for #name #ty_generics
            where
                #(#type_params: ::bytecast::HasLayout,)*
                #extra_predicates
            {
                type DestructuredPointer = <#tail_type as ::bytecast::layout::HasLayout>::DestructuredPointer;

                const LAYOUT: ::bytecast::layout::Layout = ::bytecast::layout::compute_layout_packed(&[
                    #(<#field_types as ::bytecast::layout::HasLayout>::LAYOUT,)*
                ]);
            }
        },
    };

    impl_tokens.into()
}

#[proc_macro_derive(FromBytes)]
pub fn derive_from_bytes(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let type_params: Vec<_> = input.generics.type_params().map(|p| &p.ident).collect();
    let extra_predicates = where_clause.map(|c| &c.predicates);
    let name = &input.ident;

    let impl_tokens = quote! {
        impl #impl_generics #name #ty_generics
        where
            #(#type_params: ::bytecast::HasLayout + ::bytecast::IntoBytes,)*
            #extra_predicates
        {
                const ASSERT_1: () = assert!(
                    !<#name #ty_generics as ::bytecast::layout::HasLayout>::LAYOUT.has_trap_values,
                    concat!("type ", stringify!(#name #ty_generics), " has trap values"),
                );

                const ASSERT_2: () = assert!(
                    {
                        let layout = &<#name #ty_generics as ::bytecast::layout::HasLayout>::LAYOUT;
                        layout.tail_stride == 0 || (
                            layout.size % layout.alignment.get() == 0
                            && layout.tail_stride % layout.alignment.get() == 0
                        )
                    },
                    // Unfortunately, tail padding may result in undefined behavior.
                    concat!("type ", stringify!(#name #ty_generics), " has tail padding"),
                );
        }

        impl #impl_generics ::bytecast::FromBytes for #name #ty_generics
        where
            #(#type_params: ::bytecast::HasLayout + ::bytecast::FromBytes,)*
            #extra_predicates
        {
            fn ref_from_bytes(bytes: &[u8]) -> ::std::result::Result<&Self, ::bytecast::FromBytesError> {
                let destructured = ::bytecast::layout::destructured_pointer_from_bytes::<Self>(bytes)?;
                unsafe {
                    let ptr: *const Self = *(&destructured as *const _ as *const *const Self);
                    Ok(&*ptr)
                }
            }

            fn mut_from_bytes(bytes: &mut [u8]) -> ::std::result::Result<&mut Self, ::bytecast::FromBytesError> {
                let destructured = ::bytecast::layout::destructured_pointer_from_bytes::<Self>(bytes)?;
                unsafe {
                    let ptr: *mut Self = *(&destructured as *const _ as *const *mut Self);
                    Ok(&mut *ptr)
                }
            }
        }
    };

    impl_tokens.into()
}

#[proc_macro_derive(IntoBytes)]
pub fn derive_into_bytes(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let type_params: Vec<_> = input.generics.type_params().map(|p| &p.ident).collect();
    let extra_predicates = where_clause.map(|c| &c.predicates);
    let name = &input.ident;

    let impl_tokens = quote! {
        impl #impl_generics #name #ty_generics
        where
            #(#type_params: ::bytecast::HasLayout + ::bytecast::IntoBytes,)*
            #extra_predicates
        {
            const ASSERT_3: () = assert!(
                !<#name #ty_generics as ::bytecast::layout::HasLayout>::LAYOUT.has_padding,
                concat!("type ", stringify!(#name #ty_generics), " has interior padding"),
            );
        }

        impl #impl_generics ::bytecast::IntoBytes for #name #ty_generics
        where
            #(#type_params: ::bytecast::HasLayout + ::bytecast::IntoBytes,)*
            #extra_predicates
        {
            fn as_bytes(&self) -> &[u8] {
                unsafe {
                    let this = self as *const Self;
                    let destructured = *(&this as *const *const Self as *const <Self as ::bytecast::layout::HasLayout>::DestructuredPointer);
                    &*::bytecast::layout::bytes_from_destructured_pointer::<Self>(destructured)
                }
            }
        }
    };

    impl_tokens.into()
}
