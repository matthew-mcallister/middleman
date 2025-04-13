use quote::quote;
use syn::{Ident, ItemStruct, parse_macro_input};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// If `Wrapper` is a `repr(transparent)` tuple struct wrapping a `T`
/// object, this will implement `Wrapper: Cast<T>`. The other direction,
/// `T: Cast<Wrapper>`, is not included because orphan trait rules prevent
/// it from being implemented in most cases.
#[proc_macro_derive(Cast)]
pub fn derive_cast(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let repr = get_repr(&input);
    assert_eq!(repr, Some(Repr::Transparent), "repr must 'transparent'");

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let name = &input.ident;
    assert_eq!(input.fields.len(), 1);
    let inner_type = &input.fields.iter().next().unwrap().ty;

    let impl_tokens = quote! {
        unsafe impl #impl_generics ::cast::Cast<#inner_type> for #name #ty_generics #where_clause {}
    };

    impl_tokens.into()
}
