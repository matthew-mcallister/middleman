use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{parse_macro_input, Index, ItemStruct};

#[proc_macro_attribute]
pub fn db_key(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let struct_name = &input.ident;

    let index = input.fields.iter().enumerate().map(|(i, _)| Index::from(i));
    let field = input.fields.iter().map(|field| &field.ident);
    let field_ty = input.fields.iter().map(|field| &field.ty);
    let type_var: Vec<_> = input
        .fields
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let var_name = format!("T{}", i);
            Ident::new(&var_name, Span::call_site())
        })
        .collect();

    let expanded = quote! {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        #[repr(packed)]
        #input

        impl<#(#type_var,)*> From<(#(#type_var,)*)> for #struct_name
        where
            #(#field_ty: From<#type_var>,)*
        {
            fn from(value: (#(#type_var,)*)) -> Self {
                #struct_name {
                    #(#field: value.#index.into(),)*
                }
            }
        }

        unsafe impl AsBytes for #struct_name {}

        impl AsRef<[u8]> for #struct_name {
            fn as_ref(&self) -> &[u8] {
                <Self as crate::bytes::AsBytes>::as_bytes(self)
            }
        }
    };

    expanded.into()
}
