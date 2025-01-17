use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{parse_macro_input, Index, ItemStruct, Type};

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

/*
#[proc_macro_attribute]
pub fn model(_args: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let name = &input.ident;
    let vis = &input.vis;

    fn is_sized(ty: &Type) -> bool {
        match ty {
            Type::Array(_) | Type::Tuple(_) => true,
            Type::Slice(slice) => {
                match &*slice.elem {
                    // Due to alignment, other types not currently supported
                    Type::Path(path) if path.path.is_ident("u8") => (),
                    _ => panic!("Invalid type"),
                }
                false
            },
            Type::Path(path) => !path.path.is_ident("str"),
            _ => panic!("Invalid type"),
        }
    }

    let mut sized_fields = Vec::new();
    let mut unsized_fields = Vec::new();
    for field in input.fields.iter() {
        if is_sized(&field.ty) {
            sized_fields.push(field);
        } else {
            unsized_fields.push(field);
        }
    }
    assert!(!unsized_fields.is_empty());

    let sized_field_name: Vec<_> = sized_fields.iter().map(|f| &f.ident).collect();
    let sized_field_type: Vec<_> = sized_fields.iter().map(|f| &f.ty).collect();
    let sized_field_vis: Vec<_> = sized_fields.iter().map(|f| &f.vis).collect();
    let unsized_field_name: Vec<_> = unsized_fields.iter().map(|f| &f.ident).collect();
    let unsized_field_type: Vec<_> = unsized_fields.iter().map(|f| &f.ty).collect();
    let unsized_field_vis: Vec<_> = unsized_fields.iter().map(|f| &f.vis).collect();
    let unsized_field_index = 0..unsized_fields.len();

    let num_offsets = unsized_fields.len() - 1;

    let expanded = quote! {
        #[repr(C)]
        #vis struct #name {
            #(#sized_field_vis #sized_field_name: #sized_field_type,)*
            offsets: [u32; #num_offsets],
            // Provides future compatibility with struct extensions
            next_header: u32,
            _tail_start: [u8; 0],
            tail: [u8],
        }

        impl #name {
            #[allow(unused_variables)]
            fn new(
                #(#sized_field_name: #sized_field_type,)*
                #(#unsized_field_name: &#unsized_field_type,)*
            ) -> Box<Self> {
                let len = 0;
                #(let len = len + std::mem::size_of_val(#unsized_field_name);)*
                assert!(len < u32::MAX as usize);

                let size = Self::size_of_header() + len;
                let mut bytes = Vec::<u8>::with_capacity(size);
                unsafe { bytes.set_len(size); }
                let mut bytes = bytes.into_boxed_slice();

                let mut ptr: *mut Self = unsafe { std::mem::transmute((bytes.as_mut_ptr() as *mut u8, len)) };
                unsafe {
                    #((&raw mut (*ptr).#sized_field_name).write(#sized_field_name);)*

                    let offset = 0;
                    #(
                        let src = #unsized_field_name as *const #unsized_field_type as *const u8;
                        let len = std::mem::size_of_val(#unsized_field_name);
                        let dst: *mut u8 = &raw mut (*ptr).tail[offset];
                        std::ptr::copy_nonoverlapping(src, dst, len);
                        let offset = offset + len;
                    )*

                    let offset = 0;
                    let index = 0;
                    #(
                        let offset = offset + std::mem::size_of_val(#unsized_field_name);
                        if index < #num_offsets {
                            (&raw mut (*ptr).offsets[index]).write(offset as u32);
                        }
                        let index = index + 1;
                    )*

                    (&raw mut (*ptr).next_header).write(0);
                }

                std::mem::forget(bytes);
                unsafe { Box::from_raw(ptr) }
            }

            fn get<const N: usize>(&self) -> &[u8] {
                if #num_offsets == 0 {
                    &self.tail[..]
                } else if N == 0 {
                    &self.tail[..self.offsets[0] as usize]
                } else if N == #num_offsets {
                    &self.tail[self.offsets[#num_offsets - 1] as usize..]
                } else {
                    &self.tail[self.offsets[N - 1] as usize..self.offsets[N] as usize]
                }
            }

            const fn size_of_header() -> usize {
                std::mem::offset_of!(#name, _tail_start)
            }

            const fn align_of() -> usize {
                const fn max(a: usize, b: usize) -> usize {
                    if a > b { a } else { b }
                }

                let alignment = 0;
                #(let alignment = max(alignment, std::mem::align_of::<#sized_field_type>());)*
                alignment
            }

            #(
                #unsized_field_vis fn #unsized_field_name(&self) -> &#unsized_field_type {
                    let bytes = self.get::<#unsized_field_index>();
                    unsafe {
                        <#unsized_field_type as crate::bytes::FromBytesUnchecked>::ref_from_bytes_unchecked(bytes)
                    }
                }
            )*
        }

        impl crate::bytes::FromBytesUnchecked for #name {
            unsafe fn ref_from_bytes_unchecked(bytes: &[u8]) -> &Self {
                debug_assert!(bytes.len() >= Self::size_of_header());
                debug_assert_eq!((bytes.as_ptr() as usize) % Self::align_of(), 0);
                let len = bytes.len() - Self::size_of_header();
                unsafe { std::mem::transmute((bytes.as_ptr(), len)) }
            }

            unsafe fn mut_from_bytes_unchecked(bytes: &mut [u8]) -> &mut Self {
                debug_assert!(bytes.len() >= Self::size_of_header());
                debug_assert_eq!((bytes.as_ptr() as usize) % Self::align_of(), 0);
                let len = bytes.len() - Self::size_of_header();
                unsafe { std::mem::transmute((bytes.as_mut_ptr(), len)) }
            }
        }

        impl crate::bytes::AsRawBytes for #name {
            fn as_raw_bytes(&self) -> &[std::mem::MaybeUninit<u8>] {
                let (_, tail_len): (usize, usize) = unsafe { std::mem::transmute(self) };
                let header_len = Self::size_of_header();
                let len = header_len + tail_len;
                unsafe { std::slice::from_raw_parts(self as *const Self as *const _, len) }
            }
        }
    };

    expanded.into()
}
*/
