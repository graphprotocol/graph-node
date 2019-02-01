#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Fields, Item, ItemStruct};

#[proc_macro_derive(AscType)]
pub fn asc_type_derive(input: TokenStream) -> TokenStream {
    let item: Item = syn::parse(input).unwrap();
    match item {
        Item::Struct(item_struct) => asc_type_derive_struct(item_struct),
        //Item::Enum(enum)
        _ => panic!("AscType can only be derived for structs and enums"),
    }
}

fn asc_type_derive_struct(item_struct: ItemStruct) -> TokenStream {
    let struct_name = &item_struct.ident;
    let (impl_generics, ty_generics, where_clause) = item_struct.generics.split_for_impl();
    let field_names: Vec<_> = match &item_struct.fields {
        Fields::Named(fields) => fields
            .named
            .iter()
            .map(|field| field.ident.as_ref().unwrap())
            .collect(),
        _ => panic!("AscType can only be derived for structs with named fields"),
    };
    let field_names2 = field_names.clone();
    let field_names3 = field_names.clone();
    let field_types = match &item_struct.fields {
        Fields::Named(fields) => fields.named.iter().map(|field| &field.ty),
        _ => panic!("AscType can only be derived for structs with named fields"),
    };

    TokenStream::from(quote! {
        impl#impl_generics AscType for #struct_name#ty_generics #where_clause {
            fn to_asc_bytes(&self) -> Vec<u8> {
               let mut bytes = vec![];
                #(bytes.extend_from_slice(&self.#field_names.to_asc_bytes());)*

                // Assert that the struct has no padding.
                assert_eq!(bytes.len(), size_of::<Self>());
                bytes
            }

            #[allow(unused_variables)]
            fn from_asc_bytes(asc_obj: &[u8]) -> Self {
                assert_eq!(asc_obj.len(), size_of::<Self>());
                let mut offset = 0;

                #(
                let field_size = std::mem::size_of::<#field_types>();
                let #field_names2 = AscType::from_asc_bytes(&asc_obj[offset..(offset + field_size)]);
                offset += field_size;
                )*

                Self {
                    #(#field_names3,)*
                }
        }
    }
    })
}
