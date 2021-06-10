#![recursion_limit = "128"]

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{Fields, Item, ItemEnum, ItemStruct};

#[proc_macro_derive(AscType)]
pub fn asc_type_derive(input: TokenStream) -> TokenStream {
    let item: Item = syn::parse(input).unwrap();
    match item {
        Item::Struct(item_struct) => asc_type_derive_struct(item_struct),
        Item::Enum(item_enum) => asc_type_derive_enum(item_enum),
        _ => panic!("AscType can only be derived for structs and enums"),
    }
}

// Example input:
// #[repr(C)]
// #[derive(AscType)]
// struct AscTypedMapEntry<K, V> {
//     pub key: AscPtr<K>,
//     pub value: AscPtr<V>,
// }
//
// Example output:
// impl<K, V> AscType for AscTypedMapEntry<K, V> {
//     fn to_asc_bytes(&self) -> Vec<u8> {
//         let mut bytes = Vec::new();
//         bytes.extend_from_slice(&self.key.to_asc_bytes());
//         bytes.extend_from_slice(&self.value.to_asc_bytes());
//         assert_eq!(&bytes.len(), &size_of::<Self>());
//         bytes
//     }

//     #[allow(unused_variables)]
//     fn from_asc_bytes(asc_obj: &[u8]) -> Self {
//         assert_eq!(&asc_obj.len(), &size_of::<Self>());
//         let mut offset = 0;
//         let field_size = std::mem::size_of::<AscPtr<K>>();
//         let key = AscType::from_asc_bytes(&asc_obj[offset..(offset + field_size)]);
//         offset += field_size;
//         let field_size = std::mem::size_of::<AscPtr<V>>();
//         let value = AscType::from_asc_bytes(&asc_obj[offset..(offset + field_size)]);
//         offset += field_size;
//         Self { key, value }
//     }
// }
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
            fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
               let mut bytes = Vec::new();
                #(bytes.extend_from_slice(&self.#field_names.to_asc_bytes()?);)*

                // Assert that the struct has no padding.
                assert_eq!(bytes.len(), std::mem::size_of::<Self>());
                Ok(bytes)
            }

            #[allow(unused_variables)]
            fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
                if asc_obj.len() != std::mem::size_of::<Self>() {
                    return Err(DeterministicHostError(anyhow::anyhow!("Size does not match")));
                }
                let mut offset = 0;

                #(
                let field_size = std::mem::size_of::<#field_types>();
                let field_data = asc_obj.get(offset..(offset + field_size)).ok_or_else(|| {
                    DeterministicHostError(anyhow::anyhow!("Attempted to read past end of array"))
                })?;
                let #field_names2 = AscType::from_asc_bytes(&field_data)?;
                offset += field_size;
                )*

                Ok(Self {
                    #(#field_names3,)*
                })
            }
        }
    })
}

// Example input:
// #[repr(u32)]
// #[derive(AscType)]
// enum JsonValueKind {
//     Null,
//     Bool,
//     Number,
//     String,
//     Array,
//     Object,
// }
//
// Example output:
// impl AscType for JsonValueKind {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
//         let discriminant: u32 = match *self {
//             JsonValueKind::Null => 0u32,
//             JsonValueKind::Bool => 1u32,
//             JsonValueKind::Number => 2u32,
//             JsonValueKind::String => 3u32,
//             JsonValueKind::Array => 4u32,
//             JsonValueKind::Object => 5u32,
//         };
//         Ok(discriminant.to_asc_bytes())
//     }
//
//     fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
//         let mut u32_bytes: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
//         if std::mem::size_of_val(&u32_bytes) != std::mem::size_of_val(&as_obj) {
//             return Err(DeterministicHostError(anyhow::anyhow!("Invalid asc bytes size")));
//         }
//         u32_bytes.copy_from_slice(&asc_obj);
//         let discr = u32::from_le_bytes(u32_bytes);
//         match discr {
//             0u32 => JsonValueKind::Null,
//             1u32 => JsonValueKind::Bool,
//             2u32 => JsonValueKind::Number,
//             3u32 => JsonValueKind::String,
//             4u32 => JsonValueKind::Array,
//             5u32 => JsonValueKind::Object,
//             _ => Err(DeterministicHostError(anyhow::anyhow!("value {} is out of range for {}", discr, "JsonValueKind")),
//         }
//     }
// }
fn asc_type_derive_enum(item_enum: ItemEnum) -> TokenStream {
    let enum_name = &item_enum.ident;
    let enum_name_iter = std::iter::repeat(enum_name);
    let enum_name_iter2 = enum_name_iter.clone();
    let (impl_generics, ty_generics, where_clause) = item_enum.generics.split_for_impl();
    let variant_paths: Vec<_> = item_enum
        .variants
        .iter()
        .map(|v| {
            assert!(v.discriminant.is_none());
            &v.ident
        })
        .collect();
    let variant_paths2 = variant_paths.clone();
    let variant_discriminant = 0..(variant_paths.len() as u32);
    let variant_discriminant2 = variant_discriminant.clone();

    TokenStream::from(quote! {
        impl#impl_generics AscType for #enum_name#ty_generics #where_clause {
            fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
                let discriminant: u32 = match self {
                    #(#enum_name_iter::#variant_paths => #variant_discriminant,)*
                };
                discriminant.to_asc_bytes()
            }

            fn from_asc_bytes(asc_obj: &[u8]) -> Result<Self, DeterministicHostError> {
                let u32_bytes = ::std::convert::TryFrom::try_from(asc_obj)
                    .map_err(|_| DeterministicHostError(anyhow::anyhow!("Invalid asc bytes size")))?;
                let discr = u32::from_le_bytes(u32_bytes);
                match discr {
                    #(#variant_discriminant2 => Ok(#enum_name_iter2::#variant_paths2),)*
                    _ => Err(DeterministicHostError(anyhow::anyhow!("value {} is out of range for {}", discr, stringify!(#enum_name))))
                }
            }
        }
    })
}
