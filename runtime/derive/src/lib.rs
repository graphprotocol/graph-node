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
// impl<K, V> graph::runtime::AscType for AscTypedMapEntry<K, V> {
//     fn to_asc_bytes(&self) -> Vec<u8> {
//         let mut bytes = Vec::new();
//         bytes.extend_from_slice(&self.key.to_asc_bytes());
//         bytes.extend_from_slice(&self.value.to_asc_bytes());
//         assert_eq!(&bytes.len(), &size_of::<Self>());
//         bytes
//     }

//     #[allow(unused_variables)]
//     fn from_asc_bytes(asc_obj: &[u8], api_version: graph::semver::Version) -> Self {
//         assert_eq!(&asc_obj.len(), &size_of::<Self>());
//         let mut offset = 0;
//         let field_size = std::mem::size_of::<AscPtr<K>>();
//         let key = graph::runtime::AscType::from_asc_bytes(&asc_obj[offset..(offset + field_size)],
//         api_version.clone());
//         offset += field_size;
//         let field_size = std::mem::size_of::<AscPtr<V>>();
//         let value = graph::runtime::AscType::from_asc_bytes(&asc_obj[offset..(offset + field_size)], api_version);
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
        impl #impl_generics graph::runtime::AscType for #struct_name #ty_generics #where_clause {
            fn to_asc_bytes(&self) -> Result<Vec<u8>, graph::runtime::DeterministicHostError> {
                let in_memory_byte_count = std::mem::size_of::<Self>();
                let mut bytes = Vec::with_capacity(in_memory_byte_count);
                #(bytes.extend_from_slice(&self.#field_names.to_asc_bytes()?);)*

                // Right now we do not properly implement the alignment rules dicted by
                // AssemblyScript. As such, we here enforce that the structure is tighly
                // packed and that no implicit padding has been added.
                //
                // **Important** AssemblyScript and `repr(C)` in Rust does not follow exactly
                // the same rules always. One caveats is that some struct are packed in AssemblyScript
                // but padded for alignment in `repr(C)` like a struct `{ one: AscPtr, two: AscPtr, three: AscPtr, four: u64 }`,
                // it appears this struct is always padded in `repr(C)` by Rust whatever order is tried.
                // However, it's possible to packed completely this struct in AssemblyScript and avoid
                // any padding.
                //
                // To overcome those cases where re-ordering never work, you will need to add an explicit
                // _padding field to account for missing padding and pass this check.
                assert_eq!(bytes.len(), in_memory_byte_count, "Alignment mismatch for {}, re-order fields or explicitely add a _padding field", stringify!(#struct_name));
                Ok(bytes)
            }

            #[allow(unused_variables)]
            fn from_asc_bytes(asc_obj: &[u8], api_version: &graph::semver::Version) -> Result<Self, graph::runtime::DeterministicHostError> {
                // Sanity check
                match api_version {
                    api_version if *api_version <= graph::semver::Version::new(0, 0, 4) => {
                        // This was using an double equal sign before instead of less than.
                        // This happened because of the new apiVersion support.
                        // Since some structures need different implementations for each
                        // version, their memory size got bigger because we're using an enum
                        // that contains both versions (each in a variant), and that increased
                        // the memory size, so that's why we use less than.
                        if asc_obj.len() < std::mem::size_of::<Self>() {
                            return Err(graph::runtime::DeterministicHostError::from(graph::prelude::anyhow::anyhow!("Size does not match")));
                        }
                    }
                    _ => {
                        let content_size = std::mem::size_of::<Self>();
                        let aligned_size = graph::runtime::padding_to_16(content_size);

                        if graph::runtime::HEADER_SIZE + asc_obj.len() == aligned_size + content_size {
                            return Err(graph::runtime::DeterministicHostError::from(graph::prelude::anyhow::anyhow!("Size does not match")));
                        }
                    },
                };

                let mut offset = 0;

                #(
                let field_size = std::mem::size_of::<#field_types>();
                let field_data = asc_obj.get(offset..(offset + field_size)).ok_or_else(|| {
                    graph::runtime::DeterministicHostError::from(graph::prelude::anyhow::anyhow!("Attempted to read past end of array"))
                })?;
                let #field_names2 = graph::runtime::AscType::from_asc_bytes(&field_data, api_version)?;
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
// impl graph::runtime::AscType for JsonValueKind {
//     fn to_asc_bytes(&self) -> Result<Vec<u8>, graph::runtime::DeterministicHostError> {
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
//     fn from_asc_bytes(asc_obj: &[u8], _api_version: graph::semver::Version) -> Result<Self, graph::runtime::DeterministicHostError> {
//         let mut u32_bytes: [u8; size_of::<u32>()] = [0; size_of::<u32>()];
//         if std::mem::size_of_val(&u32_bytes) != std::mem::size_of_val(&asc_obj) {
//             return Err(graph::runtime::DeterministicHostError::from(graph::prelude::anyhow::anyhow!("Invalid asc bytes size")));
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
//             _ => Err(graph::runtime::DeterministicHostError::from(graph::prelude::anyhow::anyhow!("value {} is out of range for {}", discr, "JsonValueKind"))),
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
        impl #impl_generics graph::runtime::AscType for #enum_name #ty_generics #where_clause {
            fn to_asc_bytes(&self) -> Result<Vec<u8>, graph::runtime::DeterministicHostError> {
                let discriminant: u32 = match self {
                    #(#enum_name_iter::#variant_paths => #variant_discriminant,)*
                };
                discriminant.to_asc_bytes()
            }

            fn from_asc_bytes(asc_obj: &[u8], _api_version: &graph::semver::Version) -> Result<Self, graph::runtime::DeterministicHostError> {
                let u32_bytes = ::std::convert::TryFrom::try_from(asc_obj)
                    .map_err(|_| graph::runtime::DeterministicHostError::from(graph::prelude::anyhow::anyhow!("Invalid asc bytes size")))?;
                let discr = u32::from_le_bytes(u32_bytes);
                match discr {
                    #(#variant_discriminant2 => Ok(#enum_name_iter2::#variant_paths2),)*
                    _ => Err(graph::runtime::DeterministicHostError::from(graph::prelude::anyhow::anyhow!("value {} is out of range for {}", discr, stringify!(#enum_name))))
                }
            }
        }
    })
}
