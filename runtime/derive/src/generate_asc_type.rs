use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{self, parse_macro_input, Field, ItemStruct};

pub fn generate_asc_type(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = parse_macro_input!(input as ItemStruct);
    let args = parse_macro_input!(metadata as super::Args);

    let name = item_struct.ident.clone();
    let asc_name = Ident::new(&format!("Asc{}", name.to_string()), Span::call_site());

    let enum_names = args
        .vars
        .iter()
        .filter(|f| f.ident.to_string() != super::REQUIRED_IDENT_NAME)
        .map(|f| f.ident.to_string())
        .collect::<Vec<String>>();

    //struct's fields -> need to skip enum fields
    let mut fields = item_struct
        .fields
        .iter()
        .filter(|f| !enum_names.contains(&f.ident.as_ref().unwrap().to_string()))
        .collect::<Vec<&Field>>();

    //extend fields list with enum's variants
    args.vars
        .iter()
        .filter(|f| f.ident.to_string() != super::REQUIRED_IDENT_NAME)
        .flat_map(|f| f.fields.named.iter())
        .for_each(|f| fields.push(f));

    // let struct_align = item_struct
    //     .fields
    //     .iter()
    //     .map(|f| size_of(&field_type(f)))
    //     .max()
    //     .unwrap();

    // let pad_fld_type = "u8".parse::<proc_macro2::TokenStream>().unwrap();

    // let mut current_offset = 0;
    // let mut pad_index = 0;

    //let mut m_fields = Vec::<proc_macro2::TokenStream>::new();

    let m_fields: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|f| {
            let fld_name = f.ident.clone().unwrap();
            let typ = field_type_map(field_type(f));
            let fld_type = typ.parse::<proc_macro2::TokenStream>().unwrap();

            // let pad_size = padding_needed_for(current_offset, struct_align);

            // if pad_size > 0 {
            //     for _ in 0..pad_size {
            //         let pad_fld_name = Ident::new(&format!("_pad{}_", pad_index), Span::call_site());
            //         pad_index += 1;

            //         m_fields.push(quote! {
            //             pub #pad_fld_name : #pad_fld_type ,
            //         });
            //     }
            //     current_offset += pad_size;
            // }

            // current_offset += size_of(&typ);

            //m_fields.push(snip);
            // m_fields.push(quote! {
            //     pub #fld_name : #fld_type ,
            // });
            quote! {
                pub #fld_name : #fld_type ,
            }
        })
        .collect();

    let expanded = quote! {

        #item_struct

        #[automatically_derived]

        #[repr(C)]
        #[derive(graph_runtime_derive::AscType)]
        #[derive(Debug, Default)]
        pub struct #asc_name {
            #(#m_fields)*
        }
    };

    expanded.into()
}

fn is_scalar(nm: &str) -> bool {
    match nm {
        "i8" | "u8" => true,
        "i16" | "u16" => true,
        "i32" | "u32" => true,
        "i64" | "u64" => true,
        "usize" | "isize" => true,
        "bool" => true,
        //"String"    => false,
        _ => false,
    }
}

fn field_type_map(tp: String) -> String {
    if is_scalar(&tp) {
        tp
    } else {
        match tp.as_ref() {
            "String" => "graph_runtime_wasm::asc_abi::class::AscString".into(),
            _ => tp.to_owned(),
        }
    }
}

fn field_type(fld: &syn::Field) -> String {
    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(ps) = tp.path.segments.last() {
            let name = ps.ident.to_string();
            //TODO - this must be optimized
            match name.as_ref() {
                "Vec" => {
                    //Vec<TxResult> -> AscPtr<AscTxResultArray>
                    //Vec<u8>       - > AscPtr<Uint8Array>
                    //::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>> -> AscPtr<AscBytesArray>,
                    //::prost::alloc::vec::Vec<::prost::alloc::string::String> -> AscPtr<Array<AscPtr<AscString>>>,

                    match &ps.arguments {
                        syn::PathArguments::AngleBracketed(v) => {
                            if let syn::GenericArgument::Type(syn::Type::Path(p)) = &v.args[0] {
                                let nm = path_to_string(&p.path);

                                match nm.as_ref(){
                                    "u8" => "graph::runtime::AscPtr<graph_runtime_wasm::asc_abi::class::Uint8Array>".to_owned(),
                                    "Vec<u8>" => "graph::runtime::AscPtr<crate::protobuf::AscBytesArray>".to_owned(),
                                    "String" => "graph::runtime::AscPtr<crate::protobuf::Array<graph::runtime::AscPtr<graph_runtime_wasm::asc_abi::class::AscString>>>".to_owned(),
                                    _ => format!("graph::runtime::AscPtr<crate::protobuf::Asc{}Array>", path_to_string(&p.path))
                                }
                            } else {
                                name
                            }
                        }

                        syn::PathArguments::None => name,
                        syn::PathArguments::Parenthesized(_v) => {
                            panic!("syn::PathArguments::Parenthesized is not implemented")
                        }
                    }
                }
                "Option" => match &ps.arguments {
                    syn::PathArguments::AngleBracketed(v) => {
                        if let syn::GenericArgument::Type(syn::Type::Path(p)) = &v.args[0] {
                            let tp_nm = path_to_string(&p.path);
                            if is_scalar(&tp_nm) {
                                format!("Option<{}>", tp_nm)
                            } else {
                                format!("graph::runtime::AscPtr<crate::protobuf::Asc{}>", tp_nm)
                            }
                        } else {
                            name
                        }
                    }

                    syn::PathArguments::None => name,
                    syn::PathArguments::Parenthesized(_v) => {
                        panic!("syn::PathArguments::Parenthesized is not implemented")
                    }
                },
                "String" => {
                    //format!("graph::runtime::AscPtr<Asc{}>", name)
                    "graph::runtime::AscPtr<graph_runtime_wasm::asc_abi::class::AscString>"
                        .to_owned()
                }

                _ => {
                    if is_scalar(&name) {
                        name
                    } else {
                        format!("graph::runtime::AscPtr<Asc{}>", name)
                    }
                }
            }
        } else {
            "N/A".into()
        }
    } else {
        "N/A".into()
    }
}

//recurcive
fn path_to_string(path: &syn::Path) -> String {
    if let Some(ps) = path.segments.last() {
        let nm = ps.ident.to_string();

        if let syn::PathArguments::AngleBracketed(v) = &ps.arguments {
            if let syn::GenericArgument::Type(syn::Type::Path(p)) = &v.args[0] {
                format!("{}<{}>", nm, path_to_string(&p.path))
            } else {
                nm
            }
        } else {
            nm
        }
    } else {
        panic!("path_to_string - can't get last segment!")
    }
}

// fn size_of(tp: &str) -> usize {
//     match tp {
//         "i8" | "u8" => std::mem::size_of::<i8>(),
//         "i16" | "u16" => std::mem::size_of::<i16>(),

//         "i32" | "u32" => std::mem::size_of::<i32>(),

//         "i64" | "u64" => std::mem::size_of::<i64>(),
//         "i128" | "u128" => std::mem::size_of::<i128>(),

//         "f32" => std::mem::size_of::<f32>(),
//         "f64" => std::mem::size_of::<f64>(),

//         "isize" | "usize" => std::mem::size_of::<isize>(),
//         "bool" => std::mem::size_of::<bool>(),
//         "char" => std::mem::size_of::<char>(),
//         _ => std::mem::size_of::<i32>(), //pointer
//     }
// }

// fn padding_needed_for(offset: usize, alignment: usize) -> usize {
//     let misalignment = offset % alignment;
//     if misalignment > 0 {
//         // round up to next multiple of `alignment`
//         alignment - misalignment
//     } else {
//         // already a multiple of `alignment`
//         0
//     }
// }
