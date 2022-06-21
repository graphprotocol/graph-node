use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{self, parse_macro_input, Field, ItemStruct};

pub fn generate_from_rust_type(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = parse_macro_input!(input as ItemStruct);
    let args = parse_macro_input!(metadata as super::Args);

    let enum_names = args
        .vars
        .iter()
        .filter(|f| f.ident.to_string() != super::REQUIRED_IDENT_NAME)
        .map(|f| f.ident.to_string())
        .collect::<Vec<String>>();

    let required_flds = args
        .vars
        .iter()
        .filter(|f| f.ident.to_string() == super::REQUIRED_IDENT_NAME)
        .flat_map(|f| f.fields.named.iter())
        .map(|f| f.ident.as_ref().unwrap().to_string())
        .collect::<Vec<String>>();

    //struct's standard fields
    let fields = item_struct
        .fields
        .iter()
        .filter(|f| !enum_names.contains(&f.ident.as_ref().unwrap().to_string()))
        .collect::<Vec<&Field>>();

    //struct's enum fields
    let enum_fields = item_struct
        .fields
        .iter()
        .filter(|f| enum_names.contains(&f.ident.as_ref().unwrap().to_string()))
        .collect::<Vec<&Field>>();

    let mod_name = Ident::new(
        &format!("__{}__", item_struct.ident.to_string().to_lowercase()),
        item_struct.ident.span(),
    );
    let name = item_struct.ident.clone();
    let asc_name = Ident::new(&format!("Asc{}", name.to_string()), Span::call_site());
    let asc_name_array = Ident::new(&format!("Asc{}Array", name.to_string()), Span::call_site());

    //generate enum fields validator
    let enum_validation = enum_fields.iter().map(|f|{
        let fld_name = f.ident.as_ref().unwrap();
        let type_nm = format!("\"{}\"", name.to_string()).parse::<proc_macro2::TokenStream>().unwrap();
        let fld_nm = format!("\"{}\"", fld_name.to_string()).to_string().parse::<proc_macro2::TokenStream>().unwrap();
        quote! {
            // let #fld_name = self.#fld_name.as_ref()
            //     .ok_or_else(|| missing_field_error(#type_nm, #fld_nm))?;
            // }

            let #fld_name = self.#fld_name.as_ref()
                .ok_or_else(||  DeterministicHostError::from(anyhow!("{} missing {}", #type_nm, #fld_nm)))?;
            }
    });

    let mut methods:Vec<proc_macro2::TokenStream> =
    fields.iter().map(|f| {
        let fld_name = f.ident.as_ref().unwrap();
        let self_ref =
            if is_byte_array(f){
                quote! { Bytes(&self.#fld_name) }
            }else{
                quote!{ self.#fld_name }
            };

        let is_required = is_required(f, &required_flds);

        let setter =
            if is_nullable(&f) {
                if is_required{
                    let type_nm = format!("\"{}\"", name.to_string()).parse::<proc_macro2::TokenStream>().unwrap();
                    let fld_nm = format!("\"{}\"", fld_name.to_string()).parse::<proc_macro2::TokenStream>().unwrap();

                    quote! {
                        #fld_name: crate::protobuf::asc_new_or_missing(heap, &#self_ref, gas, #type_nm, #fld_nm)?,
                    }
                }else{
                    quote! {
                        #fld_name: crate::protobuf::asc_new_or_null(heap, &#self_ref, gas)?,
                    }
                }
            } else {
                if is_scalar(&field_type(f)){
                    quote!{
                        #fld_name: #self_ref,
                    }
                }else{
                    quote! {
                        #fld_name: asc_new(heap, &#self_ref, gas)?,
                    }
                }
            };
        setter
    })
    .collect();

    for var in args.vars {
        let var_nm = var.ident.to_string();
        if var_nm == super::REQUIRED_IDENT_NAME {
            continue;
        }

        let mut c = var_nm.chars();
        let var_type_name = c.next().unwrap().to_uppercase().collect::<String>() + c.as_str();

        var.fields.named.iter().map(|f|{
            let fld_nm = f.ident.as_ref().unwrap();
            let var_nm = var.ident.clone();

            use convert_case::{Case, Casing};

            let varian_type_name = fld_nm.to_string().to_case(Case::Pascal);
            let mod_name = item_struct.ident.to_string().to_case(Case::Snake);
            let varian_type_name = format!("{}::{}::{}",mod_name, var_type_name, varian_type_name).parse::<proc_macro2::TokenStream>().unwrap();

            let setter =
                quote! {
                    #fld_nm: if let #varian_type_name(v) = #var_nm {asc_new(heap, v, gas)? } else {AscPtr::null()},
                };

            setter
        })
        .for_each(|ts| methods.push(ts));
    }

    //println!("{:#?}", methods);

    let expanded = quote! {
        #item_struct

        #[automatically_derived]
        mod #mod_name{
            use super::*;

            use graph::runtime::{
                asc_new, gas::GasCounter, AscHeap, AscPtr, AscType, DeterministicHostError,
                ToAscObj
            };
            use graph_runtime_wasm::asc_abi::class::*;
            use crate::protobuf::*;

            impl ToAscObj<#asc_name> for #name {

                #[allow(unused_variables)]
                fn to_asc_obj<H: AscHeap + ?Sized>(
                    &self,
                    heap: &mut H,
                    gas: &GasCounter,
                ) -> Result<#asc_name, DeterministicHostError> {

                    #(#enum_validation)*

                    Ok(
                         #asc_name {
                            #(#methods)*
                        }
                    )
                }
            }


        } // -------- end of mod

        pub struct #asc_name_array(pub  crate::protobuf::Array<graph::runtime::AscPtr<#asc_name>>);

        impl crate::protobuf::ToAscObj<#asc_name_array> for Vec<#name> {
            fn to_asc_obj<H: graph::runtime::AscHeap + ?Sized>(
                &self,
                heap: &mut H,
                gas: &graph::runtime::gas::GasCounter,
            ) -> Result<#asc_name_array, crate::protobuf::DeterministicHostError> {
                let content: Result<Vec<_>, _> = self.iter().map(|x| crate::protobuf::asc_new(heap, x, gas)).collect();

                Ok(#asc_name_array(crate::protobuf::Array::new(&content?, heap, gas)?))
            }
        }

        impl graph::runtime::AscType for #asc_name_array {
            fn to_asc_bytes(&self) -> Result<Vec<u8>, crate::protobuf::DeterministicHostError> {
                self.0.to_asc_bytes()
            }

            fn from_asc_bytes(
                asc_obj: &[u8],
                api_version: &crate::protobuf::Version,
            ) -> Result<Self, crate::protobuf::DeterministicHostError> {
                Ok(Self(crate::protobuf::Array::from_asc_bytes(asc_obj, api_version)?))
            }
        }

    };

    expanded.into()
}

fn is_scalar(fld: &str) -> bool {
    match fld {
        "i8" | "u8" => true,
        "i16" | "u16" => true,
        "i32" | "u32" => true,
        "i64" | "u64" => true,
        "usize" | "isize" => true,
        "bool" => true,
        //"String"        => false,
        _ => false,
    }
}

// fn field_type_map(tp:String) -> String{

//     if is_scalar(&tp){
//         tp
//     }else{
//         match tp.as_ref(){
//             "String" => "graph_runtime_wasm::asc_abi::class::AscString".into(),
//             _ => tp.to_owned()
//         }
//     }
// }

fn field_type(fld: &syn::Field) -> String {
    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(ps) = tp.path.segments.last() {
            return ps.ident.to_string();
        } else {
            "N/A".into()
        }
    } else {
        "N/A".into()
    }
}

fn is_required(fld: &syn::Field, req_list: &[String]) -> bool {
    let fld_name = fld.ident.as_ref().unwrap().to_string();
    req_list.iter().find(|r| *r == &fld_name).is_some()
}

fn is_nullable(fld: &syn::Field) -> bool {
    //println!("{} - is byte array:{}", fld.ident.as_ref().unwrap().to_string(), is_byte_array(fld));

    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(last) = tp.path.segments.last() {
            return last.ident == "Option";
        }
    }
    false
}

fn is_byte_array(fld: &syn::Field) -> bool {
    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(last) = tp.path.segments.last() {
            if last.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(ref v) = last.arguments {
                    if let Some(last) = v.args.last() {
                        if let syn::GenericArgument::Type(t) = last {
                            if let syn::Type::Path(p) = t {
                                if let Some(a) = p.path.segments.last() {
                                    return a.ident == "u8";
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    false
}
