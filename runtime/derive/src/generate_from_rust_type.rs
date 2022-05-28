
use proc_macro::TokenStream;
use quote::quote;
use syn::{self, parse_macro_input, ItemStruct, AttributeArgs, NestedMeta, Meta, Path};
use proc_macro2::{Span, Ident};

pub fn generate_from_rust_type(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = parse_macro_input!(input as ItemStruct);
    let args= parse_macro_input!(metadata as AttributeArgs);

    let required_flds = 
        args.iter().filter_map(|a|{
            if let NestedMeta::Meta(  Meta::Path(Path{segments, ..})) = a{
                if let Some(p) = segments.last(){
                    return Some(p.ident.to_string().to_owned());
                }
            }
            None
        })
    .collect::<Vec<String>>();

    let mod_name = Ident::new(&format!("__{}__", item_struct.ident.to_string().to_lowercase()), item_struct.ident.span());
    let name = item_struct.ident.clone();
    let asc_name = Ident::new(&format!("Asc{}", name.to_string()), Span::call_site());

    let methods = item_struct.fields.iter().map(|f| {
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
                    let fld_nm = format!("\"{}\"", fld_name.to_string()).to_string().parse::<proc_macro2::TokenStream>().unwrap();
                    quote! {
                        #fld_name: crate::runtime::abi::asc_new_or_missing(heap, &#self_ref, gas, #type_nm, #fld_nm)?,
                    }
                }else{
                    quote! {
                        #fld_name: crate::runtime::abi::asc_new_or_null(heap, &#self_ref, gas)?,
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
    });

    let expanded = quote! {
        #item_struct

        #[automatically_derived]
        mod #mod_name{
            use super::*;

            use graph::runtime::{
                asc_new, gas::GasCounter, AscHeap, AscPtr, AscType, DeterministicHostError, 
                ToAscObj
            };
            //use graph_runtime_wasm::asc_abi::class::{Array, Uint8Array};
            use graph_runtime_wasm::asc_abi::class::*;
            use crate::runtime::abi::*;
            use crate::protobuf::*;

            impl ToAscObj<#asc_name> for #name {

                #[allow(unused_variables)]
                fn to_asc_obj<H: AscHeap + ?Sized>(
                    &self,
                    heap: &mut H,
                    gas: &GasCounter,
                ) -> Result<#asc_name, DeterministicHostError> {
                    Ok(
                        #asc_name {
                            #(#methods)*
                        }
                    )
                }
            }
        } // -------- end of mod
    };

    expanded.into()

}

fn is_scalar(fld: &str) -> bool{

    match fld{
        "i8" | "u8"     => true,
        "i16"| "u16"    => true,
        "i32"| "u32"    => true,
        "i64"| "u64"    => true,
        "usize"|"isize" => true,
        "bool"          => true,
        //"String"        => false,
        _ => false
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

fn field_type(fld: &syn::Field) -> String{
    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(ps) = tp.path.segments.last(){
            return ps.ident.to_string();
        }else{
            "N/A".into()
        }
     }else{
         "N/A".into()
     }
}


fn is_required(fld: & syn::Field, req_list:&[String]) -> bool{
    let fld_name = fld.ident.as_ref().unwrap().to_string();
    req_list.iter().find(|r| *r == &fld_name).is_some()
}

fn is_nullable(fld: &syn::Field) -> bool {
    //println!("{} - is byte array:{}", fld.ident.as_ref().unwrap().to_string(), is_byte_array(fld));

    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(last) = tp.path.segments.last(){
            return last.ident == "Option";
        }
    }
    false
}

fn is_byte_array(fld: &syn::Field) -> bool {
    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(last) = tp.path.segments.last(){
            if last.ident == "Vec"{
                if let syn::PathArguments::AngleBracketed(ref v) = last.arguments{
                    if let Some(last) = v.args.last(){
                        if let syn::GenericArgument::Type(t) = last{
                            if let syn::Type::Path(p) = t{
                                if let Some(a ) = p.path.segments.last(){
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
