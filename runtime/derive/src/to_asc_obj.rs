use proc_macro::TokenStream;
use quote::quote;
use syn::{self, DeriveInput, Ident};

pub fn to_asc_obj_macro_derive(tokens: TokenStream) -> TokenStream {
    let DeriveInput {
        ident, data, attrs, ..
    } = syn::parse_macro_input!(tokens);

    let mod_name = Ident::new(&format!("__{}__", ident.to_string().to_lowercase()), ident.span());
    let name = ident;

    let (fields, padding) = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = data
    {        
        let size = 
            named.iter().fold(0, |acc, f|{
                let size = field_size(f);
                acc + size
            });
        (named, if size/8 > 0 {size % 8} else {0})
    } else {
        panic!("No fields detected for type {}!", name.to_string())
    };

    let attr_required = 
        attrs
        .iter()
        .filter(|a| a.path.segments.len() == 1 && a.path.segments[0].ident == "required")
        .nth(0);

    let required_flds:Vec<String> =     
    if let Some(attr_required) = attr_required{
        let parameters: super::TypeParamList =
        syn::parse2(attr_required.tokens.clone()).expect("Invalid generic type attribute!");

        parameters.0.iter().map(|i| i.to_string()).collect::<Vec<String>>()
    } else{
        vec![]
    };

    let attribute = attrs
        .iter()
        .filter(|a| a.path.segments.len() == 1 && a.path.segments[0].ident == "asc_obj_type")
        .nth(0)
        .expect("\"asc_obj_type\" attribute required for deriving ToAscObj!");

    let parameters: super::TypeParam =
        syn::parse2(attribute.tokens.clone()).expect("Invalid generic type attribute!");

    let typ = parameters.0;

    let methods = fields.iter().map(|f| {

        let fld_name = f.ident.as_ref().unwrap();

        let is_required = is_required(f, &required_flds);
        let self_ref = 
            if is_byte_array(f){
                quote! { Bytes(&self.#fld_name) }
            }else{
                quote!{ self.#fld_name }
            };

        let setter = if is_nullable(&f) {

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
            if is_scalar(f){
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



    let range = 0..padding;
    let fld_padded = 
            range.map(|i|{
                let fld_name = format!("_padding{}",i).parse::<proc_macro2::TokenStream>().unwrap();
                quote! {
                    #fld_name : 0,
                }
            });

    let expanded = quote! {
        #[automatically_derived]
        mod #mod_name{
            use super::*;

            use graph::runtime::{
                asc_new, gas::GasCounter, AscHeap, AscPtr, AscType, DeterministicHostError, //AscIndexId, 
                ToAscObj
            };
            use graph_runtime_wasm::asc_abi::class::{Array, Uint8Array};
            
            //use crate::codec;
            
            use crate::runtime::abi::*;

            impl ToAscObj<#typ> for #name {

                #[allow(unused_variables)]
                fn to_asc_obj<H: AscHeap + ?Sized>(
                    &self,
                    heap: &mut H,
                    gas: &GasCounter,
                ) -> Result<#typ, DeterministicHostError> {
                    Ok(
                        #typ {
                            #(#methods)*

                            #(#fld_padded)*
                        }
                    )
                }
            }
        } // -------- end of mod
    };

    expanded.into()
}


fn is_scalar(fld: &syn::Field) -> bool{
    let nm = field_type(fld);
    match nm.as_ref(){
        "i8" | "u8" =>     true,
        "i16"| "u16" =>    true,
        "i32"| "u32" =>    true,
        "i64"| "u64" =>    true,
        "usize"|"isize" =>  true,
        _ => false
    }

}

fn field_size(fld: &syn::Field) -> usize{
    let nm = field_type(fld);
    match nm.as_ref(){
        "i32"|"u32" => 4,
        "i64"|"u64" => 8,
        "Option" => 24,
        "Vec" => 24,
        "String" => 24,
        "bool" => 1,
        _ => panic!("Unexpected field type:{}", nm)
    }
}


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
