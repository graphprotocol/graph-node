
use proc_macro::TokenStream;
use quote::quote;
use syn::{self, parse_macro_input, ItemStruct, Field};
use proc_macro2::{Span, Ident};



pub fn generate_asc_type(metadata: TokenStream, input: TokenStream) -> TokenStream {    

    let item_struct = parse_macro_input!(input as ItemStruct);
    let args=parse_macro_input!(metadata as super::Args);

    let name = item_struct.ident.clone();
    let asc_name = Ident::new(&format!("Asc{}", name.to_string()), Span::call_site());

    let enum_names = 
        args.vars.iter()
        .filter(|f|f.ident.to_string() != super::REQUIRED_IDENT_NAME)
        .map(|f| f.ident.to_string())
        .collect::<Vec<String>>();

    //struct's fields -> need to skip enum fields
    let mut fields = item_struct.fields.iter()
    .filter(|f| !enum_names.contains(&f.ident.as_ref().unwrap().to_string()))
    .collect::<Vec<&Field>>();

    //extend fields list with enum's variants
    args.vars.iter()
    .filter(|f|f.ident.to_string() != super::REQUIRED_IDENT_NAME)
    .flat_map(|f| f.fields.named.iter())
    .for_each(|f| fields.push(f));


    let fields = 
        fields.iter().map(| f | {
        let fld_name = f.ident.clone();
        let fld_type = field_type_map(field_type(f)).parse::<proc_macro2::TokenStream>().unwrap();

        quote! {
            pub #fld_name : #fld_type ,
        }  
    });

    let expanded = quote! {
        
        #item_struct

        #[automatically_derived]

        #[repr(C)]
        #[derive(graph_runtime_derive::AscType)]
        #[derive(Debug)]
        pub struct #asc_name {
            #(#fields)*
        }
    };

    expanded.into()
}

fn is_scalar(nm: &str) -> bool{

    match nm{
         "i8"|"u8"    => true,
        "i16"|"u16"   => true,
        "i32"|"u32"   => true,
        "i64"|"u64"   => true,
      "usize"|"isize" => true,
        "bool"        => true,
        //"String"    => false,
        _             => false
    }
}


fn field_type_map(tp:String) -> String{

    if is_scalar(&tp){
        tp
    }else{
        match tp.as_ref(){
            "String" => "graph_runtime_wasm::asc_abi::class::AscString".into(),
            _ => tp.to_owned()
        }
    }
}

fn field_type(fld: &syn::Field) -> String{

    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(ps) = tp.path.segments.last(){
            let name = ps.ident.to_string();
            //TODO - this must be optimized
            match name.as_ref(){
                "Vec" => { 
                    //Vec<TxResult> -> AscPtr<AscTxResultArray>
                    //Vec<u8>       - > AscPtr<Uint8Array>
                    //::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>> -> AscPtr<AscBytesArray>,
                    //::prost::alloc::vec::Vec<::prost::alloc::string::String> -> AscPtr<Array<AscPtr<AscString>>>,


                    match  &ps.arguments{
                        syn::PathArguments::AngleBracketed(v) => {
                            if let syn::GenericArgument::Type(syn::Type::Path(p)) = &v.args[0]{

                                let nm = path_to_string(&p.path);

                                match nm.as_ref(){
                                    "u8" => "graph::runtime::AscPtr<graph_runtime_wasm::asc_abi::class::Uint8Array>".to_owned(),
                                    "Vec<u8>" => "graph::runtime::AscPtr<crate::protobuf::AscBytesArray>".to_owned(),
                                    "String" => "graph::runtime::AscPtr<crate::protobuf::Array<graph::runtime::AscPtr<crate::protobuf::AscString>>>".to_owned(),
                                    _ => format!("graph::runtime::AscPtr<crate::protobuf::Asc{}Array>", path_to_string(&p.path))
                                }
                            }else{
                                name
                            }
                        }
        
                        syn::PathArguments::None => name,
                        syn::PathArguments::Parenthesized(_v) => {
                            panic!("syn::PathArguments::Parenthesized is not implemented")
                        }
                    }

                }
                "Option" => {
                    match  &ps.arguments{
                        syn::PathArguments::AngleBracketed(v) => {

                            if let syn::GenericArgument::Type(syn::Type::Path(p)) = &v.args[0]{
                                let tp_nm = path_to_string(&p.path);
                                if is_scalar(&tp_nm){
                                    format!("Option<{}>", tp_nm)
                                }else{
                                    format!("graph::runtime::AscPtr<crate::protobuf::Asc{}>", tp_nm)
                                }
                            }else{
                                name
                            }
                        }
        
                        syn::PathArguments::None => name,
                        syn::PathArguments::Parenthesized(_v) => {
                            panic!("syn::PathArguments::Parenthesized is not implemented")
                        }
                    }
        
                }
                "String" => {
                    //format!("graph::runtime::AscPtr<Asc{}>", name)
                    "graph::runtime::AscPtr<graph_runtime_wasm::asc_abi::class::AscString>".to_owned()
                }

                _ => {
                        if is_scalar(&name){
                            name
                        }else{
                            format!("graph::runtime::AscPtr<Asc{}>", name)
                        }

                }
            }
        }else{
            "N/A".into()
        }
     }else{
         "N/A".into()
     }
}


//recurcive
fn path_to_string(path: &syn::Path) -> String{
    if let Some(ps) = path.segments.last(){
        let nm = ps.ident.to_string();

        if let syn::PathArguments::AngleBracketed(v) = &ps.arguments {
            if let syn::GenericArgument::Type(syn::Type::Path(p)) = &v.args[0]{
                format!("{}<{}>",nm, path_to_string(&p.path))
            }else{
                nm
            }
        }else{
            nm
        }
    }else{
        panic!("path_to_string - can't get last segment!")
    }

}
