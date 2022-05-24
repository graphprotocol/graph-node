
use proc_macro::TokenStream;
use quote::quote;
use syn::{self, DeriveInput};
use proc_macro2::{Span, Ident};
///This macro will take struct generated from protobuf and create asc type



pub fn from_protobuf_obj_macro_derive(tokens: TokenStream) -> TokenStream {
    let DeriveInput {
        ident, data, attrs, ..
    } = syn::parse_macro_input!(tokens);

    let name = ident.clone();
    let asc_name = Ident::new(&format!("Asc{}", ident.to_string()), Span::call_site());

    let (fields, padding) = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = data
    {        
        let size = 
            named.iter().fold(0, |acc, f|{
                acc + field_size(f)
            });

        (named, if size/8 > 0 {size % 8} else {0})
    } else {
        panic!("No fields detected for type {}!", name.to_string())
    };

    let attribute = attrs
        .iter()
        .filter(|a| a.path.segments.len() == 1 && a.path.segments[0].ident == "chain_name")
        .nth(0)
        .expect("\"chain_name\" attribute required for deriving ToAscObj!");

    let chain_name: super::TypeParam =
        syn::parse2(attribute.tokens.clone()).expect("Invalid chain name attribute!");

    let fields = fields.iter().map(|f| {
        let fld_name = f.ident.as_ref().unwrap();
        let fld_type = field_type(f).parse::<proc_macro2::TokenStream>().unwrap();
        quote! {
            pub #fld_name : #fld_type ,
        }
    });

    let range = 0..padding;

    let fld_padded = 
            range.map(|i|{
                let fld_name = format!("_padding{}",i).parse::<proc_macro2::TokenStream>().unwrap();
                quote! {
                    pub #fld_name : u8,
                }
            });

    let index_asc_type_id = 
        format!("{}{}", chain_name.0, name.to_string())
        .parse::<proc_macro2::TokenStream>().unwrap();


    let expanded = quote! {
        #[automatically_derived]

        #[repr(C)]
        #[derive(graph_runtime_derive::AscType)]
        pub struct #asc_name {
            #(#fields)*
            #(#fld_padded)*
        }

        impl graph::runtime::AscIndexId for #asc_name {
            const INDEX_ASC_TYPE_ID: graph::runtime::IndexForAscTypeId = graph::runtime::IndexForAscTypeId::#index_asc_type_id ;
        }
    };
    expanded.into()
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
        _ => 8
        //_ => panic!("Unexpected field type:{}", nm)
    }
}

fn field_type(fld: &syn::Field) -> String{
    
    if let syn::Type::Path(tp) = &fld.ty {
        if let Some(ps) = tp.path.segments.last(){
            let name = ps.ident.to_string();
            if name != "Option"{
                return name;
            }

            match  &ps.arguments{
                syn::PathArguments::AngleBracketed(v) => {
                    if let syn::GenericArgument::Type(syn::Type::Path(p)) = &v.args[0]{
                        format!("graph::runtime::AscPtr<Asc{}>", path_to_string(&p.path))
                    }else{
                        name
                    }
                }

                syn::PathArguments::None => name,
                syn::PathArguments::Parenthesized(_v) => {
                    !unimplemented!("syn::PathArguments::Parenthesized is not implemented")
                }
            }
        }else{
            "N/A".into()
        }
     }else{
         "N/A".into()
     }
}

fn path_to_string(path: &syn::Path) -> String{
    if let Some(ps) = path.segments.last(){
        ps.ident.to_string()
    }else{
        panic!("path_to_string - can't get last segment!")
    }

}
