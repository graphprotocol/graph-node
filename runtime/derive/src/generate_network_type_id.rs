use proc_macro::TokenStream;
use quote::quote;
use syn::{self, parse_macro_input, ItemStruct, AttributeArgs, NestedMeta, Meta, Path};
use proc_macro2::{Span, Ident};

pub fn generate_network_type_id(metadata: TokenStream, input: TokenStream) -> TokenStream {

    let item_struct = parse_macro_input!(input as ItemStruct);
    let name = item_struct.ident.clone();

    let asc_name = 
        if name.to_string().to_uppercase().starts_with("ASC"){
            name.clone()
        }else{
            Ident::new(&format!("Asc{}", name.to_string()), Span::call_site())
        };

    let no_asc_name = 
        if name.to_string().to_uppercase().starts_with("ASC"){
            name.to_string()[3..].to_owned()
        }else{
            name.to_string()
        };



    let args= parse_macro_input!(metadata as AttributeArgs);

    let args = 
        args.iter().filter_map(|a|{
            if let NestedMeta::Meta(  Meta::Path(Path{segments, ..})) = a{
                if let Some(p) = segments.last(){
                    return Some(p.ident.to_string().to_owned());
                }
            }
            None
        })
    .collect::<Vec<String>>();

    assert!(args.len() > 0,"arguments not found! generate_network_type_id(<network-name>)");

    //first element - network
    //let index_asc_type_id = format!("{}{}", args[0], name.to_string()).parse::<proc_macro2::TokenStream>().unwrap();
    let index_asc_type_id = format!("{}{}", args[0], no_asc_name).parse::<proc_macro2::TokenStream>().unwrap();

    let expanded = quote! {
        #item_struct
        
        #[automatically_derived]
        impl graph::runtime::AscIndexId for #asc_name {
            const INDEX_ASC_TYPE_ID: graph::runtime::IndexForAscTypeId = graph::runtime::IndexForAscTypeId::#index_asc_type_id ;
        }
    };

    expanded.into()
}
