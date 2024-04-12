use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{self, parse_macro_input, ItemStruct};

pub fn generate_network_type_id(metadata: TokenStream, input: TokenStream) -> TokenStream {
    let item_struct = parse_macro_input!(input as ItemStruct);
    let name = item_struct.ident.clone();

    let asc_name = if name.to_string().to_uppercase().starts_with("ASC") {
        name.clone()
    } else {
        Ident::new(&format!("Asc{}", name), Span::call_site())
    };

    let no_asc_name = if name.to_string().to_uppercase().starts_with("ASC") {
        name.to_string()[3..].to_owned()
    } else {
        name.to_string()
    };

    let args = {
        let mut args = Vec::new();
        let parser = syn::meta::parser(|meta| {
            if let Some(ident) = meta.path.get_ident() {
                args.push(ident.to_string());
            }
            Ok(())
        });
        parse_macro_input!(metadata with parser);
        args
    };

    assert!(
        !args.is_empty(),
        "arguments not found! generate_network_type_id(<network-name>)"
    );

    //type_id variant name
    let index_asc_type_id = format!("{}{}", args[0], no_asc_name)
        .parse::<proc_macro2::TokenStream>()
        .unwrap();

    let expanded = quote! {
        #item_struct

        #[automatically_derived]
        impl graph::runtime::AscIndexId for #asc_name {
            const INDEX_ASC_TYPE_ID: graph::runtime::IndexForAscTypeId = graph::runtime::IndexForAscTypeId::#index_asc_type_id ;
        }
    };

    expanded.into()
}
