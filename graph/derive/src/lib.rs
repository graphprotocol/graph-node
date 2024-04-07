use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(CheapClone)]
pub fn derive_cheap_clone(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);
    let crate_name = std::env::var("CARGO_PKG_NAME").unwrap();

    let cheap_clone = if crate_name == "graph" {
        quote! { crate::cheap_clone::CheapClone }
    } else {
        quote! { graph::cheap_clone::CheapClone }
    };

    let name = input.ident;
    let generics = input.generics;
    // Build the output, possibly using the input
    let expanded = quote! {
        // The generated impl
        impl #generics #cheap_clone for #name #generics { }
    };

    // Hand the output tokens back to the compiler
    TokenStream::from(expanded)
}
