use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident, Index};

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

#[proc_macro_derive(CacheWeight)]
pub fn derive_cache_weight(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let DeriveInput {
        ident,
        generics,
        data,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let crate_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let cache_weight = if crate_name == "graph" {
        quote! { crate::util::cache_weight::CacheWeight }
    } else {
        quote! { graph::util::cache_weight::CacheWeight }
    };

    let total = Ident::new("__total_cache_weight", Span::call_site());
    let body = match data {
        syn::Data::Struct(st) => {
            let mut incrs: Vec<proc_macro2::TokenStream> = Vec::new();
            for (num, field) in st.fields.iter().enumerate() {
                let incr = match &field.ident {
                    Some(ident) => quote! {
                        #total += self.#ident.indirect_weight();
                    },
                    None => {
                        let idx = Index::from(num);
                        quote! {
                            #total += self.#idx.indirect_weight();
                        }
                    }
                };
                incrs.push(incr);
            }
            quote! {
                let mut #total = 0;
                #(#incrs)*
                #total
            }
        }
        syn::Data::Enum(en) => {
            let mut match_arms = Vec::new();
            for variant in en.variants.into_iter() {
                let ident = variant.ident;
                match variant.fields {
                    syn::Fields::Named(fields) => {
                        let idents: Vec<_> =
                            fields.named.into_iter().map(|f| f.ident.unwrap()).collect();

                        let mut incrs = Vec::new();
                        for ident in &idents {
                            incrs.push(quote! { #total += #ident.indirect_weight(); });
                        }
                        match_arms.push(quote! {
                            Self::#ident{#(#idents,)*} => {
                                #(#incrs)*
                            }
                        });
                    }
                    syn::Fields::Unnamed(fields) => {
                        let num_fields = fields.unnamed.len();

                        let idents = (0..num_fields)
                            .map(|i| {
                                syn::Ident::new(&format!("v{}", i), proc_macro2::Span::call_site())
                            })
                            .collect::<Vec<_>>();
                        let mut incrs = Vec::new();
                        for ident in &idents {
                            incrs.push(quote! { #total += #ident.indirect_weight(); });
                        }
                        match_arms.push(quote! {
                            Self::#ident(#(#idents,)*) => {
                                #(#incrs)*
                            }
                        });
                    }
                    syn::Fields::Unit => {
                        match_arms.push(quote! { Self::#ident => { /* nothing to do */ }})
                    }
                };
            }
            quote! {
                let mut #total = 0;
                match &self { #(#match_arms)* };
                #total
            }
        }
        syn::Data::Union(_) => todo!(),
    };
    // Build the output, possibly using the input
    let expanded = quote! {
        // The generated impl
        impl #generics #cache_weight for #ident #generics {
            fn indirect_weight(&self) -> usize {
              #body
            }
         }
    };

    // Hand the output tokens back to the compiler
    TokenStream::from(expanded)
}
