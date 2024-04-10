#![recursion_limit = "256"]

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Generics, Ident, Index, TypeParamBound};

#[proc_macro_derive(CheapClone)]
pub fn derive_cheap_clone(input: TokenStream) -> TokenStream {
    impl_cheap_clone(input.into()).into()
}

fn impl_cheap_clone(input: TokenStream2) -> TokenStream2 {
    fn constrain_generics(generics: &Generics, bound: &TypeParamBound) -> Generics {
        let mut generics = generics.clone();
        for ty in generics.type_params_mut() {
            ty.bounds.push(bound.clone());
        }
        generics
    }

    fn cheap_clone_path() -> TokenStream2 {
        let crate_name = std::env::var("CARGO_PKG_NAME").unwrap();
        if crate_name == "graph" {
            quote! { crate::cheap_clone::CheapClone }
        } else {
            quote! { graph::cheap_clone::CheapClone }
        }
    }

    fn cheap_clone_body(data: Data) -> TokenStream2 {
        match data {
            Data::Struct(st) => match &st.fields {
                Fields::Unit => return quote! { Self  },
                Fields::Unnamed(fields) => {
                    let mut field_clones = Vec::new();
                    for (num, _) in fields.unnamed.iter().enumerate() {
                        let idx = Index::from(num);
                        field_clones.push(quote! { self.#idx.cheap_clone() });
                    }
                    quote! { Self(#(#field_clones,)*) }
                }
                Fields::Named(fields) => {
                    let mut field_clones = Vec::new();
                    for field in fields.named.iter() {
                        let ident = field.ident.as_ref().unwrap();
                        field_clones.push(quote! { #ident: self.#ident.cheap_clone() });
                    }
                    quote! {
                        Self {
                            #(#field_clones,)*
                        }
                    }
                }
            },
            Data::Enum(en) => {
                let mut arms = Vec::new();
                for variant in en.variants {
                    let ident = variant.ident;
                    match variant.fields {
                        Fields::Named(fields) => {
                            let mut idents = Vec::new();
                            let mut clones = Vec::new();
                            for field in fields.named {
                                let ident = field.ident.unwrap();
                                idents.push(ident.clone());
                                clones.push(quote! { #ident: #ident.cheap_clone() });
                            }
                            arms.push(quote! {
                                Self::#ident{#(#idents,)*} => Self::#ident{#(#clones,)*}
                            });
                        }
                        Fields::Unnamed(fields) => {
                            let num_fields = fields.unnamed.len();
                            let idents = (0..num_fields)
                                .map(|i| Ident::new(&format!("v{}", i), Span::call_site()))
                                .collect::<Vec<_>>();
                            let mut cloned = Vec::new();
                            for ident in &idents {
                                cloned.push(quote! { #ident.cheap_clone() });
                            }
                            arms.push(quote! {
                                Self::#ident(#(#idents,)*) => Self::#ident(#(#cloned,)*)
                            });
                        }
                        Fields::Unit => {
                            arms.push(quote! { Self::#ident => Self::#ident });
                        }
                    }
                }
                quote! {
                    match self {
                        #(#arms,)*
                    }
                }
            }
            Data::Union(_) => {
                panic!("Deriving CheapClone for unions is currently not supported.")
            }
        }
    }

    let input = match syn::parse2::<DeriveInput>(input) {
        Ok(input) => input,
        Err(e) => {
            return e.to_compile_error().into();
        }
    };
    let DeriveInput {
        ident: name,
        generics,
        data,
        ..
    } = input;

    let cheap_clone = cheap_clone_path();
    let constrained = constrain_generics(&generics, &syn::parse_quote!(#cheap_clone));
    let body = cheap_clone_body(data);

    let expanded = quote! {
        impl #constrained #cheap_clone for #name #generics {
            fn cheap_clone(&self) -> Self {
                #body
            }
        }
    };

    expanded
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
        syn::Data::Union(_) => {
            panic!("Deriving CacheWeight for unions is currently not supported.")
        }
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

#[cfg(test)]
mod tests {
    use proc_macro_utils::assert_expansion;

    use super::impl_cheap_clone;

    #[test]
    fn cheap_clone() {
        assert_expansion!(
            #[derive(impl_cheap_clone)]
            struct Empty;,
            {
                impl graph::cheap_clone::CheapClone for Empty {
                    fn cheap_clone(&self) -> Self {
                        Self
                    }
                }
            }
        );

        assert_expansion!(
            #[derive(impl_cheap_clone)]
            struct Foo<T> {
                a: T,
                b: u32,
            },
            {
                impl<T: graph::cheap_clone::CheapClone> graph::cheap_clone::CheapClone for Foo<T> {
                    fn cheap_clone(&self) -> Self {
                        Self {
                            a: self.a.cheap_clone(),
                            b: self.b.cheap_clone(),
                        }
                    }
                }
            }
        );

        #[rustfmt::skip]
        assert_expansion!(
            #[derive(impl_cheap_clone)]
            struct Bar(u32, u32);,
            {
                impl graph::cheap_clone::CheapClone for Bar {
                    fn cheap_clone(&self) -> Self {
                        Self(self.0.cheap_clone(), self.1.cheap_clone(),)
                    }
                }
            }
        );

        #[rustfmt::skip]
        assert_expansion!(
            #[derive(impl_cheap_clone)]
            enum Bar {
                A,
                B(u32),
                C { a: u32, b: u32 },
            },
            {
                impl graph::cheap_clone::CheapClone for Bar {
                    fn cheap_clone(&self) -> Self {
                        match self {
                            Self::A => Self::A,
                            Self::B(v0,) => Self::B(v0.cheap_clone(),),
                            Self::C { a, b, } => Self::C {
                                a: a.cheap_clone(),
                                b: b.cheap_clone(),
                            },
                        }
                    }
                }
            }
        );
    }
}
