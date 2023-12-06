extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{bracketed, parenthesized, Expr, Ident, Result, Token};

enum TermArg {
    Variable(Ident),
    Constant(Expr),
}

struct AtomArgs {
    name: Ident,
    args: Vec<TermArg>,
}

struct RuleMacroInput {
    head: AtomArgs,
    body: Vec<AtomArgs>,
}

impl Parse for RuleMacroInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let head = input.parse::<AtomArgs>()?;
        let mut distinguished_variables: HashMap<String, (&Ident, bool)> = head
            .args
            .iter()
            .filter(|term| matches!(term, TermArg::Variable(_)))
            .map(|variable| match variable {
                TermArg::Variable(ident) => (ident.to_string(), (ident, false)),
                _ => unreachable!(),
            })
            .collect();

        input.parse::<Token![<-]>()?;
        let content2;
        bracketed!(content2 in input);
        let body: syn::punctuated::Punctuated<AtomArgs, Token![,]> =
            content2.parse_terminated(AtomArgs::parse)?;
        let body_vec: Vec<AtomArgs> = body.into_iter().collect();
        body_vec.iter().for_each(|body_atom| {
            body_atom
                .args
                .iter()
                .filter(|term| matches!(term, TermArg::Variable(_)))
                .for_each(|variable| match variable {
                    TermArg::Variable(ident) => {
                        let owned_ident = ident.to_string();

                        if distinguished_variables.contains_key(&owned_ident) {
                            (distinguished_variables.get_mut(&owned_ident).unwrap()).1 = true;
                        }
                    }
                    _ => unreachable!(),
                });
        });

        for (key, value) in distinguished_variables {
            if !value.1 {
                return Err(syn::Error::new(
                    value.0.span(),
                    format!("variable {} not found in the body", key),
                ));
            }
        }

        Ok(RuleMacroInput {
            head,
            body: body_vec,
        })
    }
}

impl Parse for AtomArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let name = input.parse()?;
        let content;
        parenthesized!(content in input);

        let args: syn::punctuated::Punctuated<TermArg, Token![,]> =
            content.parse_terminated(|p| {
                if p.peek(Token![?]) {
                    p.parse::<Token![?]>()?;
                    Ok(TermArg::Variable(p.parse()?))
                } else {
                    Ok(TermArg::Constant(p.parse()?))
                }
            })?;

        let args_vec: Vec<TermArg> = args.into_iter().collect();

        Ok(AtomArgs {
            name,
            args: args_vec,
        })
    }
}

#[proc_macro]
pub fn rule(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as RuleMacroInput);

    let head_name = &input.head.name;
    let head_terms: Vec<_> = input
        .head
        .args
        .iter()
        .map(|arg| match arg {
            TermArg::Variable(ident) => quote! { Term::Variable(stringify!(#ident).to_string()) },
            TermArg::Constant(expr) => quote! { Term::Constant(TypedValue::from(#expr)) },
        })
        .collect();

    let body_atoms: Vec<_> = input
        .body
        .iter()
        .map(|atom| {
            let name = &atom.name;
            let terms: Vec<_> = atom
                .args
                .iter()
                .map(|arg| match arg {
                    TermArg::Variable(ident) => {
                        quote! { Term::Variable(stringify!(#ident).to_string()) }
                    }
                    TermArg::Constant(expr) => quote! { Term::Constant(TypedValue::from(#expr)) },
                })
                .collect();
            quote! { Atom { terms: vec![#(#terms),*], symbol: stringify!(#name).to_string() } }
        })
        .collect();

    let expanded = quote! {
        Rule {
            head: Atom { terms: vec![#(#head_terms),*], symbol: stringify!(#head_name).to_string() },
            body: vec![#(#body_atoms),*],
            id: 0
        }
    };

    expanded.into()
}

struct ProgramMacroInput {
    rules: syn::punctuated::Punctuated<RuleMacroInput, Token![,]>,
}

impl Parse for ProgramMacroInput {
    fn parse(input: ParseStream) -> Result<Self> {
        let rules = input.parse_terminated(RuleMacroInput::parse)?;
        Ok(ProgramMacroInput { rules })
    }
}

#[proc_macro]
pub fn program(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as ProgramMacroInput);

    let rules: Vec<_> = input.rules.into_iter().map(|rule_input| {
        let head_name = &rule_input.head.name;
        let head_terms: Vec<_> = rule_input
            .head
            .args
            .iter()
            .map(|arg| match arg {
                TermArg::Variable(ident) => quote! { Term::Variable(stringify!(#ident).to_string()) },
                TermArg::Constant(expr) => quote! { Term::Constant(TypedValue::from(#expr)) },
            })
            .collect();

        let body_atoms: Vec<_> = rule_input
            .body
            .iter()
            .map(|atom| {
                let name = &atom.name;
                let terms: Vec<_> = atom
                    .args
                    .iter()
                    .map(|arg| match arg {
                        TermArg::Variable(ident) => {
                            quote! { Term::Variable(stringify!(#ident).to_string()) }
                        }
                        TermArg::Constant(expr) => quote! { Term::Constant(TypedValue::from(#expr)) },
                    })
                    .collect();
                quote! { Atom { terms: vec![#(#terms),*], symbol: stringify!(#name).to_string() } }
            })
            .collect();

        quote! {
            Rule {
                head: Atom { terms: vec![#(#head_terms),*], symbol: stringify!(#head_name).to_string() },
                body: vec![#(#body_atoms),*],
                id: 0
            }
        }
    }).collect();

    let expanded = quote! {
        Program::from( vec![#(#rules),*] )
    };

    expanded.into()
}
