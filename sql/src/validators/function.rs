use anyhow::{anyhow, Ok, Result};
use core::ops::ControlFlow;
use std::collections::HashSet;

use sqlparser::ast::{Expr, Query, Visit, Visitor};

/// The type of a function in the SQL ast tree
#[derive(PartialEq, Clone, Debug)]
pub enum FunctionValidationResult {
    /// Function is unknown
    Unknown(String),
    /// Function is known and not allowed
    BlackListed,
    // Type can be extended ie Performance, Security, etc for more granular control
}

/// A validator for SQL functions
pub struct FunctionValidator<'a> {
    // Hashset<&'a str> is used instead of HashSet<String> to avoid allocations
    // Hashset<&'a str> is used instead of HashMap<&'a str, FunctionType> to avoid allocations for the values  (see Enum: https://en.wikipedia.org/wiki/Tagged_union)
    whitelisted: &'a HashSet<&'a str>,
    blacklisted: &'a HashSet<&'a str>,
}

impl<'a> FunctionValidator<'a> {
    /// Creates a new validator
    pub fn new(whitelisted: &'a HashSet<&'a str>, blacklisted: &'a HashSet<&'a str>) -> Self {
        Self {
            whitelisted,
            blacklisted,
        }
    }

    pub fn validate_query(&mut self, query: &Query) -> Result<()> {
        match query.visit(self) {
            ControlFlow::Break(FunctionValidationResult::Unknown(name)) => {
                Err(anyhow!("Function {} is unknown", name))
            }
            ControlFlow::Break(FunctionValidationResult::BlackListed) => {
                Err(anyhow!("Function is blacklisted"))
            }
            _ => Ok(()),
        }
    }
}

impl<'a> Visitor for FunctionValidator<'a> {
    type Break = FunctionValidationResult;

    /// Invoked for any expressions that appear in the AST
    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = _expr {
            if let Some(&ref ident) = function.name.0.last() {
                let name = ident.value.to_lowercase();
                if self.blacklisted.contains(name.as_str()) {
                    return ControlFlow::Break(FunctionValidationResult::BlackListed);
                }
                if !self.whitelisted.contains(name.as_str()) {
                    return ControlFlow::Break(FunctionValidationResult::Unknown(name));
                }
            }
        }

        ControlFlow::Continue(())
    }
}
