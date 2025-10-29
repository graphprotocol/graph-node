use std::ops::ControlFlow;

use alloy::primitives::Address;
use anyhow::{bail, Context, Result};
use sqlparser_latest::ast::{self, visit_expressions_mut};

static FUNCTION_NAME: &str = "sg_source_address";

/// Replaces `sg_source_address()` function calls in the SQL query with the `source_address`.
///
/// # Errors
///
/// Returns an error if the function is called with any arguments.
///
/// The returned error is deterministic.
pub(super) fn resolve_source_address(
    query: &mut ast::Query,
    source_address: &Address,
) -> Result<()> {
    let visit_result =
        visit_expressions_mut(query, |expr| match visit_expr(expr, source_address) {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(e),
        });

    if let ControlFlow::Break(e) = visit_result {
        return Err(e).with_context(|| format!("failed to resolve '{FUNCTION_NAME}' calls"));
    }

    Ok(())
}

fn visit_expr(expr: &mut ast::Expr, source_address: &Address) -> Result<()> {
    let ast::Expr::Function(function) = expr else {
        return Ok(());
    };

    let mut ident_iter = function.name.0.iter().rev();
    let Some(ast::ObjectNamePart::Identifier(ident)) = ident_iter.next() else {
        return Ok(());
    };

    if !FUNCTION_NAME.eq_ignore_ascii_case(&ident.value) {
        return Ok(());
    }

    if ident_iter.next().is_some() {
        return Ok(());
    }

    if !matches!(function.args, ast::FunctionArguments::None) {
        bail!("invalid function call: function '{FUNCTION_NAME}' does not accept arguments");
    }

    *function = ast::Function {
        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
            "arrow_cast",
        ))]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::HexStringLiteral(hex::encode(source_address)).with_empty_span(),
                ))),
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(
                    ast::Value::SingleQuotedString("FixedSizeBinary(20)".to_string())
                        .with_empty_span(),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };

    Ok(())
}
