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

    if !FUNCTION_NAME.eq_ignore_ascii_case(&function.name.to_string()) {
        return Ok(());
    }

    match &function.args {
        ast::FunctionArguments::None => {}
        ast::FunctionArguments::List(args) if args.args.is_empty() => {}
        _ => {
            bail!("invalid function call: function '{FUNCTION_NAME}' does not accept arguments");
        }
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

#[cfg(test)]
mod tests {
    use super::super::parse_query;
    use super::*;

    use self::fixtures::*;

    mod fixtures {
        use super::*;

        pub(super) const SOURCE_ADDRESS: Address = Address::ZERO;

        pub(super) const RESOLVED_FUNCTION_CALL: &str =
            "arrow_cast(X'0000000000000000000000000000000000000000', 'FixedSizeBinary(20)')";
    }

    macro_rules! test_resolve_source_address {
        ($($name:ident: $query:expr => $expected:expr),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    let mut query = parse_query($query).unwrap();
                    let result = resolve_source_address(&mut query, &SOURCE_ADDRESS);

                    match $expected {
                        Result::<&str, ()>::Ok(expected) => {
                            result.unwrap();
                            assert_eq!(query, parse_query(expected).unwrap());
                        },
                        Err(_) => {
                            result.unwrap_err();
                        }
                    }
                }
            )*
        };
    }

    test_resolve_source_address! {
        nothing_to_resolve: "SELECT a FROM b" => Ok("SELECT a FROM b"),
        call_with_one_argument: "SELECT a FROM b WHERE c = sg_source_address(d)" => Err(()),
        call_with_multiple_argument: "SELECT a FROM b WHERE c = sg_source_address(d, e)" => Err(()),

        resolve_one_call:
            "SELECT a FROM b WHERE c = sg_source_address()" =>
            Ok(&*format!("SELECT a FROM b WHERE c = {RESOLVED_FUNCTION_CALL}")),

        resolve_multiple_calls:
            "SELECT a FROM b WHERE c = sg_source_address() OR d = sg_source_address()" =>
            Ok(&*format!("SELECT a FROM b WHERE c = {RESOLVED_FUNCTION_CALL} OR d = {RESOLVED_FUNCTION_CALL}")),

        resolve_calls_with_case_insensitive_function_name:
            "SELECT a FROM b WHERE c = sg_Source_ADDRESS()" =>
            Ok(&*format!("SELECT a FROM b WHERE c = {RESOLVED_FUNCTION_CALL}")),
    }
}
