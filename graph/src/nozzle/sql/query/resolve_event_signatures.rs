use std::ops::ControlFlow;

use anyhow::{bail, Context, Result};
use sqlparser_latest::ast::{self, visit_expressions_mut};

use super::Abi;

static FUNCTION_NAME: &str = "sg_event_signature";

/// Replaces `sg_event_signature('CONTRACT_NAME', 'EVENT_NAME')` function calls with
/// the correct event signature based on `abis`.
///
/// # Errors
///
/// Returns an error if:
/// - The function is called with incorrect arguments
/// - The contract name is not found in `abis`
/// - The event name is not found in `abis`
///
/// The returned error is deterministic.
pub(super) fn resolve_event_signatures(query: &mut ast::Query, abis: &[Abi<'_>]) -> Result<()> {
    let visit_result = visit_expressions_mut(query, |expr| match visit_expr(expr, abis) {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => ControlFlow::Break(e),
    });

    if let ControlFlow::Break(e) = visit_result {
        return Err(e).with_context(|| format!("failed to resolve '{FUNCTION_NAME}' calls"));
    }

    Ok(())
}

fn visit_expr(expr: &mut ast::Expr, abis: &[Abi<'_>]) -> Result<()> {
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

    let Some((contract_name, event_name)) = get_args(function) else {
        bail!("invalid function call: expected `{FUNCTION_NAME}('CONTRACT_NAME', 'EVENT_NAME')`, found: `{function}`");
    };

    let Some(event) = get_event(abis, contract_name, event_name) else {
        bail!("invalid function call: unknown contract '{contract_name}' or event '{event_name}'");
    };

    let signature = ast::Value::SingleQuotedString(event.full_signature()).with_empty_span();
    *expr = ast::Expr::Value(signature);

    Ok(())
}

fn get_args<'a>(function: &'a ast::Function) -> Option<(&'a str, &'a str)> {
    let ast::FunctionArguments::List(args) = &function.args else {
        return None;
    };

    if args.args.len() != 2 {
        return None;
    }

    match (get_arg(&args.args[0]), get_arg(&args.args[1])) {
        (Some(contract_name), Some(event_name)) => Some((contract_name, event_name)),
        _ => None,
    }
}

fn get_arg<'a>(arg: &'a ast::FunctionArg) -> Option<&'a str> {
    let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg else {
        return None;
    };

    match expr {
        ast::Expr::Value(ast::ValueWithSpan {
            value: ast::Value::SingleQuotedString(value),
            ..
        }) if !value.is_empty() => Some(value),
        _ => None,
    }
}

fn get_event<'a>(
    abis: &'a [Abi<'_>],
    contract_name: &str,
    event_name: &str,
) -> Option<&'a alloy::json_abi::Event> {
    abis.iter()
        .find(|abi| abi.name.as_str() == contract_name)
        .map(|abi| abi.contract.event(event_name))
        .flatten()
        .map(|events| events.first())
        .flatten()
}
