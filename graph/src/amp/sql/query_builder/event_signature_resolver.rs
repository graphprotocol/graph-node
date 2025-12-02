use std::ops::ControlFlow;

use alloy::json_abi::JsonAbi;
use anyhow::{bail, Context, Result};
use sqlparser_latest::ast::{self, visit_expressions_mut};

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
pub(super) fn resolve_event_signatures(
    query: &mut ast::Query,
    abis: &[(&str, &JsonAbi)],
) -> Result<()> {
    let visit_result = visit_expressions_mut(query, |expr| match visit_expr(expr, abis) {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => ControlFlow::Break(e),
    });

    if let ControlFlow::Break(e) = visit_result {
        return Err(e).with_context(|| format!("failed to resolve '{FUNCTION_NAME}' calls"));
    }

    Ok(())
}

fn visit_expr(expr: &mut ast::Expr, abis: &[(&str, &JsonAbi)]) -> Result<()> {
    let ast::Expr::Function(function) = expr else {
        return Ok(());
    };

    if !FUNCTION_NAME.eq_ignore_ascii_case(&function.name.to_string()) {
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
    abis: &'a [(&str, &JsonAbi)],
    contract_name: &str,
    event_name: &str,
) -> Option<&'a alloy::json_abi::Event> {
    abis.iter()
        .filter(|(name, _)| *name == contract_name)
        .map(|(_, contract)| contract.event(event_name))
        .flatten()
        .map(|events| events.first())
        .flatten()
        .next()
}

#[cfg(test)]
mod tests {
    use super::super::parse_query;
    use super::*;

    use self::fixtures::*;

    mod fixtures {
        use std::sync::LazyLock;

        use super::*;

        pub(super) static ABIS: LazyLock<Vec<(&str, JsonAbi)>> = LazyLock::new(|| {
            vec![
                ("ContractA", JsonAbi::parse([&*event("TransferA")]).unwrap()),
                ("ContractB", JsonAbi::parse([&*event("TransferB")]).unwrap()),
                ("ContractB", JsonAbi::parse([&*event("TransferC")]).unwrap()),
            ]
        });

        pub(super) fn event(name: &str) -> String {
            format!("event {name}(address indexed from, address indexed to, address value)")
        }
    }

    macro_rules! test_resolve_event_signatures {
        ($($name:ident: $query:expr => $expected:expr),* $(,)?) => {
            $(
                #[test]
                fn $name() {
                    let mut query = parse_query($query).unwrap();
                    let abis = ABIS.iter().map(|abi| (abi.0, &abi.1)).collect::<Vec<_>>();
                    let result = resolve_event_signatures(&mut query, &abis);

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

    test_resolve_event_signatures! {
        nothing_to_resolve: "SELECT a FROM b" => Ok("SELECT a FROM b"),

        call_with_no_arguments: "SELECT a FROM b WHERE c = sg_event_signature()" => Err(()),
        call_with_one_argument: "SELECT a FROM b WHERE c = sg_event_signature('ContractA')" => Err(()),
        call_with_first_invalid_argument: "SELECT a FROM b WHERE c = sg_event_signature(ContractA, 'TransferA')" => Err(()),
        call_with_second_invalid_argument: "SELECT a FROM b WHERE c = sg_event_signature('ContractA', TransferA)" => Err(()),
        call_with_two_invalid_arguments: "SELECT a FROM b WHERE c = sg_event_signature(ContractA, TransferA)" => Err(()),
        call_with_unknown_contract: "SELECT a FROM b WHERE c = sg_event_signature('ContractX', 'TransferA')" => Err(()),
        call_with_unknown_event: "SELECT a FROM b WHERE c = sg_event_signature('ContractA', 'TransferX')" => Err(()),
        call_with_contract_and_event_mismatch: "SELECT a FROM b WHERE c = sg_event_signature('ContractA', 'TransferB')" => Err(()),
        call_with_invalid_argument_cases: "SELECT a FROM b WHERE c = sg_event_signature('contractA', 'transferA')" => Err(()),

        resolve_one_call:
            "SELECT a FROM b WHERE c = sg_event_signature('ContractA', 'TransferA')" =>
            Ok(&*format!("SELECT a FROM b WHERE c = '{}'", event("TransferA"))),

        resolve_multiple_calls:
            "SELECT a FROM b WHERE c = sg_event_signature('ContractA', 'TransferA') OR d = sg_event_signature('ContractA', 'TransferA')" =>
            Ok(&*format!("SELECT a FROM b WHERE c = '{}' OR d = '{}'", event("TransferA"), event("TransferA"))),

        resolve_multiple_calls_with_different_arguments:
            "SELECT a FROM b WHERE c = sg_event_signature('ContractA', 'TransferA') OR d = sg_event_signature('ContractB', 'TransferB')" =>
            Ok(&*format!("SELECT a FROM b WHERE c = '{}' OR d = '{}'", event("TransferA"), event("TransferB"))),

        resolve_multiple_calls_with_events_from_different_abis_with_the_same_name:
            "SELECT a FROM b WHERE c = sg_event_signature('ContractB', 'TransferB') OR d = sg_event_signature('ContractB', 'TransferC')" =>
            Ok(&*format!("SELECT a FROM b WHERE c = '{}' OR d = '{}'", event("TransferB"), event("TransferC"))),

        resolve_calls_with_case_insensitive_function_name:
            "SELECT a FROM b WHERE c = sg_Event_SIGNATURE('ContractA', 'TransferA')" =>
            Ok(&*format!("SELECT a FROM b WHERE c = '{}'", event("TransferA"))),
    }
}
