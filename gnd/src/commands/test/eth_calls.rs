//! ABI encoding helpers for mock Ethereum call data.

use super::trigger::json_to_sol_value;
use anyhow::{Context, Result, anyhow};
use graph::abi::FunctionExt as GraphFunctionExt;
use graph::prelude::alloy::dyn_abi::{DynSolType, FunctionExt as AlloyFunctionExt};
use graph::prelude::alloy::json_abi::Function;

/// ABI-encode a function call (selector + params) using graph-node's encoding path.
pub(super) fn encode_function_call(
    function_sig: &str,
    params: &[serde_json::Value],
) -> Result<Vec<u8>> {
    let alloy_sig = to_alloy_signature(function_sig);
    let function = Function::parse(&alloy_sig).map_err(|e| {
        anyhow!(
            "Failed to parse function signature '{}': {:?}",
            function_sig,
            e
        )
    })?;

    if params.len() != function.inputs.len() {
        return Err(anyhow!(
            "Parameter count mismatch for '{}': expected {} parameters, got {}",
            function_sig,
            function.inputs.len(),
            params.len()
        ));
    }

    let args: Vec<_> = params
        .iter()
        .zip(&function.inputs)
        .map(|(json, param)| {
            let sol_type: DynSolType = param
                .ty
                .parse()
                .map_err(|e| anyhow!("Invalid type '{}': {:?}", param.ty, e))?;
            json_to_sol_value(&sol_type, json)
        })
        .collect::<Result<Vec<_>>>()?;

    GraphFunctionExt::abi_encode_input(&function, &args).context("Failed to encode function call")
}

/// ABI-encode function return values (no selector prefix).
pub(super) fn encode_return_value(
    function_sig: &str,
    returns: &[serde_json::Value],
) -> Result<Vec<u8>> {
    let alloy_sig = to_alloy_signature(function_sig);
    let function = Function::parse(&alloy_sig).map_err(|e| {
        anyhow!(
            "Failed to parse function signature '{}': {:?}",
            function_sig,
            e
        )
    })?;

    if returns.len() != function.outputs.len() {
        return Err(anyhow!(
            "Return value count mismatch for '{}': expected {} return values, got {}",
            function_sig,
            function.outputs.len(),
            returns.len()
        ));
    }

    let output_values: Vec<_> = returns
        .iter()
        .zip(&function.outputs)
        .map(|(json, param)| {
            let sol_type: DynSolType = param
                .ty
                .parse()
                .map_err(|e| anyhow!("Invalid type '{}': {:?}", param.ty, e))?;
            json_to_sol_value(&sol_type, json)
        })
        .collect::<Result<Vec<_>>>()?;

    AlloyFunctionExt::abi_encode_output(&function, &output_values)
        .map_err(|e| anyhow!("Failed to encode return value: {}", e))
}

/// Convert graph-node `name(inputs):(outputs)` to alloy `name(inputs) returns (outputs)`.
/// Passes through signatures already in alloy format or without outputs.
pub(super) fn to_alloy_signature(sig: &str) -> String {
    // If it already contains "returns", assume alloy format.
    if sig.contains(" returns ") {
        return sig.to_string();
    }

    // Look for the "):(" pattern that separates inputs from outputs.
    if let Some(pos) = sig.find("):(") {
        let inputs = &sig[..=pos]; // "name(inputs)"
        let outputs = &sig[pos + 2..]; // "(outputs)"
        format!("{} returns {}", inputs, outputs)
    } else {
        sig.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_alloy_signature_with_colon() {
        assert_eq!(
            to_alloy_signature("balanceOf(address):(uint256)"),
            "balanceOf(address) returns (uint256)"
        );
    }

    #[test]
    fn test_to_alloy_signature_multiple_outputs() {
        assert_eq!(
            to_alloy_signature("getReserves():(uint112,uint112,uint32)"),
            "getReserves() returns (uint112,uint112,uint32)"
        );
    }

    #[test]
    fn test_to_alloy_signature_no_outputs() {
        assert_eq!(
            to_alloy_signature("transfer(address,uint256)"),
            "transfer(address,uint256)"
        );
    }

    #[test]
    fn test_to_alloy_signature_already_alloy_format() {
        assert_eq!(
            to_alloy_signature("balanceOf(address) returns (uint256)"),
            "balanceOf(address) returns (uint256)"
        );
    }

    #[test]
    fn test_encode_function_call_balanceof() {
        let encoded = encode_function_call(
            "balanceOf(address):(uint256)",
            &[serde_json::json!(
                "0x0000000000000000000000000000000000000001"
            )],
        )
        .unwrap();

        // First 4 bytes should be the selector for balanceOf(address)
        assert_eq!(&encoded[..4], &[0x70, 0xa0, 0x82, 0x31]);
        // Total length: 4 (selector) + 32 (address param) = 36
        assert_eq!(encoded.len(), 36);
    }

    #[test]
    fn test_encode_return_value_uint256() {
        let encoded = encode_return_value(
            "balanceOf(address):(uint256)",
            &[serde_json::json!("1000000000000000000")],
        )
        .unwrap();

        // ABI-encoded uint256 is 32 bytes (no selector)
        assert_eq!(encoded.len(), 32);
    }

    #[test]
    fn test_encode_function_call_no_params() {
        let encoded = encode_function_call("symbol():(string)", &[]).unwrap();

        // Just the 4-byte selector
        assert_eq!(encoded.len(), 4);
    }
}
