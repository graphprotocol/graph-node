//! Pre-populates the eth_call cache with mock responses for `gnd test`.
//!
//! Function signatures use graph-node's convention: `name(inputs):(outputs)`
//! e.g. `"balanceOf(address):(uint256)"`, `"getReserves():(uint112,uint112,uint32)"`.
//! Call data is encoded using the same path as production graph-node, so cache
//! IDs match exactly what the runtime generates.

use super::schema::{MockEthCall, TestFile};
use super::trigger::json_to_sol_value;
use anyhow::{anyhow, Context, Result};
use graph::abi::FunctionExt as GraphFunctionExt;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::BlockPtr;
use graph::components::store::EthereumCallCache;
use graph::data::store::ethereum::call;
use graph::prelude::alloy::dyn_abi::{DynSolType, FunctionExt as AlloyFunctionExt};
use graph::prelude::alloy::json_abi::Function;
use graph::prelude::alloy::primitives::Address;
use graph::slog::Logger;
use graph_chain_ethereum::Chain;
use graph_store_postgres::ChainStore;
use std::sync::Arc;

/// ABI-encode a function call (selector + params) using graph-node's encoding path.
fn encode_function_call(function_sig: &str, params: &[serde_json::Value]) -> Result<Vec<u8>> {
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
fn encode_return_value(function_sig: &str, returns: &[serde_json::Value]) -> Result<Vec<u8>> {
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

/// Convert a graph-node style function signature to alloy's expected format.
///
/// Graph-node uses `name(inputs):(outputs)` while alloy expects
/// `name(inputs) returns (outputs)`.
///
/// Examples:
/// - `"balanceOf(address):(uint256)"` → `"balanceOf(address) returns (uint256)"`
/// - `"name():(string)"` → `"name() returns (string)"`
/// - `"transfer(address,uint256)"` → `"transfer(address,uint256)"` (no change)
/// - `"balanceOf(address) returns (uint256)"` → unchanged (already alloy format)
fn to_alloy_signature(sig: &str) -> String {
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

/// Populate the eth_call cache from test block mock calls before indexing starts.
pub async fn populate_eth_call_cache(
    logger: &Logger,
    chain_store: Arc<ChainStore>,
    blocks: &[BlockWithTriggers<Chain>],
    test_file: &TestFile,
) -> Result<()> {
    for (block_data, test_block) in blocks.iter().zip(&test_file.blocks) {
        let block_ptr = block_data.ptr();

        for eth_call in &test_block.eth_calls {
            populate_single_call(logger, chain_store.clone(), &block_ptr, eth_call).await?;
        }
    }
    Ok(())
}

async fn populate_single_call(
    logger: &Logger,
    chain_store: Arc<ChainStore>,
    block_ptr: &BlockPtr,
    eth_call: &MockEthCall,
) -> Result<()> {
    let address: Address = eth_call.address.parse()?;

    let encoded_call = encode_function_call(&eth_call.function, &eth_call.params)?;

    let request = call::Request::new(address, encoded_call, 0);

    let retval = if eth_call.reverts {
        call::Retval::Null
    } else {
        let encoded_return = encode_return_value(&eth_call.function, &eth_call.returns)?;
        call::Retval::Value(encoded_return.into())
    };

    chain_store
        .set_call(logger, request, block_ptr.clone(), retval)
        .await?;

    Ok(())
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
