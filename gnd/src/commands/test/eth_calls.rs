//! Mock eth_call cache population for `gnd test`.
//!
//! When a subgraph handler executes `ethereum.call()`, graph-node looks up
//! the result in its call cache (PostgreSQL `eth_call_cache` table). By
//! pre-populating this cache with mock responses before indexing starts,
//! tests can control what contract calls return without a real Ethereum node.
//!
//! ## Encoding
//!
//! The cache key is derived from the contract address, the ABI-encoded call
//! data (4-byte selector + encoded parameters), and the block pointer. This
//! module encodes call data using the same `FunctionExt::abi_encode_input()`
//! method that graph-node uses in production (`ethereum_adapter.rs`), ensuring
//! cache IDs match exactly.
//!
//! ## Function signature format
//!
//! Function signatures follow the graph-node convention:
//! ```text
//! functionName(inputTypes):(outputTypes)
//! ```
//! Examples:
//! - `"balanceOf(address):(uint256)"`
//! - `"getReserves():(uint112,uint112,uint32)"`
//! - `"symbol():(string)"`
//!
//! The colon-separated output syntax is converted internally to alloy's
//! `"returns"` syntax for parsing.

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

/// Parse a function signature and ABI-encode the call data (selector + params).
///
/// Uses graph-node's `FunctionExt::abi_encode_input()` — the same encoding path
/// as production `ethereum_adapter.rs:1483-1487` — so the resulting call data
/// produces identical cache IDs.
///
/// # Arguments
/// * `function_sig` - Function signature, e.g. `"balanceOf(address):(uint256)"`
/// * `params` - JSON values for each input parameter
///
/// # Returns
/// Encoded call data: 4-byte selector followed by ABI-encoded parameters.
fn encode_function_call(function_sig: &str, params: &[serde_json::Value]) -> Result<Vec<u8>> {
    let alloy_sig = to_alloy_signature(function_sig);
    let function = Function::parse(&alloy_sig).map_err(|e| {
        anyhow!(
            "Failed to parse function signature '{}': {:?}",
            function_sig,
            e
        )
    })?;

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

/// Parse function outputs from the signature and ABI-encode return values.
///
/// Uses alloy's `JsonAbiExt::abi_encode_output()` which encodes the return
/// values without a selector prefix (just ABI-encoded parameters), matching
/// what an `eth_call` RPC response would contain.
///
/// # Arguments
/// * `function_sig` - Function signature, e.g. `"balanceOf(address):(uint256)"`
/// * `returns` - JSON values for each output parameter
///
/// # Returns
/// ABI-encoded return data (no selector prefix).
fn encode_return_value(function_sig: &str, returns: &[serde_json::Value]) -> Result<Vec<u8>> {
    let alloy_sig = to_alloy_signature(function_sig);
    let function = Function::parse(&alloy_sig).map_err(|e| {
        anyhow!(
            "Failed to parse function signature '{}': {:?}",
            function_sig,
            e
        )
    })?;

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

/// Populate the eth_call cache with mock call responses from test blocks.
///
/// For each `MockEthCall` in the test file's blocks, this function:
/// 1. Parses the contract address
/// 2. Encodes the function call (selector + params) using the same encoding
///    as production graph-node
/// 3. Creates a `call::Request` matching what the runtime would generate
/// 4. Encodes the return value (or marks as revert)
/// 5. Inserts into the cache via `ChainStore::set_call()`
///
/// The cache uses BLAKE3 hashing internally to compute cache IDs from the
/// request + block pointer, ensuring our mock entries are found by the same
/// lookup code that production uses.
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
    let address: Address = eth_call
        .address
        .parse()
        .with_context(|| format!("Invalid contract address: {}", eth_call.address))?;

    let encoded_call =
        encode_function_call(&eth_call.function, &eth_call.params).with_context(|| {
            format!(
                "Failed to encode call for {}::{}",
                eth_call.address, eth_call.function
            )
        })?;

    let request = call::Request::new(address, encoded_call, 0);

    let retval = if eth_call.reverts {
        call::Retval::Null
    } else {
        let encoded_return = encode_return_value(&eth_call.function, &eth_call.returns)
            .with_context(|| {
                format!(
                    "Failed to encode return value for {}::{}",
                    eth_call.address, eth_call.function
                )
            })?;
        call::Retval::Value(encoded_return.into())
    };

    chain_store
        .set_call(logger, request, block_ptr.clone(), retval)
        .await
        .with_context(|| {
            format!(
                "Failed to cache eth_call for {}::{}",
                eth_call.address, eth_call.function
            )
        })?;

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
