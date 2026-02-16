//! ABI encoding of test triggers into graph-node's Ethereum trigger types.
//!
//! This module converts the human-readable JSON test format (event signatures
//! with named parameters) into the binary format that graph-node expects:
//! - Event signatures → topic0 (keccak256 hash)
//! - Indexed parameters → topics[1..3] (ABI-encoded, 32 bytes each)
//! - Non-indexed parameters → data (ABI-encoded tuple)
//!
//! ## Encoding example
//!
//! For `Transfer(address indexed from, address indexed to, uint256 value)`:
//! - topic0 = keccak256("Transfer(address,address,uint256)")
//! - topic1 = left-padded `from` address
//! - topic2 = left-padded `to` address
//! - data = ABI-encoded `value` (uint256, 32 bytes)
//!
//! ## Block construction
//!
//! Each test block is converted to a `BlockWithTriggers<Chain>` containing:
//! - A `LightEthereumBlock` with proper parent hash chaining
//! - Dummy transactions for each unique tx hash (graph-node requires
//!   matching transactions in the block for log processing)
//! - `EthereumTrigger` variants for each log trigger in the test JSON
//! - Auto-injected `Start` and `End` block triggers (so block handlers fire correctly)

use super::schema::{LogEvent, TestFile};
use anyhow::{anyhow, ensure, Context, Result};
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::prelude::alloy::dyn_abi::{DynSolType, DynSolValue};
use graph::prelude::alloy::json_abi::Event;
use graph::prelude::alloy::primitives::{keccak256, Address, Bytes, B256, I256, U256};
use graph::prelude::alloy::rpc::types::Log;
use graph::prelude::{BlockPtr, LightEthereumBlock};
use graph_chain_ethereum::chain::BlockFinality;
use graph_chain_ethereum::trigger::{EthereumBlockTriggerType, EthereumTrigger, LogRef};
use graph_chain_ethereum::Chain;
use std::sync::Arc;

/// Convert all blocks from a test file into graph-node's `BlockWithTriggers` format.
///
/// Blocks are chained together with proper parent hashes (each block's parent_hash
/// points to the previous block's hash). Block numbers auto-increment from
/// `start_block` when not explicitly specified in the test JSON.
///
/// The returned blocks can be fed directly into a `StaticStreamBuilder` for indexing.
pub fn build_blocks_with_triggers(
    test_file: &TestFile,
    start_block: u64,
) -> Result<Vec<BlockWithTriggers<Chain>>> {
    let mut blocks = Vec::new();
    let mut current_number = start_block;
    // Chain blocks together: each block's parent_hash = previous block's hash.
    let mut parent_hash = B256::ZERO;

    for test_block in &test_file.blocks {
        // Use explicit block number or auto-increment from the last block.
        let number = test_block.number.unwrap_or(current_number);

        // Use explicit hash or generate deterministically from block number.
        let hash = test_block
            .hash
            .as_ref()
            .map(|h| h.parse::<B256>())
            .transpose()
            .context("Invalid block hash")?
            .unwrap_or_else(|| keccak256(number.to_be_bytes()));

        // Default timestamp simulates 12-second block times (Ethereum mainnet average).
        // NOTE: Magic number - could be extracted to a named constant.
        let timestamp = test_block.timestamp.unwrap_or(number * 12);

        // Parse base fee per gas if provided (EIP-1559 support).
        let base_fee_per_gas = test_block
            .base_fee_per_gas
            .as_ref()
            .map(|s| s.parse::<u64>())
            .transpose()
            .context("Invalid baseFeePerGas value")?;

        let mut triggers = Vec::new();

        for (log_index, log_event) in test_block.events.iter().enumerate() {
            let eth_trigger = build_log_trigger(number, hash, log_index as u64, log_event)?;
            triggers.push(eth_trigger);
        }

        // Auto-inject block triggers for every block so that block handlers
        // with any filter fire correctly:
        // - Start: matches `once` handlers (at start_block) and initialization handlers
        // - End: matches unfiltered and `polling` handlers
        ensure!(
            number <= i32::MAX as u64,
            "block number {} exceeds i32::MAX",
            number
        );
        let block_ptr = BlockPtr::new(hash.into(), number as i32);
        triggers.push(EthereumTrigger::Block(
            block_ptr.clone(),
            EthereumBlockTriggerType::Start,
        ));
        triggers.push(EthereumTrigger::Block(
            block_ptr,
            EthereumBlockTriggerType::End,
        ));

        let block = create_block_with_triggers(
            number,
            hash,
            parent_hash,
            timestamp,
            base_fee_per_gas,
            triggers,
        )?;
        blocks.push(block);

        // Chain to next block.
        parent_hash = hash;
        current_number = number + 1;
    }

    Ok(blocks)
}

/// Build a single Ethereum log trigger from a test JSON log trigger.
///
/// Creates a fully-formed `EthereumTrigger::Log` with:
/// - ABI-encoded topics and data from the event signature and parameters
/// - Block context (hash, number)
/// - Transaction hash (explicit or deterministic from block_number + log_index)
fn build_log_trigger(
    block_number: u64,
    block_hash: B256,
    log_index: u64,
    trigger: &LogEvent,
) -> Result<EthereumTrigger> {
    let address: Address = trigger
        .address
        .parse()
        .context("Invalid contract address")?;

    // Encode the event signature and parameters into EVM log format.
    let (topics, data) = encode_event_log(&trigger.event, &trigger.params)?;

    // Generate deterministic tx hash if not provided: keccak256(block_number || log_index).
    // This ensures each log in a block gets a unique tx hash by default.
    let tx_hash = trigger
        .tx_hash
        .as_ref()
        .map(|h| h.parse::<B256>())
        .transpose()
        .context("Invalid tx hash")?
        .unwrap_or_else(|| {
            keccak256([block_number.to_be_bytes(), log_index.to_be_bytes()].concat())
        });

    // Construct the alloy Log type that graph-node's trigger processing expects.
    let inner_log = graph::prelude::alloy::primitives::Log {
        address,
        data: graph::prelude::alloy::primitives::LogData::new_unchecked(topics, data),
    };

    let full_log = Arc::new(Log {
        inner: inner_log,
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        block_timestamp: None,
        transaction_hash: Some(tx_hash),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    });

    Ok(EthereumTrigger::Log(LogRef::FullLog(full_log, None)))
}

/// Encode event parameters into EVM log topics and data using `alloy::json_abi::Event::parse()`.
///
/// Given a human-readable event signature like:
///   `"Transfer(address indexed from, address indexed to, uint256 value)"`
/// and parameter values like:
///   `{"from": "0xaaaa...", "to": "0xbbbb...", "value": "1000"}`
///
/// Produces:
/// - topics[0] = keccak256("Transfer(address,address,uint256)") (the event selector)
/// - topics[1] = left-padded `from` address (indexed)
/// - topics[2] = left-padded `to` address (indexed)
/// - data = ABI-encoded `value` as uint256 (non-indexed)
///
/// Indexed parameters become topics (max 3 after topic0), non-indexed parameters
/// are ABI-encoded together as the log data.
pub fn encode_event_log(
    event_sig: &str,
    params: &serde_json::Map<String, serde_json::Value>,
) -> Result<(Vec<B256>, Bytes)> {
    // Event::parse expects "event EventName(...)" format.
    // If the user already wrote "event Transfer(...)" use as-is,
    // otherwise prepend "event ".
    let sig_with_prefix = if event_sig.trim_start().starts_with("event ") {
        event_sig.to_string()
    } else {
        format!("event {}", event_sig)
    };

    let event = Event::parse(&sig_with_prefix)
        .map_err(|e| anyhow!("Failed to parse event signature '{}': {:?}", event_sig, e))?;

    // topic0 is the event selector (keccak256 of canonical signature)
    let topic0 = event.selector();
    let mut topics = vec![topic0];
    let mut data_values = Vec::new();

    for input in &event.inputs {
        let value = params
            .get(&input.name)
            .ok_or_else(|| anyhow!("Missing parameter: {}", input.name))?;

        let sol_type: DynSolType = input
            .ty
            .parse()
            .map_err(|e| anyhow!("Invalid type '{}': {:?}", input.ty, e))?;

        let sol_value = json_to_sol_value(&sol_type, value)?;

        if input.indexed {
            let topic = sol_value_to_topic(&sol_value)?;
            topics.push(topic);
        } else {
            data_values.push(sol_value);
        }
    }

    let data = if data_values.is_empty() {
        Bytes::new()
    } else {
        let tuple = DynSolValue::Tuple(data_values);
        Bytes::from(tuple.abi_encode_params())
    };

    Ok((topics, data))
}

/// Convert a JSON value to the corresponding Solidity dynamic value type.
///
/// Handles the common Solidity types that appear in event parameters:
/// - `address`: hex string → 20-byte address
/// - `uint8`..`uint256`: string (decimal/hex) or JSON number → unsigned integer
/// - `int8`..`int256`: string (decimal/hex, optionally negative) → signed integer (two's complement)
/// - `bool`: JSON boolean
/// - `bytes`: hex string → dynamic byte array
/// - `string`: JSON string
/// - `bytes1`..`bytes32`: hex string → fixed-length byte array (right-zero-padded to 32 bytes)
pub fn json_to_sol_value(sol_type: &DynSolType, value: &serde_json::Value) -> Result<DynSolValue> {
    match sol_type {
        DynSolType::Address => {
            let s = value
                .as_str()
                .ok_or_else(|| anyhow!("Expected string for address"))?;
            let addr: Address = s.parse().context("Invalid address")?;
            Ok(DynSolValue::Address(addr))
        }
        DynSolType::Uint(bits) => {
            let n = match value {
                // String values support both decimal and "0x"-prefixed hex.
                serde_json::Value::String(s) => {
                    let (digits, radix) = match s.strip_prefix("0x") {
                        Some(hex) => (hex, 16),
                        None => (s.as_str(), 10),
                    };
                    U256::from_str_radix(digits, radix).context("Invalid uint")?
                }
                // JSON numbers are limited to u64 range — use strings for larger values.
                serde_json::Value::Number(n) => U256::from(n.as_u64().ok_or_else(|| {
                    anyhow!("uint value {} does not fit in u64, use a string instead", n)
                })?),
                _ => return Err(anyhow!("Expected string or number for uint")),
            };
            Ok(DynSolValue::Uint(n, *bits))
        }
        DynSolType::Int(bits) => {
            let n = match value {
                serde_json::Value::String(s) => {
                    let (is_neg, s_abs) = match s.strip_prefix('-') {
                        Some(rest) => (true, rest),
                        None => (false, s.as_str()),
                    };
                    let (digits, radix) = match s_abs.strip_prefix("0x") {
                        Some(hex) => (hex, 16),
                        None => (s_abs, 10),
                    };
                    let abs = U256::from_str_radix(digits, radix).context("Invalid int")?;
                    if is_neg {
                        !abs + U256::from(1) // Two's complement negation
                    } else {
                        abs
                    }
                }
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        // into_raw() gives the two's complement U256 representation.
                        // Handles i64::MIN correctly (unlike `-i as u64` which overflows).
                        I256::try_from(i).unwrap().into_raw()
                    } else {
                        U256::from(n.as_u64().ok_or_else(|| {
                            anyhow!(
                                "int value {} not representable as u64, use a string instead",
                                n
                            )
                        })?)
                    }
                }
                _ => return Err(anyhow!("Expected string or number for int")),
            };
            Ok(DynSolValue::Int(I256::from_raw(n), *bits))
        }
        DynSolType::Bool => {
            let b = value.as_bool().ok_or_else(|| anyhow!("Expected bool"))?;
            Ok(DynSolValue::Bool(b))
        }
        DynSolType::Bytes => {
            let s = value
                .as_str()
                .ok_or_else(|| anyhow!("Expected string for bytes"))?;
            let hex_str = s.strip_prefix("0x").unwrap_or(s);
            let bytes = hex::decode(hex_str).context("Invalid hex")?;
            Ok(DynSolValue::Bytes(bytes))
        }
        DynSolType::String => {
            let s = value.as_str().ok_or_else(|| anyhow!("Expected string"))?;
            Ok(DynSolValue::String(s.to_string()))
        }
        DynSolType::FixedBytes(len) => {
            let s = value
                .as_str()
                .ok_or_else(|| anyhow!("Expected string for bytes{}", len))?;
            let hex_str = s.strip_prefix("0x").unwrap_or(s);
            let bytes = hex::decode(hex_str).context("Invalid hex")?;
            if bytes.len() > *len {
                return Err(anyhow!(
                    "bytes{}: got {} bytes, expected at most {}",
                    len,
                    bytes.len(),
                    len
                ));
            }
            // DynSolValue::FixedBytes always wraps a B256 (32 bytes) plus the actual
            // byte count. Right-zero-pad the input to fill the full 32 bytes.
            let mut padded = [0u8; 32];
            padded[..bytes.len()].copy_from_slice(&bytes);
            Ok(DynSolValue::FixedBytes(B256::from(padded), *len))
        }
        DynSolType::Array(inner) => {
            let arr = value
                .as_array()
                .ok_or_else(|| anyhow!("Expected JSON array for array type"))?;
            let elements: Vec<DynSolValue> = arr
                .iter()
                .map(|elem| json_to_sol_value(inner, elem))
                .collect::<Result<_>>()?;
            Ok(DynSolValue::Array(elements))
        }
        DynSolType::FixedArray(inner, size) => {
            let arr = value
                .as_array()
                .ok_or_else(|| anyhow!("Expected JSON array for fixed array type"))?;
            ensure!(
                arr.len() == *size,
                "Fixed array expects {} elements, got {}",
                size,
                arr.len()
            );
            let elements: Vec<DynSolValue> = arr
                .iter()
                .map(|elem| json_to_sol_value(inner, elem))
                .collect::<Result<_>>()?;
            Ok(DynSolValue::FixedArray(elements))
        }
        DynSolType::Tuple(types) => {
            let arr = value
                .as_array()
                .ok_or_else(|| anyhow!("Expected JSON array for tuple type (positional)"))?;
            ensure!(
                arr.len() == types.len(),
                "Tuple expects {} elements, got {}",
                types.len(),
                arr.len()
            );
            let values: Vec<DynSolValue> = types
                .iter()
                .zip(arr.iter())
                .map(|(ty, val)| json_to_sol_value(ty, val))
                .collect::<Result<_>>()?;
            Ok(DynSolValue::Tuple(values))
        }
        _ => Err(anyhow!("Unsupported type: {:?}", sol_type)),
    }
}

/// Convert a Solidity value to a 32-byte topic for indexed event parameters.
///
/// EVM log topics are always exactly 32 bytes. The encoding depends on the type:
/// - Addresses: left-padded to 32 bytes (12 zero bytes + 20 address bytes)
/// - Integers: stored as big-endian 32-byte values
/// - Booleans: 0x00...00 (false) or 0x00...01 (true)
/// - Fixed bytes: stored directly (already 32 bytes in B256)
/// - Dynamic types (bytes, string): keccak256-hashed (the value itself is not recoverable)
fn sol_value_to_topic(value: &DynSolValue) -> Result<B256> {
    match value {
        DynSolValue::Address(addr) => {
            // Addresses are left-padded: 12 zero bytes + 20 address bytes.
            let mut bytes = [0u8; 32];
            bytes[12..].copy_from_slice(addr.as_slice());
            Ok(B256::from(bytes))
        }
        DynSolValue::Uint(n, _) => Ok(B256::from(*n)),
        DynSolValue::Int(n, _) => Ok(B256::from(n.into_raw())),
        DynSolValue::Bool(b) => {
            let mut bytes = [0u8; 32];
            if *b {
                bytes[31] = 1;
            }
            Ok(B256::from(bytes))
        }
        DynSolValue::FixedBytes(b, _) => Ok(*b),
        // Dynamic types are hashed per Solidity spec — the original value
        // cannot be recovered from the topic.
        DynSolValue::Bytes(b) => Ok(keccak256(b)),
        DynSolValue::String(s) => Ok(keccak256(s.as_bytes())),
        _ => Err(anyhow!("Cannot convert {:?} to topic", value)),
    }
}

/// Create a dummy transaction with a specific hash for block transaction lists.
///
/// Graph-node looks up transactions by hash during log processing, so we need
/// matching dummy transactions in the block body.
fn dummy_transaction(
    block_number: u64,
    block_hash: B256,
    transaction_index: u64,
    transaction_hash: B256,
) -> graph::prelude::alloy::rpc::types::Transaction<graph::prelude::alloy::consensus::TxEnvelope> {
    use graph::prelude::alloy::consensus::transaction::Recovered;
    use graph::prelude::alloy::consensus::{Signed, TxEnvelope, TxLegacy};
    use graph::prelude::alloy::primitives::{Address, Signature, U256};
    use graph::prelude::alloy::rpc::types::Transaction;

    let signed = Signed::new_unchecked(
        TxLegacy::default(),
        Signature::new(U256::from(1), U256::from(1), false),
        transaction_hash,
    );

    Transaction {
        inner: Recovered::new_unchecked(TxEnvelope::Legacy(signed), Address::ZERO),
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        transaction_index: Some(transaction_index),
        effective_gas_price: None,
    }
}

/// Create a `BlockWithTriggers<Chain>` from block metadata and triggers.
fn create_block_with_triggers(
    number: u64,
    hash: B256,
    parent_hash: B256,
    timestamp: u64,
    base_fee_per_gas: Option<u64>,
    triggers: Vec<EthereumTrigger>,
) -> Result<BlockWithTriggers<Chain>> {
    use graph::prelude::alloy::consensus::Header as ConsensusHeader;
    use graph::prelude::alloy::rpc::types::{Block, BlockTransactions, Header};
    use std::collections::HashSet;

    // Collect unique transaction hashes from log triggers.
    let mut tx_hashes: HashSet<B256> = HashSet::new();
    for trigger in &triggers {
        if let EthereumTrigger::Log(LogRef::FullLog(log, _)) = trigger {
            if let Some(tx_hash) = log.transaction_hash {
                tx_hashes.insert(tx_hash);
            }
        }
    }

    let transactions: Vec<_> = tx_hashes
        .into_iter()
        .enumerate()
        .map(|(idx, tx_hash)| dummy_transaction(number, hash, idx as u64, tx_hash))
        .collect();

    let alloy_block = Block::empty(Header {
        hash,
        inner: ConsensusHeader {
            number,
            parent_hash,
            timestamp,
            base_fee_per_gas,
            ..Default::default()
        },
        total_difficulty: None,
        size: None,
    })
    .with_transactions(BlockTransactions::Full(transactions));

    let light_block = LightEthereumBlock::new(alloy_block.into());
    let finality_block = BlockFinality::Final(Arc::new(light_block));

    Ok(BlockWithTriggers::new(
        finality_block,
        triggers,
        &graph::log::logger(false),
    ))
}
