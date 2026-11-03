//! ABI normalization utilities.
//!
//! Handles extraction of bare ABI arrays from various artifact formats
//! (raw arrays, Hardhat/Foundry, Truffle), the preprocessing alloy's ABI
//! parser needs (`anonymous` and `param{index}` defaults), and the shared
//! event-signature format the manifest uses.

use anyhow::{Context, Result, anyhow};
use graph::abi::{DynSolType, Event, EventParam};
use serde_json::Value;

/// The AssemblyScript width class a Solidity integer maps to. Drives both the
/// GraphQL scalar the scaffold emits and the getter/setter the codegen emits, so
/// the schema and the generated bindings always agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntWidth {
    /// Fits an `i32` -> GraphQL `Int`.
    I32,
    /// Fits an `i64` -> GraphQL `Int8`.
    I64,
    /// Needs `BigInt`.
    Big,
}

/// Classify a Solidity integer by signedness and bit width. The cutoffs are the
/// point where a value stops fitting the target AssemblyScript int:
///
/// - `i32` is signed 32-bit (`-2^31 ..= 2^31-1`): `int32` fits exactly, and an
///   unsigned value fits while `2^bits - 1 <= 2^31 - 1`, i.e. up to 31 bits.
/// - `i64` is signed 64-bit (`-2^63 ..= 2^63-1`): `int64` fits exactly, and an
///   unsigned value fits up to 63 bits (`uint64`'s `2^64-1` overflows i64).
///
/// The signed cutoffs (32, 64) are exact: lowering either would push `int32` /
/// `int64` to `BigInt` even though they fit. The unsigned cutoffs (31, 63) are
/// the bit limits; Solidity int widths only come in 8-bit steps, so no real type
/// ever lands between the largest fitting width (`uint24` / `uint56`) and the
/// cutoff, making 31/63 and 24/56 equivalent for every valid ABI.
pub fn classify_int_width(signed: bool, bits: u32) -> IntWidth {
    let (i32_max, i64_max) = if signed { (32, 64) } else { (31, 63) };
    if bits <= i32_max {
        IntWidth::I32
    } else if bits <= i64_max {
        IntWidth::I64
    } else {
        IntWidth::Big
    }
}

/// Resolve an `EventParam`'s declared type to `DynSolType`.
pub fn resolve_event_param_type(param: &EventParam) -> DynSolType {
    param
        .selector_type()
        .parse::<DynSolType>()
        .expect("valid ABI type")
}

/// Parse a selector type without panicking.
///
/// [`resolve_event_param_type`] `.expect()`s because codegen only runs on ABIs
/// that already resolved. Scaffolding also runs on hand-written ABIs that alloy
/// accepts but cannot fully resolve (a component-less `tuple`), so it must fall
/// back to a `Bytes` field rather than abort. Returns `None` for such a type.
pub fn try_resolve_selector_type(selector_type: &str) -> Option<DynSolType> {
    selector_type.parse::<DynSolType>().ok()
}

/// The type an indexed param actually carries in the log.
///
/// A topic is exactly 32 bytes, so reference types (strings, bytes, arrays and
/// tuples) do not fit: the EVM stores `keccak256(encoding)` instead and the
/// value itself is not in the log at all. Value types are stored as-is.
pub fn indexed_input_type(param_type: &DynSolType) -> DynSolType {
    match param_type {
        DynSolType::String | DynSolType::Bytes | DynSolType::Tuple(_) => DynSolType::FixedBytes(32),
        DynSolType::Array(_) | DynSolType::FixedArray(_, _) => DynSolType::FixedBytes(32),
        _ => param_type.clone(),
    }
}

/// Whether an event param is reduced to a `bytes32` hash by being indexed, so
/// its value cannot be read back out of the log.
///
/// A type that does not parse (a malformed ABI can declare `tuple` with no
/// components) is reported as not hashed: scaffolding must not abort on an ABI
/// that alloy accepted.
pub fn is_hashed_when_indexed(param: &EventParam) -> bool {
    if !param.indexed {
        return false;
    }
    match param.selector_type().parse::<DynSolType>() {
        Ok(declared) => indexed_input_type(&declared) != declared,
        Err(_) => false,
    }
}

/// Build a manifest event signature, e.g.
/// `Transfer(indexed address,indexed address,uint256)`.
///
/// The `indexed ` marker is graph-node's own convention: it is stripped before
/// hashing topic0, and only the strict manifest-vs-ABI matcher reads it.
///
/// Types come from `selector_type`, which expands a tuple to its components so
/// the hash covers the real shape. It does not normalize the `uint`/`int`
/// aliases, so an ABI declaring `uint` rather than `uint256` yields a signature
/// that hashes to the wrong topic0 and matches no log. graph-node's matcher
/// builds its comparison string the same way, so such a manifest still
/// validates.
pub fn event_signature_with_indexed(event: &Event) -> String {
    let params: Vec<String> = event
        .inputs
        .iter()
        .map(|input| {
            let ty = input.selector_type();
            if input.indexed {
                format!("indexed {}", ty)
            } else {
                ty.into_owned()
            }
        })
        .collect();

    format!("{}({})", event.name, params.join(","))
}

/// Normalize ABI JSON to extract the actual ABI array from various artifact formats.
pub fn normalize_abi_json(abi_str: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(abi_str).context("Failed to parse ABI JSON")?;
    normalize_abi_value(value)
}

/// Extract the bare ABI array from a parsed value, unwrapping artifact formats.
///
/// Supports:
/// - Raw ABI array: `[{...}]`
/// - Foundry/Hardhat format: `{"abi": [...], ...}`
/// - Truffle format: `{"compilerOutput": {"abi": [...], ...}, ...}`
pub fn normalize_abi_value(value: Value) -> Result<Value> {
    // Case 1: Already an array - return as-is
    if value.is_array() {
        return Ok(value);
    }

    // Case 2: Object with "abi" field (Foundry/Hardhat format)
    if let Some(abi) = value.get("abi")
        && abi.is_array()
    {
        return Ok(abi.clone());
    }

    // Case 3: Object with "compilerOutput.abi" field (Truffle format)
    if let Some(compiler_output) = value.get("compilerOutput")
        && let Some(abi) = compiler_output.get("abi")
        && abi.is_array()
    {
        return Ok(abi.clone());
    }

    Err(anyhow!(
        "Invalid ABI format: expected an array or an object with 'abi' field"
    ))
}

/// Normalize a parsed ABI value and add the defaults alloy's parser requires:
/// - `anonymous: false` on events (alloy requires the field)
/// - `param{index}` names for unnamed top-level event parameters (matches
///   graph-cli, so the generated getters and manifest signature agree)
pub fn preprocess_abi_value(value: Value) -> Result<Value> {
    let mut abi = normalize_abi_value(value)?;

    if let Some(items) = abi.as_array_mut() {
        for item in items {
            if let Some(obj) = item.as_object_mut() {
                let is_event = obj.get("type").and_then(|t| t.as_str()) == Some("event");
                if is_event {
                    if !obj.contains_key("anonymous") {
                        obj.insert("anonymous".to_string(), Value::Bool(false));
                    }
                    if let Some(inputs) = obj.get_mut("inputs") {
                        add_default_event_param_names(inputs);
                    }
                }
            }
        }
    }

    Ok(abi)
}

/// Normalize and preprocess ABI JSON, returning the serialized array string that
/// alloy's `JsonAbi` parser accepts.
pub fn preprocess_abi_json(abi_str: &str) -> Result<String> {
    let value: Value = serde_json::from_str(abi_str).context("Failed to parse ABI JSON")?;
    let abi = preprocess_abi_value(value)?;
    serde_json::to_string(&abi).context("Failed to serialize processed ABI")
}

/// Add `param{index}` names to unnamed event parameters to match graph-cli.
///
/// An unnamed param reaches us two ways: solc emits `"name": ""` (the key is
/// always present), while a hand-written ABI may omit the key entirely. Both
/// count as unnamed. The index matches the one the codegen counts from, so the
/// generated getter and this name agree.
fn add_default_event_param_names(params: &mut Value) {
    if let Some(params_arr) = params.as_array_mut() {
        for (index, param) in params_arr.iter_mut().enumerate() {
            if let Some(obj) = param.as_object_mut() {
                let unnamed = obj
                    .get("name")
                    .and_then(|n| n.as_str())
                    .is_none_or(|name| name.is_empty());
                if unnamed {
                    obj.insert("name".to_string(), Value::String(format!("param{}", index)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_normalize_abi_json_raw_array() {
        let raw_abi = r#"[{"type": "event", "name": "Transfer"}]"#;
        let result = normalize_abi_json(raw_abi).unwrap();
        assert!(result.is_array());
        assert_eq!(result.as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_normalize_abi_json_hardhat_format() {
        let hardhat_abi = r#"{
            "_format": "hh-sol-artifact-1",
            "contractName": "MyContract",
            "abi": [{"type": "event", "name": "Transfer"}],
            "bytecode": "0x..."
        }"#;
        let result = normalize_abi_json(hardhat_abi).unwrap();
        assert!(result.is_array());
        assert_eq!(result.as_array().unwrap().len(), 1);
        assert_eq!(
            result.as_array().unwrap()[0].get("name").unwrap(),
            "Transfer"
        );
    }

    #[test]
    fn test_normalize_abi_json_truffle_format() {
        let truffle_abi = r#"{
            "contractName": "MyContract",
            "compilerOutput": {
                "abi": [{"type": "event", "name": "Transfer"}]
            }
        }"#;
        let result = normalize_abi_json(truffle_abi).unwrap();
        assert!(result.is_array());
        assert_eq!(result.as_array().unwrap().len(), 1);
        assert_eq!(
            result.as_array().unwrap()[0].get("name").unwrap(),
            "Transfer"
        );
    }

    #[test]
    fn test_normalize_abi_json_invalid_format() {
        let invalid_abi = r#"{"contractName": "MyContract"}"#;
        let result = normalize_abi_json(invalid_abi);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid ABI format")
        );
    }

    #[test]
    fn test_is_hashed_when_indexed_tolerates_unparseable_type() {
        // alloy accepts a component-less `tuple`, whose selector type is the
        // bare word "tuple" and does not parse as a Solidity type. Scaffolding
        // must not abort on an ABI that parsed.
        let param: EventParam =
            serde_json::from_value(json!({"name": "x", "type": "tuple", "indexed": true})).unwrap();
        assert!(!is_hashed_when_indexed(&param));
    }
}
