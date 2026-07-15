//! ABI normalization utilities.
//!
//! Handles extraction of bare ABI arrays from various artifact formats
//! (raw arrays, Hardhat/Foundry, Truffle), the preprocessing alloy's ABI
//! parser needs (`anonymous` and `param{index}` defaults), and the shared
//! event-signature format the manifest uses.

use anyhow::{Context, Result, anyhow};
use graph::abi::Event;
use serde_json::Value;

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
}
