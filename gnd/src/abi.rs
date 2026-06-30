//! ABI normalization utilities.
//!
//! Handles extraction of bare ABI arrays from various artifact formats
//! (raw arrays, Hardhat/Foundry, Truffle).

use anyhow::{Context, Result, anyhow};

/// Normalize ABI JSON to extract the actual ABI array from various artifact formats.
///
/// Supports:
/// - Raw ABI array: `[{...}]`
/// - Foundry/Hardhat format: `{"abi": [...], ...}`
/// - Truffle format: `{"compilerOutput": {"abi": [...], ...}, ...}`
pub fn normalize_abi_json(abi_str: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value =
        serde_json::from_str(abi_str).context("Failed to parse ABI JSON")?;

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

/// Preprocess ABI JSON to normalize artifact formats and add defaults
/// required by alloy's ABI parser:
/// - `anonymous: false` for events (alloy requires this field)
/// - `param{index}` names for unnamed event parameters (to match graph-cli behavior)
///
/// This is shared by codegen and validation so that both parse the ABI in
/// exactly the same way: an ABI that codegen accepts must also be accepted by
/// validation (and vice versa).
pub fn preprocess_abi_json(abi_str: &str) -> Result<String> {
    // Normalize to get the ABI array from various artifact formats
    let mut abi = normalize_abi_json(abi_str)?;

    if let Some(items) = abi.as_array_mut() {
        for item in items {
            if let Some(obj) = item.as_object_mut() {
                let is_event = obj
                    .get("type")
                    .and_then(|t| t.as_str())
                    .map(|t| t == "event")
                    .unwrap_or(false);

                if is_event {
                    // Add anonymous: false for events if missing (alloy requires it)
                    if !obj.contains_key("anonymous") {
                        obj.insert("anonymous".to_string(), serde_json::Value::Bool(false));
                    }

                    // Add param{index} names for unnamed event parameters
                    if let Some(inputs) = obj.get_mut("inputs") {
                        add_default_event_param_names(inputs);
                    }
                }
            }
        }
    }

    serde_json::to_string(&abi).context("Failed to serialize processed ABI")
}

/// Add `param{index}` names to unnamed event parameters to match graph-cli behavior.
/// Alloy defaults missing names to empty strings, but for events we want `param0`, `param1`, etc.
fn add_default_event_param_names(params: &mut serde_json::Value) {
    if let Some(params_arr) = params.as_array_mut() {
        for (index, param) in params_arr.iter_mut().enumerate() {
            if let Some(obj) = param.as_object_mut()
                && !obj.contains_key("name")
            {
                obj.insert(
                    "name".to_string(),
                    serde_json::Value::String(format!("param{}", index)),
                );
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
