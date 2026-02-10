//! ABI normalization utilities.
//!
//! Handles extraction of bare ABI arrays from various artifact formats
//! (raw arrays, Hardhat/Foundry, Truffle).

use anyhow::{anyhow, Context, Result};

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
    if let Some(abi) = value.get("abi") {
        if abi.is_array() {
            return Ok(abi.clone());
        }
    }

    // Case 3: Object with "compilerOutput.abi" field (Truffle format)
    if let Some(compiler_output) = value.get("compilerOutput") {
        if let Some(abi) = compiler_output.get("abi") {
            if abi.is_array() {
                return Ok(abi.clone());
            }
        }
    }

    Err(anyhow!(
        "Invalid ABI format: expected an array or an object with 'abi' field"
    ))
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid ABI format"));
    }
}
