//! JSON patching utilities for Ethereum blocks and receipts.
//!
//! Some cached blocks are missing the transaction `type` field because
//! graph-node's rust-web3 fork didn't capture it. Alloy requires this field for
//! deserialization. These utilities patch the JSON to add `type: "0x0"` (legacy
//! transaction) where missing.
//!
//! Also used by `PatchingHttp` for chains that don't support EIP-2718 typed transactions,
//! and for blocks that are missing the `sha3Uncles` header field, such as Celo
//! blocks before the Gingerbread hard fork.

use serde_json::Value;

/// The value graph-node's rust-web3 fork used for a missing `sha3Uncles`.
/// Kept as-is so that `ethereum.Block.unclesHash` stays the same for
/// subgraphs indexed before and after the alloy migration.
const ZERO_UNCLES_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

pub fn patch_type_field(obj: &mut Value) -> bool {
    if let Value::Object(map) = obj
        && !map.contains_key("type")
    {
        map.insert("type".to_string(), Value::String("0x0".to_string()));
        return true;
    }
    false
}

pub fn patch_block_transactions(block: &mut Value) -> bool {
    let Some(txs) = block.get_mut("transactions").and_then(|t| t.as_array_mut()) else {
        return false;
    };
    let mut patched = false;
    for tx in txs {
        patched |= patch_type_field(tx);
    }
    patched
}

/// Adds a zero `sha3Uncles` to a block that doesn't have one.
///
/// Celo blocks before the Gingerbread hard fork (block 21616000 on mainnet)
/// have no uncles hash in their header, so RPC nodes omit the field. Alloy
/// requires it. Only objects that look like a block (they have `parentHash`)
/// are patched.
pub fn patch_missing_uncles_hash(block: &mut Value) -> bool {
    if let Value::Object(map) = block
        && map.contains_key("parentHash")
        && !map.contains_key("sha3Uncles")
    {
        map.insert(
            "sha3Uncles".to_string(),
            Value::String(ZERO_UNCLES_HASH.to_string()),
        );
        return true;
    }
    false
}

pub fn patch_receipts(result: &mut Value) -> bool {
    match result {
        Value::Object(_) => patch_type_field(result),
        Value::Array(arr) => {
            let mut patched = false;
            for r in arr {
                patched |= patch_type_field(r);
            }
            patched
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn patch_type_field_adds_missing_type() {
        let mut obj = json!({"status": "0x1", "gasUsed": "0x5208"});
        assert!(patch_type_field(&mut obj));
        assert_eq!(obj["type"], "0x0");
    }

    #[test]
    fn patch_type_field_preserves_existing_type() {
        let mut obj = json!({"status": "0x1", "type": "0x2"});
        assert!(!patch_type_field(&mut obj));
        assert_eq!(obj["type"], "0x2");
    }

    #[test]
    fn patch_type_field_handles_non_object() {
        let mut val = json!("not an object");
        assert!(!patch_type_field(&mut val));
    }

    #[test]
    fn patch_block_transactions_patches_all() {
        let mut block = json!({
            "hash": "0x123",
            "transactions": [
                {"hash": "0xabc", "nonce": "0x1"},
                {"hash": "0xdef", "nonce": "0x2", "type": "0x2"},
                {"hash": "0xghi", "nonce": "0x3"}
            ]
        });
        assert!(patch_block_transactions(&mut block));
        assert_eq!(block["transactions"][0]["type"], "0x0");
        assert_eq!(block["transactions"][1]["type"], "0x2");
        assert_eq!(block["transactions"][2]["type"], "0x0");
    }

    #[test]
    fn patch_block_transactions_handles_empty() {
        let mut block = json!({"hash": "0x123", "transactions": []});
        assert!(!patch_block_transactions(&mut block));
    }

    #[test]
    fn patch_block_transactions_handles_missing_field() {
        let mut block = json!({"hash": "0x123"});
        assert!(!patch_block_transactions(&mut block));
    }

    #[test]
    fn patch_receipts_single() {
        let mut receipt = json!({"status": "0x1"});
        assert!(patch_receipts(&mut receipt));
        assert_eq!(receipt["type"], "0x0");
    }

    #[test]
    fn patch_receipts_array() {
        let mut receipts = json!([
            {"status": "0x1"},
            {"status": "0x1", "type": "0x2"}
        ]);
        assert!(patch_receipts(&mut receipts));
        assert_eq!(receipts[0]["type"], "0x0");
        assert_eq!(receipts[1]["type"], "0x2");
    }

    #[test]
    fn patch_receipts_handles_null() {
        let mut val = Value::Null;
        assert!(!patch_receipts(&mut val));
    }

    #[test]
    fn patch_missing_uncles_hash_adds_zero_hash() {
        let mut block = json!({"hash": "0x123", "parentHash": "0x456"});
        assert!(patch_missing_uncles_hash(&mut block));
        assert_eq!(block["sha3Uncles"], ZERO_UNCLES_HASH);
    }

    #[test]
    fn patch_missing_uncles_hash_preserves_existing() {
        let uncles = "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347";
        let mut block = json!({"hash": "0x123", "parentHash": "0x456", "sha3Uncles": uncles});
        assert!(!patch_missing_uncles_hash(&mut block));
        assert_eq!(block["sha3Uncles"], uncles);
    }

    #[test]
    fn patch_missing_uncles_hash_ignores_non_blocks() {
        let mut receipt = json!({"status": "0x1"});
        assert!(!patch_missing_uncles_hash(&mut receipt));
        assert!(receipt.get("sha3Uncles").is_none());

        let mut val = Value::Null;
        assert!(!patch_missing_uncles_hash(&mut val));
    }
}
