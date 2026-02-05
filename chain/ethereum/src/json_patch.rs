//! JSON patching utilities for Ethereum blocks and receipts.
//!
//! Some cached blocks are missing the transaction `type` field because
//! graph-node's rust-web3 fork didn't capture it. Alloy requires this field for
//! deserialization. These utilities patch the JSON to add `type: "0x0"` (legacy
//! transaction) where missing.
//!
//! Also used by `PatchingHttp` for chains that don't support EIP-2718 typed transactions.

use graph::prelude::serde_json::Value;

pub(crate) fn patch_type_field(obj: &mut Value) -> bool {
    if let Value::Object(map) = obj {
        if !map.contains_key("type") {
            map.insert("type".to_string(), Value::String("0x0".to_string()));
            return true;
        }
    }
    false
}

pub(crate) fn patch_block_transactions(block: &mut Value) -> bool {
    let Some(txs) = block.get_mut("transactions").and_then(|t| t.as_array_mut()) else {
        return false;
    };
    let mut patched = false;
    for tx in txs {
        patched |= patch_type_field(tx);
    }
    patched
}

pub(crate) fn patch_receipts(result: &mut Value) -> bool {
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
    use graph::prelude::serde_json::json;

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
}
