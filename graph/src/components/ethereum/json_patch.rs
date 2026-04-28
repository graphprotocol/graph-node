//! JSON patching utilities for Ethereum blocks and receipts.
//!
//! Some cached blocks are missing the transaction `type` field because
//! graph-node's rust-web3 fork didn't capture it. Alloy requires this field for
//! deserialization. These utilities patch the JSON to add `type: "0x0"` (legacy
//! transaction) where missing.
//!
//! Also used by `PatchingHttp` for chains that don't support EIP-2718 typed transactions.

use serde_json::Value;

pub const MISSING_TRACE_OUTPUT_ERROR: &str =
    "data did not match any variant of untagged enum TraceOutput";

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

pub fn patch_missing_trace_output(raw_traces: &mut Value) -> bool {
    let Some(traces) = raw_traces.as_array_mut() else {
        return false;
    };

    let mut patched = false;
    for trace in traces {
        let Some(result) = trace.get_mut("result") else {
            continue;
        };
        let Some(result_obj) = result.as_object_mut() else {
            continue;
        };

        if result_obj.contains_key("gasUsed") && !result_obj.contains_key("output") {
            result_obj.insert("output".to_owned(), Value::String("0x".to_owned()));
            patched = true;
        }
    }

    patched
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
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
    fn patch_missing_trace_output_adds_missing_output() {
        let mut traces = json!([{
            "result": {
                "gasUsed": "0x0"
            }
        }]);
        assert!(patch_missing_trace_output(&mut traces));
        assert_eq!(traces[0]["result"]["output"], "0x");
    }

    #[test]
    fn patch_missing_trace_output_preserves_existing_output() {
        let mut traces = json!([{
            "result": {
                "gasUsed": "0x1",
                "output": "0xabc"
            }
        }]);
        assert!(!patch_missing_trace_output(&mut traces));
        assert_eq!(traces[0]["result"]["output"], "0xabc");
    }

    #[test]
    fn patch_missing_trace_output_create_without_result_output() {
        let mut traces = json!([{
            "result": {
                "code": "0x60016000"
            }
        }]);
        assert!(!patch_missing_trace_output(&mut traces));
        assert_eq!(traces[0]["result"]["code"], "0x60016000");
    }

    #[test]
    fn patch_missing_trace_output_handles_missing_result() {
        let mut traces = json!([{
            "action": {
                "to": "0x0000000000000000000000000000000000000000"
            }
        }]);
        assert!(!patch_missing_trace_output(&mut traces));
    }

    #[test]
    fn patch_missing_trace_output_deserializes_sonic_fixture() {
        let mut traces = json!([{
            "action": {
                "from": "0xf7cf0d9398d06d5cb7e4d37dc1e18a829bfff934",
                "value": "0x0",
                "gas": "0x0",
                "init": "0x",
                "address": "0xf7cf0d9398d06d5cb7e4d37dc1e18a829bfff934",
                "refundAddress": "0x4c3ccc98c01103be72bcfd29e1d2454c98d1a6e3",
                "balance": "0x0"
            },
            "blockHash": "0x6b747793a61c3ce4e5f3355cf80edcb6aa465913ed43f4b0136d93803cf330f3",
            "blockNumber": 66762070,
            "result": {
                "gasUsed": "0x0"
            },
            "subtraces": 0,
            "traceAddress": [1, 1],
            "transactionHash": "0x5b3dc50c4c7bd9b0e80469b21febbc5d1b54b364a01b22b1e9c426e4632e0b8f",
            "transactionPosition": 0,
            "type": "suicide"
        }]);

        assert!(patch_missing_trace_output(&mut traces));
        let decoded: Vec<LocalizedTransactionTrace> = serde_json::from_value(traces).unwrap();
        assert_eq!(decoded.len(), 1);
    }
}
