//! Mock Alloy transport that serves `eth_getBalance`, `eth_getCode`, and
//! `eth_call` from test JSON mock data. Unmocked RPC calls fail immediately.

use super::eth_calls::{encode_function_call, encode_return_value};
use super::schema::TestFile;
use anyhow::{Context, Result};
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::prelude::alloy::primitives::{Address, B256, U256};
use graph::prelude::alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use graph::prelude::alloy::transports::{TransportError, TransportErrorKind, TransportFut};
use graph_chain_ethereum::Chain;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tower::Service;

#[derive(Debug)]
enum EthCallResult {
    Success(Vec<u8>),
    Revert,
}

#[derive(Debug)]
struct Inner {
    get_balance: HashMap<(Address, B256), U256>,
    get_code: HashMap<(Address, B256), bool>,
    eth_calls: HashMap<(Address, Vec<u8>, B256), EthCallResult>,
}

/// Mock RPC transport backed by test JSON data. Cheap to clone (`Arc` inner).
#[derive(Clone)]
pub struct MockTransport {
    inner: Arc<Inner>,
}

impl MockTransport {
    /// Build from test file mock data, keyed by built block hashes.
    pub fn new(test_file: &TestFile, blocks: &[BlockWithTriggers<Chain>]) -> Result<Self> {
        let mut get_balance = HashMap::new();
        let mut get_code = HashMap::new();
        let mut eth_calls = HashMap::new();

        for (test_block, built_block) in test_file.blocks.iter().zip(blocks.iter()) {
            let block_hash = built_block.ptr().hash.as_b256();

            for mock_balance in &test_block.get_balance_calls {
                let address: Address = mock_balance
                    .address
                    .parse()
                    .context("Invalid address in getBalanceCalls mock")?;
                let value: U256 = mock_balance.value.parse().context(
                    "Invalid value in getBalanceCalls mock — expected decimal Wei string",
                )?;
                get_balance.insert((address, block_hash), value);
            }

            for mock_code in &test_block.has_code_calls {
                let address: Address = mock_code
                    .address
                    .parse()
                    .context("Invalid address in hasCodeCalls mock")?;
                get_code.insert((address, block_hash), mock_code.has_code);
            }

            for eth_call in &test_block.eth_calls {
                let address: Address = eth_call
                    .address
                    .parse()
                    .context("Invalid address in ethCalls mock")?;
                let calldata = encode_function_call(&eth_call.function, &eth_call.params)?;
                let result = if eth_call.reverts {
                    EthCallResult::Revert
                } else {
                    let return_data = encode_return_value(&eth_call.function, &eth_call.returns)?;
                    EthCallResult::Success(return_data)
                };
                eth_calls.insert((address, calldata, block_hash), result);
            }
        }

        Ok(Self {
            inner: Arc::new(Inner {
                get_balance,
                get_code,
                eth_calls,
            }),
        })
    }

    /// Successful JSON-RPC response.
    fn success_response(
        id: &serde_json::Value,
        result: serde_json::Value,
    ) -> Result<ResponsePacket, TransportError> {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        });
        serde_json::from_value(response).map_err(|e| {
            TransportErrorKind::custom_str(&format!("Failed to build mock response: {}", e))
        })
    }

    /// JSON-RPC error response (for reverts).
    fn error_response(
        id: &serde_json::Value,
        code: i64,
        message: &str,
    ) -> Result<ResponsePacket, TransportError> {
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": code,
                "message": message,
            },
        });
        serde_json::from_value(response).map_err(|e| {
            TransportErrorKind::custom_str(&format!("Failed to build mock error response: {}", e))
        })
    }

    /// Extract `(address, block_hash)` from `[address, {"blockHash": hash}]` params.
    fn parse_address_and_block_hash(params: &serde_json::Value) -> Result<(Address, B256), String> {
        let arr = params.as_array().ok_or("Expected array params")?;

        let address_str = arr
            .first()
            .and_then(|v| v.as_str())
            .ok_or("Missing address param")?;
        let address: Address = address_str
            .parse()
            .map_err(|e| format!("Invalid address '{}': {}", address_str, e))?;

        let block_hash_str = arr
            .get(1)
            .and_then(|v| v.get("blockHash"))
            .and_then(|v| v.as_str())
            .ok_or("Missing blockHash in params")?;
        let block_hash: B256 = block_hash_str
            .parse()
            .map_err(|e| format!("Invalid blockHash '{}': {}", block_hash_str, e))?;

        Ok((address, block_hash))
    }

    fn handle_get_balance(
        &self,
        params: &serde_json::Value,
        id: &serde_json::Value,
    ) -> Result<ResponsePacket, TransportError> {
        let (address, block_hash) = Self::parse_address_and_block_hash(params)
            .map_err(|e| TransportErrorKind::custom_str(&format!("eth_getBalance: {}", e)))?;

        match self.inner.get_balance.get(&(address, block_hash)) {
            Some(value) => Self::success_response(id, serde_json::json!(format!("0x{:x}", value))),
            None => Err(TransportErrorKind::custom_str(&format!(
                "gnd test: no mock getBalance entry for address {} at block hash {}. \
                 Add a 'getBalanceCalls' entry to this block in your test JSON.",
                address, block_hash
            ))),
        }
    }

    fn handle_get_code(
        &self,
        params: &serde_json::Value,
        id: &serde_json::Value,
    ) -> Result<ResponsePacket, TransportError> {
        let (address, block_hash) = Self::parse_address_and_block_hash(params)
            .map_err(|e| TransportErrorKind::custom_str(&format!("eth_getCode: {}", e)))?;

        match self.inner.get_code.get(&(address, block_hash)) {
            // graph-node checks `code.len() > 2`; "0xff" is the minimal truthy value.
            Some(true) => Self::success_response(id, serde_json::json!("0xff")),
            Some(false) => Self::success_response(id, serde_json::json!("0x")),
            None => Err(TransportErrorKind::custom_str(&format!(
                "gnd test: no mock hasCode entry for address {} at block hash {}. \
                 Add a 'hasCodeCalls' entry to this block in your test JSON.",
                address, block_hash
            ))),
        }
    }

    fn handle_eth_call(
        &self,
        params: &serde_json::Value,
        id: &serde_json::Value,
    ) -> Result<ResponsePacket, TransportError> {
        let arr = params
            .as_array()
            .ok_or_else(|| TransportErrorKind::custom_str("eth_call: expected array params"))?;

        let tx = arr.first().ok_or_else(|| {
            TransportErrorKind::custom_str("eth_call: missing transaction object")
        })?;

        let to_str = tx
            .get("to")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TransportErrorKind::custom_str("eth_call: missing 'to' field"))?;
        let address: Address = to_str.parse().map_err(|e| {
            TransportErrorKind::custom_str(&format!("eth_call: invalid 'to' address: {}", e))
        })?;

        let input_str = tx
            .get("input")
            .and_then(|v| v.as_str())
            .ok_or_else(|| TransportErrorKind::custom_str("eth_call: missing 'input' field"))?;
        let input =
            hex::decode(input_str.strip_prefix("0x").unwrap_or(input_str)).map_err(|e| {
                TransportErrorKind::custom_str(&format!("eth_call: invalid 'input' hex: {}", e))
            })?;

        let block_hash_str = arr
            .get(1)
            .and_then(|v| v.get("blockHash"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| TransportErrorKind::custom_str("eth_call: missing blockHash"))?;
        let block_hash: B256 = block_hash_str.parse().map_err(|e| {
            TransportErrorKind::custom_str(&format!("eth_call: invalid blockHash: {}", e))
        })?;

        match self.inner.eth_calls.get(&(address, input, block_hash)) {
            Some(EthCallResult::Success(data)) => {
                Self::success_response(id, serde_json::json!(format!("0x{}", hex::encode(data))))
            }
            Some(EthCallResult::Revert) => Self::error_response(id, 3, "execution reverted"),
            None => Err(TransportErrorKind::custom_str(&format!(
                "gnd test: unmocked eth_call to {} at block hash {}. \
                 Add a matching 'ethCalls' entry to this block in your test JSON.",
                address, block_hash
            ))),
        }
    }
}

impl Service<RequestPacket> for MockTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        let transport = self.clone();
        Box::pin(async move {
            let req = match &request {
                RequestPacket::Single(req) => req,
                RequestPacket::Batch(_) => {
                    return Err(TransportErrorKind::custom_str(
                        "gnd test: batch RPC requests are not supported by MockTransport",
                    ));
                }
            };

            let method = req.method();
            let params: serde_json::Value = req
                .params()
                .map(|p| serde_json::from_str(p.get()))
                .transpose()
                .map_err(|e| {
                    TransportErrorKind::custom_str(&format!("Failed to parse params: {}", e))
                })?
                .unwrap_or(serde_json::Value::Null);

            let id = serde_json::to_value(req.id()).map_err(|e| {
                TransportErrorKind::custom_str(&format!("Failed to serialize request id: {}", e))
            })?;

            match method {
                "eth_getBalance" => transport.handle_get_balance(&params, &id),
                "eth_getCode" => transport.handle_get_code(&params, &id),
                "eth_call" => transport.handle_eth_call(&params, &id),
                _ => Err(TransportErrorKind::custom_str(&format!(
                    "gnd test: unmocked RPC method '{}'. Add mock data to your test JSON file.",
                    method
                ))),
            }
        })
    }
}
