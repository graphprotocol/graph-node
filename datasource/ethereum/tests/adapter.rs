extern crate ethabi;
extern crate ethereum_types;
extern crate futures;
extern crate graph;
extern crate graph_datasource_ethereum;
extern crate jsonrpc_core;
extern crate web3;

use ethabi::{Function, Param, ParamType, Token};
use futures::prelude::*;
use futures::{failed, finished};
use graph::components::ethereum::EthereumContractCall;
use graph::prelude::{*, EthereumAdapter as EthereumAdapterTrait};
use graph::serde_json;
use graph_datasource_ethereum::{EthereumAdapter, EthereumAdapterConfig};
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use web3::error::{Error, ErrorKind};
use web3::helpers::*;
use web3::types::*;
use web3::{RequestId, Transport};

pub type Result<T> = Box<Future<Item = T, Error = Error> + Send + 'static>;

fn mock_block() -> Block<U256> {
    Block {
        hash: Some(H256::default()),
        parent_hash: H256::default(),
        uncles_hash: H256::default(),
        author: H160::default(),
        state_root: H256::default(),
        transactions_root: H256::default(),
        receipts_root: H256::default(),
        number: Some(U128::from(1)),
        gas_used: U256::from(100),
        gas_limit: U256::from(1000),
        extra_data: Bytes(String::from("0x00").into_bytes()),
        logs_bloom: H2048::default(),
        timestamp: U256::from(100000),
        difficulty: U256::from(10),
        total_difficulty: U256::from(100),
        seal_fields: vec![],
        uncles: Vec::<H256>::default(),
        transactions: Vec::<U256>::default(),
        size: Some(U256::from(10000)),
    }
}

#[derive(Debug, Default, Clone)]
pub struct TestTransport {
    asserted: usize,
    requests: Arc<Mutex<Vec<(String, Vec<jsonrpc_core::Value>)>>>,
    response: Arc<Mutex<VecDeque<jsonrpc_core::Value>>>,
}

impl Transport for TestTransport {
    type Out = Result<jsonrpc_core::Value>;

    fn prepare(
        &self,
        method: &str,
        params: Vec<jsonrpc_core::Value>,
    ) -> (RequestId, jsonrpc_core::Call) {
        let request = build_request(1, method, params.clone());
        self.requests.lock().unwrap().push((method.into(), params));
        (self.requests.lock().unwrap().len(), request)
    }

    fn send(&self, id: RequestId, request: jsonrpc_core::Call) -> Result<jsonrpc_core::Value> {
        match self.response.lock().unwrap().pop_front() {
            Some(response) => Box::new(finished(response)),
            None => {
                println!("Unexpected request (id: {:?}): {:?}", id, request);
                Box::new(failed(ErrorKind::Unreachable.into()))
            }
        }
    }
}

impl TestTransport {
    pub fn set_response(&mut self, value: jsonrpc_core::Value) {
        *self.response.lock().unwrap() = vec![value].into();
    }

    pub fn add_response(&mut self, value: jsonrpc_core::Value) {
        self.response.lock().unwrap().push_back(value);
    }

    pub fn assert_request(&mut self, method: &str, params: &[String]) {
        let idx = self.asserted;
        self.asserted += 1;

        let (m, p) = self
            .requests
            .lock()
            .unwrap()
            .get(idx)
            .expect("Expected result.")
            .clone();
        assert_eq!(&m, method);
        let p: Vec<String> = p
            .into_iter()
            .map(|p| serde_json::to_string(&p).unwrap())
            .collect();
        assert_eq!(p, params);
    }

    pub fn assert_no_more_requests(&mut self) {
        let requests = self.requests.lock().unwrap();
        assert_eq!(
            self.asserted,
            requests.len(),
            "Expected no more requests, got: {:?}",
            &requests[self.asserted..]
        );
    }
}

#[test]
fn contract_call() {
    let mut transport = TestTransport::default();

    transport.add_response(serde_json::to_value(mock_block()).unwrap());
    transport.add_response(jsonrpc_core::Value::String(format!(
        "{:?}",
        H256::from(100000)
    )));

    let logger = slog::Logger::root(slog::Discard, o!());
    let mut adapter = EthereumAdapter::new(EthereumAdapterConfig { transport, logger });
    let balance_of = Function {
        name: "balanceOf".to_owned(),
        inputs: vec![Param {
            name: "_owner".to_owned(),
            kind: ParamType::Address,
        }],
        outputs: vec![Param {
            name: "balance".to_owned(),
            kind: ParamType::Uint(256),
        }],
        constant: true,
    };
    let function = Function::from(balance_of);
    let gnt_addr = Address::from_str("eF7FfF64389B814A946f3E92105513705CA6B990").unwrap();
    let holder_addr = Address::from_str("00d04c4b12C4686305bb4F4fC93487CdFBa62580").unwrap();
    let call = EthereumContractCall {
        address: gnt_addr,
        block_id: BlockId::Number(BlockNumber::Latest),
        function: function,
        args: vec![Token::Address(holder_addr)],
    };
    let call_result = adapter.contract_call(call).wait().unwrap();

    assert_eq!(call_result[0], Token::Uint(U256::from(100000)));
}
