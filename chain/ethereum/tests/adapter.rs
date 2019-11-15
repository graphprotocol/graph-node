use futures::prelude::*;
use futures::{failed, finished};
use hex_literal::hex;
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use ethabi::{Function, Param, ParamType, Token};
use graph::components::ethereum::EthereumContractCall;
use graph::prelude::EthereumAdapter as EthereumAdapterTrait;
use graph::prelude::*;
use graph_chain_ethereum::EthereumAdapter;
use mock::MockMetricsRegistry;
use web3::helpers::*;
use web3::types::*;
use web3::{BatchTransport, RequestId, Transport};

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
        mix_hash: Some(H256::default()),
        nonce: None,
    }
}

#[derive(Debug, Default, Clone)]
pub struct TestTransport {
    asserted: usize,
    requests: Arc<Mutex<Vec<(String, Vec<jsonrpc_core::Value>)>>>,
    response: Arc<Mutex<VecDeque<jsonrpc_core::Value>>>,
}

impl Transport for TestTransport {
    type Out = Box<dyn Future<Item = jsonrpc_core::Value, Error = web3::Error> + Send + 'static>;

    fn prepare(
        &self,
        method: &str,
        params: Vec<jsonrpc_core::Value>,
    ) -> (RequestId, jsonrpc_core::Call) {
        let request = build_request(1, method, params.clone());
        self.requests.lock().unwrap().push((method.into(), params));
        (self.requests.lock().unwrap().len(), request)
    }

    fn send(&self, _: RequestId, _: jsonrpc_core::Call) -> Self::Out {
        match self.response.lock().unwrap().pop_front() {
            Some(response) => Box::new(finished(response)),
            None => Box::new(failed(web3::Error::Unreachable.into())),
        }
    }
}

impl BatchTransport for TestTransport {
    type Batch = Box<
        dyn Future<Item = Vec<Result<jsonrpc_core::Value, web3::Error>>, Error = web3::Error>
            + Send
            + 'static,
    >;

    fn send_batch<T>(&self, requests: T) -> Self::Batch
    where
        T: IntoIterator<Item = (RequestId, jsonrpc_core::Call)>,
    {
        Box::new(
            stream::futures_ordered(
                requests
                    .into_iter()
                    .map(|(id, req)| self.send(id, req).map(|v| Ok(v))),
            )
            .collect(),
        )
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

struct FakeEthereumCallCache;

impl EthereumCallCache for FakeEthereumCallCache {
    fn get_call(
        &self,
        _: ethabi::Address,
        _: &[u8],
        _: EthereumBlockPointer,
    ) -> Result<Option<Vec<u8>>, Error> {
        unimplemented!()
    }

    fn set_call(
        &self,
        _: ethabi::Address,
        _: &[u8],
        _: EthereumBlockPointer,
        _: &[u8],
    ) -> Result<(), Error> {
        unimplemented!()
    }
}

#[test]
#[ignore]
fn contract_call() {
    let registry = Arc::new(MockMetricsRegistry::new());
    let mut transport = TestTransport::default();

    transport.add_response(serde_json::to_value(mock_block()).unwrap());
    transport.add_response(jsonrpc_core::Value::String(format!(
        "{:?}",
        H256::from(hex!(
            "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f"
        )),
    )));

    let logger = Logger::root(slog::Discard, o!());

    let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(registry.clone()));

    let adapter = EthereumAdapter::new(transport, provider_metrics);
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
        block_ptr: EthereumBlockPointer::from((H256::zero(), 0 as i64)),
        function: function,
        args: vec![Token::Address(holder_addr)],
    };
    let call_result = adapter
        .contract_call(&logger, call, Arc::new(FakeEthereumCallCache))
        .wait()
        .unwrap();

    assert_eq!(call_result[0], Token::Uint(U256::from(100000)));
}
