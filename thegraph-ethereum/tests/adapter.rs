extern crate ethabi;
extern crate ethereum_types;
extern crate futures;
extern crate serde_json;
extern crate thegraph;
extern crate tokio_core;
extern crate web3;
extern crate thegraph_ethereum;
extern crate jsonrpc_core;

use futures::prelude::*;
use futures::{failed, finished};
use std::cell::RefCell;
use std::collections::VecDeque;
use web3::error::{Error, ErrorKind};
use web3::helpers::*;
use web3::{RequestId, Transport};
use thegraph_ethereum::{EthereumAdapter, EthereumAdapterConfig};
use ethabi::{Function, Param, ParamType, Token};
use std::str::FromStr;
use thegraph::components::ethereum::EthereumContractCallRequest;
use thegraph::prelude::EthereumAdapter as EthereumAdapterTrait;
use tokio_core::reactor::Core;
use web3::types::*;

pub type Result<T> = Box<Future<Item = T, Error = Error> + Send + 'static>;

#[derive(Debug, Default, Clone)]
pub struct TestTransport {
    asserted: usize,
    requests: RefCell<Vec<(String, Vec<jsonrpc_core::Value>)>>,
    response: RefCell<VecDeque<jsonrpc_core::Value>>,
}

impl Transport for TestTransport {
    type Out = Result<jsonrpc_core::Value>;

    fn prepare(
        &self,
        method: &str,
        params: Vec<jsonrpc_core::Value>,
    ) -> (RequestId, jsonrpc_core::Call) {
        let request = build_request(1, method, params.clone());
        self.requests.borrow_mut().push((method.into(), params));
        (self.requests.borrow().len(), request)
    }

    fn send(&self, id: RequestId, request: jsonrpc_core::Call) -> Result<jsonrpc_core::Value> {
        match self.response.borrow_mut().pop_front() {
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
        *self.response.borrow_mut() = vec![value].into();
    }

    pub fn add_response(&mut self, value: jsonrpc_core::Value) {
        self.response.borrow_mut().push_back(value);
    }

    pub fn assert_request(&mut self, method: &str, params: &[String]) {
        let idx = self.asserted;
        self.asserted += 1;

        let (m, p) = self.requests
            .borrow()
            .get(idx)
            .expect("Expected result.")
            .clone();
        assert_eq!(&m, method);
        let p: Vec<String> = p.into_iter()
            .map(|p| serde_json::to_string(&p).unwrap())
            .collect();
        assert_eq!(p, params);
    }

    pub fn assert_no_more_requests(&mut self) {
        let requests = self.requests.borrow();
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
    let mut core = Core::new().unwrap();
    let mut transport = TestTransport::default();
    transport.set_response(jsonrpc_core::Value::String(format!(
        "{:?}",
        H256::from(100000)
    )));

    let mut adapter = EthereumAdapter::new(
        core.handle(),
        EthereumAdapterConfig {
            transport: transport,
        },
    );
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
    let call_request = EthereumContractCallRequest {
        address: gnt_addr,
        block_number: None,
        function: function,
        args: vec![Token::Address(holder_addr)],
    };
    let work = adapter.contract_call(call_request);
    let call_result = core.run(work).unwrap();

    assert_eq!(call_result[0], Token::Uint(U256::from(100000)));

    println!(
        "Result from calling GNT.balanceOf(0x00d04c4b12C4686305bb4F4fC93487CdFBa62580): {:?}",
        call_result[0]
    );
}
