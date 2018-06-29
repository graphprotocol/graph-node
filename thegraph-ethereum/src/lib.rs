extern crate ethabi;
extern crate ethereum_types;
extern crate futures;
extern crate serde_json;
extern crate thegraph;
extern crate tokio_core;
extern crate web3;

#[cfg(test)]
extern crate jsonrpc_core;

#[cfg(test)]
#[macro_use]
pub mod test_helpers;

mod ethereum_adapter;

pub use self::ethereum_adapter::{EthereumAdapter, EthereumAdapterConfig};

/// Re-exported web3 transports.
pub use web3::transports;

#[cfg(test)]
mod tests {
    use ethabi::{Function, Param, ParamType, Token};
    use ethereum_adapter::{EthereumAdapter, EthereumAdapterConfig};
    use jsonrpc_core;
    use std::str::FromStr;
    use test_helpers::transport::TestTransport;
    use thegraph::components::ethereum::EthereumContractCallRequest;
    use thegraph::prelude::EthereumAdapter as EthereumAdapterTrait;
    use tokio_core::reactor::Core;
    use web3::types::*;

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
}
