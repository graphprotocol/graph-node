extern crate ethabi;
extern crate futures;
extern crate graph;
extern crate hex;
extern crate nan_preserving_float;
extern crate uuid;
extern crate wasmi;

mod asc_abi;
mod host;
mod module;
mod to_from;

use self::graph::web3::types::{Address, H256};

pub use self::host::{RuntimeHost, RuntimeHostBuilder, RuntimeHostConfig};

#[derive(Clone, Debug)]
pub(crate) struct UnresolvedContractCall {
    pub block_hash: H256,
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_args: Vec<ethabi::Token>,
}
