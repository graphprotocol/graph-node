extern crate ethabi;
extern crate futures;
extern crate graph;
extern crate hex;
extern crate pwasm_utils;
extern crate tiny_keccak;
extern crate wasmi;

mod asc_abi;
mod host;
mod module;
mod to_from;

/// Runtime-agnostic implementation of exports to WASM.
mod host_exports;

use graph::prelude::*;
use graph::web3::types::{Address, Transaction};

pub use self::host::{RuntimeHost, RuntimeHostBuilder, RuntimeHostConfig};

#[derive(Clone, Debug)]
pub(crate) struct UnresolvedContractCall {
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_args: Vec<ethabi::Token>,
}

#[derive(Debug)]
pub(crate) struct EventHandlerContext {
    logger: Logger,
    block: Arc<EthereumBlock>,
    transaction: Arc<Transaction>,
    entity_operations: Vec<EntityOperation>,
}
