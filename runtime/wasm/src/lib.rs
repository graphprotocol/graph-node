mod asc_abi;
mod to_from;

/// Public interface of the crate, receives triggers to be processed.
mod host;
pub use self::host::{RuntimeHost, RuntimeHostBuilder, RuntimeHostConfig};

/// Pre-processes modules and manages their threads. Serves as an interface from `host` to `module`.
mod mapping;

/// Deals with wasmi.
mod module;

/// Runtime-agnostic implementation of exports to WASM.
mod host_exports;

use graph::prelude::web3::types::Address;

#[derive(Clone, Debug)]
pub(crate) struct UnresolvedContractCall {
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_args: Vec<ethabi::Token>,
}
