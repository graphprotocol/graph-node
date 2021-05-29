mod asc_abi;
mod to_from;

/// Public interface of the crate, receives triggers to be processed.
mod host;
pub use host::RuntimeHostBuilder;

/// Pre-processes modules and manages their threads. Serves as an interface from `host` to `module`.
pub mod mapping;

/// WASM module instance.
pub mod module;

/// Runtime-agnostic implementation of exports to WASM.
pub mod host_exports;

mod error;

use graph::prelude::web3::types::Address;
use graph::prelude::SubgraphStore;

#[derive(Clone, Debug)]
pub struct UnresolvedContractCall {
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_signature: Option<String>,
    pub function_args: Vec<ethabi::Token>,
}

pub trait RuntimeStore: SubgraphStore {}
impl<S: SubgraphStore> RuntimeStore for S {}
