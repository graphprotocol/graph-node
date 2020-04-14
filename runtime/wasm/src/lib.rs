mod asc_abi;
mod host;
mod to_from;

/// Pre-processes modules and manages their threads. Serves as an interface from `host` to `module`.
mod mapping;

/// Deals with wasmi.
mod module;

/// Runtime-agnostic implementation of exports to WASM.
mod host_exports;

use graph::prelude::web3::types::Address;
use graph::prelude::{Store, SubgraphDeploymentStore};

#[derive(Clone, Debug)]
pub(crate) struct UnresolvedContractCall {
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_signature: Option<String>,
    pub function_args: Vec<ethabi::Token>,
}

// Public interface of the crate, receives triggers to be processed.

pub trait RuntimeStore: Store + SubgraphDeploymentStore {}
impl<S: Store + SubgraphDeploymentStore> RuntimeStore for S {}

pub use host::{
    HostFunction, HostModule, HostModuleError, HostModules, RuntimeHost, RuntimeHostBuilder,
};
pub use mapping::MappingRequest;
