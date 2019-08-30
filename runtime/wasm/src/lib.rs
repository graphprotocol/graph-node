mod asc_abi;
mod host;
mod module;
mod to_from;

/// Runtime-agnostic implementation of exports to WASM.
mod host_exports;

use graph::prelude::*;
use web3::types::Address;

pub use self::host::{RuntimeHost, RuntimeHostBuilder, RuntimeHostConfig};

#[derive(Clone, Debug)]
pub(crate) struct UnresolvedContractCall {
    pub contract_name: String,
    pub contract_address: Address,
    pub function_name: String,
    pub function_args: Vec<ethabi::Token>,
}

#[derive(Debug)]
pub(crate) struct MappingContext {
    logger: Logger,
    block: Arc<EthereumBlock>,
    state: BlockState,
}

/// Cloning an `MappingContext` clones all its fields,
/// except the `state_operations`, since they are an output
/// accumulator and are therefore initialized with an empty state.
impl Clone for MappingContext {
    fn clone(&self) -> Self {
        MappingContext {
            logger: self.logger.clone(),
            block: self.block.clone(),
            state: BlockState::default(),
        }
    }
}
