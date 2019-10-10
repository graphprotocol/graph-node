use failure::Error;
use futures::prelude::*;
use std::sync::Arc;

use crate::prelude::*;
use web3::types::{Log, Transaction};

/// Common trait for runtime host implementations.
pub trait RuntimeHost: Send + Sync + Debug + 'static {
    /// Returns true if the RuntimeHost has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum call.
    fn matches_call(&self, call: &EthereumCall) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum block.
    fn matches_block(&self, call: EthereumBlockTriggerType) -> bool;

    /// Process an Ethereum event and return a vector of entity operations.
    fn process_log(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;

    /// Process an Ethereum call and return a vector of entity operations
    fn process_call(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;

    /// Process an Ethereum block and return a vector of entity operations
    fn process_block(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        trigger_type: EthereumBlockTriggerType,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;
}
