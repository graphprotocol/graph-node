use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
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
    fn matches_block(&self, call: EthereumBlockTriggerType, block_number: u64) -> bool;

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

pub trait RuntimeHostBuilder: Clone + Send + Sync + 'static {
    type Host: RuntimeHost;
    type Req: 'static + Send;

    /// Build a new runtime host for a subgraph data source.
    fn build(
        &self,
        network_name: String,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
        top_level_templates: Vec<DataSourceTemplate>,
        mapping_request_sender: mpsc::Sender<Self::Req>,
    ) -> Result<Self::Host, Error>;

    /// Spawn a mapping and return a channel for mapping requests. The sender should be able to be
    /// cached and shared among mappings that have the same `parsed_module`.
    fn spawn_mapping(
        parsed_module: parity_wasm::elements::Module,
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
    ) -> Result<mpsc::Sender<Self::Req>, Error>;
}
