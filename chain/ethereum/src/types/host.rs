use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use failure::Error;
use futures::sync::mpsc;

use crate::components::metrics::HistogramVec;
use crate::prelude::*;
use web3::types::{Log, Transaction};

/// Common trait for runtime host implementations.
#[async_trait]
pub trait RuntimeHost: Send + Sync + Debug + 'static {
    /// Returns true if the RuntimeHost has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum call.
    fn matches_call(&self, call: &EthereumCall) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum block.
    fn matches_block(&self, call: &EthereumBlockTriggerType, block_number: u64) -> bool;

    /// Process an Ethereum event and return a vector of entity operations.
    async fn process_log(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        transaction: &Arc<Transaction>,
        log: &Arc<Log>,
        state: BlockState,
    ) -> Result<BlockState, Error>;

    /// Process an Ethereum call and return a vector of entity operations
    async fn process_call(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        transaction: &Arc<Transaction>,
        call: &Arc<EthereumCall>,
        state: BlockState,
    ) -> Result<BlockState, Error>;

    /// Process an Ethereum block and return a vector of entity operations
    async fn process_block(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        trigger_type: &EthereumBlockTriggerType,
        state: BlockState,
    ) -> Result<BlockState, Error>;
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
        top_level_templates: Arc<Vec<DataSourceTemplate>>,
        mapping_request_sender: mpsc::Sender<Self::Req>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error>;

    /// Spawn a mapping and return a channel for mapping requests. The sender should be able to be
    /// cached and shared among mappings that have the same `parsed_module`.
    fn spawn_mapping(
        parsed_module: parity_wasm::elements::Module,
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
        metrics: Arc<HostMetrics>,
    ) -> Result<mpsc::Sender<Self::Req>, Error>;
}
