use anyhow::Error;
use futures::Stream;

use crate::{components::store::DeploymentLocator, prelude::*};

pub enum BlockStreamEvent {
    Block(EthereumBlockWithTriggers),
    Revert(BlockPtr),
}

pub trait BlockStream: Stream<Item = BlockStreamEvent, Error = Error> {}

pub trait BlockStreamBuilder: Clone + Send + Sync + 'static {
    type Stream: BlockStream + Send + 'static;

    fn build(
        &self,
        logger: Logger,
        deployment: DeploymentLocator,
        network_name: String,
        start_blocks: Vec<BlockNumber>,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        include_calls_in_blocks: bool,
        ethrpc_metrics: Arc<BlockStreamMetrics>,
    ) -> Self::Stream;
}
