use anyhow::Error;
use futures::Stream;

use graph::{components::store::DeploymentLocator, prelude::*};

use graph::components::ethereum::TriggerFilter;

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
        filter: TriggerFilter,
        include_calls_in_blocks: bool,
        ethrpc_metrics: Arc<BlockStreamMetrics>,
    ) -> Self::Stream;
}
