use failure::Error;
use futures::Stream;

use crate::prelude::*;

pub trait BlockStream: Stream<Item = EthereumBlockWithTriggers, Error = Error> {}

pub trait BlockStreamBuilder: Clone + Send + Sync + 'static {
    type Stream: BlockStream + Send + 'static;

    fn build(
        &self,
        logger: Logger,
        deployment_id: SubgraphDeploymentId,
        network_name: String,
        start_blocks: Vec<u64>,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        templates_use_calls: bool,
        ethrpc_metrics: Arc<BlockStreamMetrics>,
    ) -> Self::Stream;
}
