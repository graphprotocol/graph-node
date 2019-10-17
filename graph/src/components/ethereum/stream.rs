use failure::Error;
use futures::Stream;

use crate::prelude::*;

pub trait BlockStream: Stream<Item = EthereumBlockWithTriggers, Error = Error> {
    fn triggers_in_block(
        &self,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        descendant_block: BlockFinality,
    ) -> Box<dyn Future<Item = EthereumBlockWithTriggers, Error = Error> + Send>;
}

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
        include_calls_in_blocks: bool,
    ) -> Self::Stream;
}
