use failure::Error;
use futures::Stream;

use crate::prelude::*;

pub trait BlockStream: Stream<Item = EthereumBlockWithTriggers, Error = Error> {
    fn parse_triggers(
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        include_calls_in_blocks: bool,
        descendant_block: EthereumBlockWithCalls,
    ) -> Result<EthereumBlockWithTriggers, Error>;
}

pub trait BlockStreamBuilder: Clone + Send + Sync {
    type Stream: BlockStream + Send + 'static;

    fn build(
        &self,
        logger: Logger,
        deployment_id: SubgraphDeploymentId,
        network_name: String,
        log_filter: EthereumLogFilter,
        call_filter: EthereumCallFilter,
        block_filter: EthereumBlockFilter,
        include_calls_in_blocks: bool,
    ) -> Self::Stream;
}
