use failure::Error;
use futures::Stream;

use crate::prelude::*;

pub trait BlockStream:
    Stream<Item = EthereumBlockWithTriggers, Error = Error> + EventConsumer<ChainHeadUpdate>
{
    fn parse_triggers(
        log_filter_opt: Option<EthereumLogFilter>,
        call_filter_opt: Option<EthereumCallFilter>,
        block_filter_opt: Option<EthereumBlockFilter>,
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
        log_filter: Option<EthereumLogFilter>,
        call_filter: Option<EthereumCallFilter>,
        block_filter: Option<EthereumBlockFilter>,
        include_calls_in_blocks: bool,
    ) -> Self::Stream;
}
