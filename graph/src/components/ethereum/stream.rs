use failure::Error;
use futures::Stream;

use crate::prelude::*;

pub trait BlockStream:
    Stream<Item = EthereumBlockWithTriggers, Error = Error> + EventConsumer<ChainHeadUpdate>
{
}

pub trait BlockStreamBuilder: Clone + Send + Sync {
    type Stream: BlockStream + Send + 'static;

    fn build(
        &self,
        logger: Logger,
        deployment_id: SubgraphDeploymentId,
        log_filter: EthereumLogFilter,
    ) -> Self::Stream;
}
