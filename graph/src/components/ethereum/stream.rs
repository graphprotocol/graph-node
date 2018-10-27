use failure::Error;
use futures::Stream;

use prelude::*;

pub trait BlockStream:
    Stream<Item = EthereumBlock, Error = Error> + EventConsumer<ChainHeadUpdate>
{
}

pub trait BlockStreamBuilder: Clone + Send + Sync {
    type Stream: BlockStream + Send + 'static;

    fn from_subgraph(&self, manifest: &SubgraphManifest, logger: Logger) -> Self::Stream;
}
