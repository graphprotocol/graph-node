use failure::Error;
use futures::Stream;

use prelude::*;
use web3::types::{Block, Log, Transaction, TransactionReceipt, H256};

pub trait BlockStream:
    Stream<Item = EthereumBlock, Error = Error> + EventConsumer<ChainHeadUpdate>
{
}

pub trait BlockStreamBuilder: Clone + Send + Sync {
    type Stream: BlockStream + Send + 'static;

    fn from_subgraph(
        &self,
        name: String,
        manifest: &SubgraphManifest,
        logger: Logger,
    ) -> Self::Stream;
}
