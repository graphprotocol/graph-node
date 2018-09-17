use failure::Error;
use futures::prelude::*;
use std::sync::{Arc, Mutex};

use graph::prelude::{
    BlockStream as BlockStreamTrait, BlockStreamBuilder as BlockStreamBuilderTrait, EthereumBlock,
    *,
};
use graph::web3::types::{Block, Log, Transaction};

pub struct BlockStream {}

impl BlockStream {}

impl BlockStreamTrait for BlockStream {}

impl Stream for BlockStream {
    type Item = EthereumBlock;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(Async::Ready(None))
    }
}

pub struct BlockStreamBuilder<S, E> {
    store: Arc<Mutex<S>>,
    ethereum: Arc<Mutex<E>>,
}

impl<S, E> Clone for BlockStreamBuilder<S, E> {
    fn clone(&self) -> Self {
        BlockStreamBuilder {
            store: self.store.clone(),
            ethereum: self.ethereum.clone(),
        }
    }
}

impl<S, E> BlockStreamBuilder<S, E>
where
    S: Store,
    E: EthereumAdapter,
{
    pub fn new(store: Arc<Mutex<S>>, ethereum: Arc<Mutex<E>>) -> Self {
        BlockStreamBuilder { store, ethereum }
    }
}

impl<S, E> BlockStreamBuilderTrait for BlockStreamBuilder<S, E>
where
    S: Store,
    E: EthereumAdapter,
{
    type Stream = BlockStream;

    fn from_subgraph(&self, manifest: &SubgraphManifest) -> Self::Stream {
        // 1. Create chain update listener for the network used by this subgraph
        // 2. Implement block stream algorithm whenever there is a chain update

        BlockStream {}
    }
}
