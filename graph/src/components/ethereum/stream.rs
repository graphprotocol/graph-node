use failure::Error;
use futures::Stream;

use prelude::*;
use web3::types::{Block, Log, Transaction};

#[derive(Clone, Debug)]
pub struct EthereumBlock {
    pub block: Block<Transaction>,
    pub logs: Vec<Log>,
}

pub trait BlockStream: Stream<Item = EthereumBlock, Error = Error> {}

pub trait BlockStreamBuilder: Clone + Send + Sync {
    type Stream: BlockStream + Send + 'static;

    fn from_subgraph(&self, manifest: &SubgraphManifest) -> Self::Stream;
}
