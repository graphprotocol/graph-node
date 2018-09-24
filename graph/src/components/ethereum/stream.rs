use failure::Error;
use futures::Stream;

use prelude::*;
use web3::types::{Block, Log, Transaction, H256};

#[derive(Clone, Debug)]
pub struct EthereumBlock {
    pub block: Block<Transaction>,
    pub logs: Vec<Log>,
}

impl EthereumBlock {
    pub fn transaction_for_log(&self, log: &Log) -> Option<Transaction> {
        log.transaction_hash
            .and_then(|hash| self.block.transactions.iter().find(|tx| tx.hash == hash))
            .cloned()
    }
}

pub trait BlockStream: Stream<Item = EthereumBlock, Error = Error> {}

pub trait BlockStreamController: Send + Sync {
    fn advance(&self, block_hash: H256) -> Box<Future<Item = (), Error = ()> + Send>;
}

pub trait BlockStreamBuilder: Clone + Send + Sync {
    type Stream: BlockStream + Send + 'static;
    type StreamController: BlockStreamController + 'static;

    fn from_subgraph(&self, manifest: &SubgraphManifest) -> (Self::Stream, Self::StreamController);
}
