use futures::prelude::Future;
use std::collections::HashMap;

use crate::data::store::scalar::{BigInt, Bytes};
use crate::prelude::{Error, EventProducer, Logger};

pub struct BlockPointer {
    pub number: BigInt,
    pub hash: Bytes,
}

pub struct NetworkProviderConfig {
    pub kind: String,
    pub url: String,
}

pub struct NetworkConfig {
    pub providers: Vec<NetworkProviderConfig>,
}

pub struct BlockchainOptions {
    pub networks: HashMap<String, NetworkConfig>,
}

pub struct LatestBlockOptions {
    logger: Logger,
}

pub struct BlockByNumberOptions {
    logger: Logger,
}

pub struct BlockByHashOptions {
    logger: Logger,
}

pub type LatestBlockFuture = Box<dyn Future<Item = BlockPointer, Error = Error> + Send>;
pub type BlockByNumberFuture = Box<dyn Future<Item = dyn Block, Error = Error> + Send>;
pub type BlockByHashFuture = Box<dyn Future<Item = dyn Block, Error = Error> + Send>;
pub type IndexerFuture = Box<dyn Future<Item = dyn NetworkIndexer, Error = Error> + Send>;

pub trait Blockchain {
    type Network: Network;

    fn new(options: BlockchainOptions) -> Self;
    fn network(&self, name: String) -> Option<Self::Network>;
}

pub trait Network {
    type Block: Block;
    type Indexer: NetworkIndexer;

    fn latest_block(&self, options: LatestBlockOptions) -> LatestBlockFuture;
    fn block_by_number(&self, options: BlockByNumberOptions) -> BlockByNumberFuture;
    fn block_by_hash(&self, options: BlockByHashOptions) -> BlockByHashFuture;

    fn indexer(&self) -> IndexerFuture;
}

pub enum NetworkIndexerEvent {
    Revert {
        from: BlockPointer,
        to: BlockPointer,
    },
    AddBlock(Block), // associated type or generic?
}

pub trait NetworkIndexer: EventProducer<NetworkIndexerEvent> {}

pub trait Block {
    fn number(&self) -> BigInt;
    fn hash(&self) -> Bytes;
    fn pointer(&self) -> BlockPointer;
    fn parent_hash(&self) -> Option<Bytes>;
}
