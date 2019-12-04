use futures::prelude::Future;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::data::store::scalar::{BigInt, Bytes};
use crate::prelude::{ChainStore, Error, EventProducer, Logger, MetricsRegistry, Store};

pub trait NetworkStore: Store + ChainStore {}

impl<S: Store + ChainStore> NetworkStore for S {}

#[derive(Debug)]
pub struct BlockPointer {
    pub number: BigInt,
    pub hash: Bytes,
}

impl fmt::Display for BlockPointer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "#{} ({})",
            self.number,
            format!("{}", self.hash).trim_start_matches("0x")
        )
    }
}

pub struct NetworkProviderOptions {
    pub kind: String,
    pub url: String,
}

pub struct NetworkOptions {
    pub name: String,
    pub logger: Logger,
    pub providers: Vec<NetworkProviderOptions>,
}

pub struct BlockchainOptions {
    pub metrics_registry: Arc<dyn MetricsRegistry>,
    pub networks: HashMap<String, NetworkOptions>,
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

pub struct NetworkIndexerOptions {
    pub start_block: Option<BlockPointer>,
    pub store: Arc<dyn NetworkStore>,
}

pub type LatestBlockFuture = Box<dyn Future<Item = BlockPointer, Error = Error> + Send>;
pub type BlockByNumberFuture = Box<dyn Future<Item = dyn Block, Error = Error> + Send>;
pub type BlockByHashFuture = Box<dyn Future<Item = dyn Block, Error = Error> + Send>;

pub trait Blockchain {
    type Network: Network;

    fn new(options: BlockchainOptions) -> Self;
    fn network(&self, name: String) -> Result<Self::Network, Error>;
}

pub trait Block: fmt::Debug {
    fn number(&self) -> BigInt;
    fn hash(&self) -> Bytes;
    fn pointer(&self) -> BlockPointer;
    fn parent_pointer(&self) -> Option<BlockPointer>;
}

pub trait Network {
    type Block: Block;
    type Indexer: NetworkIndexer;

    fn latest_block(&self, options: LatestBlockOptions) -> LatestBlockFuture;
    fn block_by_number(&self, options: BlockByNumberOptions) -> BlockByNumberFuture;
    fn block_by_hash(&self, options: BlockByHashOptions) -> BlockByHashFuture;

    fn indexer(
        &self,
        options: NetworkIndexerOptions,
    ) -> Box<dyn Future<Item = Self::Indexer, Error = Error> + Send>;
}

#[derive(Debug)]
pub enum NetworkIndexerEvent {
    Revert {
        from: BlockPointer,
        to: BlockPointer,
    },
    AddBlock(Box<dyn Block>),
}

pub trait NetworkIndexer: EventProducer<NetworkIndexerEvent> {}
