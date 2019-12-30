use std::fmt::Debug;
use std::sync::Arc;

use crate::data::store::scalar::{BigInt, Bytes};
use crate::prelude::{Error, EventProducer, Future};

/// A compact representation of a block on a chain.
pub struct BlockPointer {
    pub number: BigInt,
    pub hash: Bytes,
}

impl BlockPointer {
    /// Encodes the block hash into a hexadecimal string **without** a "0x" prefix.
    /// Hashes are stored in the database in this format.
    ///
    /// This mainly exists because of backwards incompatible changes in how the Web3 library
    /// implements `H256::to_string`.
    pub fn hash_hex(&self) -> String {
        self.hash.to_hex()
    }
}

impl From<(BigInt, Bytes)> for BlockPointer {
    fn from(input: (BigInt, Bytes)) -> Self {
        Self {
            number: input.0,
            hash: input.1,
        }
    }
}

/// Represents a block in a particular chain. Each chain has its own
/// implementation of the `Block` trait.
pub trait Block {
    fn number(&self) -> BigInt;
    fn hash(&self) -> Bytes;
    fn pointer(&self) -> BlockPointer;
}

/// Represents a chain supported by the node.
pub trait Chain: Debug {
    type Network: Network;
    type ChainOptions;

    fn new(options: Self::ChainOptions) -> Self;

    // Looks up a network by name. Fails if the node doesn't support the network.
    fn network(&self, name: String) -> Result<Arc<Self::Network>, Error>;
}

pub type BlockFuture<T: Block> = Box<dyn Future<Item = T, Error = Error>>;

/// Represents a network (essentially an instance of a `Chain`).
pub trait Network: Debug {
    type Block: Block;
    type NetworkIndexer: NetworkIndexer<Self::Block>;
    type NetworkTrigger: NetworkTrigger;
    type BlockStream: BlockStream<Self::Block, Self::NetworkTrigger>;

    // Methods common to all networks.
    // These are needed by the network indexer and block stream.
    fn latest_block(&self) -> BlockFuture<Self::Block>;
    fn block_by_number(&self, number: BigInt) -> BlockFuture<Self::Block>;
    fn block_by_hash(&self, hash: Bytes) -> BlockFuture<Self::Block>;
    fn parent_block(&self, child: BlockPointer) -> BlockFuture<Self::Block>;

    /// Indexes network data hand handles reorgs.
    fn indexer(&self) -> Result<Self::NetworkIndexer, Error>;

    /// Provides a block/trigger stream for given options (including filters).
    fn block_stream(&self) -> Result<Self::BlockStream, Error>;
}

/// Events emitted by a network indexer.
pub enum NetworkIndexerEvent<T: Block> {
    ChainHead(BlockPointer),
    Revert {
        from: BlockPointer,
        to: BlockPointer,
    },
    AddBlock(Box<T>),
}

/// Common trait for all network indexers.
pub trait NetworkIndexer<T: Block>: EventProducer<NetworkIndexerEvent<T>> {}

/// Triggers emitted by a block stream.
pub trait NetworkTrigger {
    // fn matches_data_source(&self, data_source: &DataSource) -> bool;
    // fn matches_handler(&self, handler: &DataSourceHandler) -> bool;
}

/// Combination of a block and subgraph-specific triggers for it.
pub struct BlockWithTriggers<T: Block, U: NetworkTrigger> {
    block: Box<T>,
    triggers: Vec<U>,
}

/// Events emitted by a block stream.
pub enum BlockStreamEvent<T: Block, U: NetworkTrigger> {
    ChainHead(BlockPointer),
    Revert(BlockPointer),
    Block(BlockWithTriggers<T, U>),
}

/// Common trait for all block streams.
pub trait BlockStream<T: Block, U: NetworkTrigger>: EventProducer<BlockStreamEvent<T, U>> {}
