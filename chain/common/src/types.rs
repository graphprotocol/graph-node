use std::fmt;
use std::sync::Arc;

use graph::data::store::scalar::Bytes;
use graph::prelude::{
    EntityCache, EntityKey, Error, EventProducer, Future, Store, SubgraphDeploymentId, ToEntityKey,
};

/// A compact representation of a block on a chain.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockPointer {
    pub number: u64,
    pub hash: Bytes,
}

impl BlockPointer {}

impl From<(u64, Bytes)> for BlockPointer {
    fn from(input: (u64, Bytes)) -> Self {
        Self {
            number: input.0,
            hash: input.1,
        }
    }
}

impl fmt::Display for BlockPointer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{} ({})", self.number, self.hash.hex())
    }
}

impl ToEntityKey for BlockPointer {
    fn to_entity_key(&self, subgraph: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id: subgraph,
            entity_type: "Block".into(),
            entity_id: self.hash.hex(),
        }
    }
}

pub struct ToEntitiesContext {
    subgraph_id: SubgraphDeploymentId,
    store: Arc<dyn Store>,
    cache: EntityCache,
}

pub trait ToEntities {
    fn to_entities(
        &self,
        context: ToEntitiesContext,
    ) -> Box<dyn Future<Item = ToEntitiesContext, Error = Error>>;
}

/// Represents a block in a particular chain.
///
/// Each chain has its own implementation of the `Block` trait.
pub trait Block: Sync + Send + ToEntities + Into<BlockPointer> {
    /// Returns the number of the block.
    fn number(&self) -> u64;

    /// Returns the block hash.
    fn hash(&self) -> Bytes;

    /// Returns a block pointer for the block.
    fn pointer(&self) -> BlockPointer;
}

/// A future for a network interaction.
pub type NetworkFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;

/// Events emitted by a network indexer.
pub enum NetworkIndexerEvent<N: Network> {
    ChainHead(BlockPointer),
    Revert {
        from: BlockPointer,
        to: BlockPointer,
    },
    AddBlock(N::Block),
}

impl<N: Network> fmt::Display for NetworkIndexerEvent<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NetworkIndexerEvent::*;

        match self {
            ChainHead(block) => write!(f, "Move chain to {}", &block),
            NetworkIndexerEvent::Revert { from, to } => {
                write!(f, "Revert from {} to {}", &from, &to)
            }
            NetworkIndexerEvent::AddBlock(block) => write!(f, "Add block {}", block.pointer()),
        }
    }
}
/// Common trait for all network indexers.
pub trait NetworkIndexer<N: Network>: EventProducer<NetworkIndexerEvent<N>> {}

/// Represents a network (essentially an instance of a `Chain`).
pub trait Network: Sync + Send + Sized + fmt::Debug + 'static {
    type Block: Block;
    type NetworkIndexer: NetworkIndexer<Self>;

    /// Returns the latest block on the network.
    fn latest_block(&self) -> NetworkFuture<Self::Block>;

    /// Returns a block by number, if present on chain.
    fn block_by_number(&self, number: u64) -> NetworkFuture<Option<Self::Block>>;

    /// Returns a block by block hash.
    fn block_by_hash(&self, hash: Bytes) -> NetworkFuture<Self::Block>;
    fn parent_block(&self, child: BlockPointer) -> NetworkFuture<Self::Block>;

    /// Indexes network data hand handles reorgs.
    fn indexer(&self) -> Result<Self::NetworkIndexer, Error>;
}
