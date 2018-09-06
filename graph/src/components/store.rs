use ethereum_types::H256;
use failure::Error;
use futures::Future;
use futures::Stream;
use web3::types::Block;
use web3::types::Transaction;

use data::store::*;
use std::fmt;

/// Key by which an individual entity in the store can be accessed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoreKey {
    /// ID of the subgraph.
    pub subgraph: String, // TODO use SubgraphId

    /// Name of the entity type.
    pub entity: String,

    /// ID of the individual entity.
    pub id: String,
}

/// Supported types of store filters.
#[derive(Clone, Debug, PartialEq)]
pub enum StoreFilter {
    And(Vec<StoreFilter>),
    Or(Vec<StoreFilter>),
    Equal(Attribute, Value),
    Not(Attribute, Value),
    GreaterThan(Attribute, Value),
    LessThan(Attribute, Value),
    GreaterOrEqual(Attribute, Value),
    LessOrEqual(Attribute, Value),
    In(Attribute, Vec<Value>),
    NotIn(Attribute, Vec<Value>),
    Contains(Attribute, Value),
    NotContains(Attribute, Value),
    StartsWith(Attribute, Value),
    NotStartsWith(Attribute, Value),
    EndsWith(Attribute, Value),
    NotEndsWith(Attribute, Value),
}

/// The order in which entities should be restored from a store.
#[derive(Clone, Debug, PartialEq)]
pub enum StoreOrder {
    Ascending,
    Descending,
}

/// How many entities to return, how many to skip etc.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreRange {
    /// How many entities to return.
    pub first: usize,

    /// How many entities to skip.
    pub skip: usize,
}

/// A query for entities in a store.
#[derive(Clone, Debug, PartialEq)]
pub struct StoreQuery {
    // ID of the subgraph.
    pub subgraph: String, // TODO use SubgraphId

    /// The name of the entity type.
    pub entity: String,

    /// Filter to filter entities by.
    pub filter: Option<StoreFilter>,

    /// An optional attribute to order the entities by.
    pub order_by: Option<String>,

    /// The direction to order entities in.
    pub order_direction: Option<StoreOrder>,

    /// An optional range to limit the size of the result.
    pub range: Option<StoreRange>,
}

/// Operation types that lead to entity changes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EntityChangeOperation {
    /// A new entity was added.
    Added,
    /// An existing entity was updated.
    Updated,
    /// An existing entity was removed.
    Removed,
}

/// Entity change events emitted by [Store](trait.Store.html) implementations.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EntityChange {
    /// ID of the subgraph the changed entity belongs to.
    pub subgraph: String,
    /// Entity type name of the changed entity.
    pub entity: String,
    /// ID of the changed entity.
    pub id: String,
    /// Operation that caused the change.
    pub operation: EntityChangeOperation,
}

/// A stream of entity change events.
pub type EntityChangeStream = Box<Stream<Item = EntityChange, Error = ()> + Send>;

/// The source of the events being sent to the store
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventSource {
    EthereumBlock(H256),
}

// Implementing the display trait also provides a ToString trait implementation
impl fmt::Display for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let printable_source = match *self {
            // Use LowerHex to format hash as hex string
            EventSource::EthereumBlock(hash) => format!("{:x}", hash),
        };
        write!(f, "{}", printable_source)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubgraphId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EthereumBlockPointer {
    pub hash: H256,
    pub number: u64,
}

impl EthereumBlockPointer {
    /// Creates a pointer to the parent of the specified block.
    pub fn to_parent<T>(b: &Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.parent_hash,
            number: b.number.unwrap().as_u64() - 1,
        }
    }
}

impl<T> From<Block<T>> for EthereumBlockPointer {
    fn from(b: Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.hash.unwrap(),
            number: b.number.unwrap().as_u64(),
        }
    }
}

impl From<(H256, u64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, u64)) -> EthereumBlockPointer {
        if number >= (1 << 63) {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer { hash, number }
    }
}

impl From<(H256, i64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, i64)) -> EthereumBlockPointer {
        if number < 0 {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer {
            hash,
            number: number as u64,
        }
    }
}

pub struct HeadBlockUpdateEvent {
    block_ptr: EthereumBlockPointer,
}

#[derive(Fail, Debug)]
pub enum StoreError {
    /// Indicates that an operation failed because its data is stale.
    /// API users should request the latest information and retry automatically if they receive
    /// this type of error.
    /// Usually, this is because a block pointer in the underlying store does not match what the
    /// operation requested.
    #[fail(display = "operation could not complete due to concurrent access to the same resource")]
    VersionConflict,

    #[fail(
        display = "operation could not complete due to an error from the underlying data storage"
    )]
    Database(Error),
}

#[derive(Debug)]
pub enum StoreOp {
    Set(StoreKey, Entity),
    Delete(StoreKey),
}

pub struct StoreTransaction<'a, S: BasicStore + ?Sized + 'a> {
    store: &'a S,
    subgraph: SubgraphId,
    block: Block<Transaction>,
    ops: Vec<StoreOp>,
}

impl<'a, S: BasicStore + ?Sized> StoreTransaction<'a, S> {
    fn new(store: &'a S, subgraph: SubgraphId, block: Block<Transaction>) -> Self {
        StoreTransaction {
            store,
            subgraph,
            block,
            ops: vec![],
        }
    }

    /// Updates an entity using the given store key and entity data.
    pub fn set(&mut self, key: StoreKey, entity: Entity) -> Result<(), Error> {
        self.ops.push(StoreOp::Set(key, entity));
        Ok(())
    }

    /// Deletes an entity using the given store key.
    pub fn delete(&mut self, key: StoreKey) -> Result<(), Error> {
        self.ops.push(StoreOp::Delete(key));
        Ok(())
    }

    /// Writes the queued operations of this transaction atomically to the underlying store.
    ///
    /// `StoreError::VersionConflict` is returned if the block provided to `begin_transaction` is not a
    /// direct child of the current block.
    pub fn commit(self) -> Result<(), StoreError> {
        self.store
            .commit_transaction(self.subgraph, self.ops, self.block)
    }
}

/// Common trait for store implementations that don't require interaction with the system.
pub trait BasicStore {
    // TODO make this generic over a type Blockchain, with associated types BlockPointer,
    // BlockHash, BlockStore (?)
    // Note: making this work properly for multiple blockchains at once is not easy to get right!
    // For that, we will need to rethink how mappings work, etc, in order to enforce determinism.

    /// Register a new subgraph ID in the store.
    /// Each subgraph has its own entities and separate block processing state.
    fn add_subgraph(&self, subgraph_id: SubgraphId) -> Result<(), Error>;

    /// Get the latest processed block.
    fn block_ptr(&self, subgraph_id: SubgraphId) -> Result<EthereumBlockPointer, Error>;

    /// Begin a store transaction to atomically perform a set of operations tied to a single block.
    /// Use this to add a block's worth of changes to the store.
    // TODO: just need hash, number and parent_hash from Block
    fn begin_transaction(
        &self,
        subgraph_id: SubgraphId,
        block: Block<Transaction>,
    ) -> Result<StoreTransaction<Self>, Error> {
        Ok(StoreTransaction::new(self, subgraph_id, block))
    }

    /// Updates the block pointer.  Careful: this is only safe to use if it is known that no store
    /// changes are needed to go from `from` to `to`.
    ///
    /// `StoreError::VersionConflict` is returned if `from` does not match the current block pointer.
    fn set_block_ptr_with_no_changes(
        &self,
        subgraph_id: SubgraphId,
        from: EthereumBlockPointer,
        to: EthereumBlockPointer,
    ) -> Result<(), StoreError>;

    /// Rollback the store changes made in a single block and update the subgraph pointer.
    ///
    /// `StoreError::VersionConflict` is returned if `block` does not match the current block pointer.
    // TODO: just need hash, number and parent_hash from Block
    fn revert_block(&self, subgraph_id: SubgraphId, block: Block<Transaction>)
        -> Result<(), Error>;

    /// Looks up an entity using the given store key.
    ///
    /// `StoreError::VersionConflict` is returned if `block_ptr` does not match the current block pointer.
    fn get(&self, key: StoreKey, block_ptr: EthereumBlockPointer) -> Result<Entity, StoreError>;

    /// Queries the store for entities that match the store query.
    ///
    /// `StoreError::VersionConflict` is returned if `block_ptr` does not match the current block pointer.
    fn find(
        &self,
        query: StoreQuery,
        block_ptr: EthereumBlockPointer,
    ) -> Result<Vec<Entity>, StoreError>;

    /// Do not call directly. See BasicStore::begin_transaction and StoreTransaction::commit instead.
    ///
    /// Must return `StoreError::VersionConflict` if the block provided is not a direct child of the
    /// current block.
    fn commit_transaction(
        &self,
        subgraph_id: SubgraphId,
        tx_ops: Vec<StoreOp>,
        block: Block<Transaction>,
    ) -> Result<(), StoreError>;
}

/// A pair of subgraph ID and entity type name.
pub type SubgraphEntityPair = (String, String);

/// Common trait for block data store implementations.
pub trait BlockStore {
    // TODO make this generic over a type Blockchain, with associated types BlockPointer, BlockHash

    /// Insert blocks into the store (or update if they are already present).
    fn upsert_blocks<'a, B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a>(
        &self,
        blocks: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a>;

    /// Try to update the head block pointer to the block with the highest block number.
    /// Only updates pointer if there is a block with a higher block number than the current head
    /// block, and the `ancestor_count` most recent ancestors of that block are in the store.
    ///
    /// If the pointer was updated, returns `Ok(vec![])`, and fires a HeadUpdateEvent.
    ///
    /// If no block has a number higher than the current head block, returns `Ok(vec![])`.
    ///
    /// If the candidate new head block had one or more missing ancestors, returns
    /// `Ok(missing_blocks)`, where `missing_blocks` is a nonexhaustive list of missing blocks.
    fn attempt_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error>;

    /// Get the current head block pointer for the specified network.
    /// Any changes to the head block pointer will be to a block with a larger block number, never
    /// to a block with a smaller or equal block number.
    ///
    /// The head block pointer will be None on initial set up.
    fn head_block_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error>;

    /// Get a stream of head block change events.
    fn head_block_updates(&self) -> Box<Stream<Item = HeadBlockUpdateEvent, Error = Error> + Send>;

    /// Get Some(block) if it is present in the block store, or None.
    fn block(&self, block_hash: H256) -> Result<Option<Block<Transaction>>, Error>;

    /// Get the `offset`th ancestor of `block_hash`, where offset=0 means the block matching
    /// `block_hash` and offset=1 means its parent. Returns None if unable to complete due to
    /// missing blocks in the block store.
    ///
    /// Returns an error if the offset would reach past the genesis block.
    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        offset: u64,
    ) -> Result<Option<Block<Transaction>>, Error>;
}

/// Common trait for store implementations.
pub trait Store: BasicStore + BlockStore + Send {
    /// Subscribe to entity changes for specific subgraphs and entities.
    ///
    /// Returns a stream of entity changes that match the input arguments.
    fn subscribe(&mut self, entities: Vec<SubgraphEntityPair>) -> EntityChangeStream;
}
