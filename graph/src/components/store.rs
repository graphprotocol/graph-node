use failure::Error;
use futures::Future;
use futures::Stream;
use std::fmt;
use web3::types::H256;

use data::store::*;
use prelude::*;

/// Key by which an individual entity in the store can be accessed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityKey {
    /// ID of the subgraph.
    pub subgraph_id: SubgraphDeploymentId,

    /// Name of the entity type.
    pub entity_type: String,

    /// ID of the individual entity.
    pub entity_id: String,
}

/// Supported types of store filters.
#[derive(Clone, Debug, PartialEq)]
pub enum EntityFilter {
    And(Vec<EntityFilter>),
    Or(Vec<EntityFilter>),
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
pub enum EntityOrder {
    Ascending,
    Descending,
}

/// How many entities to return, how many to skip etc.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityRange {
    /// How many entities to return.
    pub first: usize,

    /// How many entities to skip.
    pub skip: usize,
}

/// A query for entities in a store.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityQuery {
    /// ID of the subgraph.
    pub subgraph_id: SubgraphDeploymentId,

    /// The name of the entity type.
    pub entity_type: String,

    /// Filter to filter entities by.
    pub filter: Option<EntityFilter>,

    /// An optional attribute to order the entities by.
    pub order_by: Option<(String, ValueType)>,

    /// The direction to order entities in.
    pub order_direction: Option<EntityOrder>,

    /// An optional range to limit the size of the result.
    pub range: Option<EntityRange>,
}

impl EntityQuery {
    pub fn new(subgraph_id: SubgraphDeploymentId, entity_type: impl Into<String>) -> Self {
        EntityQuery {
            subgraph_id,
            entity_type: entity_type.into(),
            filter: None,
            order_by: None,
            order_direction: None,
            range: None,
        }
    }

    pub fn filter(mut self, filter: EntityFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn order_by(mut self, by: (String, ValueType), direction: EntityOrder) -> Self {
        self.order_by = Some(by);
        self.order_direction = Some(direction);
        self
    }

    pub fn range(mut self, range: EntityRange) -> Self {
        self.range = Some(range);
        self
    }
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
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct EntityChange {
    /// ID of the subgraph the changed entity belongs to.
    pub subgraph_id: SubgraphDeploymentId,
    /// Entity type name of the changed entity.
    pub entity_type: String,
    /// ID of the changed entity.
    pub entity_id: String,
    /// Operation that caused the change.
    pub operation: EntityChangeOperation,
}

impl EntityChange {
    pub fn from_key(key: EntityKey, operation: EntityChangeOperation) -> Self {
        Self {
            subgraph_id: key.subgraph_id,
            entity_type: key.entity_type,
            entity_id: key.entity_id,
            operation,
        }
    }

    pub fn subgraph_entity_pair(&self) -> SubgraphEntityPair {
        (self.subgraph_id.clone(), self.entity_type.clone())
    }
}

/// A stream of entity change events.
pub type EntityChangeStream = Box<Stream<Item = EntityChange, Error = ()> + Send>;

/// An entity operation that can be transacted into the store.
#[derive(Clone, Debug, PartialEq)]
pub enum EntityOperation {
    /// Locates the entity specified by `key` and sets its attributes according to the contents of
    /// `data`.  If no entity exists with this key, creates a new entity.
    Set { key: EntityKey, data: Entity },

    /// Removes an entity with the specified key, if one exists.
    Remove { key: EntityKey },

    /// Aborts and rolls back the transaction unless `query` returns entities exactly matching
    /// `entity_ids`.  The equality test is only sensitive to the order of the results if `query`
    /// contains an `order_by`.
    AbortUnless {
        description: String, // Programmer-friendly debug message to explain reason for abort
        query: EntityQuery,  // The query to run
        entity_ids: Vec<String>, // What entities the query should return
    },
}

impl EntityOperation {
    /// Returns true if the operation is of variant `Set`.
    pub fn is_set(&self) -> bool {
        use self::EntityOperation::*;

        match self {
            Set { .. } => true,
            _ => false,
        }
    }

    /// Returns true if the operation is an entity removal.
    pub fn is_remove(&self) -> bool {
        use self::EntityOperation::*;

        match self {
            Remove { .. } => true,
            _ => false,
        }
    }

    pub fn entity_key(&self) -> &EntityKey {
        use self::EntityOperation::*;

        match self {
            Set { ref key, .. } => key,
            Remove { ref key } => key,
            AbortUnless { .. } => panic!("cannot get entity key from AbortUnless entity operation"),
        }
    }

    /// Returns true if the operation matches a given store key.
    pub fn matches_entity(&self, key: &EntityKey) -> bool {
        self.entity_key() == key
    }

    /// Returns true if the two operations match the same entity.
    pub fn matches_same_entity(&self, other: &EntityOperation) -> bool {
        self.entity_key() == other.entity_key()
    }

    /// Applies the operation to an existing entity (may be None).
    ///
    /// Returns `Some(entity)` with an updated entity if the operation is a `Set`.
    /// Returns `None` if the operation is a `Remove`.
    pub fn apply(&self, entity: Option<Entity>) -> Option<Entity> {
        use self::EntityOperation::*;

        match self {
            Set { data, .. } => Some(
                entity
                    .map(|entity| {
                        let mut entity = entity.clone();
                        entity.merge(data.clone());
                        entity
                    })
                    .unwrap_or_else(|| data.clone()),
            ),
            Remove { .. } => None,
            AbortUnless { .. } => panic!("cannot apply AbortUnless entity operation to an entity"),
        }
    }

    /// Applies all entity operations to the given entity in order.
    /// `ops` must not contain any `AbortUnless` operations.
    pub fn apply_all(entity: Option<Entity>, ops: &Vec<EntityOperation>) -> Option<Entity> {
        use self::EntityOperation::*;

        // Only continue if all operations are Set/Remove.
        ops.iter().for_each(|op| match op {
            Set { .. } | Remove { .. } => {}
            AbortUnless { .. } => panic!("Cannot apply {:?} to an Entity", op),
        });

        // If there is a remove operations, we only need to consider the operations after that
        ops.iter()
            .rev()
            .take_while(|op| !op.is_remove())
            .collect::<Vec<_>>()
            .iter()
            .rev()
            .fold(entity, |entity, op| op.apply(entity))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum EventSource {
    None,
    EthereumBlock(EthereumBlockPointer),
}

impl fmt::Display for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EventSource::None => f.write_str("none"),
            EventSource::EthereumBlock(block_ptr) => f.write_str(&block_ptr.hash_hex()),
        }
    }
}

#[derive(Fail, Debug)]
pub enum StoreError {
    #[fail(display = "store transaction failed, need to retry: {}", _0)]
    Aborted(TransactionAbortError),
    #[fail(display = "store error: {}", _0)]
    Unknown(Error),
}

impl From<TransactionAbortError> for StoreError {
    fn from(e: TransactionAbortError) -> Self {
        StoreError::Aborted(e)
    }
}

impl From<::diesel::result::Error> for StoreError {
    fn from(e: ::diesel::result::Error) -> Self {
        StoreError::Unknown(e.into())
    }
}

impl From<Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Unknown(e)
    }
}

#[derive(Fail, Debug)]
pub enum TransactionAbortError {
    #[fail(
        display = "AbortUnless triggered abort, expected {:?} but got {:?}: {}",
        expected_entity_ids, actual_entity_ids, description
    )]
    AbortUnless {
        expected_entity_ids: Vec<String>,
        actual_entity_ids: Vec<String>,
        description: String,
    },
    #[fail(display = "transaction aborted: {}", _0)]
    Other(String),
}

/// Common trait for store implementations.
pub trait Store: Send + Sync + 'static {
    /// Get a pointer to the most recently processed block in the subgraph.
    fn block_ptr(&self, subgraph_id: SubgraphDeploymentId) -> Result<EthereumBlockPointer, Error>;

    /// Looks up an entity using the given store key.
    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;

    /// Queries the store for entities that match the store query.
    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError>;

    /// Queries the store for a single entity matching the store query.
    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError>;

    /// Updates the block pointer.  Careful: this is only safe to use if it is known that no store
    /// changes are needed to go from `block_ptr_from` to `block_ptr_to`.
    ///
    /// `block_ptr_from` must match the current value of the subgraph block pointer.
    fn set_block_ptr_with_no_changes(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError>;

    /// Transact the entity changes from a single block atomically into the store, and update the
    /// subgraph block pointer from `block_ptr_from` to `block_ptr_to`.
    ///
    /// `block_ptr_from` must match the current value of the subgraph block pointer.
    /// `block_ptr_to` must point to a child block of `block_ptr_from`.
    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
        operations: Vec<EntityOperation>,
    ) -> Result<(), StoreError>;

    /// Apply the specified entity operations
    fn apply_entity_operations(
        &self,
        operations: Vec<EntityOperation>,
        event_source: EventSource,
    ) -> Result<(), StoreError>;

    /// Revert the entity changes from a single block atomically in the store, and update the
    /// subgraph block pointer from `block_ptr_from` to `block_ptr_to`.
    ///
    /// `block_ptr_from` must match the current value of the subgraph block pointer.
    /// `block_ptr_to` must point to the parent block of `block_ptr_from`.
    fn revert_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError>;

    /// Subscribe to entity changes for specific subgraphs and entities.
    ///
    /// Returns a stream of entity changes that match the input arguments.
    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> EntityChangeStream;

    /// Counts the total number of entities in a subgraph.
    fn count_entities(&self, subgraph: SubgraphDeploymentId) -> Result<u64, Error>;
}

pub trait SubgraphDeploymentStore: Send + Sync + 'static {
    fn resolve_subgraph_name_to_id(&self, name: SubgraphName) -> Result<Option<SubgraphDeploymentId>, Error>;

    /// Check if the store is accepting queries for the specified subgraph.
    /// May return true even if the specified subgraph is not currently assigned to an indexing
    /// node, as the store will still accept queries.
    fn is_deployed(&self, id: &SubgraphDeploymentId) -> Result<bool, Error>;

    fn subgraph_schema(&self, subgraph_id: SubgraphDeploymentId) -> Result<Schema, Error>;
}

/// Common trait for blockchain store implementations.
pub trait ChainStore: Send + Sync + 'static {
    type ChainHeadUpdateListener: ChainHeadUpdateListener;

    /// Get a pointer to this blockchain's genesis block.
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error>;

    /// Insert blocks into the store (or update if they are already present).
    fn upsert_blocks<'a, B, E>(&self, blocks: B) -> Box<Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a;

    /// Try to update the head block pointer to the block with the highest block number.
    ///
    /// Only updates pointer if there is a block with a higher block number than the current head
    /// block, and the `ancestor_count` most recent ancestors of that block are in the store.
    /// Note that this means if the Ethereum node returns a different "latest block" with a
    /// different hash but same number, we do not update the chain head pointer.
    /// This situation can happen on e.g. Infura where requests are load balanced across many
    /// Ethereum nodes, in which case it's better not to continuously revert and reapply the latest
    /// blocks.
    ///
    /// If the pointer was updated, returns `Ok(vec![])`, and fires a HeadUpdateEvent.
    ///
    /// If no block has a number higher than the current head block, returns `Ok(vec![])`.
    ///
    /// If the candidate new head block had one or more missing ancestors, returns
    /// `Ok(missing_blocks)`, where `missing_blocks` is a nonexhaustive list of missing blocks.
    fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error>;

    /// Subscribe to chain head updates.
    fn chain_head_updates(&self) -> Self::ChainHeadUpdateListener;

    /// Get the current head block pointer for this chain.
    /// Any changes to the head block pointer will be to a block with a larger block number, never
    /// to a block with a smaller or equal block number.
    ///
    /// The head block pointer will be None on initial set up.
    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error>;

    /// Get Some(block) if it is present in the chain store, or None.
    fn block(&self, block_hash: H256) -> Result<Option<EthereumBlock>, Error>;

    /// Get the `offset`th ancestor of `block_hash`, where offset=0 means the block matching
    /// `block_hash` and offset=1 means its parent. Returns None if unable to complete due to
    /// missing blocks in the chain store.
    ///
    /// Returns an error if the offset would reach past the genesis block.
    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        offset: u64,
    ) -> Result<Option<EthereumBlock>, Error>;
}
