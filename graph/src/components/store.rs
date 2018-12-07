use failure::Error;
use futures::Future;
use futures::Stream;
use web3::types::H256;

use data::store::*;
use prelude::*;

/// Key by which an individual entity in the store can be accessed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EntityKey {
    /// ID of the subgraph.
    pub subgraph_id: SubgraphId,

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
    pub subgraph_id: SubgraphId,

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
    pub subgraph_id: SubgraphId,
    /// Entity type name of the changed entity.
    pub entity_type: String,
    /// ID of the changed entity.
    pub entity_id: String,
    /// Operation that caused the change.
    pub operation: EntityChangeOperation,
}

/// A stream of entity change events.
pub type EntityChangeStream = Box<Stream<Item = EntityChange, Error = ()> + Send>;

/// An entity operation that can be transacted into the store.
#[derive(Clone, Debug, PartialEq)]
pub enum EntityOperation {
    /// An entity is created or updated.
    Set { key: EntityKey, data: Entity },
    /// An entity is removed.
    Remove { key: EntityKey },
}

impl EntityOperation {
    /// Returns true if the operation is of variant `Set`.
    pub fn is_set(&self) -> bool {
        match self {
            EntityOperation::Set { .. } => true,
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
        }
    }

    /// Merges another operation into this operation. Returns the modified operation.
    ///
    /// Panics if the two operations are for different entities.
    pub fn merge(&self, other: &EntityOperation) -> Self {
        use self::EntityOperation::*;

        assert_eq!(self.entity_key(), other.entity_key());

        match other {
            Remove { .. } => other.clone(),
            Set { key, data } => Set {
                key: key.clone(),
                data: {
                    let mut entity = match self {
                        Set { data, .. } => data.clone(),
                        _ => Entity::new(),
                    };
                    entity.merge(data.clone());
                    entity
                },
            },
        }
    }

    /// Applies all entity operations to the given entity in order.
    pub fn apply_all(entity: Option<Entity>, ops: &Vec<EntityOperation>) -> Option<Entity> {
        // If there is a remove operations, we only need to consider the operations after that
        ops.iter()
            .rev()
            .take_while(|op| !op.is_remove())
            .collect::<Vec<_>>()
            .iter()
            .rev()
            .fold(entity, |entity, op| op.apply(entity))
    }

    /// Folds all entity operations for the same entity into a single operation.
    pub fn fold(ops: &Vec<EntityOperation>) -> Vec<EntityOperation> {
        ops.iter().fold(
            vec![],
            |mut out: Vec<EntityOperation>, op: &EntityOperation| {
                let mut op_updated = false;
                for existing_op in out.iter_mut() {
                    if existing_op.matches_same_entity(op) {
                        *existing_op = existing_op.merge(op);
                        op_updated = true;
                    }
                }
                if !op_updated {
                    out.push(op.clone());
                }
                out
            },
        )
    }
}

/// Common trait for store implementations.
pub trait Store: Send + Sync + 'static {
    /// Register a new subgraph ID in the store, and initialize the subgraph's block pointer to the
    /// specified value.
    /// Each subgraph has its own entities and separate block processing state.
    fn add_subgraph_if_missing(
        &self,
        subgraph_id: SubgraphId,
        block_ptr: EthereumBlockPointer,
    ) -> Result<(), Error>;

    /// Get a pointer to the most recently processed block in the subgraph.
    fn block_ptr(&self, subgraph_id: SubgraphId) -> Result<EthereumBlockPointer, Error>;

    /// Looks up an entity using the given store key.
    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;

    /// Queries the store for entities that match the store query.
    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError>;

    /// Updates the block pointer.  Careful: this is only safe to use if it is known that no store
    /// changes are needed to go from `block_ptr_from` to `block_ptr_to`.
    ///
    /// `block_ptr_from` must match the current value of the subgraph block pointer.
    fn set_block_ptr_with_no_changes(
        &self,
        subgraph_id: SubgraphId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), Error>;

    /// Transact the entity changes from a single block atomically into the store, and update the
    /// subgraph block pointer from `block_ptr_from` to `block_ptr_to`.
    ///
    /// `block_ptr_from` must match the current value of the subgraph block pointer.
    /// `block_ptr_to` must point to a child block of `block_ptr_from`.
    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
        operations: Vec<EntityOperation>,
    ) -> Result<(), Error>;

    /// For operations that are not tied to a block.
    fn apply_set_operation(
        &self,
        operation: EntityOperation,
        op_event_source: String,
    ) -> Result<(), Error>;

    /// Revert the entity changes from a single block atomically in the store, and update the
    /// subgraph block pointer from `block_ptr_from` to `block_ptr_to`.
    ///
    /// `block_ptr_from` must match the current value of the subgraph block pointer.
    /// `block_ptr_to` must point to the parent block of `block_ptr_from`.
    fn revert_block_operations(
        &self,
        subgraph_id: SubgraphId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), Error>;

    /// Subscribe to entity changes for specific subgraphs and entities.
    ///
    /// Returns a stream of entity changes that match the input arguments.
    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> EntityChangeStream;

    /// Counts the total number of entities in a subgraph.
    fn count_entities(&self, subgraph: SubgraphId) -> Result<u64, Error>;
}

pub trait SubgraphDeploymentStore: Send + Sync + 'static {
    /// List all deployment names and their associated subgraph IDs for the specified node ID.
    fn read_by_node_id(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<(SubgraphDeploymentName, SubgraphId)>, Error>;

    fn write(
        &self,
        name: SubgraphDeploymentName,
        subgraph_id: SubgraphId,
        node_id: NodeId,
    ) -> Result<(), Error>;

    fn read(&self, name: SubgraphDeploymentName) -> Result<Option<(SubgraphId, NodeId)>, Error>;

    fn remove(&self, name: SubgraphDeploymentName) -> Result<bool, Error>;

    fn deployment_events(
        &self,
        node_id: NodeId,
    ) -> Box<Stream<Item = DeploymentEvent, Error = Error> + Send>;
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
