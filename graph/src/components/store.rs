use failure::Error;
use futures::Future;
use futures::Stream;
use web3::types::{Block, Transaction, H256};

use data::store::*;
use std::fmt;

/// Key by which an individual entity in the store can be accessed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoreKey {
    // ID of the subgraph.
    pub subgraph: String,

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
    pub subgraph: String,

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

/// An entity operation that can be transacted into the store.
#[derive(Clone, Debug)]
pub enum EntityOperation {
    Set {
        subgraph: String,
        entity: String,
        id: String,
        data: Entity,
    },
    Remove {
        subgraph: String,
        entity: String,
        id: String,
    },
}

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

/// Common trait for store implementations that don't require interaction with the system.
pub trait BasicStore: Send {
    /// Looks up an entity using the given store key.
    fn get(&self, key: StoreKey) -> Result<Entity, ()>;

    /// Updates an entity using the given store key and entity data.
    fn set(&mut self, key: StoreKey, entity: Entity, event_source: EventSource) -> Result<(), ()>;

    /// Deletes an entity using the given store key.
    fn delete(&mut self, key: StoreKey, event_source: EventSource) -> Result<(), ()>;

    /// Queries the store for entities that match the store query.
    fn find(&self, query: StoreQuery) -> Result<Vec<Entity>, ()>;
}

/// A pair of subgraph ID and entity type name.
pub type SubgraphEntityPair = (String, String);

/// Common trait for block data store implementations.
pub trait BlockStore {
    /// Add a new network, but only if one with this name does not already exist in the block store
    fn add_network_if_missing(
        &self,
        network_name: &str,
        net_version: &str,
        genesis_block_hash: H256,
    ) -> Result<(), Error>;

    /// Insert blocks into the store (or update if they are already present).
    fn upsert_blocks<'a, B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a>(
        &self,
        network_name: &str,
        blocks: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a>;

    /// Try to update the head block pointer to the block with the highest block number.
    ///
    /// Only updates pointer if there is a block with a higher block number than the current head
    /// block, and the `ancestor_count` most recent ancestors of that block are in the store.
    ///
    /// If the pointer was updated, returns `Ok(vec![])`, and fires a HeadUpdateEvent.
    ///
    /// If no block has a number higher than the current head block, returns `Ok(vec![])`.
    ///
    /// If the candidate new head block had one or more missing ancestors, returns
    /// `Ok(missing_blocks)`, where `missing_blocks` is a nonexhaustive list of missing blocks.
    fn attempt_chain_head_update(
        &self,
        network_name: &str,
        ancestor_count: u64,
    ) -> Result<Vec<H256>, Error>;
}

/// Common trait for store implementations.
pub trait Store: BasicStore + BlockStore + Send {
    /// Subscribe to entity changes for specific subgraphs and entities.
    ///
    /// Returns a stream of entity changes that match the input arguments.
    fn subscribe(&mut self, entities: Vec<SubgraphEntityPair>) -> EntityChangeStream;
}
