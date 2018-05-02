use futures::sync::mpsc::{Receiver, Sender};
use std::collections::HashMap;
use super::schema::SchemaProviderEvent;
use super::util::stream::StreamError;

/// Key by which an individual entity in the store can be accessed.
pub struct StoreKey {
    /// Name of the entity type.
    pub entity: String,

    /// ID of the individual entity.
    pub id: String,
}

/// An entity attribute name is represented as a string.
pub type Attribute = String;

/// An attribute value is represented as an enum with variants for all supported value types.
pub enum Value {
    String(String),
}

/// An entity is represented as a map of attribute names to values.
pub type Entity = HashMap<Attribute, Value>;

/// Supported types of store filters.
pub enum StoreFilter {
    And(Vec<StoreFilter>),
    Or(Vec<StoreFilter>),
    Equal(Attribute, Value),
    Not(Attribute, Value),
    GreaterThan(Attribute, Value),
    LessThan(Attribute, Value),
    GreaterOrEqual(Attribute, Value),
    LessThanOrEqual(Attribute, Value),
    In(Attribute, Vec<Value>),
    NotIn(Attribute, Vec<Value>),
    Contains(Attribute, Value),
    NotContains(Attribute, Value),
    StartsWith(Attribute, Value),
    EndsWith(Attribute, Value),
    NotEndsWith(Attribute, Value),
}

/// The order in which entities should be restored from a store.
pub enum StoreOrder {
    Ascending,
    Descending,
}

/// How many entities to return, how many to skip etc.
pub struct StoreRange {
    /// How many entities to return.
    pub first: usize,

    /// How many entities to skip.
    pub skip: usize,
}

/// A query for entities in a store.
pub struct StoreQuery {
    /// The name of the entity type.
    pub entity: String,

    /// Filters to filter entities by.
    pub filters: Vec<StoreFilter>,

    /// An optional attribute to order the entities by.
    pub order_by: Option<String>,

    /// The direction to order entities in.
    pub order_direction: StoreOrder,

    /// An optional range to limit the size of the result.
    pub range: Option<StoreRange>,
}

/// Events emitted by implementations of [Store](trait.Store.html).
#[derive(Debug)]
pub enum StoreEvent {
    /// An entity was added to the store.
    EntityAdded(&'static str),

    /// An entity was removed from the store.
    EntityRemoved(&'static str),

    /// An entity was changed in the store.
    EntityChanged(&'static str),
}

/// Common trait for store implementations.
pub trait Store {
    /// Looks up an entity using the given store key.
    fn get(&self, key: StoreKey) -> Result<Entity, ()>;

    /// Updates an entity using the given store key and entity data.
    fn set(&mut self, key: StoreKey, entity: Entity) -> Result<(), ()>;

    /// Deletes an entity using the given store key.
    fn delete(&mut self, key: StoreKey) -> Result<(), ()>;

    /// Queries the store for entities that match the store query.
    fn find(&self, query: StoreQuery) -> Result<Vec<Entity>, ()>;

    /// Sender to which others should write whenever the schema that the store
    /// should implement changes.
    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent>;

    /// Receiver from which others can read events emitted by the store.
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn event_stream(&mut self) -> Result<Receiver<StoreEvent>, StreamError>;
}
