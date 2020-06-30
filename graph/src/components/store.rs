use failure::Error;
use futures::stream::poll_fn;
use futures::{Async, Future, Poll, Stream};
use lazy_static::lazy_static;
use mockall::predicate::*;
use mockall::*;
use serde::{Deserialize, Serialize};
use stable_hash::prelude::*;
use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use web3::types::{Address, H256};

use crate::data::store::*;
use crate::data::subgraph::schema::*;
use crate::prelude::*;
use crate::util::lfu_cache::LfuCache;

lazy_static! {
    pub static ref SUBSCRIPTION_THROTTLE_INTERVAL: Duration =
        env::var("SUBSCRIPTION_THROTTLE_INTERVAL")
            .ok()
            .map(|s| u64::from_str(&s).unwrap_or_else(|_| panic!(
                "failed to parse env var SUBSCRIPTION_THROTTLE_INTERVAL"
            )))
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(1000));
}

// Note: Do not modify fields without making a backward compatible change to
// the StableHash impl (below)
/// Key by which an individual entity in the store can be accessed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityKey {
    /// ID of the subgraph.
    pub subgraph_id: SubgraphDeploymentId,

    /// Name of the entity type.
    pub entity_type: String,

    /// ID of the individual entity.
    pub entity_id: String,
}

impl StableHash for EntityKey {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        self.subgraph_id
            .stable_hash(sequence_number.next_child(), state);
        self.entity_type
            .stable_hash(sequence_number.next_child(), state);
        self.entity_id
            .stable_hash(sequence_number.next_child(), state);
    }
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

// Define some convenience methods
impl EntityFilter {
    pub fn new_equal(
        attribute_name: impl Into<Attribute>,
        attribute_value: impl Into<Value>,
    ) -> Self {
        EntityFilter::Equal(attribute_name.into(), attribute_value.into())
    }

    pub fn new_in(
        attribute_name: impl Into<Attribute>,
        attribute_values: Vec<impl Into<Value>>,
    ) -> Self {
        EntityFilter::In(
            attribute_name.into(),
            attribute_values.into_iter().map(Into::into).collect(),
        )
    }

    pub fn and_maybe(self, other: Option<Self>) -> Self {
        use EntityFilter as f;
        match other {
            Some(other) => match (self, other) {
                (f::And(mut fs1), f::And(mut fs2)) => {
                    fs1.append(&mut fs2);
                    f::And(fs1)
                }
                (f::And(mut fs1), f2) => {
                    fs1.push(f2);
                    f::And(fs1)
                }
                (f1, f::And(mut fs2)) => {
                    fs2.push(f1);
                    f::And(fs2)
                }
                (f1, f2) => f::And(vec![f1, f2]),
            },
            None => self,
        }
    }
}

/// The order in which entities should be restored from a store.
#[derive(Clone, Debug, PartialEq)]
pub enum EntityOrder {
    /// Order ascending by the given attribute. Use `id` as a tie-breaker
    Ascending(String, ValueType),
    /// Order descending by the given attribute. Use `id` as a tie-breaker
    Descending(String, ValueType),
    /// Order by the `id` of the entities
    Default,
    /// Do not order at all. This speeds up queries where we know that
    /// order does not matter
    Unordered,
}

/// How many entities to return, how many to skip etc.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityRange {
    /// Limit on how many entities to return.
    pub first: Option<u32>,

    /// How many entities to skip.
    pub skip: u32,
}

impl EntityRange {
    /// Query for the first `n` entities.
    pub fn first(n: u32) -> Self {
        Self {
            first: Some(n),
            skip: 0,
        }
    }
}

/// The attribute we want to window by in an `EntityWindow`. We have to
/// distinguish between scalar and list attributes since we need to use
/// different queries for them, and the JSONB storage scheme can not
/// determine that by itself
#[derive(Clone, Debug, PartialEq)]
pub enum WindowAttribute {
    Scalar(String),
    List(String),
}

impl WindowAttribute {
    pub fn name(&self) -> &str {
        match self {
            WindowAttribute::Scalar(name) => name,
            WindowAttribute::List(name) => name,
        }
    }
}

/// How to connect children to their parent when the child table does not
/// store parent id's
#[derive(Clone, Debug, PartialEq)]
pub enum ParentLink {
    /// The parent stores a list of child ids. The ith entry in the outer
    /// vector contains the id of the children for `EntityWindow.ids[i]`
    List(Vec<Vec<String>>),
    /// The parent stores the id of one child. The ith entry in the
    /// vector contains the id of the child of the parent with id
    /// `EntityWindow.ids[i]`
    Scalar(Vec<String>),
}

/// How many children a parent can have when the child stores
/// the id of the parent
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ChildMultiplicity {
    Single,
    Many,
}

/// How to select children for their parents depending on whether the
/// child stores parent ids (`Direct`) or the parent
/// stores child ids (`Parent`)
#[derive(Clone, Debug, PartialEq)]
pub enum EntityLink {
    /// The parent id is stored in this child attribute
    Direct(WindowAttribute, ChildMultiplicity),
    /// Join with the parents table to get at the parent id
    Parent(ParentLink),
}

/// Window results of an `EntityQuery` query along the parent's id:
/// the `order_by`, `order_direction`, and `range` of the query apply to
/// entities that belong to the same parent. Only entities that belong to
/// one of the parents listed in `ids` will be included in the query result.
///
/// Note that different windows can vary both by the entity type and id of
/// the children, but also by how to get from a child to its parent, i.e.,
/// it is possible that two windows access the same entity type, but look
/// at different attributes to connect to parent entities
#[derive(Clone, Debug, PartialEq)]
pub struct EntityWindow {
    /// The entity type for this window
    pub child_type: String,
    /// The ids of parents that should be considered for this window
    pub ids: Vec<String>,
    /// How to get the parent id
    pub link: EntityLink,
}

/// The base collections from which we are going to get entities for use in
/// `EntityQuery`; the result of the query comes from applying the query's
/// filter and order etc. to the entities described in this collection. For
/// a windowed collection order and range are applied to each individual
/// window
#[derive(Clone, Debug, PartialEq)]
pub enum EntityCollection {
    /// Use all entities of the given types
    All(Vec<String>),
    /// Use entities according to the windows. The set of entities that we
    /// apply order and range to is formed by taking all entities matching
    /// the window, and grouping them by the attribute of the window. Entities
    /// that have the same value in the `attribute` field of their window are
    /// grouped together. Note that it is possible to have one window for
    /// entity type `A` and attribute `a`, and another for entity type `B` and
    /// column `b`; they will be grouped by using `A.a` and `B.b` as the keys
    Window(Vec<EntityWindow>),
}
/// The type we use for block numbers. This has to be a signed integer type
/// since Postgres does not support unsigned integer types. But 2G ought to
/// be enough for everybody
pub type BlockNumber = i32;

pub const BLOCK_NUMBER_MAX: BlockNumber = std::i32::MAX;

/// A query for entities in a store.
///
/// Details of how query generation for `EntityQuery` works can be found
/// at https://github.com/graphprotocol/rfcs/blob/master/engineering-plans/0001-graphql-query-prefetching.md
#[derive(Clone, Debug)]
pub struct EntityQuery {
    /// ID of the subgraph.
    pub subgraph_id: SubgraphDeploymentId,

    /// The block height at which to execute the query. Set this to
    /// `BLOCK_NUMBER_MAX` to run the query at the latest available block.
    /// If the subgraph uses JSONB storage, anything but `BLOCK_NUMBER_MAX`
    /// will cause an error as JSONB storage does not support querying anything
    /// but the latest block
    pub block: BlockNumber,

    /// The names of the entity types being queried. The result is the union
    /// (with repetition) of the query for each entity.
    pub collection: EntityCollection,

    /// Filter to filter entities by.
    pub filter: Option<EntityFilter>,

    /// How to order the entities
    pub order: EntityOrder,

    /// A range to limit the size of the result.
    pub range: EntityRange,

    /// Optional logger for anything related to this query
    pub logger: Option<Logger>,

    _force_use_of_new: (),
}

impl EntityQuery {
    pub fn new(
        subgraph_id: SubgraphDeploymentId,
        block: BlockNumber,
        collection: EntityCollection,
    ) -> Self {
        EntityQuery {
            subgraph_id,
            block,
            collection,
            filter: None,
            order: EntityOrder::Default,
            range: EntityRange::first(100),
            logger: None,
            _force_use_of_new: (),
        }
    }

    pub fn filter(mut self, filter: EntityFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn order(mut self, order: EntityOrder) -> Self {
        self.order = order;
        self
    }

    pub fn range(mut self, range: EntityRange) -> Self {
        self.range = range;
        self
    }

    pub fn first(mut self, first: u32) -> Self {
        self.range.first = Some(first);
        self
    }

    pub fn skip(mut self, skip: u32) -> Self {
        self.range.skip = skip;
        self
    }

    pub fn simplify(mut self) -> Self {
        // If there is one window, with one id, in a direct relation to the
        // entities, we can simplify the query by changing the filter and
        // getting rid of the window
        if let EntityCollection::Window(windows) = &self.collection {
            if windows.len() == 1 {
                let window = windows.first().expect("we just checked");
                if window.ids.len() == 1 {
                    let id = window.ids.first().expect("we just checked");
                    if let EntityLink::Direct(attribute, _) = &window.link {
                        let filter = match attribute {
                            WindowAttribute::Scalar(name) => {
                                EntityFilter::Equal(name.to_owned(), id.into())
                            }
                            WindowAttribute::List(name) => {
                                EntityFilter::Contains(name.to_owned(), Value::from(vec![id]))
                            }
                        };
                        self.filter = Some(filter.and_maybe(self.filter));
                        self.collection = EntityCollection::All(vec![window.child_type.to_owned()]);
                    }
                }
            }
        }
        self
    }
}

/// Operation types that lead to entity changes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum EntityChangeOperation {
    /// An entity was added or updated
    Set,
    /// An existing entity was removed.
    Removed,
}

/// Entity change events emitted by [Store](trait.Store.html) implementations.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

impl From<MetadataOperation> for Option<EntityChange> {
    fn from(operation: MetadataOperation) -> Self {
        use self::MetadataOperation::*;
        match operation {
            Set { entity, id, .. } | Update { entity, id, .. } => Some(EntityChange::from_key(
                MetadataOperation::entity_key(entity, id),
                EntityChangeOperation::Set,
            )),
            Remove { entity, id, .. } => Some(EntityChange::from_key(
                MetadataOperation::entity_key(entity, id),
                EntityChangeOperation::Removed,
            )),
            AbortUnless { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// The store emits `StoreEvents` to indicate that some entities have changed.
/// For block-related data, at most one `StoreEvent` is emitted for each block
/// that is processed. The `changes` vector contains the details of what changes
/// were made, and to which entity.
///
/// Since the 'subgraph of subgraphs' is special, and not directly related to
/// any specific blocks, `StoreEvents` for it are generated as soon as they are
/// written to the store.
pub struct StoreEvent {
    // The tag is only there to make it easier to track StoreEvents in the
    // logs as they flow through the system
    pub tag: usize,
    pub changes: HashSet<EntityChange>,
}

impl From<Vec<MetadataOperation>> for StoreEvent {
    fn from(operations: Vec<MetadataOperation>) -> Self {
        let changes: Vec<_> = operations.into_iter().filter_map(|op| op.into()).collect();
        StoreEvent::new(changes)
    }
}

impl<'a> FromIterator<&'a EntityModification> for StoreEvent {
    fn from_iter<I: IntoIterator<Item = &'a EntityModification>>(mods: I) -> Self {
        let changes: Vec<_> = mods
            .into_iter()
            .map(|op| {
                use self::EntityModification::*;
                match op {
                    Insert { key, .. } | Overwrite { key, .. } => {
                        EntityChange::from_key(key.clone(), EntityChangeOperation::Set)
                    }
                    Remove { key } => {
                        EntityChange::from_key(key.clone(), EntityChangeOperation::Removed)
                    }
                }
            })
            .collect();
        StoreEvent::new(changes)
    }
}

impl StoreEvent {
    pub fn new(changes: Vec<EntityChange>) -> StoreEvent {
        static NEXT_TAG: AtomicUsize = AtomicUsize::new(0);

        let tag = NEXT_TAG.fetch_add(1, Ordering::Relaxed);
        let changes = changes.into_iter().collect();
        StoreEvent { tag, changes }
    }

    /// Extend `ev1` with `ev2`. If `ev1` is `None`, just set it to `ev2`
    fn accumulate(logger: &Logger, ev1: &mut Option<StoreEvent>, ev2: StoreEvent) {
        if let Some(e) = ev1 {
            trace!(logger, "Adding changes to event";
                           "from" => ev2.tag, "to" => e.tag);
            e.changes.extend(ev2.changes);
        } else {
            *ev1 = Some(ev2);
        }
    }

    pub fn extend(mut self, other: StoreEvent) -> Self {
        self.changes.extend(other.changes);
        self
    }
}

impl fmt::Display for StoreEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StoreEvent[{}](changes: {})",
            self.tag,
            self.changes.len()
        )
    }
}

impl PartialEq for StoreEvent {
    fn eq(&self, other: &StoreEvent) -> bool {
        // Ignore tag for equality
        self.changes == other.changes
    }
}

/// A `StoreEventStream` produces the `StoreEvents`. Various filters can be applied
/// to it to reduce which and how many events are delivered by the stream.
pub struct StoreEventStream<S> {
    source: S,
}

/// A boxed `StoreEventStream`
pub type StoreEventStreamBox =
    StoreEventStream<Box<dyn Stream<Item = Arc<StoreEvent>, Error = ()> + Send>>;

impl<S> Stream for StoreEventStream<S>
where
    S: Stream<Item = Arc<StoreEvent>, Error = ()> + Send,
{
    type Item = Arc<StoreEvent>;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.source.poll()
    }
}

impl<S> StoreEventStream<S>
where
    S: Stream<Item = Arc<StoreEvent>, Error = ()> + Send + 'static,
{
    // Create a new `StoreEventStream` from another such stream
    pub fn new(source: S) -> Self {
        StoreEventStream { source }
    }

    /// Filter a `StoreEventStream` by subgraph and entity. Only events that have
    /// at least one change to one of the given (subgraph, entity) combinations
    /// will be delivered by the filtered stream.
    pub fn filter_by_entities(self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        let source = self.source.filter(move |event| {
            event.changes.iter().any({
                |change| {
                    entities.iter().any(|(subgraph_id, entity_type)| {
                        subgraph_id == &change.subgraph_id && entity_type == &change.entity_type
                    })
                }
            })
        });

        StoreEventStream::new(Box::new(source))
    }

    /// Reduce the frequency with which events are generated while a
    /// subgraph deployment is syncing. While the given `deployment` is not
    /// synced yet, events from `source` are reported at most every
    /// `interval`. At the same time, no event is held for longer than
    /// `interval`. The `StoreEvents` that arrive during an interval appear
    /// on the returned stream as a single `StoreEvent`; the events are
    /// combined by using the maximum of all sources and the concatenation
    /// of the changes of the `StoreEvents` received during the interval.
    pub fn throttle_while_syncing(
        self,
        logger: &Logger,
        store: Arc<dyn QueryStore>,
        deployment: SubgraphDeploymentId,
        interval: Duration,
    ) -> StoreEventStreamBox {
        // We refresh the synced flag every SYNC_REFRESH_FREQ*interval to
        // avoid hitting the database too often to see if the subgraph has
        // been synced in the meantime. The only downside of this approach is
        // that we might continue throttling subscription updates for a little
        // bit longer than we really should
        static SYNC_REFRESH_FREQ: u32 = 4;

        // Check whether a deployment is marked as synced in the store. The
        // special 'subgraphs' subgraph is never considered synced so that
        // we always throttle it
        let check_synced = |store: &dyn QueryStore, deployment: &SubgraphDeploymentId| {
            deployment != &*SUBGRAPHS_ID
                && store
                    .is_deployment_synced(deployment.clone())
                    .unwrap_or(false)
        };
        let mut synced = check_synced(&*store, &deployment);
        let synced_check_interval = interval.checked_mul(SYNC_REFRESH_FREQ).unwrap();
        let mut synced_last_refreshed = Instant::now();

        let mut pending_event: Option<StoreEvent> = None;
        let mut source = self.source.fuse();
        let mut had_err = false;
        let mut delay = tokio::time::delay_for(interval).unit_error().compat();
        let logger = logger.clone();

        let source = Box::new(poll_fn(move || -> Poll<Option<Arc<StoreEvent>>, ()> {
            if had_err {
                // We had an error the last time through, but returned the pending
                // event first. Indicate the error now
                had_err = false;
                return Err(());
            }

            if !synced && synced_last_refreshed.elapsed() > synced_check_interval {
                synced = check_synced(&*store, &deployment);
                synced_last_refreshed = Instant::now();
            }

            if synced {
                return source.poll();
            }

            // Check if interval has passed since the last time we sent something.
            // If it has, start a new delay timer
            let should_send = match futures::future::Future::poll(&mut delay) {
                Ok(Async::NotReady) => false,
                // Timer errors are harmless. Treat them as if the timer had
                // become ready.
                Ok(Async::Ready(())) | Err(_) => {
                    delay = tokio::time::delay_for(interval).unit_error().compat();
                    true
                }
            };

            // Get as many events as we can off of the source stream
            loop {
                match source.poll() {
                    Ok(Async::NotReady) => {
                        if should_send && pending_event.is_some() {
                            let event = pending_event.take().map(|event| Arc::new(event));
                            return Ok(Async::Ready(event));
                        } else {
                            return Ok(Async::NotReady);
                        }
                    }
                    Ok(Async::Ready(None)) => {
                        let event = pending_event.take().map(|event| Arc::new(event));
                        return Ok(Async::Ready(event));
                    }
                    Ok(Async::Ready(Some(event))) => {
                        StoreEvent::accumulate(&logger, &mut pending_event, (*event).clone());
                    }
                    Err(()) => {
                        // Before we report the error, deliver what we have accumulated so far.
                        // We will report the error the next time poll() is called
                        if pending_event.is_some() {
                            had_err = true;
                            let event = pending_event.take().map(|event| Arc::new(event));
                            return Ok(Async::Ready(event));
                        } else {
                            return Err(());
                        }
                    }
                };
            }
        }));
        StoreEventStream::new(source)
    }
}

/// An entity operation that can be transacted into the store.
#[derive(Clone, Debug, PartialEq)]
pub enum EntityOperation {
    /// Locates the entity specified by `key` and sets its attributes according to the contents of
    /// `data`.  If no entity exists with this key, creates a new entity.
    Set { key: EntityKey, data: Entity },

    /// Removes an entity with the specified key, if one exists.
    Remove { key: EntityKey },
}

/// An operation on subgraph metadata. All operations implicitly only concern
/// the subgraph of subgraphs.
#[derive(Clone, Debug)]
pub enum MetadataOperation {
    /// Locates the entity with type `entity` and the given `id` in the
    /// subgraph of subgraphs and sets its attributes according to the
    /// contents of `data`.  If no such entity exists, creates a new entity.
    Set {
        entity: String,
        id: String,
        data: Entity,
    },

    /// Removes an entity with the specified entity type and id if one exists.
    Remove { entity: String, id: String },

    /// Aborts and rolls back the transaction unless `query` returns entities
    /// exactly matching `entity_ids`. The equality test is only sensitive
    /// to the order of the results if `query` contains an `order_by`.
    AbortUnless {
        description: String, // Programmer-friendly debug message to explain reason for abort
        query: EntityQuery,  // The query to run
        entity_ids: Vec<String>, // What entities the query should return
    },

    /// Update an entity. The `data` should only contain the attributes that
    /// need to be changed, not the entire entity. The update will only happen
    /// if the given entity matches `guard` when the update is made. `Update`
    /// provides a way to atomically do a check-and-set change to an entity.
    Update {
        entity: String,
        id: String,
        data: Entity,
    },
}

impl MetadataOperation {
    pub fn entity_key(entity: String, id: String) -> EntityKey {
        EntityKey {
            subgraph_id: SUBGRAPHS_ID.clone(),
            entity_type: entity,
            entity_id: id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AttributeIndexDefinition {
    pub subgraph_id: SubgraphDeploymentId,
    pub entity_number: usize,
    pub attribute_number: usize,
    pub field_value_type: ValueType,
    pub attribute_name: String,
    pub entity_name: String,
}

#[derive(Fail, Debug)]
pub enum StoreError {
    #[fail(display = "store transaction failed, need to retry: {}", _0)]
    Aborted(TransactionAbortError),
    #[fail(display = "store error: {}", _0)]
    Unknown(Error),
    #[fail(
        display = "tried to set entity of type `{}` with ID \"{}\" but an entity of type `{}`, \
                   which has an interface in common with `{}`, exists with the same ID",
        _0, _1, _2, _0
    )]
    ConflictingId(String, String, String), // (entity, id, conflicting_entity)
    #[fail(display = "unknown field '{}'", _0)]
    UnknownField(String),
    #[fail(display = "unknown table '{}'", _0)]
    UnknownTable(String),
    #[fail(display = "malformed directive '{}'", _0)]
    MalformedDirective(String),
    #[fail(display = "query execution failed: {}", _0)]
    QueryExecutionError(String),
    #[fail(display = "invalid identifier: {}", _0)]
    InvalidIdentifier(String),
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

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Unknown(e.into())
    }
}

impl From<QueryExecutionError> for StoreError {
    fn from(e: QueryExecutionError) -> Self {
        StoreError::QueryExecutionError(e.to_string())
    }
}

#[derive(Fail, PartialEq, Eq, Debug)]
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
    fn block_ptr(
        &self,
        subgraph_id: SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error>;

    fn supports_proof_of_indexing<'a>(
        &'a self,
        subgraph_id: &'a SubgraphDeploymentId,
    ) -> DynTryFuture<'a, bool>;

    /// A value of None indicates that the table is not available. Re-deploying
    /// the subgraph fixes this. It is undesirable to force everything to
    /// re-sync from scratch, so existing deployments will continue without a
    /// Proof of Indexing. Once all subgraphs have been re-deployed the Option
    /// can be removed.
    fn get_proof_of_indexing<'a>(
        &'a self,
        subgraph_id: &'a SubgraphDeploymentId,
        indexer: &'a Option<Address>,
        block_hash: H256,
    ) -> DynTryFuture<'a, Option<[u8; 32]>>;

    /// Looks up an entity using the given store key at the latest block.
    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;

    /// Look up multiple entities as of the latest block. Returns a map of
    /// entities by type.
    fn get_many(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        ids_for_type: BTreeMap<&str, Vec<&str>>,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError>;

    /// Queries the store for entities that match the store query.
    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError>;

    /// Queries the store for a single entity matching the store query.
    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError>;

    /// Find the reverse of keccak256 for `hash` through looking it up in the
    /// rainbow table.
    fn find_ens_name(&self, _hash: &str) -> Result<Option<String>, QueryExecutionError>;

    /// Transact the entity changes from a single block atomically into the store, and update the
    /// subgraph block pointer to `block_ptr_to`.
    ///
    /// `block_ptr_to` must point to a child block of the current subgraph block pointer.
    ///
    /// Return `true` if the subgraph mentioned in `history_event` should have
    /// its schema migrated at `block_ptr_to`
    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
    ) -> Result<bool, StoreError>;

    /// Apply the specified metadata operations.
    fn apply_metadata_operations(
        &self,
        operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError>;

    /// Build indexes for a set of subgraph entity attributes
    fn build_entity_attribute_indexes(
        &self,
        subgraph: &SubgraphDeploymentId,
        indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError>;

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

    /// Subscribe to changes for specific subgraphs and entities.
    ///
    /// Returns a stream of store events that match the input arguments.
    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox;

    fn resolve_subgraph_name_to_id(
        &self,
        name: SubgraphName,
    ) -> Result<Option<SubgraphDeploymentId>, Error> {
        // Find subgraph entity by name
        let subgraph_entities = self
            .find(SubgraphEntity::query().filter(EntityFilter::Equal(
                "name".to_owned(),
                name.to_string().into(),
            )))
            .map_err(QueryError::from)?;
        let subgraph_entity = match subgraph_entities.len() {
            0 => return Ok(None),
            1 => {
                let mut subgraph_entities = subgraph_entities;
                Ok(subgraph_entities.pop().unwrap())
            }
            _ => Err(format_err!(
                "Multiple subgraphs found with name {:?}",
                name.to_string()
            )),
        }?;

        // Get current active subgraph version ID
        let current_version_id = match subgraph_entity.get("currentVersion").ok_or_else(|| {
            format_err!(
                "Subgraph entity has no `currentVersion`. \
                 The subgraph may have been created but not deployed yet. Make sure \
                 to run `graph deploy` to deploy the subgraph and have it start \
                 indexing."
            )
        })? {
            Value::String(s) => s.to_owned(),
            Value::Null => return Ok(None),
            _ => {
                return Err(format_err!(
                    "Subgraph entity has wrong type in `currentVersion`"
                ));
            }
        };

        // Read subgraph version entity
        let version_entity_opt = self
            .get(SubgraphVersionEntity::key(current_version_id))
            .map_err(QueryError::from)?;
        if version_entity_opt == None {
            return Ok(None);
        }
        let version_entity = version_entity_opt.unwrap();

        // Parse subgraph ID
        let subgraph_id_str = version_entity
            .get("deployment")
            .ok_or_else(|| format_err!("SubgraphVersion entity without `deployment`"))?
            .to_owned()
            .as_string()
            .ok_or_else(|| format_err!("SubgraphVersion entity has wrong type in `deployment`"))?;
        SubgraphDeploymentId::new(subgraph_id_str)
            .map_err(|()| {
                format_err!("SubgraphVersion entity has invalid subgraph ID in `deployment`")
            })
            .map(Some)
    }

    /// Read all version entities pointing to the specified deployment IDs and
    /// determine whether they are current or pending in order to produce
    /// `SubgraphVersionSummary`s.
    ///
    /// Returns the version summaries and a sequence of `AbortUnless`
    /// `MetadataOperation`s, which will abort the transaction if the version
    /// summaries are out of date by the time the entity operations are applied.
    fn read_subgraph_version_summaries(
        &self,
        deployment_ids: Vec<SubgraphDeploymentId>,
    ) -> Result<(Vec<SubgraphVersionSummary>, Vec<MetadataOperation>), Error> {
        let version_filter = EntityFilter::new_in(
            "deployment",
            deployment_ids.iter().map(|id| id.to_string()).collect(),
        );

        let mut ops = vec![];

        let versions = self.find(SubgraphVersionEntity::query().filter(version_filter.clone()))?;
        let version_ids = versions
            .iter()
            .map(|version_entity| version_entity.id().unwrap())
            .collect::<Vec<_>>();
        ops.push(MetadataOperation::AbortUnless {
            description: "Same set of subgraph versions must match filter".to_owned(),
            query: SubgraphVersionEntity::query().filter(version_filter),
            entity_ids: version_ids.clone(),
        });

        // Find subgraphs with one of these versions as current or pending
        let subgraphs_with_version_as_current_or_pending =
            self.find(SubgraphEntity::query().filter(EntityFilter::Or(vec![
                EntityFilter::new_in("currentVersion", version_ids.clone()),
                EntityFilter::new_in("pendingVersion", version_ids.clone()),
            ])))?;
        let subgraph_ids_with_version_as_current_or_pending =
            subgraphs_with_version_as_current_or_pending
                .iter()
                .map(|subgraph_entity| subgraph_entity.id().unwrap())
                .collect::<HashSet<_>>();
        ops.push(MetadataOperation::AbortUnless {
            description: "Same set of subgraphs must have these versions as current or pending"
                .to_owned(),
            query: SubgraphEntity::query().filter(EntityFilter::Or(vec![
                EntityFilter::new_in("currentVersion", version_ids.clone()),
                EntityFilter::new_in("pendingVersion", version_ids),
            ])),
            entity_ids: subgraph_ids_with_version_as_current_or_pending
                .into_iter()
                .collect(),
        });

        // Produce summaries, deriving flags from information in subgraph entities
        let version_summaries =
            versions
                .into_iter()
                .map(|version_entity| {
                    let version_entity_id = version_entity.id().unwrap();
                    let version_entity_id_value = Value::String(version_entity_id.clone());
                    let subgraph_id = version_entity
                        .get("subgraph")
                        .unwrap()
                        .to_owned()
                        .as_string()
                        .unwrap();

                    let is_current = subgraphs_with_version_as_current_or_pending.iter().any(
                        |subgraph_entity| {
                            if subgraph_entity.get("currentVersion")
                                == Some(&version_entity_id_value)
                            {
                                assert_eq!(subgraph_entity.id().unwrap(), subgraph_id);
                                true
                            } else {
                                false
                            }
                        },
                    );
                    let is_pending = subgraphs_with_version_as_current_or_pending.iter().any(
                        |subgraph_entity| {
                            if subgraph_entity.get("pendingVersion")
                                == Some(&version_entity_id_value)
                            {
                                assert_eq!(subgraph_entity.id().unwrap(), subgraph_id);
                                true
                            } else {
                                false
                            }
                        },
                    );

                    SubgraphVersionSummary {
                        id: version_entity_id,
                        subgraph_id,
                        deployment_id: SubgraphDeploymentId::new(
                            version_entity
                                .get("deployment")
                                .unwrap()
                                .to_owned()
                                .as_string()
                                .unwrap(),
                        )
                        .unwrap(),
                        current: is_current,
                        pending: is_pending,
                    }
                })
                .collect();

        Ok((version_summaries, ops))
    }

    /// Produce the MetadataOperations needed to create/remove
    /// SubgraphDeploymentAssignments to reflect the addition/removal of
    /// SubgraphVersions between `versions_before` and `versions_after`.
    /// Any new assignments are created with the specified `node_id`.
    /// `node_id` can be `None` if it is known that no versions were added.
    fn reconcile_assignments(
        &self,
        logger: &Logger,
        versions_before: Vec<SubgraphVersionSummary>,
        versions_after: Vec<SubgraphVersionSummary>,
        node_id: Option<NodeId>,
    ) -> Vec<MetadataOperation> {
        fn should_have_assignment(version: &SubgraphVersionSummary) -> bool {
            version.pending || version.current
        }

        let assignments_before = versions_before
            .into_iter()
            .filter(should_have_assignment)
            .map(|v| v.deployment_id)
            .collect::<HashSet<_>>();
        let assignments_after = versions_after
            .into_iter()
            .filter(should_have_assignment)
            .map(|v| v.deployment_id)
            .collect::<HashSet<_>>();
        let removed_assignments = &assignments_before - &assignments_after;
        let added_assignments = &assignments_after - &assignments_before;

        let mut ops = vec![];

        if !removed_assignments.is_empty() {
            debug!(
                logger,
                "Removing subgraph node assignments for {} subgraph deployment ID(s) ({})",
                removed_assignments.len(),
                removed_assignments
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if !added_assignments.is_empty() {
            debug!(
                logger,
                "Adding subgraph node assignments for {} subgraph deployment ID(s) ({})",
                added_assignments.len(),
                added_assignments
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        ops.extend(removed_assignments.into_iter().map(|deployment_id| {
            MetadataOperation::Remove {
                entity: SubgraphDeploymentAssignmentEntity::TYPENAME.to_owned(),
                id: deployment_id.to_string(),
            }
        }));
        ops.extend(added_assignments.iter().flat_map(|deployment_id| {
            SubgraphDeploymentAssignmentEntity::new(
                node_id
                    .clone()
                    .expect("Cannot create new subgraph deployment assignment without node ID"),
            )
            .write_operations(deployment_id)
        }));

        ops
    }

    /// Check if the store is accepting queries for the specified subgraph.
    /// May return true even if the specified subgraph is not currently assigned to an indexing
    /// node, as the store will still accept queries.
    fn is_deployed(&self, id: &SubgraphDeploymentId) -> Result<bool, Error> {
        // The subgraph of subgraphs is always deployed.
        if id == &*SUBGRAPHS_ID {
            return Ok(true);
        }

        // Check store for a deployment entity for this subgraph ID
        self.get(SubgraphDeploymentEntity::key(id.to_owned()))
            .map_err(|e| format_err!("Failed to query SubgraphDeployment entities: {}", e))
            .map(|entity_opt| entity_opt.is_some())
    }

    /// Return true if the deployment with the given id is fully synced,
    /// and return false otherwise. Errors from the store are passed back up
    fn is_deployment_synced(&self, id: SubgraphDeploymentId) -> Result<bool, Error> {
        let entity = self.get(SubgraphDeploymentEntity::key(id))?;
        entity
            .map(|entity| match entity.get("synced") {
                Some(Value::Bool(true)) => Ok(true),
                _ => Ok(false),
            })
            .unwrap_or(Ok(false))
    }

    /// Create a new subgraph deployment. The deployment must not exist yet. `ops`
    /// needs to contain all the operations on subgraphs and subgraph deployments to
    /// create the deployment, including any assignments as a current or pending
    /// version
    fn create_subgraph_deployment(
        &self,
        schema: &Schema,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError>;

    /// Start an existing subgraph deployment. This will reset the state of
    /// the subgraph to a known good state. `ops` needs to contain all the
    /// operations on the subgraph of subgraphs to reset the metadata of the
    /// subgraph
    fn start_subgraph_deployment(
        &self,
        logger: &Logger,
        subgraph_id: &SubgraphDeploymentId,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError>;

    /// Try to perform a pending migration for a subgraph schema. Even if a
    /// subgraph has a pending schema migration, this method might not actually
    /// perform the migration because of limits on the total number of
    /// migrations that can happen at the same time across the whole system.
    ///
    /// Any errors happening during the migration will be logged as warnings
    /// on `logger`, but otherwise ignored
    fn migrate_subgraph_deployment(
        &self,
        logger: &Logger,
        subgraph_id: &SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    );

    /// Return the number of the block with the given hash for the given
    /// subgraph
    fn block_number(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        block_hash: H256,
    ) -> Result<Option<BlockNumber>, StoreError>;

    /// Get a new `QueryStore`. A `QueryStore` is tied to a DB replica, so if Graph Node is
    /// configured to use secondary DB servers the queries will be distributed between servers.
    ///
    /// If `for_subscription` is true, the main replica will always be used.
    fn query_store(self: Arc<Self>, for_subscription: bool) -> Arc<dyn QueryStore + Send + Sync>;
}

mock! {
    pub Store {
        fn get_many_mock<'a>(
            &self,
            _subgraph_id: &SubgraphDeploymentId,
            _ids_for_type: BTreeMap<&'a str, Vec<&'a str>>,
        ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError>;
    }
}

// The type that the connection pool uses to track wait times for
// connection checkouts
pub type PoolWaitStats = Arc<RwLock<MovingStats>>;

// The store trait must be implemented manually because mockall does not support async_trait, nor borrowing from arguments.
impl Store for MockStore {
    fn block_ptr(
        &self,
        _subgraph_id: SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn supports_proof_of_indexing<'a>(
        &'a self,
        _subgraph_id: &'a SubgraphDeploymentId,
    ) -> DynTryFuture<'a, bool> {
        unimplemented!();
    }

    fn get_proof_of_indexing<'a>(
        &'a self,
        _subgraph_id: &'a SubgraphDeploymentId,
        _indexer: &'a Option<Address>,
        _block_hash: H256,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        unimplemented!();
    }

    fn get(&self, _key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        unimplemented!()
    }

    fn get_many(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        ids_for_type: BTreeMap<&str, Vec<&str>>,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError> {
        self.get_many_mock(subgraph_id, ids_for_type)
    }

    fn find(&self, _query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        unimplemented!()
    }

    fn find_one(&self, _query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        unimplemented!()
    }

    fn find_ens_name(&self, _hash: &str) -> Result<Option<String>, QueryExecutionError> {
        unimplemented!()
    }

    fn transact_block_operations(
        &self,
        _subgraph_id: SubgraphDeploymentId,
        _block_ptr_to: EthereumBlockPointer,
        _mods: Vec<EntityModification>,
        _stopwatch: StopwatchMetrics,
    ) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn apply_metadata_operations(
        &self,
        _operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn build_entity_attribute_indexes(
        &self,
        _subgraph: &SubgraphDeploymentId,
        _indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        unimplemented!()
    }

    fn revert_block_operations(
        &self,
        _subgraph_id: SubgraphDeploymentId,
        _block_ptr_from: EthereumBlockPointer,
        _block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn subscribe(&self, _entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        unimplemented!()
    }

    fn create_subgraph_deployment(
        &self,
        _schema: &Schema,
        _ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn start_subgraph_deployment(
        &self,
        _logger: &Logger,
        _subgraph_id: &SubgraphDeploymentId,
        _ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn migrate_subgraph_deployment(
        &self,
        _logger: &Logger,
        _subgraph_id: &SubgraphDeploymentId,
        _block_ptr: &EthereumBlockPointer,
    ) {
        unimplemented!()
    }

    fn block_number(
        &self,
        _subgraph_id: &SubgraphDeploymentId,
        _block_hash: H256,
    ) -> Result<Option<BlockNumber>, StoreError> {
        unimplemented!()
    }

    fn query_store(self: Arc<Self>, _: bool) -> Arc<dyn QueryStore + Send + Sync> {
        unimplemented!()
    }
}

#[automock]
pub trait SubgraphDeploymentStore: Send + Sync + 'static {
    /// Return the GraphQL schema supplied by the user
    fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error>;

    /// Return the GraphQL schema that was derived from the user's schema by
    /// adding a root query type etc. to it
    fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error>;

    /// Return true if the subgraph uses the relational storage scheme; if
    /// it is false, the subgraph uses JSONB storage. This method exposes
    /// store internals that should really be hidden and should be used
    /// sparingly and only when absolutely needed
    fn uses_relational_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<bool, Error>;

    /// Return the name of the network that the subgraph is indexing from. The
    /// names returned are things like `mainnet` or `ropsten`
    fn network_name(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Option<String>, Error>;
}

/// Common trait for blockchain store implementations.
#[automock]
pub trait ChainStore: Send + Sync + 'static {
    /// Get a pointer to this blockchain's genesis block.
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error>;

    /// Insert blocks into the store (or update if they are already present).
    fn upsert_blocks<B, E>(
        &self,
        _blocks: B,
    ) -> Box<dyn Future<Item = (), Error = E> + Send + 'static>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'static,
        E: From<Error> + Send + 'static,
        Self: Sized,
    {
        unimplemented!()
    }

    fn upsert_light_blocks(&self, blocks: Vec<LightEthereumBlock>) -> Result<(), Error>;

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
    fn chain_head_updates(&self) -> ChainHeadUpdateStream;

    /// Get the current head block pointer for this chain.
    /// Any changes to the head block pointer will be to a block with a larger block number, never
    /// to a block with a smaller or equal block number.
    ///
    /// The head block pointer will be None on initial set up.
    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error>;

    /// Returns the blocks present in the store.
    fn blocks(&self, hashes: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error>;

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

    /// Remove old blocks from the cache we maintain in the database and
    /// return a pair containing the number of the oldest block retained
    /// and the number of blocks deleted.
    /// We will never remove blocks that are within `ancestor_count` of
    /// the chain head.
    fn cleanup_cached_blocks(&self, ancestor_count: u64) -> Result<(BlockNumber, usize), Error>;

    /// Return the hashes of all blocks with the given number
    fn block_hashes_by_block_number(&self, number: u64) -> Result<Vec<H256>, Error>;

    /// Confirm that block number `number` has hash `hash` and that the store
    /// may purge any other blocks with that number
    fn confirm_block_hash(&self, number: u64, hash: &H256) -> Result<usize, Error>;
}

pub trait EthereumCallCache: Send + Sync + 'static {
    /// Cached return value.
    fn get_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
    ) -> Result<Option<Vec<u8>>, Error>;

    // Add entry to the cache.
    fn set_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
        return_value: &[u8],
    ) -> Result<(), Error>;
}

/// Store operations used when serving queries
pub trait QueryStore: Send + Sync {
    fn find_query_values(
        &self,
        query: EntityQuery,
    ) -> Result<Vec<BTreeMap<String, graphql_parser::query::Value>>, QueryExecutionError>;

    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox;

    fn is_deployment_synced(&self, id: SubgraphDeploymentId) -> Result<bool, Error>;
}

/// An entity operation that can be transacted into the store; as opposed to
/// `EntityOperation`, we already know whether a `Set` should be an `Insert`
/// or `Update`
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntityModification {
    /// Insert the entity
    Insert { key: EntityKey, data: Entity },
    /// Update the entity by overwriting it
    Overwrite { key: EntityKey, data: Entity },
    /// Remove the entity
    Remove { key: EntityKey },
}

impl EntityModification {
    pub fn entity_key(&self) -> &EntityKey {
        use EntityModification::*;
        match self {
            Insert { key, .. } | Overwrite { key, .. } | Remove { key } => key,
        }
    }

    /// Return `true` if self modifies the metadata subgraph
    pub fn is_meta(&self) -> bool {
        self.entity_key().subgraph_id.is_meta()
    }

    pub fn is_remove(&self) -> bool {
        match self {
            EntityModification::Remove { .. } => true,
            _ => false,
        }
    }
}

/// A cache for entities from the store that provides the basic functionality
/// needed for the store interactions in the host exports. This struct tracks
/// how entities are modified, and caches all entities looked up from the
/// store. The cache makes sure that
///   (1) no entity appears in more than one operation
///   (2) only entities that will actually be changed from what they
///       are in the store are changed
#[derive(Clone)]
pub struct EntityCache {
    /// The state of entities in the store. An entry of `None`
    /// means that the entity is not present in the store
    current: LfuCache<EntityKey, Option<Entity>>,

    /// The accumulated changes to an entity. An entry of `None`
    /// means that the entity should be deleted
    updates: BTreeMap<EntityKey, Option<Entity>>,

    pub store: Arc<dyn Store>,
}

impl Debug for EntityCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("EntityCache")
            .field("current", &self.current)
            .field("updates", &self.updates)
            .finish()
    }
}

pub struct ModificationsAndCache {
    pub modifications: Vec<EntityModification>,
    pub entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}

impl EntityCache {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self {
            current: LfuCache::new(),
            updates: BTreeMap::new(),
            store,
        }
    }

    pub fn with_current(
        store: Arc<dyn Store>,
        current: LfuCache<EntityKey, Option<Entity>>,
    ) -> EntityCache {
        EntityCache {
            current,
            updates: BTreeMap::new(),
            store,
        }
    }

    pub fn get(&mut self, key: &EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let current = self.current.get_entity(&*self.store, &key)?;
        let updates = self.updates.get(&key).cloned();
        match (current, updates) {
            // Entity is unchanged
            (current, None) => Ok(current),
            // Entity was deleted
            (_, Some(None)) => Ok(None),
            // Entity created
            (None, Some(updates)) => Ok(updates),
            // Entity updated
            (Some(current), Some(Some(updates))) => {
                let mut current = current;
                current.merge_remove_null_fields(updates);
                Ok(Some(current))
            }
        }
    }

    pub fn remove(&mut self, key: EntityKey) {
        self.updates.insert(key, None);
    }

    pub fn set(&mut self, key: EntityKey, mut entity: Entity) -> Result<(), QueryExecutionError> {
        use std::collections::btree_map::Entry;

        let update = self.updates.entry(key.clone());

        match update {
            // First change.
            Entry::Vacant(entry) => {
                entry.insert(Some(entity));
            }

            // Previously changed.
            Entry::Occupied(mut entry) => match entry.get_mut() {
                Some(prev_update) => prev_update.merge(entity),

                // Previous change was a removal, clear fields in `current`.
                None => {
                    if let Some(current) = self.current.get_entity(&*self.store, &key)? {
                        // Entity was removed so the fields not updated need to be unset.
                        for field in current.keys().cloned() {
                            entity.entry(field).or_insert(Value::Null);
                        }
                    }

                    entry.insert(Some(entity));
                }
            },
        }
        Ok(())
    }

    pub fn append(&mut self, operations: Vec<EntityOperation>) -> Result<(), QueryExecutionError> {
        for operation in operations {
            match operation {
                EntityOperation::Set { key, data } => {
                    self.set(key, data)?;
                }
                EntityOperation::Remove { key } => {
                    self.remove(key);
                }
            }
        }
        Ok(())
    }

    pub fn extend(&mut self, other: EntityCache) -> Result<(), QueryExecutionError> {
        self.current.extend(other.current);
        for (key, update) in other.updates {
            match update {
                Some(update) => self.set(key, update)?,
                None => self.remove(key),
            }
        }
        Ok(())
    }

    /// Return the changes that have been made via `set` and `remove` as
    /// `EntityModification`, making sure to only produce one when a change
    /// to the current state is actually needed.
    ///
    /// Also returns the updated `LfuCache`.
    pub fn as_modifications(
        mut self,
        store: &(impl Store + ?Sized),
    ) -> Result<ModificationsAndCache, QueryExecutionError> {
        // The first step is to make sure all entities being set are in `self.current`.
        // For each subgraph, we need a map of entity type to missing entity ids.
        let missing = self
            .updates
            .keys()
            .filter(|key| !self.current.contains_key(key));

        let mut missing_by_subgraph: BTreeMap<_, BTreeMap<&str, Vec<&str>>> = BTreeMap::new();
        for key in missing {
            missing_by_subgraph
                .entry(&key.subgraph_id)
                .or_default()
                .entry(&key.entity_type)
                .or_default()
                .push(&key.entity_id);
        }

        for (subgraph_id, keys) in missing_by_subgraph {
            for (entity_type, entities) in store.get_many(subgraph_id, keys)? {
                for entity in entities {
                    let key = EntityKey {
                        subgraph_id: subgraph_id.clone(),
                        entity_type: entity_type.clone(),
                        entity_id: entity.id().unwrap(),
                    };
                    self.current.insert(key, Some(entity));
                }
            }
        }

        let mut mods = Vec::new();
        for (key, update) in self.updates {
            use EntityModification::*;
            let current = self.current.remove(&key).and_then(|entity| entity);
            let modification = match (current, update) {
                // Entity was created
                (None, Some(updates)) => {
                    // Merging with an empty entity removes null fields.
                    let mut data = Entity::new();
                    data.merge_remove_null_fields(updates);
                    self.current.insert(key.clone(), Some(data.clone()));
                    Some(Insert { key, data })
                }
                // Entity may have been changed
                (Some(current), Some(updates)) => {
                    let mut data = current.clone();
                    data.merge_remove_null_fields(updates);
                    self.current.insert(key.clone(), Some(data.clone()));
                    if current != data {
                        Some(Overwrite { key, data })
                    } else {
                        None
                    }
                }
                // Existing entity was deleted
                (Some(_), None) => {
                    self.current.insert(key.clone(), None);
                    Some(Remove { key })
                }
                // Entity was deleted, but it doesn't exist in the store
                (None, None) => None,
            };
            if let Some(modification) = modification {
                mods.push(modification)
            }
        }
        Ok(ModificationsAndCache {
            modifications: mods,
            entity_lfu_cache: self.current,
        })
    }
}

impl LfuCache<EntityKey, Option<Entity>> {
    // Helper for cached lookup of an entity.
    fn get_entity(
        &mut self,
        store: &(impl Store + ?Sized),
        key: &EntityKey,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        match self.get(&key) {
            None => {
                let mut entity = store.get(key.clone())?;
                if let Some(entity) = &mut entity {
                    // `__typename` is for queries not for mappings.
                    entity.remove("__typename");
                }
                self.insert(key.clone(), entity.clone());
                Ok(entity)
            }
            Some(data) => Ok(data.to_owned()),
        }
    }
}
