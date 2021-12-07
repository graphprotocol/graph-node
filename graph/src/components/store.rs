use futures::stream::poll_fn;
use futures::{Async, Poll, Stream};
use graphql_parser::schema as s;
use lazy_static::lazy_static;
use mockall::predicate::*;
use mockall::*;
use serde::{Deserialize, Serialize};
use stable_hash::prelude::*;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::env;
use std::fmt;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;
use web3::types::{Address, H256};

use crate::blockchain::{Block, Blockchain};
use crate::components::server::index_node::VersionInfo;
use crate::components::transaction_receipt;
use crate::data::subgraph::status;
use crate::data::{store::*, subgraph::Source};
use crate::prelude::*;
use crate::util::lfu_cache::LfuCache;
use crate::{
    blockchain::DataSource,
    data::{query::QueryTarget, subgraph::schema::*},
};

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

/// The type name of an entity. This is the string that is used in the
/// subgraph's GraphQL schema as `type NAME @entity { .. }`
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityType(String);

impl EntityType {
    /// Construct a new entity type. Ideally, this is only called when
    /// `entity_type` either comes from the GraphQL schema, or from
    /// the database from fields that are known to contain a valid entity type
    pub fn new(entity_type: String) -> Self {
        Self(entity_type)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> From<&s::ObjectType<'a, String>> for EntityType {
    fn from(object_type: &s::ObjectType<'a, String>) -> Self {
        EntityType::new(object_type.name.to_owned())
    }
}

impl<'a> From<&s::InterfaceType<'a, String>> for EntityType {
    fn from(interface_type: &s::InterfaceType<'a, String>) -> Self {
        EntityType::new(interface_type.name.to_owned())
    }
}

// This conversion should only be used in tests since it makes it too
// easy to convert random strings into entity types
#[cfg(debug_assertions)]
impl From<&str> for EntityType {
    fn from(s: &str) -> Self {
        EntityType::new(s.to_owned())
    }
}

impl CheapClone for EntityType {}

// Note: Do not modify fields without making a backward compatible change to
// the StableHash impl (below)
/// Key by which an individual entity in the store can be accessed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityKey {
    /// ID of the subgraph.
    pub subgraph_id: DeploymentHash,

    /// Name of the entity type.
    pub entity_type: EntityType,

    /// ID of the individual entity.
    pub entity_id: String,
}

impl StableHash for EntityKey {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        self.subgraph_id
            .stable_hash(sequence_number.next_child(), state);
        self.entity_type
            .as_str()
            .stable_hash(sequence_number.next_child(), state);
        self.entity_id
            .stable_hash(sequence_number.next_child(), state);
    }
}

impl EntityKey {
    pub fn data(subgraph_id: DeploymentHash, entity_type: String, entity_id: String) -> Self {
        Self {
            subgraph_id,
            entity_type: EntityType::new(entity_type),
            entity_id,
        }
    }
}

#[test]
fn key_stable_hash() {
    use stable_hash::crypto::SetHasher;
    use stable_hash::utils::stable_hash;

    #[track_caller]
    fn hashes_to(key: &EntityKey, exp: &str) {
        let hash = hex::encode(stable_hash::<SetHasher, _>(&key));
        assert_eq!(exp, hash.as_str());
    }

    let id = DeploymentHash::new("QmP9MRvVzwHxr3sGvujihbvJzcTz2LYLMfi5DyihBg6VUd").unwrap();
    let key = EntityKey::data(id.clone(), "Account".to_string(), "0xdeadbeef".to_string());
    hashes_to(
        &key,
        "905b57035d6f98cff8281e7b055e10570a2bd31190507341c6716af2d3c1ad98",
    );
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
    pub child_type: EntityType,
    /// The ids of parents that should be considered for this window
    pub ids: Vec<String>,
    /// How to get the parent id
    pub link: EntityLink,
    pub column_names: AttributeNames,
}

/// The base collections from which we are going to get entities for use in
/// `EntityQuery`; the result of the query comes from applying the query's
/// filter and order etc. to the entities described in this collection. For
/// a windowed collection order and range are applied to each individual
/// window
#[derive(Clone, Debug, PartialEq)]
pub enum EntityCollection {
    /// Use all entities of the given types
    All(Vec<(EntityType, AttributeNames)>),
    /// Use entities according to the windows. The set of entities that we
    /// apply order and range to is formed by taking all entities matching
    /// the window, and grouping them by the attribute of the window. Entities
    /// that have the same value in the `attribute` field of their window are
    /// grouped together. Note that it is possible to have one window for
    /// entity type `A` and attribute `a`, and another for entity type `B` and
    /// column `b`; they will be grouped by using `A.a` and `B.b` as the keys
    Window(Vec<EntityWindow>),
}

impl EntityCollection {
    pub fn entity_types_and_column_names(&self) -> BTreeMap<EntityType, AttributeNames> {
        let mut map = BTreeMap::new();
        match self {
            EntityCollection::All(pairs) => pairs.iter().for_each(|(entity_type, column_names)| {
                map.insert(entity_type.clone(), column_names.clone());
            }),
            EntityCollection::Window(windows) => windows.iter().for_each(
                |EntityWindow {
                     child_type,
                     column_names,
                     ..
                 }| match map.entry(child_type.clone()) {
                    Entry::Occupied(mut entry) => entry.get_mut().extend(column_names.clone()),
                    Entry::Vacant(entry) => {
                        entry.insert(column_names.clone());
                    }
                },
            ),
        }
        map
    }
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
    pub subgraph_id: DeploymentHash,

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

    pub query_id: Option<String>,

    _force_use_of_new: (),
}

impl EntityQuery {
    pub fn new(
        subgraph_id: DeploymentHash,
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
            query_id: None,
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
                        self.collection = EntityCollection::All(vec![(
                            window.child_type.to_owned(),
                            window.column_names.clone(),
                        )]);
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
pub enum EntityChange {
    Data {
        subgraph_id: DeploymentHash,
        /// Entity type name of the changed entity.
        entity_type: EntityType,
    },
    Assignment {
        deployment: DeploymentLocator,
        operation: EntityChangeOperation,
    },
}

impl EntityChange {
    pub fn for_data(key: EntityKey) -> Self {
        Self::Data {
            subgraph_id: key.subgraph_id,
            entity_type: key.entity_type,
        }
    }

    pub fn for_assignment(deployment: DeploymentLocator, operation: EntityChangeOperation) -> Self {
        Self::Assignment {
            deployment,
            operation,
        }
    }

    pub fn as_filter(&self) -> SubscriptionFilter {
        use EntityChange::*;
        match self {
            Data {
                subgraph_id,
                entity_type,
                ..
            } => SubscriptionFilter::Entities(subgraph_id.clone(), entity_type.clone()),
            Assignment { .. } => SubscriptionFilter::Assignment,
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

impl<'a> FromIterator<&'a EntityModification> for StoreEvent {
    fn from_iter<I: IntoIterator<Item = &'a EntityModification>>(mods: I) -> Self {
        let changes: Vec<_> = mods
            .into_iter()
            .map(|op| {
                use self::EntityModification::*;
                match op {
                    Insert { key, .. } | Overwrite { key, .. } | Remove { key } => {
                        EntityChange::for_data(key.clone())
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

    pub fn matches(&self, filters: &BTreeSet<SubscriptionFilter>) -> bool {
        self.changes
            .iter()
            .any(|change| filters.iter().any(|filter| filter.matches(change)))
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

pub type UnitStream = Box<dyn futures03::Stream<Item = ()> + Unpin + Send + Sync>;

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
    pub fn filter_by_entities(self, filters: BTreeSet<SubscriptionFilter>) -> StoreEventStreamBox {
        let source = self.source.filter(move |event| event.matches(&filters));

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
    //
    // Currently unused, needs to be made compatible with `subscribe_no_payload`.
    pub async fn throttle_while_syncing(
        self,
        logger: &Logger,
        store: Arc<dyn QueryStore>,
        interval: Duration,
    ) -> StoreEventStreamBox {
        // Check whether a deployment is marked as synced in the store. Note that in the moment a
        // subgraph becomes synced any existing subscriptions will continue to be throttled since
        // this is not re-checked.
        let synced = store.is_deployment_synced().await.unwrap_or(false);

        let mut pending_event: Option<StoreEvent> = None;
        let mut source = self.source.fuse();
        let mut had_err = false;
        let mut delay = tokio::time::sleep(interval).unit_error().boxed().compat();
        let logger = logger.clone();

        let source = Box::new(poll_fn(move || -> Poll<Option<Arc<StoreEvent>>, ()> {
            if had_err {
                // We had an error the last time through, but returned the pending
                // event first. Indicate the error now
                had_err = false;
                return Err(());
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
                    delay = tokio::time::sleep(interval).unit_error().boxed().compat();
                    true
                }
            };

            // Get as many events as we can off of the source stream
            loop {
                match source.poll() {
                    Ok(Async::NotReady) => {
                        if should_send && pending_event.is_some() {
                            let event = pending_event.take().map(Arc::new);
                            return Ok(Async::Ready(event));
                        } else {
                            return Ok(Async::NotReady);
                        }
                    }
                    Ok(Async::Ready(None)) => {
                        let event = pending_event.take().map(Arc::new);
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
                            let event = pending_event.take().map(Arc::new);
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

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("store error: {0}")]
    Unknown(Error),
    #[error(
        "tried to set entity of type `{0}` with ID \"{1}\" but an entity of type `{2}`, \
         which has an interface in common with `{0}`, exists with the same ID"
    )]
    ConflictingId(String, String, String), // (entity, id, conflicting_entity)
    #[error("unknown field '{0}'")]
    UnknownField(String),
    #[error("unknown table '{0}'")]
    UnknownTable(String),
    #[error("malformed directive '{0}'")]
    MalformedDirective(String),
    #[error("query execution failed: {0}")]
    QueryExecutionError(String),
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    #[error(
        "subgraph `{0}` has already processed block `{1}`; \
         there are most likely two (or more) nodes indexing this subgraph"
    )]
    DuplicateBlockProcessing(DeploymentHash, BlockNumber),
    /// An internal error where we expected the application logic to enforce
    /// some constraint, e.g., that subgraph names are unique, but found that
    /// constraint to not hold
    #[error("internal constraint violated: {0}")]
    ConstraintViolation(String),
    #[error("deployment not found: {0}")]
    DeploymentNotFound(String),
    #[error("shard not found: {0} (this usually indicates a misconfiguration)")]
    UnknownShard(String),
    #[error("Fulltext search not yet deterministic")]
    FulltextSearchNonDeterministic,
    #[error("operation was canceled")]
    Canceled,
    #[error("database unavailable")]
    DatabaseUnavailable,
}

// Convenience to report a constraint violation
#[macro_export]
macro_rules! constraint_violation {
    ($msg:expr) => {{
        StoreError::ConstraintViolation(format!("{}", $msg))
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        StoreError::ConstraintViolation(format!($fmt, $($arg)*))
    }}
}

impl From<::diesel::result::Error> for StoreError {
    fn from(e: ::diesel::result::Error) -> Self {
        StoreError::Unknown(e.into())
    }
}

impl From<::diesel::r2d2::PoolError> for StoreError {
    fn from(e: ::diesel::r2d2::PoolError) -> Self {
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

impl From<std::fmt::Error> for StoreError {
    fn from(e: std::fmt::Error) -> Self {
        StoreError::Unknown(anyhow!("{}", e.to_string()))
    }
}

pub struct StoredDynamicDataSource {
    pub name: String,
    pub source: Source,
    pub context: Option<String>,
    pub creation_block: Option<BlockNumber>,
}

pub trait SubscriptionManager: Send + Sync + 'static {
    /// Subscribe to changes for specific subgraphs and entities.
    ///
    /// Returns a stream of store events that match the input arguments.
    fn subscribe(&self, entities: BTreeSet<SubscriptionFilter>) -> StoreEventStreamBox;

    /// If the payload is not required, use for a more efficient subscription mechanism backed by a watcher.
    fn subscribe_no_payload(&self, entities: BTreeSet<SubscriptionFilter>) -> UnitStream;
}

/// An internal identifer for the specific instance of a deployment. The
/// identifier only has meaning in the context of a specific instance of
/// graph-node. Only store code should ever construct or consume it; all
/// other code passes it around as an opaque token.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DeploymentId(pub i32);

impl Display for DeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl DeploymentId {
    pub fn new(id: i32) -> Self {
        Self(id)
    }
}

/// A unique identifier for a deployment that specifies both its external
/// identifier (`hash`) and its unique internal identifier (`id`) which
/// ensures we are talking about a unique location for the deployment's data
/// in the store
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DeploymentLocator {
    pub id: DeploymentId,
    pub hash: DeploymentHash,
}

impl CheapClone for DeploymentLocator {}

impl slog::Value for DeploymentLocator {
    fn serialize(
        &self,
        record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::Value::serialize(&self.to_string(), record, key, serializer)
    }
}

impl DeploymentLocator {
    pub fn new(id: DeploymentId, hash: DeploymentHash) -> Self {
        Self { id, hash }
    }
}

impl Display for DeploymentLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.hash, self.id)
    }
}

/// A special trait to handle looking up ENS names from special rainbow
/// tables that need to be manually loaded into the system
pub trait EnsLookup: Send + Sync + 'static {
    /// Find the reverse of keccak256 for `hash` through looking it up in the
    /// rainbow table.
    fn find_name(&self, hash: &str) -> Result<Option<String>, StoreError>;
}

/// Common trait for store implementations.
#[async_trait]
pub trait SubgraphStore: Send + Sync + 'static {
    fn ens_lookup(&self) -> Arc<dyn EnsLookup>;

    /// Check if the store is accepting queries for the specified subgraph.
    /// May return true even if the specified subgraph is not currently assigned to an indexing
    /// node, as the store will still accept queries.
    fn is_deployed(&self, id: &DeploymentHash) -> Result<bool, StoreError>;

    /// Create a new deployment for the subgraph `name`. If the deployment
    /// already exists (as identified by the `schema.id`), reuse that, otherwise
    /// create a new deployment, and point the current or pending version of
    /// `name` at it, depending on the `mode`
    fn create_subgraph_deployment(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        network: String,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<DeploymentLocator, StoreError>;

    /// Create a new subgraph with the given name. If one already exists, use
    /// the existing one. Return the `id` of the newly created or existing
    /// subgraph
    fn create_subgraph(&self, name: SubgraphName) -> Result<String, StoreError>;

    /// Remove a subgraph and all its versions; if deployments that were used
    /// by this subgraph do not need to be indexed anymore, also remove
    /// their assignment, but keep the deployments themselves around
    fn remove_subgraph(&self, name: SubgraphName) -> Result<(), StoreError>;

    /// Assign the subgraph with `id` to the node `node_id`. If there is no
    /// assignment for the given deployment, report an error.
    fn reassign_subgraph(
        &self,
        deployment: &DeploymentLocator,
        node_id: &NodeId,
    ) -> Result<(), StoreError>;

    fn assigned_node(&self, deployment: &DeploymentLocator) -> Result<Option<NodeId>, StoreError>;

    fn assignments(&self, node: &NodeId) -> Result<Vec<DeploymentLocator>, StoreError>;

    /// Return `true` if a subgraph `name` exists, regardless of whether the
    /// subgraph has any deployments attached to it
    fn subgraph_exists(&self, name: &SubgraphName) -> Result<bool, StoreError>;

    /// Return the GraphQL schema supplied by the user
    fn input_schema(&self, subgraph_id: &DeploymentHash) -> Result<Arc<Schema>, StoreError>;

    /// Return the GraphQL schema that was derived from the user's schema by
    /// adding a root query type etc. to it
    fn api_schema(&self, subgraph_id: &DeploymentHash) -> Result<Arc<ApiSchema>, StoreError>;

    /// Return a `WritableStore` that is used for indexing subgraphs. Only
    /// code that is part of indexing a subgraph should ever use this. The
    /// `logger` will be used to log important messages related to the
    /// subgraph
    async fn writable(
        self: Arc<Self>,
        logger: Logger,
        deployment: DeploymentId,
    ) -> Result<Arc<dyn WritableStore>, StoreError>;

    /// The network indexer does not follow the normal flow of how subgraphs
    /// are indexed, and therefore needs a special way to get a
    /// `WritableStore`. This method should not be used outside of that, and
    /// `writable` should be used instead
    fn writable_for_network_indexer(
        &self,
        logger: Logger,
        id: &DeploymentHash,
    ) -> Result<Arc<dyn WritableStore>, StoreError>;

    /// Return the minimum block pointer of all deployments with this `id`
    /// that we would use to query or copy from; in particular, this will
    /// ignore any instances of this deployment that are in the process of
    /// being set up
    fn least_block_ptr(&self, id: &DeploymentHash) -> Result<Option<BlockPtr>, StoreError>;

    /// Find the deployment locators for the subgraph with the given hash
    fn locators(&self, hash: &str) -> Result<Vec<DeploymentLocator>, StoreError>;
}

/// A view of the store for indexing. All indexing-related operations need
/// to go through this trait. Methods in this trait will never return a
/// `StoreError::DatabaseUnavailable`. Instead, they will retry the
/// operation indefinitely until it succeeds.
#[async_trait]
pub trait WritableStore: Send + Sync + 'static {
    /// Get a pointer to the most recently processed block in the subgraph.
    fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError>;

    /// Returns the Firehose `cursor` this deployment is currently at in the block stream of events. This
    /// is used when re-connecting a Firehose stream to start back exactly where we left off.
    fn block_cursor(&self) -> Result<Option<String>, StoreError>;

    /// Start an existing subgraph deployment.
    fn start_subgraph_deployment(&self, logger: &Logger) -> Result<(), StoreError>;

    /// Revert the entity changes from a single block atomically in the store, and update the
    /// subgraph block pointer to `block_ptr_to`.
    ///
    /// `block_ptr_to` must point to the parent block of the subgraph block pointer.
    fn revert_block_operations(&self, block_ptr_to: BlockPtr) -> Result<(), StoreError>;

    /// If a deterministic error happened, this function reverts the block operations from the
    /// current block to the previous block.
    fn unfail_deterministic_error(
        &self,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<(), StoreError>;

    /// If a non-deterministic error happened and the current deployment head is past the error
    /// block range, this function unfails the subgraph and deletes the error.
    fn unfail_non_deterministic_error(&self, current_ptr: &BlockPtr) -> Result<(), StoreError>;

    /// Set subgraph status to failed with the given error as the cause.
    async fn fail_subgraph(&self, error: SubgraphError) -> Result<(), StoreError>;

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError>;

    /// Looks up an entity using the given store key at the latest block.
    fn get(&self, key: &EntityKey) -> Result<Option<Entity>, StoreError>;

    /// Transact the entity changes from a single block atomically into the store, and update the
    /// subgraph block pointer to `block_ptr_to`, and update the firehose cursor to `firehose_cursor`
    ///
    /// `block_ptr_to` must point to a child block of the current subgraph block pointer.
    fn transact_block_operations(
        &self,
        block_ptr_to: BlockPtr,
        firehose_cursor: Option<String>,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError>;

    /// Look up multiple entities as of the latest block. Returns a map of
    /// entities by type.
    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError>;

    /// The deployment `id` finished syncing, mark it as synced in the database
    /// and promote it to the current version in the subgraphs where it was the
    /// pending version so far
    fn deployment_synced(&self) -> Result<(), StoreError>;

    /// Return true if the deployment with the given id is fully synced,
    /// and return false otherwise. Errors from the store are passed back up
    async fn is_deployment_synced(&self) -> Result<bool, StoreError>;

    fn unassign_subgraph(&self) -> Result<(), StoreError>;

    /// Load the dynamic data sources for the given deployment
    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError>;

    /// Report the name of the shard in which the subgraph is stored. This
    /// should only be used for reporting and monitoring
    fn shard(&self) -> &str;

    async fn health(&self, id: &DeploymentHash) -> Result<SubgraphHealth, StoreError>;

    fn input_schema(&self) -> Arc<Schema>;
}

#[async_trait]
pub trait QueryStoreManager: Send + Sync + 'static {
    /// Get a new `QueryStore`. A `QueryStore` is tied to a DB replica, so if Graph Node is
    /// configured to use secondary DB servers the queries will be distributed between servers.
    ///
    /// The query store is specific to a deployment, and `id` must indicate
    /// which deployment will be queried. It is not possible to use the id of the
    /// metadata subgraph, though the resulting store can be used to query
    /// metadata about the deployment `id` (but not metadata about other deployments).
    ///
    /// If `for_subscription` is true, the main replica will always be used.
    async fn query_store(
        &self,
        target: QueryTarget,
        for_subscription: bool,
    ) -> Result<Arc<dyn QueryStore + Send + Sync>, QueryExecutionError>;
}

mock! {
    pub Store {
        fn get_many_mock<'a>(
            &self,
            _ids_for_type: BTreeMap<&'a EntityType, Vec<&'a str>>,
        ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError>;
    }
}

// The type that the connection pool uses to track wait times for
// connection checkouts
pub type PoolWaitStats = Arc<RwLock<MovingStats>>;

// The store trait must be implemented manually because mockall does not support async_trait, nor borrowing from arguments.
#[async_trait]
impl SubgraphStore for MockStore {
    fn ens_lookup(&self) -> Arc<dyn EnsLookup> {
        unimplemented!()
    }

    fn create_subgraph_deployment(
        &self,
        _: SubgraphName,
        _: &Schema,
        _: SubgraphDeploymentEntity,
        _: NodeId,
        _: String,
        _: SubgraphVersionSwitchingMode,
    ) -> Result<DeploymentLocator, StoreError> {
        unimplemented!()
    }

    fn create_subgraph(&self, _: SubgraphName) -> Result<String, StoreError> {
        unimplemented!()
    }

    fn remove_subgraph(&self, _: SubgraphName) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn reassign_subgraph(&self, _: &DeploymentLocator, _: &NodeId) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn assigned_node(&self, _: &DeploymentLocator) -> Result<Option<NodeId>, StoreError> {
        unimplemented!()
    }

    fn assignments(&self, _: &NodeId) -> Result<Vec<DeploymentLocator>, StoreError> {
        unimplemented!()
    }

    fn subgraph_exists(&self, _: &SubgraphName) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn input_schema(&self, _: &DeploymentHash) -> Result<Arc<Schema>, StoreError> {
        unimplemented!()
    }

    fn api_schema(&self, _: &DeploymentHash) -> Result<Arc<ApiSchema>, StoreError> {
        unimplemented!()
    }

    async fn writable(
        self: Arc<Self>,
        _: Logger,
        _: DeploymentId,
    ) -> Result<Arc<dyn WritableStore>, StoreError> {
        Ok(Arc::new(MockStore::new()))
    }

    fn is_deployed(&self, _: &DeploymentHash) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn least_block_ptr(&self, _: &DeploymentHash) -> Result<Option<BlockPtr>, StoreError> {
        unimplemented!()
    }

    fn writable_for_network_indexer(
        &self,
        _: Logger,
        _: &DeploymentHash,
    ) -> Result<Arc<dyn WritableStore>, StoreError> {
        unimplemented!()
    }

    fn locators(&self, _: &str) -> Result<Vec<DeploymentLocator>, StoreError> {
        unimplemented!()
    }
}

// The store trait must be implemented manually because mockall does not support async_trait, nor borrowing from arguments.
#[async_trait]
impl WritableStore for MockStore {
    fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        unimplemented!()
    }

    fn block_cursor(&self) -> Result<Option<String>, StoreError> {
        unimplemented!()
    }

    fn start_subgraph_deployment(&self, _: &Logger) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn revert_block_operations(&self, _: BlockPtr) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn unfail_deterministic_error(&self, _: &BlockPtr, _: &BlockPtr) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn unfail_non_deterministic_error(&self, _: &BlockPtr) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn fail_subgraph(&self, _: SubgraphError) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn get(&self, _: &EntityKey) -> Result<Option<Entity>, StoreError> {
        unimplemented!()
    }

    fn transact_block_operations(
        &self,
        _: BlockPtr,
        _: Option<String>,
        _: Vec<EntityModification>,
        _: StopwatchMetrics,
        _: Vec<StoredDynamicDataSource>,
        _: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn get_many(
        &self,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        self.get_many_mock(ids_for_type)
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    async fn load_dynamic_data_sources(&self) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        unimplemented!()
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn shard(&self) -> &str {
        unimplemented!()
    }

    async fn health(&self, _: &DeploymentHash) -> Result<SubgraphHealth, StoreError> {
        unimplemented!()
    }

    fn input_schema(&self) -> Arc<Schema> {
        unimplemented!()
    }
}

pub trait BlockStore: Send + Sync + 'static {
    type ChainStore: ChainStore;

    fn chain_store(&self, network: &str) -> Option<Arc<Self::ChainStore>>;
}

/// Common trait for blockchain store implementations.
#[async_trait]
pub trait ChainStore: Send + Sync + 'static {
    /// Get a pointer to this blockchain's genesis block.
    fn genesis_block_ptr(&self) -> Result<BlockPtr, Error>;

    /// Insert a block into the store (or update if they are already present).
    async fn upsert_block(&self, block: Arc<dyn Block>) -> Result<(), Error>;

    fn upsert_light_blocks(&self, blocks: &[&dyn Block]) -> Result<(), Error>;

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
    async fn attempt_chain_head_update(
        self: Arc<Self>,
        ancestor_count: BlockNumber,
    ) -> Result<Option<H256>, Error>;

    /// Get the current head block pointer for this chain.
    /// Any changes to the head block pointer will be to a block with a larger block number, never
    /// to a block with a smaller or equal block number.
    ///
    /// The head block pointer will be None on initial set up.
    fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error>;

    /// Get the current head block cursor for this chain.
    ///
    /// The head block cursor will be None on initial set up.
    fn chain_head_cursor(&self) -> Result<Option<String>, Error>;

    /// This method does actually three operations:
    /// - Upserts received block into blocks table
    /// - Update chain head block into networks table
    /// - Update chain head cursor into networks table
    async fn set_chain_head(
        self: Arc<Self>,
        block: Arc<dyn Block>,
        cursor: String,
    ) -> Result<(), Error>;

    /// Returns the blocks present in the store.
    fn blocks(&self, hashes: &[H256]) -> Result<Vec<serde_json::Value>, Error>;

    /// Get the `offset`th ancestor of `block_hash`, where offset=0 means the block matching
    /// `block_hash` and offset=1 means its parent. Returns None if unable to complete due to
    /// missing blocks in the chain store.
    ///
    /// Returns an error if the offset would reach past the genesis block.
    fn ancestor_block(
        &self,
        block_ptr: BlockPtr,
        offset: BlockNumber,
    ) -> Result<Option<serde_json::Value>, Error>;

    /// Remove old blocks from the cache we maintain in the database and
    /// return a pair containing the number of the oldest block retained
    /// and the number of blocks deleted.
    /// We will never remove blocks that are within `ancestor_count` of
    /// the chain head.
    fn cleanup_cached_blocks(
        &self,
        ancestor_count: BlockNumber,
    ) -> Result<Option<(BlockNumber, usize)>, Error>;

    /// Return the hashes of all blocks with the given number
    fn block_hashes_by_block_number(&self, number: BlockNumber) -> Result<Vec<H256>, Error>;

    /// Confirm that block number `number` has hash `hash` and that the store
    /// may purge any other blocks with that number
    fn confirm_block_hash(&self, number: BlockNumber, hash: &H256) -> Result<usize, Error>;

    /// Find the block with `block_hash` and return the network name and number
    fn block_number(&self, block_hash: H256) -> Result<Option<(String, BlockNumber)>, StoreError>;

    /// Tries to retrieve all transactions receipts for a given block.
    async fn transaction_receipts_in_block(
        &self,
        block_ptr: &H256,
    ) -> Result<Vec<transaction_receipt::LightTransactionReceipt>, StoreError>;
}

pub trait EthereumCallCache: Send + Sync + 'static {
    /// Cached return value.
    fn get_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: BlockPtr,
    ) -> Result<Option<Vec<u8>>, Error>;

    // Add entry to the cache.
    fn set_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: BlockPtr,
        return_value: &[u8],
    ) -> Result<(), Error>;
}

/// Store operations used when serving queries for a specific deployment
#[async_trait]
pub trait QueryStore: Send + Sync {
    fn find_query_values(
        &self,
        query: EntityQuery,
    ) -> Result<Vec<BTreeMap<String, r::Value>>, QueryExecutionError>;

    async fn is_deployment_synced(&self) -> Result<bool, Error>;

    fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError>;

    fn block_number(&self, block_hash: H256) -> Result<Option<BlockNumber>, StoreError>;

    fn wait_stats(&self) -> PoolWaitStats;

    /// If `block` is `None`, assumes the latest block.
    async fn has_non_fatal_errors(&self, block: Option<BlockNumber>) -> Result<bool, StoreError>;

    /// Find the current state for the subgraph deployment `id` and
    /// return details about it needed for executing queries
    async fn deployment_state(&self) -> Result<DeploymentState, QueryExecutionError>;

    fn api_schema(&self) -> Result<Arc<ApiSchema>, QueryExecutionError>;

    fn network_name(&self) -> &str;

    /// A permit should be acquired before starting query execution.
    async fn query_permit(&self) -> tokio::sync::OwnedSemaphorePermit;
}

/// A view of the store that can provide information about the indexing status
/// of any subgraph and any deployment
#[async_trait]
pub trait StatusStore: Send + Sync + 'static {
    /// A permit should be acquired before starting query execution.
    async fn query_permit(&self) -> tokio::sync::OwnedSemaphorePermit;

    fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError>;

    /// Support for the explorer-specific API
    fn version_info(&self, version_id: &str) -> Result<VersionInfo, StoreError>;

    /// Support for the explorer-specific API; note that `subgraph_id` must be
    /// the id of an entry in `subgraphs.subgraph`, not that of a deployment.
    /// The return values are the ids of the `subgraphs.subgraph_version` for
    /// the current and pending versions of the subgraph
    fn versions_for_subgraph_id(
        &self,
        subgraph_id: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError>;

    /// Support for the explorer-specific API. Returns a vector of (name, version) of all
    /// subgraphs for a given deployment hash.
    fn subgraphs_for_deployment_hash(
        &self,
        deployment_hash: &str,
    ) -> Result<Vec<(String, String)>, StoreError>;

    /// A value of None indicates that the table is not available. Re-deploying
    /// the subgraph fixes this. It is undesirable to force everything to
    /// re-sync from scratch, so existing deployments will continue without a
    /// Proof of Indexing. Once all subgraphs have been re-deployed the Option
    /// can be removed.
    async fn get_proof_of_indexing(
        &self,
        subgraph_id: &DeploymentHash,
        indexer: &Option<Address>,
        block: BlockPtr,
    ) -> Result<Option<[u8; 32]>, StoreError>;
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

    pub fn is_remove(&self) -> bool {
        match self {
            EntityModification::Remove { .. } => true,
            _ => false,
        }
    }
}

/// A representation of entity operations that can be accumulated.
#[derive(Debug, Clone)]
enum EntityOp {
    Remove,
    Update(Entity),
    Overwrite(Entity),
}

impl EntityOp {
    fn apply_to(self, entity: Option<Entity>) -> Option<Entity> {
        use EntityOp::*;
        match (self, entity) {
            (Remove, _) => None,
            (Overwrite(new), _) | (Update(new), None) => Some(new),
            (Update(updates), Some(mut entity)) => {
                entity.merge_remove_null_fields(updates);
                Some(entity)
            }
        }
    }

    fn accumulate(&mut self, next: EntityOp) {
        use EntityOp::*;
        let update = match next {
            // Remove and Overwrite ignore the current value.
            Remove | Overwrite(_) => {
                *self = next;
                return;
            }
            Update(update) => update,
        };

        // We have an update, apply it.
        match self {
            // This is how `Overwrite` is constructed, by accumulating `Update` onto `Remove`.
            Remove => *self = Overwrite(update),
            Update(current) | Overwrite(current) => current.merge(update),
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
pub struct EntityCache {
    /// The state of entities in the store. An entry of `None`
    /// means that the entity is not present in the store
    current: LfuCache<EntityKey, Option<Entity>>,

    /// The accumulated changes to an entity.
    updates: HashMap<EntityKey, EntityOp>,

    // Updates for a currently executing handler.
    handler_updates: HashMap<EntityKey, EntityOp>,

    // Marks whether updates should go in `handler_updates`.
    in_handler: bool,

    data_sources: Vec<StoredDynamicDataSource>,

    /// The store is only used to read entities.
    pub store: Arc<dyn WritableStore>,
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
    pub data_sources: Vec<StoredDynamicDataSource>,
    pub entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}

impl EntityCache {
    pub fn new(store: Arc<dyn WritableStore>) -> Self {
        Self {
            current: LfuCache::new(),
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            data_sources: vec![],
            store,
        }
    }

    pub fn with_current(
        store: Arc<dyn WritableStore>,
        current: LfuCache<EntityKey, Option<Entity>>,
    ) -> EntityCache {
        EntityCache {
            current,
            updates: HashMap::new(),
            handler_updates: HashMap::new(),
            in_handler: false,
            data_sources: vec![],
            store,
        }
    }

    pub(crate) fn enter_handler(&mut self) {
        assert!(!self.in_handler);
        self.in_handler = true;
    }

    pub(crate) fn exit_handler(&mut self) {
        assert!(self.in_handler);
        self.in_handler = false;

        // Apply all handler updates to the main `updates`.
        let handler_updates = Vec::from_iter(self.handler_updates.drain());
        for (key, op) in handler_updates {
            self.entity_op(key, op)
        }
    }

    pub(crate) fn exit_handler_and_discard_changes(&mut self) {
        assert!(self.in_handler);
        self.in_handler = false;
        self.handler_updates.clear();
    }

    pub fn get(&mut self, key: &EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        // Get the current entity, apply any updates from `updates`, then from `handler_updates`.
        let mut entity = self.current.get_entity(&*self.store, key)?;
        if let Some(op) = self.updates.get(key).cloned() {
            entity = op.apply_to(entity)
        }
        if let Some(op) = self.handler_updates.get(key).cloned() {
            entity = op.apply_to(entity)
        }
        Ok(entity)
    }

    pub fn remove(&mut self, key: EntityKey) {
        self.entity_op(key, EntityOp::Remove);
    }

    pub fn set(&mut self, key: EntityKey, entity: Entity) {
        self.entity_op(key, EntityOp::Update(entity))
    }

    pub fn append(&mut self, operations: Vec<EntityOperation>) {
        assert!(!self.in_handler);

        for operation in operations {
            match operation {
                EntityOperation::Set { key, data } => {
                    self.entity_op(key, EntityOp::Update(data));
                }
                EntityOperation::Remove { key } => {
                    self.entity_op(key, EntityOp::Remove);
                }
            }
        }
    }

    /// Add a dynamic data source
    pub fn add_data_source<C: Blockchain>(&mut self, data_source: &impl DataSource<C>) {
        self.data_sources
            .push(data_source.as_stored_dynamic_data_source());
    }

    fn entity_op(&mut self, key: EntityKey, op: EntityOp) {
        use std::collections::hash_map::Entry;
        let updates = match self.in_handler {
            true => &mut self.handler_updates,
            false => &mut self.updates,
        };

        match updates.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(op);
            }
            Entry::Occupied(mut entry) => entry.get_mut().accumulate(op),
        }
    }

    pub(crate) fn extend(&mut self, other: EntityCache) {
        assert!(!other.in_handler);

        self.current.extend(other.current);
        for (key, op) in other.updates {
            self.entity_op(key, op);
        }
    }

    /// Return the changes that have been made via `set` and `remove` as
    /// `EntityModification`, making sure to only produce one when a change
    /// to the current state is actually needed.
    ///
    /// Also returns the updated `LfuCache`.
    pub fn as_modifications(mut self) -> Result<ModificationsAndCache, QueryExecutionError> {
        assert!(!self.in_handler);

        // The first step is to make sure all entities being set are in `self.current`.
        // For each subgraph, we need a map of entity type to missing entity ids.
        let missing = self
            .updates
            .keys()
            .filter(|key| !self.current.contains_key(key));

        let mut missing_by_subgraph: BTreeMap<_, BTreeMap<&EntityType, Vec<&str>>> =
            BTreeMap::new();
        for key in missing {
            missing_by_subgraph
                .entry(&key.subgraph_id)
                .or_default()
                .entry(&key.entity_type)
                .or_default()
                .push(&key.entity_id);
        }

        for (subgraph_id, keys) in missing_by_subgraph {
            for (entity_type, entities) in self.store.get_many(keys)? {
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
                (None, EntityOp::Update(updates)) | (None, EntityOp::Overwrite(updates)) => {
                    // Merging with an empty entity removes null fields.
                    let mut data = Entity::new();
                    data.merge_remove_null_fields(updates);
                    self.current.insert(key.clone(), Some(data.clone()));
                    Some(Insert { key, data })
                }
                // Entity may have been changed
                (Some(current), EntityOp::Update(updates)) => {
                    let mut data = current.clone();
                    data.merge_remove_null_fields(updates);
                    self.current.insert(key.clone(), Some(data.clone()));
                    if current != data {
                        Some(Overwrite { key, data })
                    } else {
                        None
                    }
                }
                // Entity was removed and then updated, so it will be overwritten
                (Some(current), EntityOp::Overwrite(data)) => {
                    self.current.insert(key.clone(), Some(data.clone()));
                    if current != data {
                        Some(Overwrite { key, data })
                    } else {
                        None
                    }
                }
                // Existing entity was deleted
                (Some(_), EntityOp::Remove) => {
                    self.current.insert(key.clone(), None);
                    Some(Remove { key })
                }
                // Entity was deleted, but it doesn't exist in the store
                (None, EntityOp::Remove) => None,
            };
            if let Some(modification) = modification {
                mods.push(modification)
            }
        }
        Ok(ModificationsAndCache {
            modifications: mods,
            data_sources: self.data_sources,
            entity_lfu_cache: self.current,
        })
    }
}

impl LfuCache<EntityKey, Option<Entity>> {
    // Helper for cached lookup of an entity.
    fn get_entity(
        &mut self,
        store: &(impl WritableStore + ?Sized),
        key: &EntityKey,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        match self.get(key) {
            None => {
                let mut entity = store.get(key)?;
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

/// Determines which columns should be selected in a table.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum AttributeNames {
    /// Select all columns. Equivalent to a `"SELECT *"`.
    All,
    /// Individual column names to be selected.
    Select(BTreeSet<String>),
}

impl AttributeNames {
    fn insert(&mut self, column_name: &str) {
        match self {
            AttributeNames::All => {
                let mut set = BTreeSet::new();
                set.insert(column_name.to_string());
                *self = AttributeNames::Select(set)
            }
            AttributeNames::Select(set) => {
                set.insert(column_name.to_string());
            }
        }
    }

    /// Adds a attribute name. Ignores meta fields.
    pub fn add(&mut self, field: &q::Field) {
        if Self::is_meta_field(&field.name) {
            return;
        }
        self.insert(&field.name)
    }

    /// Adds a attribute name. Ignores meta fields.
    pub fn add_str(&mut self, field_name: &str) {
        if Self::is_meta_field(field_name) {
            return;
        }
        self.insert(field_name);
    }

    /// Returns `true` for meta field names, `false` otherwise.
    fn is_meta_field(field_name: &str) -> bool {
        field_name.starts_with("__")
    }

    pub fn extend(&mut self, other: Self) {
        use AttributeNames::*;
        match (self, other) {
            (All, All) => {}
            (self_ @ All, other @ Select(_)) => *self_ = other,
            (Select(_), All) => {
                unreachable!()
            }
            (Select(a), Select(b)) => a.extend(b),
        }
    }
}
