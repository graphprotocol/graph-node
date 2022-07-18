mod cache;
mod err;
mod traits;

pub use cache::{CachedEthereumCall, EntityCache, ModificationsAndCache};
pub use err::StoreError;
use itertools::Itertools;
pub use traits::*;

use futures::stream::poll_fn;
use futures::{Async, Poll, Stream};
use graphql_parser::schema as s;
use serde::{Deserialize, Serialize};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::blockchain::DataSource;
use crate::blockchain::{Block, Blockchain};
use crate::data::store::scalar::Bytes;
use crate::data::store::*;
use crate::prelude::*;
use crate::util::stable_hash_glue::impl_stable_hash;

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

    pub fn is_poi(&self) -> bool {
        &self.0 == "Poi$"
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityFilterDerivative(bool);

impl EntityFilterDerivative {
    pub fn new(derived: bool) -> Self {
        Self(derived)
    }

    pub fn is_derived(&self) -> bool {
        self.0
    }
}

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

impl_stable_hash!(EntityKey {
    subgraph_id,
    entity_type: EntityType::as_str,
    entity_id
});

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
    use stable_hash_legacy::crypto::SetHasher;
    use stable_hash_legacy::utils::stable_hash;

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
#[derive(Clone, Debug, PartialEq)]
pub struct Child {
    pub attr: Attribute,
    pub entity_type: EntityType,
    pub filter: Box<EntityFilter>,
    pub derived: bool,
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
    ContainsNoCase(Attribute, Value),
    NotContains(Attribute, Value),
    NotContainsNoCase(Attribute, Value),
    StartsWith(Attribute, Value),
    StartsWithNoCase(Attribute, Value),
    NotStartsWith(Attribute, Value),
    NotStartsWithNoCase(Attribute, Value),
    EndsWith(Attribute, Value),
    EndsWithNoCase(Attribute, Value),
    NotEndsWith(Attribute, Value),
    NotEndsWithNoCase(Attribute, Value),
    ChangeBlockGte(BlockNumber),
    Child(Child),
}

// A somewhat concise string representation of a filter
impl fmt::Display for EntityFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use EntityFilter::*;

        match self {
            And(fs) => {
                write!(f, "{}", fs.iter().map(|f| f.to_string()).join(" and "))
            }
            Or(fs) => {
                write!(f, "{}", fs.iter().map(|f| f.to_string()).join(" or "))
            }
            Equal(a, v) => write!(f, "{a} = {v}"),
            Not(a, v) => write!(f, "{a} != {v}"),
            GreaterThan(a, v) => write!(f, "{a} > {v}"),
            LessThan(a, v) => write!(f, "{a} < {v}"),
            GreaterOrEqual(a, v) => write!(f, "{a} >= {v}"),
            LessOrEqual(a, v) => write!(f, "{a} <= {v}"),
            In(a, vs) => write!(
                f,
                "{a} in ({})",
                vs.into_iter().map(|v| v.to_string()).join(",")
            ),
            NotIn(a, vs) => write!(
                f,
                "{a} not in ({})",
                vs.into_iter().map(|v| v.to_string()).join(",")
            ),
            Contains(a, v) => write!(f, "{a} ~ *{v}*"),
            ContainsNoCase(a, v) => write!(f, "{a} ~ *{v}*i"),
            NotContains(a, v) => write!(f, "{a} !~ *{v}*"),
            NotContainsNoCase(a, v) => write!(f, "{a} !~ *{v}*i"),
            StartsWith(a, v) => write!(f, "{a} ~ ^{v}*"),
            StartsWithNoCase(a, v) => write!(f, "{a} ~ ^{v}*i"),
            NotStartsWith(a, v) => write!(f, "{a} !~ ^{v}*"),
            NotStartsWithNoCase(a, v) => write!(f, "{a} !~ ^{v}*i"),
            EndsWith(a, v) => write!(f, "{a} ~ *{v}$"),
            EndsWithNoCase(a, v) => write!(f, "{a} ~ *{v}$i"),
            NotEndsWith(a, v) => write!(f, "{a} !~ *{v}$"),
            NotEndsWithNoCase(a, v) => write!(f, "{a} !~ *{v}$i"),
            ChangeBlockGte(b) => write!(f, "block >= {b}"),
            Child(child /* a, et, cf, _ */) => write!(
                f,
                "join on {} with {}({})",
                child.attr,
                child.entity_type,
                child.filter.to_string()
            ),
        }
    }
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
    Parent(EntityType, ParentLink),
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

#[derive(Debug, PartialEq)]
pub enum UnfailOutcome {
    Noop,
    Unfailed,
}

#[derive(Clone)]
pub struct StoredDynamicDataSource {
    pub name: String,
    pub param: Option<Bytes>,
    pub context: Option<serde_json::Value>,
    pub creation_block: Option<BlockNumber>,
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

// The type that the connection pool uses to track wait times for
// connection checkouts
pub type PoolWaitStats = Arc<RwLock<MovingStats>>;

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

    pub fn entity(&self) -> Option<&Entity> {
        match self {
            EntityModification::Insert { data, .. }
            | EntityModification::Overwrite { data, .. } => Some(data),
            EntityModification::Remove { .. } => None,
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

    pub fn update(&mut self, field_name: &str) {
        if Self::is_meta_field(field_name) {
            return;
        }
        self.insert(field_name)
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

#[derive(Debug, Clone)]
pub struct PartialBlockPtr {
    pub number: BlockNumber,
    pub hash: Option<BlockHash>,
}

impl From<BlockNumber> for PartialBlockPtr {
    fn from(number: BlockNumber) -> Self {
        Self { number, hash: None }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeploymentSchemaVersion {
    /// V0, baseline version, in which:
    /// - A relational schema is used.
    /// - Each deployment has its own namespace for entity tables.
    /// - Dynamic data sources are stored in `subgraphs.dynamic_ethereum_contract_data_source`.
    V0 = 0,

    /// V1: Dynamic data sources moved to `sgd*.data_sources$`.
    V1 = 1,
}

impl DeploymentSchemaVersion {
    // Latest schema version supported by this version of graph node.
    pub const LATEST: Self = Self::V0;

    pub fn private_data_sources(self) -> bool {
        use DeploymentSchemaVersion::*;
        match self {
            V0 => false,
            V1 => true,
        }
    }
}

impl TryFrom<i32> for DeploymentSchemaVersion {
    type Error = StoreError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::V0),
            1 => Ok(Self::V1),
            _ => Err(StoreError::UnsupportedDeploymentSchemaVersion(value)),
        }
    }
}

impl fmt::Display for DeploymentSchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&(*self as i32), f)
    }
}
