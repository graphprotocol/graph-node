use failure::Error;
use futures::stream::poll_fn;
use futures::{Async, Future, Poll, Stream};
use std::collections::HashSet;
use std::env;
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use web3::types::H256;

use crate::data::store::*;
use crate::data::subgraph::schema::*;
use crate::prelude::*;

lazy_static! {
    pub static ref SUBSCRIPTION_THROTTLE_INTERVAL: Duration =
        env::var("SUBSCRIPTION_THROTTLE_INTERVAL")
            .ok()
            .map(|s| u64::from_str(&s).unwrap_or_else(|_| panic!(
                "failed to parse env var SUBSCRIPTION_THROTTLE_INTERVAL"
            )))
            .map(|millis| Duration::from_millis(millis))
            .unwrap_or(Duration::from_millis(1000));
}

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

/// A query for entities in a store.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityQuery {
    /// ID of the subgraph.
    pub subgraph_id: SubgraphDeploymentId,

    /// The names of the entity types being queried. The result is the union
    /// (with repetition) of the query for each entity.
    pub entity_types: Vec<String>,

    /// Filter to filter entities by.
    pub filter: Option<EntityFilter>,

    /// An optional attribute to order the entities by.
    pub order_by: Option<(String, ValueType)>,

    /// The direction to order entities in.
    pub order_direction: Option<EntityOrder>,

    /// A range to limit the size of the result.
    pub range: EntityRange,
}

impl EntityQuery {
    pub fn new(
        subgraph_id: SubgraphDeploymentId,
        entity_types: Vec<String>,
        range: EntityRange,
    ) -> Self {
        EntityQuery {
            subgraph_id,
            entity_types,
            filter: None,
            order_by: None,
            order_direction: None,
            range,
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
        self.range = range;
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

    /// Convert an `EntityOperation` into the corresponding `EntityChange`.
    /// `Set` and `Update` operations are mapped to that `EntityChange`.
    /// `AbortUnless` operations are mapped to `None`, as they do not represent
    ///  a change to any entity.
    pub fn from_entity_operation(operation: EntityOperation) -> Option<Self> {
        use self::EntityOperation::*;

        match operation {
            Set { key, .. } | Update { key, .. } => {
                Some(Self::from_key(key, EntityChangeOperation::Set))
            }
            Remove { key } => Some(Self::from_key(key, EntityChangeOperation::Removed)),
            // AbortUnless is uninteresting for when we need an EntityChange
            AbortUnless { .. } => None,
        }
    }

    pub fn subgraph_entity_pair(&self) -> SubgraphEntityPair {
        (self.subgraph_id.clone(), self.entity_type.clone())
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
pub type StoreEventStreamBox = StoreEventStream<Box<Stream<Item = StoreEvent, Error = ()> + Send>>;

impl<S> Stream for StoreEventStream<S>
where
    S: Stream<Item = StoreEvent, Error = ()> + Send,
{
    type Item = StoreEvent;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.source.poll()
    }
}

impl<S> StoreEventStream<S>
where
    S: Stream<Item = StoreEvent, Error = ()> + Send + 'static,
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
        store: Arc<impl Store>,
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
        let check_synced = |store: &Store, deployment: &SubgraphDeploymentId| {
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
        let mut delay = tokio_timer::Delay::new(Instant::now() + interval);
        let logger = logger.clone();

        let source = Box::new(poll_fn(move || -> Poll<Option<StoreEvent>, ()> {
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
            let should_send = match delay.poll() {
                Ok(Async::NotReady) => false,
                // Timer errors are harmless. Treat them as if the timer had
                // become ready.
                Ok(Async::Ready(())) | Err(_) => {
                    delay = tokio_timer::Delay::new(Instant::now() + interval);
                    true
                }
            };

            // Get as many events as we can off of the source stream
            loop {
                match source.poll() {
                    Ok(Async::NotReady) => {
                        if should_send && pending_event.is_some() {
                            return Ok(Async::Ready(pending_event.take()));
                        } else {
                            return Ok(Async::NotReady);
                        }
                    }
                    Ok(Async::Ready(None)) => {
                        return Ok(Async::Ready(pending_event.take()));
                    }
                    Ok(Async::Ready(Some(event))) => {
                        StoreEvent::accumulate(&logger, &mut pending_event, event);
                    }
                    Err(()) => {
                        // Before we report the error, deliver what we have accumulated so far.
                        // We will report the error the next time poll() is called
                        if pending_event.is_some() {
                            had_err = true;
                            return Ok(Async::Ready(pending_event.take()));
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

    /// Aborts and rolls back the transaction unless `query` returns entities exactly matching
    /// `entity_ids`.  The equality test is only sensitive to the order of the results if `query`
    /// contains an `order_by`.
    AbortUnless {
        description: String, // Programmer-friendly debug message to explain reason for abort
        query: EntityQuery,  // The query to run
        entity_ids: Vec<String>, // What entities the query should return
    },

    /// Update an entity. The `data` should only contain the attributes that
    /// need to be changed, not the entire entity. The update will only happen
    /// if the given entity matches `guard` when the update is made. `Update`
    /// provides a way to atomically do a check-and-set change to an entity.
    /// This operation can currently only be performed on the subgraph of
    /// subgraphs
    Update {
        key: EntityKey,
        data: Entity,
        guard: Option<EntityFilter>,
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
            Set { ref key, .. } | Update { ref key, .. } => key,
            Remove { ref key } => key,
            AbortUnless { .. } => panic!("cannot get entity key from AbortUnless entity operation"),
        }
    }

    /// Return the subgraph that this operation applies to
    pub fn subgraph(&self) -> Option<&SubgraphDeploymentId> {
        use self::EntityOperation::*;

        match self {
            Set { key, .. } | Update { key, .. } | Remove { key } => Some(&key.subgraph_id),
            AbortUnless { .. } => None,
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
    pub fn apply(&self, entity: Option<Entity>) -> Result<Option<Entity>, Error> {
        use self::EntityOperation::*;

        match self {
            Set { data, .. } | Update { data, .. } => {
                let mut entity = entity.unwrap_or(Entity::new());
                entity.merge(data.clone());
                Ok(Some(entity))
            }
            Remove { .. } => Ok(None),
            AbortUnless { .. } => Err(format_err!(
                "Cannot apply AbortUnless entity operation to an entity"
            )),
        }
    }

    /// Applies all entity operations to the given entity in order.
    /// `ops` must not contain any `AbortUnless` operations.
    pub fn apply_all(
        entity: Option<Entity>,
        ops: &[&EntityOperation],
    ) -> Result<Option<Entity>, Error> {
        use self::EntityOperation::*;

        // Only continue if all operations are Set/Remove.
        ops.iter().try_for_each(|op| match op {
            Set { .. } | Remove { .. } | Update { .. } => Ok(()),
            AbortUnless { .. } => Err(format_err!("Cannot apply {:?} to an Entity", op)),
        })?;

        // If there is a remove operations, we only need to consider the operations after that
        ops.iter()
            .rev()
            .take_while(|op| !op.is_remove())
            .collect::<Vec<_>>()
            .iter()
            .rev()
            .try_fold(entity, |entity, op| op.apply(entity))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEvent {
    pub id: i32,
    pub subgraph: SubgraphDeploymentId,
    pub source: EventSource,
}

impl HistoryEvent {
    pub fn to_event_source_string(event: &Option<&HistoryEvent>) -> String {
        event.map_or(String::from("none"), |event| event.source.to_string())
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

    /// Build indexes for a set of subgraph entity attributes
    fn build_entity_attribute_indexes(
        &self,
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
    /// `EntityOperation`s, which will abort the transaction if the version
    /// summaries are out of date by the time the entity operations are applied.
    fn read_subgraph_version_summaries(
        &self,
        deployment_ids: Vec<SubgraphDeploymentId>,
    ) -> Result<(Vec<SubgraphVersionSummary>, Vec<EntityOperation>), Error> {
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
        ops.push(EntityOperation::AbortUnless {
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
        ops.push(EntityOperation::AbortUnless {
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

    /// Produce the EntityOperations needed to create/remove
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
    ) -> Vec<EntityOperation> {
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

        ops.extend(
            removed_assignments
                .into_iter()
                .map(|deployment_id| EntityOperation::Remove {
                    key: SubgraphDeploymentAssignmentEntity::key(deployment_id),
                }),
        );
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
        subgraph_id: &SubgraphDeploymentId,
        ops: Vec<EntityOperation>,
    ) -> Result<(), StoreError>;
}

pub trait SubgraphDeploymentStore: Send + Sync + 'static {
    fn subgraph_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error>;
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
    fn chain_head_updates(&self) -> ChainHeadUpdateStream;

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
