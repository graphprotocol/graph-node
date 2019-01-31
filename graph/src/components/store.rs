use failure::Error;
use futures::Future;
use futures::Stream;
use std::collections::HashSet;
use std::fmt;
use web3::types::H256;

use data::store::*;
use data::subgraph::schema::*;
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
    pub fn apply(&self, entity: Option<Entity>) -> Result<Option<Entity>, Error> {
        use self::EntityOperation::*;

        match self {
            Set { data, .. } => {
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
            Set { .. } | Remove { .. } => Ok(()),
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

#[derive(Clone, Debug)]
pub struct AttributeIndexDefinition {
    pub subgraph_id: SubgraphDeploymentId,
    pub index_name: String,
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

    /// Subscribe to entity changes for specific subgraphs and entities.
    ///
    /// Returns a stream of entity changes that match the input arguments.
    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> EntityChangeStream;

    /// Counts the total number of entities in a subgraph.
    fn count_entities(&self, subgraph: SubgraphDeploymentId) -> Result<u64, Error>;

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
        let current_version_id = match subgraph_entity
            .get("currentVersion")
            .ok_or_else(|| format_err!("Subgraph entity without `currentVersion`"))?
        {
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
                removed_assignments.len(),
                removed_assignments
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if removed_assignments.is_empty() && added_assignments.is_empty() {
            debug!(
                logger,
                "No subgraph node assignment additions/removals needed"
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
    fn is_deployment_synced(&self, id: &SubgraphDeploymentId) -> Result<bool, Error> {
        let filter = EntityFilter::new_equal("id", id.to_string());
        let entity = self.find_one(SubgraphDeploymentEntity::query().filter(filter))?;
        entity
            .map(|entity| match entity.get("synced") {
                Some(Value::Bool(true)) => Ok(true),
                _ => Ok(false),
            })
            .unwrap_or(Ok(false))
    }
}

pub trait SubgraphDeploymentStore: Send + Sync + 'static {
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
