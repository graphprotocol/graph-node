use detail::DeploymentDetail;
use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use graph::components::store::{EntityType, StoredDynamicDataSource};
use graph::data::subgraph::status;
use graph::prelude::{
    tokio, CancelHandle, CancelToken, CancelableError, PoolWaitStats, SubgraphDeploymentEntity,
};
use lru_time_cache::LruCache;
use rand::{seq::SliceRandom, thread_rng};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::convert::Into;
use std::convert::TryInto;
use std::env;
use std::iter::FromIterator;
use std::ops::Bound;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use graph::components::store::EntityCollection;
use graph::components::subgraph::ProofOfIndexingFinisher;
use graph::constraint_violation;
use graph::data::subgraph::schema::{SubgraphError, POI_OBJECT};
use graph::prelude::{
    anyhow, debug, info, lazy_static, o, warn, web3, ApiSchema, AttributeNames, BlockNumber,
    BlockPtr, CheapClone, DeploymentHash, DeploymentState, Entity, EntityKey, EntityModification,
    EntityQuery, Error, Logger, QueryExecutionError, Schema, StopwatchMetrics, StoreError,
    StoreEvent, Value, BLOCK_NUMBER_MAX,
};
use graph_graphql::prelude::api_schema;
use web3::types::Address;

use crate::block_range::block_number;
use crate::catalog;
use crate::deployment;
use crate::relational::{Layout, LayoutCache};
use crate::relational_queries::FromEntityData;
use crate::{connection_pool::ConnectionPool, detail};
use crate::{dynds, primary::Site};

lazy_static! {
    /// `GRAPH_QUERY_STATS_REFRESH_INTERVAL` is how long statistics that
    /// influence query execution are cached in memory (in seconds) before
    /// they are reloaded from the database. Defaults to 300s (5 minutes).
    static ref STATS_REFRESH_INTERVAL: Duration = {
        env::var("GRAPH_QUERY_STATS_REFRESH_INTERVAL")
        .ok()
        .map(|s| {
            let secs = u64::from_str(&s).unwrap_or_else(|_| {
                panic!("GRAPH_QUERY_STATS_REFRESH_INTERVAL must be a number, but is `{}`", s)
            });
            Duration::from_secs(secs)
        }).unwrap_or(Duration::from_secs(300))
    };
}

/// When connected to read replicas, this allows choosing which DB server to use for an operation.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ReplicaId {
    /// The main server has write and read access.
    Main,

    /// A read replica identified by its index.
    ReadOnly(usize),
}

/// Commonly needed information about a subgraph that we cache in
/// `Store.subgraph_cache`. Only immutable subgraph data can be cached this
/// way as the cache lives for the lifetime of the `Store` object
#[derive(Clone)]
pub(crate) struct SubgraphInfo {
    /// The schema as supplied by the user
    pub(crate) input: Arc<Schema>,
    /// The schema we derive from `input` with `graphql::schema::api::api_schema`
    pub(crate) api: Arc<ApiSchema>,
    /// The block number at which this subgraph was grafted onto
    /// another one. We do not allow reverting past this block
    pub(crate) graft_block: Option<BlockNumber>,
    pub(crate) description: Option<String>,
    pub(crate) repository: Option<String>,
}

pub struct StoreInner {
    logger: Logger,

    pool: ConnectionPool,
    read_only_pools: Vec<ConnectionPool>,

    /// A list of the available replicas set up such that when we run
    /// through the list once, we picked each replica according to its
    /// desired weight. Each replica can appear multiple times in the list
    replica_order: Vec<ReplicaId>,
    /// The current position in `replica_order` so we know which one to
    /// pick next
    conn_round_robin_counter: AtomicUsize,

    /// A cache of commonly needed data about a subgraph.
    subgraph_cache: Mutex<LruCache<DeploymentHash, SubgraphInfo>>,

    /// A cache for the layout metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) layout_cache: LayoutCache,
}

/// Storage of the data for individual deployments. Each `DeploymentStore`
/// corresponds to one of the database shards that `SubgraphStore` manages.
#[derive(Clone)]
pub struct DeploymentStore(Arc<StoreInner>);

impl CheapClone for DeploymentStore {}

impl Deref for DeploymentStore {
    type Target = StoreInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DeploymentStore {
    pub fn new(
        logger: &Logger,
        pool: ConnectionPool,
        read_only_pools: Vec<ConnectionPool>,
        mut pool_weights: Vec<usize>,
    ) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Create a list of replicas with repetitions according to the weights
        // and shuffle the resulting list. Any missing weights in the list
        // default to 1
        pool_weights.resize(read_only_pools.len() + 1, 1);
        let mut replica_order: Vec<_> = pool_weights
            .iter()
            .enumerate()
            .map(|(i, weight)| {
                let replica = if i == 0 {
                    ReplicaId::Main
                } else {
                    ReplicaId::ReadOnly(i - 1)
                };
                vec![replica; *weight]
            })
            .flatten()
            .collect();
        let mut rng = thread_rng();
        replica_order.shuffle(&mut rng);
        debug!(logger, "Using postgres host order {:?}", replica_order);

        // Create the store
        let store = StoreInner {
            logger: logger.clone(),
            pool,
            read_only_pools,
            replica_order,
            conn_round_robin_counter: AtomicUsize::new(0),
            subgraph_cache: Mutex::new(LruCache::with_capacity(100)),
            layout_cache: LayoutCache::new(*STATS_REFRESH_INTERVAL),
        };
        let store = DeploymentStore(Arc::new(store));

        // Return the store
        store
    }

    pub(crate) fn create_deployment(
        &self,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        site: Arc<Site>,
        graft_base: Option<Arc<Layout>>,
        replace: bool,
    ) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| -> Result<_, StoreError> {
            let exists = deployment::exists(&conn, &site)?;

            // Create (or update) the metadata. Update only happens in tests
            if replace || !exists {
                deployment::create_deployment(
                    &conn,
                    &site,
                    deployment,
                    exists,
                    replace,
                )?;
            };

            // Create the schema for the subgraph data
            if !exists {
                let query = format!("create schema {}", &site.namespace);
                conn.batch_execute(&query)?;

                let layout = Layout::create_relational_schema(&conn, site.clone(), schema)?;
                // See if we are grafting and check that the graft is permissible
                if let Some(base) = graft_base {
                    let errors = layout.can_copy_from(&base);
                    if !errors.is_empty() {
                        return Err(StoreError::Unknown(anyhow!(
                            "The subgraph `{}` cannot be used as the graft base \
                                                    for `{}` because the schemas are incompatible:\n    - {}",
                            &base.catalog.site.namespace,
                            &layout.catalog.site.namespace,
                            errors.join("\n    - ")
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    pub(crate) fn load_deployment(
        &self,
        site: &Site,
    ) -> Result<SubgraphDeploymentEntity, StoreError> {
        let conn = self.get_conn()?;
        detail::deployment_entity(&conn, site)
    }

    // Remove the data and metadata for the deployment `site`. This operation
    // is not reversible
    pub(crate) fn drop_deployment(&self, site: &Site) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| {
            crate::deployment::drop_schema(&conn, &site.namespace)?;
            crate::dynds::drop(&conn, &site.deployment)?;
            crate::deployment::drop_metadata(&conn, site)
        })
    }

    pub(crate) fn execute_query<T: FromEntityData>(
        &self,
        conn: &PgConnection,
        site: Arc<Site>,
        query: EntityQuery,
    ) -> Result<Vec<T>, QueryExecutionError> {
        let layout = self.layout(conn, site)?;

        let logger = query.logger.unwrap_or(self.logger.clone());
        layout.query(
            &logger,
            conn,
            query.collection,
            query.filter,
            query.order,
            query.range,
            query.block,
            query.query_id,
        )
    }

    fn check_interface_entity_uniqueness(
        &self,
        conn: &PgConnection,
        layout: &Layout,
        key: &EntityKey,
    ) -> Result<(), StoreError> {
        assert_eq!(&key.subgraph_id, &layout.site.deployment);

        // Collect all types that share an interface implementation with this
        // entity type, and make sure there are no conflicting IDs.
        //
        // To understand why this is necessary, suppose that `Dog` and `Cat` are
        // types and both implement an interface `Pet`, and both have instances
        // with `id: "Fred"`. If a type `PetOwner` has a field `pets: [Pet]`
        // then with the value `pets: ["Fred"]`, there's no way to disambiguate
        // if that's Fred the Dog, Fred the Cat or both.
        //
        // This assumes that there are no concurrent writes to a subgraph.
        let schema = self.subgraph_info_with_conn(&conn, &layout.site)?.api;
        let types_for_interface = schema.types_for_interface();
        let entity_type = key.entity_type.to_string();
        let types_with_shared_interface = Vec::from_iter(
            schema
                .interfaces_for_type(&key.entity_type)
                .into_iter()
                .flatten()
                .map(|interface| &types_for_interface[&interface.into()])
                .flatten()
                .map(EntityType::from)
                .filter(|type_name| type_name != &key.entity_type),
        );

        if !types_with_shared_interface.is_empty() {
            if let Some(conflicting_entity) =
                layout.conflicting_entity(conn, &key.entity_id, types_with_shared_interface)?
            {
                return Err(StoreError::ConflictingId(
                    entity_type.clone(),
                    key.entity_id.clone(),
                    conflicting_entity,
                ));
            }
        }
        Ok(())
    }

    fn apply_entity_modifications(
        &self,
        conn: &PgConnection,
        layout: &Layout,
        mods: &[EntityModification],
        ptr: &BlockPtr,
        stopwatch: StopwatchMetrics,
    ) -> Result<i32, StoreError> {
        use EntityModification::*;
        let mut count = 0;

        // Group `Insert`s and `Overwrite`s by key, and accumulate `Remove`s.
        let mut inserts = HashMap::new();
        let mut overwrites = HashMap::new();
        let mut removals = HashMap::new();
        for modification in mods.into_iter() {
            match modification {
                Insert { key, data } => {
                    inserts
                        .entry(key.entity_type.clone())
                        .or_insert_with(Vec::new)
                        .push((key, Cow::from(data)));
                }
                Overwrite { key, data } => {
                    overwrites
                        .entry(key.entity_type.clone())
                        .or_insert_with(Vec::new)
                        .push((key, Cow::from(data)));
                }
                Remove { key } => {
                    removals
                        .entry(key.entity_type.clone())
                        .or_insert_with(Vec::new)
                        .push(key.entity_id.as_str());
                }
            }
        }

        // Apply modification groups.
        // Inserts:
        for (entity_type, mut entities) in inserts.into_iter() {
            count +=
                self.insert_entities(&entity_type, &mut entities, conn, layout, ptr, &stopwatch)?
                    as i32
        }

        // Overwrites:
        for (entity_type, mut entities) in overwrites.into_iter() {
            // we do not update the count since the number of entities remains the same
            self.overwrite_entities(&entity_type, &mut entities, conn, layout, ptr, &stopwatch)?;
        }

        // Removals
        for (entity_type, entity_keys) in removals.into_iter() {
            count -= self.remove_entities(
                &entity_type,
                entity_keys.as_slice(),
                conn,
                layout,
                ptr,
                &stopwatch,
            )? as i32;
        }
        Ok(count)
    }

    fn insert_entities<'a>(
        &'a self,
        entity_type: &'a EntityType,
        data: &'a mut [(&'a EntityKey, Cow<'a, Entity>)],
        conn: &PgConnection,
        layout: &'a Layout,
        ptr: &BlockPtr,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let section = stopwatch.start_section("check_interface_entity_uniqueness");
        for (key, _) in data.iter() {
            // WARNING: This will potentially execute 2 queries for each entity key.
            self.check_interface_entity_uniqueness(conn, layout, key)?;
        }
        section.end();

        let _section = stopwatch.start_section("apply_entity_modifications_insert");
        layout.insert(conn, entity_type, data, block_number(ptr), stopwatch)
    }

    fn overwrite_entities<'a>(
        &'a self,
        entity_type: &'a EntityType,
        data: &'a mut [(&'a EntityKey, Cow<'a, Entity>)],
        conn: &PgConnection,
        layout: &'a Layout,
        ptr: &BlockPtr,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let section = stopwatch.start_section("check_interface_entity_uniqueness");
        for (key, _) in data.iter() {
            // WARNING: This will potentially execute 2 queries for each entity key.
            self.check_interface_entity_uniqueness(conn, layout, key)?;
        }
        section.end();

        let _section = stopwatch.start_section("apply_entity_modifications_update");
        layout.update(conn, &entity_type, data, block_number(ptr), stopwatch)
    }

    fn remove_entities(
        &self,
        entity_type: &EntityType,
        entity_keys: &[&str],
        conn: &PgConnection,
        layout: &Layout,
        ptr: &BlockPtr,
        stopwatch: &StopwatchMetrics,
    ) -> Result<usize, StoreError> {
        let _section = stopwatch.start_section("apply_entity_modifications_delete");
        layout
            .delete(
                conn,
                entity_type,
                &entity_keys,
                block_number(ptr),
                stopwatch,
            )
            .map_err(|_error| anyhow!("Failed to remove entities: {:?}", entity_keys).into())
    }

    /// Execute a closure with a connection to the database.
    ///
    /// # API
    ///   The API of using a closure to bound the usage of the connection serves several
    ///   purposes:
    ///
    ///   * Moves blocking database access out of the `Future::poll`. Within
    ///     `Future::poll` (which includes all `async` methods) it is illegal to
    ///     perform a blocking operation. This includes all accesses to the
    ///     database, acquiring of locks, etc. Calling a blocking operation can
    ///     cause problems with `Future` combinators (including but not limited
    ///     to select, timeout, and FuturesUnordered) and problems with
    ///     executors/runtimes. This method moves the database work onto another
    ///     thread in a way which does not block `Future::poll`.
    ///
    ///   * Limit the total number of connections. Because the supplied closure
    ///     takes a reference, we know the scope of the usage of all entity
    ///     connections and can limit their use in a non-blocking way.
    ///
    /// # Cancellation
    ///   The normal pattern for futures in Rust is drop to cancel. Once we
    ///   spawn the database work in a thread though, this expectation no longer
    ///   holds because the spawned task is the independent of this future. So,
    ///   this method provides a cancel token which indicates that the `Future`
    ///   has been dropped. This isn't *quite* as good as drop on cancel,
    ///   because a drop on cancel can do things like cancel http requests that
    ///   are in flight, but checking for cancel periodically is a significant
    ///   improvement.
    ///
    ///   The implementation of the supplied closure should check for cancel
    ///   between every operation that is potentially blocking. This includes
    ///   any method which may interact with the database. The check can be
    ///   conveniently written as `token.check_cancel()?;`. It is low overhead
    ///   to check for cancel, so when in doubt it is better to have too many
    ///   checks than too few.
    ///
    /// # Panics:
    ///   * This task will panic if the supplied closure panics
    ///   * This task will panic if the supplied closure returns Err(Cancelled)
    ///     when the supplied cancel token is not cancelled.
    pub(crate) async fn with_conn<T: Send + 'static>(
        &self,
        f: impl 'static
            + Send
            + FnOnce(
                &PooledConnection<ConnectionManager<PgConnection>>,
                &CancelHandle,
            ) -> Result<T, CancelableError<StoreError>>,
    ) -> Result<T, StoreError> {
        self.pool.with_conn(f).await
    }

    /// Deprecated. Use `with_conn` instead.
    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, StoreError> {
        self.pool.get()
    }

    /// Panics if `idx` is not a valid index for a read only pool.
    fn read_only_conn(
        &self,
        idx: usize,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        self.read_only_pools[idx].get().map_err(Error::from)
    }

    pub(crate) fn get_replica_conn(
        &self,
        replica: ReplicaId,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        let conn = match replica {
            ReplicaId::Main => self.get_conn()?,
            ReplicaId::ReadOnly(idx) => self.read_only_conn(idx)?,
        };
        Ok(conn)
    }

    pub(crate) async fn query_permit(
        &self,
        replica: ReplicaId,
    ) -> tokio::sync::OwnedSemaphorePermit {
        let pool = match replica {
            ReplicaId::Main => &self.pool,
            ReplicaId::ReadOnly(idx) => &self.read_only_pools[idx],
        };
        pool.query_permit().await
    }

    pub(crate) fn wait_stats(&self, replica: ReplicaId) -> PoolWaitStats {
        match replica {
            ReplicaId::Main => self.pool.wait_stats(),
            ReplicaId::ReadOnly(idx) => self.read_only_pools[idx].wait_stats(),
        }
    }

    /// Return the layout for a deployment. Since constructing a `Layout`
    /// object takes a bit of computation, we cache layout objects that do
    /// not have a pending migration in the Store, i.e., for the lifetime of
    /// the Store. Layout objects with a pending migration can not be
    /// cached for longer than a transaction since they might change
    /// without us knowing
    pub(crate) fn layout(
        &self,
        conn: &PgConnection,
        site: Arc<Site>,
    ) -> Result<Arc<Layout>, StoreError> {
        self.layout_cache.get(&self.logger, conn, site)
    }

    /// Return the layout for a deployment. This might use a database
    /// connection for the lookup and should only be called if the caller
    /// does not have a connection currently. If it does, use `layout`
    pub(crate) fn find_layout(&self, site: Arc<Site>) -> Result<Arc<Layout>, StoreError> {
        if let Some(layout) = self.layout_cache.find(site.as_ref()) {
            return Ok(layout.clone());
        }

        let conn = self.get_conn()?;
        self.layout(&conn, site)
    }

    fn subgraph_info_with_conn(
        &self,
        conn: &PgConnection,
        site: &Site,
    ) -> Result<SubgraphInfo, StoreError> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&site.deployment) {
            return Ok(info.clone());
        }

        let (input_schema, description, repository) = deployment::manifest_info(&conn, site)?;

        let graft_block =
            deployment::graft_point(&conn, &site.deployment)?.map(|(_, ptr)| ptr.number as i32);

        // Generate an API schema for the subgraph and make sure all types in the
        // API schema have a @subgraphId directive as well
        let mut schema = input_schema.clone();
        schema.document =
            api_schema(&schema.document).map_err(|e| StoreError::Unknown(e.into()))?;
        schema.add_subgraph_id_directives(site.deployment.clone());

        let info = SubgraphInfo {
            input: Arc::new(input_schema),
            api: Arc::new(ApiSchema::from_api_schema(schema)?),
            graft_block,
            description,
            repository,
        };

        // Insert the schema into the cache.
        let mut cache = self.subgraph_cache.lock().unwrap();
        cache.insert(site.deployment.clone(), info);

        Ok(cache.get(&site.deployment).unwrap().clone())
    }

    pub(crate) fn subgraph_info(&self, site: &Site) -> Result<SubgraphInfo, StoreError> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&site.deployment) {
            return Ok(info.clone());
        }

        let conn = self.get_conn()?;
        self.subgraph_info_with_conn(&conn, site)
    }

    fn block_ptr_with_conn(
        subgraph_id: &DeploymentHash,
        conn: &PgConnection,
    ) -> Result<Option<BlockPtr>, StoreError> {
        deployment::block_ptr(&conn, subgraph_id)
    }

    pub(crate) fn deployment_details(
        &self,
        ids: Vec<String>,
    ) -> Result<Vec<DeploymentDetail>, StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| -> Result<_, StoreError> { detail::deployment_details(&conn, ids) })
    }

    pub(crate) fn deployment_statuses(
        &self,
        sites: &Vec<Arc<Site>>,
    ) -> Result<Vec<status::Info>, StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| -> Result<Vec<status::Info>, StoreError> {
            detail::deployment_statuses(&conn, sites)
        })
    }

    pub(crate) fn deployment_exists_and_synced(
        &self,
        id: &DeploymentHash,
    ) -> Result<bool, StoreError> {
        let conn = self.get_conn()?;
        deployment::exists_and_synced(&conn, id.as_str())
    }

    pub(crate) fn deployment_synced(&self, id: &DeploymentHash) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| deployment::set_synced(&conn, id))
    }

    // Only used for tests
    #[cfg(debug_assertions)]
    pub(crate) fn drop_deployment_schema(
        &self,
        namespace: &crate::primary::Namespace,
    ) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        deployment::drop_schema(&conn, namespace)
    }

    // Only used for tests
    #[cfg(debug_assertions)]
    pub(crate) fn drop_all_metadata(&self) -> Result<(), StoreError> {
        // Delete metadata entities in each shard

        // This needs to touch all the tables in the subgraphs schema
        const QUERY: &str = "
        delete from subgraphs.dynamic_ethereum_contract_data_source;
        delete from subgraphs.subgraph;
        delete from subgraphs.subgraph_deployment;
        delete from subgraphs.subgraph_deployment_assignment;
        delete from subgraphs.subgraph_version;
        delete from subgraphs.subgraph_manifest;
        delete from subgraphs.copy_table_state;
        delete from subgraphs.copy_state;
        delete from active_copies;
    ";

        let conn = self.get_conn()?;
        conn.batch_execute(QUERY)?;
        conn.batch_execute("delete from deployment_schemas;")?;
        Ok(())
    }

    pub(crate) async fn vacuum(&self) -> Result<(), StoreError> {
        self.with_conn(|conn, _| {
            conn.batch_execute("vacuum (analyze) subgraphs.subgraph_deployment")?;
            Ok(())
        })
        .await
    }
}

/// Methods that back the trait `graph::components::Store`, but have small
/// variations in their signatures
impl DeploymentStore {
    pub(crate) fn block_ptr(&self, site: &Site) -> Result<Option<BlockPtr>, StoreError> {
        let conn = self.get_conn()?;
        Self::block_ptr_with_conn(&site.deployment, &conn)
    }

    pub(crate) fn block_cursor(&self, site: &Site) -> Result<Option<String>, StoreError> {
        let conn = self.get_conn()?;

        Ok(deployment::get_subgraph_firehose_cursor(
            &conn,
            &site.deployment,
        )?)
    }

    pub(crate) async fn supports_proof_of_indexing<'a>(
        &self,
        site: Arc<Site>,
    ) -> Result<bool, StoreError> {
        let store = self.clone();
        self.with_conn(move |conn, cancel| {
            cancel.check_cancel()?;
            let layout = store.layout(conn, site)?;
            Ok(layout.supports_proof_of_indexing())
        })
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn get_proof_of_indexing(
        &self,
        site: Arc<Site>,
        indexer: &Option<Address>,
        block: BlockPtr,
    ) -> Result<Option<[u8; 32]>, StoreError> {
        let indexer = *indexer;
        let site3 = site.clone();
        let site4 = site.clone();
        let store = self.clone();
        let block2 = block.clone();

        let entities = self
            .with_conn(move |conn, cancel| {
                cancel.check_cancel()?;

                let layout = store.layout(conn, site4.clone())?;

                if !layout.supports_proof_of_indexing() {
                    return Ok(None);
                }

                conn.transaction::<_, CancelableError<anyhow::Error>, _>(move || {
                    let latest_block_ptr = match Self::block_ptr_with_conn(&site.deployment, conn)?
                    {
                        Some(inner) => inner,
                        None => return Ok(None),
                    };

                    cancel.check_cancel()?;

                    // FIXME: (Determinism)
                    //
                    // It is vital to ensure that the block hash given in the query
                    // is a parent of the latest block indexed for the subgraph.
                    // Unfortunately the machinery needed to do this is not yet in place.
                    // The best we can do right now is just to make sure that the block number
                    // is high enough.
                    if latest_block_ptr.number < block.number {
                        return Ok(None);
                    }

                    let query = EntityQuery::new(
                        site4.deployment.clone(),
                        block.number.try_into().unwrap(),
                        EntityCollection::All(vec![(
                            POI_OBJECT.cheap_clone(),
                            AttributeNames::All,
                        )]),
                    );
                    let entities = store
                        .execute_query::<Entity>(conn, site4, query)
                        .map_err(anyhow::Error::from)?;

                    Ok(Some(entities))
                })
                .map_err(Into::into)
            })
            .await?;

        let entities = if let Some(entities) = entities {
            entities
        } else {
            return Ok(None);
        };

        let mut by_causality_region = entities
            .into_iter()
            .map(|e| {
                let causality_region = e.id()?;
                let digest = match e.get("digest") {
                    Some(Value::Bytes(b)) => Ok(b.to_owned()),
                    other => Err(anyhow::anyhow!(
                        "Entity has non-bytes digest attribute: {:?}",
                        other
                    )),
                }?;

                Ok((causality_region, digest))
            })
            .collect::<Result<HashMap<_, _>, anyhow::Error>>()?;

        let mut finisher = ProofOfIndexingFinisher::new(&block2, &site3.deployment, &indexer);
        for (name, region) in by_causality_region.drain() {
            finisher.add_causality_region(&name, &region);
        }

        Ok(Some(finisher.finish()))
    }

    pub(crate) fn get(
        &self,
        site: Arc<Site>,
        key: &EntityKey,
    ) -> Result<Option<Entity>, StoreError> {
        let conn = self.get_conn()?;
        let layout = self.layout(&conn, site)?;

        // We should really have callers pass in a block number; but until
        // that is fully plumbed in, we just use the biggest possible block
        // number so that we will always return the latest version,
        // i.e., the one with an infinite upper bound

        layout.find(&conn, &key.entity_type, &key.entity_id, BLOCK_NUMBER_MAX)
    }

    pub(crate) fn get_many(
        &self,
        site: Arc<Site>,
        ids_for_type: &BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        if ids_for_type.is_empty() {
            return Ok(BTreeMap::new());
        }
        let conn = self.get_conn()?;
        let layout = self.layout(&conn, site)?;

        layout.find_many(&conn, ids_for_type, BLOCK_NUMBER_MAX)
    }

    // Only used by tests
    #[cfg(debug_assertions)]
    pub(crate) fn find(
        &self,
        site: Arc<Site>,
        query: EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let conn = self.get_conn()?;
        self.execute_query(&conn, site, query)
    }

    pub(crate) fn transact_block_operations(
        &self,
        site: Arc<Site>,
        block_ptr_to: &BlockPtr,
        firehose_cursor: Option<&str>,
        mods: &[EntityModification],
        stopwatch: StopwatchMetrics,
        data_sources: &[StoredDynamicDataSource],
        deterministic_errors: &[SubgraphError],
    ) -> Result<StoreEvent, StoreError> {
        // All operations should apply only to data or metadata for this subgraph
        if mods
            .iter()
            .map(|modification| modification.entity_key())
            .any(|key| key.subgraph_id != site.deployment)
        {
            panic!(
                "transact_block_operations must affect only entities \
                 in the subgraph or in the subgraph of subgraphs"
            );
        }

        let conn = {
            let _section = stopwatch.start_section("transact_blocks_get_conn");
            self.get_conn()?
        };

        let event = conn.transaction(|| -> Result<_, StoreError> {
            // Emit a store event for the changes we are about to make. We
            // wait with sending it until we have done all our other work
            // so that we do not hold a lock on the notification queue
            // for longer than we have to
            let event: StoreEvent = mods.iter().collect();

            // Make the changes
            let layout = self.layout(&conn, site.clone())?;
            let section = stopwatch.start_section("apply_entity_modifications");
            let count = self.apply_entity_modifications(
                &conn,
                layout.as_ref(),
                mods,
                &block_ptr_to,
                stopwatch,
            )?;
            deployment::update_entity_count(
                &conn,
                site.as_ref(),
                layout.count_query.as_str(),
                count,
            )?;
            section.end();

            dynds::insert(&conn, &site.deployment, data_sources, &block_ptr_to)?;

            if !deterministic_errors.is_empty() {
                deployment::insert_subgraph_errors(
                    &conn,
                    &site.deployment,
                    deterministic_errors,
                    block_ptr_to.block_number(),
                )?;
            }

            deployment::forward_block_ptr(&conn, &site.deployment, block_ptr_to)?;

            if let Some(cursor) = firehose_cursor {
                if cursor != "" {
                    deployment::update_firehose_cursor(&conn, &site.deployment, &cursor)?;
                }
            }

            Ok(event)
        })?;

        Ok(event)
    }

    fn rewind_with_conn(
        &self,
        conn: &PgConnection,
        site: Arc<Site>,
        block_ptr_to: BlockPtr,
    ) -> Result<StoreEvent, StoreError> {
        let event = conn.transaction(|| -> Result<_, StoreError> {
            // Don't revert past a graft point
            let info = self.subgraph_info_with_conn(&conn, site.as_ref())?;
            if let Some(graft_block) = info.graft_block {
                if graft_block > block_ptr_to.number {
                    return Err(anyhow!(
                        "Can not revert subgraph `{}` to block {} as it was \
                        grafted at block {} and reverting past a graft point \
                        is not possible",
                        site.deployment.clone(),
                        block_ptr_to.number,
                        graft_block
                    )
                    .into());
                }
            }

            deployment::revert_block_ptr(&conn, &site.deployment, block_ptr_to.clone())?;

            // Revert the data
            let layout = self.layout(&conn, site.clone())?;

            // At 1 block per 15 seconds, the maximum i32
            // value affords just over 1020 years of blocks.
            let block: BlockNumber = block_ptr_to
                .number
                .try_into()
                .expect("block numbers fit into an i32");
            // The revert functions want the number of the first block that we need to get rid of
            let block = block + 1;

            let (event, count) = layout.revert_block(&conn, &site.deployment, block)?;

            // Revert the meta data changes that correspond to this subgraph.
            // Only certain meta data changes need to be reverted, most
            // importantly creation of dynamic data sources. We ensure in the
            // rest of the code that we only record history for those meta data
            // changes that might need to be reverted
            Layout::revert_metadata(&conn, &site.deployment, block)?;

            deployment::update_entity_count(
                &conn,
                site.as_ref(),
                layout.count_query.as_str(),
                count,
            )?;
            Ok(event)
        })?;

        Ok(event)
    }

    pub(crate) fn rewind(
        &self,
        site: Arc<Site>,
        block_ptr_to: BlockPtr,
    ) -> Result<StoreEvent, StoreError> {
        let conn = self.get_conn()?;

        // Unwrap: If we are reverting then the block ptr is not `None`.
        let block_ptr_from = Self::block_ptr_with_conn(&site.deployment, &conn)?.unwrap();

        // Sanity check on block numbers
        if block_ptr_from.number <= block_ptr_to.number {
            constraint_violation!(
                "rewind must go backwards, but would go from block {} to block {}",
                block_ptr_from.number,
                block_ptr_to.number
            );
        }
        self.rewind_with_conn(&conn, site, block_ptr_to)
    }

    pub(crate) fn revert_block_operations(
        &self,
        site: Arc<Site>,
        block_ptr_to: BlockPtr,
    ) -> Result<StoreEvent, StoreError> {
        let conn = self.get_conn()?;
        // Unwrap: If we are reverting then the block ptr is not `None`.
        let block_ptr_from = Self::block_ptr_with_conn(&site.deployment, &conn)?.unwrap();

        // Sanity check on block numbers
        if block_ptr_from.number != block_ptr_to.number + 1 {
            panic!("revert_block_operations must revert a single block only");
        }

        self.rewind_with_conn(&conn, site, block_ptr_to)
    }

    pub(crate) async fn deployment_state_from_id(
        &self,
        id: DeploymentHash,
    ) -> Result<DeploymentState, StoreError> {
        self.with_conn(|conn, _| deployment::state(&conn, id).map_err(|e| e.into()))
            .await
    }

    pub(crate) async fn fail_subgraph(
        &self,
        id: DeploymentHash,
        error: SubgraphError,
    ) -> Result<(), StoreError> {
        self.with_conn(move |conn, _| {
            conn.transaction(|| deployment::fail(&conn, &id, &error))
                .map_err(Into::into)
        })
        .await?;
        Ok(())
    }

    pub(crate) fn replica_for_query(
        &self,
        for_subscription: bool,
    ) -> Result<ReplicaId, StoreError> {
        use std::sync::atomic::Ordering;

        let replica_id = match for_subscription {
            // Pick a weighted ReplicaId. `replica_order` contains a list of
            // replicas with repetitions according to their weight
            false => {
                let weights_count = self.replica_order.len();
                let index =
                    self.conn_round_robin_counter.fetch_add(1, Ordering::SeqCst) % weights_count;
                *self.replica_order.get(index).unwrap()
            }
            // Subscriptions always go to the main replica.
            true => ReplicaId::Main,
        };

        Ok(replica_id)
    }

    pub(crate) async fn load_dynamic_data_sources(
        &self,
        id: DeploymentHash,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.with_conn(move |conn, _| {
            conn.transaction(|| crate::dynds::load(&conn, id.as_str()))
                .map_err(Into::into)
        })
        .await
    }

    pub(crate) fn exists_and_synced(&self, id: DeploymentHash) -> Result<bool, StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| deployment::exists_and_synced(&conn, &id))
    }

    pub(crate) fn graft_pending(
        &self,
        id: &DeploymentHash,
    ) -> Result<Option<(DeploymentHash, BlockPtr)>, StoreError> {
        let conn = self.get_conn()?;
        deployment::graft_pending(&conn, id)
    }

    /// Bring the subgraph into a state where we can start or resume
    /// indexing.
    ///
    /// If `graft_src` is `Some(..)`, copy data from that subgraph. It
    /// should only be `Some(..)` if we know we still need to copy data. The
    /// code is idempotent so that a copy process that has been interrupted
    /// can be resumed seamlessly, but the code sets the block pointer back
    /// to the graph point, so that calling this needlessly with `Some(..)`
    /// will remove any progress that might have been made since the last
    /// time the deployment was started.
    pub(crate) fn start_subgraph(
        &self,
        logger: &Logger,
        site: Arc<Site>,
        graft_src: Option<(Arc<Layout>, BlockPtr)>,
    ) -> Result<(), StoreError> {
        let dst = self.find_layout(site)?;

        // Do any cleanup to bring the subgraph into a known good state
        if let Some((src, block)) = graft_src {
            info!(
                logger,
                "Initializing graft by copying data from {} to {}",
                src.catalog.site.namespace,
                dst.catalog.site.namespace
            );

            // Copy subgraph data
            // We allow both not copying tables at all from the source, as well
            // as adding new tables in `self`; we only need to check that tables
            // that actually need to be copied from the source are compatible
            // with the corresponding tables in `self`
            let copy_conn = crate::copy::Connection::new(
                logger,
                self.pool.clone(),
                src.clone(),
                dst.clone(),
                block.clone(),
            )?;
            let status = copy_conn.copy_data()?;
            if status == crate::copy::Status::Cancelled {
                return Err(StoreError::Canceled);
            }

            let conn = self.get_conn()?;
            conn.transaction(|| -> Result<(), StoreError> {
                // Copy dynamic data sources and adjust their ID
                let start = Instant::now();
                let count = dynds::copy(&conn, &src.site, &dst.site, &block)?;
                info!(logger, "Copied {} dynamic data sources", count;
                      "time_ms" => start.elapsed().as_millis());

                // Copy errors across
                let start = Instant::now();
                let count = deployment::copy_errors(&conn, &src.site, &dst.site, &block)?;
                info!(logger, "Copied {} existing errors", count;
                      "time_ms" => start.elapsed().as_millis());

                catalog::copy_account_like(&conn, &src.site, &dst.site)?;

                // Rewind the subgraph so that entity versions that are
                // clamped in the future (beyond `block`) become valid for
                // all blocks after `block`. `revert_block` gets rid of
                // everything including the block passed to it. We want to
                // preserve `block` and therefore revert `block+1`
                let start = Instant::now();
                let block_to_revert: BlockNumber = (block.number + 1)
                    .try_into()
                    .expect("block numbers fit into an i32");
                dst.revert_block(&conn, &dst.site.deployment, block_to_revert)?;
                info!(logger, "Rewound subgraph to block {}", block.number;
                      "time_ms" => start.elapsed().as_millis());

                let start = Instant::now();
                deployment::set_entity_count(&conn, &dst.site, &dst.count_query)?;
                info!(logger, "Counted the entities";
                      "time_ms" => start.elapsed().as_millis());

                // Set the block ptr to the graft point to signal that we successfully
                // performed the graft
                crate::deployment::forward_block_ptr(&conn, &dst.site.deployment, &block)?;
                info!(logger, "Subgraph successfully initialized";
                    "time_ms" => start.elapsed().as_millis());
                Ok(())
            })?;
        }
        Ok(())
    }

    // If the current block of the deployment is the same as the fatal error,
    // we revert all block operations to it's parent/previous block.
    //
    // This should be called once per subgraph on `graph-node` initialization,
    // before processing the first block on start.
    //
    // It will do nothing (early return) if:
    //
    // - There's no fatal error for the subgraph
    // - The error is NOT deterministic
    pub(crate) fn unfail_deterministic_error(
        &self,
        site: Arc<Site>,
        current_ptr: &BlockPtr,
        parent_ptr: &BlockPtr,
    ) -> Result<(), StoreError> {
        let conn = &self.get_conn()?;
        let deployment_id = &site.deployment;

        conn.transaction(|| {
            // We'll only unfail subgraphs that had fatal errors
            let subgraph_error = match detail::fatal_error(conn, deployment_id)? {
                Some(fatal_error) => fatal_error,
                // If the subgraph is not failed then there is nothing to do.
                None => return Ok(()),
            };

            // Confidence check
            if !subgraph_error.deterministic {
                return Ok(()); // Nothing to do
            }

            use deployment::SubgraphHealth::*;
            // Decide status based on if there are any errors for the previous/parent block
            let prev_health =
                if deployment::has_non_fatal_errors(conn, deployment_id, Some(parent_ptr.number))? {
                    Unhealthy
                } else {
                    Healthy
                };

            match &subgraph_error.block_hash {
                // The error happened for the current deployment head.
                // We should revert everything (deployment head, subgraph errors, etc)
                // to the previous/parent hash/block.
                Some(bytes) if bytes == current_ptr.hash.as_slice() => {
                    info!(
                        self.logger,
                        "Reverting errored block";
                        "subgraph_id" => deployment_id,
                        "from_block_number" => format!("{}", current_ptr.number),
                        "from_block_hash" => format!("{}", current_ptr.hash),
                        "to_block_number" => format!("{}", parent_ptr.number),
                        "to_block_hash" => format!("{}", parent_ptr.hash),
                    );

                    // We ignore the StoreEvent that's being returned, we'll not use it.
                    let _ = self.revert_block_operations(site.clone(), parent_ptr.clone())?;

                    // Unfail the deployment.
                    deployment::update_deployment_status(conn, deployment_id, prev_health, None)?;
                }
                // Found error, but not for deployment head, we don't need to
                // revert the block operations.
                //
                // If you find this warning in the logs, something is wrong, this
                // shoudn't happen.
                Some(hash_bytes) => {
                    warn!(self.logger, "Subgraph error does not have same block hash as deployment head";
                        "subgraph_id" => deployment_id,
                        "error_id" => &subgraph_error.id,
                        "error_block_hash" => format!("0x{}", hex::encode(&hash_bytes)),
                        "deployment_head" => format!("{}", current_ptr.hash),
                    );
                }
                // Same as branch above, if you find this warning in the logs,
                // something is wrong, this shouldn't happen.
                None => {
                    warn!(self.logger, "Subgraph error should have block hash";
                        "subgraph_id" => deployment_id,
                        "error_id" => &subgraph_error.id,
                    );
                }
            };

            Ok(())
        })
    }

    // If a non-deterministic error happens and the deployment head advances,
    // we should unfail the subgraph (status: Healthy, failed: false) and delete
    // the error itself.
    //
    // This should be called after successfully processing a block for a subgraph.
    //
    // It will do nothing (early return) if:
    //
    // - There's no fatal error for the subgraph
    // - The error IS deterministic
    pub(crate) fn unfail_non_deterministic_error(
        &self,
        site: Arc<Site>,
        current_ptr: &BlockPtr,
    ) -> Result<(), StoreError> {
        let conn = &self.get_conn()?;
        let deployment_id = &site.deployment;

        conn.transaction(|| {
            // We'll only unfail subgraphs that had fatal errors
            let subgraph_error = match detail::fatal_error(conn, deployment_id)? {
                Some(fatal_error) => fatal_error,
                // If the subgraph is not failed then there is nothing to do.
                None => return Ok(()),
            };

            // Confidence check
            if subgraph_error.deterministic {
                return Ok(()); // Nothing to do
            }

            match subgraph_error.block_range {
                // Deployment head (current_ptr) advanced more than the error.
                // That means it's healthy, and the non-deterministic error got
                // solved (didn't happen on another try).
                (Bound::Included(error_block_number), _)
                    if current_ptr.number >= error_block_number =>
                    {
                        info!(
                            self.logger,
                            "Unfailing the deployment status";
                            "subgraph_id" => deployment_id,
                        );

                        // Unfail the deployment.
                        deployment::update_deployment_status(
                            conn,
                            deployment_id,
                            deployment::SubgraphHealth::Healthy,
                            None,
                        )?;

                        // Delete the fatal error.
                        deployment::delete_error(conn, &subgraph_error.id)?;

                        Ok(())
                    }
                // NOOP, the deployment head is still before where non-deterministic error happened.
                block_range => {
                    info!(
                        self.logger,
                        "Subgraph error is still ahead of deployment head, nothing to unfail";
                        "subgraph_id" => deployment_id,
                        "block_number" => format!("{}", current_ptr.number),
                        "block_hash" => format!("{}", current_ptr.hash),
                        "error_block_range" => format!("{:?}", block_range),
                        "error_block_hash" => subgraph_error.block_hash.as_ref().map(|hash| format!("0x{}", hex::encode(hash))),
                    );

                    Ok(())
                }
            }
        })
    }

    #[cfg(debug_assertions)]
    pub fn error_count(&self, id: &DeploymentHash) -> Result<usize, StoreError> {
        let conn = self.get_conn()?;
        deployment::error_count(&conn, id)
    }

    pub(crate) async fn mirror_primary_tables(&self, logger: &Logger) {
        self.pool.mirror_primary_tables().await.unwrap_or_else(|e| {
            warn!(logger, "Mirroring primary tables failed. We will try again in a few minutes";
                  "error" => e.to_string(),
                  "shard" => self.pool.shard.as_str())
        });
    }

    pub(crate) async fn health(
        &self,
        id: &DeploymentHash,
    ) -> Result<deployment::SubgraphHealth, StoreError> {
        let id = id.clone();
        self.with_conn(move |conn, _| deployment::health(&conn, &id).map_err(Into::into))
            .await
    }
}
