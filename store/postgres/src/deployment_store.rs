use detail::DeploymentDetail;
use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use futures03::FutureExt as _;
use graph::components::store::{EntityType, StoredDynamicDataSource};
use graph::data::subgraph::status;
use graph::prelude::{
    error, CancelHandle, CancelToken, CancelableError, PoolWaitStats, SubgraphDeploymentEntity,
};
use lru_time_cache::LruCache;
use rand::{seq::SliceRandom, thread_rng};
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::iter::FromIterator;
use std::ops::Deref;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};
use std::time::Instant;

use graph::components::store::EntityCollection;
use graph::components::subgraph::ProofOfIndexingFinisher;
use graph::data::subgraph::schema::{SubgraphError, POI_OBJECT};
use graph::prelude::{
    anyhow, debug, futures03, info, o, web3, ApiSchema, BlockNumber, CheapClone, DeploymentState,
    DynTryFuture, Entity, EntityKey, EntityModification, EntityQuery, EntityRange, Error,
    EthereumBlockPointer, Logger, QueryExecutionError, Schema, StopwatchMetrics, StoreError,
    StoreEvent, SubgraphDeploymentId, Value, BLOCK_NUMBER_MAX,
};

use graph_graphql::prelude::api_schema;
use web3::types::Address;

use crate::block_range::block_number;
use crate::relational::{Catalog, Layout};
use crate::relational_queries::FromEntityData;
use crate::{connection_pool::ConnectionPool, detail};
use crate::{deployment, primary::Namespace};
use crate::{dynds, primary::Site};

embed_migrations!("./migrations");

/// Run all schema migrations.
///
/// When multiple `graph-node` processes start up at the same time, we ensure
/// that they do not run migrations in parallel by using `blocking_conn` to
/// serialize them. The `conn` is used to run the actual migration.
fn initiate_schema(logger: &Logger, conn: &PgConnection, blocking_conn: &PgConnection) {
    // Collect migration logging output
    let mut output = vec![];

    // Make sure the locking table exists so we have something
    // to lock. We intentionally ignore errors here, because they are most
    // likely caused by us losing a race to create the table against another
    // graph-node. If this truly is an error, we will trip over it when
    // we try to lock the table and report it to the user
    if let Err(e) = blocking_conn.batch_execute(
        "create table if not exists \
         __graph_node_global_lock(id int)",
    ) {
        debug!(
            logger,
            "Creating lock table failed, this is most likely harmless";
            "error" => format!("{:?}", e)
        );
    }

    // blocking_conn holds the lock on the migrations table for the duration
    // of the migration on conn. Since all nodes execute this code, only one
    // of them can run this code at the same time. We need to use two
    // connections for this because diesel will run each migration in its
    // own txn, which makes it impossible to hold a lock across all of them
    // on that connection
    info!(
        logger,
        "Waiting for other graph-node instances to finish migrating"
    );
    let result = blocking_conn.transaction(|| {
        diesel::sql_query("lock table __graph_node_global_lock in exclusive mode")
            .execute(blocking_conn)?;
        info!(logger, "Running migrations");
        embedded_migrations::run_with_output(conn, &mut output)
    });
    info!(logger, "Migrations finished");

    // If there was any migration output, log it now
    let has_output = !output.is_empty();
    if has_output {
        let msg = String::from_utf8(output).unwrap_or_else(|_| String::from("<unreadable>"));
        if result.is_err() {
            error!(logger, "Postgres migration output"; "output" => msg);
        } else {
            debug!(logger, "Postgres migration output"; "output" => msg);
        }
    }

    if let Err(e) = result {
        panic!(
            "Error setting up Postgres database: \
             You may need to drop and recreate your database to work with the \
             latest version of graph-node. Error information: {:?}",
            e
        )
    };

    if has_output {
        // We take getting output as a signal that a migration was actually
        // run, which is not easy to tell from the Diesel API, and reset the
        // query statistics since a schema change makes them not all that
        // useful. An error here is not serious and can be ignored.
        conn.batch_execute("select pg_stat_statements_reset()").ok();
    }
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

    conn: ConnectionPool,
    read_only_pools: Vec<ConnectionPool>,

    /// A list of the available replicas set up such that when we run
    /// through the list once, we picked each replica according to its
    /// desired weight. Each replica can appear multiple times in the list
    replica_order: Vec<ReplicaId>,
    /// The current position in `replica_order` so we know which one to
    /// pick next
    conn_round_robin_counter: AtomicUsize,

    /// A cache of commonly needed data about a subgraph.
    subgraph_cache: Mutex<LruCache<SubgraphDeploymentId, SubgraphInfo>>,

    /// A cache for the layout metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) layout_cache: Mutex<HashMap<SubgraphDeploymentId, Arc<Layout>>>,
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

        initiate_schema(&logger, &pool.get().unwrap(), &pool.get().unwrap());

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
            conn: pool,
            read_only_pools,
            replica_order,
            conn_round_robin_counter: AtomicUsize::new(0),
            subgraph_cache: Mutex::new(LruCache::with_capacity(100)),
            layout_cache: Mutex::new(HashMap::new()),
        };
        let store = DeploymentStore(Arc::new(store));

        // Return the store
        store
    }

    pub(crate) fn create_deployment(
        &self,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        site: &Site,
        graft_site: Option<Site>,
        replace: bool,
    ) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| -> Result<_, StoreError> {
            let exists = deployment::exists(&conn, &site.deployment)?;

            // Create (or update) the metadata. Update only happens in tests
            if replace || !exists {
                deployment::create_deployment(
                    &conn,
                    &site.deployment,
                    deployment,
                    exists,
                    replace,
                )?;
            };

            // Create the schema for the subgraph data
            if !exists {
                let query = format!("create schema {}", &site.namespace);
                conn.batch_execute(&query)?;

                let layout = Layout::create_relational_schema(&conn, schema, site.namespace.clone())?;
                // See if we are grafting and check that the graft is permissible
                if let Some(graft_site) = graft_site {
                    let base =
                        self.layout(&conn, &graft_site)?;
                    let errors = layout.can_copy_from(&base);
                    if !errors.is_empty() {
                        return Err(StoreError::Unknown(anyhow!(
                            "The subgraph `{}` cannot be used as the graft base \
                                                    for `{}` because the schemas are incompatible:\n    - {}",
                            &base.catalog.namespace,
                            &layout.catalog.namespace,
                            errors.join("\n    - ")
                        )));
                    }
                }
            }
            Ok(())
        })
    }

    // Remove the data and metadata for the deployment `site`. This operation
    // is not reversible
    pub(crate) fn drop_deployment(&self, site: &Site) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| {
            crate::deployment::drop_schema(&conn, &site.namespace)?;
            crate::dynds::drop(&conn, &site.deployment)?;
            crate::deployment::drop_metadata(&conn, &site.deployment)
        })
    }

    pub(crate) fn execute_query<T: FromEntityData>(
        &self,
        conn: &PgConnection,
        site: &Site,
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
        let schema = self.subgraph_info_with_conn(&conn, &key.subgraph_id)?.api;
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
        mods: Vec<EntityModification>,
        ptr: &EthereumBlockPointer,
        stopwatch: StopwatchMetrics,
    ) -> Result<i32, StoreError> {
        let mut count = 0;

        for modification in mods {
            use EntityModification::*;

            let n = match modification {
                Overwrite { key, data } => {
                    let section = stopwatch.start_section("check_interface_entity_uniqueness");
                    self.check_interface_entity_uniqueness(conn, layout, &key)?;
                    section.end();

                    let _section = stopwatch.start_section("apply_entity_modifications_update");
                    layout
                        .update(conn, &key, data, block_number(ptr))
                        .map(|_| 0)
                }
                Insert { key, data } => {
                    let section = stopwatch.start_section("check_interface_entity_uniqueness");
                    self.check_interface_entity_uniqueness(conn, layout, &key)?;
                    section.end();

                    let _section = stopwatch.start_section("apply_entity_modifications_insert");
                    layout
                        .insert(conn, &key, data, block_number(ptr))
                        .map(|_| 1)
                }
                Remove { key } => layout
                    .delete(conn, &key, block_number(ptr))
                    // This conversion is ok since n will only be 0 or 1
                    .map(|n| -(n as i32))
                    .map_err(|e| {
                        anyhow!(
                            "Failed to remove entity ({}, {}, {}): {}",
                            key.subgraph_id,
                            key.entity_type,
                            key.entity_id,
                            e
                        )
                        .into()
                    }),
            }?;
            count += n;
        }
        Ok(count)
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
        self.conn.with_conn(f).await
    }

    /// Deprecated. Use `with_conn` instead.
    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        self.conn.get_with_timeout_warning(&self.logger)
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

    pub(crate) fn wait_stats(&self, replica: ReplicaId) -> &PoolWaitStats {
        match replica {
            ReplicaId::Main => &self.conn.wait_stats,
            ReplicaId::ReadOnly(idx) => &self.read_only_pools[idx].wait_stats,
        }
    }

    /// Return the layout for the subgraph. Since constructing a `Layout`
    /// object takes a bit of computation, we cache layout objects that do
    /// not have a pending migration in the Store, i.e., for the lifetime of
    /// the Store. Layout objects with a pending migration can not be
    /// cached for longer than a transaction since they might change
    /// without us knowing
    pub(crate) fn layout(
        &self,
        conn: &PgConnection,
        site: &Site,
    ) -> Result<Arc<Layout>, StoreError> {
        if let Some(layout) = self.layout_cache.lock().unwrap().get(&site.deployment) {
            return Ok(layout.clone());
        }

        let subgraph_schema = deployment::schema(conn, site.deployment.clone())?;
        let has_poi = crate::catalog::supports_proof_of_indexing(conn, &site.namespace)?;
        let catalog = Catalog::new(conn, site.namespace.clone())?;
        let layout = Arc::new(Layout::new(&subgraph_schema, catalog, has_poi)?);

        if layout.is_cacheable() {
            &self
                .layout_cache
                .lock()
                .unwrap()
                .insert(site.deployment.clone(), layout.clone());
        }
        Ok(layout.clone())
    }

    fn subgraph_info_with_conn(
        &self,
        conn: &PgConnection,
        subgraph_id: &SubgraphDeploymentId,
    ) -> Result<SubgraphInfo, StoreError> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&subgraph_id) {
            return Ok(info.clone());
        }

        let (input_schema, description, repository) =
            deployment::manifest_info(&conn, subgraph_id.to_owned())?;

        let graft_block =
            deployment::graft_point(&conn, &subgraph_id)?.map(|(_, ptr)| ptr.number as i32);

        let features = deployment::features(&conn, subgraph_id)?;

        // Generate an API schema for the subgraph and make sure all types in the
        // API schema have a @subgraphId directive as well
        let mut schema = input_schema.clone();
        schema.document =
            api_schema(&schema.document, &features).map_err(|e| StoreError::Unknown(e.into()))?;
        schema.add_subgraph_id_directives(subgraph_id.clone());

        let info = SubgraphInfo {
            input: Arc::new(input_schema),
            api: Arc::new(ApiSchema::from_api_schema(schema)?),
            graft_block,
            description,
            repository,
        };

        // Insert the schema into the cache.
        let mut cache = self.subgraph_cache.lock().unwrap();
        cache.insert(subgraph_id.clone(), info);

        Ok(cache.get(&subgraph_id).unwrap().clone())
    }

    pub(crate) fn subgraph_info(
        &self,
        subgraph_id: &SubgraphDeploymentId,
    ) -> Result<SubgraphInfo, StoreError> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&subgraph_id) {
            return Ok(info.clone());
        }

        let conn = self.get_conn()?;
        self.subgraph_info_with_conn(&conn, subgraph_id)
    }

    fn block_ptr_with_conn(
        subgraph_id: &SubgraphDeploymentId,
        conn: &PgConnection,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        Ok(deployment::block_ptr(&conn, subgraph_id)?)
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
        id: &SubgraphDeploymentId,
    ) -> Result<bool, StoreError> {
        let conn = self.get_conn()?;
        deployment::exists_and_synced(&conn, id.as_str())
    }

    pub(crate) fn deployment_synced(&self, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| deployment::set_synced(&conn, id))
    }

    // Only used for tests
    #[cfg(debug_assertions)]
    pub(crate) fn drop_deployment_schema(&self, namespace: &Namespace) -> Result<(), StoreError> {
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
    ";

        let conn = self.get_conn()?;
        conn.batch_execute(QUERY)?;
        conn.batch_execute("delete from deployment_schemas;")?;
        Ok(())
    }
}

/// Methods that back the trait `graph::components::Store`, but have small
/// variations in their signatures
impl DeploymentStore {
    pub(crate) fn block_ptr(&self, site: &Site) -> Result<Option<EthereumBlockPointer>, Error> {
        let conn = self.get_conn()?;
        Self::block_ptr_with_conn(&site.deployment, &conn)
    }

    pub(crate) fn supports_proof_of_indexing<'a>(
        self: Arc<Self>,
        site: Arc<Site>,
    ) -> DynTryFuture<'a, bool> {
        let store = self.clone();
        async move {
            self.with_conn(move |conn, cancel| {
                cancel.check_cancel()?;
                let layout = store.layout(conn, &site)?;
                Ok(layout.tables.contains_key(&*POI_OBJECT))
            })
            .await
            .map_err(|e| e.into())
        }
        .boxed()
    }

    pub(crate) fn get_proof_of_indexing<'a>(
        self: Arc<Self>,
        site: Arc<Site>,
        indexer: &'a Option<Address>,
        block: EthereumBlockPointer,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        let indexer = indexer.clone();
        let site3 = site.clone();
        let site4 = site.clone();
        let store = self.clone();

        async move {
            let entities = self
                .with_conn(move |conn, cancel| {
                    cancel.check_cancel()?;

                    let layout = store.layout(conn, &site4)?;

                    if !layout.tables.contains_key(&*POI_OBJECT) {
                        return Ok(None);
                    }

                    conn.transaction::<_, CancelableError<anyhow::Error>, _>(move || {
                        let latest_block_ptr =
                            match Self::block_ptr_with_conn(&site.deployment, conn)? {
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
                            EntityCollection::All(vec![POI_OBJECT.cheap_clone()]),
                        );
                        let entities = store
                            .execute_query::<Entity>(conn, &site4, query)
                            .map_err(anyhow::Error::from)?;

                        Ok(Some(entities))
                    })
                    .map_err(|e| e.into())
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

            let mut finisher = ProofOfIndexingFinisher::new(&block, &site3.deployment, &indexer);
            for (name, region) in by_causality_region.drain() {
                finisher.add_causality_region(&name, &region);
            }

            Ok(Some(finisher.finish()))
        }
        .boxed()
    }

    pub(crate) fn get(
        &self,
        site: &Site,
        key: EntityKey,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        let conn = self.get_conn().map_err(|e| StoreError::Unknown(e))?;
        let layout = self.layout(&conn, site)?;

        // We should really have callers pass in a block number; but until
        // that is fully plumbed in, we just use the biggest possible block
        // number so that we will always return the latest version,
        // i.e., the one with an infinite upper bound

        layout
            .find(&conn, &key.entity_type, &key.entity_id, BLOCK_NUMBER_MAX)
            .map_err(|e| {
                QueryExecutionError::ResolveEntityError(
                    key.subgraph_id.clone(),
                    key.entity_type.to_string(),
                    key.entity_id.clone(),
                    format!("Invalid entity {}", e),
                )
            })
    }

    pub(crate) fn get_many(
        &self,
        site: &Site,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        if ids_for_type.is_empty() {
            return Ok(BTreeMap::new());
        }
        let conn = self.get_conn()?;
        let layout = self.layout(&conn, site)?;

        layout.find_many(&conn, ids_for_type, BLOCK_NUMBER_MAX)
    }

    pub(crate) fn find(
        &self,
        site: &Site,
        query: EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let conn = self
            .get_conn()
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.execute_query(&conn, site, query)
    }

    pub(crate) fn find_one(
        &self,
        site: &Site,
        mut query: EntityQuery,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        query.range = EntityRange::first(1);

        let conn = self
            .get_conn()
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;

        let mut results = self.execute_query(&conn, site, query)?;
        match results.len() {
            0 | 1 => Ok(results.pop()),
            n => panic!("find_one query found {} results", n),
        }
    }

    pub(crate) fn transact_block_operations(
        &self,
        site: &Site,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
        data_sources: Vec<StoredDynamicDataSource>,
        deterministic_errors: Vec<SubgraphError>,
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

        let conn = self.get_conn()?;

        let event = conn.transaction(|| -> Result<_, StoreError> {
            let block_ptr_from = Self::block_ptr_with_conn(&site.deployment, &conn)?;
            if let Some(ref block_ptr_from) = block_ptr_from {
                if block_ptr_from.number >= block_ptr_to.number {
                    return Err(StoreError::DuplicateBlockProcessing(
                        site.deployment.clone(),
                        block_ptr_to.number,
                    ));
                }
            }

            // Emit a store event for the changes we are about to make. We
            // wait with sending it until we have done all our other work
            // so that we do not hold a lock on the notification queue
            // for longer than we have to
            let event: StoreEvent = mods.iter().collect();

            // Make the changes
            let layout = self.layout(&conn, site)?;
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
                &site.deployment,
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
            Ok(event)
        })?;

        Ok(event)
    }

    pub(crate) fn revert_block_operations(
        &self,
        site: &Site,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<StoreEvent, StoreError> {
        let conn = self.get_conn()?;

        let event = conn.transaction(|| -> Result<_, StoreError> {
            // Unwrap: If we are reverting then the block ptr is not `None`.
            let block_ptr_from = Self::block_ptr_with_conn(&site.deployment, &conn)?.unwrap();

            // Sanity check on block numbers
            if block_ptr_from.number != block_ptr_to.number + 1 {
                panic!("revert_block_operations must revert a single block only");
            }

            // Don't revert past a graft point
            let info = self.subgraph_info_with_conn(&conn, &site.deployment)?;
            if let Some(graft_block) = info.graft_block {
                if graft_block as u64 > block_ptr_to.number {
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

            deployment::revert_block_ptr(&conn, &site.deployment, block_ptr_to)?;

            // Revert the data
            let layout = self.layout(&conn, site)?;

            // At 1 block per 15 seconds, the maximum i32
            // value affords just over 1020 years of blocks.
            let block = block_ptr_from
                .number
                .try_into()
                .expect("block numbers fit into an i32");

            let (event, count) = layout.revert_block(&conn, &site.deployment, block)?;

            // Revert the meta data changes that correspond to this subgraph.
            // Only certain meta data changes need to be reverted, most
            // importantly creation of dynamic data sources. We ensure in the
            // rest of the code that we only record history for those meta data
            // changes that might need to be reverted
            Layout::revert_metadata(&conn, &site.deployment, block)?;

            deployment::update_entity_count(
                &conn,
                &site.deployment,
                layout.count_query.as_str(),
                count,
            )?;
            Ok(event)
        })?;

        Ok(event)
    }

    pub(crate) async fn deployment_state_from_id(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<DeploymentState, StoreError> {
        self.with_conn(|conn, _| deployment::state(&conn, id).map_err(|e| e.into()))
            .await
    }

    pub(crate) async fn fail_subgraph(
        &self,
        id: SubgraphDeploymentId,
        error: SubgraphError,
    ) -> Result<(), StoreError> {
        self.with_conn(move |conn, _| {
            conn.transaction(|| deployment::fail(&conn, &id, error))
                .map_err(|e| e.into())
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
        id: SubgraphDeploymentId,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.with_conn(move |conn, _| {
            conn.transaction(|| crate::dynds::load(&conn, id.as_str()))
                .map_err(|e| e.into())
        })
        .await
    }

    pub(crate) fn exists_and_synced(&self, id: &SubgraphDeploymentId) -> Result<bool, StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| deployment::exists_and_synced(&conn, id))
    }

    pub(crate) fn graft_pending(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<Option<(SubgraphDeploymentId, EthereumBlockPointer)>, StoreError> {
        let conn = self.get_conn()?;
        deployment::graft_pending(&conn, id)
    }

    pub(crate) fn start_subgraph(
        &self,
        logger: &Logger,
        site: Arc<Site>,
        graft_base: Option<(Site, EthereumBlockPointer)>,
    ) -> Result<(), StoreError> {
        let conn = self.get_conn()?;

        // Do any cleanup to bring the subgraph into a known good state
        conn.transaction(|| {
            let layout = self.layout(&conn, site.as_ref())?;
            if let Some((base, block)) = graft_base {
                let base_layout = self.layout(&conn, &base)?;
                let start = Instant::now();
                layout.copy_from(
                    logger,
                    &conn,
                    &site.deployment,
                    &base_layout,
                    &base.deployment,
                    block,
                )?;
                // Set the block ptr to the graft point to signal that we successfully
                // performed the graft
                deployment::forward_block_ptr(&conn, &site.deployment, block.clone())?;
                info!(logger, "Subgraph successfully initialized";
                    "time_ms" => start.elapsed().as_millis());
            }
            Ok(())
        })
    }

    pub(crate) fn unfail(&self, site: Arc<Site>) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| deployment::unfail(&conn, &site.deployment))
    }

    #[cfg(debug_assertions)]
    pub fn error_count(&self, id: &SubgraphDeploymentId) -> Result<usize, StoreError> {
        let conn = self.get_conn()?;
        deployment::error_count(&conn, id)
    }
}
