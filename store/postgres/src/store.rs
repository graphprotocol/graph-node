use detail::DeploymentDetail;
use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::{insert_into, update};
use futures03::FutureExt as _;
use graph::components::store::{EntityType, StoredDynamicDataSource};
use graph::data::subgraph::status;
use graph::prelude::{
    error, CancelGuard, CancelHandle, CancelToken, CancelableError, PoolWaitStats,
    SubgraphDeploymentEntity,
};
use lazy_static::lazy_static;
use lru_time_cache::LruCache;
use rand::{seq::SliceRandom, thread_rng};
use std::convert::TryInto;
use std::iter::FromIterator;
use std::ops::Deref;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};
use std::time::Instant;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use tokio::sync::Semaphore;

use graph::components::store::EntityCollection;
use graph::components::subgraph::ProofOfIndexingFinisher;
use graph::data::subgraph::schema::{SubgraphError, POI_OBJECT};
use graph::prelude::{
    anyhow, debug, ethabi, futures03, info, o, tiny_keccak, tokio, trace, web3, ApiSchema,
    BlockNumber, CheapClone, DeploymentState, DynTryFuture, Entity, EntityKey, EntityModification,
    EntityOrder, EntityQuery, EntityRange, Error, EthereumBlockPointer, EthereumCallCache, Logger,
    MetadataOperation, MetricsRegistry, QueryExecutionError, Schema, StopwatchMetrics, StoreError,
    StoreEvent, SubgraphDeploymentId, Value, BLOCK_NUMBER_MAX,
};

use graph_graphql::prelude::api_schema;
use web3::types::Address;

use crate::primary::Site;
use crate::relational::{Layout, METADATA_LAYOUT};
use crate::relational_queries::FromEntityData;
use crate::{connection_pool::ConnectionPool, detail, entities as e};
use crate::{deployment, primary::Namespace};

lazy_static! {
    static ref CONNECTION_LIMITER: Semaphore = {
        let db_conn_pool_size = std::env::var("STORE_CONNECTION_POOL_SIZE")
            .unwrap_or("10".into())
            .parse::<usize>()
            .expect("invalid STORE_CONNECTION_POOL_SIZE");

        Semaphore::new(db_conn_pool_size)
    };
}

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

/// Configuration for the Diesel/Postgres store.
pub struct StoreConfig {
    pub postgres_url: String,
    pub network_name: String,
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
    replica_order: Vec<ReplicaId>,
    conn_round_robin_counter: AtomicUsize,

    /// A cache of commonly needed data about a subgraph.
    subgraph_cache: Mutex<LruCache<SubgraphDeploymentId, SubgraphInfo>>,

    /// A cache for the layout metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) layout_cache: e::LayoutCache,

    registry: Arc<dyn MetricsRegistry>,
}

/// A Store based on Diesel and Postgres.
#[derive(Clone)]
pub struct Store(Arc<StoreInner>);

impl CheapClone for Store {}

impl Deref for Store {
    type Target = StoreInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Store {
    pub fn new(
        logger: &Logger,
        pool: ConnectionPool,
        read_only_pools: Vec<ConnectionPool>,
        mut pool_weights: Vec<usize>,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        // Create a store-specific logger
        let logger = logger.new(o!("component" => "Store"));

        // Create the entities table (if necessary)
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
            layout_cache: e::make_layout_cache(),
            registry,
        };
        let store = Store(Arc::new(store));

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
    ) -> Result<StoreEvent, StoreError> {
        let conn = self.get_conn()?;
        // This is a bit of a Frankenconnection: we don't have the actual
        // layout yet; but for applying metadata, it's fine to use the metadata
        // layout
        let econn = e::Connection::new(
            conn.into(),
            METADATA_LAYOUT.clone(),
            site.deployment.clone(),
        );
        econn.transaction(|| -> Result<_, StoreError> {
            let exists = deployment::exists(&econn.conn, &site.deployment)?;

            let event = if replace || !exists {
                let ops = deployment.create_operations(&site.deployment);
                self.apply_metadata_operations_with_conn(&econn, ops)?
            } else {
                StoreEvent::new(vec![])
            };

            if !exists {
                econn.create_schema(site.namespace.clone(), schema, graft_site)?;
            }
            Ok(event)
        })
    }

    // Remove the data and metadata for the deployment `site`. This operation
    // is not reversible
    pub(crate) fn drop_deployment(&self, site: &Site) -> Result<(), StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| e::Connection::drop_deployment(&conn, site))
    }

    /// Gets an entity from Postgres.
    fn get_entity(
        &self,
        conn: &e::Connection,
        key: &EntityKey,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        // We should really have callers pass in a block number; but until
        // that is fully plumbed in, we just use the biggest possible block
        // number so that we will always return the latest version,
        // i.e., the one with an infinite upper bound
        conn.find(key, BLOCK_NUMBER_MAX).map_err(|e| {
            QueryExecutionError::ResolveEntityError(
                key.subgraph_id.clone(),
                key.entity_type.to_string(),
                key.entity_id.clone(),
                format!("Invalid entity {}", e),
            )
        })
    }

    pub(crate) fn execute_query<T: FromEntityData>(
        &self,
        conn: &e::Connection,
        query: EntityQuery,
    ) -> Result<Vec<T>, QueryExecutionError> {
        // Process results; deserialize JSON data
        let logger = query.logger.unwrap_or(self.logger.clone());
        conn.query(
            &logger,
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
        conn: &e::Connection,
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
        let schema = self
            .subgraph_info_with_conn(&conn.conn, &key.subgraph_id)?
            .api;
        let types_for_interface = schema.types_for_interface();
        let entity_type = match &key.entity_type {
            EntityType::Data(s) => s,
            EntityType::Metadata(_) => {
                // Metadata has no interfaces
                return Ok(());
            }
        };
        let types_with_shared_interface = Vec::from_iter(
            schema
                .interfaces_for_type(entity_type)
                .into_iter()
                .flatten()
                .map(|interface| &types_for_interface[&interface.name])
                .flatten()
                .map(|object_type| &object_type.name)
                .filter(|type_name| *type_name != entity_type),
        );

        if !types_with_shared_interface.is_empty() {
            if let Some(conflicting_entity) =
                conn.conflicting_entity(&key.entity_id, types_with_shared_interface)?
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

    /// Apply a metadata operation in Postgres.
    fn apply_metadata_operation(
        &self,
        conn: &e::Connection,
        operation: MetadataOperation,
    ) -> Result<i32, StoreError> {
        match operation {
            MetadataOperation::Set { key, data } => {
                let key = key.into();

                // Load the entity if exists
                let entity = self.get_entity(conn, &key)?;

                // Identify whether this is an insert or an update operation and
                // merge the changes into the entity.
                let result = match entity {
                    Some(mut entity) => {
                        entity.merge_remove_null_fields(data);
                        conn.update(&key, entity, None).map(|_| 0)
                    }
                    None => {
                        // Merge with a new entity since that removes values that
                        // were set to Value::Null
                        let mut entity = Entity::new();
                        entity.merge_remove_null_fields(data);
                        conn.insert(&key, entity, None).map(|_| 1)
                    }
                };

                result.map_err(|e| {
                    anyhow!(
                        "Failed to set entity ({}, {}, {}): {}",
                        key.subgraph_id,
                        key.entity_type,
                        key.entity_id,
                        e
                    )
                    .into()
                })
            }
            MetadataOperation::Remove { .. } => unreachable!("metadata is never deleted"),
        }
    }

    fn apply_entity_modifications(
        &self,
        conn: &e::Connection,
        mods: Vec<EntityModification>,
        ptr: Option<&EthereumBlockPointer>,
        stopwatch: StopwatchMetrics,
    ) -> Result<(), StoreError> {
        let mut count = 0;

        for modification in mods {
            use EntityModification::*;

            let do_count = modification.entity_key().entity_type.is_data_type();
            let n = match modification {
                Overwrite { key, data } => {
                    let section = stopwatch.start_section("check_interface_entity_uniqueness");
                    self.check_interface_entity_uniqueness(conn, &key)?;
                    section.end();

                    let _section = stopwatch.start_section("apply_entity_modifications_update");
                    conn.update(&key, data, ptr).map(|_| 0)
                }
                Insert { key, data } => {
                    let section = stopwatch.start_section("check_interface_entity_uniqueness");
                    self.check_interface_entity_uniqueness(conn, &key)?;
                    section.end();

                    let _section = stopwatch.start_section("apply_entity_modifications_insert");
                    conn.insert(&key, data, ptr).map(|_| 1)
                }
                Remove { key } => conn
                    .delete(&key, ptr)
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
            if do_count {
                count += n;
            }
        }
        conn.update_entity_count(count)?;
        Ok(())
    }

    pub(crate) fn apply_metadata_operations_with_conn(
        &self,
        econn: &e::Connection,
        operations: Vec<MetadataOperation>,
    ) -> Result<StoreEvent, StoreError> {
        // Emit a store event for the changes we are about to make
        let event: StoreEvent = operations.clone().into();

        // Actually apply the operations
        for operation in operations.into_iter() {
            self.apply_metadata_operation(econn, operation)?;
        }
        Ok(event)
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
        let _permit = CONNECTION_LIMITER.acquire().await;
        let store = self.clone();

        let cancel_guard = CancelGuard::new();
        let cancel_handle = cancel_guard.handle();

        let result = graph::spawn_blocking_allow_panic(move || {
            // It is possible time has passed between scheduling on the
            // threadpool and being executed. Time to check for cancel.
            cancel_handle.check_cancel()?;

            // A failure to establish a connection is propagated as though the
            // closure failed.
            let conn = store
                .get_conn()
                .map_err(|e| CancelableError::Error(StoreError::Unknown(e)))?;

            // It is possible time has passed while establishing a connection.
            // Time to check for cancel.
            cancel_handle.check_cancel()?;

            f(&conn, &cancel_handle)
        })
        .await
        .unwrap(); // Propagate panics, though there shouldn't be any.

        drop(cancel_guard);

        // Finding cancel isn't technically unreachable, since there is nothing
        // stopping the supplied closure from returning Canceled even if the
        // supplied handle wasn't canceled. That would be very unexpected, the
        // doc comment for this function says we will panic in this scenario.
        match result {
            Ok(t) => Ok(t),
            Err(CancelableError::Error(e)) => Err(e),
            Err(CancelableError::Cancel) => panic!("The closure supplied to with_entity_conn must not return Err(Canceled) unless the supplied token was canceled."),
        }
    }

    /// Executes a closure with an `e::Connection` reference.
    /// The `e::Connection` gives access to subgraph specific storage in the database - mostly for
    /// storing and loading entities local to that subgraph.
    ///
    /// This uses `with_conn` under the hood. Please see it's documentation for important details
    /// about usage.
    async fn with_entity_conn<T: Send + 'static>(
        self: Arc<Self>,
        site: Arc<Site>,
        f: impl 'static
            + Send
            + FnOnce(&e::Connection, &CancelHandle) -> Result<T, CancelableError<StoreError>>,
    ) -> Result<T, StoreError> {
        let store = self.cheap_clone();

        // Duplicated logic: No need to make re-usable when the
        // other end will go away.
        // See also 220c1ae9-3e8a-42d3-bcc5-b1244a69b8a9
        let start = Instant::now();
        let registry = self.registry.cheap_clone();

        self.with_conn(move |conn, cancel_handle| {
            registry
                .global_deployment_counter(
                    "deployment_get_entity_conn_secs",
                    "total time spent getting an entity connection",
                    site.deployment.as_str(),
                )
                .map_err(|e| CancelableError::Error(StoreError::Unknown(e.into())))?
                .inc_by(start.elapsed().as_secs_f64());

            cancel_handle.check_cancel()?;
            let layout = store.layout(&conn, &site.namespace, &site.deployment)?;
            cancel_handle.check_cancel()?;
            let conn = e::Connection::new(conn.into(), layout, site.deployment.clone());

            f(&conn, cancel_handle)
        })
        .await
    }

    /// Deprecated. Use `with_conn` instead.
    pub(crate) fn get_conn(
        &self,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        loop {
            match self.conn.get_timeout(Duration::from_secs(60)) {
                Ok(conn) => return Ok(conn),
                Err(e) => error!(self.logger, "Error checking out connection, retrying";
                   "error" => e.to_string(),
                ),
            }
        }
    }

    /// Panics if `idx` is not a valid index for a read only pool.
    fn read_only_conn(
        &self,
        idx: usize,
    ) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        self.read_only_pools[idx].get().map_err(Error::from)
    }

    // Duplicated logic - this function may eventually go away.
    // See also 220c1ae9-3e8a-42d3-bcc5-b1244a69b8a9
    /// Deprecated. Use `with_entity_conn` instead
    pub(crate) fn get_entity_conn(
        &self,
        site: &Site,
        replica: ReplicaId,
    ) -> Result<e::Connection, Error> {
        assert!(!site.namespace.is_metadata());

        let start = Instant::now();
        let conn = match replica {
            ReplicaId::Main => self.get_conn()?,
            ReplicaId::ReadOnly(idx) => self.read_only_conn(idx)?,
        };
        self.registry
            .global_deployment_counter(
                "deployment_get_entity_conn_secs",
                "total time spent getting an entity connection",
                site.deployment.as_str(),
            )?
            .inc_by(start.elapsed().as_secs_f64());
        let data = self.layout(&conn, &site.namespace, &site.deployment)?;
        Ok(e::Connection::new(
            conn.into(),
            data,
            site.deployment.clone(),
        ))
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
        namespace: &Namespace,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<Arc<Layout>, StoreError> {
        assert!(!namespace.is_metadata());

        if let Some(layout) = self.layout_cache.lock().unwrap().get(subgraph) {
            return Ok(layout.clone());
        }

        let layout = Arc::new(e::Connection::layout(conn, namespace.clone(), subgraph)?);
        if layout.is_cacheable() {
            &self
                .layout_cache
                .lock()
                .unwrap()
                .insert(subgraph.clone(), layout.clone());
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
        conn: &e::Connection,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        Ok(deployment::block_ptr(&conn.conn, subgraph_id)?)
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
}

/// Methods that back the trait `graph::components::Store`, but have small
/// variations in their signatures
impl Store {
    pub(crate) fn block_ptr(&self, site: &Site) -> Result<Option<EthereumBlockPointer>, Error> {
        Self::block_ptr_with_conn(
            &site.deployment,
            &self
                .get_entity_conn(site, ReplicaId::Main)
                .map_err(|e| QueryExecutionError::StoreError(e.into()))?,
        )
    }

    pub(crate) fn supports_proof_of_indexing<'a>(
        self: Arc<Self>,
        site: Arc<Site>,
    ) -> DynTryFuture<'a, bool> {
        async move {
            self.with_entity_conn(site, |conn, cancel| {
                cancel.check_cancel()?;
                Ok(conn.supports_proof_of_indexing())
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
        let logger = self.logger.cheap_clone();
        let indexer = indexer.clone();
        let site2 = site.clone();
        let site3 = site.clone();

        async move {
            let entities = self
                .with_entity_conn(site2, move |conn, cancel| {
                    cancel.check_cancel()?;

                    if !conn.supports_proof_of_indexing() {
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

                        let entities = conn
                            .query::<Entity>(
                                &logger,
                                EntityCollection::All(vec![POI_OBJECT.to_owned()]),
                                None,
                                EntityOrder::Default,
                                EntityRange {
                                    first: None,
                                    skip: 0,
                                },
                                block.number.try_into().unwrap(),
                                None,
                            )
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
        let conn = self
            .get_entity_conn(site, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.get_entity(&conn, &key)
    }

    pub(crate) fn get_many(
        &self,
        site: &Site,
        ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        if ids_for_type.is_empty() {
            return Ok(BTreeMap::new());
        }
        let conn = self
            .get_entity_conn(site, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        conn.find_many(ids_for_type, BLOCK_NUMBER_MAX)
    }

    pub(crate) fn find(
        &self,
        site: &Site,
        query: EntityQuery,
    ) -> Result<Vec<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(site, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.execute_query(&conn, query)
    }

    pub(crate) fn find_one(
        &self,
        site: &Site,
        mut query: EntityQuery,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        query.range = EntityRange::first(1);

        let conn = self
            .get_entity_conn(site, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;

        let mut results = self.execute_query(&conn, query)?;
        match results.len() {
            0 | 1 => Ok(results.pop()),
            n => panic!("find_one query found {} results", n),
        }
    }

    pub(crate) fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        use crate::db_schema::ens_names as dsl;

        let conn = self
            .get_conn()
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;

        dsl::table
            .select(dsl::name)
            .find(hash)
            .get_result::<String>(&conn)
            .optional()
            .map_err(|e| {
                QueryExecutionError::StoreError(
                    anyhow!("error looking up ens_name for hash {}: {}", hash, e).into(),
                )
            })
    }

    pub(crate) fn transact_block_operations(
        &self,
        site: &Site,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
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

        let econn = self.get_entity_conn(site, ReplicaId::Main)?;

        let event = econn.transaction(|| -> Result<_, StoreError> {
            let block_ptr_from = Self::block_ptr_with_conn(&site.deployment, &econn)?;
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
            let section = stopwatch.start_section("apply_entity_modifications");
            self.apply_entity_modifications(&econn, mods, Some(&block_ptr_to), stopwatch)?;
            section.end();

            if !deterministic_errors.is_empty() {
                deployment::insert_subgraph_errors(
                    &econn.conn,
                    &site.deployment,
                    deterministic_errors,
                )?;
            }

            let metadata_event =
                deployment::forward_block_ptr(&econn.conn, &site.deployment, block_ptr_to)?;
            Ok(event.extend(metadata_event))
        })?;

        Ok(event)
    }

    pub(crate) fn revert_block_operations(
        &self,
        site: &Site,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<StoreEvent, StoreError> {
        let econn = self.get_entity_conn(site, ReplicaId::Main)?;

        let event = econn.transaction(|| -> Result<_, StoreError> {
            // Unwrap: If we are reverting then the block ptr is not `None`.
            let block_ptr_from = Self::block_ptr_with_conn(&site.deployment, &econn)?.unwrap();

            // Sanity check on block numbers
            if block_ptr_from.number != block_ptr_to.number + 1 {
                panic!("revert_block_operations must revert a single block only");
            }

            // Don't revert past a graft point
            let info = self.subgraph_info_with_conn(&econn.conn, &site.deployment)?;
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

            let metadata_event =
                deployment::revert_block_ptr(&econn.conn, &site.deployment, block_ptr_to)?;

            let (event, count) = econn.revert_block(&block_ptr_from)?;
            econn.update_entity_count(count)?;
            Ok(event.extend(metadata_event))
        })?;

        Ok(event)
    }

    pub(crate) fn deployment_state_from_id(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<DeploymentState, StoreError> {
        let conn = self.get_conn()?;
        deployment::state(&conn, id)
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
        let econn = self.get_entity_conn(&site, ReplicaId::Main)?;
        econn.transaction(|| {
            deployment::unfail(&econn.conn, &site.deployment)?;
            econn.start_subgraph(logger, graft_base)
        })
    }

    #[cfg(debug_assertions)]
    pub fn error_count(&self, id: &SubgraphDeploymentId) -> Result<usize, StoreError> {
        let conn = self.get_conn()?;
        deployment::error_count(&conn, id)
    }
}

impl EthereumCallCache for Store {
    fn get_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
    ) -> Result<Option<Vec<u8>>, Error> {
        use crate::db_schema::{eth_call_cache, eth_call_meta};
        use diesel::dsl::sql;

        let id = contract_call_id(&contract_address, encoded_call, &block);
        let conn = &*self.get_conn()?;
        if let Some(call_output) = conn.transaction::<_, Error, _>(|| {
            if let Some((return_value, update_accessed_at)) = eth_call_cache::table
                .find(id.as_ref())
                .inner_join(eth_call_meta::table)
                .select((
                    eth_call_cache::return_value,
                    sql("CURRENT_DATE > eth_call_meta.accessed_at"),
                ))
                .get_result(conn)
                .optional()?
            {
                if update_accessed_at {
                    update(eth_call_meta::table.find(contract_address.as_ref()))
                        .set(eth_call_meta::accessed_at.eq(sql("CURRENT_DATE")))
                        .execute(conn)?;
                }
                Ok(Some(return_value))
            } else {
                Ok(None)
            }
        })? {
            Ok(Some(call_output))
        } else {
            // No entry with the new id format, try the old one.
            let old_id = old_contract_call_id(&contract_address, &encoded_call, &block);
            if let Some(return_value) = eth_call_cache::table
                .find(old_id.as_ref())
                .select(eth_call_cache::return_value)
                .get_result::<Vec<u8>>(conn)
                .optional()?
            {
                use crate::db_schema::eth_call_cache::dsl;

                // Once we stop getting these logs, this code can be removed.
                trace!(self.logger, "Updating eth call cache entry");

                // Migrate to the new format by re-inserting the call and deleting the old entry.
                self.set_call(contract_address, encoded_call, block, &return_value)?;
                diesel::delete(eth_call_cache::table.filter(dsl::id.eq(old_id.as_ref())))
                    .execute(conn)?;
                Ok(Some(return_value))
            } else {
                Ok(None)
            }
        }
    }

    fn set_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
        return_value: &[u8],
    ) -> Result<(), Error> {
        use crate::db_schema::{eth_call_cache, eth_call_meta};
        use diesel::dsl::sql;

        let id = contract_call_id(&contract_address, encoded_call, &block);
        let conn = &*self.get_conn()?;
        conn.transaction(|| {
            insert_into(eth_call_cache::table)
                .values((
                    eth_call_cache::id.eq(id.as_ref()),
                    eth_call_cache::contract_address.eq(contract_address.as_ref()),
                    eth_call_cache::block_number.eq(block.number as i32),
                    eth_call_cache::return_value.eq(return_value),
                ))
                .on_conflict_do_nothing()
                .execute(conn)?;

            let accessed_at = eth_call_meta::accessed_at.eq(sql("CURRENT_DATE"));
            insert_into(eth_call_meta::table)
                .values((
                    eth_call_meta::contract_address.eq(contract_address.as_ref()),
                    accessed_at.clone(),
                ))
                .on_conflict(eth_call_meta::contract_address)
                .do_update()
                .set(accessed_at)
                .execute(conn)
                .map(|_| ())
                .map_err(Error::from)
        })
    }
}

/// Deprecated format for the contract call id.
fn old_contract_call_id(
    contract_address: &ethabi::Address,
    encoded_call: &[u8],
    block: &EthereumBlockPointer,
) -> [u8; 16] {
    let mut id = [0; 16];
    let mut hash = tiny_keccak::Keccak::new_shake128();
    hash.update(contract_address.as_ref());
    hash.update(encoded_call);
    hash.update(block.hash.as_ref());
    hash.finalize(&mut id);
    id
}

/// The id is the hashed encoded_call + contract_address + block hash to uniquely identify the call.
/// 256 bits of output, and therefore 128 bits of security against collisions, are needed since this
/// could be targeted by a birthday attack.
fn contract_call_id(
    contract_address: &ethabi::Address,
    encoded_call: &[u8],
    block: &EthereumBlockPointer,
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(encoded_call);
    hash.update(contract_address.as_ref());
    hash.update(block.hash.as_ref());
    *hash.finalize().as_bytes()
}
