use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, PooledConnection};
use diesel::{insert_into, update};
use futures03::FutureExt as _;
use graph::data::subgraph::status;
use graph::prelude::{
    CancelGuard, CancelHandle, CancelToken, CancelableError, NodeId, PoolWaitStats,
    SubgraphVersionSwitchingMode,
};
use lazy_static::lazy_static;
use lru_time_cache::LruCache;
use rand::{seq::SliceRandom, thread_rng};
use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;
use std::ops::Deref;
use std::sync::{atomic::AtomicUsize, Arc, Mutex};
use std::time::Instant;
use tokio::sync::Semaphore;

use graph::components::store::{EntityCollection, QueryStore, Store as StoreTrait};
use graph::components::subgraph::ProofOfIndexingFinisher;
use graph::data::subgraph::schema::{
    SubgraphDeploymentEntity, TypedEntity as _, POI_OBJECT, SUBGRAPHS_ID,
};
use graph::prelude::{
    debug, ethabi, format_err, futures03, info, o, tiny_keccak, tokio, trace, warn, web3,
    ApiSchema, BigInt, BlockNumber, CheapClone, DeploymentState, DynTryFuture, Entity, EntityKey,
    EntityModification, EntityOrder, EntityQuery, EntityRange, Error, EthereumBlockPointer,
    EthereumCallCache, Logger, MetadataOperation, MetricsRegistry, QueryExecutionError, Schema,
    StopwatchMetrics, StoreError, StoreEvent, StoreEventStreamBox, SubgraphDeploymentId,
    SubgraphDeploymentStore, SubgraphEntityPair, SubgraphName, TransactionAbortError, Value,
    BLOCK_NUMBER_MAX,
};

use graph_graphql::prelude::api_schema;
use web3::types::{Address, H256};

use crate::metadata;
use crate::relational::Layout;
use crate::relational_queries::FromEntityData;
use crate::store_events::SubscriptionManager;
use crate::{connection_pool::ConnectionPool, detail, entities as e};

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

    match result {
        Ok(_) => info!(logger, "Completed pending Postgres schema migrations"),
        Err(e) => panic!(
            "Error setting up Postgres database: \
             You may need to drop and recreate your database to work with the \
             latest version of graph-node. Error information: {:?}",
            e
        ),
    };
    // If there was any migration output, log it now
    if !output.is_empty() {
        debug!(
            logger, "Postgres migration output";
            "output" => String::from_utf8(output)
                .unwrap_or_else(|_| String::from("<unreadable>"))
        );
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
struct SubgraphInfo {
    /// The schema as supplied by the user
    input: Arc<Schema>,
    /// The schema we derive from `input` with `graphql::schema::api::api_schema`
    api: Arc<ApiSchema>,
    /// The name of the network from which the subgraph is syncing
    network: Option<String>,
    /// The block number at which this subgraph was grafted onto
    /// another one. We do not allow reverting past this block
    graft_block: Option<BlockNumber>,
}

pub struct StoreInner {
    logger: Logger,
    subscriptions: Arc<SubscriptionManager>,

    conn: ConnectionPool,
    read_only_pools: Vec<ConnectionPool>,
    replica_order: Vec<ReplicaId>,
    conn_round_robin_counter: AtomicUsize,

    /// A cache of commonly needed data about a subgraph.
    subgraph_cache: Mutex<LruCache<SubgraphDeploymentId, SubgraphInfo>>,

    /// A cache for the storage metadata for subgraphs. The Store just
    /// hosts this because it lives long enough, but it is managed from
    /// the entities module
    pub(crate) storage_cache: e::StorageCache,

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
        subscriptions: Arc<SubscriptionManager>,
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
            subscriptions,
            conn: pool,
            read_only_pools,
            replica_order,
            conn_round_robin_counter: AtomicUsize::new(0),
            subgraph_cache: Mutex::new(LruCache::with_capacity(100)),
            storage_cache: e::make_storage_cache(),
            registry,
        };
        let store = Store(Arc::new(store));

        // Return the store
        store
    }

    /// Gets an entity from Postgres.
    fn get_entity(
        &self,
        conn: &e::Connection,
        op_subgraph: &SubgraphDeploymentId,
        op_entity: &String,
        op_id: &String,
    ) -> Result<Option<Entity>, QueryExecutionError> {
        // We should really have callers pass in a block number; but until
        // that is fully plumbed in, we just use the biggest possible block
        // number so that we will always return the latest version,
        // i.e., the one with an infinite upper bound
        conn.find(op_entity, op_id, BLOCK_NUMBER_MAX).map_err(|e| {
            QueryExecutionError::ResolveEntityError(
                op_subgraph.clone(),
                op_entity.clone(),
                op_id.clone(),
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
        let schema = self.api_schema(&key.subgraph_id)?;
        let types_for_interface = schema.types_for_interface();
        let types_with_shared_interface = Vec::from_iter(
            schema
                .interfaces_for_type(&key.entity_type)
                .into_iter()
                .flatten()
                .map(|interface| &types_for_interface[&interface.name])
                .flatten()
                .map(|object_type| &object_type.name)
                .filter(|type_name| **type_name != key.entity_type),
        );

        if !types_with_shared_interface.is_empty() {
            if let Some(conflicting_entity) =
                conn.conflicting_entity(&key.entity_id, types_with_shared_interface)?
            {
                return Err(StoreError::ConflictingId(
                    key.entity_type.clone(),
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
            MetadataOperation::Set { entity, id, data } => {
                let key = MetadataOperation::entity_key(entity, id);

                self.check_interface_entity_uniqueness(conn, &key)?;

                // Load the entity if exists
                let entity = self
                    .get_entity(conn, &key.subgraph_id, &key.entity_type, &key.entity_id)
                    .map_err(Error::from)?;

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
                    format_err!(
                        "Failed to set entity ({}, {}, {}): {}",
                        key.subgraph_id,
                        key.entity_type,
                        key.entity_id,
                        e
                    )
                    .into()
                })
            }
            MetadataOperation::Update { entity, id, data } => {
                let key = MetadataOperation::entity_key(entity, id);

                self.check_interface_entity_uniqueness(conn, &key)?;

                // Update the entity in Postgres
                match conn.update_metadata(&key, &data)? {
                    0 => Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids: vec![key.entity_id.clone()],
                        actual_entity_ids: vec![],
                        description: format!("update for entity {:?} with data {:?} did not change any rows", key, data),
                    }
                    .into()),
                    1 => Ok(0),
                    res => Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids: vec![key.entity_id.clone()],
                        actual_entity_ids: vec![],
                        description: format!(
                            "update for entity {:?} with data {:?} changed {} rows instead of just one",
                            key, data, res
                        ),
                    }
                    .into()),
                }
            }
            MetadataOperation::Remove { entity, id } => {
                let key = MetadataOperation::entity_key(entity, id);
                conn.delete(&key, None)
                    // This conversion is ok since n will only be 0 or 1
                    .map(|n| -(n as i32))
                    .map_err(|e| {
                        format_err!(
                            "Failed to remove entity ({}, {}, {}): {}",
                            key.subgraph_id,
                            key.entity_type,
                            key.entity_id,
                            e
                        )
                        .into()
                    })
            }
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

            let do_count = !modification.entity_key().subgraph_id.is_meta();
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
                        format_err!(
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

    fn apply_metadata_operations_with_conn(
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
    async fn with_conn<T: Send + 'static>(
        &self,
        f: impl 'static
            + Send
            + FnOnce(
                &PooledConnection<ConnectionManager<PgConnection>>,
                &CancelHandle,
            ) -> Result<T, CancelableError>,
    ) -> Result<T, Error> {
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
            let conn = store.get_conn()?;

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
        &self,
        subgraph: &SubgraphDeploymentId,
        f: impl 'static + Send + FnOnce(&e::Connection, &CancelHandle) -> Result<T, CancelableError>,
    ) -> Result<T, Error> {
        let store = self.cheap_clone();
        // Unfortunate clone in order to pass a 'static closure to with_conn.
        let subgraph = subgraph.clone();

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
                    subgraph.as_str(),
                )
                .map_err(Into::<Error>::into)?
                .inc_by(start.elapsed().as_secs_f64());

            cancel_handle.check_cancel()?;
            let storage = store
                .storage(&conn, &subgraph)
                .map_err(Into::<Error>::into)?;
            cancel_handle.check_cancel()?;
            let metadata = store
                .storage(&conn, &*SUBGRAPHS_ID)
                .map_err(Into::<Error>::into)?;
            cancel_handle.check_cancel()?;
            let conn = e::Connection::new(conn.into(), storage, metadata);

            f(&conn, cancel_handle)
        })
        .await
    }

    /// Deprecated. Use `with_conn` instead.
    fn get_conn(&self) -> Result<PooledConnection<ConnectionManager<PgConnection>>, Error> {
        self.conn.get().map_err(Error::from)
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
        subgraph: &SubgraphDeploymentId,
        replica: ReplicaId,
    ) -> Result<e::Connection, Error> {
        let start = Instant::now();
        let conn = match replica {
            ReplicaId::Main => self.get_conn()?,
            ReplicaId::ReadOnly(idx) => self.read_only_conn(idx)?,
        };
        self.registry
            .global_deployment_counter(
                "deployment_get_entity_conn_secs",
                "total time spent getting an entity connection",
                subgraph.as_str(),
            )?
            .inc_by(start.elapsed().as_secs_f64());
        let storage = self.storage(&conn, subgraph)?;
        let metadata = self.storage(&conn, &*SUBGRAPHS_ID)?;
        Ok(e::Connection::new(conn.into(), storage, metadata))
    }

    pub(crate) fn wait_stats(&self, replica: ReplicaId) -> &PoolWaitStats {
        match replica {
            ReplicaId::Main => &self.conn.wait_stats,
            ReplicaId::ReadOnly(idx) => &self.read_only_pools[idx].wait_stats,
        }
    }

    /// Return the storage for the subgraph. Since constructing a `Storage`
    /// object takes a bit of computation, we cache storage objects that do
    /// not have a pending migration in the Store, i.e., for the lifetime of
    /// the Store. Storage objects with a pending migration can not be
    /// cached for longer than a transaction since they might change
    /// without us knowing
    fn storage(
        &self,
        conn: &PgConnection,
        subgraph: &SubgraphDeploymentId,
    ) -> Result<Arc<Layout>, StoreError> {
        if let Some(storage) = self.storage_cache.lock().unwrap().get(subgraph) {
            return Ok(storage.clone());
        }

        let storage = Arc::new(e::Connection::layout(conn, subgraph)?);
        if storage.is_cacheable() {
            &self
                .storage_cache
                .lock()
                .unwrap()
                .insert(subgraph.clone(), storage.clone());
        }
        Ok(storage.clone())
    }

    fn subgraph_info(&self, subgraph_id: &SubgraphDeploymentId) -> Result<SubgraphInfo, Error> {
        if let Some(info) = self.subgraph_cache.lock().unwrap().get(&subgraph_id) {
            return Ok(info.clone());
        }

        let conn = self.get_conn()?;
        let input_schema = metadata::subgraph_schema(&conn, subgraph_id.to_owned())?;
        let network = if subgraph_id.is_meta() {
            // The subgraph of subgraphs schema is built-in. Use an impossible
            // network name so that we will never find blocks for this subgraph
            Some(subgraph_id.as_str().to_owned())
        } else {
            metadata::subgraph_network(&conn, &subgraph_id)?
        };

        let graft_block =
            metadata::deployment_graft(&conn, &subgraph_id)?.map(|(_, ptr)| ptr.number as i32);

        // Generate an API schema for the subgraph and make sure all types in the
        // API schema have a @subgraphId directive as well
        let mut schema = input_schema.clone();
        schema.document = api_schema(&schema.document)?;
        schema.add_subgraph_id_directives(subgraph_id.clone());

        let info = SubgraphInfo {
            input: Arc::new(input_schema),
            api: Arc::new(
                ApiSchema::from_api_schema(schema)
                    .map_err(|e| Error::from_boxed_compat(Box::from(e)))?,
            ),
            network,
            graft_block,
        };

        // Insert the schema into the cache.
        let mut cache = self.subgraph_cache.lock().unwrap();
        cache.insert(subgraph_id.clone(), info);

        Ok(cache.get(&subgraph_id).unwrap().clone())
    }

    fn block_ptr_with_conn(
        subgraph_id: &SubgraphDeploymentId,
        conn: &e::Connection,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        let key = SubgraphDeploymentEntity::key(subgraph_id.clone());
        let subgraph_entity = conn
            .find_metadata(&key.entity_type, &key.entity_id)
            .map_err(|e| format_err!("error reading subgraph entity: {}", e))?
            .ok_or_else(|| {
                format_err!(
                    "could not read block ptr for non-existent subgraph {}",
                    subgraph_id
                )
            })?;

        let hash: Option<H256> = match subgraph_entity.get("latestEthereumBlockHash") {
            None => None,
            Some(value) => value.clone().try_into()?,
        };

        let number: Option<BigInt> = match subgraph_entity.get("latestEthereumBlockNumber") {
            None => None,
            Some(value) => value.clone().try_into()?,
        };

        match (hash, number) {
            (Some(hash), Some(number)) => Ok(Some(EthereumBlockPointer {
                hash,
                number: number.to_u64(),
            })),
            (None, None) => Ok(None),
            _ => Err(format_err!(
                "Ethereum block pointer has invalid `latestEthereumBlockHash` \
                 or `latestEthereumBlockNumber`"
            )),
        }
    }

    fn create_deployment_internal(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        mode: SubgraphVersionSwitchingMode,
        // replace == true is only used in tests; for non-test code, it must
        // be 'false'
        replace: bool,
    ) -> Result<(), StoreError> {
        #[cfg(not(debug_assertions))]
        assert!(!replace);

        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        econn.transaction(|| -> Result<(), StoreError> {
            let exists = metadata::deployment_exists(&econn.conn, &schema.id)?;
            let mut event = if replace || !exists {
                let ops = deployment.create_operations(&schema.id);
                self.apply_metadata_operations_with_conn(&econn, ops)?
            } else {
                StoreEvent::new(vec![])
            };

            if !exists {
                econn.create_schema(schema)?;
            }

            // Create subgraph, subgraph version, and assignment
            let changes =
                metadata::create_subgraph_version(&econn.conn, name, &schema.id, node_id, mode)?;
            event.changes.extend(changes);

            econn.send_store_event(&event)
        })
    }

    // Only for tests to simplify their handling of test fixtures, so that
    // tests can reset the block pointer of a subgraph by recreating it
    #[cfg(debug_assertions)]
    pub fn create_deployment_replace(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        self.create_deployment_internal(name, schema, deployment, node_id, mode, true)
    }

    pub(crate) fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError> {
        let conn = self.get_conn()?;
        conn.transaction(|| -> Result<Vec<status::Info>, StoreError> {
            match filter {
                status::Filter::SubgraphName(name) => {
                    let deployments = detail::deployments_for_subgraph(&conn, name)?;
                    if deployments.is_empty() {
                        Ok(Vec::new())
                    } else {
                        detail::deployment_statuses(&conn, deployments)
                    }
                }
            }
        })
    }
}

impl StoreTrait for Store {
    fn block_ptr(
        &self,
        subgraph_id: SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        Self::block_ptr_with_conn(
            &subgraph_id,
            &self
                .get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)
                .map_err(|e| QueryExecutionError::StoreError(e.into()))?,
        )
    }

    fn supports_proof_of_indexing<'a>(
        &'a self,
        subgraph_id: &'a SubgraphDeploymentId,
    ) -> DynTryFuture<'a, bool> {
        self.with_entity_conn(subgraph_id, |conn, cancel| {
            cancel.check_cancel()?;
            Ok(conn.supports_proof_of_indexing())
        })
        .boxed()
    }

    fn get_proof_of_indexing<'a>(
        &'a self,
        subgraph_id: &'a SubgraphDeploymentId,
        indexer: &'a Option<Address>,
        block_hash: H256,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        let logger = self.logger.cheap_clone();
        let subgraph_id_inner = subgraph_id.clone();
        let indexer = indexer.clone();
        let self_inner = self.cheap_clone();

        async move {
            let entities_blocknumber = self
                .with_entity_conn(subgraph_id, move |conn, cancel| {
                    cancel.check_cancel()?;

                    if !conn.supports_proof_of_indexing() {
                        return Ok(None);
                    }

                    conn.transaction::<_, CancelableError, _>(move || {
                        let latest_block_ptr =
                            match Self::block_ptr_with_conn(&subgraph_id_inner, conn)? {
                                Some(inner) => inner,
                                None => return Ok(None),
                            };

                        cancel.check_cancel()?;

                        // FIXME: (Determinism)
                        // There is no guarantee that we are able to look up the block number
                        // even if we have indexed beyond it, because the cache is sparse and
                        // only populated by blocks with triggers.
                        //
                        // This is labeled as a determinism bug because the PoI needs to be
                        // submitted to the network reliably. Strictly speaking, that's not
                        // indeterminism to miss an opportunity to claim a reward, but it's very
                        // similar to most determinism bugs in that money is on the line.
                        let block_number = self_inner
                            .block_number(&subgraph_id_inner, block_hash)
                            .map_err(|e| CancelableError::Error(e.into()))?;
                        let block_number = match block_number {
                            Some(n) => n.try_into().unwrap(),
                            None => return Ok(None),
                        };
                        cancel.check_cancel()?;

                        // FIXME: (Determinism)
                        // It is vital to ensure that the block hash given in the query
                        // is a parent of the latest block indexed for the subgraph.
                        // Unfortunately the machinery needed to do this is not yet in place.
                        // The best we can do right now is just to make sure that the block number
                        // is high enough.
                        if latest_block_ptr.number < block_number {
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
                                block_number.try_into().unwrap(),
                                None,
                            )
                            .map_err(Error::from)?;

                        Ok(Some((entities, block_number)))
                    })
                })
                .await?;

            let (entities, block_number) = if let Some(entities_blocknumber) = entities_blocknumber
            {
                entities_blocknumber
            } else {
                return Ok(None);
            };

            let mut by_causality_region = entities
                .into_iter()
                .map(|e| {
                    let causality_region = e.id()?;
                    let digest = match e.get("digest") {
                        Some(Value::Bytes(b)) => Ok(b.to_owned()),
                        other => Err(format_err!(
                            "Entity has non-bytes digest attribute: {:?}",
                            other
                        )),
                    }?;

                    Ok((causality_region, digest))
                })
                .collect::<Result<HashMap<_, _>, Error>>()?;

            let block = EthereumBlockPointer {
                number: block_number,
                hash: block_hash,
            };
            let mut finisher = ProofOfIndexingFinisher::new(&block, &subgraph_id, &indexer);
            for (name, region) in by_causality_region.drain() {
                finisher.add_causality_region(&name, &region);
            }

            Ok(Some(finisher.finish()))
        }
        .boxed()
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(&key.subgraph_id, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.get_entity(&conn, &key.subgraph_id, &key.entity_type, &key.entity_id)
    }

    fn get_many(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        ids_for_type: BTreeMap<&str, Vec<&str>>,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError> {
        if ids_for_type.is_empty() {
            return Ok(BTreeMap::new());
        }
        let conn = self
            .get_entity_conn(subgraph_id, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        conn.find_many(ids_for_type, BLOCK_NUMBER_MAX)
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        let conn = self
            .get_entity_conn(&query.subgraph_id, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.execute_query(&conn, query)
    }

    fn find_one(&self, mut query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        query.range = EntityRange::first(1);

        let conn = self
            .get_entity_conn(&query.subgraph_id, ReplicaId::Main)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;

        let mut results = self.execute_query(&conn, query)?;
        match results.len() {
            0 | 1 => Ok(results.pop()),
            n => panic!("find_one query found {} results", n),
        }
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
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
                    format_err!("error looking up ens_name for hash {}: {}", hash, e).into(),
                )
            })
    }

    fn transact_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
    ) -> Result<bool, StoreError> {
        // All operations should apply only to entities in this subgraph or
        // the subgraph of subgraphs
        if mods
            .iter()
            .map(|modification| modification.entity_key())
            .any(|key| key.subgraph_id != subgraph_id && key.subgraph_id != *SUBGRAPHS_ID)
        {
            panic!(
                "transact_block_operations must affect only entities \
                 in the subgraph or in the subgraph of subgraphs"
            );
        }

        let econn = self.get_entity_conn(&subgraph_id, ReplicaId::Main)?;

        let (event, metadata_event, should_migrate) =
            econn.transaction(|| -> Result<_, StoreError> {
                let block_ptr_from = Self::block_ptr_with_conn(&subgraph_id, &econn)?;
                if let Some(ref block_ptr_from) = block_ptr_from {
                    if block_ptr_from.number >= block_ptr_to.number {
                        return Err(StoreError::DuplicateBlockProcessing(
                            subgraph_id,
                            block_ptr_to.number,
                        ));
                    }
                }

                let should_migrate = econn.should_migrate(&subgraph_id, &block_ptr_to)?;

                // Emit a store event for the changes we are about to make. We
                // wait with sending it until we have done all our other work
                // so that we do not hold a lock on the notification queue
                // for longer than we have to
                let event: StoreEvent = mods.iter().collect();

                // Make the changes
                let section = stopwatch.start_section("apply_entity_modifications");
                self.apply_entity_modifications(&econn, mods, Some(&block_ptr_to), stopwatch)?;
                section.end();

                let metadata_event =
                    metadata::forward_block_ptr(&econn.conn, &subgraph_id, block_ptr_to)?;
                Ok((event, metadata_event, should_migrate))
            })?;

        // Send the events separately, because NOTIFY uses a global DB lock.
        econn.transaction(|| {
            econn.send_store_event(&metadata_event)?;
            econn.send_store_event(&event)
        })?;

        Ok(should_migrate)
    }

    /// Apply a series of entity operations. Return `true` if the subgraph
    /// mentioned in `history_event` should have its schema migrated
    fn apply_metadata_operations(
        &self,
        _: &SubgraphDeploymentId,
        operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        let event =
            econn.transaction(|| self.apply_metadata_operations_with_conn(&econn, operations))?;

        // Send the event separately, because NOTIFY uses a global DB lock.
        econn.transaction(|| econn.send_store_event(&event))
    }

    fn revert_block_operations(
        &self,
        subgraph_id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        // Sanity check on block numbers
        if block_ptr_from.number != block_ptr_to.number + 1 {
            panic!("revert_block_operations must revert a single block only");
        }
        // Don't revert past a graft point
        let info = self.subgraph_info(&subgraph_id)?;
        if let Some(graft_block) = info.graft_block {
            if graft_block as u64 > block_ptr_to.number {
                return Err(format_err!(
                    "Can not revert subgraph `{}` to block {} as it was \
                    grafted at block {} and reverting past a graft point \
                    is not possible",
                    subgraph_id,
                    block_ptr_to.number,
                    graft_block
                )
                .into());
            }
        }

        let econn = self.get_entity_conn(&subgraph_id, ReplicaId::Main)?;
        let (event, metadata_event) = econn.transaction(|| -> Result<_, StoreError> {
            assert_eq!(
                Some(block_ptr_from),
                Self::block_ptr_with_conn(&subgraph_id, &econn)?
            );
            let metadata_event =
                metadata::revert_block_ptr(&econn.conn, &subgraph_id, block_ptr_to)?;

            let (event, count) = econn.revert_block(&block_ptr_from)?;
            econn.update_entity_count(count)?;
            Ok((event, metadata_event))
        })?;

        // Send the events separately, because NOTIFY uses a global DB lock.
        econn.transaction(|| {
            econn.send_store_event(&metadata_event)?;
            econn.send_store_event(&event)
        })
    }

    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        self.subscriptions.subscribe(entities)
    }

    fn deployment_state_from_name(
        &self,
        name: SubgraphName,
    ) -> Result<DeploymentState, StoreError> {
        let conn = self.get_conn()?;
        metadata::deployment_state_from_name(&conn, name)
    }

    fn deployment_state_from_id(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<DeploymentState, StoreError> {
        if id.is_meta() {
            Ok(DeploymentState::meta())
        } else {
            let conn = self.get_conn()?;
            metadata::deployment_state_from_id(&conn, id)
        }
    }

    fn create_subgraph_deployment(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        self.create_deployment_internal(name, schema, deployment, node_id, mode, false)
    }

    fn create_subgraph(&self, name: SubgraphName) -> Result<String, StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        econn.transaction(|| metadata::create_subgraph(&econn.conn, &name))
    }

    fn remove_subgraph(&self, name: SubgraphName) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        econn.transaction(|| -> Result<(), StoreError> {
            let changes = metadata::remove_subgraph(&econn.conn, name)?;
            let event = StoreEvent::new(changes);
            econn.send_store_event(&event)
        })
    }

    fn reassign_subgraph(
        &self,
        id: &SubgraphDeploymentId,
        node: &NodeId,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        econn.transaction(|| -> Result<(), StoreError> {
            let changes = metadata::reassign_subgraph(&econn.conn, id, node)?;
            let event = StoreEvent::new(changes);
            econn.send_store_event(&event)
        })
    }

    fn start_subgraph_deployment(
        &self,
        logger: &Logger,
        subgraph_id: &SubgraphDeploymentId,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(subgraph_id, ReplicaId::Main)?;

        econn.transaction(|| {
            metadata::unfail_deployment(&econn.conn, subgraph_id)?;
            econn.start_subgraph(logger)
        })
    }

    fn migrate_subgraph_deployment(
        &self,
        logger: &Logger,
        subgraph_id: &SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    ) {
        let econn = match self.get_entity_conn(subgraph_id, ReplicaId::Main) {
            Ok(econn) => econn,
            Err(e) => {
                warn!(logger, "failed to get connection to start migrating";
                                "subgraph" => subgraph_id.to_string(),
                                "error" => e.to_string(),
                );
                return;
            }
        };

        if let Err(e) = econn.migrate(logger, block_ptr) {
            // An error in a migration should not lead to the
            // subgraph being marked as failed
            warn!(logger, "aborted migrating";
                            "subgraph" => subgraph_id.to_string(),
                            "error" => e.to_string(),
            );
        }
    }

    fn block_number(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        hash: H256,
    ) -> Result<Option<BlockNumber>, StoreError> {
        use crate::db_schema::ethereum_blocks::dsl;

        // We should also really check that the block with the given hash is
        // on the chain starting at the subgraph's current head. That check is
        // very expensive though with the data structures we have currently
        // available. Ideally, we'd have the last REORG_THRESHOLD blocks in
        // memory so that we can check against them, and then mark in the
        // database the blocks on the main chain that we consider final
        let block: Option<(i64, String)> = dsl::ethereum_blocks
            .select((dsl::number, dsl::network_name))
            .filter(dsl::hash.eq(format!("{:x}", hash)))
            .first(&*self.get_conn()?)
            .optional()?;
        let subgraph_network = self.network_name(subgraph_id)?;
        block
            .map(|(number, network_name)| {
                if subgraph_network.is_none() || Some(&network_name) == subgraph_network.as_ref() {
                    BlockNumber::try_from(number)
                        .map_err(|e| StoreError::QueryExecutionError(e.to_string()))
                } else {
                    Err(StoreError::QueryExecutionError(format!(
                        "subgraph {} belongs to network {} but block {:x} belongs to network {}",
                        subgraph_id,
                        subgraph_network.unwrap_or("(none)".to_owned()),
                        hash,
                        network_name
                    )))
                }
            })
            .transpose()
    }

    fn query_store(
        self: Arc<Self>,
        for_subscription: bool,
    ) -> Arc<(dyn QueryStore + Send + Sync + 'static)> {
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

        Arc::new(crate::query_store::QueryStore::new(
            self,
            for_subscription,
            replica_id,
        ))
    }

    fn deployment_synced(&self, id: &SubgraphDeploymentId) -> Result<(), Error> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        econn.transaction(|| {
            let changes = metadata::deployment_synced(&econn.conn, id)?;
            econn.send_store_event(&StoreEvent::new(changes))?;
            Ok(())
        })
    }
}

impl SubgraphDeploymentStore for Store {
    fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.input)
    }

    fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<ApiSchema>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.api)
    }

    fn network_name(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Option<String>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.network)
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

/// Delete all entities. This function exists solely for integration tests
/// and should never be called from any other code. Unfortunately, Rust makes
/// it very hard to export items just for testing
#[cfg(debug_assertions)]
pub use crate::entities::delete_all_entities_for_test_use_only;
