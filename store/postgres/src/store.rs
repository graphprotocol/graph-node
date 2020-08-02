use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::{insert_into, select, update};
use futures03::FutureExt as _;
use graph::prelude::{CancelGuard, CancelHandle, CancelToken, CancelableError};
use graph::spawn_blocking_async_allow_panic;
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
    debug, ethabi, format_err, futures03, info, o, serde_json, tiny_keccak, tokio, trace, warn,
    web3, AttributeIndexDefinition, BigInt, BlockNumber, ChainHeadUpdateListener as _,
    ChainHeadUpdateStream, ChainStore, CheapClone, DynTryFuture, Entity, EntityKey,
    EntityModification, EntityOrder, EntityQuery, EntityRange, Error, EthereumBlock,
    EthereumBlockPointer, EthereumCallCache, EthereumNetworkIdentifier, Future, LightEthereumBlock,
    Logger, MetadataOperation, MetricsRegistry, QueryExecutionError, Schema, StopwatchMetrics,
    StoreError, StoreEvent, StoreEventStreamBox, Stream, SubgraphAssignmentProviderError,
    SubgraphDeploymentId, SubgraphDeploymentStore, SubgraphEntityPair, TransactionAbortError,
    Value, BLOCK_NUMBER_MAX,
};

use graph_graphql::prelude::api_schema;
use web3::types::{Address, H256};

use crate::chain_head_listener::ChainHeadUpdateListener;
use crate::entities as e;
use crate::functions::{attempt_chain_head_update, lookup_ancestor_block};
use crate::history_event::HistoryEvent;
use crate::metadata;
use crate::relational_queries::FromEntityData;
use crate::store_events::SubscriptionManager;

// TODO: Integrate with https://github.com/graphprotocol/graph-node/pull/1522/files
lazy_static! {
    static ref CONNECTION_LIMITER: Semaphore = {
        // TODO: Consolidate access to db. There are 3 places right now where connections
        // are limited. Move everything to use the `with_connection` API and remove the
        // other limiters
        // See also 82d5dad6-b633-4350-86d9-70c8b2e65805
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
    api: Arc<Schema>,
    /// The name of the network from which the subgraph is syncing
    network: Option<String>,
    /// The block number at which this subgraph was grafted onto
    /// another one. We do not allow reverting past this block
    graft_block: Option<BlockNumber>,
}

pub struct StoreInner {
    logger: Logger,
    subscriptions: Arc<SubscriptionManager>,

    chain_head_update_listener: Arc<ChainHeadUpdateListener>,
    network_name: String,
    genesis_block_ptr: EthereumBlockPointer,
    conn: Pool<ConnectionManager<PgConnection>>,
    read_only_pools: Vec<Pool<ConnectionManager<PgConnection>>>,
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
        config: StoreConfig,
        logger: &Logger,
        net_identifiers: EthereumNetworkIdentifier,
        chain_head_update_listener: Arc<ChainHeadUpdateListener>,
        subscriptions: Arc<SubscriptionManager>,
        pool: Pool<ConnectionManager<PgConnection>>,
        read_only_pools: Vec<Pool<ConnectionManager<PgConnection>>>,
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
            chain_head_update_listener,
            network_name: config.network_name.clone(),
            genesis_block_ptr: (net_identifiers.genesis_block_hash, 0 as u64).into(),
            conn: pool,
            read_only_pools,
            replica_order,
            conn_round_robin_counter: AtomicUsize::new(0),
            subgraph_cache: Mutex::new(LruCache::with_capacity(100)),
            storage_cache: e::make_storage_cache(),
            registry,
        };
        let store = Store(Arc::new(store));

        // Add network to store and check network identifiers
        store.add_network_if_missing(net_identifiers).unwrap();

        // Return the store
        store
    }

    fn add_network_if_missing(
        &self,
        new_net_identifiers: EthereumNetworkIdentifier,
    ) -> Result<(), Error> {
        use crate::db_schema::ethereum_networks::dsl::*;

        let new_genesis_block_hash = new_net_identifiers.genesis_block_hash;
        let new_net_version = new_net_identifiers.net_version;

        let network_identifiers_opt = ethereum_networks
            .select((net_version, genesis_block_hash))
            .filter(name.eq(&self.network_name))
            .first::<(Option<String>, Option<String>)>(&*self.get_conn()?)
            .optional()?;

        match network_identifiers_opt {
            // Network is missing in database
            None => {
                insert_into(ethereum_networks)
                    .values((
                        name.eq(&self.network_name),
                        head_block_hash.eq::<Option<String>>(None),
                        head_block_number.eq::<Option<i64>>(None),
                        net_version.eq::<Option<String>>(Some(new_net_version.to_owned())),
                        genesis_block_hash
                            .eq::<Option<String>>(Some(format!("{:x}", new_genesis_block_hash))),
                    ))
                    .on_conflict(name)
                    .do_nothing()
                    .execute(&*self.get_conn()?)?;
            }

            // Network is in database and has identifiers
            Some((Some(last_net_version), Some(last_genesis_block_hash))) => {
                if last_net_version != new_net_version {
                    panic!(
                        "Ethereum node provided net_version {}, \
                         but we expected {}. Did you change networks \
                         without changing the network name?",
                        new_net_version, last_net_version
                    );
                }

                if last_genesis_block_hash.parse().ok() != Some(new_genesis_block_hash) {
                    panic!(
                        "Ethereum node provided genesis block hash {}, \
                         but we expected {}. Did you change networks \
                         without changing the network name?",
                        new_genesis_block_hash, last_genesis_block_hash
                    );
                }
            }

            // Network is in database but is missing identifiers
            Some(_) => {
                update(ethereum_networks)
                    .set((
                        net_version.eq::<Option<String>>(Some(new_net_version.to_owned())),
                        genesis_block_hash
                            .eq::<Option<String>>(Some(format!("{:x}", new_genesis_block_hash))),
                    ))
                    .filter(name.eq(&self.network_name))
                    .execute(&*self.get_conn()?)?;
            }
        }

        Ok(())
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
            MetadataOperation::AbortUnless {
                description,
                query,
                entity_ids: mut expected_entity_ids,
            } => {
                // Execute query
                let actual_entities =
                    self.execute_query::<Entity>(conn, query.clone())
                        .map_err(|e| {
                            format_err!(
                                "AbortUnless ({}): query execution error: {:?}, {}",
                                description,
                                query,
                                e
                            )
                        })?;

                // Extract IDs from entities
                let mut actual_entity_ids: Vec<String> = actual_entities
                    .into_iter()
                    .map(|entity| entity.id())
                    .collect::<Result<_, _>>()?;

                // Sort entity IDs lexicographically if and only if no sort order is specified.
                // When no sort order is specified, the entity ordering is arbitrary and should not be a
                // factor in deciding whether or not to abort.
                if matches!(query.order, EntityOrder::Default) {
                    expected_entity_ids.sort();
                    actual_entity_ids.sort();
                }

                // Abort if actual IDs do not match expected
                if actual_entity_ids != expected_entity_ids {
                    return Err(TransactionAbortError::AbortUnless {
                        expected_entity_ids,
                        actual_entity_ids,
                        description,
                    }
                    .into());
                }

                // Safe to continue
                Ok(0)
            }
        }
    }

    fn apply_entity_modifications(
        &self,
        conn: &e::Connection,
        mods: Vec<EntityModification>,
        history_event: Option<&HistoryEvent>,
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
                    conn.update(&key, data, history_event).map(|_| 0)
                }
                Insert { key, data } => {
                    let section = stopwatch.start_section("check_interface_entity_uniqueness");
                    self.check_interface_entity_uniqueness(conn, &key)?;
                    section.end();

                    let _section = stopwatch.start_section("apply_entity_modifications_insert");
                    conn.insert(&key, data, history_event).map(|_| 1)
                }
                Remove { key } => conn
                    .delete(&key, history_event)
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

    /// Build a partial Postgres index on a Subgraph-Entity-Attribute
    fn build_entity_attribute_index_with_conn(
        &self,
        conn: &e::Connection,
        index: AttributeIndexDefinition,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        conn.build_attribute_index(&index)
            .map_err(|e| SubgraphAssignmentProviderError::Unknown(e.into()))
            .and_then(move |row_count| match row_count {
                1 => Ok(()),
                _ => Err(SubgraphAssignmentProviderError::BuildIndexesError(
                    index.subgraph_id.to_string(),
                    index.entity_name.clone(),
                    index.attribute_name.clone(),
                )),
            })
    }

    /// Build a set of indexes on the entities table
    fn build_entity_attribute_indexes_with_conn(
        &self,
        conn: &e::Connection,
        indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        for index in indexes.into_iter() {
            self.build_entity_attribute_index_with_conn(conn, index)?;
        }
        Ok(())
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

        let result = spawn_blocking_async_allow_panic(move || {
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
        .await;

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
                .global_subgraph_counter(
                    "subgraph_get_entity_conn_secs",
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
            .global_subgraph_counter(
                "subgraph_get_entity_conn_secs",
                "total time spent getting an entity connection",
                subgraph.as_str(),
            )?
            .inc_by(start.elapsed().as_secs_f64());
        let storage = self.storage(&conn, subgraph)?;
        let metadata = self.storage(&conn, &*SUBGRAPHS_ID)?;
        Ok(e::Connection::new(conn.into(), storage, metadata))
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
    ) -> Result<Arc<e::Storage>, StoreError> {
        if let Some(storage) = self.storage_cache.lock().unwrap().get(subgraph) {
            return Ok(storage.clone());
        }

        let storage = Arc::new(e::Storage::new(conn, subgraph)?);
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
            api: Arc::new(schema),
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
                    assert!(block_ptr_from.number < block_ptr_to.number);
                }

                // Ensure the history event exists in the database
                let history_event = econn.create_history_event(block_ptr_to, &mods)?;

                let should_migrate = econn.should_migrate(&subgraph_id, &block_ptr_to)?;

                // Emit a store event for the changes we are about to make. We
                // wait with sending it until we have done all our other work
                // so that we do not hold a lock on the notification queue
                // for longer than we have to
                let event: StoreEvent = mods.iter().collect();

                // Make the changes
                let section = stopwatch.start_section("apply_entity_modifications");
                self.apply_entity_modifications(&econn, mods, Some(&history_event), stopwatch)?;
                section.end();

                // Update the subgraph block pointer, without an event source; this way
                // no entity history is recorded for the block pointer update itself
                let block_ptr_ops =
                    SubgraphDeploymentEntity::update_ethereum_block_pointer_operations(
                        &subgraph_id,
                        block_ptr_to,
                    );
                let metadata_event =
                    self.apply_metadata_operations_with_conn(&econn, block_ptr_ops)?;
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
        operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        let event =
            econn.transaction(|| self.apply_metadata_operations_with_conn(&econn, operations))?;

        // Send the event separately, because NOTIFY uses a global DB lock.
        econn.transaction(|| econn.send_store_event(&event))
    }

    fn build_entity_attribute_indexes(
        &self,
        subgraph: &SubgraphDeploymentId,
        indexes: Vec<AttributeIndexDefinition>,
    ) -> Result<(), SubgraphAssignmentProviderError> {
        let econn = self.get_entity_conn(subgraph, ReplicaId::Main)?;
        econn.transaction(|| self.build_entity_attribute_indexes_with_conn(&econn, indexes))
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
            let ops = SubgraphDeploymentEntity::update_ethereum_block_pointer_operations(
                &subgraph_id,
                block_ptr_to,
            );
            let metadata_event = self.apply_metadata_operations_with_conn(&econn, ops)?;

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

    fn create_subgraph_deployment(
        &self,
        schema: &Schema,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(&*SUBGRAPHS_ID, ReplicaId::Main)?;
        econn.transaction(|| -> Result<(), StoreError> {
            let event = self.apply_metadata_operations_with_conn(&econn, ops.clone())?;
            econn.create_schema(schema)?;
            econn.send_store_event(&event)
        })
    }

    fn start_subgraph_deployment(
        &self,
        logger: &Logger,
        subgraph_id: &SubgraphDeploymentId,
        ops: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        let econn = self.get_entity_conn(subgraph_id, ReplicaId::Main)?;

        if !econn.uses_relational_schema() {
            warn!(
                logger,
                "This subgraph uses JSONB storage, which is \
              deprecated and support for it will be removed in a future release. \
              Please redeploy the subgraph to address this warning"
            );
        }

        econn.transaction(|| {
            let event = self.apply_metadata_operations_with_conn(&econn, ops)?;
            econn.start_subgraph(logger)?;
            econn.send_store_event(&event)
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
}

impl SubgraphDeploymentStore for Store {
    fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.input)
    }

    fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.api)
    }

    fn uses_relational_schema(&self, subgraph: &SubgraphDeploymentId) -> Result<bool, Error> {
        self.get_entity_conn(subgraph, ReplicaId::Main)
            .map(|econn| econn.uses_relational_schema())
    }

    fn network_name(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Option<String>, Error> {
        Ok(self.subgraph_info(subgraph_id)?.network)
    }
}

impl ChainStore for Store {
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        Ok(self.genesis_block_ptr)
    }

    fn upsert_blocks<B, E>(
        &self,
        blocks: B,
    ) -> Box<dyn Future<Item = (), Error = E> + Send + 'static>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'static,
        E: From<Error> + Send + 'static,
    {
        use crate::db_schema::ethereum_blocks::dsl::*;

        let conn = self.conn.clone();
        let net_name = self.network_name.clone();
        Box::new(blocks.for_each(move |block| {
            let json_blob = serde_json::to_value(&block).expect("Failed to serialize block");
            let values = (
                hash.eq(format!("{:x}", block.block.hash.unwrap())),
                number.eq(block.block.number.unwrap().as_u64() as i64),
                parent_hash.eq(format!("{:x}", block.block.parent_hash)),
                network_name.eq(&net_name),
                data.eq(json_blob),
            );

            // Insert blocks.
            //
            // If the table already contains a block with the same hash, then overwrite that block
            // if it may be adding transaction receipts.
            insert_into(ethereum_blocks)
                .values(values.clone())
                .on_conflict(hash)
                .do_update()
                .set(values)
                .execute(&*conn.get().map_err(Error::from)?)
                .map_err(Error::from)
                .map_err(E::from)
                .map(|_| ())
        }))
    }

    fn upsert_light_blocks(&self, blocks: Vec<LightEthereumBlock>) -> Result<(), Error> {
        use crate::db_schema::ethereum_blocks::dsl::*;

        let conn = self.conn.clone();
        let net_name = self.network_name.clone();
        for block in blocks {
            let block_hash = format!("{:x}", block.hash.unwrap());
            let p_hash = format!("{:x}", block.parent_hash);
            let block_number = block.number.unwrap().as_u64();
            let json_blob = serde_json::to_value(&EthereumBlock {
                block,
                transaction_receipts: Vec::new(),
            })
            .expect("Failed to serialize block");
            let values = (
                hash.eq(block_hash),
                number.eq(block_number as i64),
                parent_hash.eq(p_hash),
                network_name.eq(&net_name),
                data.eq(json_blob),
            );

            // Insert blocks. On conflict do nothing, we don't want to erase transaction receipts.
            insert_into(ethereum_blocks)
                .values(values.clone())
                .on_conflict(hash)
                .do_nothing()
                .execute(&*conn.get()?)?;
        }
        Ok(())
    }

    fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error> {
        // Call attempt_head_update SQL function
        select(attempt_chain_head_update(
            &self.network_name,
            ancestor_count as i64,
        ))
        .load(&*self.get_conn()?)
        .map_err(Error::from)
        // We got a single return value, but it's returned generically as a set of rows
        .map(|mut rows: Vec<_>| {
            assert_eq!(rows.len(), 1);
            rows.pop().unwrap()
        })
        // Parse block hashes into H256 type
        .map(|hashes: Vec<String>| {
            hashes
                .into_iter()
                .map(|h| h.parse())
                .collect::<Result<Vec<H256>, _>>()
        })
        .and_then(|r| r.map_err(Error::from))
    }

    fn chain_head_updates(&self) -> ChainHeadUpdateStream {
        self.chain_head_update_listener
            .subscribe(self.0.network_name.to_owned())
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        use crate::db_schema::ethereum_networks::dsl::*;

        ethereum_networks
            .select((head_block_hash, head_block_number))
            .filter(name.eq(&self.network_name))
            .load::<(Option<String>, Option<i64>)>(&*self.get_conn()?)
            .map(|rows| {
                rows.first()
                    .map(|(hash_opt, number_opt)| match (hash_opt, number_opt) {
                        (Some(hash), Some(number)) => Some((hash.parse().unwrap(), *number).into()),
                        (None, None) => None,
                        _ => unreachable!(),
                    })
                    .and_then(|opt| opt)
            })
            .map_err(Error::from)
    }

    fn blocks(&self, hashes: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error> {
        use crate::db_schema::ethereum_blocks::dsl::*;
        use diesel::dsl::{any, sql};
        use diesel::sql_types::Jsonb;

        ethereum_blocks
            .select(sql::<Jsonb>("data -> 'block'"))
            .filter(network_name.eq(&self.network_name))
            .filter(hash.eq(any(Vec::from_iter(
                hashes.into_iter().map(|h| format!("{:x}", h)),
            ))))
            .load::<serde_json::Value>(&*self.get_conn()?)?
            .into_iter()
            .map(|block| serde_json::from_value(block).map_err(Into::into))
            .collect()
    }

    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        offset: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        if block_ptr.number < offset {
            failure::bail!("block offset points to before genesis block");
        }

        select(lookup_ancestor_block(block_ptr.hash_hex(), offset as i64))
            .first::<Option<serde_json::Value>>(&*self.get_conn()?)
            .map(|val_opt| {
                val_opt.map(|val| {
                    serde_json::from_value::<EthereumBlock>(val)
                        .expect("Failed to deserialize block from database")
                })
            })
            .map_err(Error::from)
    }

    fn cleanup_cached_blocks(&self, ancestor_count: u64) -> Result<(BlockNumber, usize), Error> {
        use crate::db_schema::ethereum_blocks::dsl;
        use diesel::sql_types::{Integer, Text};

        #[derive(QueryableByName)]
        struct MinBlock {
            #[sql_type = "Integer"]
            block: i32,
        };

        // Remove all blocks from the cache that are behind the slowest
        // subgraph's head block, but retain the genesis block. We stay
        // behind the slowest subgraph so that we do not interfere with its
        // syncing activity.
        // We also stay `ancestor_count` many blocks behind the head of the
        // chain since the block ingestor consults these blocks frequently
        //
        // Only consider active subgraphs that have not failed
        let conn = self.get_conn()?;
        let query = "
            select coalesce(
                   least(a.block,
                        (select head_block_number::int - $1
                           from ethereum_networks
                          where name = $2)), -1)::int as block
              from (
                select min(d.latest_ethereum_block_number) as block
                  from subgraphs.subgraph_deployment d,
                       subgraphs.subgraph_deployment_assignment a,
                       subgraphs.ethereum_contract_data_source ds
                 where left(ds.id, 46) = d.id
                   and a.id = d.id
                   and not d.failed
                   and ds.network = $2) a;";
        let ancestor_count = i32::try_from(ancestor_count)
            .expect("ancestor_count fits into a signed 32 bit integer");
        diesel::sql_query(query)
            .bind::<Integer, _>(ancestor_count)
            .bind::<Text, _>(&self.network_name)
            .load::<MinBlock>(&conn)?
            .first()
            .map(|MinBlock { block }| {
                // If we could not determine a minimum block, the query
                // returns -1, and we should not do anything. We also guard
                // against removing the genesis block
                if *block > 0 {
                    diesel::delete(dsl::ethereum_blocks)
                        .filter(dsl::network_name.eq(&self.network_name))
                        .filter(dsl::number.lt(*block as i64))
                        .filter(dsl::number.gt(0))
                        .execute(&conn)
                        .map(|rows| (*block, rows))
                } else {
                    Ok((0, 0))
                }
            })
            .unwrap_or(Ok((0, 0)))
            .map_err(|e| e.into())
    }

    fn block_hashes_by_block_number(&self, number: u64) -> Result<Vec<H256>, Error> {
        use crate::db_schema::ethereum_blocks::dsl;

        let conn = self.get_conn()?;
        dsl::ethereum_blocks
            .select(dsl::hash)
            .filter(dsl::network_name.eq(&self.network_name))
            .filter(dsl::number.eq(number as i64))
            .get_results::<String>(&conn)?
            .into_iter()
            .map(|h| h.parse())
            .collect::<Result<Vec<H256>, _>>()
            .map_err(Error::from)
    }

    fn confirm_block_hash(&self, number: u64, hash: &H256) -> Result<usize, Error> {
        use crate::db_schema::ethereum_blocks::dsl;

        let conn = self.get_conn()?;
        diesel::delete(dsl::ethereum_blocks)
            .filter(dsl::network_name.eq(&self.network_name))
            .filter(dsl::number.eq(number as i64))
            .filter(dsl::hash.ne(&format!("{:x}", hash)))
            .execute(&conn)
            .map_err(Error::from)
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
